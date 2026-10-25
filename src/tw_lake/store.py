"""Versioned, content-addressed dataset store on Google Drive.

Layout of one dataset root:

    <root>/data/<partition>/<content_hash[:32]>.parquet     immutable data files
    <root>/_manifest/versions/<seq:06d>_<run_id>/           immutable manifest versions
        meta.json, partitions.parquet, <other tables>.parquet
    <root>/_manifest/published.json                         pointer = current research version
    <root>/_manifest/.staging/                              in-flight manifest versions

Write protocol for a data file (never overwrites, no rename on Drive):
    1. canonicalize + content hash
    2. if <hash>.parquet already exists -> read back, verify content hash, reuse.
       If it exists but is corrupt/partial (and is unreferenced, or recovery marked it bad), it is left
       untouched and the data is written to a fresh path <hash32>-<run_id>.parquet: a path that was
       renamed/moved on Drive FUSE is never written again (2026-10-25: rewrites after a move stayed 0-byte)
    3. write to local tmp, read back, verify rows + content hash, compute SHA256
    4. write directly to the final content-addressed path with exclusive create ('xb') + fsync,
       verify SHA256. No staging->final rename: on Colab's Drive FUSE a rename right after a write
       left 0-byte files on Drive (audit 2026-10-25, F1/F2) although local read-back passed.
       A partial final file is harmless: the name is content-addressed and nothing references it
       until the manifest commit; recovery reports it and the next write replaces it.
    Durability on Drive cannot be proven from inside the same FUSE mount (reads hit the local
    cache); run ``python -m tw_lake.audit.verify`` after flush/unmount + remount.
Manifest commit (after all data files of a batch are published):
    6. write tables to local tmp, verify, copy to _manifest/.staging/<version>, verify SHA256
    7. rename to _manifest/versions/<version>
    8. atomically replace published.json  (this is the commit point)

A crash before step 8 leaves only unreferenced files; recovery_scan() reports them, quarantines
staging leftovers, and verifies that every file referenced by the published version exists.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

from .clock import Clock
from .errors import ImmutableViolation, IntegrityError, ParamsMismatchError
from .hashing import canonicalize, content_hash, file_sha256
from .lock import WriterLock
from .paths import Layout

log = logging.getLogger(__name__)

PARTITION_COLUMNS = [
    "partition", "relpath", "content_hash", "file_sha256", "n_rows", "file_bytes",
    "min_key", "max_key", "created_run_id",
]


def empty_partitions() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="int64" if c in ("n_rows", "file_bytes") else "object")
                         for c in PARTITION_COLUMNS})


@dataclass
class StoreContext:
    layout: Layout
    run_id: str
    local_tmp: Path
    clock: Clock
    lock: WriterLock | None = None
    # test hook: called with a stage name; tests raise to simulate an interrupted write
    fault: Callable[[str], None] = field(default=lambda stage: None)

    def tmp_dir(self) -> Path:
        d = Path(self.local_tmp) / self.run_id
        d.mkdir(parents=True, exist_ok=True)
        return d


def _durable_write(src: Path, dst: Path, fault: Callable[[str], None] = lambda s: None) -> None:
    """Copy src to dst with exclusive create + fsync; never renames, never overwrites."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, "rb") as fi, open(dst, "xb") as fo:
        first = fi.read(max(1, min(4096, os.path.getsize(src) // 2)))
        fo.write(first)
        fault("mid_publish")                       # test hook: crash with a partial final file
        shutil.copyfileobj(fi, fo, 1 << 20)
        fo.flush()
        os.fsync(fo.fileno())


def _move(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(src, dst)
    except OSError:  # e.g. cross-device; shutil.move copies then removes the source
        shutil.move(str(src), str(dst))


class DatasetStore:
    def __init__(self, name: str, ctx: StoreContext, key_cols: list[str], schema_version: str,
                 params: dict | None = None):
        self.name = name
        self.ctx = ctx
        self.root = ctx.layout.dataset(name)
        self.key_cols = list(key_cols)
        self.schema_version = schema_version
        self.params = params or {}
        self._tables_cache: dict[str, pd.DataFrame] | None = None
        # partitions whose published file is missing/corrupt (set by recovery_scan); they are rebuilt
        self.bad_partitions: set[str] = set()

    # ------------------------------------------------------------------ paths
    @property
    def manifest_dir(self) -> Path:
        return self.root / "_manifest"

    @property
    def versions_dir(self) -> Path:
        return self.manifest_dir / "versions"

    @property
    def pointer_path(self) -> Path:
        return self.manifest_dir / "published.json"

    def params_hash(self) -> str:
        import hashlib
        return hashlib.sha256(json.dumps(self.params, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]

    # --------------------------------------------------------------- reading
    def pointer(self) -> dict | None:
        if not self.pointer_path.exists():
            return None
        return json.loads(self.pointer_path.read_text(encoding="utf-8"))

    def check_params(self) -> None:
        p = self.pointer()
        if p and p.get("params_hash") and p["params_hash"] != self.params_hash():
            raise ParamsMismatchError(
                f"{self.name}: request/schema parameters changed (published {p['params_hash']} vs "
                f"current {self.params_hash()}). Old data cannot be mixed with new parameters; "
                f"restore the old config or point the dataset to a new folder."
            )
        if p and p.get("schema_version") and str(p["schema_version"]) != str(self.schema_version):
            raise ParamsMismatchError(f"{self.name}: schema_version {p['schema_version']} != {self.schema_version}")

    def meta(self) -> dict:
        p = self.pointer()
        if not p:
            return {}
        return json.loads((self.manifest_dir / p["version_dir"] / "meta.json").read_text(encoding="utf-8"))

    def tables(self) -> dict[str, pd.DataFrame]:
        if self._tables_cache is not None:
            return self._tables_cache
        p = self.pointer()
        out: dict[str, pd.DataFrame] = {}
        if p:
            vdir = self.manifest_dir / p["version_dir"]
            meta = json.loads((vdir / "meta.json").read_text(encoding="utf-8"))
            for tname, info in meta["tables"].items():
                fp = vdir / f"{tname}.parquet"
                if not fp.exists():
                    raise IntegrityError(f"{self.name}: published manifest table missing: {fp}")
                if file_sha256(fp) != info["sha256"]:
                    raise IntegrityError(f"{self.name}: published manifest table corrupt: {fp}")
                out[tname] = pd.read_parquet(fp)
        if "partitions" not in out:
            out["partitions"] = empty_partitions()
        self._tables_cache = out
        return out

    def partitions(self) -> pd.DataFrame:
        return self.tables()["partitions"].copy()

    def read_record(self, rec: dict | pd.Series, verify: bool = False) -> pd.DataFrame:
        fp = self.root / rec["relpath"]
        if verify and file_sha256(fp) != rec["file_sha256"]:
            raise IntegrityError(f"{self.name}: SHA256 mismatch {fp}")
        return pd.read_parquet(fp)

    def read_partition(self, partition: str, verify: bool = False) -> pd.DataFrame | None:
        if partition in self.bad_partitions:
            return None
        parts = self.partitions()
        hit = parts[parts["partition"] == partition]
        if hit.empty:
            return None
        return self.read_record(hit.iloc[0], verify=verify)

    def read_all(self) -> pd.DataFrame:
        parts = self.partitions()
        if parts.empty:
            return pd.DataFrame()
        return pd.concat([self.read_record(r) for _, r in parts.iterrows()], ignore_index=True)

    # --------------------------------------------------------------- writing
    def publish_partition(self, df: pd.DataFrame, partition: str, key_cols: list[str] | None = None) -> dict:
        """Publish one immutable data file for `partition`; returns a partitions-table record."""
        layout, ctx = self.ctx.layout, self.ctx
        keys = list(key_cols) if key_cols else self.key_cols
        d = canonicalize(df, keys)
        # The content hash is defined on the data *as stored* (after a parquet round trip), so
        # in-memory dtype differences (object vs bool, ns vs us) cannot produce false mismatches.
        local = ctx.tmp_dir() / f"{uuid.uuid4().hex}.parquet"
        d.to_parquet(local, index=False)
        back = pd.read_parquet(local)
        if len(back) != len(d):
            raise IntegrityError(f"local read-back row count mismatch for {partition}")
        _verify_same_values(d, back, partition)
        ch = content_hash(back, keys)
        fname = f"{ch[:32]}.parquet"
        rel = Path("data") / partition / fname
        final = self.root / rel
        layout.assert_writable(final)

        def record(fsha: str, nbytes: int, reused: bool) -> dict:
            k = keys[0]
            return {
                "partition": partition, "relpath": rel.as_posix(), "content_hash": ch,
                "file_sha256": fsha, "n_rows": int(len(d)), "file_bytes": int(nbytes),
                "min_key": _safe_extreme(d[k], "min"), "max_key": _safe_extreme(d[k], "max"),
                "created_run_id": ctx.run_id, "reused": reused,
            }

        if final.exists():  # content-addressed: same content -> same file; verify and reuse
            try:
                back = pd.read_parquet(final)
                same = content_hash(back, keys) == ch
            except Exception:  # noqa: BLE001 - unreadable file counts as corrupt
                same = False
            if same:
                local.unlink(missing_ok=True)
                return record(file_sha256(final), final.stat().st_size, reused=True)
            referenced = rel.as_posix() in self._published_relpaths()
            if referenced and partition not in self.bad_partitions:
                raise ImmutableViolation(f"{final} exists with different content; refusing to overwrite")
            # Never move/rename the bad file and never write at its path again: on Colab's Drive FUSE,
            # "move away + create the same path" left the new file 0-byte on Drive (run
            # 20261025_140026_c169ddf, 82/82 repaired partitions). The bad file stays in place (it is
            # unreferenced after the commit and reported as an orphan); the repaired copy gets a fresh
            # path: <hash32>-<run_id>[-n].parquet
            rel, final = self._fresh_relpath(partition, ch)
            layout.assert_writable(final)
            log.warning("%s %s: existing %s file is %s; writing repaired copy to fresh path %s", self.name,
                        partition, "referenced" if referenced else "unreferenced", "corrupt/partial", rel)

        fsha = file_sha256(local)
        ctx.fault("after_local_verify")
        _durable_write(local, final, fault=ctx.fault)
        if file_sha256(final) != fsha:
            raise IntegrityError(f"published SHA256 mismatch for {final}")
        nbytes = final.stat().st_size
        local.unlink(missing_ok=True)
        ctx.fault("after_publish")
        return record(fsha, nbytes, reused=False)

    def _fresh_relpath(self, partition: str, ch: str) -> tuple[Path, Path]:
        base = f"{ch[:32]}-{self.ctx.run_id}"
        for i in range(1000):
            name = f"{base}.parquet" if i == 0 else f"{base}-{i}.parquet"
            rel = Path("data") / partition / name
            if not (self.root / rel).exists():
                return rel, self.root / rel
        raise ImmutableViolation(f"no fresh path available for {partition}")

    def _published_relpaths(self) -> set[str]:
        parts = self.partitions()          # raises IntegrityError on a corrupt manifest (fail closed)
        return set(parts["relpath"]) if len(parts) else set()

    def _existing_seqs(self) -> list[int]:
        if not self.versions_dir.exists():
            return []
        seqs = []
        for p in self.versions_dir.iterdir():
            try:
                seqs.append(int(p.name.split("_", 1)[0]))
            except ValueError:
                continue
        return seqs

    def commit(self, tables: dict[str, pd.DataFrame], extra_meta: dict | None = None) -> dict:
        """Write a new immutable manifest version and move the published pointer to it."""
        ctx, layout = self.ctx, self.ctx.layout
        if "partitions" not in tables:
            raise ValueError("commit requires a 'partitions' table")
        if ctx.lock is not None:
            ctx.lock.heartbeat()  # fails closed if another writer took over

        ptr = self.pointer()
        seq = max([0, *(self._existing_seqs()), (ptr or {}).get("seq", 0)]) + 1
        vname = f"{seq:06d}_{ctx.run_id}"

        local_dir = ctx.tmp_dir() / f"manifest_{self.name.replace('/', '_')}_{vname}"
        local_dir.mkdir(parents=True, exist_ok=True)
        table_meta = {}
        for tname, tdf in tables.items():
            t = tdf.drop(columns=["reused"], errors="ignore").reset_index(drop=True)
            fp = local_dir / f"{tname}.parquet"
            t.to_parquet(fp, index=False)
            back = pd.read_parquet(fp)
            if len(back) != len(t):
                raise IntegrityError(f"manifest table read-back failed: {tname}")
            table_meta[tname] = {"sha256": file_sha256(fp), "n_rows": int(len(t))}
        meta = {
            "dataset": self.name, "seq": seq, "run_id": ctx.run_id,
            "committed_at": ctx.clock.now().isoformat(), "schema_version": self.schema_version,
            "params": self.params, "params_hash": self.params_hash(), "tables": table_meta,
            **(extra_meta or {}),
        }
        (local_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str),
                                             encoding="utf-8")

        staging = self.manifest_dir / ".staging" / vname
        layout.assert_writable(staging)
        if staging.exists():
            raise ImmutableViolation(f"manifest staging already exists: {staging}")
        for f in sorted(local_dir.iterdir()):             # per-file exclusive write + fsync (no copy2)
            _durable_write(f, staging / f.name)
        for tname, info in table_meta.items():
            if file_sha256(staging / f"{tname}.parquet") != info["sha256"]:
                raise IntegrityError(f"manifest staging SHA256 mismatch: {tname}")
        ctx.fault("manifest_after_staging")

        final = self.versions_dir / vname
        layout.assert_writable(final)
        if final.exists():
            raise ImmutableViolation(f"manifest version already exists: {final}")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final)
        shutil.rmtree(local_dir, ignore_errors=True)
        ctx.fault("manifest_before_pointer")

        pointer = {
            "dataset": self.name, "seq": seq, "version_dir": f"versions/{vname}",
            "run_id": ctx.run_id, "committed_at": meta["committed_at"],
            "schema_version": self.schema_version, "params_hash": self.params_hash(),
            "role": "current_research_version",
            **{k: v for k, v in (extra_meta or {}).items() if k in ("asof", "data_max_date")},
        }
        tmp = self.manifest_dir / f"published.json.{ctx.run_id}.tmp"
        blob = json.dumps(pointer, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        with open(tmp, "wb") as fh:
            fh.write(blob)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.pointer_path)      # atomic pointer swap (the commit point)
        if self.pointer_path.read_bytes() != blob:
            raise IntegrityError(f"{self.name}: published.json read-back differs after commit")
        self._tables_cache = None
        return pointer

    # -------------------------------------------------------------- recovery
    def recovery_scan(self, deep: bool = False, apply: bool = True) -> dict:
        """Inspect and repair the *bookkeeping* of this dataset. Never deletes data files.
        apply=False (dry-run) only reports; nothing is moved."""
        layout, ctx = self.ctx.layout, self.ctx
        report: dict = {"dataset": self.name, "staging_quarantined": [], "uncommitted_versions": [],
                        "missing_files": [], "corrupt_files": [], "orphan_files": [],
                        "pointer_missing_with_versions": False}
        stg = self.manifest_dir / ".staging"
        if stg.exists() and not apply:
            report["staging_quarantined"] = [f"(dry-run, not moved) {p.name}" for p in sorted(stg.iterdir())]
        elif stg.exists():
            for p in sorted(stg.iterdir()):
                dest = layout.quarantine / ctx.run_id / self.name / "manifest_staging" / p.name
                layout.assert_writable(dest)
                _move(p, dest)
                report["staging_quarantined"].append(p.name)
        for p in sorted(self.manifest_dir.glob("published.json.*.tmp")) if apply else []:
            dest = layout.quarantine / ctx.run_id / self.name / p.name
            _move(p, dest)
            report["staging_quarantined"].append(p.name)

        ptr = self.pointer()
        seqs = self._existing_seqs()
        if ptr is None and seqs:
            report["pointer_missing_with_versions"] = True
        cur = (ptr or {}).get("seq", 0)
        report["uncommitted_versions"] = sorted(f"{s:06d}" for s in seqs if s > cur)

        self._tables_cache = None
        parts = self.partitions()
        for _, r in parts.iterrows():
            fp = self.root / r["relpath"]
            if not fp.exists():
                report["missing_files"].append(r["partition"])
            elif fp.stat().st_size != int(r["file_bytes"]) or (deep and file_sha256(fp) != r["file_sha256"]):
                report["corrupt_files"].append(r["partition"])
        self.bad_partitions = set(report["missing_files"]) | set(report["corrupt_files"])

        referenced = set(parts["relpath"]) if len(parts) else set()
        if self.versions_dir.exists():
            for v in self.versions_dir.iterdir():
                fp = v / "partitions.parquet"
                if fp.exists():
                    referenced |= set(pd.read_parquet(fp)["relpath"])
        data_dir = self.root / "data"
        if data_dir.exists():
            for fp in data_dir.rglob("*.parquet"):
                rel = fp.relative_to(self.root).as_posix()
                if rel not in referenced:
                    report["orphan_files"].append(rel)
        return report


def _safe_extreme(s: pd.Series, how: str) -> str:
    """min/max as text for the partitions table; never fails (categoricals, mixed types, NaN)."""
    if len(s) == 0:
        return ""
    try:
        return str(s.min() if how == "min" else s.max())
    except Exception:  # noqa: BLE001
        vals = sorted(s.dropna().astype(str))
        return (vals[0] if how == "min" else vals[-1]) if vals else ""


def _verify_same_values(expected: pd.DataFrame, back: pd.DataFrame, partition: str) -> None:
    """Value-level read-back check (dtype representation may differ, values may not). Vectorized."""
    import numpy as np
    from .hashing import _datetime_ns, _kind
    if list(expected.columns) != list(back.columns) or len(expected) != len(back):
        raise IntegrityError(f"read-back shape/columns differ for {partition}")
    for c in expected.columns:
        a, b = expected[c], back[c]
        ka, kb = _kind(a), _kind(b)
        if ka in ("float", "int") and kb in ("float", "int"):
            ok = np.array_equal(a.to_numpy(dtype="float64", na_value=np.nan),
                                b.to_numpy(dtype="float64", na_value=np.nan), equal_nan=True)
        elif ka.startswith("datetime") and kb.startswith("datetime"):
            ok = np.array_equal(_datetime_ns(a), _datetime_ns(b))
        else:
            sa = pd.Series(np.asarray(a, dtype=object))
            sb = pd.Series(np.asarray(b, dtype=object))
            ok = bool(((sa == sb) | (sa.isna() & sb.isna())).all())
        if not ok:
            raise IntegrityError(f"read-back values differ for {partition} column {c}")


def quarantine_system_staging(layout: Layout, run_id: str) -> list[str]:
    """Move staging leftovers of earlier (crashed) runs to quarantine. Returns moved run ids."""
    moved = []
    if not layout.staging.exists():
        return moved
    for p in sorted(layout.staging.iterdir()):
        if p.name == run_id:
            continue
        if p.is_dir() and not any(x.is_file() for x in p.rglob("*")):
            shutil.rmtree(p)          # only empty directories left behind by a successful run
            continue
        dest = layout.quarantine / run_id / "system_staging" / p.name
        layout.assert_writable(dest)
        _move(p, dest)
        moved.append(p.name)
    return moved


def upsert_partition_records(parts: pd.DataFrame, records: list[dict]) -> pd.DataFrame:
    """Replace records for the given partitions (old file stays on disk and in older versions)."""
    if not records:
        return parts
    new = pd.DataFrame(records)
    keep = parts[~parts["partition"].isin(new["partition"])] if len(parts) else parts
    cols = PARTITION_COLUMNS
    out = pd.concat([keep[cols] if len(keep) else empty_partitions(), new[cols]], ignore_index=True)
    return out.sort_values("partition").reset_index(drop=True)


def cleanup_own_staging(layout: Layout, run_id: str) -> None:
    """Remove this run's staging tree if it only contains empty directories."""
    p = layout.staging / run_id
    if p.exists() and not any(x.is_file() for x in p.rglob("*")):
        shutil.rmtree(p)
