"""Read-only access to published dataset versions.

Nothing here opens a file for writing. All paths are resolved from the dataset's
``_manifest/published.json`` pointer; the raw ``data/`` directory is never globbed to decide
what is research-valid (only to *report* unreferenced files, which is informational).
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

_CHUNK = 1 << 20


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:          # read-only
        for b in iter(lambda: fh.read(_CHUNK), b""):
            h.update(b)
    return h.hexdigest()


@dataclass
class PublishedVersion:
    dataset: str
    root: Path
    pointer: dict | None
    meta: dict = field(default_factory=dict)
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)
    table_issues: list[dict] = field(default_factory=list)

    @property
    def seq(self):
        return (self.pointer or {}).get("seq")

    @property
    def version_dir(self) -> Path | None:
        return self.root / "_manifest" / self.pointer["version_dir"] if self.pointer else None

    @property
    def partitions(self) -> pd.DataFrame:
        return self.tables.get("partitions", pd.DataFrame())

    def data_path(self, relpath: str) -> Path:
        return self.root / relpath

    def locator(self, partition: str | None = None) -> str:
        loc = f"{self.dataset}@seq={self.seq}"
        return f"{loc}:{partition}" if partition else loc


def load_published(dataset: str, root: Path) -> PublishedVersion:
    """Load pointer, meta and manifest tables. Problems are recorded, never raised."""
    ptr_path = root / "_manifest" / "published.json"
    if not ptr_path.exists():
        return PublishedVersion(dataset, root, None,
                                table_issues=[{"check": "pointer_exists", "status": "fail",
                                               "detail": f"missing {ptr_path}"}])
    try:
        pointer = json.loads(ptr_path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        return PublishedVersion(dataset, root, None, table_issues=[
            {"check": "pointer_readable", "status": "fail", "detail": repr(e)}])
    pv = PublishedVersion(dataset, root, pointer)
    vdir = pv.version_dir
    meta_path = vdir / "meta.json"
    if not meta_path.exists():
        pv.table_issues.append({"check": "meta_exists", "status": "fail", "detail": str(meta_path)})
        return pv
    pv.meta = json.loads(meta_path.read_text(encoding="utf-8"))
    for tname, info in pv.meta.get("tables", {}).items():
        fp = vdir / f"{tname}.parquet"
        if not fp.exists():
            pv.table_issues.append({"check": f"table_exists:{tname}", "status": "fail", "detail": str(fp)})
            continue
        sha = sha256_file(fp)
        ok_sha = sha == info.get("sha256")
        pv.table_issues.append({"check": f"table_sha256:{tname}", "status": "pass" if ok_sha else "fail",
                                "detail": "" if ok_sha else f"expected {info.get('sha256')} got {sha}"})
        try:
            t = pd.read_parquet(fp)
        except Exception as e:  # noqa: BLE001
            pv.table_issues.append({"check": f"table_readable:{tname}", "status": "fail", "detail": repr(e)})
            continue
        ok_rows = len(t) == int(info.get("n_rows", -1))
        pv.table_issues.append({"check": f"table_rows:{tname}", "status": "pass" if ok_rows else "fail",
                                "detail": f"meta={info.get('n_rows')} actual={len(t)}"})
        pv.tables[tname] = t
    return pv


# ------------------------------------------------------------------ source fingerprint
def fingerprint(paths: list[Path], hash_small_bytes: int = 5_000_000) -> dict[str, dict]:
    """Stat (and hash, when small) every given path. Used before/after the audit to prove
    that the audit did not change any source file."""
    out = {}
    for p in paths:
        try:
            st = os.stat(p)
        except FileNotFoundError:
            out[str(p)] = {"exists": False}
            continue
        rec = {"exists": True, "size": st.st_size, "mtime_ns": st.st_mtime_ns}
        if p.is_file() and st.st_size <= hash_small_bytes:
            rec["sha256"] = sha256_file(p)
        out[str(p)] = rec
    return out


def control_files(data_root: Path, dataset_dirs: dict[str, Path], system_dir: Path,
                  protected: list[Path]) -> list[Path]:
    """Pointers, manifest tables, lock/staging/quarantine/run listings, protected folders."""
    files: list[Path] = []
    for root in dataset_dirs.values():
        man = root / "_manifest"
        if man.exists():
            files += [p for p in man.rglob("*") if p.is_file()]
    for sub in ("locks", "staging", "quarantine", "runs"):
        d = system_dir / sub
        if d.exists():
            files += [p for p in d.rglob("*") if p.is_file()]
    for prot in protected:
        if prot.exists():
            files += [p for p in prot.rglob("*") if p.is_file()]
    return sorted(set(files))


def diff_fingerprints(before: dict, after: dict) -> list[dict]:
    changes = []
    for k in sorted(set(before) | set(after)):
        b, a = before.get(k), after.get(k)
        if b != a:
            changes.append({"path": k, "before": b, "after": a})
    return changes
