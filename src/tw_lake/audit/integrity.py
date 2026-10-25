"""A1 - storage / manifest integrity (read-only).

stat_checks()   per published partition, without reading content: existence, size vs manifest
                (0-byte detection), path / content-hash naming, duplicate partitions, optional SHA256
read_checks()   per partition after it has been read: row count, key range, duplicate keys, schema
version_checks() pointer <-> meta <-> version directory consistency, uncommitted versions, staging
reconstruct_handoff()  can the handoff statistics be rebuilt from the manifest alone?
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

from .reader import PublishedVersion, sha256_file

KEY_COLUMNS = {
    "finlab_daily_raw": ["date", "symbol"],
    "finlab_daily_adjusted": ["date", "symbol"],
    "universe_daily": ["date", "symbol"],
    "universe_daily_v2": ["date", "symbol"],
    "fubon_5m": ["bar_start"],
    "security_categories": None,          # snapshot: all columns are the key
    "disposal_attention": None,
    "fubon_symbol_master": ["market", "flag", "symbol"],
}

COLUMNS = ["dataset", "seq", "partition", "relpath", "check", "status", "severity", "detail"]


def _row(pv: PublishedVersion, partition, relpath, check, status, severity, detail="") -> dict:
    return {"dataset": pv.dataset, "seq": pv.seq, "partition": partition, "relpath": relpath,
            "check": check, "status": status, "severity": severity, "detail": str(detail)}


def version_checks(pv: PublishedVersion) -> list[dict]:
    rows = [_row(pv, None, None, i["check"], i["status"],
                 "critical" if i["status"] == "fail" else "info", i.get("detail", ""))
            for i in pv.table_issues]
    if pv.pointer is None:
        return rows
    vdir = pv.version_dir
    rows.append(_row(pv, None, None, "version_dir_exists", "pass" if vdir.exists() else "fail",
                     "critical" if not vdir.exists() else "info", str(vdir)))
    try:
        prefix = int(Path(pv.pointer["version_dir"]).name.split("_", 1)[0])
    except ValueError:
        prefix = None
    ok = prefix == pv.seq and pv.meta.get("seq", pv.seq) == pv.seq
    rows.append(_row(pv, None, None, "seq_consistent", "pass" if ok else "fail", "critical" if not ok else "info",
                     f"pointer.seq={pv.seq} dir_prefix={prefix} meta.seq={pv.meta.get('seq')}"))
    ok = pv.meta.get("run_id", pv.pointer.get("run_id")) == pv.pointer.get("run_id")
    rows.append(_row(pv, None, None, "run_id_consistent", "pass" if ok else "fail", "warning" if not ok else "info",
                     f"pointer={pv.pointer.get('run_id')} meta={pv.meta.get('run_id')}"))
    vroot = pv.root / "_manifest" / "versions"
    later = []
    if vroot.exists():
        for p in vroot.iterdir():
            try:
                if int(p.name.split("_", 1)[0]) > (pv.seq or 0):
                    later.append(p.name)
            except ValueError:
                continue
    rows.append(_row(pv, None, None, "no_uncommitted_versions", "pass" if not later else "warn",
                     "warning" if later else "info", ",".join(sorted(later))))
    stg = pv.root / "_manifest" / ".staging"
    left = sorted(p.name for p in stg.iterdir()) if stg.exists() else []
    rows.append(_row(pv, None, None, "no_manifest_staging_leftovers", "pass" if not left else "warn",
                     "warning" if left else "info", ",".join(left)))
    return rows


def _check_one(pv: PublishedVersion, r, do_sha: bool, deep: bool) -> tuple[list[dict], tuple]:
    rows = []
    part, rel = r.partition, r.relpath
    if not (rel.startswith(f"data/{part}/") and Path(rel).name.startswith(str(r.content_hash)[:32])
            and rel.endswith(".parquet")):
        rows.append(_row(pv, part, rel, "path_matches_partition_and_hash", "fail", "warning", rel))
    fp = pv.data_path(rel)
    try:
        st = os.stat(fp)
    except FileNotFoundError:
        rows.append(_row(pv, part, rel, "file_exists", "fail", "critical", "referenced file missing"))
        return rows, (str(fp), {"exists": False})
    fpr = (str(fp), {"exists": True, "size": st.st_size, "mtime_ns": st.st_mtime_ns})
    if st.st_size == 0:
        rows.append(_row(pv, part, rel, "file_size", "fail", "critical", f"zero_byte_file manifest_bytes={int(r.file_bytes)}"))
    elif st.st_size != int(r.file_bytes):
        rows.append(_row(pv, part, rel, "file_size", "fail", "critical", f"size={st.st_size} manifest_bytes={int(r.file_bytes)}"))
    elif do_sha:
        sha = sha256_file(fp)
        ok = sha == r.file_sha256
        rows.append(_row(pv, part, rel, "file_sha256", "pass" if ok else "fail", "info" if ok else "critical",
                         ("deep" if deep else "sample") if ok else f"got {sha}"))
    return rows, fpr


def stat_checks(pv: PublishedVersion, deep: bool = False, sha_sample: set[str] | None = None,
                only_partitions: set[str] | None = None, workers: int = 32,
                fp_out: dict | None = None) -> list[dict]:
    """Per published partition: exists, size vs manifest (0-byte), naming, optional SHA256.
    Runs in parallel threads; fp_out (if given) receives {path: {size, mtime_ns}} for the
    source-unchanged comparison. only_partitions limits the scope (e.g. --symbols)."""
    from concurrent.futures import ThreadPoolExecutor
    rows: list[dict] = []
    parts = pv.partitions
    if parts.empty:
        rows.append(_row(pv, None, None, "partitions_nonempty", "fail", "critical", "no partitions"))
        return rows
    dup = parts["partition"][parts["partition"].duplicated()].tolist()
    rows.append(_row(pv, None, None, "unique_partitions", "pass" if not dup else "fail",
                     "critical" if dup else "info", ",".join(dup[:20])))
    recs = [r for r in parts.itertuples(index=False) if only_partitions is None or r.partition in only_partitions]
    sample = sha_sample or set()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(lambda r: _check_one(pv, r, deep or r.partition in sample, deep), recs))
    for rr, fpr in results:
        rows += rr
        if fp_out is not None:
            fp_out[fpr[0]] = fpr[1]
    return rows


def _as_ts(v):
    try:
        return pd.Timestamp(v)
    except Exception:  # noqa: BLE001
        return None


def read_checks(pv: PublishedVersion, rec, df: pd.DataFrame, schema_ref: dict) -> list[dict]:
    """rec: one partitions-table row. schema_ref: mutable {'columns': tuple} to compare schemas."""
    rows = []
    part, rel = rec.partition, rec.relpath
    if len(df) != int(rec.n_rows):
        rows.append(_row(pv, part, rel, "row_count", "fail", "critical", f"file={len(df)} manifest={int(rec.n_rows)}"))
    cols = tuple(df.columns)
    if schema_ref.get("columns") is None:
        schema_ref["columns"] = cols
    elif cols != schema_ref["columns"]:
        rows.append(_row(pv, part, rel, "schema_consistent", "fail", "warning",
                         f"columns differ from first partition: {sorted(set(cols) ^ set(schema_ref['columns']))}"))
    keys = KEY_COLUMNS.get(pv.dataset)
    if keys and all(k in df.columns for k in keys) and len(df):
        k0 = df[keys[0]]
        lo, hi = k0.min(), k0.max()
        exp_lo, exp_hi = _as_ts(rec.min_key), _as_ts(rec.max_key)
        got_lo, got_hi = _as_ts(lo), _as_ts(hi)
        same = (exp_lo is not None and got_lo is not None and exp_lo == got_lo and exp_hi == got_hi) or \
               (str(lo) == str(rec.min_key) and str(hi) == str(rec.max_key))
        if not same:
            rows.append(_row(pv, part, rel, "key_range", "fail", "warning",
                             f"file={lo}..{hi} manifest={rec.min_key}..{rec.max_key}"))
        ndup = int(df.duplicated(subset=keys).sum())
        if ndup:
            rows.append(_row(pv, part, rel, "duplicate_keys", "fail", "critical", f"{ndup} duplicated {keys}"))
    return rows


def unreadable(pv: PublishedVersion, rec, err: Exception) -> dict:
    return _row(pv, rec.partition, rec.relpath, "file_readable", "fail", "critical", repr(err)[:300])


def reconstruct_handoff(pv5: PublishedVersion) -> dict:
    """Rebuild the 5m handoff statistics from manifest tables only."""
    w = pv5.tables.get("coverage_by_window", pd.DataFrame())
    p = pv5.partitions
    out = {"manifest": pv5.locator(), "windows": int(len(w))}
    if len(w):
        out["window_status_counts"] = {k: int(v) for k, v in w["status"].value_counts().items()}
        out["symbols"] = int(w["symbol"].nunique())
        out["window_bars_total"] = int(w["n_bars"].sum())
        ngap = lambda ser: sum(len(json.loads(s)) for s in ser.dropna())  # noqa: E731
        cwg = w[w["status"] == "complete_with_gaps"]
        out["complete_with_gaps_symbols"] = int(cwg["symbol"].nunique())
        out["complete_with_gaps_gap_days"] = ngap(cwg["gap_days"])
        out["all_gap_days"] = ngap(w["gap_days"])
        out["partial_windows"] = w.loc[w["status"] == "partial", ["symbol", "window_id", "gap_days"]] \
            .to_dict("records")
        out["covered_to_max"] = str(pd.to_datetime(w["covered_to"]).max().date())
    if len(p):
        out["partitions"] = int(len(p))
        out["partition_rows_total"] = int(p["n_rows"].sum())
        if len(w):
            ps = p.assign(symbol=p["partition"].str.extract(r"symbol=([^/]+)")[0],
                          year=p["partition"].str.extract(r"year=(\d+)")[0].astype(int))
            wy = w.groupby(["symbol", "year"])["n_bars"].sum().reset_index()
            m = wy.merge(ps[["symbol", "year", "n_rows"]], on=["symbol", "year"], how="outer").fillna(0)
            out["symbol_year_bar_mismatches"] = int((m["n_bars"] != m["n_rows"]).sum())
            out["windows_vs_partitions_total_equal"] = out["window_bars_total"] == out["partition_rows_total"]
    out["latest_available_date"] = pv5.meta.get("latest_available_date")
    return out
