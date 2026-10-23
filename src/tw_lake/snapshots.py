"""Snapshot datasets (adjusted prices, reference tables) with content-hash de-duplication.

A snapshot fully replaces the previous one. Each partition is content-addressed, so unchanged
partitions reuse the existing file, and if *no* partition changed the previous manifest version
stays current and nothing new is written.
"""
from __future__ import annotations

import pandas as pd

from .hashing import stored_content_hash
from .store import DatasetStore, empty_partitions


def publish_snapshot(store: DatasetStore, partitions: dict[str, pd.DataFrame], asof: str,
                     extra_meta: dict | None = None, extra_tables: dict[str, pd.DataFrame] | None = None,
                     all_columns_key: bool = False, keep_existing: bool = False) -> dict:
    """keep_existing=True: partitions not passed in are carried over (append-style daily series)."""
    current = store.partitions()
    cur_map = dict(zip(current["partition"], current["content_hash"])) if len(current) else {}

    cur_records = {r["partition"]: r for _, r in current.iterrows()} if len(current) else {}
    records = []
    for part, df in sorted(partitions.items()):
        keys = list(df.columns) if all_columns_key else store.key_cols
        ch = stored_content_hash(df, keys)
        if part in cur_records and cur_records[part]["content_hash"] == ch and part not in store.bad_partitions:
            records.append({**cur_records[part].to_dict(), "reused": True})   # unchanged: no IO at all
        else:
            records.append(store.publish_partition(df, part, key_cols=keys))
    if keep_existing:
        passed = {r["partition"] for r in records}
        records += [{**r.to_dict(), "reused": True} for p, r in cur_records.items() if p not in passed]
    new_map = {r["partition"]: r["content_hash"] for r in records}
    n_new_files = sum(1 for r in records if not r["reused"])

    if new_map == cur_map and store.pointer() is not None and not store.bad_partitions:
        ptr = store.pointer()
        return {"dataset": store.name, "changed": False, "seq": ptr["seq"], "asof": ptr.get("asof"),
                "new_files": 0, "partitions": len(records)}

    parts = pd.DataFrame(records) if records else empty_partitions()
    tables = {"partitions": parts, **(extra_tables or {})}
    meta = {"asof": asof, "snapshot_partitions": len(records), **(extra_meta or {})}
    changed_parts = sorted(p for p in new_map if cur_map.get(p) != new_map[p])
    removed_parts = sorted(set(cur_map) - set(new_map))
    meta["changed_partitions"] = changed_parts
    meta["removed_partitions"] = removed_parts
    ptr = store.commit(tables, meta)
    return {"dataset": store.name, "changed": True, "seq": ptr["seq"], "asof": asof,
            "new_files": n_new_files, "partitions": len(records),
            "changed_partitions": changed_parts, "removed_partitions": removed_parts}
