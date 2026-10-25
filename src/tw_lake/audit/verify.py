"""Post-run durability check (read-only).

Run it AFTER ``drive.flush_and_unmount()`` + ``drive.mount()``: a fresh mount reads what Drive
really stores instead of the local FUSE cache, which is the only way to see a file that looked
fine to the writer but never reached Drive (audit 2026-10-25, F1/F2).

    python -m tw_lake.audit.verify --data-root "$DATA_ROOT"              # files created by the latest run
    python -m tw_lake.audit.verify --data-root "$DATA_ROOT" --run-id X   # a given run
    python -m tw_lake.audit.verify --data-root "$DATA_ROOT" --all        # every published file (stat only)

Exit code 0 = every checked file matches the manifest (size and SHA256), 1 = problems found.
Nothing is repaired here; the next ``update`` run's recovery scan rebuilds missing/0-byte files.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ..config import load_config
from ..paths import Layout
from . import integrity as A1
from .reader import load_published


def latest_run_id(layout: Layout) -> str | None:
    runs = sorted(p.name for p in layout.runs.iterdir() if p.is_dir()) if layout.runs.exists() else []
    return runs[-1] if runs else None


def verify(layout: Layout, run_id: str | None, check_all: bool = False, sha: bool = True,
           deep_all: bool = False) -> dict:
    report = {"run_id": run_id, "all": check_all, "datasets": {}, "failures": []}
    for name, root in layout.dataset_dirs.items():
        pv = load_published(name, root)
        rows = A1.version_checks(pv)
        parts = pv.partitions
        only = None
        if len(parts) and not check_all:
            only = set(parts.loc[parts["created_run_id"] == run_id, "partition"])
        n = len(parts) if only is None else len(only)
        if n:
            rows += A1.stat_checks(pv, deep=(sha and not check_all) or deep_all, only_partitions=only)
        fails = [r for r in rows if r["status"] == "fail"]
        report["datasets"][name] = {"seq": pv.seq, "checked_partitions": n, "failures": len(fails)}
        report["failures"] += fails
    report["ok"] = not report["failures"]
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.verify")
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--run-id", default=None)
    p.add_argument("--all", action="store_true", help="every published file (stat only, no SHA256)")
    p.add_argument("--no-sha", action="store_true")
    p.add_argument("--deep", action="store_true", help="with --all: also SHA256 every published file")
    p.add_argument("--out", default=None, help="optional local JSON report path")
    a = p.parse_args(argv)
    cfg = load_config(a.config, {"data_root": a.data_root} if a.data_root else None)
    layout = Layout.from_config(cfg)
    run_id = a.run_id or (None if a.all else latest_run_id(layout))
    rep = verify(layout, run_id, a.all, sha=not a.no_sha, deep_all=a.deep)
    for ds, d in rep["datasets"].items():
        print(f"{ds:24s} seq={d['seq']}  checked={d['checked_partitions']:5d}  failures={d['failures']}")
    for f in rep["failures"][:50]:
        print(f"  FAIL {f['dataset']} {f['partition']} {f['check']}: {f['detail']}")
    if len(rep["failures"]) > 50:
        print(f"  ... {len(rep['failures']) - 50} more")
    print("RESULT:", "OK - all checked files are on Drive and match the manifest" if rep["ok"] else
          "PROBLEMS - do not use these partitions; run `update` again (recovery rebuilds them) and re-verify")
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text(json.dumps(rep, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
