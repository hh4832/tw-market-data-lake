"""python -m tw_lake.audit.cli --data-root /content/drive/MyDrive/00Quant_Research/data --out-dir /content/tw_lake_audits

Read-only. Never calls a market-data API, never writes under DATA_ROOT except an optional copy of
the finished audit folder into ``<system_dir>/audits/<audit_id>`` (``--publish-to-drive``).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ..config import load_config
from .readiness import parse_allowance
from .runner import AuditOptions, run_audit


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tw_lake.audit")
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--out-dir", default="/content/tw_lake_audits", help="local folder for audit outputs")
    p.add_argument("--publish-to-drive", action="store_true",
                   help="after finishing, copy the audit folder to <data_root>/<system_dir>/audits/<audit_id>")
    p.add_argument("--symbols", default=None, help="comma separated subset of 5m symbols")
    p.add_argument("--max-partitions", type=int, default=None)
    p.add_argument("--deep-verify", action="store_true", help="SHA256 every published file")
    p.add_argument("--sha-sample", type=int, default=20, help="random published files per dataset to SHA256")
    p.add_argument("--template-symbols", default="2330,0050,006208")
    p.add_argument("--volume-ratio-band", default="0.95,1.02")
    p.add_argument("--missing-allowance", default=None,
                   help="readiness v0.2, e.g. ge5000_lots=0,500_5000_lots=10,50_500_lots=none,lt50_lots=none")
    p.add_argument("--no-progress", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)
    cfg = load_config(a.config, {"data_root": a.data_root} if a.data_root else None)
    lo, hi = (float(x) for x in a.volume_ratio_band.split(","))
    opts = AuditOptions(
        out_root=Path(a.out_dir), symbols=[s.strip() for s in a.symbols.split(",")] if a.symbols else None,
        max_partitions=a.max_partitions, deep_verify=a.deep_verify, sha_sample=a.sha_sample,
        template_symbols=tuple(s.strip() for s in a.template_symbols.split(",")), volume_ratio_band=(lo, hi),
        publish_dir=(cfg.data_root / cfg.raw["system_dir"] / "audits") if a.publish_to_drive else None,
        missing_allowance=parse_allowance(a.missing_allowance),
        progress=not a.no_progress)
    out = run_audit(cfg, opts)
    print(f"audit written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
