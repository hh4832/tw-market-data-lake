"""Recompute research readiness from a finished audit folder - no DATA_ROOT access at all.

python -m tw_lake.audit.rescore --audit-dir /content/drive/MyDrive/.../audit_20261025_163404_e53478a

Reads only the audit's own outputs (audit_5m_by_symbol_day, audit_ohlcv_reconciliation,
audit_integrity) and writes a new sub-folder ``rescore_<readiness_version>_<timestamp>`` next to them:
audit_research_ready.parquet, audit_5m_missing_by_liquidity.parquet, rescore_summary.json/.md.
The original audit files are never modified. Takes seconds instead of re-running the full audit.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .readiness import (DAY_INPUT_COLUMNS, READINESS_VERSION, missing_by_liquidity, parse_allowance,
                        readiness_summary, research_ready)

TAIPEI = ZoneInfo("Asia/Taipei")


def _read(path: Path, columns: list[str]) -> pd.DataFrame:
    import pyarrow.parquet as pq
    have = set(pq.read_schema(path).names)
    return pd.read_parquet(path, columns=[c for c in columns if c in have])


def rescore(audit_dir: Path, out_dir: Path | None = None, allowance: dict | None = None,
            require_close_auction: bool = False) -> Path:
    audit_dir = Path(audit_dir)
    day = _read(audit_dir / "audit_5m_by_symbol_day.parquet", DAY_INPUT_COLUMNS)
    rec_p = audit_dir / "audit_ohlcv_reconciliation.parquet"
    rec = _read(rec_p, ["symbol", "date", "classification"]) if rec_p.exists() else pd.DataFrame()
    integ = pd.read_parquet(audit_dir / "audit_integrity.parquet")
    bad = set(integ.loc[(integ["dataset"] == "fubon_5m") & (integ["status"] == "fail") & integ["partition"].notna(),
                        "partition"])
    allowance = parse_allowance(None) if allowance is None else allowance
    ready = research_ready(day, rec, bad, allowance, require_close_auction)
    hist = missing_by_liquidity(ready)
    summ = readiness_summary(ready, allowance, require_close_auction)

    stamp = datetime.now(TAIPEI).strftime("%Y%m%d_%H%M%S")
    out = Path(out_dir) if out_dir else audit_dir / f"rescore_{READINESS_VERSION}_{stamp}"
    out.mkdir(parents=True, exist_ok=False)
    ready.to_parquet(out / "audit_research_ready.parquet", index=False)
    hist.to_parquet(out / "audit_5m_missing_by_liquidity.parquet", index=False)
    hist.to_csv(out / "audit_5m_missing_by_liquidity.csv", index=False, encoding="utf-8-sig")
    meta = {"source_audit_dir": str(audit_dir), "readiness_version": READINESS_VERSION,
            "created_at": datetime.now(TAIPEI).isoformat(), "bad_partitions": len(bad),
            "statement": "Derived audit output; does not declare the data research-ready."}
    (out / "rescore_summary.json").write_text(json.dumps({"meta": meta, "readiness": summ}, ensure_ascii=False,
                                                         indent=2, default=str), encoding="utf-8")
    md = ["# Readiness rescore", "", f"- source audit: `{audit_dir.name}`", f"- readiness version: {READINESS_VERSION}",
          "- statement: derived audit output; this does **not** declare the data research-ready", "",
          "## Summary", "", "```json", json.dumps(summ, ensure_ascii=False, indent=2, default=str), "```", "",
          "## Missing-count calibration by liquidity bucket", "", "```", hist.to_string(index=False) if len(hist) else "(empty)", "```"]
    (out / "rescore_summary.md").write_text("\n".join(md), encoding="utf-8")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.rescore")
    p.add_argument("--audit-dir", required=True)
    p.add_argument("--out-dir", default=None)
    p.add_argument("--missing-allowance", default=None,
                   help="default ge5000_lots=0,500_5000_lots=10,50_500_lots=none,lt50_lots=none")
    p.add_argument("--require-close-auction", action="store_true",
                   help="v0.2.0 behaviour: tolerant days must have a 13:30 closing-auction bar")
    p.add_argument("--no-require-close-auction", action="store_true", help=argparse.SUPPRESS)  # v0.2.1 default
    a = p.parse_args(argv)
    out = rescore(Path(a.audit_dir), Path(a.out_dir) if a.out_dir else None, parse_allowance(a.missing_allowance),
                  a.require_close_auction)
    s = json.loads((out / "rescore_summary.json").read_text(encoding="utf-8"))["readiness"]
    print(f"rescore written to {out}")
    for k in ("ready_5m_normal_session", "ready_5m_no_trade_tolerant"):
        print(f"  {k}: {s[k]['count']:,} ({s[k]['pct']}%)")
    for b, v in s.get("by_liquidity_bucket", {}).items():
        print(f"  {b:16s} rows={v['rows']:>9,}  strict={v['strict_pct']}%  tolerant={v['tolerant_pct']}%  "
              f"missing p50={v['missing_count_p50']} p90={v['missing_count_p90']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
