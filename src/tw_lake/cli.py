"""Command line entry point.

    python -m tw_lake.cli --mode smoke
    python -m tw_lake.cli --mode pilot
    python -m tw_lake.cli --mode full --dry-run
    python -m tw_lake.cli --mode full --confirm-full
    python -m tw_lake.cli --mode update
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from typing import Callable

import pandas as pd

from .clock import Clock
from .config import load_config
from .errors import ConfigError, LockHeldError
from .finlab_source import RealFinlabSource
from .fubon_source import RealFubonClient
from .jobs_finlab import run_finlab, run_finlab_readonly
from .jobs_fubon import FubonJob
from .lock import WriterLock
from .run_context import RunContext
from .smoke import run_smoke
from .jobs_fubon_master import run_fubon_master
from .store import DatasetStore, StoreContext, cleanup_own_staging, quarantine_system_staging

EXIT_OK, EXIT_FAILED, EXIT_PARTIAL = 0, 1, 2


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tw_lake")
    p.add_argument("--mode", required=True, choices=["smoke", "pilot", "full", "update"])
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--symbols", default=None, help="comma separated; limits 5m symbols")
    p.add_argument("--max-requests", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="plan + estimate only; writes no dataset/manifest")
    p.add_argument("--confirm-full", action="store_true")
    p.add_argument("--break-stale-lock", action="store_true")
    p.add_argument("--retry-conflicts", action="store_true")
    p.add_argument("--retry-exhausted", action="store_true")
    p.add_argument("--deep-verify", action="store_true", help="SHA256-verify every published file")
    p.add_argument("--skip-fubon-master", action="store_true", help="skip the current-day tickers flag snapshot")
    return p


def pick_pilot_symbols(cfg, targets: pd.DataFrame) -> list[str]:
    tset = set(targets["symbol"])
    picks = [s for s in cfg.raw.get("pilot_symbols", []) if s in tset]
    dl = targets[targets["listing_status_latest"] == "delisted_inferred"].sort_values("last_price_date")
    if len(dl):
        picks += list(dl["symbol"].iloc[[0, len(dl) // 2, len(dl) - 1]].unique())
    inferred = targets[targets["classification_source"] == "code_rule_inferred"]
    picks += list(inferred["symbol"].head(2))
    return sorted(dict.fromkeys(picks))


def qa_summary_md(rc: RunContext, status: str) -> str:
    i = rc.info
    lines = [f"# tw-market-data-lake run {rc.run_id}", "",
             f"- mode: `{rc.mode}`  status: **{status}**  git: `{rc.sha[:12]}`",
             f"- started: {i.get('started_at')}  finished: {i.get('finished_at', '')}", ""]
    fin = i.get("finlab", {})
    if fin:
        lines += ["## FinLab", "```json", json.dumps({k: v for k, v in fin.items() if k != "finlab_daily_raw"} | (
            {"finlab_daily_raw": {k: v for k, v in fin.get("finlab_daily_raw", {}).items() if k != "delta"}}),
            ensure_ascii=False, indent=2, default=str), "```", ""]
    m = i.get("fubon_5m")
    if m:
        recon = m.get("reconciliation", {})
        lines += ["## Fubon 5m", f"- latest available date: {m.get('latest_available_date')}",
                  f"- plan: {m.get('plan')}", f"- requests used: {m.get('requests_used')}  stop: {m.get('stop_reason')}",
                  f"- outcomes: {m.get('outcomes')}", f"- conflicts: {m.get('conflicts')}  new bars: {m.get('new_bars')}",
                  f"- reconciliation status: **{recon.get('status', 'n/a')}**  {recon.get('by_classification', {})}",
                  "  (vendor_definition_diff is informational; only data_error is a data failure)", ""]
    rec = [r for r in i.get("recovery", []) if r.get("missing_files") or r.get("corrupt_files")
           or r.get("staging_quarantined") or r.get("pointer_missing_with_versions") or r.get("uncommitted_versions")]
    if rec or i.get("system_staging_quarantined"):
        lines += ["## Recovery", "```json", json.dumps({"datasets": rec, "system_staging_quarantined":
                                                       i.get("system_staging_quarantined")}, ensure_ascii=False,
                                                      indent=2, default=str)[:6000], "```", ""]
    if i.get("error"):
        lines += ["## Error", "```", str(i["error"])[:4000], "```"]
    return "\n".join(lines)


def main(argv: list[str] | None = None, finlab_factory: Callable | None = None,
         fubon_factory: Callable | None = None, clock: Clock | None = None,
         sleep: Callable[[float], None] = time.sleep, monotonic: Callable[[], float] = time.monotonic) -> int:
    args = build_parser().parse_args(argv)
    overrides = {"data_root": args.data_root} if args.data_root else None
    cfg = load_config(args.config, overrides)
    clock = clock or Clock()
    rc = RunContext.create(cfg, args.mode, clock)
    rc.info.update({"dry_run": args.dry_run, "deep_verify": args.deep_verify,
                    "args": {k: v for k, v in vars(args).items()}})
    finlab_factory = finlab_factory or RealFinlabSource
    fubon_factory = fubon_factory or RealFubonClient.login_from_env
    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else None
    status, lock = "failed", None
    try:
        rc.layout.check_data_root()
        src = finlab_factory()
        if args.mode == "smoke":
            fin = run_finlab_readonly(rc, src)
            client = fubon_factory()
            run_smoke(rc, src, fin, client)
            status = "success"
        elif args.dry_run:
            fin = run_finlab_readonly(rc, src)
            sctx = StoreContext(rc.layout, rc.run_id, cfg.local_tmp, clock, lock=None)
            if args.mode == "pilot" and not symbols:
                symbols = pick_pilot_symbols(cfg, fin.targets)
            rc.info["fubon_5m"] = FubonJob(rc, sctx, None, fin, symbols=symbols, dry_run=True).run()
            status = "success"
        else:
            if args.mode == "full" and not args.confirm_full:
                raise ConfigError("full mode needs --confirm-full (run with --dry-run first to see the estimate)")
            probe_ctx = StoreContext(rc.layout, rc.run_id, cfg.local_tmp, clock)
            if args.mode == "update" and DatasetStore("fubon_5m", probe_ctx, ["bar_start"], "1").pointer() is None:
                raise ConfigError("update needs an existing published fubon_5m manifest; run pilot or full first")
            lock = WriterLock(rc.layout.locks, rc.run_id, clock, int(cfg.manifest["lock_stale_minutes"]),
                              rc.layout.quarantine)
            lock.acquire(break_stale=args.break_stale_lock)
            sctx = StoreContext(rc.layout, rc.run_id, cfg.local_tmp, clock, lock=lock)
            rc.info["system_staging_quarantined"] = quarantine_system_staging(rc.layout, rc.run_id)
            fin = run_finlab(rc, sctx, src)
            rc.info["finlab"] = fin.summaries
            if args.mode == "pilot" and not symbols:
                symbols = pick_pilot_symbols(cfg, fin.targets)
                rc.log.info("pilot symbols: %s", symbols)
            client = fubon_factory()
            if not args.skip_fubon_master:
                with rc.stage("fubon: current-day symbol flags snapshot"):
                    rc.info["fubon_symbol_master"] = run_fubon_master(rc, sctx, client, sleep=sleep)
            with rc.stage("fubon: 5m ingestion"):
                res = FubonJob(rc, sctx, client, fin, symbols=symbols, max_requests=args.max_requests,
                               retry_conflicts=args.retry_conflicts, retry_exhausted=args.retry_exhausted,
                               sleep=sleep, monotonic=monotonic).run()
            rc.info["fubon_5m"] = res
            bad = {k: v for k, v in (res.get("outcomes") or {}).items()
                   if k in ("failed", "qa_fail", "conflict", "write_failed")}
            status = "partial" if (res.get("stop_reason") or bad) else "success"
    except LockHeldError as e:
        rc.info["error"] = str(e)
        rc.log.error("%s", e)
        status = "blocked_by_lock"
    except Exception as e:  # noqa: BLE001 - recorded, then exit non-zero
        rc.info["error"] = "".join(traceback.format_exception(e))
        rc.log.error("run failed: %r", e)
        status = "failed"
    finally:
        if lock is not None:
            cleanup_own_staging(rc.layout, rc.run_id)
            lock.release()
        rc.info["finished_at"] = clock.now().isoformat()
        rc.add_text("qa_summary.md", qa_summary_md(rc, status))
        try:
            rc.finalize(status)
        except Exception as e:  # noqa: BLE001
            rc.log.error("could not write run folder: %r", e)
    return {"success": EXIT_OK, "partial": EXIT_PARTIAL}.get(status, EXIT_FAILED)


if __name__ == "__main__":
    sys.exit(main())
