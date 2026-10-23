"""Fubon current-day symbol lists (attention / disposition / halted flags, ETF security types).

These are CURRENT-DAY snapshots only (Fubon has no history for these flags). They are stored with
content-hash de-duplication so a history of daily snapshots accumulates as the job runs, but no
historical claim is made for dates before the first snapshot.
"""
from __future__ import annotations

import pandas as pd

from . import SCHEMA_VERSION
from .fubon_source import FubonCaller, MarketClient, RateLimiter, RequestBudget, RetryPolicy
from .jobs_finlab import prepare_store
from .run_context import RunContext
from .snapshots import publish_snapshot
from .store import DatasetStore, StoreContext

FLAGS = ("isNormal", "isAttention", "isDisposition", "isHalted")


def run_fubon_master(rc: RunContext, sctx: StoreContext, client: MarketClient, sleep=None) -> dict:
    f = rc.cfg.fubon
    caller = FubonCaller(RateLimiter(int(f["intraday_requests_per_min"])),
                         RetryPolicy(int(f["max_attempts_per_request"]), float(f["rate_limit_wait_sec"]),
                                     float(f["backoff_base_sec"]), float(f["backoff_max_sec"])),
                         RequestBudget(50), **({"sleep": sleep} if sleep else {}))
    rows, errors = [], []
    for market in ("TSE", "OTC"):
        for flag in FLAGS:
            params = {"type": "EQUITY", "market": market, flag: True}
            try:
                resp = caller.call(client.tickers, params) or {}
                for r in resp.get("data") or []:
                    rows.append({"market": market, "flag": flag, "symbol": str(r.get("symbol")),
                                 "name": r.get("name"), "list_date": resp.get("date")})
            except Exception as e:  # noqa: BLE001 - recorded; never blocks 5m ingestion
                errors.append({"market": market, "flag": flag, "error": f"{type(e).__name__}: {e}"[:300]})
    if not rows:
        return {"changed": False, "rows": 0, "errors": errors}
    df = pd.DataFrame(rows)
    dates = df["list_date"].dropna()
    asof = str(dates.max()) if len(dates) else rc.clock.now().date().isoformat()
    store = DatasetStore("fubon_symbol_master", sctx, ["market", "flag", "symbol"], SCHEMA_VERSION,
                         params={"source": "fubon", "endpoint": "intraday/tickers", "flags": list(FLAGS)})
    prepare_store(rc, store)
    res = publish_snapshot(store, {f"asof={asof}": df}, asof,
                           {"note": "current-day flags only; not point-in-time history", "errors": errors},
                           keep_existing=True)  # previous days' snapshots stay in the current version
    return {**res, "rows": int(len(df)), "errors": errors}
