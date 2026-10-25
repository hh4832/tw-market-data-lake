"""A5 - gap cases (complete_with_gaps / partial windows) and backfill suggestions.

A gap day is a FinLab trade day without any 5m bar in a window. Classification is evidence
based and never upgrades a case to "verified complete":
    trading_regime_disposal              a disposal call-auction regime was in force
    no_regular_session_trade_probable    FinLab volume < 1 board lot (1000 shares): no regular
                                         board-lot trade was possible that day (odd-lot / other only)
    source_missing_suspected             >= 50 lots traded, no regime: a regular-session trade is
                                         very likely, so the empty 5m day looks like a source gap
    unresolved                           anything else
Two identical API answers (gap_retry_limit) are recorded as ``api_repeat_identical`` evidence,
not as proof of completeness.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

GAP_COLUMNS = ["symbol", "date", "window_id", "window_status", "gap_attempts", "finlab_volume_shares",
               "finlab_turnover", "finlab_close", "regime", "auction_interval_min", "classification",
               "evidence", "manifest_seq"]


def explode_gap_days(windows: pd.DataFrame) -> pd.DataFrame:
    w = windows[windows["gap_days"].notna() & windows["status"].isin(["complete_with_gaps", "partial"])]
    rows = []
    for r in w.itertuples(index=False):
        for g in json.loads(r.gap_days):
            rows.append({"symbol": str(r.symbol), "date": pd.Timestamp(g), "window_id": r.window_id,
                         "window_status": r.status, "gap_attempts": int(r.gap_attempts)})
    return pd.DataFrame(rows, columns=["symbol", "date", "window_id", "window_status", "gap_attempts"])


def classify_gaps(gaps: pd.DataFrame, finlab: pd.DataFrame, regime: pd.DataFrame, seq,
                  source_missing_min_lots: float = 50.0) -> pd.DataFrame:
    if len(gaps) == 0:
        return pd.DataFrame(columns=GAP_COLUMNS)
    f = finlab[["symbol", "date", "volume_shares", "turnover", "close"]].copy()
    f["date"] = pd.to_datetime(f["date"]).dt.normalize()
    x = gaps.merge(f, on=["symbol", "date"], how="left").rename(columns={
        "volume_shares": "finlab_volume_shares", "turnover": "finlab_turnover", "close": "finlab_close"})
    rg = regime[["symbol", "date", "regime", "auction_interval_min"]].copy() if len(regime) else \
        pd.DataFrame(columns=["symbol", "date", "regime", "auction_interval_min"])
    rg["date"] = pd.to_datetime(rg["date"])
    x = x.merge(rg, on=["symbol", "date"], how="left")
    x["regime"] = x["regime"].fillna("normal")
    lots = x["finlab_volume_shares"] / 1000.0
    conds = [x["regime"] != "normal", lots.isna(), lots < 1, lots >= source_missing_min_lots]
    x["classification"] = np.select(conds, ["trading_regime_disposal", "unresolved", "no_regular_session_trade_probable",
                                            "source_missing_suspected"], default="unresolved")
    x["evidence"] = ("finlab_lots=" + lots.round(3).astype(str) + ";api_repeat_identical="
                     + (x["gap_attempts"] >= 2).astype(str) + ";regime=" + x["regime"])
    x["manifest_seq"] = seq
    return x[GAP_COLUMNS].sort_values(["symbol", "date"]).reset_index(drop=True)


def partial_window_plan(windows: pd.DataFrame) -> pd.DataFrame:
    w = windows[windows["status"] == "partial"]
    rows = []
    for r in w.itertuples(index=False):
        g = json.loads(r.gap_days) if r.gap_days else []
        rows.append({"symbol": r.symbol, "window_id": r.window_id, "gap_days": ",".join(g), "n_gap_days": len(g),
                     "suggested_backfill_from": min(g) if g else str(pd.Timestamp(r.window_start).date()),
                     "suggested_backfill_to": max(g) if g else str(pd.Timestamp(r.window_end).date()),
                     "error_class": r.error_class, "phase": "B (requires explicit authorization; not executed)"})
    return pd.DataFrame(rows)


def backfill_candidates(integrity: pd.DataFrame, windows: pd.DataFrame) -> pd.DataFrame:
    """Symbol-years whose published file is missing/0-byte/corrupt: request estimate for Phase B."""
    bad = integrity[(integrity["dataset"] == "fubon_5m") & (integrity["status"] == "fail")
                    & integrity["check"].isin(["file_exists", "file_size", "file_sha256", "file_readable", "row_count"])]
    if len(bad) == 0:
        return pd.DataFrame(columns=["partition", "symbol", "year", "check", "detail", "windows", "estimated_requests"])
    b = bad[["partition", "check", "detail"]].drop_duplicates("partition").copy()
    b["symbol"] = b["partition"].str.extract(r"symbol=([^/]+)")[0]
    b["year"] = b["partition"].str.extract(r"year=(\d+)")[0].astype(int)
    wy = windows.groupby(["symbol", "year"]).apply(
        lambda g: ",".join(f"{a}:{s}" for a, s in zip(g["window_id"], g["status"])), include_groups=False
    ).rename("windows").reset_index()
    wy["symbol"] = wy["symbol"].astype(str)
    out = b.merge(wy, on=["symbol", "year"], how="left")
    out["estimated_requests"] = out["windows"].fillna("").map(lambda s: len([x for x in s.split(",") if x]))
    return out.sort_values("partition").reset_index(drop=True)
