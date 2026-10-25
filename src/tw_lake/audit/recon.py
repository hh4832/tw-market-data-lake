"""A3 - daily OHLCV rebuilt from 5m vs FinLab raw (unadjusted) daily.

Differences are kept as raw values (ratio and difference), never collapsed into an
"explained" class. Volume: minute volume is converted with its own ``volume_unit``
(lot -> x1000 shares); FinLab ``成交股數`` is shares and (per vendor definitions, unverified
here) may include after-hours fixed-price, block and odd-lot trades, so a ratio < 1 is
``pending_verification`` - not automatically a vendor definition difference.

classification precedence
    data_error_candidate   5m high/low outside FinLab daily range (impossible if both are correct)
    suspect                close or open mismatch, 5m volume above daily, bars on a non-trading/no-trade day
    missing_5m_day         FinLab traded, no 5m bars
    pending_verification   volume ratio below band or daily range wider than 5m range
    ok
Thresholds are exploratory (config ``audit.volume_ratio_band``) and reported, not official rules.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

UNIT_MULT = {"lot": 1000.0, "share": 1.0, "shares": 1.0}

RECON_COLUMNS = [
    "symbol", "date", "f_open", "f_high", "f_low", "f_close", "f_volume_shares", "m_open", "m_high", "m_low",
    "m_close", "m_volume_raw", "volume_unit", "m_volume_shares", "m_bars", "m_first_bar", "m_last_bar",
    "volume_ratio", "volume_diff_shares", "open_diff", "close_diff", "high_diff", "low_diff",
    "flags", "classification", "severity",
]


def reconcile(minute_daily: pd.DataFrame, finlab: pd.DataFrame, trading_days: set,
              band=(0.95, 1.02), price_tol: float = 1e-9) -> pd.DataFrame:
    f = finlab.rename(columns={"open": "f_open", "high": "f_high", "low": "f_low", "close": "f_close",
                               "volume_shares": "f_volume_shares"}).copy()
    f["date"] = pd.to_datetime(f["date"]).dt.normalize()
    f = f[["symbol", "date", "f_open", "f_high", "f_low", "f_close", "f_volume_shares"]]
    m = minute_daily.copy()
    if len(m):
        m["date"] = pd.to_datetime(m["date"]).dt.normalize()
    r = f.merge(m, on=["symbol", "date"], how="outer")
    f_trade = r["f_volume_shares"].fillna(0) > 0
    m_has = r["m_bars"].fillna(0) > 0
    r = r[f_trade | m_has].copy()
    f_trade, m_has = f_trade[r.index], m_has[r.index]
    mult = r["volume_unit"].map(UNIT_MULT).fillna(1000.0)
    r["m_volume_shares"] = r["m_volume_raw"] * mult
    r["volume_ratio"] = r["m_volume_shares"] / r["f_volume_shares"].where(r["f_volume_shares"] > 0)
    r["volume_diff_shares"] = r["m_volume_shares"] - r["f_volume_shares"]
    for k in ("open", "close", "high", "low"):
        r[f"{k}_diff"] = r[f"m_{k}"] - r[f"f_{k}"]
    both = f_trade & m_has
    outside = both & ((r["m_high"] > r["f_high"] + price_tol) | (r["m_low"] < r["f_low"] - price_tol))
    close_mm = both & (r["close_diff"].abs() > price_tol)
    open_mm = both & (r["open_diff"].abs() > price_tol)
    vol_hi = both & (r["volume_ratio"] > band[1])
    vol_lo = both & (r["volume_ratio"] < band[0])
    wider = both & ((r["m_high"] < r["f_high"] - price_tol) | (r["m_low"] > r["f_low"] + price_tol))
    extra = m_has & ~f_trade
    non_td = m_has & ~r["date"].isin(pd.to_datetime(sorted(trading_days)))
    missing = f_trade & ~m_has
    flag_defs = [("price_outside_daily_range", outside), ("close_mismatch", close_mm), ("open_mismatch", open_mm),
                 ("volume_above_daily", vol_hi), ("volume_below_band", vol_lo),
                 ("daily_range_wider_than_5m", wider), ("bars_without_finlab_trade", extra),
                 ("bars_on_non_trading_day", non_td), ("missing_5m_day", missing)]
    flags = pd.Series("", index=r.index, dtype=object)
    for name, mask in flag_defs:
        flags = flags.where(~mask, flags + np.where(flags == "", "", ";") + name)
    r["flags"] = flags
    conds = [outside, close_mm | open_mm | vol_hi | extra | non_td, missing, vol_lo | wider]
    r["classification"] = np.select(conds, ["data_error_candidate", "suspect", "missing_5m_day",
                                            "pending_verification"], default="ok")
    r["severity"] = r["classification"].map({"data_error_candidate": "critical", "suspect": "warning",
                                             "missing_5m_day": "warning", "pending_verification": "info",
                                             "ok": "info"})
    return r.reindex(columns=RECON_COLUMNS).sort_values(["symbol", "date"]).reset_index(drop=True)


def summarize(rec: pd.DataFrame, day_audit: pd.DataFrame | None = None) -> dict:
    if len(rec) == 0:
        return {"rows": 0}
    out = {"rows": int(len(rec)), "denominator": "symbol-days with FinLab trade or 5m bars",
           "by_classification": {k: int(v) for k, v in rec["classification"].value_counts().items()}}
    both = rec[rec["volume_ratio"].notna()].copy()
    both["volume_ratio"] = both["volume_ratio"].astype(float)
    both["date"] = pd.to_datetime(both["date"])
    flag_counts = rec["flags"].fillna("").str.split(";").explode()
    out["flag_counts"] = {k: int(v) for k, v in flag_counts[flag_counts != ""].value_counts().items()}
    if len(both) == 0:
        out["volume_ratio_quantiles"] = None
        return out
    q = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    out["volume_ratio_quantiles"] = {str(k): round(float(v), 4) for k, v in both["volume_ratio"].quantile(q).items()}
    by_year = both.groupby(both["date"].dt.year)["volume_ratio"].quantile([0.05, 0.5, 0.95]).unstack()
    out["volume_ratio_by_year"] = {str(y): {str(k): round(float(v), 4) for k, v in r.items()} for y, r in by_year.iterrows()}
    if day_audit is not None and len(day_audit):
        b = both.merge(day_audit[["symbol", "date", "liquidity_bucket"]], on=["symbol", "date"], how="left")
        lq = b.groupby("liquidity_bucket")["volume_ratio"].quantile([0.05, 0.5, 0.95]).unstack()
        out["volume_ratio_by_liquidity"] = {str(i): {str(k): round(float(v), 4) for k, v in r.items()} for i, r in lq.iterrows()}
    return out


def top_anomalies(rec: pd.DataFrame, n: int = 100) -> pd.DataFrame:
    sev = {"critical": 0, "warning": 1, "info": 2}
    x = rec[rec["classification"] != "ok"].copy()
    x["_s"] = x["severity"].map(sev)
    x["_mag"] = (1 - x["volume_ratio"]).abs().fillna(0) + x[["high_diff", "low_diff", "close_diff"]].abs().sum(axis=1).fillna(0)
    return x.sort_values(["_s", "_mag"], ascending=[True, False]).head(n).drop(columns=["_s", "_mag"])
