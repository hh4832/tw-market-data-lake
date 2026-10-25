"""Quality checks.

Two levels:
  * window_qa(): gate used before a window may be marked complete (hard failures vs source gaps)
  * reconcile_daily(): run-level 5m vs FinLab daily reconciliation, only over the date ranges
    actually fetched in this run, that separates
      data_error                   impossible data (5m high/low outside the daily range)
      missing_in_source            FinLab says traded, Fubon has no bars that day
      suspect                      close/open mismatch, 5m volume above daily, bars without a FinLab trade
      pending_verification         5m volume below the band or daily range wider than 5m range.
                                   NOT auto-explained as a vendor definition difference (audit
                                   2026-10-25): FinLab's volume definition is unverified.
    A volume mismatch alone is never classified as missing data.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

from .calendar import TradingCalendar
from .clock import parse_hhmm


@dataclass
class WindowQA:
    hard_failures: list[str] = field(default_factory=list)
    gap_days: list[str] = field(default_factory=list)
    n_rows: int = 0

    @property
    def ok(self) -> bool:
        return not self.hard_failures


def window_qa(df: pd.DataFrame, cal: TradingCalendar, symbol: str, start: date, end: date,
              session_start: str = "09:00", session_end: str = "13:30") -> WindowQA:
    q = WindowQA(n_rows=int(len(df)))
    if len(df):
        if df["bar_start"].duplicated().any():
            q.hard_failures.append("duplicate_bar_start")
        o, h, l, c = (df[k] for k in ("open", "high", "low", "close"))
        if ((l > o) | (l > c) | (h < o) | (h < c) | (l > h)).any():
            q.hard_failures.append("ohlc_inconsistent")
        if ((o <= 0) | (h <= 0) | (l <= 0) | (c <= 0)).any():
            q.hard_failures.append("non_positive_price")
        if (df["volume_raw"] < 0).any():
            q.hard_failures.append("negative_volume")
        d = df["bar_start"].dt.date
        if ((d < start) | (d > end)).any():
            q.hard_failures.append("bar_outside_requested_range")
        t = df["bar_start"].dt.time
        ss, se = parse_hhmm(session_start), parse_hhmm(session_end)
        out = (t < ss) | (t > se)
        if out.any():
            sample = df.loc[out, "bar_start"].dt.strftime("%Y-%m-%d %H:%M").head(5).tolist()
            q.hard_failures.append(f"bar_outside_session(n={int(out.sum())};e.g.={'|'.join(sample)})")
        market_days = set(cal.trading_days_between(start, end))
        if not set(d.unique()).issubset(market_days):
            q.hard_failures.append("bar_on_non_trading_day")
    have = set(df["bar_start"].dt.date.unique()) if len(df) else set()
    expected = cal.symbol_trade_days_between(symbol, start, end)
    q.gap_days = [x.isoformat() for x in expected if x not in have]
    return q


# ---------------------------------------------------------------------------------------------
def daily_from_minute(m: pd.DataFrame) -> pd.DataFrame:
    if len(m) == 0:
        return pd.DataFrame(columns=["symbol", "date", "m_high", "m_low", "m_open", "m_close", "m_volume_raw", "m_bars"])
    x = m.copy()
    x["date"] = pd.to_datetime(x["bar_start"].dt.date)
    x = x.sort_values("bar_start")
    g = x.groupby(["symbol", "date"])
    return pd.DataFrame({
        "m_open": g["open"].first(), "m_close": g["close"].last(),
        "m_high": g["high"].max(), "m_low": g["low"].min(),
        "m_volume_raw": g["volume_raw"].sum(), "m_bars": g.size(),
        "volume_unit": g["volume_unit"].first(),
    }).reset_index()


def reconcile_daily(minute_daily: pd.DataFrame, finlab_daily: pd.DataFrame, cal: TradingCalendar,
                    symbols: list[str], start: date, end: date, ratio_ok=(0.95, 1.02),
                    price_tol: float = 0.0) -> pd.DataFrame:
    """One row per symbol x FinLab trading day in [start, end] with a classification."""
    fd = finlab_daily.copy()
    fd["date"] = pd.to_datetime(fd["date"]).dt.normalize()
    fd = fd[(fd["symbol"].isin(symbols)) & (fd["date"] >= pd.Timestamp(start)) & (fd["date"] <= pd.Timestamp(end))]
    days = pd.DatetimeIndex(pd.to_datetime(cal.trading_days_between(start, end)))
    grid = pd.MultiIndex.from_product([sorted(symbols), days], names=["symbol", "date"]).to_frame(index=False)
    cols = [c for c in ("open", "high", "low", "close", "volume_shares") if c in fd.columns]
    r = grid.merge(fd[["symbol", "date", *cols]], on=["symbol", "date"], how="left")
    for c in ("open", "close"):
        if c not in r.columns:
            r[c] = np.nan
    md = minute_daily.copy()
    if len(md):
        md["date"] = pd.to_datetime(md["date"]).dt.normalize()
    r = r.merge(md, on=["symbol", "date"], how="left")

    f_trade = r["volume_shares"].fillna(0) > 0
    m_has = r["m_bars"].fillna(0) > 0
    mult = np.where(r.get("volume_unit", pd.Series(index=r.index, dtype=object)).fillna("lot") == "lot", 1000.0, 1.0)
    m_shares = r["m_volume_raw"] * mult
    ratio = m_shares / r["volume_shares"].replace(0, np.nan)
    r["volume_ratio"] = ratio

    hi, lo = r["high"], r["low"]
    mh, ml = r["m_high"], r["m_low"]
    c_none = ~f_trade & ~m_has
    c_missing = f_trade & ~m_has
    c_extra = ~f_trade & m_has
    both = f_trade & m_has
    c_err = both & ((mh > hi + price_tol) | (ml < lo - price_tol))
    close_mm = both & r["close"].notna() & ((r["m_close"] - r["close"]).abs() > price_tol)
    open_mm = both & r["open"].notna() & ((r["m_open"] - r["open"]).abs() > price_tol)
    c_volhi = both & (ratio > ratio_ok[1])
    c_susp = both & ~c_err & (c_volhi | close_mm | open_mm)
    wider = (mh < hi - price_tol) | (ml > lo + price_tol)
    vollo = ratio < ratio_ok[0]
    c_pend = both & ~c_err & ~c_susp & (wider | vollo)
    conds = [c_none, c_missing, c_extra, c_err, c_susp, c_pend]
    cls = np.select(conds, ["no_trade_expected", "missing_in_source", "suspect", "data_error", "suspect",
                            "pending_verification"], default="ok")
    sev = np.select(conds, ["info", "warning", "warning", "error", "warning", "info"], default="info")
    ratio_txt = ratio.map(lambda v: "" if pd.isna(v) else f"{v:.4f}")
    parts = [(c_extra, "bars_without_finlab_trade"), (c_err, "minute_range_outside_daily_range"),
             (close_mm, "close_mismatch"), (open_mm, "open_mismatch"),
             (c_volhi, "minute_volume_exceeds_daily ratio=" + ratio_txt),
             (both & wider, "daily_range_wider_than_minute"),
             (both & vollo, "minute_volume_below_daily ratio=" + ratio_txt)]
    detail = pd.Series("", index=r.index, dtype=object)
    for mask, text in parts:
        detail = detail.where(~mask, detail + np.where(detail == "", "", ";") + text)
    r["classification"], r["severity"], r["detail"] = cls, sev, detail
    return r


def summarize(rec: pd.DataFrame) -> dict:
    if len(rec) == 0:
        return {"rows": 0}
    vc = rec["classification"].value_counts().to_dict()
    return {
        "rows": int(len(rec)),
        "by_classification": {k: int(v) for k, v in vc.items()},
        "symbols_with_data_error": sorted(rec.loc[rec["classification"] == "data_error", "symbol"].unique().tolist())[:50],
        "symbols_with_missing_in_source": sorted(rec.loc[rec["classification"] == "missing_in_source", "symbol"].unique().tolist())[:50],
        "status": "fail" if vc.get("data_error", 0) else ("warn" if vc.get("missing_in_source", 0) or vc.get("suspect", 0) else "pass"),
        "note": "pending_verification = unexplained volume/range difference (not counted as failure, not explained)",
    }


def gap_list_json(days: list[str]) -> str | None:
    return json.dumps(sorted(days)) if days else None
