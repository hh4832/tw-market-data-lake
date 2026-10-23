"""Point-in-time universe built from FinLab daily prices + a *current* classification snapshot.

Separate facts are kept apart on purpose:
  has_price        FinLab close is present that day
  has_trade        FinLab volume_shares > 0 that day
  listing_status   inferred listing state (from first/last FinLab price date)
  tradable_status  traded / listed_no_trade (suspension, no volume ...)
Classification (common_stock / etf / other) comes from FinLab `security_categories`, which is a
snapshot of *today*. It is applied to history but labelled as such (classification_is_pit=False,
classification_source, classification_asof); symbols missing from the snapshot (typically
delisted) get a code-rule classification with source "code_rule_inferred".
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

MINUTE_TYPES = ("common_stock", "etf")


def _code_rule(stock_id: str) -> str:
    s = str(stock_id).strip()
    if s.startswith("00") and 4 <= len(s) <= 7:
        return "etf"
    if len(s) == 4 and s.isdigit():
        return "common_stock"
    return "other"


def classify(security_categories: pd.DataFrame, symbols: list[str], asof: str) -> pd.DataFrame:
    """Return one row per symbol: instrument_type, market, category, classification_source, ..."""
    cats = security_categories.copy()
    for c in ("stock_id", "category", "market"):
        if c not in cats.columns:
            raise KeyError(f"security_categories missing column {c}")
        cats[c] = cats[c].astype(str).str.strip()
    cats = cats.drop_duplicates("stock_id", keep="last").set_index("stock_id")

    rows = []
    for sym in sorted(set(map(str, symbols)) | set(cats.index)):
        if sym in cats.index:
            r = cats.loc[sym]
            cat, mkt = r["category"], r["market"]
            if mkt not in ("sii", "otc"):
                itype = "other"
            elif "存託憑證" in cat:
                itype = "other"
            elif sym.startswith("00") or "ETF" in cat.upper():
                itype = "etf"
            elif len(sym) == 4 and sym.isdigit():
                itype = "common_stock"
            else:
                itype = "other"
            src, in_cur = "security_categories_current_snapshot", True
        else:
            cat, mkt = None, None
            itype = _code_rule(sym)
            src, in_cur = "code_rule_inferred", False
        rows.append({
            "symbol": sym, "instrument_type": itype, "market": mkt, "category": cat,
            "classification_source": src, "classification_asof": asof,
            "classification_is_pit": False, "in_current_security_categories": in_cur,
        })
    return pd.DataFrame(rows)


def symbol_master_pit(raw_long: pd.DataFrame, classification: pd.DataFrame, trading_days: list[date],
                      delist_gap_trading_days: int) -> pd.DataFrame:
    df = raw_long[["date", "symbol", "close", "volume_shares"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    priced = df[df["close"].notna()]
    g = priced.groupby("symbol")["date"]
    m = pd.DataFrame({"first_price_date": g.min(), "last_price_date": g.max()})
    m["n_trade_days"] = df[df["volume_shares"].fillna(0) > 0].groupby("symbol").size()
    m["n_trade_days"] = m["n_trade_days"].fillna(0).astype("int64")
    m.index = m.index.astype(str)
    m.index.name = "symbol"
    dataset_start = pd.Timestamp(trading_days[0])
    m["first_price_is_dataset_start"] = m["first_price_date"] <= dataset_start
    cutoff_idx = max(0, len(trading_days) - 1 - delist_gap_trading_days)
    cutoff = pd.Timestamp(trading_days[cutoff_idx])
    m["listing_status_latest"] = np.where(m["last_price_date"] < cutoff, "delisted_inferred", "listed")
    m["listing_status_source"] = "inferred_from_finlab_price"
    out = m.reset_index().merge(classification, on="symbol", how="left")
    out["instrument_type"] = out["instrument_type"].fillna("other")
    return out


def universe_daily(raw_long: pd.DataFrame, master: pd.DataFrame, trading_days: list[date]) -> pd.DataFrame:
    """One row per symbol x trading day inside the symbol's [first_price_date, last_price_date]."""
    td = pd.DatetimeIndex(pd.to_datetime(trading_days))
    df = raw_long[["date", "symbol", "close", "volume_shares"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["symbol"] = df["symbol"].astype(str)

    lo = np.searchsorted(td.values, master["first_price_date"].values.astype("datetime64[ns]"), side="left")
    hi = np.searchsorted(td.values, master["last_price_date"].values.astype("datetime64[ns]"), side="right")
    lens = np.maximum(hi - lo, 0)
    sym_rep = np.repeat(master["symbol"].astype(str).values, lens)
    idx = np.concatenate([np.arange(a, b) for a, b in zip(lo, hi)]) if lens.sum() else np.array([], dtype=int)
    grid = pd.DataFrame({"date": td.values[idx] if len(idx) else pd.to_datetime([]), "symbol": sym_rep})

    u = grid.merge(df, on=["date", "symbol"], how="left")
    u["has_price"] = u["close"].notna()
    u["has_trade"] = u["volume_shares"].fillna(0) > 0
    u["listing_status"] = "listed"
    u["tradable_status"] = np.where(u["has_trade"], "traded", "listed_no_trade")
    keep = master[["symbol", "instrument_type", "market", "classification_source", "classification_asof",
                   "classification_is_pit"]]
    u = u.merge(keep, on="symbol", how="left")
    u["listing_status_source"] = "inferred_from_finlab_price"
    return u.drop(columns=["close", "volume_shares"])


def minute_targets(master: pd.DataFrame, minute_start: date) -> pd.DataFrame:
    """Symbols that need 5m data: stock/ETF that had a price on/after minute_start."""
    m = master[master["instrument_type"].isin(MINUTE_TYPES)]
    m = m[m["last_price_date"] >= pd.Timestamp(minute_start)]
    return m[["symbol", "instrument_type", "market", "first_price_date", "last_price_date",
              "listing_status_latest", "classification_source"]].sort_values("symbol").reset_index(drop=True)
