"""trading_regime_daily: per symbol/date trading regime derived from FinLab disposal history.

Columns
    symbol, date, regime, auction_interval_min, effective_from, effective_to, source, known_at,
    expected_bar_policy
known_at is the announcement date in disposal_information (``date``), i.e. the regime is known
after that day's publication, not at the open of that day. The expected bar count under a
call-auction regime is *not* asserted (policy says "unknown"); A2 reports the observed counts.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

REGIME_COLUMNS = ["symbol", "date", "regime", "auction_interval_min", "effective_from", "effective_to",
                  "source", "known_at", "expected_bar_policy"]


_CN = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


def interval_minutes(v) -> float:
    """5.0 | "5" | "五分鐘" | "二十分鐘" | "四十五分鐘" -> minutes (NaN when unknown)."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    try:
        return float(v)
    except (TypeError, ValueError):
        pass
    s = str(v).replace("分鐘", "").replace("分", "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return float(digits)
    if not s or any(ch not in _CN and ch != "十" for ch in s):
        return np.nan
    if "十" in s:
        a, _, b = s.partition("十")
        return float((_CN.get(a, 1) if a else 1) * 10 + (_CN.get(b, 0) if b else 0))
    return float(_CN[s]) if len(s) == 1 else np.nan


def _sym(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.split(" ").str[0]


def trading_regime_daily(disposal_info: pd.DataFrame | None, trading_days: list) -> pd.DataFrame:
    if disposal_info is None or len(disposal_info) == 0:
        return pd.DataFrame(columns=REGIME_COLUMNS)
    d = disposal_info.copy()
    sym = _sym(d["stock_id"] if "stock_id" in d.columns else d["symbol"])
    start = pd.to_datetime(d.get("處置開始時間"), errors="coerce").dt.normalize()
    end = pd.to_datetime(d.get("處置結束時間"), errors="coerce").dt.normalize()
    interval = d["分時交易"].map(interval_minutes) if "分時交易" in d.columns else pd.Series(np.nan, index=d.index)
    known = pd.to_datetime(d.get("date"), errors="coerce").dt.normalize()
    ev = pd.DataFrame({"symbol": sym, "effective_from": start, "effective_to": end,
                       "auction_interval_min": interval, "known_at": known}).dropna(subset=["effective_from", "effective_to"])
    td = pd.DatetimeIndex(pd.to_datetime(sorted(trading_days)))
    rows = []
    for e in ev.itertuples(index=False):
        lo = td.searchsorted(e.effective_from, side="left")
        hi = td.searchsorted(e.effective_to, side="right")
        if hi <= lo:
            continue
        days = td[lo:hi]
        n = None if pd.isna(e.auction_interval_min) else int(e.auction_interval_min)
        rows.append(pd.DataFrame({
            "symbol": e.symbol, "date": days,
            "regime": f"disposal_call_auction_{n}min" if n else "disposal_interval_unknown",
            "auction_interval_min": n if n else np.nan, "effective_from": e.effective_from,
            "effective_to": e.effective_to, "source": "finlab:disposal_information", "known_at": e.known_at,
            "expected_bar_policy": "unknown_call_auction_count" if n else "unknown",
        }))
    if not rows:
        return pd.DataFrame(columns=REGIME_COLUMNS)
    out = pd.concat(rows, ignore_index=True)
    # overlapping events: keep the longest interval (most restrictive) per symbol/date
    out = out.sort_values(["symbol", "date", "auction_interval_min"], ascending=[True, True, False])
    return out.drop_duplicates(["symbol", "date"]).reset_index(drop=True)[REGIME_COLUMNS]


def matrix_agreement(regime: pd.DataFrame, matrix: pd.DataFrame | None, name: str) -> dict:
    """Compare event-derived disposal days with a FinLab daily boolean matrix (wide or long)."""
    if matrix is None or len(matrix) == 0:
        return {"matrix": name, "available": False}
    m = matrix.copy()
    if "date" not in m.columns:
        first = m.columns[0]
        m = m.rename(columns={first: "date"})
    m["date"] = pd.to_datetime(m["date"], errors="coerce").dt.normalize()
    long = m.melt(id_vars=["date"], var_name="symbol", value_name="flag")
    long = long[long["flag"].astype("boolean").fillna(False)]
    a = set(zip(regime["symbol"].astype(str), pd.to_datetime(regime["date"])))
    b = set(zip(long["symbol"].astype(str), long["date"]))
    lo = min((d for _, d in b), default=None)
    if lo is not None:
        a = {x for x in a if x[1] >= lo}
    return {"matrix": name, "available": True, "event_days": len(a), "matrix_true_days": len(b),
            "both": len(a & b), "event_only": len(a - b), "matrix_only": len(b - a),
            "note": "disagreement is reported, not resolved; matrix semantics (announcement vs effective day) unverified"}
