"""Raw-layer standardization for Fubon 5m candles and merge/conflict detection.

Rows deliberately exclude fetch time / run id so identical source data always produces an
identical content hash (provenance lives in request_log and the partitions table).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .clock import TAIPEI

MINUTE_COLUMNS = [
    "symbol", "bar_start", "bar_ts_raw", "open", "high", "low", "close",
    "volume_raw", "volume_unit", "avg_price_cum", "turnover", "turnover_present",
    "timeframe", "bar_label_assumption", "adjusted", "source",
]
VALUE_COLUMNS = ["bar_ts_raw", "open", "high", "low", "close", "volume_raw", "avg_price_cum", "turnover"]


def empty_minute() -> pd.DataFrame:
    return normalize_candles([], "0000", "lot", "5", "start")


def normalize_candles(rows: list[dict], symbol: str, volume_unit: str, timeframe: str,
                      bar_label: str) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "average", "turnover"])
    if "date" not in df.columns:
        raise ValueError("candle rows have no 'date' field")
    raw_ts = df["date"].astype(str).reset_index(drop=True)
    if len(df):
        ts = pd.to_datetime(raw_ts, utc=True, errors="raise").dt.tz_convert(TAIPEI)
    else:
        ts = pd.Series(pd.DatetimeIndex([], tz=TAIPEI))
    if bar_label == "end":
        ts = ts - pd.Timedelta(minutes=int(timeframe))
    out = pd.DataFrame({
        "symbol": pd.Series([str(symbol)] * len(df), dtype=object),
        "bar_start": ts.reset_index(drop=True),
        "bar_ts_raw": raw_ts.astype(object),
    })
    df = df.reset_index(drop=True)
    for src, dst in (("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"),
                     ("volume", "volume_raw"), ("average", "avg_price_cum"), ("turnover", "turnover")):
        out[dst] = pd.to_numeric(df[src], errors="coerce").astype("float64") if src in df.columns \
            else pd.Series([float("nan")] * len(df), dtype="float64")
    out["volume_unit"] = volume_unit
    out["turnover_present"] = out["turnover"].notna()
    out["timeframe"] = str(timeframe)
    out["bar_label_assumption"] = bar_label
    out["adjusted"] = False
    out["source"] = "fubon"
    out = coerce_minute_schema(out[MINUTE_COLUMNS])
    return out.drop_duplicates(subset=["symbol", "bar_start"], keep="last").sort_values("bar_start").reset_index(drop=True)


_FLOAT = ["open", "high", "low", "close", "volume_raw", "avg_price_cum", "turnover"]
_BOOL = ["turnover_present", "adjusted"]
_STR = ["symbol", "bar_ts_raw", "volume_unit", "timeframe", "bar_label_assumption", "source"]


def coerce_minute_schema(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Force canonical dtypes (parquet round trips and pandas versions may change tz objects/dtypes)."""
    if df is None:
        return None
    out = df.copy()
    out["bar_start"] = pd.to_datetime(out["bar_start"], utc=True).dt.tz_convert(TAIPEI)
    for c in _FLOAT:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    for c in _BOOL:
        out[c] = out[c].astype(bool)
    for c in _STR:
        out[c] = out[c].astype(object).where(out[c].notna(), None)
    return out[MINUTE_COLUMNS]


def _eq(a, b) -> np.ndarray:
    """Positional equality treating NaN == NaN (index labels/tz implementations are ignored)."""
    a = pd.Series(np.asarray(a, dtype=object))
    b = pd.Series(np.asarray(b, dtype=object))
    return ((a == b) | (a.isna() & b.isna())).to_numpy()


def merge_with_conflicts(old: pd.DataFrame | None, new: pd.DataFrame, key: str | list[str] = "bar_start",
                         value_cols: list[str] | None = None) -> tuple[pd.DataFrame, int, pd.DataFrame]:
    """Union old and new. Overlapping keys must match exactly; mismatches are returned as conflicts
    and the OLD value is kept. Returns (merged, n_added, conflicts)."""
    value_cols = value_cols or VALUE_COLUMNS
    if old is None or len(old) == 0:
        return new.copy(), int(len(new)), pd.DataFrame()
    o = old.set_index(key)
    n = new.set_index(key)
    common = o.index.intersection(n.index)
    conflicts = []
    if len(common):
        oc, nc = o.loc[common], n.loc[common]
        cols = [c for c in value_cols if c in oc.columns and c in nc.columns]
        diff = np.zeros(len(common), dtype=bool)
        eqs = {}
        for c in cols:
            eqs[c] = _eq(oc[c].to_numpy(), nc[c].to_numpy())
            diff |= ~eqs[c]
        for pos in np.flatnonzero(diff):
            rec = {"key": str(common[pos])}
            for c in cols:
                if not eqs[c][pos]:
                    rec[f"old_{c}"] = oc[c].iloc[pos]
                    rec[f"new_{c}"] = nc[c].iloc[pos]
            conflicts.append(rec)
    added = n.loc[n.index.difference(o.index)]
    merged = pd.concat([o, added]).reset_index()
    merged = merged.sort_values(key if isinstance(key, list) else [key]).reset_index(drop=True)
    return merged, int(len(added)), pd.DataFrame(conflicts)
