"""FinLab access (daily raw, daily adjusted, security_categories, disposal/attention history)."""
from __future__ import annotations

import logging
import os
from typing import Protocol

import pandas as pd

from .errors import ConfigError

log = logging.getLogger(__name__)


class FinlabSource(Protocol):
    def get(self, name: str) -> pd.DataFrame: ...
    def adjusted(self, price_name: str) -> pd.DataFrame: ...


def plain(df) -> pd.DataFrame | None:
    """Convert FinlabDataFrame (which forbids index assignment) into a plain pandas DataFrame.
    Safe here: we only store daily prices / reference tables and do our own date handling;
    no quarterly/monthly report alignment happens in this repo."""
    if df is None:
        return None
    out = pd.DataFrame(df).copy()
    if type(out) is not pd.DataFrame:  # pragma: no cover - defensive
        out = pd.DataFrame(out.to_dict("series"), index=out.index)
    return out


class RealFinlabSource:
    def __init__(self):
        try:
            import finlab  # type: ignore
            from finlab import data  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise ConfigError("finlab not installed") from exc
        token = os.getenv("FINLAB_API_TOKEN")
        if not token:
            raise ConfigError("missing FINLAB_API_TOKEN")
        finlab.login(token)
        self._data = data

    def get(self, name: str) -> pd.DataFrame:
        return plain(self._data.get(name))

    def adjusted(self, price_name: str) -> pd.DataFrame:
        """Same access path as complete-pullback-fubon-pipeline (TWMarket adj=True)."""
        from finlab.markets.tw import TWMarket  # type: ignore
        mkt = TWMarket()
        if hasattr(mkt, "get_trading_price"):
            return plain(mkt.get_trading_price(price_name, adj=True))
        if hasattr(mkt, "get_price"):
            return plain(mkt.get_price(price_name, adj=True))
        raise ConfigError("TWMarket has neither get_trading_price nor get_price")


def wide_to_long(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """{field: wide(date x symbol)} -> long(date, symbol, field...). Rows with all-NaN values dropped."""
    parts = []
    for fld, w in frames.items():
        w = plain(w)
        w.index = pd.to_datetime(w.index).normalize()
        w.index.name = "date"
        w.columns = w.columns.astype(str)
        w.columns.name = "symbol"
        s = w.stack(dropna=False) if _stack_has_dropna() else w.stack(future_stack=True)
        parts.append(s.rename(fld))
    df = pd.concat(parts, axis=1).reset_index()
    vals = list(frames.keys())
    df = df.dropna(subset=vals, how="all")
    for v in vals:
        df[v] = pd.to_numeric(df[v], errors="coerce").astype("float64")
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    df["symbol"] = df["symbol"].astype(str)
    return df.sort_values(["date", "symbol"]).reset_index(drop=True)


def _stack_has_dropna() -> bool:
    major = int(pd.__version__.split(".")[0])
    return major < 2 or (major == 2 and int(pd.__version__.split(".")[1]) < 1)


def load_raw_daily(src: FinlabSource, fields: dict[str, str]) -> pd.DataFrame:
    frames = {}
    for col, name in fields.items():
        log.info("FinLab get %s", name)
        frames[col] = src.get(name)
    return wide_to_long(frames)


def load_adjusted_daily(src: FinlabSource) -> pd.DataFrame:
    frames = {f"adj_{p}": src.adjusted(p) for p in ("open", "high", "low", "close")}
    return wide_to_long(frames)


def discover_datasets(src: FinlabSource, candidates: list[str]) -> dict[str, dict]:
    """Try each candidate dataset name; report shape/columns/date range or the error."""
    out = {}
    for name in candidates:
        try:
            df = plain(src.get(name))
            info = {"ok": df is not None and len(df) > 0, "shape": list(df.shape) if df is not None else None,
                    "columns": [str(c) for c in list(df.columns)[:30]] if df is not None else None}
            if df is not None and len(df):
                try:
                    idx = pd.to_datetime(df.index)
                    info["index_min"], info["index_max"] = str(idx.min()), str(idx.max())
                except Exception:  # noqa: BLE001 - index may not be dates
                    pass
            out[name] = {**info, "frame": df}
        except Exception as exc:  # noqa: BLE001
            out[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300], "frame": None}
    return out


def to_storable(df: pd.DataFrame) -> pd.DataFrame:
    """Make an arbitrary FinLab frame parquet-safe and hashable: index -> columns,
    categorical / mixed objects -> str (FinLab returns unordered categoricals for event tables)."""
    d = plain(df)
    if not isinstance(d.index, pd.RangeIndex):
        d = d.reset_index()
    d.columns = [str(c) for c in d.columns]
    for c in d.columns:
        col = d[c]
        if isinstance(col.dtype, pd.CategoricalDtype):
            col = col.astype(object)
        if col.dtype == object or pd.api.types.is_string_dtype(col):
            d[c] = col.map(lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)).astype(object)
    return d
