"""Trading calendar and per-symbol trading facts derived from FinLab raw daily data.

FinLab daily is the reference for "was the market open" and "did this symbol trade".
A missing 5m day is only ever classified against these facts; it is never assumed to be a holiday.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import numpy as np
import pandas as pd

from .clock import Clock, parse_hhmm


@dataclass
class SymbolFacts:
    first_price_date: date | None
    last_price_date: date | None
    trade_days: frozenset  # dates with volume_shares > 0


class TradingCalendar:
    def __init__(self, raw_long: pd.DataFrame):
        """raw_long columns: date (datetime64), symbol, close, volume_shares."""
        df = raw_long[["date", "symbol", "close", "volume_shares"]].copy()
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        traded = df["volume_shares"].fillna(0) > 0
        priced = df["close"].notna()
        self.trading_days: list[date] = sorted({d.date() for d in df.loc[traded, "date"].unique()})
        if not self.trading_days:
            raise ValueError("FinLab raw daily contains no trading days")
        self._td_index = pd.DatetimeIndex(pd.to_datetime(self.trading_days))

        g_price = df[priced].groupby("symbol")["date"]
        first = g_price.min()
        last = g_price.max()
        trades = df[traded].groupby("symbol")["date"].agg(lambda s: frozenset(d.date() for d in s))
        self.symbols: dict[str, SymbolFacts] = {}
        for sym in set(first.index) | set(trades.index):
            self.symbols[str(sym)] = SymbolFacts(
                first_price_date=first[sym].date() if sym in first.index else None,
                last_price_date=last[sym].date() if sym in last.index else None,
                trade_days=trades.get(sym, frozenset()),
            )

    @property
    def latest_market_date(self) -> date:
        return self.trading_days[-1]

    @property
    def first_market_date(self) -> date:
        return self.trading_days[0]

    def trading_days_between(self, start: date, end: date) -> list[date]:
        lo = np.searchsorted(self._td_index.values, np.datetime64(pd.Timestamp(start)), side="left")
        hi = np.searchsorted(self._td_index.values, np.datetime64(pd.Timestamp(end)), side="right")
        return self.trading_days[lo:hi]

    def previous_trading_day(self, d: date) -> date | None:
        i = np.searchsorted(self._td_index.values, np.datetime64(pd.Timestamp(d)), side="left")
        return self.trading_days[i - 1] if i > 0 else None

    def facts(self, symbol: str) -> SymbolFacts:
        return self.symbols.get(symbol, SymbolFacts(None, None, frozenset()))

    def symbol_trade_days_between(self, symbol: str, start: date, end: date) -> list[date]:
        f = self.facts(symbol)
        return sorted(d for d in f.trade_days if start <= d <= end)

    def latest_available_date(self, clock: Clock, ready_hhmm: str = "16:30") -> date:
        """Last trading day whose Fubon historical data is complete at `clock.now()`."""
        now = clock.now()
        latest = self.latest_market_date
        if latest > now.date():
            latest = self.previous_trading_day(now.date()) or latest
        if latest == now.date() and now.time() < parse_hhmm(ready_hhmm):
            prev = self.previous_trading_day(latest)
            if prev is None:
                raise ValueError("no trading day before today in calendar")
            latest = prev
        return latest


def as_date(x) -> date | None:
    if x is None or (isinstance(x, float) and np.isnan(x)) or x is pd.NaT:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    ts = pd.Timestamp(x)
    return None if pd.isna(ts) else ts.date()
