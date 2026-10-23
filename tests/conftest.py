"""SYNTHETIC_SMOKE_ONLY fixtures: fake FinLab and fake Fubon. No network, no credentials.
Passing these tests says nothing about real-data coverage; see smoke/pilot QA reports for that."""
from __future__ import annotations

import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tw_lake.clock import TAIPEI, FixedClock  # noqa: E402
from tw_lake.config import load_config  # noqa: E402

SYNTHETIC_SMOKE_ONLY = True
HOLIDAYS = {date(2024, 1, 1)} | {date(2024, 2, 8) + timedelta(days=i) for i in range(7)} | {date(2023, 10, 10)}


def trading_days(start=date(2023, 4, 3), end=date(2024, 3, 29)) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in HOLIDAYS:
            out.append(d)
        d += timedelta(days=1)
    return out


class GuardedFrame(pd.DataFrame):
    """Mimics FinlabDataFrame: assigning .index raises (finlab >= 2.0.18 behaviour)."""

    @property
    def _constructor(self):
        return GuardedFrame

    def __setattr__(self, name, value):
        if name == "index":
            raise AttributeError("不要直接覆寫 FinlabDataFrame 的 index！")
        super().__setattr__(name, value)


class FakeFinlab:
    """Wide frames like finlab.data.get(): index=date, columns=symbol."""

    def __init__(self, end=date(2024, 3, 29), adj_factor: float = 1.0, change_close: dict | None = None):
        self.days = trading_days(end=end)
        self.adj_factor = adj_factor
        self.change_close = change_close or {}
        self.symbols = {
            "2330": (None, None), "0050": (None, None),
            "1234": (None, date(2023, 9, 29)),          # delisted, absent from security_categories
            "9999": (date(2023, 11, 1), None),          # new listing
            "5555": (None, None),                        # suspended (no volume) 2023-10..12
        }
        self.calls = []

    def _frame(self, field: str) -> pd.DataFrame:
        idx = pd.to_datetime(self.days)
        data = {}
        for k, (first, last) in self.symbols.items():
            base = 100.0 + int(k) % 97
            vals = []
            for i, d in enumerate(self.days):
                listed = (first is None or d >= first) and (last is None or d <= last)
                if not listed:
                    vals.append(np.nan)
                    continue
                close = base + (i % 7) * 0.5
                suspended = k == "5555" and date(2023, 10, 1) <= d <= date(2023, 12, 31)
                v = {"price:開盤價": close - 0.5, "price:最高價": close + 1.0, "price:最低價": close - 1.0,
                     "price:收盤價": close, "price:成交股數": 0.0 if suspended else 1_000_000.0 + i * 1000,
                     "price:成交金額": (0.0 if suspended else 1_000_000.0) * close}[field]
                if field == "price:收盤價" and (k, d) in self.change_close:
                    v = self.change_close[(k, d)]
                vals.append(v)
            data[k] = vals
        return GuardedFrame(data, index=idx)

    def get(self, name: str) -> pd.DataFrame:
        self.calls.append(name)
        if name.startswith("price:"):
            return self._frame(name)
        if name == "security_categories":
            return GuardedFrame({"stock_id": ["2330", "0050", "9999", "5555", "9105", "00679B"],
                                 "symbol": ["2330", "0050", "9999", "5555", "9105", "00679B"],
                                 "category": ["半導體業", "domestic_etf", "電子零組件業", "鋼鐵工業", "存託憑證", "其他證券"],
                                 "market": ["sii", "etf", "otc", "sii", "sii", "other_securities"]})   # real FinLab layout
        if name == "disposal_information":
            # real FinLab: RangeIndex event table with UNORDERED categorical columns
            df = pd.DataFrame({"symbol": ["5555 測試", "5555 測試"], "date": pd.to_datetime(["2023-08-31", "2023-11-01"]),
                               "處置開始時間": ["2023-09-01", "2023-11-02"], "處置結束時間": ["2023-09-14", "2023-11-15"],
                               "分時交易": ["五分鐘", "二十分鐘"], "stock_id": ["5555", "5555"]})
            for c in ("symbol", "分時交易", "stock_id"):
                df[c] = df[c].astype("category")
            return GuardedFrame(df)
        if name == "etl:disposal_stock_filter":
            close = self._frame("price:收盤價")
            return GuardedFrame(pd.DataFrame(False, index=close.index, columns=close.columns))
        raise KeyError(f"unknown dataset {name}")

    def adjusted(self, price_name: str) -> pd.DataFrame:
        m = {"open": "price:開盤價", "high": "price:最高價", "low": "price:最低價", "close": "price:收盤價"}
        return GuardedFrame(pd.DataFrame(self._frame(m[price_name])) * self.adj_factor)


class FakeAPIError(Exception):
    def __init__(self, status_code: int, msg: str = ""):
        super().__init__(msg or f"HTTP {status_code}")
        self.status_code = status_code


class FakeFubon:
    """historical_candles consistent with FakeFinlab daily (high/low/volume reconcile exactly)."""

    def __init__(self, finlab: FakeFinlab):
        self.fin = finlab
        self.close = finlab._frame("price:收盤價")
        self.high = finlab._frame("price:最高價")
        self.low = finlab._frame("price:最低價")
        self.vol = finlab._frame("price:成交股數")
        self.not_found_symbols: set[str] = set()
        self.missing_days: dict[str, set] = {}
        self.transient_failures: dict[tuple, int] = {}
        self.rate_limit_once: set[tuple] = set()
        self.price_shift: dict[tuple, float] = {}     # (symbol, date) -> added to all prices (conflicts)
        self.calls: list[dict] = []

    def historical_candles(self, params: dict) -> dict:
        self.calls.append(dict(params))
        sym = params["symbol"]
        key = (sym, params["from"], params["to"])
        if key in self.rate_limit_once:
            self.rate_limit_once.discard(key)
            raise FakeAPIError(429)
        if self.transient_failures.get(key, 0) > 0:
            self.transient_failures[key] -= 1
            raise FakeAPIError(503)
        if sym in self.not_found_symbols:
            raise FakeAPIError(404)
        a, b = date.fromisoformat(params["from"]), date.fromisoformat(params["to"])
        rows = []
        if sym in self.close.columns:
            for ts in self.close.index:
                d = ts.date()
                if not (a <= d <= b) or d in self.missing_days.get(sym, set()):
                    continue
                v = self.vol.at[ts, sym]
                if not (v > 0):
                    continue
                c, h, lo = self.close.at[ts, sym], self.high.at[ts, sym], self.low.at[ts, sym]
                sh = self.price_shift.get((sym, d), 0.0)
                lots = v / 1000.0
                per = lots / 54
                for j in range(54):
                    t = datetime.combine(d, time(9, 0)) + timedelta(minutes=5 * j)
                    bh = h if j == 0 else c
                    bl = lo if j == 1 else c
                    rows.append({"date": t.strftime("%Y-%m-%dT%H:%M:%S.000+08:00"), "open": c + sh,
                                 "high": max(bh, c) + sh, "low": min(bl, c) + sh, "close": c + sh,
                                 "volume": per, "average": c})
        if not rows:
            raise FakeAPIError(404, "Resource Not Found")
        return {"symbol": sym, "timeframe": params["timeframe"], "data": rows}

    def tickers(self, params):
        self.ticker_calls = getattr(self, "ticker_calls", 0) + 1
        if params.get("isDisposition"):
            return {"date": "2024-03-29", "data": [{"symbol": "5555", "name": "fake"}]}
        if params.get("isNormal") and params.get("market") == "TSE":
            return {"date": "2024-03-29", "data": [{"symbol": "2330", "name": "fake"}]}
        return {"date": "2024-03-29", "data": []}

    def ticker(self, params):  # pragma: no cover
        return {}


@pytest.fixture
def tmp_cfg(tmp_path):
    root = tmp_path / "drive" / "00Quant_Research" / "data"
    root.mkdir(parents=True)
    for p in ("finlab_0050", "fubon_txf", "IKBR", "finlab_txf_calendar", "finlab_futures_afterhours"):
        (root / p).mkdir()
        (root / p / "keep.txt").write_text("existing user data")
    cfg = load_config(overrides={"data_root": str(root), "local_tmp": str(tmp_path / "local_tmp"),
                                 "manifest": {"commit_every_symbols": 2, "lock_stale_minutes": 30},
                                 "fubon": {"rate_limit_wait_sec": 0, "backoff_base_sec": 0, "backoff_max_sec": 0,
                                           "window_months": 3}})   # tests pin quarter windows
    return cfg


@pytest.fixture
def clock():
    return FixedClock(datetime(2024, 3, 29, 17, 0, tzinfo=TAIPEI))


def no_sleep(_s):
    return None
