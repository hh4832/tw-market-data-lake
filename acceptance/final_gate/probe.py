"""FINAL GATE - deterministic probe runs over a sample of real symbol-days (no tuning, no performance feedback).

Three fixed probes, always long one lot, chosen by a hash of (seed, symbol, date):
  INTRADAY_PRICE   signal from the 30m bucket B0 (available 09:30); buy at the first real 5m bar >= 09:30;
                   sell at the 13:20 bar. Selection: previous session's price mask; same-day price mask only
                   INVALIDATES afterwards (contract S5).
  INTRADAY_VOLUME  same, selection and invalidation with the Volume Strict mask; the B0 volume is recorded.
  MULTI_DAY        signal from the CLOSE_AUCTION bucket of day t (available 13:30); buy at the first real bar of
                   the next session; sell at the first real bar `horizon` sessions later (retry up to 5 sessions
                   when no executable bar / halted; never the last observed price). Corporate actions in the
                   holding window must all be SUPPORTED and are accounted (shares, cash).
Every attempt is a ledger row; nothing is dropped silently.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

from . import core as C

LEDGER_COLUMNS = ["trade_id", "strategy", "symbol", "signal_date", "signal_available_at", "signal_input_end",
                  "signal_value", "signal_value_truncated", "entry_status", "entry_time", "entry_px", "entry_open",
                  "entry_bar_low", "entry_bar_high", "exit_status", "exit_time", "exit_px", "exit_open",
                  "exit_bar_low", "exit_bar_high", "exit_retries", "shares_entry", "shares_exit", "cash_received",
                  "n_events", "price_factor", "cost", "gross_pnl", "net_pnl", "acc_return_open", "adj_return_open",
                  "status", "same_day_mask_ok"]
LATEST_INTRADAY = pd.Timedelta(hours=13, minutes=20)


@dataclass
class Sources:
    mask: pd.DataFrame                    # symbol, date, can_use_for_price_backtest, can_use_for_volume_backtest
    panel: pd.DataFrame                   # symbol, date, official_listed, security_type, delisted_official, halted_official
    rules: pd.DataFrame                   # B4a trading_rules
    ca: pd.DataFrame                      # B3a corporate_actions_v2
    bars: Callable[[str, int], pd.DataFrame]
    sessions: pd.DatetimeIndex
    quarantine: list = field(default_factory=list)


class Probe:
    def __init__(self, src: Sources, seed: int = 20261031, horizon: int = 5):
        self.s, self.seed, self.h = src, seed, horizon
        self._bars: dict = {}
        self.ridx = src.rules.set_index(["symbol", "date"]).sort_index()
        self.mask_by_date = {d: g for d, g in src.mask.groupby("date")}
        self.panel_by_date = {d: g for d, g in src.panel.groupby("date")}
        self.mask_idx = src.mask.set_index(["symbol", "date"])
        self.halted = set(zip(src.panel.loc[src.panel["halted_official"], "symbol"],
                              src.panel.loc[src.panel["halted_official"], "date"]))
        self.counter = 0

    def day_bars(self, symbol: str, date: pd.Timestamp) -> pd.DataFrame:
        key = (symbol, date.year)
        if key not in self._bars:
            self._bars[key] = self.s.bars(symbol, date.year)
        b = self._bars[key]
        return b[b["bar_start"].dt.normalize() == date]

    def mask_ok(self, symbol, date, col) -> bool:
        try:
            return bool(self.mask_idx.at[(symbol, date), col])
        except KeyError:
            return False

    def rules_for(self, symbol, date):
        try:
            r = self.ridx.loc[(symbol, date)]
            r = (r.iloc[0] if isinstance(r, pd.DataFrame) else r).to_dict()
        except KeyError:
            r = None
        if r is not None and (symbol, date) in self.halted:
            r = dict(r, buy_eligibility="BLOCKED_NO_TRADE", sell_eligibility="BLOCKED_NO_TRADE")
        return r

    # -------------------------------------------------------------------------------------- one trade
    def _row(self, **kw) -> dict:
        self.counter += 1
        row = {c: np.nan for c in LEDGER_COLUMNS}
        row.update(trade_id=self.counter, **kw)
        return row

    def intraday(self, strategy: str, symbol: str, date: pd.Timestamp) -> dict:
        bars = self.day_bars(symbol, date)
        bk = C.resample_30m(bars, date)
        b0 = bk.iloc[0]
        trunc = C.resample_30m(bars[bars["bar_start"] < b0["available_at"]], date).iloc[0]   # look-ahead probe
        sig = b0["volume_lots"] if strategy == "INTRADAY_VOLUME" else b0["close"] - b0["open"]
        sig_t = trunc["volume_lots"] if strategy == "INTRADAY_VOLUME" else trunc["close"] - trunc["open"]
        col = "can_use_for_volume_backtest" if strategy == "INTRADAY_VOLUME" else "can_use_for_price_backtest"
        row = self._row(strategy=strategy, symbol=symbol, signal_date=date, signal_available_at=b0["available_at"],
                        signal_input_end=b0["bar_end"], signal_value=sig, signal_value_truncated=sig_t,
                        same_day_mask_ok=self.mask_ok(symbol, date, col), exit_retries=0)
        rules = self.rules_for(symbol, date)
        e = C.execute(bars, "BUY", b0["available_at"], rules, LATEST_INTRADAY)
        row.update(entry_status=e.status, entry_time=e.time, entry_px=e.px, entry_bar_low=e.bar_low,
                   entry_bar_high=e.bar_high)
        if e.status != "FILLED":
            row["status"] = e.status
            return row
        x = C.execute(bars, "SELL", date + LATEST_INTRADAY, rules, LATEST_INTRADAY)
        if x.status == "FILLED" and x.time <= e.time:
            x = C.Fill("NO_FILL_NO_EXECUTABLE_BAR")
        row.update(exit_status=x.status, exit_time=x.time, exit_px=x.px, exit_bar_low=x.bar_low,
                   exit_bar_high=x.bar_high, shares_entry=C.LOT, shares_exit=C.LOT, cash_received=0.0, n_events=0,
                   price_factor=1.0)
        if x.status != "FILLED":
            row["status"] = "INVALIDATED_NO_EXIT_SLOT"      # closing auction is fail-closed; no carry
            return row
        self._pnl(row)
        row["status"] = "CLOSED" if row["same_day_mask_ok"] else "INVALIDATED_MASK"
        return row

    def multi_day(self, symbol: str, date: pd.Timestamp) -> dict:
        ses = self.s.sessions
        i = ses.searchsorted(date)
        bars_t = self.day_bars(symbol, date)
        ca_b = C.resample_30m(bars_t, date).iloc[-1]
        row = self._row(strategy="MULTI_DAY", symbol=symbol, signal_date=date,
                        signal_available_at=ca_b["available_at"], signal_input_end=ca_b["bar_end"],
                        signal_value=ca_b["close"], signal_value_truncated=ca_b["close"],
                        same_day_mask_ok=True, exit_retries=0)
        if i + 1 + self.h >= len(ses):
            row.update(entry_status="NOT_ATTEMPTED", status="NO_EXIT_SESSION_IN_DATA")
            return row
        d1 = ses[i + 1]
        b1 = self.day_bars(symbol, d1)
        e = C.execute(b1, "BUY", d1 + pd.Timedelta(hours=9), self.rules_for(symbol, d1), LATEST_INTRADAY)
        row.update(entry_status=e.status, entry_time=e.time, entry_px=e.px, entry_bar_low=e.bar_low,
                   entry_bar_high=e.bar_high, entry_open=b1.loc[b1["bar_start"] == e.time, "open"].min()
                   if e.status == "FILLED" else np.nan)
        if e.status != "FILLED":
            row["status"] = e.status
            return row
        x, dx, tries = None, None, 0
        for k in range(6):
            if i + 1 + self.h + k >= len(ses):
                break
            dx = ses[i + 1 + self.h + k]
            ok, why = C.holding_allowed(self.s.ca, symbol, d1, dx, self.s.quarantine)
            if not ok:
                row.update(exit_status="NOT_ATTEMPTED", status=why, exit_retries=k)
                return row
            bx = self.day_bars(symbol, dx)
            x = C.execute(bx, "SELL", dx + pd.Timedelta(hours=9), self.rules_for(symbol, dx), LATEST_INTRADAY)
            tries = k
            if x.status == "FILLED":
                row["exit_open"] = bx.loc[bx["bar_start"] == x.time, "open"].min()
                break
            if x.status.startswith("INVALIDATED"):
                break
        row["exit_retries"] = tries
        if x is None or x.status != "FILLED":
            row.update(exit_status=x.status if x else "NO_SESSION", status=x.status if x and
                       x.status.startswith("INVALIDATED") else "INVALIDATED_NO_EXIT_PRICE")
            return row
        sh, cash, fac, n = C.apply_events(self.s.ca, symbol, d1, dx, C.LOT)
        row.update(exit_status=x.status, exit_time=x.time, exit_px=x.px, exit_bar_low=x.bar_low,
                   exit_bar_high=x.bar_high, shares_entry=C.LOT, shares_exit=sh, cash_received=cash, n_events=n,
                   price_factor=fac)
        self._pnl(row)
        row["acc_return_open"] = (row["exit_open"] * sh + cash) / (row["entry_open"] * C.LOT) - 1
        row["adj_return_open"] = row["exit_open"] / (row["entry_open"] * fac) - 1
        row["status"] = "CLOSED"
        return row

    @staticmethod
    def _pnl(row: dict) -> None:
        q0, q1 = row["shares_entry"], row["shares_exit"]
        cost = C.costs("BUY", row["entry_px"], q0) + C.costs("SELL", row["exit_px"], q1)
        gross = row["exit_px"] * q1 + row["cash_received"] - row["entry_px"] * q0
        row.update(cost=cost, gross_pnl=gross, net_pnl=gross - cost)

    # -------------------------------------------------------------------------------------- run
    def run(self, dates: list[pd.Timestamp], k: int) -> pd.DataFrame:
        rows = []
        ses = self.s.sessions
        for d in dates:
            i = ses.searchsorted(d)
            if i == 0:
                continue
            prev = ses[i - 1]
            mp = self.mask_by_date.get(prev, self.s.mask.iloc[0:0])
            pp = self.panel_by_date.get(prev, self.s.panel.iloc[0:0])
            for strat, vol in (("INTRADAY_PRICE", False), ("INTRADAY_VOLUME", True), ("MULTI_DAY", False)):
                off = {"INTRADAY_PRICE": 0, "INTRADAY_VOLUME": 1, "MULTI_DAY": 2}[strat]
                cands = C.select_candidates(pp, mp, d, k, self.seed + off, self.s.quarantine, volume=vol)
                for s in cands["symbol"]:
                    rows.append(self.multi_day(s, d) if strat == "MULTI_DAY" else self.intraday(strat, s, d))
        return pd.DataFrame(rows, columns=LEDGER_COLUMNS)


# ------------------------------------------------------------------------------------------ checks
def check_ledger(led: pd.DataFrame) -> dict:
    """No look-ahead, no fake fills, consistent accounting. Each entry: (passed, n_checked, failures sample)."""
    out = {}
    f = led[led["entry_status"] == "FILLED"]
    late_sig = f[f["signal_input_end"].notna() & (f["signal_input_end"] > f["signal_available_at"])]
    out["signal_inputs_before_available_at"] = (late_sig.empty, len(f), late_sig["trade_id"].head(10).tolist())
    early = f[f["entry_time"] < f["signal_available_at"]]
    out["entry_not_before_available_at"] = (early.empty, len(f), early["trade_id"].head(10).tolist())
    intr = led[led["strategy"] != "MULTI_DAY"]
    neq = intr[~((intr["signal_value"] == intr["signal_value_truncated"])
                 | (intr["signal_value"].isna() & intr["signal_value_truncated"].isna()))]
    out["signal_identical_on_truncated_data"] = (neq.empty, len(intr), neq["trade_id"].head(10).tolist())
    filled_px = pd.concat([
        f[["trade_id", "entry_px", "entry_bar_low", "entry_bar_high"]].set_axis(["id", "px", "lo", "hi"], axis=1),
        led[led["exit_status"] == "FILLED"][["trade_id", "exit_px", "exit_bar_low", "exit_bar_high"]]
        .set_axis(["id", "px", "lo", "hi"], axis=1)])
    fake = filled_px[(filled_px["px"] < filled_px["lo"] - 1e-9) | (filled_px["px"] > filled_px["hi"] + 1e-9)]
    out["fill_price_inside_real_bar"] = (fake.empty, len(filled_px), fake["id"].head(10).tolist())
    times = pd.concat([f["entry_time"], led.loc[led["exit_status"] == "FILLED", "exit_time"]]).dropna()
    auction = times[(times - times.dt.normalize()) > LATEST_INTRADAY]
    out["no_fill_in_closing_auction"] = (auction.empty, len(times), auction.astype(str).head(10).tolist())
    md = led[(led["strategy"] == "MULTI_DAY") & (led["status"] == "CLOSED")]
    order = md[md["exit_time"] <= md["entry_time"]]
    out["exit_after_entry"] = (order.empty, len(md), order["trade_id"].head(10).tolist())
    ev = md[md["n_events"] > 0]
    diff = (ev["acc_return_open"] - ev["adj_return_open"]).abs()
    tol = (1 - ev["price_factor"]).abs() * (ev["adj_return_open"].abs() + 0.05) + 0.005
    bad = ev[diff > tol]
    out["corporate_action_accounting_consistent"] = (bad.empty, len(ev), bad["trade_id"].head(10).tolist())
    nan_pnl = led[(led["status"] == "CLOSED") & led["net_pnl"].isna()]
    out["closed_trades_fully_priced"] = (nan_pnl.empty, int((led["status"] == "CLOSED").sum()),
                                         nan_pnl["trade_id"].head(10).tolist())
    return out
