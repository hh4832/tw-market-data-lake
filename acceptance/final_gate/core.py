"""FINAL GATE - pure functions of the minimal independent acceptance consumer.

ISOLATION: this package lives outside `tw_lake`, is never imported by it, writes nothing into the lake and is not a
strategy or a backtest engine. The probe rules below are fixed, deterministic and deliberately meaningless (no
parameter is fitted, no performance is read back into any decision); they exist only to drive every data path a
real Backtest Agent will use: 5m -> 30m resampling, time availability, PIT selection, corporate-action accounting,
price limits / UNKNOWN / halts / unfillable orders, the trade ledger with INVALIDATED events, costs and slippage.
All timestamps are naive Asia/Taipei.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

# ------------------------------------------------------------------------------------------ 30m buckets
GRID = [pd.Timedelta(hours=9) + pd.Timedelta(minutes=5 * i) for i in range(53)] + [pd.Timedelta(hours=13, minutes=30)]
BUCKETS = ([(f"B{i}", pd.Timedelta(hours=9, minutes=30 * i), pd.Timedelta(hours=9, minutes=30 * (i + 1)))
            for i in range(8)]
           + [("B8", pd.Timedelta(hours=13), pd.Timedelta(hours=13, minutes=25)),
              ("CLOSE_AUCTION", pd.Timedelta(hours=13, minutes=30), pd.Timedelta(hours=13, minutes=30))])
BUCKET_COLUMNS = ["date", "bucket", "bucket_start", "bucket_end", "available_at", "n_expected", "n_real", "open",
                  "high", "low", "close", "volume_lots", "bar_end", "complete"]


def resample_30m(day: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    """One symbol-day of REAL 5m bars (bar_start naive, open/high/low/close/volume_raw) -> 10 buckets per
    docs/RESEARCH_REPO_INTERFACE.md section 2. No forward fill: empty bucket -> NaN prices."""
    d = day.copy()
    off = d["bar_start"] - date
    rows = []
    for name, a, b in BUCKETS:
        if name == "CLOSE_AUCTION":
            exp = [g for g in GRID if g == a]
            x = d[off == a]
            bucket_end = date + a
        else:
            exp = [g for g in GRID if a <= g < b and g < pd.Timedelta(hours=13, minutes=25)]
            x = d[(off >= a) & (off < b) & (off < pd.Timedelta(hours=13, minutes=25))]
            bucket_end = date + exp[-1] + pd.Timedelta(minutes=5)
        x = x.sort_values("bar_start")
        rows.append({"date": date, "bucket": name, "bucket_start": date + a, "bucket_end": bucket_end,
                     "available_at": bucket_end, "n_expected": len(exp), "n_real": len(x),
                     "open": x["open"].iloc[0] if len(x) else np.nan, "high": x["high"].max() if len(x) else np.nan,
                     "low": x["low"].min() if len(x) else np.nan, "close": x["close"].iloc[-1] if len(x) else np.nan,
                     "volume_lots": float(x["volume_raw"].sum()) if len(x) else 0.0,
                     # the 13:30 call auction is an instant: its bar_end is 13:30, not bar_start + 5m
                     "bar_end": (x["bar_start"].max() + (pd.Timedelta(0) if name == "CLOSE_AUCTION"
                                                         else pd.Timedelta(minutes=5))) if len(x) else pd.NaT,
                     "complete": len(x) == len(exp)})
    return pd.DataFrame(rows, columns=BUCKET_COLUMNS)


def unexpected_bars(day: pd.DataFrame, date: pd.Timestamp) -> int:
    off = set(day["bar_start"] - date)
    return len(off - set(GRID))


# ------------------------------------------------------------------------------------------ costs / ticks
COMMISSION, SELL_TAX_STOCK, SLIP_TICKS, LOT = 0.001425, 0.003, 1, 1000
PARTICIPATION_CAP = 0.10


def tick(p: float) -> float:
    for lim, t in ((10, 0.01), (50, 0.05), (100, 0.1), (500, 0.5), (1000, 1.0)):
        if p < lim:
            return t
    return 5.0


def costs(side: str, px: float, qty: int) -> float:
    c = px * qty * COMMISSION
    return c + (px * qty * SELL_TAX_STOCK if side == "SELL" else 0.0)


# ------------------------------------------------------------------------------------------ probe selection
def probe_rank(symbol: str, date: pd.Timestamp, seed: int) -> str:
    """Deterministic pseudo-random rank: selection is independent of prices and outcomes."""
    return hashlib.sha256(f"{seed}|{symbol}|{date:%Y-%m-%d}".encode()).hexdigest()


def select_candidates(panel_prev: pd.DataFrame, mask_prev: pd.DataFrame, date: pd.Timestamp, k: int, seed: int,
                      quarantine: list[dict], volume: bool = False) -> pd.DataFrame:
    """PIT selection for session `date` using only information known before its open: the previous session's
    official membership (B5a panel) and the previous session's quality mask. The same-day mask is never read."""
    col = "can_use_for_volume_backtest" if volume else "can_use_for_price_backtest"
    ok_prev = set(mask_prev.loc[mask_prev[col].astype(bool), "symbol"])
    p = panel_prev[(panel_prev["official_listed"]) & (panel_prev["security_type"] == "STOCK")
                   & (~panel_prev["delisted_official"]) & panel_prev["symbol"].isin(ok_prev)]
    syms = [s for s in p["symbol"] if not any(q["symbol"] == s and pd.Timestamp(q["start"]) <= date
                                              <= pd.Timestamp(q["end"]) for q in quarantine)]
    out = pd.DataFrame({"symbol": syms, "date": date})
    out["rank"] = [probe_rank(s, date, seed) for s in out["symbol"]]
    return out.sort_values("rank").head(k).reset_index(drop=True)


# ------------------------------------------------------------------------------------------ execution
@dataclass
class Fill:
    status: str            # FILLED / NO_FILL_* / INVALIDATED_*
    time: pd.Timestamp | None = None
    px: float = np.nan
    bar_low: float = np.nan
    bar_high: float = np.nan
    note: str = ""


def execute(bars: pd.DataFrame, side: str, not_before: pd.Timestamp, rules: dict | None, latest: pd.Timedelta,
            qty: int = LOT) -> Fill:
    """Market order at the open of the first REAL 5m bar with bar_start >= not_before and offset <= latest
    (13:30 closing auction is never targeted: fail-closed). Price limits from the B4a dataset; UNKNOWN fails
    closed; a bar opening at the limit in the order's direction does not fill; participation cap 10%; slippage
    one tick against the order, clipped into the bar's range."""
    if rules is None:
        return Fill("INVALIDATED_LIMIT_UNKNOWN", note="no trading-rules row")
    elig = rules["buy_eligibility" if side == "BUY" else "sell_eligibility"]
    if elig == "UNKNOWN":
        return Fill("INVALIDATED_LIMIT_UNKNOWN", note=rules.get("price_limit_status", ""))
    if elig == "BLOCKED_NO_TRADE":
        return Fill("NO_FILL_NO_TRADE")
    day = not_before.normalize()
    cand = bars[(bars["bar_start"] >= not_before) & (bars["bar_start"] - day <= latest)].sort_values("bar_start")
    if cand.empty:
        return Fill("NO_FILL_NO_EXECUTABLE_BAR")
    b = cand.iloc[0]
    up, dn = rules.get("limit_up", np.nan), rules.get("limit_down", np.nan)
    if side == "BUY" and pd.notna(up) and b["open"] >= up - 1e-9:
        return Fill("NO_FILL_AT_LIMIT_UP", time=b["bar_start"])
    if side == "SELL" and pd.notna(dn) and b["open"] <= dn + 1e-9:
        return Fill("NO_FILL_AT_LIMIT_DOWN", time=b["bar_start"])
    if b["volume_raw"] * LOT * PARTICIPATION_CAP < qty:
        return Fill("NO_FILL_LIQUIDITY", time=b["bar_start"])
    t = tick(b["open"]) * SLIP_TICKS
    px = b["open"] + t if side == "BUY" else b["open"] - t
    px = float(min(max(px, b["low"]), b["high"]))
    if pd.notna(up):
        px = min(px, up)
    if pd.notna(dn):
        px = max(px, dn)
    return Fill("FILLED", b["bar_start"], px, float(b["low"]), float(b["high"]))


# ------------------------------------------------------------------------------------------ corporate actions
def holding_allowed(ca: pd.DataFrame, symbol: str, start: pd.Timestamp, end: pd.Timestamp,
                    quarantine: list[dict]) -> tuple[bool, str]:
    ev = ca[(ca["symbol"] == symbol) & (ca["effective_market_session"] > start)
            & (ca["effective_market_session"] <= end)]
    bad = ev[ev["accounting_status"] != "SUPPORTED"]
    if len(bad):
        return False, "INVALIDATED_CORPORATE_ACTION"
    for q in quarantine:
        if q["symbol"] == symbol and start <= pd.Timestamp(q["end"]) and end >= pd.Timestamp(q["start"]):
            return False, "INVALIDATED_RESEARCH_QUARANTINE"
    return True, ""


def apply_events(ca: pd.DataFrame, symbol: str, start: pd.Timestamp, end: pd.Timestamp, shares: float
                 ) -> tuple[float, float, float, int]:
    """SUPPORTED events with effective session in (start, end]: shares x (1+stock), cash += shares x cash.
    Returns shares, cash, cumulative price factor, number of events."""
    ev = ca[(ca["symbol"] == symbol) & (ca["effective_market_session"] > start)
            & (ca["effective_market_session"] <= end)].sort_values("effective_market_session")
    cash, factor = 0.0, 1.0
    for r in ev.itertuples(index=False):
        c = r.cash_per_share if pd.notna(r.cash_per_share) else 0.0
        s = r.stock_per_share if pd.notna(r.stock_per_share) else 0.0
        cash += shares * c
        shares *= 1 + s
        factor *= r.price_factor
    return shares, cash, factor, len(ev)


def factors_known_at(ca: pd.DataFrame, symbol: str, as_of: pd.Timestamp) -> pd.DataFrame:
    a = pd.to_datetime(ca["available_at"], utc=True).dt.tz_convert("Asia/Taipei").dt.tz_localize(None)
    return ca[(ca["symbol"] == symbol) & (a <= as_of)]


# ------------------------------------------------------------------------------------------ scope
def multi_day_retention(rules: pd.DataFrame, ca: pd.DataFrame, horizon: int, quarantine: list[dict]) -> pd.DataFrame:
    """For every stock symbol-day as entry session: can a horizon-session hold be accounted formally?
    Requires usable limit status on entry and exit sessions, an exit session inside the data, and every
    corporate action with effective session in (entry, exit] SUPPORTED; quarantine excluded."""
    r = rules[rules["instrument_class"] == "STOCK"][["symbol", "date", "price_limit_status", "liquidity_bucket"]] \
        .sort_values(["symbol", "date"]).reset_index(drop=True)
    usable = ~r["price_limit_status"].isin(["UNKNOWN_INSTRUMENT_CLASS", "UNKNOWN_NO_REFERENCE", "DERIVED_CONFLICT"])
    g = r.groupby("symbol")
    r["exit_date"] = g["date"].shift(-horizon)
    r["exit_usable"] = usable.groupby(r["symbol"]).shift(-horizon).astype("boolean").fillna(False).astype(bool)
    bad = ca[ca["accounting_status"] != "SUPPORTED"].dropna(subset=["effective_market_session"])
    badmap = bad.groupby("symbol")["effective_market_session"].apply(lambda s: np.sort(s.values)).to_dict()
    hit = np.zeros(len(r), dtype=bool)
    for s, idx in r.groupby("symbol").groups.items():
        arr = badmap.get(s)
        if arr is None:
            continue
        e = r.loc[idx, "date"].values
        x = r.loc[idx, "exit_date"].values
        lo = np.searchsorted(arr, e, side="right")
        hi = np.searchsorted(arr, np.where(pd.isna(x), e, x), side="right")
        hit[np.asarray(idx)] = hi > lo
    quar = np.zeros(len(r), dtype=bool)
    for q in quarantine:
        m = (r["symbol"] == q["symbol"]) & (r["date"] <= pd.Timestamp(q["end"])) \
            & (r["exit_date"].fillna(r["date"]) >= pd.Timestamp(q["start"]))
        quar |= m.to_numpy()
    r["reason"] = np.select(
        [r["exit_date"].isna(), ~usable, ~r["exit_usable"], hit, quar],
        ["NO_EXIT_SESSION_IN_DATA", "ENTRY_LIMIT_UNKNOWN", "EXIT_LIMIT_UNKNOWN", "NON_SUPPORTED_CORPORATE_ACTION",
         "RESEARCH_QUARANTINE"], default="USABLE")
    return r


def sha_frame(df: pd.DataFrame) -> str:
    return hashlib.sha256(df.to_csv(index=False, float_format="%.10g").encode()).hexdigest()


def isclose(a: float, b: float, tol: float) -> bool:
    return (a is not None and b is not None and math.isfinite(a) and math.isfinite(b) and abs(a - b) <= tol)
