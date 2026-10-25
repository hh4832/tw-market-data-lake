"""Coverage bookkeeping for Fubon 5m bars: coverage_by_window (source of truth) and
coverage_by_symbol (derived summary), plus the fetch planner and 404/empty classification.

Window = (symbol, calendar block of `window_months` months, clipped to minute_start).
Blocks never cross a calendar year, so one window always maps to one year partition.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from .calendar import TradingCalendar, as_date

# ---- statuses ---------------------------------------------------------------
PENDING = "pending"
PARTIAL = "partial"            # fetched, but not (yet) acceptable: QA gaps/hard failure, write problem
FAILED = "failed"              # request failed / unknown 404 / file missing
CONFLICT = "conflict"          # refetch disagrees with published data; held for human review
COMPLETE = "complete"
COMPLETE_WITH_GAPS = "complete_with_gaps"   # identical source gaps after gap_retry_limit refetches
NO_DATA_MARKET_CLOSED = "no_data_market_closed"
NO_DATA_NOT_LISTED = "no_data_not_yet_listed"
NO_DATA_DELISTED = "no_data_delisted"
NO_DATA_NO_TRADE = "no_data_no_trade"

REPAIR_STATUSES = (FAILED, PARTIAL, PENDING)
DATA_DONE = (COMPLETE, COMPLETE_WITH_GAPS)
NO_DATA_DONE = (NO_DATA_MARKET_CLOSED, NO_DATA_NOT_LISTED, NO_DATA_DELISTED, NO_DATA_NO_TRADE)
DONE_STATUSES = DATA_DONE + NO_DATA_DONE

WINDOW_COLUMNS = [
    "symbol", "window_id", "year", "window_start", "window_end", "covered_to", "status",
    "error_class", "error_msg", "attempts", "gap_days", "gap_attempts", "n_bars",
    "last_attempt_run_id", "last_success_run_id", "updated_at",
]


# status = download/coverage status. data_quality_status is a separate, derived label (B4):
# two identical API answers confirm a *source* gap, they do not make the data complete, and
# intraday completeness is never asserted here (see the read-only audit / audit_research_ready).
QUALITY_BY_STATUS = {
    COMPLETE: "no_missing_days_intraday_unverified",
    COMPLETE_WITH_GAPS: "source_gap_confirmed_by_repeat",
    PARTIAL: "gaps_pending_retry",
    FAILED: "not_usable",
    CONFLICT: "not_usable_conflict",
    PENDING: "not_fetched",
    NO_DATA_MARKET_CLOSED: "no_data_expected",
    NO_DATA_NOT_LISTED: "no_data_expected",
    NO_DATA_DELISTED: "no_data_expected",
    NO_DATA_NO_TRADE: "no_data_expected",
}


def with_quality_status(windows: pd.DataFrame) -> pd.DataFrame:
    out = windows.copy()
    out["data_quality_status"] = out["status"].map(QUALITY_BY_STATUS).fillna("unknown")
    out["research_ready"] = "see_audit"          # decided by tw_lake.audit, never by ingestion
    return out


def empty_windows() -> pd.DataFrame:
    df = pd.DataFrame({c: pd.Series(dtype="object") for c in WINDOW_COLUMNS})
    for c in ("window_start", "window_end", "covered_to"):
        df[c] = pd.Series(dtype="datetime64[ns]")
    for c in ("year", "attempts", "gap_attempts", "n_bars"):
        df[c] = pd.Series(dtype="int64")
    return df


def normalize_windows(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return empty_windows()
    out = df.copy()
    for c in WINDOW_COLUMNS:
        if c not in out.columns:
            out[c] = None
    for c in ("window_start", "window_end", "covered_to"):
        out[c] = pd.to_datetime(out[c])
    for c in ("year", "attempts", "gap_attempts", "n_bars"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")
    for c in ("error_class", "error_msg", "gap_days", "last_attempt_run_id", "last_success_run_id", "updated_at"):
        out[c] = out[c].astype(object).where(out[c].notna(), None)
    out["symbol"] = out["symbol"].astype(str)
    return out[WINDOW_COLUMNS].sort_values(["symbol", "window_start"]).reset_index(drop=True)


# ---- windows -------------------------------------------------------------------
def window_blocks(start: date, end: date, months: int) -> list[tuple[str, date, date]]:
    """Calendar blocks [start..end] of `months` months, clipped to start/end. ID = 'YYYY-MM'."""
    if end < start:
        return []
    out = []
    y, m = start.year, ((start.month - 1) // months) * months + 1
    while True:
        bs = date(y, m, 1)
        ny, nm = (y + (m - 1 + months) // 12, (m - 1 + months) % 12 + 1)
        be = (pd.Timestamp(date(ny, nm, 1)) - pd.Timedelta(days=1)).date()
        if bs > end:
            break
        out.append((f"{y:04d}-{m:02d}", max(bs, start), be))
        y, m = ny, nm
    return out


def ensure_windows(windows: pd.DataFrame, symbols: list[str], minute_start: date,
                   latest_available: date, months: int) -> tuple[pd.DataFrame, set]:
    """Add PENDING windows for every (symbol, block) up to latest_available. Returns (df, new_keys)."""
    w = normalize_windows(windows)
    have = set(zip(w["symbol"], w["window_id"]))
    blocks = window_blocks(minute_start, latest_available, months)
    rows, new_keys = [], set()
    for sym in symbols:
        for wid, ws, we in blocks:
            if (sym, wid) in have:
                continue
            new_keys.add((sym, wid))
            rows.append({"symbol": sym, "window_id": wid, "year": ws.year, "window_start": pd.Timestamp(ws),
                         "window_end": pd.Timestamp(we), "covered_to": pd.NaT, "status": PENDING,
                         "error_class": None, "error_msg": None, "attempts": 0, "gap_days": None,
                         "gap_attempts": 0, "n_bars": 0, "last_attempt_run_id": None,
                         "last_success_run_id": None, "updated_at": None})
    if rows:
        w = normalize_windows(pd.concat([w, pd.DataFrame(rows)], ignore_index=True))
    return w, new_keys


# ---- planning ------------------------------------------------------------------
@dataclass(frozen=True)
class FetchTask:
    symbol: str
    window_id: str
    year: int
    fetch_from: date
    fetch_to: date
    reason: str            # repair | extend | new
    prior_status: str


def is_exhausted(row: pd.Series, max_window_attempts: int) -> bool:
    return row["status"] == FAILED and row["error_class"] in ("unknown_404", "unknown_empty") \
        and int(row["attempts"]) >= max_window_attempts


def plan_tasks(windows: pd.DataFrame, new_keys: set, latest_available: date, max_window_attempts: int,
               retry_conflicts: bool = False, retry_exhausted: bool = False,
               symbols: list[str] | None = None) -> tuple[list[FetchTask], dict]:
    """Repair first (failed, partial, pre-existing pending, optionally conflicts) for ALL symbols,
    then extend completed windows and fetch brand-new windows."""
    w = normalize_windows(windows)
    if symbols is not None:
        w = w[w["symbol"].isin(set(symbols))]
    repair, later = [], []
    stats = {"repair": 0, "extend": 0, "new": 0, "skipped_exhausted": 0, "held_conflict": 0, "up_to_date": 0}
    for _, r in w.iterrows():
        ws, we = r["window_start"].date(), r["window_end"].date()
        if ws > latest_available:
            continue
        target_end = min(we, latest_available)
        key = (r["symbol"], r["window_id"])
        st = r["status"]
        mk = lambda a, b, reason: FetchTask(r["symbol"], r["window_id"], int(r["year"]), a, b, reason, st)
        if st == CONFLICT:
            if retry_conflicts:
                repair.append(mk(ws, target_end, "repair"))
                stats["repair"] += 1
            else:
                stats["held_conflict"] += 1
            continue
        if st == PENDING and key in new_keys:
            later.append(mk(ws, target_end, "new"))
            stats["new"] += 1
            continue
        if st in REPAIR_STATUSES:
            if is_exhausted(r, max_window_attempts) and not retry_exhausted:
                stats["skipped_exhausted"] += 1
                continue
            repair.append(mk(ws, target_end, "repair"))
            stats["repair"] += 1
            continue
        cov = as_date(r["covered_to"])
        if cov is None or cov < target_end:
            if st in DATA_DONE and cov is not None:
                later.append(mk(cov, target_end, "extend"))     # inclusive overlap day
            else:
                later.append(mk(ws, target_end, "extend"))      # no stored data in this window yet
            stats["extend"] += 1
        else:
            stats["up_to_date"] += 1
    repair.sort(key=lambda t: (t.symbol, t.fetch_from))
    later.sort(key=lambda t: (t.symbol, t.fetch_from))
    return repair + later, stats


# ---- no-data / 404 classification --------------------------------------------------
def classify_no_data(cal: TradingCalendar, symbol: str, start: date, end: date,
                     allow_no_trade: bool = True) -> str | None:
    """Explain an empty/404 response using FinLab facts. None = unexplained (must not be complete)."""
    if not cal.trading_days_between(start, end):
        return NO_DATA_MARKET_CLOSED
    f = cal.facts(symbol)
    if f.first_price_date is None or f.first_price_date > end:
        return NO_DATA_NOT_LISTED
    if f.last_price_date is not None and f.last_price_date < start:
        return NO_DATA_DELISTED
    if allow_no_trade and not cal.symbol_trade_days_between(symbol, start, end):
        return NO_DATA_NO_TRADE
    return None


def pre_request_skip(cal: TradingCalendar, symbol: str, start: date, end: date) -> str | None:
    """Cases certain enough to skip the API call (no trade days are still requested and verified)."""
    return classify_no_data(cal, symbol, start, end, allow_no_trade=False)


# ---- symbol summary ------------------------------------------------------------------
def coverage_by_symbol(windows: pd.DataFrame, partitions: pd.DataFrame, targets: pd.DataFrame,
                       latest_available: date, max_window_attempts: int) -> pd.DataFrame:
    w = normalize_windows(windows)
    rows = []
    parts = partitions.copy() if len(partitions) else pd.DataFrame(columns=["partition", "n_rows", "min_key", "max_key"])
    if len(parts):
        parts["symbol"] = parts["partition"].str.extract(r"symbol=([^/]+)")[0]
    tmeta = targets.set_index("symbol") if len(targets) else pd.DataFrame()
    for sym, g in w.groupby("symbol"):
        g = g.sort_values("window_start")
        counts = g["status"].value_counts().to_dict()
        last_complete = None
        for _, r in g.iterrows():
            if r["status"] in DONE_STATUSES:
                last_complete = as_date(r["covered_to"]) or as_date(r["window_end"])
            else:
                break
        p = parts[parts["symbol"] == sym] if len(parts) else parts
        if any(s == CONFLICT for s in g["status"]):
            sst = "conflict"
        elif any(g.apply(lambda r: is_exhausted(r, max_window_attempts), axis=1)):
            sst = "failed_exhausted"
        elif any(s in REPAIR_STATUSES for s in g["status"]):
            sst = "needs_repair"
        elif last_complete is not None and last_complete >= latest_available:
            sst = "complete"
        else:
            sst = "needs_update"
        rows.append({
            "symbol": sym,
            "instrument_type": tmeta.loc[sym, "instrument_type"] if sym in tmeta.index else None,
            "market": tmeta.loc[sym, "market"] if sym in tmeta.index else None,
            "status": sst,
            "first_bar_ts": p["min_key"].min() if len(p) else None,
            "last_bar_ts": p["max_key"].max() if len(p) else None,
            "last_complete_trade_date": pd.Timestamp(last_complete) if last_complete else pd.NaT,
            "n_bars": int(p["n_rows"].sum()) if len(p) else 0,
            "n_windows": int(len(g)),
            "status_counts": json.dumps(counts, sort_keys=True),
        })
    out = pd.DataFrame(rows)
    if len(out):
        out["last_complete_trade_date"] = pd.to_datetime(out["last_complete_trade_date"])
    return out
