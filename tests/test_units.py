"""Unit tests: lock, planner, 404 classification, retries, universe, QA, FinLab raw immutability.
SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from conftest import FakeAPIError, FakeFinlab, FakeFubon, trading_days
from test_e2e import run
from tw_lake import cli
from tw_lake.calendar import TradingCalendar
from tw_lake.clock import TAIPEI, FixedClock
from tw_lake.errors import ApiNotFound, LockHeldError, LockLostError, RequestBudgetExceeded, RetriesExhausted
from tw_lake.finlab_source import load_raw_daily
from tw_lake.fubon_source import FubonCaller, RateLimiter, RequestBudget, RetryPolicy
from tw_lake.lock import WriterLock
from tw_lake.normalize import normalize_candles
from tw_lake.paths import Layout
from tw_lake.qa import daily_from_minute, reconcile_daily
from tw_lake.store import DatasetStore, StoreContext
from tw_lake.universe import classify, minute_targets, symbol_master_pit, universe_daily
from tw_lake.windows import (FAILED, PARTIAL, PENDING, classify_no_data, ensure_windows, plan_tasks,
                             pre_request_skip, window_blocks)

RAW_FIELDS = {"open": "price:開盤價", "high": "price:最高價", "low": "price:最低價", "close": "price:收盤價",
              "volume_shares": "price:成交股數", "turnover": "price:成交金額"}


@pytest.fixture
def cal():
    return TradingCalendar(load_raw_daily(FakeFinlab(), RAW_FIELDS))


# ---------------------------------------------------------------- lock
def test_second_writer_is_rejected(tmp_path, clock):
    a = WriterLock(tmp_path, "run_a", clock)
    a.acquire()
    b = WriterLock(tmp_path, "run_b", clock)
    with pytest.raises(LockHeldError):
        b.acquire()
    a.release()
    b.acquire()
    b.release()


def test_stale_lock_needs_explicit_break(tmp_path):
    c = FixedClock(datetime(2024, 1, 1, 9, 0, tzinfo=TAIPEI))
    a = WriterLock(tmp_path / "locks", "run_a", c, stale_minutes=30, quarantine_dir=tmp_path / "q")
    a.acquire()
    c.set(datetime(2024, 1, 1, 10, 0, tzinfo=TAIPEI))
    b = WriterLock(tmp_path / "locks", "run_b", c, stale_minutes=30, quarantine_dir=tmp_path / "q")
    with pytest.raises(LockHeldError, match="stale"):
        b.acquire()
    b.acquire(break_stale=True)
    assert list((tmp_path / "q").iterdir())          # old lock preserved, not deleted
    with pytest.raises(LockLostError):
        a.heartbeat()                                 # the dead writer can no longer commit


def test_concurrent_cli_writer_blocked(tmp_cfg, clock):
    lay = Layout.from_config(tmp_cfg)
    other = WriterLock(lay.locks, "other_run", clock)
    other.acquire()
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub) == 1
    assert fub.calls == []                             # nothing fetched
    assert not (tmp_cfg.data_root / "fubon_stock_5m").exists()
    runs = sorted(lay.runs.iterdir())
    assert json.loads((runs[-1] / "run_info.json").read_text())["status"] == "blocked_by_lock"
    other.release()


# ---------------------------------------------------------------- windows / planner
def test_window_blocks_never_cross_year():
    b = window_blocks(date(2023, 5, 23), date(2024, 3, 29), 3)
    assert [x[0] for x in b] == ["2023-04", "2023-07", "2023-10", "2024-01"]
    assert b[0][1] == date(2023, 5, 23) and b[-1][2] == date(2024, 3, 31)
    assert all(s.year == e.year for _, s, e in b)
    assert [x[0] for x in window_blocks(date(2023, 11, 5), date(2024, 1, 3), 1)] == ["2023-11", "2023-12", "2024-01"]


def test_plan_repair_before_extend_and_new():
    w, new = ensure_windows(None, ["A", "B"], date(2023, 5, 23), date(2023, 12, 15), 3)
    w = w.set_index(["symbol", "window_id"])
    w.loc[("A", "2023-04"), ["status", "covered_to"]] = ["complete", pd.Timestamp("2023-06-30")]
    w.loc[("A", "2023-07"), ["status"]] = [FAILED]
    w.loc[("A", "2023-10"), ["status", "covered_to"]] = ["complete", pd.Timestamp("2023-11-30")]
    w.loc[("B", "2023-04"), ["status"]] = [PARTIAL]
    w = w.reset_index()
    tasks, stats = plan_tasks(w, {("B", "2023-07"), ("B", "2023-10")}, date(2023, 12, 15), 5)
    order = [(t.reason, t.symbol, t.window_id) for t in tasks]
    assert order[:2] == [("repair", "A", "2023-07"), ("repair", "B", "2023-04")]
    ext = [t for t in tasks if t.reason == "extend"][0]
    assert (ext.fetch_from, ext.fetch_to) == (date(2023, 11, 30), date(2023, 12, 15))   # inclusive overlap
    assert stats == {"repair": 2, "extend": 1, "new": 2, "skipped_exhausted": 0, "held_conflict": 0, "up_to_date": 1}


def test_pre_existing_pending_is_repaired_first():
    w, _ = ensure_windows(None, ["A"], date(2023, 5, 23), date(2023, 9, 1), 3)
    tasks, stats = plan_tasks(w, set(), date(2023, 9, 1), 5)       # interrupted run left pending windows
    assert all(t.reason == "repair" for t in tasks) and stats["repair"] == 2


def test_exhausted_unknown_404_is_reported_not_retried():
    w, _ = ensure_windows(None, ["A"], date(2023, 5, 23), date(2023, 6, 30), 3)
    w.loc[0, ["status", "error_class", "attempts"]] = [FAILED, "unknown_404", 5]
    tasks, stats = plan_tasks(w, set(), date(2023, 6, 30), 5)
    assert tasks == [] and stats["skipped_exhausted"] == 1
    tasks, _ = plan_tasks(w, set(), date(2023, 6, 30), 5, retry_exhausted=True)
    assert len(tasks) == 1


# ---------------------------------------------------------------- 404 classification
def test_classify_no_data(cal):
    assert classify_no_data(cal, "2330", date(2024, 2, 8), date(2024, 2, 14)) == "no_data_market_closed"
    assert classify_no_data(cal, "9999", date(2023, 7, 1), date(2023, 9, 30)) == "no_data_not_yet_listed"
    assert classify_no_data(cal, "1234", date(2023, 10, 1), date(2023, 12, 31)) == "no_data_delisted"
    assert classify_no_data(cal, "5555", date(2023, 10, 1), date(2023, 12, 31)) == "no_data_no_trade"
    assert classify_no_data(cal, "2330", date(2023, 7, 1), date(2023, 9, 30)) is None        # unexplained
    assert classify_no_data(cal, "NEVER", date(2023, 7, 1), date(2023, 9, 30)) == "no_data_not_yet_listed"
    # no-trade is NOT a pre-request skip: it is requested and verified
    assert pre_request_skip(cal, "5555", date(2023, 10, 1), date(2023, 12, 31)) is None
    # listing inside the window is not "not yet listed"
    assert classify_no_data(cal, "9999", date(2023, 10, 1), date(2023, 12, 31)) is None


def test_latest_available_respects_1630(cal):
    assert cal.latest_available_date(FixedClock(datetime(2024, 3, 29, 15, 0, tzinfo=TAIPEI))) == date(2024, 3, 28)
    assert cal.latest_available_date(FixedClock(datetime(2024, 3, 29, 16, 31, tzinfo=TAIPEI))) == date(2024, 3, 29)
    assert cal.latest_available_date(FixedClock(datetime(2024, 3, 30, 9, 0, tzinfo=TAIPEI))) == date(2024, 3, 29)


# ---------------------------------------------------------------- retries / budget
def _caller(budget=None):
    return FubonCaller(RateLimiter(1000, sleep=lambda s: None), RetryPolicy(3, 0, 0, 0),
                       RequestBudget(budget), sleep=lambda s: None)


def test_retry_policy():
    c = _caller()
    n = {"k": 0}

    def flaky(p):
        n["k"] += 1
        if n["k"] == 1:
            raise FakeAPIError(429)
        if n["k"] == 2:
            raise FakeAPIError(503)
        return {"data": [1]}
    assert c.call(flaky, {}) == {"data": [1]}
    assert [r.status for r in c.records] == ["ApiRateLimited", "ApiTransient", "ok"]
    with pytest.raises(ApiNotFound):
        _caller().call(lambda p: (_ for _ in ()).throw(FakeAPIError(404)), {})
    with pytest.raises(RetriesExhausted):
        _caller().call(lambda p: (_ for _ in ()).throw(FakeAPIError(500)), {})
    c2 = _caller(budget=1)
    c2.call(lambda p: {}, {})
    with pytest.raises(RequestBudgetExceeded):
        c2.call(lambda p: {}, {})


def test_rate_limiter_waits():
    t = {"now": 0.0}
    slept = []
    rl = RateLimiter(2, clock=lambda: t["now"], sleep=lambda s: (slept.append(s), t.__setitem__("now", t["now"] + s)))
    for _ in range(3):
        rl.acquire()
    assert slept and slept[0] >= 59.9


def test_budget_stop_leaves_rest_pending(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub, extra=("--max-requests", "3")) == 2
    from test_e2e import windows
    w = windows(tmp_cfg, clock)
    assert (w["status"] == PENDING).sum() > 0
    assert len(fub.calls) == 3


# ---------------------------------------------------------------- normalize
def test_turnover_bar_ts_raw_and_units():
    rows = [{"date": "2024-01-02T09:05:00.000+08:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 3}]
    df = normalize_candles(rows, "2330", "lot", "5", "end")
    assert df.loc[0, "bar_ts_raw"] == "2024-01-02T09:05:00.000+08:00"
    assert df.loc[0, "bar_start"].strftime("%H:%M") == "09:00"       # end-label shifted to start
    assert pd.isna(df.loc[0, "turnover"]) and not df.loc[0, "turnover_present"]
    assert df.loc[0, "volume_unit"] == "lot"
    df2 = normalize_candles([{**rows[0], "turnover": 12345}], "2330", "lot", "5", "start")
    assert df2.loc[0, "turnover"] == 12345 and df2.loc[0, "turnover_present"]


# ---------------------------------------------------------------- universe
def test_point_in_time_universe(cal):
    fin = FakeFinlab()
    raw = load_raw_daily(fin, RAW_FIELDS)
    cl = classify(fin.get("security_categories"), sorted(raw["symbol"].unique()), "2024-03-29")
    c = cl.set_index("symbol")
    assert c.loc["0050", "instrument_type"] == "etf"          # FinLab market == "etf"
    assert c.loc["0050", "market"] == "etf"
    assert c.loc["00679B", "instrument_type"] == "etf"       # bond ETF outside market "etf"
    assert c.loc["9105", "instrument_type"] == "other"            # depositary receipt
    assert c.loc["1234", "classification_source"] == "code_rule_inferred"
    assert not c["classification_is_pit"].any()
    m = symbol_master_pit(raw, cl, cal.trading_days, 20).set_index("symbol")
    assert m.loc["1234", "listing_status_latest"] == "delisted_inferred"
    assert m.loc["9999", "first_price_date"] == pd.Timestamp("2023-11-01")
    u = universe_daily(raw, m.reset_index(), cal.trading_days)
    u55 = u[(u["symbol"] == "5555") & (u["date"] == pd.Timestamp("2023-11-01"))].iloc[0]
    assert u55["has_price"] and not u55["has_trade"] and u55["tradable_status"] == "listed_no_trade"
    assert u[u["symbol"] == "9999"]["date"].min() == pd.Timestamp("2023-11-01")   # no rows before listing
    assert u[u["symbol"] == "1234"]["date"].max() == pd.Timestamp("2023-09-29")   # none after delisting
    t = minute_targets(m.reset_index(), date(2023, 5, 23))
    assert set(t["symbol"]) == {"2330", "0050", "1234", "9999", "5555"}


# ---------------------------------------------------------------- QA reconciliation
def test_reconciliation_separates_pending_from_errors(cal):
    fin = FakeFinlab()
    raw = load_raw_daily(fin, RAW_FIELDS)
    fub = FakeFubon(fin)
    rows = fub.historical_candles({"symbol": "2330", "from": "2023-08-01", "to": "2023-08-31", "timeframe": "5"})["data"]
    m = normalize_candles(rows, "2330", "lot", "5", "start")
    m = m[m["bar_start"].dt.date != date(2023, 8, 15)]                       # a missing day
    d16 = m["bar_start"].dt.date == date(2023, 8, 16)
    m.loc[d16, "volume_raw"] = m.loc[d16, "volume_raw"] * 0.9                 # FinLab includes after-hours
    d17 = m["bar_start"].dt.date == date(2023, 8, 17)
    m.loc[d17 & (m["bar_start"].dt.minute == 0) & (m["bar_start"].dt.hour == 9), "high"] += 50   # impossible
    d21 = (m["bar_start"].dt.date == date(2023, 8, 21)) & (m["bar_start"].dt.strftime("%H:%M") == "13:30")
    m.loc[d21, ["close", "low"]] = m.loc[d21, "close"] - 0.5                 # closing auction != official close
    rec = reconcile_daily(daily_from_minute(m), raw, cal, ["2330"], date(2023, 8, 1), date(2023, 8, 31))
    by = rec.set_index("date")["classification"]
    det = rec.set_index("date")["detail"]
    assert by[pd.Timestamp("2023-08-15")] == "missing_in_source"
    assert by[pd.Timestamp("2023-08-16")] == "pending_verification"           # not auto-explained
    assert "minute_volume_below_daily" in det[pd.Timestamp("2023-08-16")]
    assert by[pd.Timestamp("2023-08-17")] == "data_error"
    assert by[pd.Timestamp("2023-08-21")] == "suspect" and "close_mismatch" in det[pd.Timestamp("2023-08-21")]
    assert by[pd.Timestamp("2023-08-18")] == "ok"
    assert "vendor_definition_diff" not in set(rec["classification"])


def test_merge_ranges():
    from tw_lake.jobs_fubon import merge_ranges
    r = merge_ranges([(date(2024, 1, 1), date(2024, 6, 30)), (date(2023, 7, 1), date(2023, 12, 31)),
                      (date(2025, 1, 1), date(2025, 6, 30))])
    assert r == [(date(2023, 7, 1), date(2024, 6, 30)), (date(2025, 1, 1), date(2025, 6, 30))]


# ---------------------------------------------------------------- FinLab raw immutability
def test_finlab_raw_history_change_is_logged_not_overwritten(tmp_cfg, clock):
    fin = FakeFinlab()
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    fin2 = FakeFinlab(change_close={("2330", date(2023, 6, 1)): 999.0})
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, FakeFubon(fin2), mode="update") == 0
    ctx = StoreContext(Layout.from_config(tmp_cfg), "x", tmp_cfg.local_tmp, clock)
    s = DatasetStore("finlab_daily_raw", ctx, ["date", "symbol"], "1")
    p = s.read_partition("year=2023")
    v = p[(p["symbol"] == "2330") & (p["date"] == pd.Timestamp("2023-06-01"))]["close"].iloc[0]
    assert v != 999.0
    ch = s.tables()["changes_log"]
    assert len(ch) == 1 and ch.iloc[0]["source_value"] == 999.0


# ---------------------------------------------------------------- modes
def test_dry_run_writes_no_dataset(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub, mode="full", extra=("--dry-run",)) == 0
    assert fub.calls == []
    for d in ("fubon_stock_5m", "finlab_stock_daily", "finlab_stock_reference"):
        assert not (tmp_cfg.data_root / d).exists()
    runs = list((tmp_cfg.data_root / "_tw-market-data-lake" / "runs").iterdir())
    info = json.loads((runs[0] / "run_info.json").read_text())
    assert info["fubon_5m"]["estimated_requests"] > 0


def test_full_requires_confirmation_and_update_requires_manifest(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub, mode="full") == 1
    clock.set(datetime(2024, 3, 29, 17, 1, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, fub, mode="update") == 1
    assert fub.calls == []
    clock.set(datetime(2024, 3, 29, 17, 2, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, fub, mode="full", extra=("--confirm-full",)) == 0


def test_missing_disposal_dataset_fails_closed(tmp_cfg, clock):
    class NoDisposal(FakeFinlab):
        def get(self, name):
            if name in ("disposal_information", "etl:disposal_stock_filter", "etl:noticed_stock_filter",
                        "notice_information"):
                raise KeyError(name)
            return super().get(name)
    fin = NoDisposal()
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub) == 1
    assert fub.calls == []


def test_fubon_current_flags_snapshot_accumulates(tmp_cfg, clock):
    fin = FakeFinlab()
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    ctx = StoreContext(Layout.from_config(tmp_cfg), "x", tmp_cfg.local_tmp, clock)
    s = DatasetStore("fubon_symbol_master", ctx, ["market", "flag", "symbol"], "1")
    assert list(s.partitions()["partition"]) == ["asof=2024-03-29"]
    df = s.read_all()
    assert set(df["flag"]) == {"isNormal", "isDisposition"}
    assert "not point-in-time" in s.meta()["note"]


def test_smoke_writes_only_to_smoke_folder(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub, mode="smoke") == 0
    created = sorted(p.name for p in tmp_cfg.data_root.iterdir())
    assert "fubon_stock_5m" not in created and "finlab_stock_daily" not in created
    rep_path = next((tmp_cfg.data_root / "_tw-market-data-lake" / "smoke").rglob("capability_report.json"))
    rep = json.loads(rep_path.read_text())
    prof = rep["fubon"]["2330_360d"]["profile"]
    assert prof["bars_per_day_distribution"] == {"54": prof["days_with_bars"]}
    assert prof["first_bar_time_counts"] == {"09:00": prof["days_with_bars"]}
    assert abs(prof["volume_x1000_vs_finlab_shares_ratio"]["median"] - 1.0) < 1e-9
    assert rep["recommendations"]["bar_label"] == "start"
    assert rep["recommendations"]["row_cap_suspected"] is False
    assert rep["fubon"]["holiday_window_2024-02-08_14"]["status_code"] == 404
    assert rep["finlab"]["disposal_attention_candidates"]["disposal_information"]["ok"] is True


def test_default_config_half_year_windows():
    from tw_lake.config import load_config
    cfg = load_config()
    assert cfg.fubon["window_months"] == 6
    b = window_blocks(date(2023, 5, 23), date(2026, 10, 22), 6)
    assert [x[0] for x in b] == ["2023-01", "2023-07", "2024-01", "2024-07", "2025-01", "2025-07", "2026-01", "2026-07"]
    assert b[0][1] == date(2023, 5, 23)
    assert all((e - s).days < 366 and s.year == e.year for _, s, e in b)



def test_categorical_event_table_is_stored(tmp_cfg, clock):
    fin = FakeFinlab()
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    ctx = StoreContext(Layout.from_config(tmp_cfg), "x", tmp_cfg.local_tmp, clock)
    s = DatasetStore("disposal_attention", ctx, ["_dummy"], "1")
    parts = set(s.partitions()["partition"])
    assert parts == {"dataset=disposal_information", "dataset=etl_disposal_stock_filter"}
    ev = s.read_partition("dataset=disposal_information")
    assert list(ev["分時交易"]) == ["五分鐘", "二十分鐘"]
    assert not isinstance(ev["stock_id"].dtype, pd.CategoricalDtype) and list(ev["stock_id"]) == ["5555", "5555"]



def test_off_session_bar_rejected_with_details_and_quarantined(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    fub.extra_bars = {"2330": [{"date": "2023-08-15T14:00:00.000+08:00", "open": 1, "high": 1, "low": 1,
                                "close": 1, "volume": 1, "average": 1}]}
    assert run(tmp_cfg, clock, fin, fub) == 2
    from test_e2e import windows
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    r = w.loc[("2330", "2023-07")]
    assert r["status"] == "partial" and "2023-08-15 14:00" in r["error_msg"]
    q = list((tmp_cfg.data_root / "_tw-market-data-lake" / "quarantine").rglob("2330_2023-07_*.parquet"))
    assert len(q) == 1 and len(pd.read_parquet(q[0])) > 0



def test_emerging_board_bars_before_listing_are_excluded(tmp_cfg, clock):
    """9999 lists 2023-11-01; Fubon also returns pre-IPO emerging-board bars incl. 14:00."""
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    fub.extra_bars = {"9999": [{"date": f"2023-10-{d:02d}T{h}:00.000+08:00", "open": 5, "high": 5, "low": 5,
                                "close": 5, "volume": 1000, "average": 5}
                               for d in (16, 17, 18) for h in ("10:00", "14:00", "14:55")]}
    assert run(tmp_cfg, clock, fin, fub) == 0
    from test_e2e import store, windows
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("9999", "2023-10"), "status"] == "complete"
    p = store(tmp_cfg, clock).read_partition("symbol=9999/year=2023")
    assert p["bar_start"].min().date() == date(2023, 11, 1)
    runs = sorted((tmp_cfg.data_root / "_tw-market-data-lake" / "runs").iterdir())
    ex = pd.read_parquet(runs[-1] / "excluded_outside_listing_5m.parquet")
    assert ex.set_index("symbol").loc["9999", "bars_excluded"] == 9
    saved = pd.read_parquet(ex.set_index("symbol").loc["9999", "saved_to"])        # B7: per-bar rows kept
    assert len(saved) == 9 and saved["bar_start"].dt.strftime("%H:%M").max() == "14:55"
    assert "quarantine" in ex.set_index("symbol").loc["9999", "saved_to"]
