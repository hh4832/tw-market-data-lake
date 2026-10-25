"""End-to-end with fakes: pilot -> update -> repair. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd

from conftest import FakeFinlab, FakeFubon, no_sleep
from tw_lake import cli
from tw_lake.clock import TAIPEI
from tw_lake.paths import Layout
from tw_lake.store import DatasetStore, StoreContext

SYMS = "2330,0050,1234,9999,5555"


def run(cfg, clock, fin, fub, mode="pilot", extra=()):
    import os
    import yaml
    cfg_path = cfg.local_tmp.parent / "test_config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg.raw, allow_unicode=True), encoding="utf-8")
    argv = ["--mode", mode, "--config", str(cfg_path), "--data-root", str(cfg.data_root), "--symbols", SYMS, *extra]
    os.environ["TW_LAKE_LOCAL_TMP"] = str(cfg.local_tmp)
    return cli.main(argv, finlab_factory=lambda: fin, fubon_factory=lambda: fub, clock=clock,
                    sleep=no_sleep, monotonic=lambda: 0.0)


def store(cfg, clock, name="fubon_5m"):
    ctx = StoreContext(Layout.from_config(cfg), "inspect", cfg.local_tmp, clock)
    return DatasetStore(name, ctx, ["bar_start"], "1", params=cfg.minute_params())


def windows(cfg, clock):
    return store(cfg, clock).tables()["coverage_by_window"]


def test_pilot_then_update_then_idempotent(tmp_cfg, clock):
    fin = FakeFinlab(end=date(2024, 3, 15))
    fub = FakeFubon(fin)
    clock.set(datetime(2024, 3, 15, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, fub) == 0
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2024-01"), "status"] == "complete"
    assert w.loc[("2330", "2024-01"), "covered_to"] == pd.Timestamp("2024-03-15")
    assert w.loc[("1234", "2023-10"), "status"] == "no_data_delisted"
    assert w.loc[("9999", "2023-07"), "status"] == "no_data_not_yet_listed"
    assert w.loc[("5555", "2023-10"), "status"] == "no_data_no_trade"
    assert w.loc[("1234", "2023-07"), "status"] == "complete"
    # protected folders untouched
    assert (tmp_cfg.data_root / "finlab_0050" / "keep.txt").read_text() == "existing user data"
    assert sorted(p.name for p in (tmp_cfg.data_root / "finlab_0050").iterdir()) == ["keep.txt"]

    # two weeks later: only the extension of the current window is fetched
    fin2 = FakeFinlab(end=date(2024, 3, 29))
    fub2 = FakeFubon(fin2)
    clock.set(datetime(2024, 3, 29, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, fub2, mode="update") == 0
    froms = sorted({c["from"] for c in fub2.calls if c["symbol"] == "2330"})
    assert froms == ["2024-03-15"], froms         # inclusive overlap day, nothing older
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2024-01"), "covered_to"] == pd.Timestamp("2024-03-29")
    s = store(tmp_cfg, clock)
    part = s.read_partition("symbol=2330/year=2024")
    assert part["bar_start"].max().date() == date(2024, 3, 29)
    assert not part["bar_start"].duplicated().any()
    seq_before = s.pointer()["seq"]

    # same day again: nothing to do, no new manifest version, no requests
    fub3 = FakeFubon(fin2)
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, fub3, mode="update") == 0
    assert fub3.calls == []
    assert store(tmp_cfg, clock).pointer()["seq"] == seq_before

    # run folders are immutable and complete
    runs = sorted((tmp_cfg.data_root / "_tw-market-data-lake" / "runs").iterdir())
    assert len(runs) == 3
    info = json.loads((runs[-1] / "run_info.json").read_text())
    assert info["status"] == "success" and info["git_commit"]
    assert (runs[0] / "qa_summary.md").exists() and (runs[0] / "request_log.parquet").exists()


def test_old_partition_versions_are_kept(tmp_cfg, clock):
    fin = FakeFinlab(end=date(2024, 3, 15))
    clock.set(datetime(2024, 3, 15, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    s = store(tmp_cfg, clock)
    old_rel = s.partitions().set_index("partition").loc["symbol=2330/year=2024", "relpath"]
    fin2 = FakeFinlab(end=date(2024, 3, 29))
    clock.set(datetime(2024, 3, 29, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, FakeFubon(fin2), mode="update") == 0
    s2 = store(tmp_cfg, clock)
    new_rel = s2.partitions().set_index("partition").loc["symbol=2330/year=2024", "relpath"]
    assert new_rel != old_rel
    assert (s2.root / old_rel).exists(), "previous version must be retained"
    # 2023 partition untouched by the 2024 extension
    p23 = s2.partitions().set_index("partition").loc["symbol=2330/year=2023"]
    assert p23["created_run_id"] == s.partitions().set_index("partition").loc["symbol=2330/year=2023", "created_run_id"]


def test_unknown_404_is_failed_not_complete(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    fub.not_found_symbols = {"1234"}         # FinLab shows trades, Fubon says 404
    assert run(tmp_cfg, clock, fin, fub) == 2  # partial
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    r = w.loc[("1234", "2023-07")]
    assert r["status"] == "failed" and r["error_class"] == "unknown_404"
    # windows after delisting are explained without any request
    assert w.loc[("1234", "2023-10"), "status"] == "no_data_delisted"
    cov = store(tmp_cfg, clock).tables()["coverage_by_symbol"].set_index("symbol")
    assert cov.loc["1234", "status"] == "needs_repair"


def test_repair_first_then_append(tmp_cfg, clock):
    fin = FakeFinlab(end=date(2024, 3, 15))
    fub = FakeFubon(fin)
    fub.transient_failures = {("2330", "2023-07-01", "2023-09-30"): 99}   # this window fails
    clock.set(datetime(2024, 3, 15, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, fub) == 2
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "failed"
    assert w.loc[("2330", "2023-07"), "error_class"] == "retries_exhausted"

    fin2 = FakeFinlab(end=date(2024, 3, 29))
    fub2 = FakeFubon(fin2)
    clock.set(datetime(2024, 3, 29, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, fub2, mode="update") == 0
    calls = [(c["symbol"], c["from"]) for c in fub2.calls]
    i_repair = calls.index(("2330", "2023-07-01"))
    i_extend = calls.index(("2330", "2024-03-15"))
    assert i_repair < i_extend
    # every repair request precedes every extension request
    repair_idx = [i for i, c in enumerate(calls) if c[1] == "2023-07-01"]
    extend_idx = [i for i, c in enumerate(calls) if c[1] == "2024-03-15"]
    assert max(repair_idx) < min(extend_idx)
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "complete"


def test_cross_year_gap_repair(tmp_cfg, clock):
    """Failed windows on both sides of a year boundary are repaired into separate year partitions."""
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    fub.transient_failures = {("2330", "2023-10-01", "2023-12-31"): 99,
                              ("2330", "2024-01-01", "2024-03-29"): 99}
    assert run(tmp_cfg, clock, fin, fub) == 2
    s = store(tmp_cfg, clock)
    parts = set(s.partitions()["partition"])
    assert "symbol=2330/year=2023" in parts          # from the 2023-05 / 2023-07 windows
    assert "symbol=2330/year=2024" not in parts
    before23 = s.read_partition("symbol=2330/year=2023")
    assert before23["bar_start"].max().date() < date(2023, 10, 1)

    fub2 = FakeFubon(fin)
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, fub2, mode="update") == 0
    s2 = store(tmp_cfg, clock)
    p23 = s2.read_partition("symbol=2330/year=2023")
    p24 = s2.read_partition("symbol=2330/year=2024")
    assert p23["bar_start"].max().date() == date(2023, 12, 29)
    assert p24["bar_start"].min().date() == date(2024, 1, 2)
    assert set(p23["bar_start"].dt.year) == {2023} and set(p24["bar_start"].dt.year) == {2024}
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-10"), "status"] == "complete"
    assert w.loc[("2330", "2024-01"), "status"] == "complete"


def test_source_gap_partial_then_complete_with_gaps(tmp_cfg, clock):
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    fub.missing_days = {"2330": {date(2023, 8, 15)}}
    assert run(tmp_cfg, clock, fin, fub) == 0  # data was written; gap is tracked as partial
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "partial"
    assert json.loads(w.loc[("2330", "2023-07"), "gap_days"]) == ["2023-08-15"]
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    fub2 = FakeFubon(fin)
    fub2.missing_days = fub.missing_days
    run(tmp_cfg, clock, fin, fub2, mode="update")
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "complete_with_gaps"   # gap_retry_limit=2, same gap


def test_authorized_refetch_of_complete_with_gaps_window(tmp_cfg, clock):
    gap = date(2023, 8, 15)
    fin = FakeFinlab()
    fub = FakeFubon(fin)
    fub.missing_days = {"2330": {gap}}
    run(tmp_cfg, clock, fin, fub)
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    fub2 = FakeFubon(fin)
    fub2.missing_days = fub.missing_days
    run(tmp_cfg, clock, fin, fub2, mode="update")
    assert windows(tmp_cfg, clock).set_index(["symbol", "window_id"]).loc[("2330", "2023-07"), "status"] \
        == "complete_with_gaps"
    # plain update never re-opens a complete_with_gaps window
    clock.set(datetime(2024, 3, 29, 19, 0, tzinfo=TAIPEI))
    fub3 = FakeFubon(fin)
    run(tmp_cfg, clock, fin, fub3, mode="update")
    assert not [c for c in fub3.calls if c["symbol"] == "2330" and c["from"] == "2023-07-01"]
    # source still missing -> stays complete_with_gaps, nothing else changes
    clock.set(datetime(2024, 3, 29, 20, 0, tzinfo=TAIPEI))
    fub4 = FakeFubon(fin)
    fub4.missing_days = fub.missing_days
    run(tmp_cfg, clock, fin, fub4, mode="update", extra=("--refetch-gap-days", "2330:2023-08-15,0050:2023-08-15"))
    assert [c for c in fub4.calls if c["symbol"] == "2330" and c["from"] == "2023-07-01"]
    assert not [c for c in fub4.calls if c["symbol"] == "0050" and c["from"] == "2023-07-01"]   # not a listed gap
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "complete_with_gaps"
    # source now has the day -> merged, window complete, no conflicts
    clock.set(datetime(2024, 3, 29, 21, 0, tzinfo=TAIPEI))
    fub5 = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub5, mode="update", extra=("--refetch-gap-days", "2330:2023-08-15")) == 0
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "complete" and pd.isna(w.loc[("2330", "2023-07"), "gap_days"])
    t = store(tmp_cfg, clock).tables()
    assert len(t["conflicts_log"]) == 0
    p = t["partitions"].set_index("partition").loc["symbol=2330/year=2023"]
    bars = pd.read_parquet(tmp_cfg.data_root / "fubon_stock_5m" / p["relpath"])
    assert (bars["bar_start"].dt.tz_convert("Asia/Taipei").dt.date == gap).sum() == 54


def test_conflict_keeps_published_value(tmp_cfg, clock):
    fin = FakeFinlab()
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    s = store(tmp_cfg, clock)
    before = s.read_partition("symbol=2330/year=2023")
    # force a repair refetch of a window whose source data changed
    fub = FakeFubon(fin)
    fub.price_shift = {("2330", date(2023, 8, 15)): 5.0}
    ws = s.tables()["coverage_by_window"]
    m = (ws["symbol"] == "2330") & (ws["window_id"] == "2023-07")
    ws.loc[m, "status"] = "failed"
    tables = s.tables()
    s.ctx.run_id = "manual_edit"
    s.commit({**tables, "coverage_by_window": ws})
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, fub, mode="update") == 2
    s2 = store(tmp_cfg, clock)
    w = s2.tables()["coverage_by_window"].set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2023-07"), "status"] == "conflict"
    after = s2.read_partition("symbol=2330/year=2023")
    pd.testing.assert_frame_equal(before.reset_index(drop=True), after.reset_index(drop=True))
    assert len(s2.tables()["conflicts_log"]) > 0


def test_run_reconciliation_covers_only_fetched_ranges(tmp_cfg, clock):
    """Audit F5: an update that only extends the current window must not reconcile older windows."""
    fin = FakeFinlab(end=date(2024, 3, 15))
    clock.set(datetime(2024, 3, 15, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    fin2 = FakeFinlab(end=date(2024, 3, 29))
    clock.set(datetime(2024, 3, 29, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, FakeFubon(fin2), mode="update") == 0
    runs = sorted((tmp_cfg.data_root / "_tw-market-data-lake" / "runs").iterdir())
    rec = pd.read_parquet(runs[-1] / "qa_reconciliation_5m.parquet")
    assert rec["date"].min() >= pd.Timestamp("2024-03-15")
    assert "missing_in_source" not in set(rec["classification"])
