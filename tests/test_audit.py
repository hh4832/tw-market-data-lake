"""Phase A audit: offline tests on synthetic lakes built by the real pipeline. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from conftest import FakeFinlab, FakeFubon
from test_e2e import run, windows
from tw_lake.audit import bars as A2
from tw_lake.audit import universe as A4
from tw_lake.audit.runner import AuditOptions, AuditWriteRefused, run_audit
from tw_lake.clock import TAIPEI

TPE = "Asia/Taipei"


def tree_snapshot(root: Path) -> dict:
    out = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            out[str(p.relative_to(root))] = (p.stat().st_size, p.stat().st_mtime_ns,
                                             hashlib.sha256(p.read_bytes()).hexdigest())
    return out


def build_lake(cfg, clock, fub_setup=None, end=date(2024, 3, 15)):
    fin = FakeFinlab(end=end)
    fub = FakeFubon(fin)
    if fub_setup:
        fub_setup(fub)
    clock.set(datetime(end.year, end.month, end.day, 17, 0, tzinfo=TAIPEI))
    assert run(cfg, clock, fin, fub) in (0, 2)
    return fin, fub


def audit(cfg, tmp_path, **kw) -> tuple[Path, dict]:
    opts = AuditOptions(out_root=tmp_path / "audits", progress=False, template_symbols=("2330", "0050"),
                        sha_sample=1000, **kw)
    out = run_audit(cfg, opts)
    return out, json.loads((out / "audit_summary.json").read_text(encoding="utf-8"))


def table(out: Path, name: str) -> pd.DataFrame:
    return pd.read_parquet(out / f"{name}.parquet")


# ------------------------------------------------------------------------------------------------
def test_audit_is_read_only_and_reconstructs_handoff(tmp_cfg, clock, tmp_path):
    build_lake(tmp_cfg, clock)
    before = tree_snapshot(tmp_cfg.data_root)
    out, s = audit(tmp_cfg, tmp_path)
    assert tree_snapshot(tmp_cfg.data_root) == before          # nothing under DATA_ROOT changed
    assert s["findings"]["source_unchanged"]["ok"] is True
    assert not str(out).startswith(str(tmp_cfg.data_root))
    h = s["findings"]["handoff_reconstruction"]
    w = windows(tmp_cfg, clock)
    assert h["windows"] == len(w)
    assert h["window_status_counts"] == {k: int(v) for k, v in w["status"].value_counts().items()}
    assert h["windows_vs_partitions_total_equal"] is True and h["symbol_year_bar_mismatches"] == 0
    integ = table(out, "audit_integrity")
    assert not ((integ["status"] == "fail") & (integ["severity"] == "critical")).any(), \
        integ[integ["status"] == "fail"].to_string()
    days = table(out, "audit_5m_by_symbol_day")
    d2330 = days[days["symbol"] == "2330"]
    assert (d2330["quality_status"] == "complete").all()
    assert d2330["has_close_auction_bar"].all() and d2330["close_auction_flat"].all()
    rec = table(out, "audit_ohlcv_reconciliation")
    r2330 = rec[rec["symbol"] == "2330"]
    assert (r2330["classification"] == "ok").all() and np.allclose(r2330["volume_ratio"], 1.0)
    assert (out / "README.md").exists() and (out / "audit_summary.md").exists()
    assert "does not declare the data research-ready" in s["verdict"]["statement"]


def test_single_missing_bar_is_not_complete(tmp_cfg, clock, tmp_path):
    hole = date(2024, 2, 20)
    build_lake(tmp_cfg, clock, lambda f: f.drop_bars.add(("2330", hole, "10:05")))
    w = windows(tmp_cfg, clock).set_index(["symbol", "window_id"])
    assert w.loc[("2330", "2024-01"), "status"] == "complete"        # ingestion gate cannot see it
    out, _ = audit(tmp_cfg, tmp_path)
    days = table(out, "audit_5m_by_symbol_day").set_index(["symbol", "date"])
    row = days.loc[("2330", pd.Timestamp(hole))]
    assert row["quality_status"] == "intraday_missing"
    assert row["missing_timestamps"] == "10:05" and row["actual_bar_count"] == 53
    assert row["missing_cause"] == "unverified_no_trade_or_source_gap"
    ready = table(out, "audit_research_ready").set_index(["symbol", "date"])
    assert not ready.loc[("2330", pd.Timestamp(hole)), "ready_5m_normal_session"]
    rec = table(out, "audit_ohlcv_reconciliation").set_index(["symbol", "date"])
    r = rec.loc[("2330", pd.Timestamp(hole))]
    assert r["classification"] in ("ok", "pending_verification") and r["volume_ratio"] < 1   # raw ratio kept


def test_zero_byte_published_file_is_critical_and_listed_for_backfill(tmp_cfg, clock, tmp_path):
    build_lake(tmp_cfg, clock)
    root = tmp_cfg.data_root / "fubon_stock_5m"
    ptr = json.loads((root / "_manifest" / "published.json").read_text())
    parts = pd.read_parquet(root / "_manifest" / ptr["version_dir"] / "partitions.parquet")
    victim = parts[parts["partition"] == "symbol=2330/year=2024"].iloc[0]
    (root / victim["relpath"]).write_bytes(b"")                      # simulate the Drive FUSE 0-byte upload
    out, s = audit(tmp_cfg, tmp_path)
    integ = table(out, "audit_integrity")
    hit = integ[(integ["partition"] == "symbol=2330/year=2024") & (integ["check"] == "file_size")]
    assert len(hit) == 1 and hit.iloc[0]["severity"] == "critical" and "zero_byte_file" in hit.iloc[0]["detail"]
    bf = table(out, "audit_backfill_candidates")
    assert "symbol=2330/year=2024" in set(bf["partition"])
    ready = table(out, "audit_research_ready")
    r = ready[(ready["symbol"] == "2330") & (ready["date"].dt.year == 2024)]
    assert len(r) and (~r["partition_ok"]).all() and (~r["ready_5m_normal_session"]).all()
    assert s["verdict"]["critical_items"] >= 1


def test_disposal_call_auction_regime_is_not_reported_as_missing(tmp_cfg, clock, tmp_path):
    d = date(2023, 9, 5)                                              # FakeFinlab: 5555 disposal 09-01..09-14 (5 min)
    keep = {"09:00", "09:20", "09:40", "10:00", "10:20", "10:40", "11:00", "11:20", "11:40", "12:00", "12:20",
            "12:40", "13:00", "13:20", "13:30"}
    build_lake(tmp_cfg, clock, lambda f: f.only_bars.__setitem__(("5555", d), keep))
    out, _ = audit(tmp_cfg, tmp_path)
    reg = table(out, "trading_regime_daily")
    r = reg[(reg["symbol"] == "5555") & (reg["date"] == pd.Timestamp(d))]
    assert len(r) == 1 and r.iloc[0]["regime"] == "disposal_call_auction_5min"
    assert r.iloc[0]["known_at"] == pd.Timestamp("2023-08-31")
    days = table(out, "audit_5m_by_symbol_day").set_index(["symbol", "date"])
    row = days.loc[("5555", pd.Timestamp(d))]
    assert row["quality_status"] == "regime_call_auction_unverified" and row["actual_bar_count"] == 15
    assert np.isnan(row["expected_bar_count"])                        # never fabricated


def test_price_outside_daily_range_is_data_error_candidate(tmp_cfg, clock, tmp_path):
    d = date(2024, 3, 1)
    build_lake(tmp_cfg, clock, lambda f: f.price_shift.__setitem__(("0050", d), 5.0))
    out, s = audit(tmp_cfg, tmp_path)
    rec = table(out, "audit_ohlcv_reconciliation").set_index(["symbol", "date"])
    row = rec.loc[("0050", pd.Timestamp(d))]
    assert row["classification"] == "data_error_candidate" and "price_outside_daily_range" in row["flags"]
    assert s["findings"]["recon_summary"]["by_classification"]["data_error_candidate"] == 1
    top = pd.read_csv(out / "audit_ohlcv_top100.csv", dtype={"symbol": str})
    assert top.iloc[0]["symbol"] == "0050"


def test_gap_day_with_large_finlab_volume_is_source_missing_suspected(tmp_cfg, clock, tmp_path):
    gap = date(2024, 2, 21)
    build_lake(tmp_cfg, clock, lambda f: f.missing_days.__setitem__("2330", {gap}))
    out, _ = audit(tmp_cfg, tmp_path)
    g = table(out, "audit_gap_cases")
    row = g[(g["symbol"] == "2330") & (g["date"] == pd.Timestamp(gap))].iloc[0]
    assert row["classification"] == "source_missing_suspected" and row["window_status"] == "partial"
    p = table(out, "audit_partial_windows")
    assert p.loc[p["symbol"] == "2330", "suggested_backfill_from"].iloc[0] == gap.isoformat()
    days = table(out, "audit_5m_by_symbol_day").set_index(["symbol", "date"])
    assert days.loc[("2330", pd.Timestamp(gap)), "quality_status"] == "no_bars_finlab_traded"


def test_adjusted_snapshot_change_reads_current_version(tmp_cfg, clock, tmp_path):
    build_lake(tmp_cfg, clock)
    fin2 = FakeFinlab(end=date(2024, 3, 15), adj_factor=0.5)         # e.g. a split re-bases adjusted prices
    clock.set(datetime(2024, 3, 16, 17, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, FakeFubon(fin2), mode="update") in (0, 2)
    _, s = audit(tmp_cfg, tmp_path)
    assert s["meta"]["sources"]["finlab_daily_adjusted"]["seq"] == 2


def test_audit_refuses_to_write_inside_source_folders(tmp_cfg, clock, tmp_path):
    build_lake(tmp_cfg, clock)
    for bad in (tmp_cfg.data_root / "fubon_stock_5m", tmp_cfg.data_root / "finlab_0050",
                tmp_cfg.data_root / "_tw-market-data-lake" / "runs"):
        with pytest.raises(AuditWriteRefused):
            run_audit(tmp_cfg, AuditOptions(out_root=bad, progress=False))


# ---------------------------------------------------------------------------------------- units
def _bars(times, d="2024-03-04", sym="9999", vol=1.0):
    ts = pd.to_datetime([f"{d} {t}" for t in times]).tz_localize(TPE)
    n = len(ts)
    return pd.DataFrame({"symbol": sym, "bar_start": ts, "open": [10.0] * n, "high": [10.5] * n, "low": [9.5] * n,
                         "close": [10.0] * n, "volume_raw": [vol] * n, "volume_unit": "lot"})


def test_partition_level_invalid_bars_and_non_trading_day():
    tmpl_times = ["09:00", "09:05", "13:30"]
    templates = {pd.Timestamp("2024-03-04"): frozenset(tmpl_times)}
    df = pd.concat([_bars(["09:00", "09:00", "09:07", "13:30"]),       # duplicate + off-grid
                    _bars(["10:00"], d="2024-03-09")])                  # Saturday: not a trading day
    df.loc[df.index[-1], "volume_raw"] = 0.0
    fin = pd.DataFrame({"date": pd.to_datetime(["2024-03-04"]), "volume_shares": [50_000.0]})
    out = A2.audit_symbol_partition(df.reset_index(drop=True), "9999", fin, templates,
                                    pd.DataFrame(columns=["date", "regime", "auction_interval_min"]),
                                    {pd.Timestamp("2024-03-04")}, "symbol=9999/year=2024", 1).set_index("date")
    a = out.loc[pd.Timestamp("2024-03-04")]
    assert a["duplicate_bars"] == 1 and "09:07" in a["unexpected_timestamps"] and a["quality_status"] == "invalid"
    assert a["missing_timestamps"] == "09:05"
    b = out.loc[pd.Timestamp("2024-03-09")]
    assert b["not_trading_day"] and b["quality_status"] == "invalid" and b["zero_volume_bars"] == 1
    assert b["regime_unknown"]                                          # no template for that date


def test_excluded_bars_boundary_risk():
    tds = list(pd.bdate_range("2024-01-01", "2024-12-31"))
    ex = pd.DataFrame({
        "symbol": ["A", "B", "C"], "window_id": ["2024-01"] * 3, "bars_excluded": [100, 50, 10],
        "first_excluded": ["2024-01-02 09:00:00+08:00", "2024-01-02 09:00:00+08:00", "2024-06-03 09:00:00+08:00"],
        "last_excluded": ["2024-03-04 14:55:00+08:00", "2024-03-04 13:20:00+08:00", "2024-06-05 13:30:00+08:00"],
        "first_price_date": ["2024-03-05", "2024-03-05", "2024-01-02"],
        "last_price_date": ["2026-10-22", "2026-10-22", "2024-05-31"]})
    r = A4.excluded_bars_audit(ex, tds).set_index("symbol")
    assert r.loc["A", "risk"] == "consistent_with_pre_listing_emerging" and r.loc["A", "emerging_session_evidence"]
    assert r.loc["B", "risk"] == "adjacent_without_emerging_evidence"
    assert r.loc["C", "risk"] == "excluded_after_last_price_review"


def test_classification_issues_flag_non_pit_and_etf_rule():
    master = pd.DataFrame({"symbol": ["2330", "00679B", "00631L", "1234"],
                           "instrument_type": ["common_stock", "etf", "etf", "common_stock"],
                           "classification_source": ["security_categories_current_snapshot"] * 3 + ["code_rule_inferred"],
                           "classification_asof": "2024-03-15", "classification_is_pit": False,
                           "listing_status_latest": ["listed", "listed", "listed", "delisted_inferred"],
                           "last_price_date": pd.to_datetime(["2024-03-15"] * 3 + ["2023-09-29"])})
    cats = pd.DataFrame({"stock_id": ["2330", "00679B", "00631L"], "category": ["半導體業", "其他證券", "domestic_etf"],
                         "market": ["sii", "other_securities", "etf"]})
    iss = A4.classification_issues(master, cats)
    kinds = set(zip(iss["symbol"], iss["issue"]))
    assert ("*", "classification_not_pit") in kinds
    assert ("00679B", "etf_by_code_rule_overrides_category") in kinds
    assert ("00631L", "etf_subtype_leveraged") in kinds and ("00679B", "etf_subtype_bond") in kinds
    assert ("1234", "classified_by_code_rule_only") in kinds and ("1234", "delisting_inferred_from_price_gap") in kinds
    prov = A4.provenance_table().set_index("column")
    assert not prov.loc["has_trade", "usable_for_same_day_pre_open_selection"]
    assert prov.loc["listing_status", "provenance"] == "reconstructed_ex_post"


def test_verify_latest_run_detects_zero_byte_file(tmp_cfg, clock):
    from tw_lake.audit.verify import latest_run_id, verify
    from tw_lake.paths import Layout
    build_lake(tmp_cfg, clock)
    layout = Layout.from_config(tmp_cfg)
    rid = latest_run_id(layout)
    rep = verify(layout, rid)
    assert rep["ok"] and rep["datasets"]["fubon_5m"]["checked_partitions"] > 0
    root = tmp_cfg.data_root / "fubon_stock_5m"
    ptr = json.loads((root / "_manifest" / "published.json").read_text())
    parts = pd.read_parquet(root / "_manifest" / ptr["version_dir"] / "partitions.parquet")
    (root / parts.iloc[0]["relpath"]).write_bytes(b"")
    rep2 = verify(layout, rid)
    assert not rep2["ok"] and any("zero_byte_file" in f["detail"] for f in rep2["failures"])
    assert not verify(layout, None, check_all=True, deep_all=True)["ok"]


def test_vectorized_partition_audit_matches_reference(tmp_cfg, clock, tmp_path):
    """The fast A2 implementation must produce exactly the reference (loop) output."""
    hole = date(2024, 2, 20)
    d5 = date(2023, 9, 5)
    keep = {"09:00", "09:20", "13:30"}

    def setup(f):
        f.drop_bars.add(("2330", hole, "10:05"))
        f.only_bars[("5555", d5)] = keep
    build_lake(tmp_cfg, clock, setup)
    from tw_lake.audit.reader import load_published
    from tw_lake.audit.regime import trading_regime_daily
    from tw_lake.paths import Layout
    lay = Layout.from_config(tmp_cfg)
    pv = load_published("fubon_5m", lay.dataset("fubon_5m"))
    raw = pd.concat(pd.read_parquet(lay.dataset("finlab_daily_raw") / r) for r in
                    load_published("finlab_daily_raw", lay.dataset("finlab_daily_raw")).partitions["relpath"])
    raw["symbol"] = raw["symbol"].astype(str)
    disp = load_published("disposal_attention", lay.dataset("disposal_attention"))
    di = pd.read_parquet(lay.dataset("disposal_attention") /
                         disp.partitions.set_index("partition").loc["dataset=disposal_information", "relpath"])
    tds = sorted(pd.to_datetime(raw.loc[raw["volume_shares"] > 0, "date"]).unique())
    regime = trading_regime_daily(di, tds)
    frames = {r.partition: pd.read_parquet(pv.data_path(r.relpath)) for r in pv.partitions.itertuples()}
    tm = A2.build_templates([v for k, v in frames.items() if k.split("/")[0] in ("symbol=2330", "symbol=0050")])
    for part, df in frames.items():
        sym, yr = part.split("/")[0].split("=")[1], int(part.split("=")[-1])
        fd = raw[(raw["symbol"] == sym) & (pd.to_datetime(raw["date"]).dt.year == yr)]
        rg = regime[regime["symbol"] == sym]
        a = A2.audit_symbol_partition(df, sym, fd, tm, rg, set(tds), part, 1).reset_index(drop=True)
        b = A2._audit_symbol_partition_reference(df, sym, fd, tm, rg, set(tds), part, 1).reset_index(drop=True)
        pd.testing.assert_frame_equal(a, b, check_dtype=False, check_exact=False)


# ---- readiness v0.2 (no-trade tolerant) ------------------------------------------------------
def _day(symbol, lots, status, missing, close_bar=True, partition="p"):
    return {"symbol": symbol, "date": pd.Timestamp("2024-02-20"), "quality_status": status, "partition": partition,
            "finlab_volume_lots": lots, "regime": "normal", "liquidity_bucket": A2.liquidity_bucket(pd.Series([lots]))[0],
            "missing_count": missing, "has_close_auction_bar": close_bar}


def test_tolerant_readiness_rules():
    from tw_lake.audit.readiness import missing_by_liquidity, readiness_summary, research_ready
    days = pd.DataFrame([
        _day("A", 20000, "intraday_missing", 1),            # liquid: a missing bar is not believable as no-trade
        _day("B", 10, "intraday_missing", 30),              # illiquid, clean recon -> tolerant
        _day("C", 10, "intraday_missing", 30),              # illiquid, volume below band -> rejected
        _day("D", 10, "intraday_missing", 30, close_bar=False),
        _day("E", 200, "intraday_missing", 16),             # 50_500 bucket allows 15
        _day("F", 200, "intraday_missing", 15),
        _day("G", 20000, "complete", 0),
        _day("H", 10, "intraday_missing", 5, partition="bad"),
        _day("I", 10, "regime_call_auction_unverified", 0),
        _day("J", 20000, "complete", 0),                    # complete but suspect recon
    ])
    rec = pd.DataFrame({"symbol": list("ABCDEFGHIJ"), "date": pd.Timestamp("2024-02-20"),
                        "classification": ["ok", "ok", "pending_verification", "ok", "ok", "ok", "pending_verification",
                                           "ok", "ok", "suspect"]})
    v020 = {"ge5000_lots": 0, "500_5000_lots": 3, "50_500_lots": 15, "lt50_lots": None}
    r = research_ready(days, rec, {"bad"}, v020, require_close_auction=True).set_index("symbol")
    tol = r["ready_5m_no_trade_tolerant"]
    assert tol.to_dict() == {"A": False, "B": True, "C": False, "D": False, "E": False, "F": True, "G": True,
                             "H": False, "I": False, "J": False}
    assert (r["ready_30m_no_trade_tolerant"] == tol).all()
    assert (~r["ready_5m_normal_session"] | tol).all()                # superset of strict
    reasons = r["tolerant_not_ready_reason"]
    assert reasons["A"] == "missing_over_allowance_ge5000_lots" and reasons["C"] == "recon_pending_verification"
    assert reasons["D"] == "no_close_auction_bar" and reasons["H"] == "partition_integrity_failure"
    assert reasons["I"] == "5m_regime_call_auction_unverified" and reasons["J"] == "recon_suspect"
    assert r.loc["G", "ready_5m_normal_session"] and not r.loc["B", "ready_5m_normal_session"]
    s = readiness_summary(r.reset_index(), v020, True)
    assert s["ready_5m_no_trade_tolerant"]["count"] == 3 and s["by_liquidity_bucket"]["lt50_lots"]["tolerant_pct"] == 20.0
    h = missing_by_liquidity(r.reset_index())
    assert h["days"].sum() == 9 and set(h["liquidity_bucket"]) == {"ge5000_lots", "50_500_lots", "lt50_lots"}


def test_parse_allowance():
    from tw_lake.audit.readiness import DEFAULT_MISSING_ALLOWANCE, parse_allowance
    assert parse_allowance(None) == DEFAULT_MISSING_ALLOWANCE
    a = parse_allowance("500_5000_lots=0,lt50_lots=none")
    assert a["500_5000_lots"] == 0 and a["lt50_lots"] is None and a["ge5000_lots"] == 0
    with pytest.raises(ValueError):
        parse_allowance("bogus=1")


def test_rescore_matches_runner_and_leaves_audit_untouched(tmp_cfg, clock, tmp_path):
    from tw_lake.audit.rescore import rescore
    hole = date(2024, 2, 20)
    build_lake(tmp_cfg, clock, lambda f: f.drop_bars.add(("2330", hole, "10:05")))
    out, s = audit(tmp_cfg, tmp_path)
    before = tree_snapshot(out)
    key = ("2330", pd.Timestamp(hole))
    runner_ready = table(out, "audit_research_ready").set_index(["symbol", "date"]).sort_index()
    # fake 2330 trades ~1,000 lots (500_5000 bucket, allowance 10) and the dropped bar holds <2% of the
    # volume, so the volume band cannot see it: tolerant accepts it - the documented limitation.
    assert runner_ready.loc[key, "ready_5m_no_trade_tolerant"] and not runner_ready.loc[key, "ready_5m_normal_session"]
    assert "ready_5m_no_trade_tolerant" in s["findings"]["readiness"]
    r1 = rescore(out)
    again = pd.read_parquet(r1 / "audit_research_ready.parquet").set_index(["symbol", "date"]).sort_index()
    pd.testing.assert_frame_equal(again, runner_ready, check_dtype=False)
    r2 = rescore(out, tmp_path / "strict_rescore", {"ge5000_lots": 0, "500_5000_lots": 0, "50_500_lots": 15,
                                                    "lt50_lots": None})
    strict = pd.read_parquet(r2 / "audit_research_ready.parquet").set_index(["symbol", "date"])
    assert not strict.loc[key, "ready_5m_no_trade_tolerant"]
    assert strict.loc[key, "tolerant_not_ready_reason"] == "missing_over_allowance_500_5000_lots"
    after = {k: v for k, v in tree_snapshot(out).items() if not k.startswith("rescore_")}
    assert after == before                                           # original audit files unchanged
    summ = json.loads((r1 / "rescore_summary.json").read_text(encoding="utf-8"))
    assert "research-ready" in summ["meta"]["statement"]


def test_tolerant_without_close_auction_requirement():
    from tw_lake.audit.readiness import research_ready
    days = pd.DataFrame([_day("D", 10, "intraday_missing", 30, close_bar=False),
                         _day("C", 10, "intraday_missing", 30, close_bar=False)])
    rec = pd.DataFrame({"symbol": ["D", "C"], "date": pd.Timestamp("2024-02-20"),
                        "classification": ["ok", "pending_verification"]})
    on = research_ready(days, rec, set(), require_close_auction=True).set_index("symbol")["ready_5m_no_trade_tolerant"]
    off = research_ready(days, rec, set()).set_index("symbol")["ready_5m_no_trade_tolerant"]
    assert not on["D"] and off["D"] and not off["C"]


def test_v021_defaults():
    from tw_lake.audit.readiness import DEFAULT_MISSING_ALLOWANCE, READINESS_VERSION, research_ready
    assert READINESS_VERSION == "0.2.1"
    assert DEFAULT_MISSING_ALLOWANCE == {"ge5000_lots": 0, "500_5000_lots": 10, "50_500_lots": None, "lt50_lots": None}
    days = pd.DataFrame([_day("A", 20000, "intraday_missing", 1), _day("B", 1000, "intraday_missing", 10),
                         _day("C", 1000, "intraday_missing", 11), _day("D", 200, "intraday_missing", 45, close_bar=False)])
    rec = pd.DataFrame({"symbol": list("ABCD"), "date": pd.Timestamp("2024-02-20"), "classification": "ok"})
    tol = research_ready(days, rec, set()).set_index("symbol")["ready_5m_no_trade_tolerant"]
    assert tol.to_dict() == {"A": False, "B": True, "C": False, "D": True}


def test_gap_with_finlab_volume_but_no_close_is_no_regular_trade():
    from tw_lake.audit.gaps import classify_gaps
    gaps = pd.DataFrame({"symbol": ["A", "B", "C"], "date": pd.to_datetime(["2024-12-16"] * 3), "window_id": "2024-07",
                         "window_status": "complete_with_gaps", "gap_attempts": 2})
    fin = pd.DataFrame({"symbol": ["A", "B", "C"], "date": pd.to_datetime(["2024-12-16"] * 3),
                        "volume_shares": [500000.0, 3617707.0, 300.0], "turnover": [1.3e7, 1.1e8, 1e4],
                        "close": [np.nan, 31.23, np.nan]})
    g = classify_gaps(gaps, fin, pd.DataFrame(), 1).set_index("symbol")
    assert g.loc["A", "classification"] == "no_regular_session_trade_probable"     # block trade only
    assert "finlab_close_missing=True" in g.loc["A", "evidence"]
    assert g.loc["B", "classification"] == "source_missing_suspected"
    assert g.loc["C", "classification"] == "no_regular_session_trade_probable"
