"""FINAL GATE acceptance harness - synthetic end-to-end and known-answer tests (SYNTHETIC_SMOKE_ONLY)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from acceptance.final_gate import core as C  # noqa: E402
from acceptance.final_gate import run as G  # noqa: E402
from acceptance.final_gate.probe import Probe, Sources, check_ledger  # noqa: E402

T = pd.Timestamp
SES = pd.bdate_range("2025-01-02", periods=40)
SYMS = ["1101", "2330", "3333", "4444", "8101", "00999"]
DIV_DAY, LOCK_DAY, UNK_DAY = SES[20], SES[10], SES[12]


def base_px(sym, i):
    p = {"1101": 40.0, "2330": 600.0, "3333": 50.0, "4444": 80.0, "8101": 12.0, "00999": 20.0}[sym]
    p = p * (1 + 0.002 * ((i * 7 + len(sym)) % 5 - 2))
    if sym == "3333" and i >= 20:
        p -= 1.0                                                    # ex-dividend 1.0
    return round(p / C.tick(p)) * C.tick(p)


def day_bars(sym, d):
    i = SES.get_loc(d)
    p = base_px(sym, i)
    times = [d + g for g in C.GRID]
    if sym == "1101" and d == LOCK_DAY:                              # locked limit-up day
        up = round(base_px(sym, i - 1) * 1.1 / 0.05 - 0.5) * 0.05
        return pd.DataFrame({"bar_start": times, "open": up, "high": up, "low": up, "close": up, "volume_raw": 80.0})
    k = np.arange(len(times))
    o = p + C.tick(p) * ((k % 3) - 1)
    return pd.DataFrame({"bar_start": times, "open": o, "high": o + C.tick(p), "low": o - C.tick(p),
                         "close": o, "volume_raw": 60.0})


def make_src():
    rules, mask, panel = [], [], []
    for s in SYMS:
        for i, d in enumerate(SES):
            ref = base_px(s, i - 1) if i else base_px(s, 0)
            up, dn = round(ref * 1.1, 2), round(ref * 0.9, 2)
            if s == "1101" and d == LOCK_DAY:
                up = float(day_bars(s, d)["open"].iloc[0])
            st, be, se = "DERIVED", "ELIGIBLE_SUBJECT_TO_LIQUIDITY", "ELIGIBLE_SUBJECT_TO_LIQUIDITY"
            if s == "1101" and d == LOCK_DAY:
                be = "BLOCKED_LOCKED_LIMIT_UP"
            if s == "2330" and d == UNK_DAY:
                st, be, se = "UNKNOWN_NO_REFERENCE", "UNKNOWN", "UNKNOWN"
            ic = "ETF" if s.startswith("00") else "STOCK"
            rules.append({"symbol": s, "date": d, "instrument_class": ic, "price_limit_status": st, "limit_up": up,
                          "limit_down": dn, "buy_eligibility": be, "sell_eligibility": se,
                          "liquidity_bucket": "50_500_lots"})
            mask.append({"symbol": s, "exchange": "TWSE", "date": d, "liquidity_bucket": "50_500_lots",
                         "session_type": "normal_template", "level_A_strict": True,
                         "can_use_for_price_backtest": not (s == "1101" and i == 15),
                         "can_use_for_volume_backtest": True, "research_tier": "LEVEL_A", "exclusion_reason": ""})
            panel.append({"symbol": s, "date": d, "official_listed": True, "delisted_official": False,
                          "security_type": "ETF" if s.startswith("00") else "STOCK", "halted_official": False})
    ca = pd.DataFrame([
        {"symbol": "3333", "effective_market_session": DIV_DAY, "accounting_status": "SUPPORTED",
         "cash_per_share": 1.0, "stock_per_share": 0.0, "price_factor": base_px("3333", 20) / base_px("3333", 19),
         "available_at": f"{DIV_DAY:%Y-%m-%d}T08:30:00+08:00"},
        {"symbol": "4444", "effective_market_session": DIV_DAY, "accounting_status": "PRICE_ONLY",
         "cash_per_share": np.nan, "stock_per_share": np.nan, "price_factor": 0.9,
         "available_at": f"{DIV_DAY:%Y-%m-%d}T08:30:00+08:00"}])
    cache = {s: pd.concat([day_bars(s, d) for d in SES], ignore_index=True) for s in SYMS}
    return Sources(pd.DataFrame(mask), pd.DataFrame(panel), pd.DataFrame(rules), ca,
                   lambda s, y: cache[s], SES, [{"symbol": "8101", "start": "2025-01-01", "end": "2025-12-31"}])


# ------------------------------------------------------------------------------------------ known answers
def test_resample_buckets_and_availability():
    d = SES[3]
    b = day_bars("2330", d)
    early = b[(b["bar_start"] < d + pd.Timedelta(hours=9, minutes=40)) | (b["bar_start"] >= d + pd.Timedelta(hours=10))]
    r = C.resample_30m(early, d).set_index("bucket")
    assert len(r) == 10 and r.at["B1", "n_expected"] == 6 and r.at["B1", "n_real"] == 2
    assert r.at["B1", "bar_end"] == d + pd.Timedelta(hours=9, minutes=40)
    assert r.at["B1", "available_at"] == d + pd.Timedelta(hours=10)               # not bar_end (K1)
    assert r.at["B8", "n_expected"] == 5 and r.at["B8", "available_at"] == d + pd.Timedelta(hours=13, minutes=25)
    assert r.at["CLOSE_AUCTION", "available_at"] == d + pd.Timedelta(hours=13, minutes=30)   # K2
    assert r["n_expected"].sum() == 54
    empty = C.resample_30m(b.iloc[0:0], d)
    assert empty["open"].isna().all()                                              # no forward fill


def test_execute_fail_closed_limits_and_auction():
    d = SES[5]
    b = day_bars("2330", d)
    r = {"buy_eligibility": "UNKNOWN", "sell_eligibility": "UNKNOWN", "limit_up": np.nan, "limit_down": np.nan}
    assert C.execute(b, "BUY", d + pd.Timedelta(hours=9, minutes=30), r, pd.Timedelta(hours=13, minutes=20)).status \
        == "INVALIDATED_LIMIT_UNKNOWN"
    assert C.execute(b, "BUY", d, None, pd.Timedelta(hours=13, minutes=20)).status == "INVALIDATED_LIMIT_UNKNOWN"
    ok = {"buy_eligibility": "ELIGIBLE_SUBJECT_TO_LIQUIDITY", "sell_eligibility": "ELIGIBLE_SUBJECT_TO_LIQUIDITY",
          "limit_up": 10_000.0, "limit_down": 1.0}
    f = C.execute(b, "BUY", d + pd.Timedelta(hours=13, minutes=21), ok, pd.Timedelta(hours=13, minutes=20))
    assert f.status == "NO_FILL_NO_EXECUTABLE_BAR"                                  # 13:30 never targeted
    f = C.execute(b, "BUY", d + pd.Timedelta(hours=9, minutes=30), ok, pd.Timedelta(hours=13, minutes=20))
    assert f.status == "FILLED" and f.bar_low <= f.px <= f.bar_high
    at = dict(ok, limit_up=float(b["open"].iloc[6]))
    assert C.execute(b, "BUY", d + pd.Timedelta(hours=9, minutes=30), at,
                     pd.Timedelta(hours=13, minutes=20)).status == "NO_FILL_AT_LIMIT_UP"
    thin = b.assign(volume_raw=5.0)
    assert C.execute(thin, "BUY", d + pd.Timedelta(hours=9, minutes=30), ok,
                     pd.Timedelta(hours=13, minutes=20)).status == "NO_FILL_LIQUIDITY"


def test_selection_is_pit_and_ignores_same_day_mask():
    src = make_src()
    d, prev = SES[16], SES[15]
    mp = src.mask[src.mask["date"] == prev]
    pp = src.panel[src.panel["date"] == prev]
    c = C.select_candidates(pp, mp, d, 10, 1, src.quarantine)
    assert "1101" not in set(c["symbol"])                     # excluded by the PREVIOUS session's mask
    assert "8101" not in set(c["symbol"]) and "00999" not in set(c["symbol"])
    c2 = C.select_candidates(pp, mp, d, 10, 1, src.quarantine)
    assert c.equals(c2)


def test_multi_day_retention_reasons():
    src = make_src()
    r = C.multi_day_retention(src.rules, src.ca, 5, src.quarantine)
    x = r.set_index(["symbol", "date"])["reason"]
    assert x[("4444", SES[17])] == "NON_SUPPORTED_CORPORATE_ACTION"
    assert x[("3333", SES[17])] == "USABLE"                    # SUPPORTED event inside the window
    assert x[("2330", UNK_DAY)] == "ENTRY_LIMIT_UNKNOWN" and x[("2330", SES[7])] == "EXIT_LIMIT_UNKNOWN"
    assert x[("8101", SES[3])] == "RESEARCH_QUARANTINE" and x[("1101", SES[-1])] == "NO_EXIT_SESSION_IN_DATA"


# ------------------------------------------------------------------------------------------ end to end
@pytest.fixture(scope="module")
def gate_result():
    src = make_src()
    pins = {"all_ok": True, "x": {"ok": True}}
    return src, G.gate(src, list(SES[1:30]), 4, 7, pins)


def test_gate_end_to_end_scoped_go(gate_result):
    src, res = gate_result
    assert res["verdict"] == "SCOPED GO", res["checks"]
    assert all(c["passed"] for c in res["checks"].values())
    led = res["ledger"]
    st = set(led["status"])
    assert {"CLOSED", "INVALIDATED_CORPORATE_ACTION"} <= st
    assert not led["symbol"].isin(["8101", "00999"]).any()
    md = led[(led["strategy"] == "MULTI_DAY") & (led["symbol"] == "3333") & (led["status"] == "CLOSED")
             & (led["n_events"] > 0)]
    assert len(md) and np.allclose(md["cash_received"], 1000.0)


def test_gate_detects_lookahead_and_fake_fills(gate_result):
    _, res = gate_result
    led = res["ledger"].copy()
    i = led.index[led["entry_status"] == "FILLED"][0]
    led.loc[i, "entry_time"] = led.loc[i, "signal_available_at"] - pd.Timedelta(minutes=5)
    led.loc[i, "entry_px"] = led.loc[i, "entry_bar_high"] + 1
    chk = check_ledger(led)
    assert not chk["entry_not_before_available_at"][0] and not chk["fill_price_inside_real_bar"][0]


def test_gate_no_go_when_pins_fail():
    src = make_src()
    res = G.gate(src, list(SES[1:6]), 2, 7, {"all_ok": False})
    assert res["verdict"] == "NO-GO"


def test_scope_matrix_and_blockers(gate_result):
    src, res = gate_result
    sc = G.scope_matrix(src, res["probe_ok"])
    assert {"S1", "S2", "S1A", "M1", "M5", "M20", "X1", "X2", "X3", "X4"} == set(sc["scope_id"])
    assert sc.set_index("scope_id").at["X1", "status"] == "EXCLUDED_FAIL_CLOSED"
    s1a = json.loads(sc.set_index("scope_id").at["S1", "top_exclusion_reasons"])
    assert "QUARANTINE_8101" in s1a                                  # 8101 rows labelled as quarantine
    src2 = make_src()
    src2.mask.loc[src2.mask["symbol"] == "2330", "level_A_strict"] = False
    a = json.loads(G.scope_matrix(src2, res["probe_ok"]).set_index("scope_id").at["S1A", "top_exclusion_reasons"])
    assert a.get("price-usable but not Level A strict", 0) > 0 and a.get("QUARANTINE_8101", 0) == 40
    blk = G.blocker_register(res["checks"], {"all_ok": True})
    assert (blk["severity"] != "BLOCKING").all() and len(blk) == len(G.STATIC_BLOCKERS)
    md = G.report_md(res, sc, blk, {"all_ok": True}, {"run_id": "t", "repo_commit": "abcdef0"})
    assert "Verdict: SCOPED GO" in md


def test_factors_known_at_respects_available_at():
    src = make_src()
    assert C.factors_known_at(src.ca, "3333", DIV_DAY + pd.Timedelta(hours=8, minutes=29)).empty
    assert len(C.factors_known_at(src.ca, "3333", DIV_DAY + pd.Timedelta(hours=8, minutes=30))) == 1


def test_run_from_files_writes_all_deliverables(tmp_path, monkeypatch):
    import json
    src = make_src()
    dr = tmp_path / "data"
    lake = dr / "_tw-market-data-lake"
    for key, df, name in (("mask_dir", src.mask.assign(date=src.mask["date"].dt.strftime("%Y-%m-%d")),
                           "quality_mask_symbol_day.parquet"),
                          ("universe_pit", src.panel, "universe_panel.parquet"),
                          ("trading_rules", src.rules, "trading_rules.parquet"),
                          ("corporate_actions_v2", src.ca, "corporate_actions_v2.parquet")):
        d = lake / G.PINS[key]
        d.mkdir(parents=True, exist_ok=True)
        df.to_parquet(d / name, index=False)
    root = dr / "fubon_stock_5m"
    vdir = root / "_manifest" / "versions" / "v1"
    vdir.mkdir(parents=True)
    rows = []
    for s in SYMS:
        b = src.bars(s, 2025).copy()
        b["bar_start"] = b["bar_start"].dt.tz_localize("Asia/Taipei")
        rel = f"data/symbol={s}/year=2025/part.parquet"
        (root / rel).parent.mkdir(parents=True)
        b.to_parquet(root / rel, index=False)
        rows.append({"partition": f"symbol={s}/year=2025", "relpath": rel})
    pd.DataFrame(rows).to_parquet(vdir / "partitions.parquet", index=False)
    from tw_lake.audit.reader import sha256_file
    (vdir / "meta.json").write_text(json.dumps({"tables": {"partitions": {
        "sha256": sha256_file(vdir / "partitions.parquet"), "n_rows": len(rows)}}}))
    (root / "_manifest" / "published.json").write_text(json.dumps({"seq": 68, "version_dir": "versions/v1"}))
    monkeypatch.setattr(G, "verify_pins", lambda lake_, dr_, *a: {"all_ok": True, "fake": {"ok": True}})
    out = G.run(dr, tmp_path / "out", 25, 4, 7, log=lambda *_: None)
    for f in ("FINAL_READINESS_REPORT.md", "SCOPE_MATRIX.csv", "BLOCKER_REGISTER.csv", "run_manifest.json",
              "probe_ledger.parquet", "checks.json"):
        assert (out / f).exists(), f
    m = json.loads((out / "run_manifest.json").read_text(encoding="utf-8"))
    assert m["verdict"] == "SCOPED GO" and "probe_ledger.parquet" in m["outputs_sha256"]
    with pytest.raises(SystemExit):
        G.run(dr, lake / "trading_rules", 5, 2, 7, log=lambda *_: None)
