"""Official volume probe + volume-bound reconciliation on synthetic data. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd

from tw_lake.audit.volume_recon import run_recon
from tw_lake.reference import official_volume as OV

D = date(2025, 6, 23)
SYMS = [f"{1100 + i}" for i in range(60)] + ["A", "B", "C", "E", "P", "00636K"]
TOTAL = {s: 1_000_000.0 + i * 1000 for i, s in enumerate(SYMS)}
FIXED = {s: 2000.0 for s in SYMS}
ODD = {s: 345.0 for s in SYMS}
BLOCK = {"1100": 500_000.0}                      # 1100 has a block trade NOT included in the daily total


def fake_get(url, params):
    if url.endswith("MI_INDEX"):
        body = {"stat": "OK", "tables": [{"title": "大盤", "fields": ["指數", "收盤指數"], "data": [["x", "1"]]},
                                         {"title": "每日收盤行情", "fields": ["證券代號", "證券名稱", "成交股數", "成交筆數"],
                                          "data": [[s, "n", f"{TOTAL[s]:,.0f}", "10"] for s in SYMS]}]}
    elif url.endswith("BFT41U"):
        body = {"stat": "OK", "fields": ["證券代號", "證券名稱", "成交股數"], "data": [[s, "n", f"{FIXED[s]:,.0f}"] for s in SYMS]}
    elif url.endswith("TWT53U"):
        body = {"stat": "OK", "fields": ["證券代號", "證券名稱", "成交股數"], "data": [[s, "n", f"{ODD[s]:,.0f}"] for s in SYMS]}
    elif url.endswith("BFIAUU"):
        body = {"stat": "OK", "fields": ["證券代號", "證券名稱", "交易別", "成交股數"],
                "data": [[s, "n", "配對", f"{v:,.0f}"] for s, v in BLOCK.items()]}
    else:
        return 404, b"<html>not found</html>", "text/html", url
    return 200, json.dumps(body, ensure_ascii=False).encode(), "application/json", url


def ledger(tmp_path):
    rows = []
    for s in SYMS:
        regular = TOTAL[s] - FIXED[s] - ODD[s]
        missing, m5, status = 0, regular, "all_expected_slots_real"
        if s == "A":                    # missing bars, nothing lost -> candidate no-trade
            missing, m5, status = 5, regular, "has_unresolved_slots"
        if s == "B":                    # 3,000 shares lost somewhere -> day-level evidence
            missing, m5, status = 4, regular - 3000, "has_unresolved_slots"
        if s == "C":                    # stored more than official -> excess
            missing, m5, status = 2, regular + 2000, "has_unresolved_slots"
        if s == "E":                    # 600 shares short (< 1 lot) -> candidate
            missing, m5, status = 3, regular - 600, "has_unresolved_slots"
        if s == "P":                    # whole day absent at the provider
            missing, m5, status = 54, 0.0, "provider_missing_evidenced"
        if s == "00636K":               # F13: unit not verified
            missing, m5, status = 1, regular * 10, "has_unresolved_slots"
        rows.append({"symbol": s, "date": pd.Timestamp(D), "year": 2025, "expected_policy": "normal_template",
                     "liquidity_bucket": "500_5000_lots", "expected_slots": 54, "real_bars": 54 - missing,
                     "missing_slots": missing, "m_volume_shares": m5, "f_volume_shares": TOTAL[s] + 1000.0,
                     "day_status": status})
    d = tmp_path / "ledger"
    d.mkdir()
    pd.DataFrame(rows).to_parquet(d / "gap_day_summary.parquet", index=False)
    return d


def test_probe_saves_evidence_and_parses(tmp_path):
    out = OV.probe([D], tmp_path / "probe", getter=fake_get, sleep=lambda _s: None, log=lambda *_a: None)
    rq = pd.read_parquet(out / "requests.parquet")
    assert set(rq["report"]) == {e.report for e in OV.ENDPOINTS}
    ok = rq.set_index("report")
    assert ok.loc["MI_INDEX_ALLBUT0999", "http_status"] == 200 and ok.loc["MI_INDEX_ALLBUT0999", "parsed_rows"] > 0
    assert ok.loc["fixedPrice", "http_status"] == 404 and ok.loc["fixedPrice", "parsed_rows"] == 0   # finding, no crash
    for r in rq.itertuples():
        raw = (out / r.raw_file).read_bytes()
        import hashlib
        assert hashlib.sha256(raw).hexdigest() == r.sha256 and r.known_at.startswith("2025-06-23T20:00")
    pv = pd.read_parquet(out / "parsed_volume.parquet")
    t = pv[(pv["report"] == "MI_INDEX_ALLBUT0999") & (pv["field"] == "成交股數")].set_index("symbol")
    assert t.loc["1100", "shares"] == 1_000_000.0 and t["table"].eq("每日收盤行情").all()
    assert OV.unit_multiplier("成交數量(千股)") == 1000.0


def test_recon_identity_and_classes(tmp_path):
    probe = OV.probe([D], tmp_path / "probe", getter=fake_get, sleep=lambda _s: None, log=lambda *_a: None)
    out = run_recon(probe, ledger(tmp_path), tmp_path / "recon")
    s = json.loads((out / "volume_recon_summary.json").read_text(encoding="utf-8"))
    idn = s["identity"]["TWSE"]
    assert idn["verified"] and set(idn["subtract"]) == {"rep_BFT41U", "rep_TWT53U"}   # block not part of the total
    f = [r for r in s["finlab_vs_official_on_complete_days"] if r["exchange"] == "TWSE"][0]
    assert f["exact_pct"] == 0.0 and f["residual_p50"] == 1000.0                       # FinLab definition differs
    r = pd.read_parquet(out / "volume_recon_symbol_day.parquet").set_index("symbol")["volume_bound_class"]
    assert r["A"] == "CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND"
    assert r["E"] == "CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND"
    assert r["B"] == "DAY_LEVEL_MISSING_VOLUME_EVIDENCED"
    assert r["C"] == "VOLUME_EXCESS_UNEXPLAINED"
    assert r["P"] == "DAY_LEVEL_MISSING_VOLUME_EVIDENCED"
    assert r["00636K"] == "UNIT_UNVERIFIED_EXCLUDED"
    m = s["missing_days"]
    assert m["explained_symbol_days"] == 4 and m["explained_missing_slots_candidate_no_trade"] == 8
    total_regular_p = TOTAL["P"] - FIXED["P"] - ODD["P"]
    assert np.isclose(m["unlocatable_missing_shares_total"], 3000 + total_regular_p)
    assert s["complete_days"]["exact_pct"] == 100.0
