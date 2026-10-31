"""B3a corporate actions v2 - known-answer scenarios K-B3-1 .. K-B3-10 (SYNTHETIC; the oracle below is test-only).

Oracle: a holder of `shares` with `cash` keeps wealth through an event when
    shares x prev_close + cash == shares' x ref_price + cash'   with cash' = cash + shares x cash_per_share,
                                                                    shares' = shares x (1 + stock_per_share)
It may only be applied to SUPPORTED events (PRICE_ONLY / UNSUPPORTED must refuse)."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from tw_lake.audit import corporate_actions_v2 as V
from tw_lake.audit import corporate_events as C
from tw_lake.audit.quality_mask import QUARANTINE_FROZEN_V1
from tw_lake.reference import corporate_actions as CA

T = pd.Timestamp


def _ev(rows):
    base = {c: np.nan for c in C.EVENT_COLUMNS}
    out = []
    for r in rows:
        x = dict(base, detail="", known_at="", report="R", period="p", sha256="sha_" + r["symbol"], name="n")
        x.update(r)
        x["event_date"] = T(x["event_date"])
        out.append(x)
    return pd.DataFrame(out)[C.EVENT_COLUMNS]


def _wide_sub(symbol, date, shares, price):
    return {"TPEX_exDailyQ_b": pd.DataFrame({"代號": [symbol], "除權息日期": [date], "現金增資股數": [str(shares)],
                                             "現金增資認購價": [str(price)], "按持股比例仟股認購": ["0"]})}


def _cal(*days):
    return pd.DatetimeIndex(pd.to_datetime(list(days)))


def oracle(shares, cash, e):
    if e["accounting_status"] != "SUPPORTED":
        raise ValueError("position accounting refused: " + e["accounting_status"])
    return shares * (1 + (e["stock_per_share"] if pd.notna(e["stock_per_share"]) else 0)), \
        cash + shares * (e["cash_per_share"] if pd.notna(e["cash_per_share"]) else 0)


def _build(rows, cal, raw=None, wide=None):
    return V.build_v2(_ev(rows), wide or {}, cal, raw, {"sha_" + r["symbol"]: "2026-10-30T14:00:00+08:00" for r in rows})


def test_k_b3_1_cash_dividend_wealth_preserved():
    v = _build([dict(exchange="TWSE", symbol="1111", event_type="cash_dividend", event_date="2025-06-02",
                     prev_close=100.0, ref_price=97.0, cash_dividend=3.0)], _cal("2025-05-30", "2025-06-02")).iloc[0]
    assert v["accounting_status"] == "SUPPORTED" and v["price_factor"] == pytest.approx(0.97)
    s, c = oracle(1000, 0.0, v)
    assert s * v["ref_price"] + c == pytest.approx(1000 * 100.0)
    assert v["retrieved_at"] == "2026-10-30T14:00:00+08:00" and v["source_published_at"] is None
    assert v["available_at"] == "2025-06-02T08:30:00+08:00"
    assert "not observed" in v["available_at_policy"]


def test_k_b3_2_tpex_stock_dividend_10pct():
    v = _build([dict(exchange="TPEX", symbol="2222", event_type="stock_dividend", event_date="2025-07-01",
                     prev_close=110.0, ref_price=100.0, cash_dividend=0.0, stock_per_1000=100.0)],
               _cal("2025-07-01")).iloc[0]
    assert v["accounting_status"] == "SUPPORTED" and v["stock_per_share"] == pytest.approx(0.1)
    s, c = oracle(1000, 0.0, v)
    assert s == pytest.approx(1100) and s * v["ref_price"] + c == pytest.approx(1000 * 110.0)


def test_k_b3_3_twse_stock_dividend_price_only_blocks_holding():
    v = _build([dict(exchange="TWSE", symbol="3333", event_type="stock_and_cash_dividend", event_date="2025-08-01",
                     prev_close=50.0, ref_price=44.0)], _cal("2025-07-31", "2025-08-01"))
    assert v.iloc[0]["accounting_status"] == "PRICE_ONLY"
    with pytest.raises(ValueError):
        oracle(1000, 0.0, v.iloc[0])
    ok, why = V.holding_allowed(v, "3333", T("2025-07-01"), T("2025-08-15"))
    assert not ok and "PRICE_ONLY" in why
    assert V.holding_allowed(v, "3333", T("2025-08-01"), T("2025-08-15"))[0]        # bought on/after the session


def test_k_b3_4_etf_split_1_to_4_signal_continuity():
    v = _build([dict(exchange="TWSE", symbol="0050", event_type="etf_split", event_date="2025-06-18",
                     prev_close=188.65, ref_price=47.16)], _cal("2025-06-10", "2025-06-18"))
    e = v.iloc[0]
    assert e["accounting_status"] == "PRICE_ONLY" and e["price_factor"] == pytest.approx(0.25, abs=1e-3)
    p = V.signal_adjusted(pd.DataFrame({"date": pd.to_datetime(["2025-06-10", "2025-06-18"]),
                                        "raw": [188.65, 47.16]}), v)
    assert p["signal_adjusted_price"].iloc[0] == pytest.approx(47.16) and p["cum_factor"].iloc[1] == 1.0


def test_k_b3_5_reverse_split():
    v = _build([dict(exchange="TWSE", symbol="00672L", event_type="etf_reverse_split", event_date="2025-09-01",
                     prev_close=5.0, ref_price=20.0)], _cal("2025-08-29", "2025-09-01"))
    e = v.iloc[0]
    assert e["accounting_status"] == "PRICE_ONLY" and e["price_factor"] == pytest.approx(4.0)
    p = V.signal_adjusted(pd.DataFrame({"date": pd.to_datetime(["2025-08-29", "2025-09-01"]), "raw": [5.0, 20.0]}), v)
    assert list(p["signal_adjusted_price"]) == [20.0, 20.0]


def test_k_b3_6_8101_research_quarantine_separate_from_b2():
    q = V.RESEARCH_QUARANTINE[0]
    assert (q["symbol"], q["start"], q["end"]) == ("8101", "2024-08-22", "2024-12-31")
    assert all("8101" not in map(str, k) for k in QUARANTINE_FROZEN_V1)                 # B2 list is exchange-date
    ev = _build([dict(exchange="TWSE", symbol="8101", event_type="cash_dividend", event_date="2024-09-02",
                      prev_close=2.0, ref_price=1.9, cash_dividend=0.1)], _cal("2024-09-02"))
    assert bool(ev.iloc[0]["in_research_quarantine"])
    ok, why = V.holding_allowed(ev.iloc[0:0], "8101", T("2024-11-01"), T("2024-11-30"))
    assert not ok and "quarantine" in why
    assert V.holding_allowed(ev.iloc[0:0], "8101", T("2025-01-02"), T("2025-02-01"))[0]
    jumps = pd.DataFrame({"symbol": ["8101"], "jump_date": [T("2024-11-19")], "implied_factor": [5.0]})
    _, orphan = V.diagnostics(ev, jumps, T("2023-05-01"))
    assert len(orphan) == 1 and bool(orphan.iloc[0]["in_research_quarantine"])


def test_k_b3_7_closure_shift_and_first_observed_trade():
    cal = _cal("2024-07-23", "2024-07-26", "2024-07-29")          # 07-24 / 07-25 typhoon closures
    raw = pd.DataFrame({"symbol": "4444", "date": pd.to_datetime(["2024-07-23", "2024-07-26", "2024-07-29"]),
                        "close": [10.0, np.nan, 9.6], "volume_shares": [1000, 0, 2000]})
    v = _build([dict(exchange="TPEX", symbol="4444", event_type="cash_dividend", event_date="2024-07-25",
                     prev_close=10.0, ref_price=9.5, cash_dividend=0.5)], cal, raw=raw)
    e = v.iloc[0]
    assert e["official_effective_date"] == T("2024-07-25")                               # never moved
    assert e["effective_market_session"] == T("2024-07-26") and bool(e["session_shifted"])
    assert e["first_observed_trade_date"] == T("2024-07-29")
    jumps = pd.DataFrame({"symbol": ["4444"], "jump_date": [T("2024-07-29")], "implied_factor": [0.95]})
    d, orphan = V.diagnostics(v, jumps, T("2023-05-01"))
    assert d.iloc[0]["timing_status"] == "MATCH_FIRST_OBSERVED_TRADE" and len(orphan) == 0
    assert d.iloc[0]["factor_rel_err"] == pytest.approx(0.0)


def test_k_b3_8_no_time_travel():
    v = _build([dict(exchange="TWSE", symbol="5555", event_type="cash_dividend", event_date="2025-06-02",
                     prev_close=100.0, ref_price=95.0, cash_dividend=5.0)], _cal("2025-05-30", "2025-06-02"))
    px = pd.DataFrame({"date": pd.to_datetime(["2025-05-30", "2025-06-02"]), "raw": [100.0, 95.0]})
    before = V.signal_adjusted(px, v, as_of=T("2025-06-02 08:29"))
    after = V.signal_adjusted(px, v, as_of=T("2025-06-02 08:30"))
    assert before["signal_adjusted_price"].iloc[0] == 100.0 and after["signal_adjusted_price"].iloc[0] == 95.0
    assert V.signal_adjusted(px, v, as_of=T("2025-05-30 13:30"))["cum_factor"].eq(1.0).all()


def test_k_b3_9_rights_and_mixed_never_supported():
    rows = [dict(exchange="TPEX", symbol="6666", event_type="stock_and_cash_dividend", event_date="2025-07-01",
                 prev_close=50.0, ref_price=47.0, cash_dividend=1.0, stock_per_1000=50.0),
            dict(exchange="TPEX", symbol="6667", event_type="stock_dividend", event_date="2025-07-01",
                 prev_close=50.0, ref_price=46.0, cash_dividend=0.0),                       # stock ratio missing
            dict(exchange="TWSE", symbol="6668", event_type="stock_dividend", event_date="2025-07-01",
                 prev_close=50.0, ref_price=51.0)]                                          # ref > prev: subscription
    wide = _wide_sub("6666", "114/07/01", 120000, 30.0)
    v = _build(rows, _cal("2025-07-01"), wide=wide).set_index("symbol")
    assert v.at["6666", "accounting_status"] == "UNSUPPORTED" and v.at["6666", "subscription_shares"] == 120000
    assert v.at["6667", "accounting_status"] == "PRICE_ONLY"
    assert v.at["6668", "accounting_status"] == "UNSUPPORTED"
    assert (v["accounting_status"] != "SUPPORTED").all()


def test_k_b3_10_cumulative_factor_order_and_unsessioned_events():
    rows = [dict(exchange="TWSE", symbol="7777", event_type="cash_dividend", event_date="2025-03-03",
                 prev_close=100.0, ref_price=98.0, cash_dividend=2.0),
            dict(exchange="TWSE", symbol="7777", event_type="capital_reduction", event_date="2025-06-02",
                 prev_close=50.0, ref_price=100.0),
            dict(exchange="TWSE", symbol="7777", event_type="cash_dividend", event_date="2026-12-01",
                 prev_close=100.0, ref_price=99.0, cash_dividend=1.0)]                      # after calendar end
    v = _build(rows, _cal("2025-02-28", "2025-03-03", "2025-05-30", "2025-06-02"))
    assert v.iloc[2]["accounting_status"] == "UNSUPPORTED" and pd.isna(v.iloc[2]["effective_market_session"])
    assert v.iloc[1]["accounting_status"] == "PRICE_ONLY"
    px = pd.DataFrame({"date": pd.to_datetime(["2025-02-28", "2025-03-03", "2025-05-30", "2025-06-02"]),
                       "raw": [100.0, 98.0, 50.0, 100.0]})
    p = V.signal_adjusted(px, v)
    assert list(p["cum_factor"].round(6)) == [round(0.98 * 2.0, 6), 2.0, 2.0, 1.0]
    assert v["event_id"].is_unique


def test_run_offline_writes_manifest(tmp_path):
    t49 = pd.DataFrame({"資料日期": ["114年06月02日", "114年06月03日"], "股票代號": ["1598", "4737"],
                        "股票名稱": ["岱宇", "華廣"], "除權息前收盤價": ["22.70", "55.00"],
                        "除權息參考價": ["22.20", "54.35"], "權值+息值": ["0.500000", "0.65"], "權/息": ["息", "權"],
                        "漲停價格": ["24.40", "60.50"], "跌停價格": ["20.00", "48.95"],
                        "開盤競價基準": ["22.20", "55.00"], "減除股利參考價": ["22.20", "54.35"]})
    body = {"stat": "OK", "fields": list(t49.columns), "data": t49.values.tolist()}

    def get(url, params, *_):
        return 200, json.dumps(body, ensure_ascii=False).encode(), "application/json", url
    root = tmp_path / "ref"
    CA.fetch(root, tmp_path / "w", ["TWSE_TWT49U"], T("2025-06-01").date(), T("2025-06-30").date(), getter=get,
             sleep=lambda *_: None, log=lambda *_: None, today=T("2026-10-30").date())
    out = V.run(root, tmp_path / "derived", log=lambda *_: None)
    v = pd.read_parquet(out / "corporate_actions_v2.parquet").set_index("symbol")
    assert v.at["1598", "accounting_status"] == "SUPPORTED" and v.at["4737", "accounting_status"] == "PRICE_ONLY"
    assert v["retrieved_at"].notna().all()
    m = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert set(m["sha256"]) >= {"corporate_actions_v2.parquet", "research_quarantine.csv", "summary.json"}
    with pytest.raises(SystemExit):
        V.run(root, tmp_path / "quality_acceptance", log=lambda *_: None)


def test_finlab_late_adjustment_with_matching_factor_is_classified():
    cal = _cal("2025-07-09", "2025-07-10", "2025-07-11")
    raw = pd.DataFrame({"symbol": "8905", "date": cal, "close": [10.0, 9.5, 9.6], "volume_shares": [1e3, 1e3, 1e3]})
    v = _build([dict(exchange="TPEX", symbol="8905", event_type="cash_dividend", event_date="2025-07-10",
                     prev_close=10.0, ref_price=9.5, cash_dividend=0.5)], cal, raw=raw)
    late = pd.DataFrame({"symbol": ["8905"], "jump_date": [T("2025-07-11")], "implied_factor": [0.95]})
    d, _ = V.diagnostics(v, late, T("2023-05-01"))
    assert d.iloc[0]["timing_status"] == "FINLAB_LATE_ADJUSTMENT_FACTOR_MATCH" and d.iloc[0]["finlab_lag_days"] == 1
    off = late.assign(implied_factor=0.90)                                   # factor disagrees: stays unexplained
    assert V.diagnostics(v, off, T("2023-05-01"))[0].iloc[0]["timing_status"] == "UNEXPLAINED_TIMING"
