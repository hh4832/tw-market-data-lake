"""B4a trading rules - known-answer scenarios K-B4-1 .. K-B4-6 (SYNTHETIC)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tw_lake.audit import corporate_events as C
from tw_lake.audit import trading_rules as R
from tw_lake.reference.official_daily import COLUMNS

T = pd.Timestamp
DAYS = ["2025-03-03", "2025-03-04", "2025-03-05", "2025-03-06"]


def _od(rows):
    base = {c: np.nan for c in COLUMNS}
    out = []
    for r in rows:
        x = dict(base, name="n", not_compared=False, source_report="S", raw_sha256="h")
        x.update(r)
        x["exchange"] = x.get("exchange") if isinstance(x.get("exchange"), str) else "TWSE"
        x["date"] = T(x["date"])
        out.append(x)
    return pd.DataFrame(out)[COLUMNS]


def _anchor():          # a liquid symbol trading every day defines the market calendar
    return [dict(symbol="2330", date=d, volume_shares=1e6, close=1000.0) for d in DAYS]


def _ev(rows=()):
    base = {c: np.nan for c in C.EVENT_COLUMNS}
    return pd.DataFrame([dict(base, **{**r, "event_date": T(r["event_date"])}) for r in rows], columns=C.EVENT_COLUMNS)


NO_LI = pd.DataFrame(columns=C.LISTING_COLUMNS)


def _rules(rows, ev=(), li=NO_LI, raw=None):
    return R.build_rules(_od(_anchor() + rows), _ev(ev), li, raw).set_index(["symbol", "date"])


def test_k_b4_1_derived_stock_limits_from_previous_official_close():
    r = _rules([dict(symbol="1101", date=DAYS[0], volume_shares=5e5, close=47.3),
                dict(symbol="1101", date=DAYS[1], volume_shares=5e5, close=50.0, change=2.7)])
    x = r.loc[("1101", T(DAYS[1]))]
    assert x["price_limit_status"] == "DERIVED" and x["reference_source"] == "PREV_SESSION_OFFICIAL_CLOSE"
    assert (x["limit_up"], x["limit_down"]) == (52.0, 42.6)                    # 52.03 -> 0.05 tick; 42.57 -> 42.6
    assert r.loc[("1101", T(DAYS[0])), "price_limit_status"] == "UNKNOWN_NO_REFERENCE"   # first row: no change


def test_k_b4_2_no_previous_price_uses_official_next_day_limits():
    rows = [dict(exchange="TPEX", symbol="6488", date=DAYS[0], volume_shares=1e5, close=500.0),
            dict(exchange="TPEX", symbol="6488", date=DAYS[1], volume_shares=0, next_reference=495.0,
                 next_limit_up=544.0, next_limit_down=446.0),
            dict(exchange="TPEX", symbol="6488", date=DAYS[2], volume_shares=1e5, close=520.0)]
    x = _rules(rows).loc[("6488", T(DAYS[2]))]
    assert x["price_limit_status"] == "VERIFIED_OFFICIAL_NEXT_DAY" and x["reference_source"] == "OFFICIAL_NEXT_DAY"
    assert (x["limit_up"], x["limit_down"], x["reference_price"]) == (544.0, 446.0, 495.0)


def test_k_b4_3_no_previous_price_no_official_limits_is_unknown_not_forward_filled():
    rows = [dict(symbol="1102", date=DAYS[0], volume_shares=1e5, close=30.0),
            dict(symbol="1102", date=DAYS[1], volume_shares=0),                     # listed, no trade
            dict(symbol="1102", date=DAYS[2], volume_shares=1e5, close=31.0)]       # change unknown (no compare)
    r = _rules(rows)
    x = r.loc[("1102", T(DAYS[2]))]
    assert x["price_limit_status"] == "UNKNOWN_NO_REFERENCE" and np.isnan(x["limit_up"])   # 30.0 NOT reused
    assert x["buy_eligibility"] == "UNKNOWN" and x["sell_eligibility"] == "UNKNOWN"
    # with an official change on day t the reference is official (close - change), not the older close
    rows[2].update(change=0.5, reference_price=30.5)                         # as parse_daily reconstructs it
    y = _rules(rows).loc[("1102", T(DAYS[2]))]
    assert y["reference_source"] == "OFFICIAL_CLOSE_MINUS_CHANGE" and y["price_limit_status"] == "DERIVED"
    assert (y["limit_up"], y["limit_down"]) == (33.55, 27.45)


def test_k_b4_4_fund_like_classification_fails_closed():
    rows = [dict(symbol=s, date=d, volume_shares=1e5, close=20.0) for s in ("00999", "00998", "00997") for d in DAYS[:2]]
    ev = [dict(exchange="TWSE", symbol="00998", event_type="cash_dividend", event_date=DAYS[0], prev_close=20.5,
               ref_price=20.0, limit_up=9999.95, limit_down=0.01),
          dict(exchange="TWSE", symbol="00997", event_type="cash_dividend", event_date=DAYS[0], prev_close=20.5,
               ref_price=20.0, limit_up=22.0, limit_down=18.0)]
    r = _rules(rows, ev=ev)
    assert r.loc[("00999", T(DAYS[1])), "price_limit_status"] == "UNKNOWN_INSTRUMENT_CLASS"
    assert r.loc[("00999", T(DAYS[1])), "buy_eligibility"] == "UNKNOWN"
    assert r.loc[("00998", T(DAYS[0])), "price_limit_status"] == "NO_LIMIT_VERIFIED"
    assert r.loc[("00998", T(DAYS[1])), "price_limit_status"] == "NO_LIMIT_OFFICIAL_CLASS"
    assert r.loc[("00997", T(DAYS[0])), "price_limit_status"] == "VERIFIED_OFFICIAL_EVENT"
    assert r.loc[("00997", T(DAYS[1])), "price_limit_status"] == "DERIVED"
    assert r.loc[("00997", T(DAYS[1])), "limit_up"] == 22.0


def test_k_b4_5_limit_validity_separate_from_execution_eligibility():
    rows = [dict(symbol="1103", date=DAYS[0], volume_shares=1e5, close=100.0),
            dict(symbol="1103", date=DAYS[1], volume_shares=2e3, close=110.0, change=10.0),
            dict(symbol="1103", date=DAYS[2], volume_shares=1e5, close=115.0, change=5.0),
            dict(symbol="1103", date=DAYS[3], volume_shares=1e5, close=140.0, change=25.0)]
    raw = pd.DataFrame({"symbol": "1103", "date": pd.to_datetime(DAYS), "high": [100.0, 110.0, 121.0, 140.0],
                        "low": [99.0, 110.0, 112.0, 130.0]})
    r = _rules(rows, raw=raw)
    locked = r.loc[("1103", T(DAYS[1]))]
    assert locked["price_limit_status"] == "DERIVED" and locked["limit_consistent"] == "TRUE"
    assert locked["buy_eligibility"] == "BLOCKED_LOCKED_LIMIT_UP"
    assert locked["sell_eligibility"] == "ELIGIBLE_SUBJECT_TO_LIQUIDITY"
    touched = r.loc[("1103", T(DAYS[2]))]
    assert touched["buy_eligibility"] == "CONDITIONAL_LIMIT_TOUCHED"
    conflict = r.loc[("1103", T(DAYS[3]))]                                 # 140 > 126.5: band contradicted
    assert conflict["price_limit_status"] == "DERIVED_CONFLICT" and conflict["buy_eligibility"] == "UNKNOWN"


def test_k_b4_6_closing_signal_never_executes_in_same_auction():
    cal = pd.DatetimeIndex(pd.to_datetime(DAYS))
    for t in ("2025-03-04 13:25", "2025-03-04 13:30", "2025-03-04 13:33"):
        ts, how = R.earliest_execution(T(t), cal)
        assert ts == T("2025-03-05 09:00") and how == "OPENING_AUCTION_NEXT_SESSION"
    ts, how = R.earliest_execution(T("2025-03-04 10:00"), cal)
    assert ts == T("2025-03-04 10:00") and how.endswith("NOT_GUARANTEED")
    assert R.earliest_execution(T("2025-03-06 14:00"), cal)[1] == "NO_FUTURE_SESSION"


def test_new_listing_first_five_sessions_no_limit():
    li = pd.DataFrame([{"exchange": "TWSE", "symbol": "7867", "name": "x", "event": "listed", "date": T(DAYS[1]),
                        "detail": "", "report": "R", "period": "p", "sha256": "s"}])
    rows = [dict(symbol="7867", date=d, volume_shares=1e5, close=44.0, change=1.0) for d in DAYS[1:]]
    r = _rules(rows, li=li)
    assert (r.xs("7867")["price_limit_status"] == "NO_LIMIT_NEW_LISTING").all()


def test_counts_and_validation():
    rows = [dict(symbol="1101", date=DAYS[0], volume_shares=5e5, close=47.3),
            dict(symbol="1101", date=DAYS[1], volume_shares=5e5, close=50.0, change=2.7)]
    ev = [dict(exchange="TWSE", symbol="1101", event_type="cash_dividend", event_date=DAYS[1], prev_close=47.3,
               ref_price=47.0, limit_up=51.7, limit_down=42.3)]
    rules = R.build_rules(_od(_anchor() + rows), _ev(ev), NO_LI)
    c = R.counts(rules)
    assert c["symbol_days"].sum() == len(rules) and {"year", "exchange", "liquidity_bucket"} <= set(c.columns)
    v = R.validation(rules)
    assert v["VERIFIED_OFFICIAL_EVENT"]["n"] == 1 and v["VERIFIED_OFFICIAL_EVENT"]["both_match"] == 1.0


def write_official_shards(root, days):
    """Archived official_volume-style shards (MI_INDEX / dailyQuotes raw gzip + requests.parquet + _SUCCESS)."""
    import gzip
    import json
    for i, d in enumerate(days):
        sd = root / "shards" / f"year={d[:4]}" / f"date={d}"
        (sd / "raw").mkdir(parents=True)
        mi = {"stat": "OK", "tables": [{"fields": ["證券代號", "證券名稱", "成交股數", "收盤價", "漲跌(+/-)", "漲跌價差"],
                                        "data": [["2330", "a", "1,000,000", f"{1000 + 10 * i:.2f}", "<p>+</p>", "10.00"],
                                                 ["0050", "b", "5,000", "180.00", "<p> </p>", "0.00"]]}]}
        tp = {"stat": "ok", "tables": [{"fields": ["代號", "名稱", "收盤", "漲跌", "成交股數"],
                                        "data": [["6488", "c", "500.00", "+5.00", "100,000"]]}]}
        (sd / "raw" / "mi.json.gz").write_bytes(gzip.compress(json.dumps(mi, ensure_ascii=False).encode()))
        (sd / "raw" / "dq.json.gz").write_bytes(gzip.compress(json.dumps(tp, ensure_ascii=False).encode()))
        pd.DataFrame([{"report": "MI_INDEX_ALLBUT0999", "http_status": 200, "raw_file": "raw/mi.json.gz", "sha256": "a"},
                      {"report": "dailyQuotes_EW", "http_status": 200, "raw_file": "raw/dq.json.gz", "sha256": "b"}]) \
            .to_parquet(sd / "requests.parquet")
        (sd / "_SUCCESS").write_text("{}")


def test_run_offline_from_archived_official_tables(tmp_path):
    import json
    write_official_shards(tmp_path / "ov", DAYS[:3])
    (tmp_path / "ca").mkdir()
    out = R.run(tmp_path / "ov", tmp_path / "ca", tmp_path / "derived", log=lambda *_: None)
    rules = pd.read_parquet(out / "trading_rules.parquet")
    assert len(rules) == 9
    st = rules.set_index(["symbol", "date"])["price_limit_status"]
    assert st[("2330", T(DAYS[1]))] == "DERIVED" and st[("0050", T(DAYS[1]))] == "UNKNOWN_INSTRUMENT_CLASS"
    m = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert {"trading_rules.parquet", "status_counts.csv", "execution_semantics.json"} <= set(m["sha256"])
    with pytest.raises(SystemExit):
        R.run(tmp_path / "ov", tmp_path / "ca", tmp_path / "quality_acceptance", log=lambda *_: None)


def test_vectorised_limits_equal_scalar_rule():
    rng = np.random.default_rng(0)
    refs = np.concatenate([rng.uniform(0.5, 2000, 4000), [9.99, 10, 45.45, 50, 99.95, 100, 499.5, 500, 999, 1000]])
    syms = pd.Series(np.resize(["2330", "0050", "00631L", "00679B", "1312A"], len(refs)))
    up, dn = R.limit_prices_vec(refs, syms)
    exp = [C.limit_prices(r, C.tick_for(s), C.pct_for(s)) for r, s in zip(refs, syms)]
    assert np.allclose(up, [e[0] for e in exp]) and np.allclose(dn, [e[1] for e in exp])
    u, d = R.limit_prices_vec([np.nan, 0.0], pd.Series(["2330", "2330"]))
    assert np.isnan(u).all() and np.isnan(d).all()


def test_scope_filter_drops_warrants():
    from tw_lake.reference.official_daily import lake_scope
    f = lake_scope({"999999"})
    m = f(pd.Series(["2330", "030001", "70123P", "00679B", "999999"]))
    assert list(m) == [True, False, False, True, True]
