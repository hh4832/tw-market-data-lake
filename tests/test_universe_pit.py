"""B5a PIT universe reconciliation - known-answer scenarios K-B5-1 .. K-B5-4 (SYNTHETIC)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from tw_lake.audit import corporate_events as C
from tw_lake.audit import universe_pit as U
from tw_lake.reference.official_daily import COLUMNS, security_type

T = pd.Timestamp
DAYS = list(pd.to_datetime(["2026-03-25", "2026-03-26", "2026-03-27", "2026-03-30", "2026-03-31"]))


def _od(rows):
    base = {c: np.nan for c in COLUMNS}
    return pd.DataFrame([dict(base, name="n", not_compared=False, source_report="S", raw_sha256="h", **r)
                         for r in rows])[COLUMNS]


def _anchor():
    return [dict(exchange=ex, symbol=s, date=d, volume_shares=1e6, close=100.0)
            for ex, s in (("TWSE", "2330"), ("TPEX", "6488")) for d in DAYS]


def _raw(rows):
    return pd.DataFrame(rows, columns=["symbol", "date", "close", "volume_shares"])


def _li(rows):
    base = dict(name="n", detail="", report="R", period="p", sha256="s")
    return pd.DataFrame([dict(base, **r) for r in rows], columns=C.LISTING_COLUMNS) if rows \
        else pd.DataFrame(columns=C.LISTING_COLUMNS)


NO_HALT = pd.DataFrame(columns=["symbol", "date", "halt_source"])


def _raw_anchor():
    return [(s, d, 100.0, 1e6) for s in ("2330", "6488") for d in DAYS]


def test_k_b5_1_delisting_last_price_separate_and_never_liquidated():
    od = _od(_anchor() + [dict(exchange="TWSE", symbol="3454", date=d, volume_shares=1e5, close=20.0) for d in DAYS[:2]])
    raw = _raw(_raw_anchor() + [("3454", d, 20.0, 1e5) for d in DAYS[:2]])
    li = _li([dict(exchange="TWSE", symbol="3454", event="delisted", date=DAYS[3])])
    p, tr = U.build_panel(od, raw, li, NO_HALT)
    sym = U.symbol_table(p, li, tr, DAYS[-1], stop_gap_sessions=2).set_index("symbol")
    assert sym.at["3454", "last_observed_price_date"] == DAYS[1]
    assert sym.at["3454", "official_delisting_date"] == DAYS[3]                  # kept separately
    assert sym.at["3454", "termination_status"] == "DELISTED_OFFICIAL"
    assert U.valuation_status(p, "3454", DAYS[2]) == "UNPRICED_NO_SOURCE"         # not valued at 20.0
    assert U.valuation_status(p, "3454", DAYS[4]) == "DELISTED_NO_EXIT_PRICE"
    assert "liquidat" not in " ".join(p.columns)


def test_k_b5_2_price_after_official_delisting_is_flagged():
    od = _od(_anchor() + [dict(exchange="TWSE", symbol="3454", date=d, volume_shares=1e5, close=20.0) for d in DAYS[:2]])
    raw = _raw(_raw_anchor() + [("3454", d, 20.0, 1e5) for d in DAYS[:2]] + [("3454", DAYS[3], 19.0, 1e3)])
    li = _li([dict(exchange="TWSE", symbol="3454", event="delisted", date=DAYS[2])])
    p, tr = U.build_panel(od, raw, li, NO_HALT)
    x = p.set_index(["symbol", "date"]).loc[("3454", DAYS[3])]
    assert x["coverage_class"] == "PRICE_AFTER_OFFICIAL_DELISTING" and bool(x["delisted_official"])
    sym = U.symbol_table(p, li, tr, DAYS[-1]).set_index("symbol")
    assert sym.at["3454", "price_after_delisting_days"] == 1
    assert U.valuation_status(p, "3454", DAYS[3]) == "DELISTED_NO_EXIT_PRICE"


def test_k_b5_3_board_transfer_continuous_membership():
    od = _od(_anchor()
             + [dict(exchange="TPEX", symbol="6423", date=d, volume_shares=1e5, close=50.0) for d in DAYS[:2]]
             + [dict(exchange="TWSE", symbol="6423", date=d, volume_shares=1e5, close=51.0) for d in DAYS[2:]])
    raw = _raw(_raw_anchor() + [("6423", d, 50.0, 1e5) for d in DAYS])
    li = _li([dict(exchange="TPEX", symbol="6423", event="delisted", date=DAYS[2]),
              dict(exchange="TWSE", symbol="6423", event="listed", date=DAYS[2])])
    p, tr = U.build_panel(od, raw, li, NO_HALT)
    x = p[p["symbol"] == "6423"].set_index("date")
    assert list(x["exchange"]) == ["TPEX", "TPEX", "TWSE", "TWSE", "TWSE"]
    assert (x["coverage_class"] == "OK_TRADED_PRICED").all() and not x["delisted_official"].any()
    sym = U.symbol_table(p, li, tr, DAYS[-1]).set_index("symbol")
    assert sym.at["6423", "termination_status"] == "ACTIVE" and sym.at["6423", "transfer_dates"] == "2026-03-27"
    assert sym.at["6423", "exchanges"] == "TPEX,TWSE" and len(tr) == 1


def test_k_b5_4_missing_symbol_days_classified():
    od = _od(_anchor()
             + [dict(exchange="TWSE", symbol="1101", date=DAYS[0], volume_shares=1e5, close=30.0),
                dict(exchange="TWSE", symbol="1101", date=DAYS[1], volume_shares=0),            # listed, no trade
                dict(exchange="TWSE", symbol="1101", date=DAYS[2], volume_shares=0),            # halted
                dict(exchange="TWSE", symbol="1101", date=DAYS[3], volume_shares=2e5, close=31.0),  # price missing
                dict(exchange="TWSE", symbol="00999", date=DAYS[3], volume_shares=1e4, close=10.0)])
    raw = _raw(_raw_anchor() + [("1101", DAYS[0], 30.0, 1e5), ("9999", DAYS[1], 5.0, 1e3)])
    halts = pd.DataFrame({"symbol": ["1101"], "date": [DAYS[2]], "halt_source": ["TWSE_TWTAWU"]})
    p, tr = U.build_panel(od, raw, _li([]), halts)
    x = p.set_index(["symbol", "date"])["coverage_class"]
    assert x[("1101", DAYS[0])] == "OK_TRADED_PRICED"
    assert x[("1101", DAYS[1])] == "OK_NO_TRADE_NO_PRICE"
    assert x[("1101", DAYS[2])] == "HALTED_NO_PRICE"
    assert x[("1101", DAYS[3])] == "OFFICIAL_TRADED_PRICE_MISSING"
    assert x[("9999", DAYS[1])] == "PRICE_WITHOUT_OFFICIAL_ROW"
    sym = U.symbol_table(p, _li([]), tr, DAYS[-1])
    cov = U.coverage(p, sym)
    etf = cov[cov["security_type"] == "ETF"]
    assert etf["finlab_priced"].sum() == 0 and etf["official_traded_days"].sum() == 1   # broken down by type
    assert {"year", "exchange", "liquidity_bucket", "termination_status"} <= set(cov.columns)


def test_halt_days_from_official_intervals():
    cal = pd.DatetimeIndex(DAYS)
    wide = {"TWSE_TWTAWU": pd.DataFrame({"證券代號": ["2429"], "暫停交易日期": ["115/03/26"],
                                         "恢復交易日期": ["115/03/30"]})}
    h = U.halt_days(wide, cal)
    assert list(h["date"]) == [DAYS[1], DAYS[2]]


def test_security_types():
    assert [security_type(s) for s in ("2330", "0050", "00679B", "020020", "01001T", "910322", "1312A", "X")] == \
        ["STOCK", "ETF", "ETF", "ETN", "REIT", "TDR", "PREFERRED_OR_SPECIAL", "OTHER"]


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
    days = [d.strftime("%Y-%m-%d") for d in DAYS[:3]]
    write_official_shards(tmp_path / "ov", days)
    (tmp_path / "ca").mkdir()
    out = U.run(tmp_path / "ov", tmp_path / "ca", tmp_path / "audits", log=lambda *_: None)
    p = pd.read_parquet(out / "universe_panel.parquet")
    assert len(p) == 9 and set(p["coverage_class"]) == {"OFFICIAL_TRADED_PRICE_MISSING"}   # no FinLab given
    s = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert s["sessions"]["n"] == 3 and "not evidence" in s["statement"]
    assert "universe_symbols.parquet" in json.loads((out / "manifest.json").read_text(encoding="utf-8"))["sha256"]
