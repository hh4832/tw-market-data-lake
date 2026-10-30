"""Corporate-event / listing / limit audit (B3, B4, B5). SYNTHETIC_SMOKE_ONLY except the limit cases, which are
official values copied from the 2026-10-30 fetch (TWT49U / TPEx exDailyQ / TWTCAU) as known answers."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from tw_lake.audit import corporate_events as C
from tw_lake.reference import corporate_actions as CA


@pytest.mark.parametrize("v,exp", [("112年05月17日", "2023-05-17"), ("114/06/18", "2025-06-18"), ("1120710", "2023-07-10"),
                                   ("112-02-10", "2023-02-10"), ("100.01.03", "2011-01-03"), ("1994/09/05", "1994-09-05"),
                                   ("20230517", "2023-05-17")])
def test_parse_tw_date(v, exp):
    assert C.parse_tw_date(v) == pd.Timestamp(exp)


def test_parse_tw_date_rejects_garbage():
    assert C.parse_tw_date("--") is None and C.parse_tw_date("") is None


@pytest.mark.parametrize("sym,ref,up,dn", [
    ("1598", 22.20, 24.40, 20.00),        # TWT49U 2025-06-02
    ("2405", 15.63, 17.15, 14.10),        # limits from the (unrounded) reference price, not the opening reference
    ("3443", 1441.00, 1585.00, 1300.00),  # tick 5 above 1,000
    ("6791", 111.02, 122.00, 100.00),     # TPEx: limit down crosses into the 0.5 band
    ("01004T", 13.65, 15.01, 12.29),      # REIT: 0.01 tick
    ("0050", 47.16, 51.85, 42.45),        # ETF >= 50 / < 50 ticks
    ("00663L", 24.30, 29.16, 19.44),      # leveraged 2x ETF: 20 %
])
def test_limit_prices_known_answers(sym, ref, up, dn):
    assert C.limit_prices(ref, C.tick_for(sym), C.pct_for(sym)) == (up, dn)


def test_tick_tables():
    assert [C.tick_stock(p) for p in (9.99, 10, 49.95, 50, 99.9, 100, 499.5, 500, 999, 1000)] == \
        [0.01, 0.05, 0.05, 0.1, 0.1, 0.5, 0.5, 1.0, 1.0, 5.0]
    assert [C.tick_etf(p) for p in (49.99, 50.0)] == [0.01, 0.05]
    assert C.is_fund_like("00878") and C.is_fund_like("01004T") and C.is_fund_like("020035")
    assert not C.is_fund_like("2330") and not C.is_fund_like("1312A")


def _wide():
    t49 = pd.DataFrame({"period": "2025-06", "table": "0", "row": [0, 1, 2], "sha256": "s",
                        "資料日期": ["114年06月02日", "114年06月03日", "114年06月04日"],
                        "股票代號": ["1598", "4737", "00679B"], "股票名稱": ["岱宇", "華廣", "元大美債20年"],
                        "除權息前收盤價": ["22.70", "55.00", "30.00"], "除權息參考價": ["22.20", "54.35", "29.50"],
                        "權值+息值": ["0.500000", "0.65", "0.50"], "權/息": ["息", "權", "息"],
                        "漲停價格": ["24.40", "60.50", "9999.95"], "跌停價格": ["20.00", "48.95", "0.01"],
                        "開盤競價基準": ["22.20", "55.00", "29.50"], "減除股利參考價": ["22.20", "54.35", "29.50"]})
    tcau = pd.DataFrame({"period": "2025-06", "table": "0", "row": [0, 1], "sha256": "s",
                         "恢復買賣日期": ["114/06/18", "115/03/31"], "ETF代號": ["0050", "00631L"],
                         "名稱": ["元大台灣50", "元大台灣50正2"], "分割(反分割)": ["分割", ""],
                         "停止買賣前收盤價格": ["188.65", "443.15"], "恢復買賣參考價": ["47.16", "20.14"],
                         "漲停價格": ["51.85", "24.16"], "跌停價格": ["42.45", "16.12"], "開盤競價基準": ["47.16", "20.14"]})
    delist = pd.DataFrame({"period": "snapshot_2026-10-30", "table": "0", "row": [0, 1], "sha256": "s",
                           "終止上市日期": ["115/09/01", "113/03/15"], "公司名稱": ["三商壽", "X"],
                           "上市編號": ["2867", "9999"]})
    newl = pd.DataFrame({"period": "snapshot_2026-10-30", "table": "0", "row": [0], "sha256": "s",
                         "公司代號": ["7867"], "公司簡稱": ["特力屋"], "股票上市買賣日期": ["115.10.30"], "承銷價": ["44.06"]})
    return {"TWSE_TWT49U": t49, "TWSE_TWTCAU": tcau, "TWSE_suspendListing": delist, "TWSE_newlisting": newl}


def test_build_events_types_factor_and_known_at():
    ev = C.build_events(_wide()).set_index("symbol")
    assert ev.at["1598", "event_type"] == "cash_dividend" and ev.at["1598", "cash_dividend"] == 0.5
    assert ev.at["4737", "event_type"] == "stock_dividend" and np.isnan(ev.at["4737", "cash_dividend"])
    assert ev.at["0050", "event_type"] == "etf_split" and ev.at["0050", "price_factor"] == pytest.approx(47.16 / 188.65)
    assert ev.at["00631L", "event_type"] == "etf_split"                    # blank type: inferred from the ratio
    assert ev.at["0050", "known_at"] == "2025-06-18T08:30:00+08:00"
    assert set(ev["report"]) == {"TWSE_TWT49U", "TWSE_TWTCAU"}


def test_check_limits_on_known_events():
    lc = C.check_limits(C.build_events(_wide())).set_index("symbol")
    assert bool(lc.at["00679B", "no_price_limit"]) and not bool(lc.at["00679B", "has_official_limits"])
    assert lc.loc[lc["has_official_limits"], "limit_rule_match"].all()
    assert lc.at["4737", "limit_up_basis"] == "open_ref" and lc.at["1598", "limit_up_basis"] == "ref_price"


def test_listings_and_lake_checks():
    w = _wide()
    li = C.build_listings(w)
    assert set(li["event"]) == {"delisted", "listed"}
    raw = pd.DataFrame({"symbol": ["2867"] * 3 + ["7867"] + ["9999"] * 2 + ["1111"] * 2,
                        "date": pd.to_datetime(["2026-08-28", "2026-08-31", "2026-09-01", "2026-10-30",
                                                "2024-03-14", "2024-03-20", "2024-01-02", "2024-01-03"]),
                        "close": [10, 10, 10, 50, 5, 5, 8, 8], "high": 1.0, "low": 1.0})
    lk = C.check_listings(li, raw).set_index("symbol")
    assert lk.at["2867", "status"] == "PRICE_ON_OR_AFTER_DELISTING"           # a price on the delisting date
    assert lk.at["9999", "status"] == "PRICE_ON_OR_AFTER_DELISTING"
    assert lk.at["7867", "status"] == "OK_FIRST_PRICE_ON_OR_AFTER_LISTING"
    inf = C.inferred_without_official(raw, li, pd.Timestamp("2026-10-30")).set_index("symbol")
    assert not inf.at["1111", "official_delisting"] and inf.at["9999", "official_delisting"]


def test_prev_close_and_event_day_limits():
    ev = C.build_events(_wide())
    raw = pd.DataFrame({"symbol": ["1598", "1598", "4737", "4737"],
                        "date": pd.to_datetime(["2025-05-30", "2025-06-02", "2025-06-02", "2025-06-03"]),
                        "close": [22.70, 22.5, 54.0, 55.0], "high": [23.0, 24.40, 54.0, 61.0],
                        "low": [22.5, 21.0, 54.0, 50.0]})
    pc = C.check_prev_close(ev, raw).set_index("symbol")
    assert pc.at["1598", "prev_close_match"] == "TRUE" and pc.at["1598", "event_day_within_limits"] == "TRUE"
    assert pc.at["4737", "prev_close_match"] == "FALSE"                        # FinLab 54.0 vs official 55.00
    assert pc.at["4737", "event_day_within_limits"] == "FALSE"                 # high 61 > limit 60.5


def test_adjusted_jumps_match_events():
    raw = pd.DataFrame({"symbol": "1598", "date": pd.to_datetime(["2025-05-29", "2025-05-30", "2025-06-02"]),
                        "close": [22.8, 22.7, 22.3]})
    adj = raw.assign(adj_close=[22.8 * 0.978, 22.7 * 0.978, 22.3])[["symbol", "date", "adj_close"]]
    j = C.adjusted_jumps(raw, adj)
    assert list(j["jump_date"]) == [pd.Timestamp("2025-06-02")]
    e, j2 = C.match_jumps(C.build_events(_wide()), j)
    assert bool(e.set_index("symbol").at["1598", "finlab_adjusted"]) and j2["official_event"].all()


def test_ordinary_day_limits():
    ev = C.build_events(_wide())
    li = C.build_listings(_wide())
    raw = pd.DataFrame({"symbol": ["2330"] * 3 + ["00679B"] * 2,
                        "date": pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-02", "2025-01-03"]),
                        "close": [100.0, 110.0, 121.5, 30.0, 40.0], "high": [100.0, 110.0, 121.5, 30.0, 40.0],
                        "low": [100.0, 99.0, 110.0, 30.0, 30.0]})
    summ, viol = C.normal_day_limits(raw, ev, li)
    assert list(viol["date"]) == [pd.Timestamp("2025-01-06")]                 # 121.5 > 121.0 (110 x 1.1)
    assert int(summ.loc[summ["class"] == "no_limit_symbol", "symbol_days"].iloc[0]) == 1


def test_run_offline_from_reference_dataset(tmp_path):
    root = tmp_path / "ref"
    body = {"stat": "OK", "fields": list(_wide()["TWSE_TWT49U"].columns[4:]),
            "data": _wide()["TWSE_TWT49U"].iloc[:, 4:].values.tolist()}

    def get(url, params, *_):
        return 200, json.dumps(body, ensure_ascii=False).encode(), "application/json", url
    CA.fetch(root, tmp_path / "w", ["TWSE_TWT49U"], pd.Timestamp("2025-06-01").date(),
             pd.Timestamp("2025-06-30").date(), getter=get, sleep=lambda *_: None, log=lambda *_: None,
             today=pd.Timestamp("2026-10-30").date())
    out = C.run(root, tmp_path / "out", log=lambda *_: None)
    s = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    assert s["limit_rule_match_overall"] == 1.0 and s["no_price_limit_events"] == 1
    assert (out / "corporate_events.parquet").exists() and (out / "run_info.json").exists()
