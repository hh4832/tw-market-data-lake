"""Official daily table parser (archived MI_INDEX / dailyQuotes). SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import gzip
import json

import numpy as np
import pandas as pd

from tw_lake.reference import official_daily as OD

MI = {"stat": "OK", "tables": [
    {"title": "價格指數", "fields": ["指數", "收盤指數"], "data": [["發行量加權股價指數", "20000"]]},
    {"title": "每日收盤行情", "fields": ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價",
                                     "收盤價", "漲跌(+/-)", "漲跌價差", "最後揭示買價"],
     "data": [["2330", "台積電", "30,000,000", "1", "1", "1000", "1010", "990", "1,005.00",
               "<p style= color:red>+</p>", "5.00", "1"],
              ["2317", "鴻海", "1,000", "1", "1", "200", "200", "195", "196.00", "<p style= color:green>-</p>", "4.00", "1"],
              ["1598", "岱宇", "500", "1", "1", "22", "22", "22", "22.30", "<p>X</p>", "0.00", "1"],
              ["9999", "無成交", "0", "0", "0", "--", "--", "--", "--", "<p> </p>", "0.00", "--"]]}]}
TP = {"stat": "ok", "tables": [{"fields": ["代號", "名稱", "收盤 ", "漲跌", "開盤 ", "成交股數  ", "次日參考價 ", "次日漲停價 ",
                                           "次日跌停價"],
                                "data": [["6488", "環球晶", "500.00", "+5.00", "495", "100,000", "500.00", "550.00", "450.00"],
                                         ["6791", "虎門", "111.00", "除息", "111", "2,000", "111.00", "122.00", "100.00"]]}]}


def test_parse_twse_signs_and_reference():
    d = OD.parse_daily(json.dumps(MI, ensure_ascii=False).encode(), "TWSE").set_index("symbol")
    assert d.at["2330", "reference_price"] == 1000.0 and d.at["2317", "reference_price"] == 200.0
    assert bool(d.at["1598", "not_compared"]) and np.isnan(d.at["1598", "reference_price"])
    assert d.at["9999", "volume_shares"] == 0 and np.isnan(d.at["9999", "close"])
    assert d.at["2330", "volume_shares"] == 30_000_000


def test_parse_tpex_next_day_limits():
    d = OD.parse_daily(json.dumps(TP, ensure_ascii=False).encode(), "TPEX").set_index("symbol")
    assert d.at["6488", "reference_price"] == 495.0 and d.at["6488", "next_limit_up"] == 550.0
    assert bool(d.at["6791", "not_compared"]) and np.isnan(d.at["6791", "change"])
    assert d.at["6791", "next_limit_down"] == 100.0


def test_load_from_archived_shards(tmp_path):
    sd = tmp_path / "shards" / "year=2025" / "date=2025-06-02"
    (sd / "raw" / "TWSE").mkdir(parents=True)
    (sd / "raw" / "TPEX").mkdir(parents=True)
    (sd / "raw" / "TWSE" / "mi.json.gz").write_bytes(gzip.compress(json.dumps(MI, ensure_ascii=False).encode()))
    (sd / "raw" / "TPEX" / "dq.json.gz").write_bytes(gzip.compress(json.dumps(TP, ensure_ascii=False).encode()))
    pd.DataFrame([{"report": "MI_INDEX_ALLBUT0999", "http_status": 200, "raw_file": "raw/TWSE/mi.json.gz", "sha256": "a"},
                  {"report": "dailyQuotes_EW", "http_status": 200, "raw_file": "raw/TPEX/dq.json.gz", "sha256": "b"}]) \
        .to_parquet(sd / "requests.parquet")
    (sd / "_SUCCESS").write_text("{}")
    out = OD.load_official_daily(tmp_path, log=lambda *_: None)
    assert set(out["exchange"]) == {"TWSE", "TPEX"} and len(out) == 6
    assert out["date"].iloc[0] == pd.Timestamp("2025-06-02")
