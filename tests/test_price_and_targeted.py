"""Official price check + targeted follow-up checks. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from conftest import FakeFubon
from test_audit import build_lake, tree_snapshot
from tw_lake.audit import price_check as PC
from tw_lake.audit import targeted_checks as T

noop = lambda *_a, **_k: None  # noqa: E731


def test_parse_official_ohlc_twse_and_tpex():
    twse = {"tables": [{"title": "指數", "fields": ["指數", "收盤指數"], "data": [["x", "1"]]},
                       {"title": "每日收盤行情", "fields": ["證券代號", "證券名稱", "成交股數", "開盤價", "最高價", "最低價",
                                                         "收盤價", "漲跌(+/-)"],
                        "data": [["2330", "台積電", "1,000", "1,000.00", "1,010.00", "995.00", "1,005.00",
                                  "<p style= color:red>+</p>"], ["9999", "x", "0", "--", "--", "--", "--", ""]]}]}
    t = PC.parse_official_ohlc(json.dumps(twse, ensure_ascii=False).encode()).set_index("symbol")
    assert t.loc["2330", "close"] == 1005.0 and t.loc["2330", "high"] == 1010.0 and np.isnan(t.loc["9999", "open"])
    tpex = {"tables": [{"fields": ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"],
                        "data": [["8299", "群聯", "500.50", "+1", "498.00", "502.00", "497.50", "1,000"]]}]}
    t2 = PC.parse_official_ohlc(json.dumps(tpex, ensure_ascii=False).encode()).set_index("symbol")
    assert t2.loc["8299", "open"] == 498.0 and t2.loc["8299", "close"] == 500.5


def test_price_check_compare_and_resume(tmp_cfg, clock, tmp_path):
    build_lake(tmp_cfg, clock)
    before = tree_snapshot(tmp_cfg.data_root)
    chunks = tmp_path / "chunks"
    bars = PC.stored_prices(tmp_cfg.data_root, {"2330"}, None, chunks, workers=2, log=noop)
    assert len(bars) > 5 and set(bars["symbol"]) == {"2330"}
    again = PC.stored_prices(tmp_cfg.data_root, {"2330"}, None, chunks, workers=2, log=noop)   # resume: no re-read
    assert len(again) == len(bars)
    assert tree_snapshot(tmp_cfg.data_root) == before
    d0, d1, d2 = bars["date"].iloc[0], bars["date"].iloc[1], bars["date"].iloc[2]
    b = bars.set_index("date")
    off = pd.DataFrame([
        {"symbol": "2330", "date": d0, "exchange": "TWSE", "official_open": b.at[d0, "bar_open"],
         "official_high": b.at[d0, "bar_high"], "official_low": b.at[d0, "bar_low"], "official_close": b.at[d0, "bar_close"]},
        {"symbol": "2330", "date": d1, "exchange": "TWSE", "official_open": b.at[d1, "bar_open"],
         "official_high": b.at[d1, "bar_high"] + 0.5, "official_low": b.at[d1, "bar_low"],
         "official_close": b.at[d1, "bar_close"]},
        {"symbol": "2330", "date": d2, "exchange": "TWSE", "official_open": np.nan, "official_high": np.nan,
         "official_low": np.nan, "official_close": np.nan}])
    x = PC.compare(bars, off).set_index("date")
    assert x.at[d0, "price_reliable"] == "TRUE"
    assert x.at[d1, "price_reliable"] == "FALSE" and x.at[d1, "price_mismatch"] == "high"
    assert x.at[d2, "price_reliable"] == "UNKNOWN"


def _recon_with_overcount(fin, tmp_path, d):
    shares = float(fin._frame("price:成交股數").loc[pd.Timestamp(d), "2330"])
    rows = []
    for dd, res in (("2024-02-19", 2000.0), (d, -2000.0), ("2024-02-21", 0.0)):
        rows.append({"symbol": "2330", "date": dd, "exchange": "TWSE", "complete_day": True, "unit_unverified": False,
                     "liquidity_bucket": "ge5000_lots", "official_total": shares - res + 3000,
                     "official_regular": shares + res, "m_volume_shares": shares, "residual_shares": res,
                     "finlab_vs_official": 0.0, "rep_BFT41U": 3000.0, "rep_TWTC7U": 2000.0})
    rd = tmp_path / "recon"
    rd.mkdir()
    pd.DataFrame(rows).to_parquet(rd / "volume_recon_symbol_day.parquet", index=False)
    return rd, shares


def test_overcount_and_fubon_targeted(tmp_cfg, clock, tmp_path):
    fin, _ = build_lake(tmp_cfg, clock)
    d = "2024-02-20"
    rd, shares = _recon_with_overcount(fin, tmp_path, d)
    before = tree_snapshot(tmp_cfg.data_root)
    out = T.run_overcount(tmp_cfg.data_root, rd, None, tmp_path / "t", log=noop)
    o = pd.read_parquet(out / "overcount_symbol_days.parquet")
    assert len(o) == 1 and o["date"].iloc[0] == d
    r = o.iloc[0]
    assert r["excess_shares"] == 2000 and r["excess_whole_lots"] and r["excess_equals_component"] == "TWTC7U"
    assert r["opposite_residual_adjacent_day"] == "previous_day" and r["n_bars"] > 0
    assert json.loads((out / "run_info.json").read_text(encoding="utf-8"))["status"] == "done"
    fub = FakeFubon(fin)

    class Client:
        def historical_candles(self, params):
            if params["timeframe"] == "D":
                return {"data": [{"date": d, "volume": (shares + 1000) / 1000}]}
            return fub.historical_candles(dict(params, timeframe="5"))
    fo = T.run_fubon(tmp_cfg.data_root, out, tmp_path / "f", client=Client(), log=noop)
    v = pd.read_csv(fo / "overcount_fubon_verdicts.csv")
    assert v["overcount_verdict"].iloc[0] in {"provider_intraday_exceeds_official_regular_daily_matches", "other",
                                               "provider_daily_exceeds_official_total"}
    assert tree_snapshot(tmp_cfg.data_root) == before


def test_official_refetch_compares_without_touching_shards(tmp_path):
    from tests.test_official_volume_batch import _ledger_two_days, getter_factory
    from tw_lake.reference import official_volume_batch as B
    led = _ledger_two_days(tmp_path)
    root = tmp_path / "ref" / "v1"
    B.fetch(led, root, tmp_path / "work", getter=getter_factory(), sleep=noop, log=noop)
    snap = sorted((p.relative_to(root), p.stat().st_size) for p in root.rglob("*") if p.is_file())
    el = tmp_path / "elig"
    el.mkdir()
    pd.DataFrame([{"exchange": "TWSE", "date": "2025-06-23", "report": "BFT41U"}]).to_csv(el / "refetch_minimal.csv",
                                                                                         index=False)
    out = T.run_official(el, root, tmp_path / "o", getter=getter_factory(), sleep=noop, log=noop)
    c = pd.read_csv(out / "official_refetch_compare.csv")
    assert c["same_bytes"].iloc[0] and c["symbols_value_differs"].iloc[0] == 0
    assert snap == sorted((p.relative_to(root), p.stat().st_size) for p in root.rglob("*") if p.is_file())
