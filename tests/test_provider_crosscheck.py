"""Provider cross-check on a synthetic lake. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

from datetime import date

import pandas as pd

from conftest import FakeFubon
from test_audit import build_lake, tree_snapshot
from tw_lake.audit.provider_crosscheck import run_crosscheck, verdict


def test_verdict_rules():
    assert verdict(100_000, 100_000, 100_000, 100_000, 100_000) == "consistent"
    assert verdict(100_000, 97_000, 98_000, None, None) == "pipeline_differs"
    assert verdict(100_000, 97_000, 97_000, 100_000, 100_000) == "one_minute_complete"
    assert verdict(100_000, 97_000, 97_000, 97_000, 100_000) == "intraday_short_vs_daily"
    assert verdict(100_000, 97_000, 97_000, 97_000, 97_000) == "provider_feed_short"


def test_crosscheck_reads_stored_and_classifies(tmp_cfg, clock, tmp_path):
    fin, _ = build_lake(tmp_cfg, clock)
    d = "2024-02-20"
    shares = float(fin._frame("price:成交股數").loc[pd.Timestamp(d), "2330"])
    recon = pd.DataFrame([
        {"symbol": "2330", "date": d, "exchange": "TWSE", "complete_day": True, "unit_unverified": False,
         "liquidity_bucket": "500_5000_lots", "official_total": shares + 5000, "official_regular": shares + 3000,
         "m_volume_shares": shares, "residual_shares": 3000.0},
        {"symbol": "0050", "date": "2024-02-21", "exchange": "TWSE", "complete_day": True, "unit_unverified": False,
         "liquidity_bucket": "500_5000_lots", "official_total": 1.0, "official_regular": 1.0,
         "m_volume_shares": 1.0, "residual_shares": 0.0}])
    rd = tmp_path / "recon"
    rd.mkdir()
    recon.to_parquet(rd / "volume_recon_symbol_day.parquet", index=False)
    pd.DataFrame({"exchange": ["TWSE", "TWSE"], "date": [d, "2024-02-21"], "date_verified": [False, True]}) \
        .to_csv(rd / "identity_by_date.csv", index=False)

    fub = FakeFubon(fin)

    class Client:                         # D candle carries the official regular volume, 1m == 5m
        def historical_candles(self, params):
            if params["timeframe"] == "D":
                return {"data": [{"date": d, "volume": (shares + 3000) / 1000}]}
            return fub.historical_candles(dict(params, timeframe="5"))
    before = tree_snapshot(tmp_cfg.data_root)
    out = run_crosscheck(tmp_cfg, rd, tmp_path / "x", Client(), per_date=2, control_per_date=1,
                         sleep=lambda _s: None, log=lambda *_a: None)
    assert tree_snapshot(tmp_cfg.data_root) == before
    r = pd.read_csv(out / "crosscheck.csv", dtype={"symbol": str}).set_index("symbol")
    assert abs(r.loc["2330", "stored_5m_shares"] - shares) < 1e-6
    assert r.loc["2330", "verdict"] == "intraday_short_vs_daily" and r.loc["2330", "api_D_unit"] == "lot"
    assert r.loc["2330", "stratum"] == "unverified_top_residual"
    assert (out / "responses" / f"2330_{d}_D.json").exists()
    assert r.loc["0050", "stratum"] == "verified_control"
