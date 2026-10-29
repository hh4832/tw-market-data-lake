"""Eligibility v0.1 on synthetic reference shards + reconciliation. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd

from tw_lake.audit import eligibility as E
from tw_lake.reference import official_volume_batch as B
from tests.test_official_volume_batch import getter_factory
from tests.test_volume_recon import D, ledger

D2, D3, D4 = date(2025, 6, 24), date(2025, 6, 25), date(2025, 6, 26)
noop = lambda *_a, **_k: None  # noqa: E731


def build(tmp_path):
    led = ledger(tmp_path)
    d = pd.read_parquet(led / "gap_day_summary.parquet")
    frames = [d]
    for dd in (D2, D3, D4):
        x = d.copy()
        x["date"] = pd.Timestamp(dd)
        if dd == D2:      # 10 complete days short by 2 lots: no over-count, not verified
            x.loc[x["symbol"].isin([f"{1100 + i}" for i in range(10)]), "m_volume_shares"] -= 2000
        if dd == D4:      # one complete day stores MORE than official: over-count on this date
            x.loc[x["symbol"] == "1101", "m_volume_shares"] += 1000
            x.loc[x["symbol"].isin([f"{1102 + i}" for i in range(10)]), "m_volume_shares"] -= 1000
        frames.append(x)
    pd.concat(frames).to_parquet(led / "gap_day_summary.parquet", index=False)
    root = tmp_path / "ref" / "v1"
    B.fetch(led, root, tmp_path / "work", getter=getter_factory(), sleep=noop, log=noop)
    recon = B.recon(led, root, tmp_path / "recon")
    return led, root, recon


def test_eligibility_end_to_end(tmp_path):
    led, root, recon = build(tmp_path)
    out = E.run(led, recon, root, tmp_path / "elig", log=noop, sample_frac=1.0)
    p1 = json.loads((out / "phase1_reproduce.json").read_text(encoding="utf-8"))
    assert p1["ok"], p1["issues"]
    q = pd.read_parquet(out / "exchange_date_quality.parquet").set_index("date")["identity_status"]
    assert q["2025-06-23"] == "VERIFIED" and q["2025-06-24"] == "NO_OVERCOUNT"
    assert q["2025-06-26"] == "OVERCOUNT_PRESENT"
    s = pd.read_parquet(out / "symbol_day_quality.parquet").set_index(["symbol", "date"])
    c = s["missing_bar_evidence_status"]
    assert c[("A", "2025-06-24")] == E.NO_TRADE                 # exact residual, no over-count date
    assert c[("E", "2025-06-24")] == E.NO_TRADE                 # 600 shares < 1 lot: hidden board lots must be 0
    assert c[("B", "2025-06-24")] == E.DAY_LEVEL                # whole lots short, reference complete
    assert c[("C", "2025-06-23")] == "UNRESOLVED_VOLUME_EXCESS"
    assert c[("A", "2025-06-26")] == "UNRESOLVED_OVERCOUNT_EXCHANGE_DATE"
    assert s.loc[("A", "2025-06-24"), "evidence_status"] == "CANDIDATE"   # never VERIFIED
    # PIT: the first date has no previous trading day
    assert not s.loc[("1105", "2025-06-23"), "pit_eligible"] and s.loc[("1105", "2025-06-25"), "pit_eligible"]
    assert s.loc[("1105", "2025-06-25"), "eligible_level_A"]
    assert not s.loc[("1105", "2025-06-24"), "eligible_level_A"]         # date not VERIFIED
    assert s.loc[("1115", "2025-06-24"), "eligible_level_B"]
    assert not s.loc[("1100", "2025-06-24"), "eligible_level_B"]         # 2 lots short: volume not reliable
    assert s.loc[("1100", "2025-06-24"), "intraday_volume_shortfall"] == 2000
    assert s.loc[("A", "2025-06-24"), "eligible_level_B"] and not s.loc[("A", "2025-06-25"), "eligible_level_A"]
    assert (s["price_reliable"] == "UNKNOWN").all()
    m = json.loads((out / "backtest_eligibility_manifest.json").read_text(encoding="utf-8"))
    assert m["row_counts"]["level_A"] == int(s["eligible_level_A"].sum())
    assert set(m["sha256"]) == {"exchange_date_quality.parquet", "symbol_day_quality.parquet"}
    raw = pd.read_csv(out / "raw_hash_check.csv")
    assert raw["sha_ok"].all()


def test_reproduce_mismatch_stops(tmp_path):
    led, root, recon = build(tmp_path)
    sp = recon / "volume_recon_summary.json"
    s = json.loads(sp.read_text(encoding="utf-8"))
    s["missing_days"]["by_class"][0]["missing_slots"] += 1
    sp.write_text(json.dumps(s), encoding="utf-8")
    out = E.run(led, recon, root, tmp_path / "elig", log=noop)
    assert not (out / "symbol_day_quality.parquet").exists()
    assert json.loads((out / "phase1_reproduce.json").read_text(encoding="utf-8"))["ok"] is False
