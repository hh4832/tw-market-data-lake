"""Final quality mask on the synthetic eligibility fixture. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from tw_lake.audit import eligibility as E
from tw_lake.audit import quality_mask as Q
from tests.test_eligibility import build, noop


def _price(sdq, overrides):
    p = sdq[["symbol", "date"]].copy()
    for c in ("open_match", "high_match", "low_match", "close_match"):
        p[c] = 1.0
    for (sym, d), vals in overrides.items():
        m = (p["symbol"] == sym) & (p["date"] == d)
        for c, v in vals.items():
            p.loc[m, c] = v
    return p


def test_quality_mask_rules(tmp_path):
    led, root, recon = build(tmp_path)
    el = E.run(led, recon, root, tmp_path / "elig", log=noop)
    sdq = pd.read_parquet(el / "symbol_day_quality.parquet")
    pc = tmp_path / "pc"
    pc.mkdir()
    _price(sdq, {("E", "2025-06-24"): {"high_match": 0.0},           # no-trade candidate but range differs
                 ("1110", "2025-06-25"): {"open_match": 0.0},         # complete day, open tick difference
                 ("1111", "2025-06-25"): {"open_match": np.nan, "high_match": np.nan, "low_match": np.nan,
                                          "close_match": np.nan}}) \
        .to_parquet(pc / "price_check_symbol_day.parquet", index=False)
    out = Q.run(led, el, tmp_path / "q", price_check_dir=pc, log=noop)
    m = pd.read_parquet(out / "quality_mask_symbol_day.parquet").set_index(["symbol", "date"])
    g = m["gap_evidence_status"]
    assert g[("A", "2025-06-24")] == Q.ACCEPTED                     # residual 0, range matches, no over-count
    assert g[("E", "2025-06-24")] == Q.NO_TRADE                     # range mismatch: stays CANDIDATE
    assert g[("B", "2025-06-24")] == Q.DAY_LEVEL
    assert g[("A", "2025-06-26")] == "UNRESOLVED_QUARANTINED_EXCHANGE_DATE"
    # quarantined date: complete symbols are not upgraded
    assert not m.loc[("1115", "2025-06-26"), "can_use_for_price_backtest"]
    assert m.loc[("1115", "2025-06-26"), "volume_reconciliation_status"] == "QUARANTINED_DATE"
    # UNKNOWN never counts as TRUE
    assert m.loc[("1111", "2025-06-25"), "price_quality_status"] == "UNKNOWN"
    assert not m.loc[("1111", "2025-06-25"), "can_use_for_price_backtest"]
    assert m.loc[("1110", "2025-06-25"), "price_quality_status"] == "RANGE_ONLY"
    assert not m.loc[("1110", "2025-06-25"), "level_A_strict"]
    assert m.loc[("1112", "2025-06-25"), "level_A_strict"] and m.loc[("1112", "2025-06-25"), "volume_strict"]
    # volume dimension
    assert m.loc[("1100", "2025-06-24"), "volume_reconciliation_status"] == "SHORTFALL_WHOLE_LOTS"
    assert m.loc[("1100", "2025-06-24"), "can_use_for_price_backtest"]
    assert not m.loc[("1100", "2025-06-24"), "can_use_for_volume_backtest"]
    assert abs(m.loc[("1100", "2025-06-24"), "intraday_volume_shortfall_ratio"]
               - 2000 / m.loc[("1100", "2025-06-24"), "official_regular_volume"]) < 1e-12
    assert m.loc[("1101", "2025-06-26"), "intraday_volume_overcount"] == 1000
    assert (m["evidence_version"].str.startswith("quality_v")).all()
    q = pd.read_csv(out / "quarantine_exchange_dates.csv")
    assert set(q["date"]) == {"2025-06-26"}
    man = json.loads((out / "quality_manifest.json").read_text(encoding="utf-8"))
    assert "quality_mask_symbol_day.parquet" in man["sha256"]
    prem = json.loads((out / "acceptance_rule_v2.json").read_text(encoding="utf-8"))
    assert "P3_no_overcount_in_stored_bars" in prem and "statement" in prem


def test_reference_fix_uses_refetch_evidence():
    edq = pd.DataFrame([{"exchange": "TWSE", "date": "2025-04-10", "reference_completeness": "INCOMPLETE",
                         "incomplete_reports": "BFT41U"},
                        {"exchange": "TWSE", "date": "2025-04-11", "reference_completeness": "INCOMPLETE",
                         "incomplete_reports": "BFT41U"}])
    ref = pd.DataFrame([{"date": "2025-04-10", "report": "BFT41U", "new_http": 200, "symbols_value_differs": 0,
                         "old_symbols": 7105, "new_symbols": 7105}])
    fixed, log = Q.reference_fix(edq, ref)
    # M6: a consistent re-fetch is REFETCH_CONSISTENT, never REFERENCE_COMPLETE
    assert list(fixed["reference_completeness"]) == [Q.REFETCH_CONSISTENT, "INCOMPLETE"]
    assert len(log) == 1 and log.iloc[0]["evidence_status"] == Q.REFETCH_CONSISTENT
    assert not bool(log.iloc[0]["reference_complete"])
