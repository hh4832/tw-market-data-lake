"""Rule v2 counterexamples and B2 semantic guards (GPT review 14.2 / 14.8 M1-M6). SYNTHETIC_SMOKE_ONLY.

The counterexample tests do NOT assert that rule v2 is right. They pin down cases where a symbol-day that
really traded in a missing bar is still accepted (r = 0, official high/low unchanged), so the contract can
never claim per-bar proof. If the rule is tightened later these tests must be changed deliberately under a
new quality version.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tw_lake.audit import quality_mask as Q

LOT = 1000
DOCS = Path(__file__).resolve().parents[1] / "docs"


# ---------------------------------------------------------------- tiny market simulator
def day(true_trades, stored_bars, components_true=0, components_measured=None, slots=54):
    """true_trades: list of (slot, price, shares) that really happened in the regular session.
    stored_bars: dict slot -> (open, high, low, close, shares) as stored by the provider.
    Returns the per-symbol-day inputs the mask sees: residual, missing bars, official OHLC vs stored OHLC."""
    components_measured = components_true if components_measured is None else components_measured
    official_regular = sum(q for _, _, q in true_trades)
    official_total = official_regular + components_true
    stored = sum(b[4] for b in stored_bars.values())
    r = official_total - components_measured - stored
    tr = sorted(true_trades)
    off = dict(open=tr[0][1], high=max(p for _, p, _ in tr), low=min(p for _, p, _ in tr), close=tr[-1][1])
    sb = [stored_bars[k] for k in sorted(stored_bars)]
    st = dict(open=sb[0][0], high=max(b[1] for b in sb), low=min(b[2] for b in sb), close=sb[-1][3])
    return {"residual": r, "missing": slots - len(stored_bars), "real": len(stored_bars),
            "official_total": official_total, "official_regular": official_regular, "stored": stored,
            **{f"{k}_match": float(off[k] == st[k]) for k in ("open", "high", "low", "close")}}


def mask_of(cases: dict[str, dict], exchange="TWSE", date="2025-06-23", identity="VERIFIED", session=None,
            unit=None, ref="COMPLETE"):
    rows, price = [], []
    for sym, c in cases.items():
        rows.append({"symbol": sym, "exchange": exchange, "date": date, "identity_status": identity,
                     "residual_shares": c["residual"], "missing_bar_count": c["missing"],
                     "real_bar_count": c["real"], "official_total": c["official_total"],
                     "official_regular_volume": c["official_regular"], "fubon_intraday_volume": c["stored"],
                     "session_type": (session or {}).get(sym, "normal_template"),
                     "missing_bar_evidence_status": (unit or {}).get(sym, "X"),
                     "liquidity_bucket": "mid", "pit_eligible": True, "eligible_level_A": False,
                     "prev_trading_date": "2025-06-20", "prev_official_total_shares": 1e6, "manifest_seq": 68})
        price.append({"symbol": sym, "date": date, **{k: c[k] for k in
                                                       ("open_match", "high_match", "low_match", "close_match")}})
    edq = pd.DataFrame([{"exchange": exchange, "date": date, "reference_completeness": ref}])
    m = Q.build_mask(pd.DataFrame(rows), edq, pd.DataFrame(price), pd.DataFrame(), "synthetic")
    return m.set_index("symbol")


def bar(p, q, hi=None, lo=None):
    return (p, hi if hi is not None else p, lo if lo is not None else p, p, q)


BASE = {k: bar(100.0, 2 * LOT, hi=101.0 if k == 10 else None, lo=99.0 if k == 20 else None) for k in range(0, 54)}


def _true(bars, extra=()):
    tr = []
    for k, (o, h, l, c, q) in bars.items():
        tr += [(k, o, LOT), (k + 0.5, c, q - LOT)] + ([(k + 0.2, h, 0)] if h != o else []) \
              + ([(k + 0.3, l, 0)] if l != o else [])
    return tr + list(extra)


# ---------------------------------------------------------------- 14.2 (1)-(3): r = 0 with a hidden trade
def test_cx1_missing_trade_offset_by_double_count_is_accepted():
    stored = {k: v for k, v in BASE.items() if k != 30}                    # bar 30 really traded, not stored
    stored[31] = bar(100.0, 3 * LOT)                                        # bar 31 double-counts 1,000 shares
    true = {k: v for k, v in BASE.items()}
    true[30] = bar(100.0, LOT)                                              # 1 lot at 100 in the missing bar
    true[31] = bar(100.0, 2 * LOT)
    c = day(_true(true), stored)
    assert c["residual"] == 0 and c["high_match"] == 1.0 and c["low_match"] == 1.0
    m = mask_of({"CX1": c})
    assert m.at["CX1", "gap_evidence_status"] == Q.ACCEPTED                # documented false acceptance
    assert m.at["CX1", "can_use_for_price_backtest"]


def test_cx2_missing_trade_offset_by_overstated_component_is_accepted():
    stored = {k: v for k, v in BASE.items() if k != 30}
    true = dict(BASE)
    true[30] = bar(100.0, LOT)
    # component (e.g. odd-lot / fixed-price) measured 1 lot ABOVE truth lowers r by exactly the hidden lot
    c = day(_true(true), stored, components_true=4 * LOT, components_measured=5 * LOT)
    assert c["residual"] == 0
    assert mask_of({"CX2": c}).at["CX2", "gap_evidence_status"] == Q.ACCEPTED


def test_cx3_hidden_trade_inside_range_passes_high_low_gate():
    stored = {k: v for k, v in BASE.items() if k != 30}
    stored[31] = bar(100.0, 3 * LOT)
    true = dict(BASE)
    true[30] = bar(100.5, LOT)                                               # inside 99..101
    true[31] = bar(100.0, 2 * LOT)
    c = day(_true(true), stored)
    assert c["high_match"] == 1.0 and c["low_match"] == 1.0 and c["residual"] == 0
    assert mask_of({"CX3": c}).at["CX3", "price_range_match"] == "TRUE"
    assert mask_of({"CX3": c}).at["CX3", "gap_evidence_status"] == Q.ACCEPTED


def test_cx3b_hidden_trade_outside_range_is_caught():
    stored = {k: v for k, v in BASE.items() if k != 30}
    stored[31] = bar(100.0, 3 * LOT)
    true = dict(BASE)
    true[30] = bar(102.0, LOT)                                               # new high -> official high differs
    true[31] = bar(100.0, 2 * LOT)
    c = day(_true(true), stored)
    m = mask_of({"CX3B": c})
    assert m.at["CX3B", "price_range_match"] == "FALSE"
    assert m.at["CX3B", "gap_evidence_status"] == Q.NO_TRADE and not m.at["CX3B", "can_use_for_price_backtest"]


# ---------------------------------------------------------------- 14.2 (4): boundaries
def test_cx4_first_bar_of_open_missing():
    stored = {k: v for k, v in BASE.items() if k != 0}
    stored[1] = bar(100.0, 3 * LOT)
    for open_px, caught in ((100.0, False), (100.5, True)):
        true = dict(BASE)
        true[0] = bar(open_px, LOT)
        true[1] = bar(100.0, 2 * LOT)
        c = day(_true(true), stored)
        m = mask_of({"OPEN": c})
        assert m.at["OPEN", "gap_evidence_status"] == Q.ACCEPTED            # gap rule alone cannot see it
        assert (m.at["OPEN", "open_match"] == "FALSE") is caught            # only the open check can
        assert bool(m.at["OPEN", "can_use_for_price_backtest"]) is (not caught)


def test_cx4_closing_auction_bar_missing():
    stored = {k: v for k, v in BASE.items() if k != 53}
    stored[52] = bar(100.0, 3 * LOT)
    for close_px, caught in ((100.0, False), (100.5, True)):
        true = dict(BASE)
        true[53] = bar(close_px, LOT)                                        # 13:30 closing call
        true[52] = bar(100.0, 2 * LOT)
        c = day(_true(true), stored)
        m = mask_of({"CLOSE": c})
        assert m.at["CLOSE", "gap_evidence_status"] == Q.ACCEPTED
        assert (m.at["CLOSE", "close_match"] == "FALSE") is caught


def test_cx4_adjacent_day_attribution_masks_a_hidden_trade():
    # day t: provider moves 1 lot to day t+1 -> t is short 1 lot; day t+1 has 1 extra lot but also
    # misses 1 real lot in a missing bar -> r(t+1) = 0 and t+1 is accepted.
    stored_t = {k: v for k, v in BASE.items()}
    stored_t[5] = bar(100.0, LOT)
    t = day(_true(BASE), stored_t)
    assert t["residual"] == LOT
    stored_t1 = {k: v for k, v in BASE.items() if k != 30}
    stored_t1[0] = bar(100.0, 3 * LOT)                                       # attributed from day t
    true_t1 = dict(BASE)
    true_t1[30] = bar(100.0, LOT)
    true_t1[31] = bar(100.0, 2 * LOT)
    t1 = day(_true(true_t1), stored_t1)
    assert t1["residual"] == 0
    assert mask_of({"ADJ": t1}, date="2025-06-24").at["ADJ", "gap_evidence_status"] == Q.ACCEPTED


def test_cx4_disposal_session_and_nonstandard_unit_never_accepted():
    c = day(_true(BASE), {k: v for k, v in BASE.items() if k % 6})          # call auction every few bars
    m = mask_of({"DISP": c, "ETFK": c}, session={"DISP": "special_auction_or_halt"},
                unit={"ETFK": "UNIT_UNVERIFIED_EXCLUDED"})
    assert m.at["DISP", "gap_evidence_status"] == "SPECIAL_SESSION_NOT_EVALUATED"
    assert m.at["DISP", "research_tier"] == "ISOLATE"
    assert m.at["ETFK", "gap_evidence_status"] == "UNIT_UNVERIFIED_EXCLUDED"
    assert not m["can_use_for_price_backtest"].any()


# ---------------------------------------------------------------- 14.2 (5): clustered negatives
def test_cx5_clustered_negatives_disallow_binomial_bound():
    edq = pd.DataFrame({"exchange": ["TWSE"] * 5, "date": [f"2025-01-0{i}" for i in range(1, 6)],
                        "n_complete_days": [900] * 5, "n_negative": [0, 0, 40, 0, 35],
                        "identity_status": ["VERIFIED", "VERIFIED", "OVERCOUNT_PRESENT", "VERIFIED",
                                            "OVERCOUNT_PRESENT"]})
    cl = Q.negative_residual_clustering(edq)
    assert cl["exchange_dates_with_negative"] == 2 and cl["share_on_quarantined_dates"] == 1.0
    assert cl["independent_trials_bound_valid"] is False
    prem = Q.acceptance_premises(pd.DataFrame(), edq, None, None)
    text = json.dumps(prem)
    assert "95 % upper bound" not in text and "no binomial / rule-of-three bound is claimed" in text
    assert "not independent truth" in prem["P1_official_composition"]["evidence"]
    assert all("uncertainty" in v for k, v in prem.items() if k.startswith("P"))


# ---------------------------------------------------------------- M2 / M6 guards
def test_frozen_quarantine_cannot_be_lifted_by_identity():
    assert len(Q.QUARANTINE_FROZEN_V1) == 13
    c = day(_true(BASE), dict(BASE))
    m = mask_of({"Q": c}, date="2024-02-02", identity="VERIFIED")           # frozen TWSE date, now "VERIFIED"
    assert m.at["Q", "volume_reconciliation_status"] == "QUARANTINED_DATE"
    assert not m.at["Q", "can_use_for_price_backtest"]


def test_frozen_list_matches_contract():
    j = json.loads((DOCS / "backtest_data_contract.json").read_text(encoding="utf-8"))
    listed = {(x["exchange"], x["date"]) for x in j["quarantine_frozen_v1"]["exchange_dates"]}
    assert listed == set(Q.QUARANTINE_FROZEN_V1)


def test_refetch_consistent_does_not_promote_to_day_level_or_change_flags():
    stored = {k: v for k, v in BASE.items() if k not in (30, 31)}
    true = dict(BASE)
    c = day(_true(true), stored)                                             # short by 4 whole lots
    assert c["residual"] % LOT == 0 and c["residual"] > 0
    full = mask_of({"S": c}, ref=Q.REFERENCE_COMPLETE)
    rc = mask_of({"S": c}, ref=Q.REFETCH_CONSISTENT)
    assert full.at["S", "gap_evidence_status"] == Q.DAY_LEVEL
    assert rc.at["S", "gap_evidence_status"] == Q.DAY_LEVEL_REFETCH
    flags = ["can_use_for_price_backtest", "can_use_for_volume_backtest", "level_A_strict", "research_tier"]
    assert full.loc["S", flags].tolist() == rc.loc["S", flags].tolist()


# ---------------------------------------------------------------- M1 / M3 / M4 contract guards
def test_contract_semantics():
    j = json.loads((DOCS / "backtest_data_contract.json").read_text(encoding="utf-8"))
    assert "official_open_close_override" not in j
    assert "NO_TRADE_INTERVAL_EVIDENCED" not in j["slot_types"]
    assert j["status_glossary"][Q.ACCEPTED]["per_bar_proof"] is False
    assert j["volume"]["volume_strict_requires"] == "EXACT"
    assert j["volume"]["volume_tolerance_exploratory"]["formal_use"] is False
    assert j["official_daily"]["may_overwrite_5m_ohlc"] is False
    assert j["official_daily"]["view"] == "daily_official"
    md = (DOCS / "BACKTEST_DATA_CONTRACT.md").read_text(encoding="utf-8")
    assert "以官方日 K 開盤價或收盤價為準" not in md
