"""Final data-quality acceptance + machine-readable quality mask (read-only, offline, no network).

python -m tw_lake.audit.quality_mask --ledger-dir .../gap_ledger_... \
    --eligibility-dir .../eligibility_<price-merged run> --price-check-dir .../price_check_... \
    --refetch-evidence .../official_refetch_.../official_refetch_compare.csv \
    --overcount-dir .../overcount_diag_... [--fubon-overcount-dir .../overcount_fubon_...] \
    --out-dir /content/tw_lake_quality

Inputs are earlier, immutable outputs; nothing is rewritten. Output is a new versioned folder:
    quality_mask_symbol_day.parquet   one row per symbol-day (schema in docs/BACKTEST_DATA_CONTRACT.md)
    quarantine_exchange_dates.csv     exchange-dates whose negative residuals break the upper-bound premise
    quarantine_symbol_days.csv        the over-count symbol-days themselves (+ Fubon evidence when given)
    anomaly_ledger.csv                every remaining unexplained gap run, slot by slot range
    acceptance_rule_v2.json           premises, the evidence for each, and the decision per premise
    stats_*.csv, QUALITY_REPORT.md, quality_manifest.json (SHA-256 of every output, input versions)

Acceptance rule (VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH, a symbol-day level statement, not a per-bar one):
    normal session; unit verified; official record; exchange-date identity VERIFIED or NO_OVERCOUNT;
    exchange-date not quarantined; residual (official regular - stored 5m) exactly 0; official high and low
    equal to the stored bars' high and low. Everything else that was a no-trade candidate stays CANDIDATE.
UNKNOWN is never treated as TRUE.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from .stagelog import Stages
from .volume_recon import TRADING_UNIT

QUALITY_VERSION = "1.1.0"
# VERIFIED = evidence/rule acceptance; CANDIDATE = not proven per trade. Conditional research acceptance,
# never a per-bar NO_TRADE_INTERVAL_EVIDENCED (see docs/BACKTEST_DATA_CONTRACT.md section 0).
ACCEPTED = "VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH"
NO_TRADE = "CANDIDATE_NO_BOARD_LOT_TRADE"
DAY_LEVEL = "DAY_LEVEL_MISSING_VOLUME"
# whole-lot shortfall whose fixed-price reference was only re-fetched consistently (not proven complete)
DAY_LEVEL_REFETCH = "DAY_LEVEL_MISSING_VOLUME_REFETCH_CONSISTENT_ONLY"
REFERENCE_COMPLETE = "COMPLETE"
REFETCH_CONSISTENT = "REFETCH_CONSISTENT"
# Frozen at quality v1.0.0 (13 exchange-dates). Always quarantined; un-quarantining any symbol or date needs a
# new version, independent evidence and a separate sign-off. Never shrink this list in place.
QUARANTINE_FROZEN_V1 = frozenset(
    [("TPEX", d) for d in ("2023-08-24", "2026-01-27", "2026-01-29", "2026-05-12")]
    + [("TWSE", d) for d in ("2023-05-25", "2023-07-19", "2023-08-24", "2023-08-28", "2023-12-25", "2024-02-02",
                             "2026-01-27", "2026-01-29", "2026-05-12")])


def _tri(v) -> pd.Series:
    """1/0/NaN (or bool / None) -> 'TRUE'/'FALSE'/'UNKNOWN'."""
    s = pd.Series(v)
    out = np.where(s.isna(), "UNKNOWN", np.where(s.astype(float) > 0.5, "TRUE", "FALSE"))
    return pd.Series(out, index=s.index)


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def reference_fix(edq: pd.DataFrame, refetch: pd.DataFrame | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Coverage flags whose re-request returned the same values (HTTP 200) become REFETCH_CONSISTENT.

    A consistent re-fetch shows the response is reproducible, not that the report is complete, so the
    exchange-date is NOT promoted to COMPLETE (quality >= 1.1.0; v1.0.0 wrote COMPLETE, see contract errata)."""
    edq = edq.copy()
    log = pd.DataFrame(columns=["exchange", "date", "report", "evidence_status", "reference_complete"])
    if refetch is None or edq.empty:
        return edq, log
    ok = refetch[(refetch["new_http"] == 200) & (refetch["symbols_value_differs"] == 0)
                 & (refetch["new_symbols"] == refetch["old_symbols"])]
    rows = []
    for i, r in edq[edq["reference_completeness"] == "INCOMPLETE"].iterrows():
        reps = [x for x in str(r["incomplete_reports"]).split("|") if x]
        confirmed = [x for x in reps if ((ok["date"] == r["date"]) & (ok["report"] == x)).any()]
        if confirmed and len(confirmed) == len(reps):
            edq.at[i, "reference_completeness"] = REFETCH_CONSISTENT
            rows.append({"exchange": r["exchange"], "date": r["date"], "report": "|".join(confirmed),
                         "evidence_status": REFETCH_CONSISTENT, "reference_complete": False})
    return edq, pd.DataFrame(rows, columns=log.columns)


def build_mask(sdq: pd.DataFrame, edq: pd.DataFrame, price: pd.DataFrame | None, days: pd.DataFrame,
               evidence_version: str) -> pd.DataFrame:
    s = sdq.copy()
    s = s.merge(edq[["exchange", "date", "reference_completeness"]], on=["exchange", "date"], how="left")
    frozen = pd.Series([(e, d) in QUARANTINE_FROZEN_V1 for e, d in zip(s["exchange"], s["date"])], index=s.index)
    quarantined = (s["identity_status"] == "OVERCOUNT_PRESENT") | frozen
    r = s["residual_shares"]
    unit_ok = s["missing_bar_evidence_status"].ne("UNIT_UNVERIFIED_EXCLUDED") & s["official_total"].notna()
    normal = s["session_type"] == "normal_template"
    ident_ok = s["identity_status"].isin(["VERIFIED", "NO_OVERCOUNT"]) & ~frozen
    # --- price dimensions
    if price is not None:
        p = price[["symbol", "date", "open_match", "high_match", "low_match", "close_match"]]
        s = s.drop(columns=[c for c in ("open_match", "high_match", "low_match", "close_match") if c in s.columns]) \
            .merge(p, on=["symbol", "date"], how="left")
    else:
        for c in ("open_match", "high_match", "low_match", "close_match"):
            s[c] = np.nan
    hi, lo = _tri(s["high_match"].values), _tri(s["low_match"].values)
    s["price_range_match"] = np.select([(hi == "TRUE") & (lo == "TRUE"), (hi == "FALSE") | (lo == "FALSE")],
                                       ["TRUE", "FALSE"], default="UNKNOWN")
    s["open_match"] = _tri(s["open_match"].values).values
    s["close_match"] = _tri(s["close_match"].values).values
    s["price_quality_status"] = np.select(
        [s["price_range_match"] == "FALSE", s["price_range_match"] == "UNKNOWN",
         (s["open_match"] == "TRUE") & (s["close_match"] == "TRUE")],
        ["FAIL_RANGE", "UNKNOWN", "PASS"], default="RANGE_ONLY")
    # --- gap evidence (re-derived; reference completeness fix applied)
    whole = (r >= TRADING_UNIT) & ((r % TRADING_UNIT) == 0)
    has_missing = s["missing_bar_count"].fillna(0) > 0
    accepted = (has_missing & normal & unit_ok & ident_ok & ~quarantined & (r.abs() < 1)
                & (s["price_range_match"] == "TRUE"))
    s["gap_evidence_status"] = np.select(
        [~normal, ~has_missing & (s["real_bar_count"].fillna(0) > 0), ~has_missing,
         s["missing_bar_evidence_status"] == "UNIT_UNVERIFIED_EXCLUDED", s["official_total"].isna(),
         quarantined, accepted,
         ident_ok & (r >= 0) & (r < TRADING_UNIT),
         ident_ok & whole & (s["reference_completeness"] == REFERENCE_COMPLETE),
         ident_ok & whole & (s["reference_completeness"] == REFETCH_CONSISTENT),
         ident_ok & (r <= -1)],
        ["SPECIAL_SESSION_NOT_EVALUATED", "NONE_MISSING", "NO_BARS",
         "UNIT_UNVERIFIED_EXCLUDED", "UNRESOLVED_NO_OFFICIAL_RECORD",
         "UNRESOLVED_QUARANTINED_EXCHANGE_DATE", ACCEPTED, NO_TRADE, DAY_LEVEL, DAY_LEVEL_REFETCH,
         "UNRESOLVED_VOLUME_EXCESS"],
        default="UNRESOLVED_ANOMALY")
    # --- volume dimension
    s["volume_reconciliation_status"] = np.select(
        [~normal | ~unit_ok, quarantined, ~ident_ok, r.abs() < 1, (r >= 1) & (r < TRADING_UNIT), whole, r <= -1],
        ["NOT_EVALUATED", "QUARANTINED_DATE", "IDENTITY_UNVERIFIED", "EXACT", "WITHIN_ONE_LOT",
         "SHORTFALL_WHOLE_LOTS", "OVERCOUNT"], default="RESIDUAL_NOT_WHOLE_LOT")
    reg = s["official_regular_volume"].where(s["official_regular_volume"] > 0)
    s["intraday_volume_shortfall_ratio"] = np.where(
        s["volume_reconciliation_status"] == "SHORTFALL_WHOLE_LOTS", r / reg,
        np.where(s["volume_reconciliation_status"].isin(["EXACT", "WITHIN_ONE_LOT"]), 0.0, np.nan))
    s["intraday_volume_overcount"] = np.where(r <= -1, -r, np.where(r.notna(), 0.0, np.nan))
    s["has_special_auction"] = s["session_type"] == "special_auction_or_halt"
    # --- use flags (UNKNOWN never counts as TRUE)
    gap_ok = s["gap_evidence_status"].isin(["NONE_MISSING", ACCEPTED])
    pit = s["pit_eligible"].fillna(False).astype(bool)
    s["can_use_for_price_backtest"] = (normal & unit_ok & ident_ok & ~quarantined & gap_ok & pit
                                       & (s["price_quality_status"] == "PASS"))
    s["can_use_for_volume_backtest"] = s["can_use_for_price_backtest"] & (s["volume_reconciliation_status"] == "EXACT")
    s["level_A_strict"] = s["eligible_level_A"].fillna(False).astype(bool) & (s["price_quality_status"] == "PASS")
    s["level_B_price_gate"] = s["can_use_for_price_backtest"]
    s["volume_strict"] = s["can_use_for_volume_backtest"]
    rules = [("special session (disposal call auction) or unknown regime", normal),
             ("unit unverified or no official record", unit_ok),
             ("exchange-date quarantined (negative residuals)", ~quarantined),
             ("exchange-date identity not verified", ident_ok),
             ("missing bars without accepted no-trade evidence", gap_ok),
             ("price range mismatch or unknown", s["price_range_match"] == "TRUE"),
             ("open/close mismatch or unknown", s["price_quality_status"] == "PASS"),
             ("no previous-day official record (PIT)", pit)]
    reason = pd.Series("", index=s.index, dtype=object)
    for name, m in reversed(rules):
        reason = reason.where(m.fillna(False).astype(bool), name)
    s["exclusion_reason"] = reason
    s["volume_exclusion_reason"] = np.where(
        s["can_use_for_price_backtest"] & ~s["can_use_for_volume_backtest"],
        "volume_reconciliation_status=" + s["volume_reconciliation_status"], "")
    s["evidence_version"] = evidence_version
    s["source_manifest_sequence"] = s["manifest_seq"]
    s["research_tier"] = np.select(
        [s["level_A_strict"], s["volume_strict"], s["level_B_price_gate"],
         s["gap_evidence_status"].isin(["UNRESOLVED_QUARANTINED_EXCHANGE_DATE", "UNRESOLVED_ANOMALY",
                                        "UNRESOLVED_VOLUME_EXCESS", "UNRESOLVED_NO_OFFICIAL_RECORD"])
         | ~normal],
        ["LEVEL_A_STRICT", "VOLUME_STRICT", "LEVEL_B_PRICE_GATE", "ISOLATE"], default="EXPLORATORY_ONLY")
    cols = ["symbol", "exchange", "date", "session_type", "has_special_auction", "liquidity_bucket",
            "real_bar_count", "missing_bar_count", "gap_evidence_status", "price_range_match", "open_match",
            "close_match", "price_quality_status", "volume_reconciliation_status", "official_total",
            "official_regular_volume", "fubon_intraday_volume", "residual_shares", "intraday_volume_shortfall_ratio",
            "intraday_volume_overcount", "identity_status", "reference_completeness", "prev_trading_date",
            "prev_official_total_shares", "pit_eligible", "level_A_strict", "level_B_price_gate", "volume_strict",
            "can_use_for_price_backtest", "can_use_for_volume_backtest", "research_tier", "exclusion_reason",
            "volume_exclusion_reason", "evidence_version", "source_manifest_sequence"]
    return s[cols]


def negative_residual_clustering(edq: pd.DataFrame) -> dict:
    """How concentrated negative residuals are; concentration rules out an independent-trials bound."""
    neg = edq["n_negative"].fillna(0)
    n = float(neg.sum())
    q = edq["identity_status"] == "OVERCOUNT_PRESENT"
    return {"negative_symbol_days": int(n), "exchange_dates_with_negative": int((neg > 0).sum()),
            "exchange_dates_total": int(len(edq)),
            "share_on_quarantined_dates": float(neg[q].sum() / n) if n else 0.0,
            "max_on_one_exchange_date": int(neg.max()) if len(neg) else 0,
            "independent_trials_bound_valid": False}


def acceptance_premises(sdq: pd.DataFrame, edq: pd.DataFrame, price_xt: pd.DataFrame | None,
                        fubon: pd.DataFrame | None) -> dict:
    cd = edq["n_complete_days"].fillna(0).sum()
    neg = edq["n_negative"].fillna(0).sum()
    neg_outside = edq.loc[edq["identity_status"] != "OVERCOUNT_PRESENT", "n_negative"].fillna(0).sum()
    n_out = edq.loc[edq["identity_status"] != "OVERCOUNT_PRESENT", "n_complete_days"].fillna(0).sum()
    prem = {
        "P1_official_composition": {
            "premise": "official daily total = regular session (incl. 13:30 closing call) + fixed-price + odd-lot "
                       "(intraday, after-hours) + block; auctions/tenders excluded",
            "evidence": "TWSE MI_INDEX note; identity exact on complete days of verified exchange-dates (>= 99 %); "
                        "FinLab daily volume equals the official total on 100 % of complete days (FinLab is a "
                        "redistribution of the same exchange data, not independent truth)",
            "uncertainty": "composition notes can change; a component missing from both the official total and "
                           "our component list would not be detected",
            "decision": "accepted"},
        "P2_units": {
            "premise": "components converted to shares correctly; board lot = 1,000 shares",
            "evidence": "unit decided per value (header, amount/(qty x price), report notes) and recorded; foreign-"
                        "currency ETFs (lot = 100 units) excluded; residuals on verified dates are 0, not x1000 off",
            "uncertainty": "non-standard trading units other than the excluded ETFs are only checked where a "
                           "unit decision was recorded; others are UNIT_UNVERIFIED_EXCLUDED",
            "decision": "accepted (foreign-currency ETFs excluded)"},
        "P3_no_overcount_in_stored_bars": {
            "premise": "stored 5m volume never exceeds the true regular volume (no duplicate counting)",
            "evidence": f"negative residuals on {int(neg)} of {int(cd)} complete days; {int(neg_outside)} of "
                        f"{int(n_out)} outside the quarantined exchange-dates",
            "uncertainty": "negatives are clustered on a few exchange-dates (date-common provider errors), so "
                           "symbol-days are not independent trials and no binomial / rule-of-three bound is "
                           "claimed; an over-count smaller than any detectable negative residual is not excluded",
            "clustering": negative_residual_clustering(edq),
            "decision": "accepted outside quarantined exchange-dates; rejected on them"},
        "P4_day_attribution": {
            "premise": "trades are attributed to the right trading day by the provider",
            "evidence": "Fubon D candle equals the official total on every cross-checked symbol-day; adjacent-day "
                        "opposite residuals are rare and confined to quarantined dates",
            "uncertainty": "a trade moved to an adjacent day can be masked if that day also misses an equal "
                           "board-lot volume (r = 0 on both); only checked where cross-checked",
            "decision": "accepted outside quarantined exchange-dates"},
        "P5_no_cancellation": {
            "premise": "a residual of exactly 0 cannot hide trades offset by an over-count on the same symbol-day",
            "evidence": "offset needs an over-count (P3); over-count is only observed on quarantined dates",
            "uncertainty": "counterexamples (tests/test_rule_v2_counterexamples.py): a missing 1,000-share trade "
                           "offset by a 1,000-share double count in another bar, or by an overstated component, "
                           "gives r = 0 and unchanged official high/low",
            "decision": "accepted with P3; not a per-bar verification"},
        "P6_components_not_overstated": {
            "premise": "measured components do not exceed the true components (else residual is pushed down)",
            "evidence": "over-subtraction would produce negative residuals on complete days, covered by P3; "
                        "re-parse of raw responses reproduces the component tables exactly; re-request of the "
                        "flagged BFT41U dates returns identical values (REFETCH_CONSISTENT, not proof of "
                        "completeness)",
            "uncertainty": "a component overstated by exactly the hidden board-lot volume yields r = 0",
            "decision": "accepted"},
        "P7_regime": {
            "premise": "normal continuous session + 13:30 closing call; disposal call-auction days are different",
            "evidence": "special sessions are not evaluated (SPECIAL_SESSION_NOT_EVALUATED)",
            "uncertainty": "first bar of the open and the 13:30 closing call are matched by open/close checks "
                           "only when the hidden trade price differs from the stored one",
            "decision": "scope restricted to normal sessions"},
        "P8_price_corroboration": {
            "premise": "a hidden trade outside the stored range would change the official high/low",
            "evidence": "required per symbol-day: official high and low equal the stored range",
            "uncertainty": "a hidden trade priced inside the stored high/low range passes this gate",
            "decision": "required for acceptance"},
    }
    if price_xt is not None:
        prem["P8_price_corroboration"]["cross_tab"] = price_xt.to_dict()
    if fubon is not None and len(fubon):
        f = fubon
        eq = lambda a, b: ((a - b).abs() < 1).mean()  # noqa: E731
        prem["P3_no_overcount_in_stored_bars"]["fubon_quarantine_check"] = {
            "symbol_days": int(len(f)), "api_5m_equals_stored": float(eq(f["api_5m_shares"], f["stored_5m_shares"])),
            "api_D_equals_official_total": float(eq(f["api_D_shares"], f["official_total"])),
            "api_5m_minus_official_regular_min": float((f["api_5m_shares"] - f["official_regular"]).min())}
    prem["statement"] = ("VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH is a conditional research acceptance of a "
                         "symbol-day derived from official daily volume and range, not an observation of each "
                         "missing bar; it never maps to NO_TRADE_INTERVAL_EVIDENCED. Statistical agreement is not "
                         "per-bar verification.")
    return prem


def anomaly_ledger(mask: pd.DataFrame, ledger_dir: Path) -> pd.DataFrame:
    tgt = mask[mask["gap_evidence_status"].isin(["UNRESOLVED_ANOMALY", "UNRESOLVED_VOLUME_EXCESS",
                                                 "UNRESOLVED_NO_OFFICIAL_RECORD"])]
    keys = tgt[["symbol", "date"]].drop_duplicates()
    rd = Path(ledger_dir) / "gap_runs"
    frames = []
    for f in sorted(rd.glob("*.parquet")) if rd.exists() else []:
        r = pd.read_parquet(f, columns=["symbol", "date", "start_slot", "end_slot", "bar_start_from", "bar_start_to",
                                        "n_slots", "includes_close_auction", "status", "evidence_candidate"])
        r = r[r["symbol"].isin(set(keys["symbol"]))]
        if r.empty:
            continue
        r["date"] = pd.to_datetime(r["date"]).dt.strftime("%Y-%m-%d")
        r = r.merge(keys, on=["symbol", "date"])
        if len(r):
            frames.append(r)
    runs = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["symbol", "date"])
    cols = ["symbol", "exchange", "date", "gap_evidence_status", "volume_reconciliation_status", "residual_shares",
            "official_total", "official_regular_volume", "fubon_intraday_volume", "missing_bar_count",
            "identity_status", "reference_completeness", "price_range_match"]
    out = runs.merge(tgt[cols], on=["symbol", "date"], how="right")
    out["next_evidence_needed"] = np.select(
        [out["gap_evidence_status"] == "UNRESOLVED_NO_OFFICIAL_RECORD",
         out["gap_evidence_status"] == "UNRESOLVED_VOLUME_EXCESS"],
        ["official listing/market record of the symbol on that date (board change, suspension)",
         "per-symbol Fubon D/5m re-request and official correction notice; stored bars exceed official regular"],
        default="identify the component the official total includes but the measured components miss "
                "(non-whole residual >= 1 lot): component report rows for this symbol-date")
    return out


def run(ledger_dir: Path, eligibility_dir: Path, out_root: Path, price_check_dir: Path | None = None,
        refetch_evidence: list[Path] | None = None, overcount_dir: Path | None = None,
        fubon_overcount_dir: Path | None = None, log=print) -> Path:
    out = Path(out_root) / f"quality_mask_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    out.mkdir(parents=True, exist_ok=False)
    st = Stages(out, "quality_mask", {"ledger_dir": str(ledger_dir), "eligibility_dir": str(eligibility_dir),
                                      "price_check_dir": str(price_check_dir), "overcount_dir": str(overcount_dir),
                                      "fubon_overcount_dir": str(fubon_overcount_dir),
                                      "refetch_evidence": [str(p) for p in refetch_evidence or []]}, log)
    for name in ("symbol_day_quality.parquet", "exchange_date_quality.parquet", "backtest_eligibility_manifest.json"):
        st.input_file(f"eligibility/{name}", Path(eligibility_dir) / name)
    if price_check_dir:
        st.input_file("price_check_symbol_day", Path(price_check_dir) / "price_check_symbol_day.parquet")
    with st.stage("load") as rec:
        sdq = pd.read_parquet(Path(eligibility_dir) / "symbol_day_quality.parquet")
        edq = pd.read_parquet(Path(eligibility_dir) / "exchange_date_quality.parquet")
        emf = json.loads((Path(eligibility_dir) / "backtest_eligibility_manifest.json").read_text(encoding="utf-8"))
        price = pd.read_parquet(Path(price_check_dir) / "price_check_symbol_day.parquet",
                                columns=["symbol", "date", "open_match", "high_match", "low_match", "close_match"]) \
            if price_check_dir else None
        days = pd.read_parquet(Path(ledger_dir) / "gap_day_summary.parquet", columns=["symbol", "date"])
        refetch = pd.concat([pd.read_csv(p) for p in refetch_evidence], ignore_index=True) if refetch_evidence else None
        rec["symbol_days"] = int(len(sdq))
    with st.stage("reference_fix") as rec:
        edq2, cleared = reference_fix(edq, refetch)
        cleared.to_csv(out / "reference_refetch_consistency.csv", index=False, encoding="utf-8-sig")
        rec["refetch_consistent"] = int(len(cleared))
    ev = (f"quality_v{QUALITY_VERSION}+eligibility_{emf.get('code_commit', '')[:7]}"
          f"+recon_{emf.get('inputs', {}).get('reconciliation', {}).get('git_sha', '')[:7]}"
          f"+price_{Path(price_check_dir).name if price_check_dir else 'none'}")
    with st.stage("mask") as rec:
        mask = build_mask(sdq, edq2, price, days, ev)
        mask.to_parquet(out / "quality_mask_symbol_day.parquet", index=False)
        rec["rows"] = int(len(mask))
    with st.stage("quarantine_and_anomalies") as rec:
        fz = pd.Series([(e, d) in QUARANTINE_FROZEN_V1 for e, d in zip(edq2["exchange"], edq2["date"])],
                       index=edq2.index)
        q = edq2[(edq2["identity_status"] == "OVERCOUNT_PRESENT") | fz].copy()
        q["frozen_list_v1"] = fz[q.index]
        qs = mask[mask["gap_evidence_status"] == "UNRESOLVED_QUARANTINED_EXCHANGE_DATE"] \
            .groupby(["exchange", "date"]).agg(quarantined_symbol_days=("symbol", "size"),
                                               quarantined_missing_slots=("missing_bar_count", "sum")).reset_index()
        q = q.merge(qs, on=["exchange", "date"], how="left")
        q["reason"] = "complete days with stored 5m > official regular; provider intraday over-count confirmed " \
                      "where cross-checked; same-day symbols are not upgraded"
        q.to_csv(out / "quarantine_exchange_dates.csv", index=False, encoding="utf-8-sig")
        if overcount_dir:
            oc = pd.read_parquet(Path(overcount_dir) / "overcount_symbol_days.parquet")
            if fubon_overcount_dir:
                fb = pd.read_parquet(Path(fubon_overcount_dir) / "crosscheck.parquet")[
                    ["symbol", "date", "api_5m_shares", "api_1m_shares", "api_D_shares"]]
                oc = oc.merge(fb, on=["symbol", "date"], how="left")
            oc.to_csv(out / "quarantine_symbol_days.csv", index=False, encoding="utf-8-sig")
        an = anomaly_ledger(mask, ledger_dir)
        an.to_csv(out / "anomaly_ledger.csv", index=False, encoding="utf-8-sig")
        rec["quarantine_exchange_dates"] = int(len(q))
        rec["anomaly_symbol_days"] = int(an[["symbol", "date"]].drop_duplicates().shape[0])
    with st.stage("acceptance_and_stats") as rec:
        xt = pd.crosstab(mask["gap_evidence_status"], mask["price_range_match"])
        fub = pd.read_parquet(Path(fubon_overcount_dir) / "crosscheck.parquet") if fubon_overcount_dir else None
        prem = acceptance_premises(sdq, edq2, xt, fub)
        (out / "acceptance_rule_v2.json").write_text(json.dumps(prem, ensure_ascii=False, indent=2, default=str),
                                                     encoding="utf-8")
        mask["year"] = mask["date"].str[:4]
        gap = mask[mask["missing_bar_count"].fillna(0) > 0].groupby("gap_evidence_status") \
            .agg(symbol_days=("symbol", "size"), missing_slots=("missing_bar_count", "sum")).reset_index()
        gap["pct_slots"] = (100 * gap["missing_slots"] / gap["missing_slots"].sum()).round(4)
        gap.to_csv(out / "stats_gap_classification.csv", index=False, encoding="utf-8-sig")
        n = mask[mask["session_type"] == "normal_template"]
        pq = n.groupby(["year", "exchange", "price_quality_status"]).size().unstack(fill_value=0).reset_index()
        pq.to_csv(out / "stats_price_quality.csv", index=False, encoding="utf-8-sig")
        mask[(mask["price_quality_status"] != "PASS") & (mask["session_type"] == "normal_template")][
            ["symbol", "exchange", "date", "price_quality_status", "price_range_match", "open_match", "close_match",
             "gap_evidence_status", "liquidity_bucket"]].to_csv(out / "price_exclusions.csv", index=False,
                                                                 encoding="utf-8-sig")
        vq = n.groupby(["year", "exchange", "volume_reconciliation_status"]).size().unstack(fill_value=0).reset_index()
        vq.to_csv(out / "stats_volume_quality.csv", index=False, encoding="utf-8-sig")
        tiers = ["level_A_strict", "level_B_price_gate", "volume_strict"]

        def ret(keys):
            g = n.groupby(keys).agg(symbol_days=("symbol", "size"), **{t: (t, "sum") for t in tiers}).reset_index()
            for t in tiers:
                g[f"{t}_pct"] = (100 * g[t] / g["symbol_days"]).round(2)
            return g
        ret(["year", "exchange"]).to_csv(out / "stats_tiers_by_year_exchange.csv", index=False, encoding="utf-8-sig")
        ret(["liquidity_bucket", "exchange", "year"]).to_csv(out / "stats_retention_liquidity_exchange_year.csv",
                                                             index=False, encoding="utf-8-sig")
        reasons = n.loc[~n["can_use_for_price_backtest"], "exclusion_reason"].value_counts().rename("symbol_days")
        reasons.to_csv(out / "stats_exclusion_reasons.csv", encoding="utf-8-sig")
        rec["price_backtest_symbol_days"] = int(n["can_use_for_price_backtest"].sum())
        rec["volume_backtest_symbol_days"] = int(n["can_use_for_volume_backtest"].sum())
    contract = Path(__file__).resolve().parents[3] / "docs" / "BACKTEST_DATA_CONTRACT.md"
    schema = Path(__file__).resolve().parents[3] / "docs" / "backtest_data_contract.json"
    for p in (contract, schema):
        if p.exists():
            (out / p.name).write_bytes(p.read_bytes())
    files = sorted(x for x in out.iterdir() if x.is_file() and x.name not in ("run_info.json", "quality_manifest.json"))
    man = {"quality_version": QUALITY_VERSION, "evidence_version": ev, "code_commit": git_sha(),
           "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
           "source_manifest_sequence": {"fubon_5m": sorted(mask["source_manifest_sequence"].dropna().unique().tolist()),
                                        "from_eligibility": emf.get("inputs", {}).get("ledger", {}).get("sources")},
           "inputs": st.info["inputs"], "sha256": {f.name: _sha(f) for f in files},
           "row_counts": {"symbol_days": int(len(mask)),
                          "price_backtest": int(mask["can_use_for_price_backtest"].sum()),
                          "volume_backtest": int(mask["can_use_for_volume_backtest"].sum()),
                          "level_A_strict": int(mask["level_A_strict"].sum())},
           "statement": "Derived quality mask for review. Raw data, ledgers and earlier outputs are not modified."}
    (out / "quality_manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=2, default=str),
                                               encoding="utf-8")
    md = ["# Final data-quality acceptance", "", f"- evidence_version `{ev}`", f"- code `{git_sha()}`", "",
          "## Gap classification", "", "```", gap.to_string(index=False), "```", "",
          "## Tiers by year / exchange", "", "```", ret(["year", "exchange"]).to_string(index=False), "```", "",
          "## Exclusion reasons (price backtest)", "", "```", reasons.to_string(), "```", "",
          "## Quarantined exchange-dates", "", "```",
          q[["exchange", "date", "n_negative", "quarantined_symbol_days", "quarantined_missing_slots"]].to_string(index=False),
          "```", "", f"## Anomaly ledger: {an[['symbol', 'date']].drop_duplicates().shape[0]} symbol-days, "
          f"{int(an['n_slots'].fillna(0).sum()) if 'n_slots' in an else 0} slots in runs", ""]
    (out / "QUALITY_REPORT.md").write_text("\n".join(md), encoding="utf-8")
    st.finish(**man["row_counts"])
    log((out / "QUALITY_REPORT.md").read_text(encoding="utf-8"))
    log(f"written to {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.quality_mask")
    p.add_argument("--ledger-dir", required=True)
    p.add_argument("--eligibility-dir", required=True)
    p.add_argument("--price-check-dir", default=None)
    p.add_argument("--refetch-evidence", default="", help="comma separated official_refetch_compare.csv paths")
    p.add_argument("--overcount-dir", default=None)
    p.add_argument("--fubon-overcount-dir", default=None)
    p.add_argument("--out-dir", default="/content/tw_lake_quality")
    a = p.parse_args(argv)
    run(Path(a.ledger_dir), Path(a.eligibility_dir), Path(a.out_dir),
        Path(a.price_check_dir) if a.price_check_dir else None,
        [Path(x) for x in a.refetch_evidence.split(",") if x.strip()],
        Path(a.overcount_dir) if a.overcount_dir else None,
        Path(a.fubon_overcount_dir) if a.fubon_overcount_dir else None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
