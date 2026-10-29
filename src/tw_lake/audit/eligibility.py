"""Review of the unexplained gap share + Backtest Eligibility v0.1 (read-only, offline, no network).

python -m tw_lake.audit.eligibility --data-root ... --ledger-dir .../gap_ledger_... \
    --recon-dir .../official_volume_batch_recon/volume_recon_... \
    --dataset-root .../reference/official_volume/v1 --out-dir /content/tw_lake_eligibility

Phase 1  reproduce: recount the reconciliation (gap total, unexplained slots, class table) from its own
         symbol-day output and the gap ledger; inventory the official reference shards; record versions.
         A mismatch stops the re-classification (outputs: phase1_reproduce.json only).
Phase 2  review every exchange-date whose identity is not verified, offline:
         - reference completeness: per report rows vs the median of the surrounding 21 trading days
         - raw evidence: every raw file exists and its SHA-256 matches (unverified dates; sample elsewhere)
         - re-parse of the saved raw responses (gzip) and comparison with the stored component table
         - residual structure on complete days (exact / positive whole lot / non-whole / negative)
         - which component is absent for the non-whole residual symbols
         then re-classifies every symbol-day with missing bars under rule v2 (below).
Phase 3  writes exchange_date_quality.parquet, symbol_day_quality.parquet, backtest_eligibility_manifest.json,
         plus the 2026 intraday-volume shortfall report, class diff v1 -> v2, unresolved exchange-dates with
         the minimum evidence still needed, and eligibility counts with a sample-selection comparison.

Rule v2 (official volume upper bound). For a symbol-day:
    r = official_total - sum(measured components) - stored 5m volume      (shares)
    true regular volume = stored 5m volume + hidden volume in missing bars (+ any undercount of stored bars)
    components are >= 0; a component missing from the reference only makes r larger.
  If stored bars do not over-count (m5 <= true regular) and no component is over-stated, then
    0 <= hidden <= r. Board lots trade in multiples of 1,000 shares, so 0 <= r < 1,000 => no board-lot
    trade in the missing bars.
  Over-count / over-subtraction shows up as negative residuals. The exchange-date is therefore accepted as
  NO_OVERCOUNT only when none of its complete days has a negative residual; VERIFIED needs >= 99 % exact.
  Negative r on the symbol-day itself is never evidence. A whole-lot r >= 1,000 is day-level missing volume
  only when the reference for that exchange-date is complete; otherwise it stays unresolved. Nothing is
  allocated to a bar; nothing is forward-filled. Every no-trade class is a CANDIDATE, never VERIFIED.
Writes only to its own output folder.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from .volume_recon import TOTAL_REPORTS, TRADING_UNIT, _total, component_table

ELIGIBILITY_VERSION = "0.1.0"
COVERAGE_REPORTS = {"MI_INDEX_ALLBUT0999", "BFT41U", "TWT53U", "TWTC7U", "dailyQuotes_EW", "fixPricing",
                    "oddQuote", "odd_Daily"}          # dense reports (block trades are naturally sparse)
COVERAGE_MIN_RATIO = 0.8
MIN_COMPLETE_DAYS = 30
VERIFIED_EXACT_PCT = 99.0
NO_TRADE = "CANDIDATE_NO_BOARD_LOT_TRADE"
DAY_LEVEL = "DAY_LEVEL_MISSING_VOLUME"


# ----------------------------------------------------------------------------------------------- inputs
def load_inputs(ledger_dir: Path, recon_dir: Path) -> dict:
    days = pd.read_parquet(Path(ledger_dir) / "gap_day_summary.parquet")
    for c in ("manifest_seq", "day_status", "liquidity_bucket"):
        if c not in days.columns:
            days[c] = None
    days["date"] = pd.to_datetime(days["date"]).dt.strftime("%Y-%m-%d")
    recon = pd.read_parquet(Path(recon_dir) / "volume_recon_symbol_day.parquet")
    summ = json.loads((Path(recon_dir) / "volume_recon_summary.json").read_text(encoding="utf-8"))
    by_date = pd.read_csv(Path(recon_dir) / "identity_by_date.csv")
    lsum_p = Path(ledger_dir) / "gap_ledger_summary.json"
    lsum = json.loads(lsum_p.read_text(encoding="utf-8")) if lsum_p.exists() else {}
    return {"days": days, "recon": recon, "summary": summ, "by_date": by_date, "ledger_summary": lsum}


def shard_inventory(root: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """requests of every shard (+ shard path and completeness) and the official totals of every symbol."""
    reqs, comps, shards = [], [], {}
    for sd in sorted((Path(root) / "shards").glob("year=*/date=*")):
        d = sd.name.split("=", 1)[1]
        complete = all((sd / f).exists() for f in ("_SUCCESS", "requests.parquet", "components.parquet"))
        shards[d] = {"path": str(sd), "complete": complete}
        if (sd / "requests.parquet").exists():
            r = pd.read_parquet(sd / "requests.parquet")
            r["shard"] = str(sd)
            r["shard_complete"] = complete
            reqs.append(r)
        if complete:
            comps.append(pd.read_parquet(sd / "components.parquet"))
    rq = pd.concat(reqs, ignore_index=True) if reqs else pd.DataFrame()
    comp = pd.concat(comps, ignore_index=True) if comps else pd.DataFrame(columns=["symbol", "date"])
    return rq, comp, shards


def official_totals(comp: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ex in ("TWSE", "TPEX"):
        cols = [f"rep_{r}" for r in TOTAL_REPORTS[ex] if f"rep_{r}" in comp.columns]
        if not cols:
            continue
        t = comp[["symbol", "date", *cols]].copy()
        t["official_total"] = _total(t, ex)
        t = t[t["official_total"].notna()]
        rows.append(t[["symbol", "date", "official_total"]].assign(exchange=ex))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["symbol", "date", "official_total",
                                                                                 "exchange"])


# ------------------------------------------------------------------------------------------------ phase 1
def reproduce(inp: dict, rq: pd.DataFrame, shards: dict) -> dict:
    days, recon, summ = inp["days"], inp["recon"], inp["summary"]
    normal = days[days["expected_policy"] == "normal_template"]
    mx = recon[recon["missing_slots"] > 0]
    by_class = mx.groupby("volume_bound_class").agg(symbol_days=("symbol", "size"),
                                                    missing_slots=("missing_slots", "sum")).reset_index()
    stated = pd.DataFrame(summ["missing_days"]["by_class"])[["volume_bound_class", "symbol_days", "missing_slots"]]
    cmp = by_class.merge(stated, on="volume_bound_class", how="outer", suffixes=("_recount", "_stated")).fillna(0)
    cmp["match"] = (cmp["symbol_days_recount"] == cmp["symbol_days_stated"]) & \
        (cmp["missing_slots_recount"] == cmp["missing_slots_stated"])
    ledger_dates = sorted(days["date"].unique())
    missing_shards = [d for d in ledger_dates if not shards.get(d, {}).get("complete")]
    issues = []
    total_gap = int(normal["missing_slots"].sum())
    recon_gap = int(mx["missing_slots"].sum())
    if total_gap != recon_gap:
        issues.append(f"ledger missing slots {total_gap} != reconciliation missing slots {recon_gap}")
    if not bool(cmp["match"].all()):
        issues.append("class table recount differs from the stated summary")
    unexplained = int(mx.loc[mx["volume_bound_class"].str.startswith("UNRESOLVED_IDENTITY"), "missing_slots"].sum())
    bd = inp["by_date"]
    s3454 = recon[(recon["symbol"] == "3454")].sort_values("date")
    near = s3454[(s3454["date"] >= "2026-03-20") & (s3454["date"] <= "2026-04-10")]
    lsum = inp["ledger_summary"].get("meta", {})
    return {
        "ok": not issues, "issues": issues,
        "ledger": {"ledger_id": lsum.get("ledger_id"), "ledger_git_sha": lsum.get("git_sha"),
                   "sources": lsum.get("sources"), "normal_symbol_days": int(len(normal)),
                   "expected_slots": int(normal["expected_slots"].sum()), "missing_slots": total_gap},
        "reconciliation": {"git_sha": summ.get("git_sha"), "recon_version": summ.get("recon_version"),
                           "missing_slots": recon_gap, "unexplained_slots_identity_not_verified": unexplained,
                           "class_table": cmp.to_dict("records")},
        "exchange_dates": {"total": int(len(bd)), "by_exchange": bd["exchange"].value_counts().to_dict(),
                           "note": "units are exchange x date; one calendar date counts once per exchange",
                           "unverified_not_whole_lot_pattern": int((bd["date_pattern"] == "formula_unverified").sum()),
                           "unverified_calendar_dates": int(bd.loc[bd["date_pattern"] == "formula_unverified",
                                                                   "date"].nunique())},
        "reference_shards": {"ledger_dates": len(ledger_dates), "complete": len(ledger_dates) - len(missing_shards),
                             "missing_or_incomplete": missing_shards,
                             "requests": int(len(rq)), "non_200": int((rq["http_status"] != 200).sum()) if len(rq) else 0},
        "open_items": {
            "3454_2026-03-30": near[["date", "exchange", "official_total", "m_volume_shares", "missing_slots",
                                     "volume_bound_class"]].to_dict("records"),
        },
        "code_git_sha": git_sha(),
    }


# ------------------------------------------------------------------------------------------------ phase 2
def coverage(rq: pd.DataFrame) -> pd.DataFrame:
    """rows parsed per report and date vs the median of the surrounding 21 trading days of that report."""
    r = rq[["exchange", "report", "date", "http_status", "parsed_rows", "json"]].copy()
    r = r.sort_values(["report", "date"])
    r["neighbour_median"] = r.groupby("report")["parsed_rows"].transform(
        lambda s: s.rolling(21, center=True, min_periods=5).median())
    r["coverage_ratio"] = r["parsed_rows"] / r["neighbour_median"].where(r["neighbour_median"] > 0)
    r["checked"] = r["report"].isin(COVERAGE_REPORTS)
    r["incomplete"] = r["checked"] & ((r["http_status"] != 200) | (r["coverage_ratio"].fillna(1) < COVERAGE_MIN_RATIO))
    return r


def verify_raw(rq: pd.DataFrame, dates: set[str], sample_frac: float = 0.05, seed: int = 7) -> pd.DataFrame:
    from ..reference.official_volume import read_raw
    pick = rq[rq["date"].isin(dates)]
    rest = rq[~rq["date"].isin(dates)]
    if len(rest) and sample_frac > 0:
        pick = pd.concat([pick, rest.sample(frac=min(1.0, sample_frac), random_state=seed)])
    out = []
    for r in pick.itertuples(index=False):
        p = Path(r.shard) / r.raw_file
        if not p.exists():
            out.append({"exchange": r.exchange, "date": r.date, "report": r.report, "raw_exists": False,
                        "sha_ok": False})
            continue
        out.append({"exchange": r.exchange, "date": r.date, "report": r.report, "raw_exists": True,
                    "sha_ok": hashlib.sha256(read_raw(p)).hexdigest() == r.sha256})
    return pd.DataFrame(out, columns=["exchange", "date", "report", "raw_exists", "sha_ok"])


def reparse_compare(rq: pd.DataFrame, dates: set[str]) -> pd.DataFrame:
    """Parse the saved raw responses again with the current parser; compare with components.parquet."""
    from ..reference import official_volume as OV
    eps = {e.report: e for e in OV.ENDPOINTS}
    ex_of = dict(zip(rq["report"], rq["exchange"]))
    rows = []
    for d in sorted(dates):
        g = rq[(rq["date"] == d) & (rq["http_status"] == 200)]
        if g.empty:
            continue
        frames = []
        for r in g.itertuples(index=False):
            p = Path(r.shard) / r.raw_file
            if r.report in eps and p.exists():
                df, _ = OV.parse_response(eps[r.report], date.fromisoformat(d), OV.read_raw(p))
                if len(df):
                    frames.append(df)
        new = component_table(pd.concat(frames, ignore_index=True)) if frames else pd.DataFrame(columns=["symbol"])
        old = pd.read_parquet(Path(g["shard"].iloc[0]) / "components.parquet")
        cols = sorted((set(new.columns) | set(old.columns)) - {"symbol", "date"})
        m = old.merge(new, on=["symbol", "date"], how="outer", suffixes=("_old", "_new"))
        for c in cols:
            a = m.get(f"{c}_old", pd.Series(np.nan, index=m.index))
            b = m.get(f"{c}_new", pd.Series(np.nan, index=m.index))
            diff = ~((a.isna() & b.isna()) | ((a - b).abs() < 1e-6))
            rows.append({"exchange": ex_of.get(c[4:], "unknown"), "date": d, "report": c[4:], "symbols_old": int(a.notna().sum()),
                         "symbols_new": int(b.notna().sum()), "cells_differ": int(diff.sum())})
    return pd.DataFrame(rows, columns=["exchange", "date", "report", "symbols_old", "symbols_new", "cells_differ"])


def residual_type(r: pd.Series) -> str:
    nz = r[r.abs() >= 1]
    if nz.empty:
        return "NONE"
    if (nz <= -1).any():
        return "HAS_NEGATIVE"
    whole = (nz % TRADING_UNIT) == 0
    if whole.all():
        return "WHOLE_LOT_POSITIVE"
    return "NON_WHOLE" if whole.mean() < 0.5 else "MIXED_WHOLE_NONWHOLE"


def exchange_date_quality(recon: pd.DataFrame, by_date: pd.DataFrame, cov: pd.DataFrame,
                          raw: pd.DataFrame, rep: pd.DataFrame, checked_at: str) -> pd.DataFrame:
    cd = recon[recon["complete_day"] & ~recon["unit_unverified"] & recon["official_regular"].notna()]
    rows = []
    for (ex, d), g in cd.groupby(["exchange", "date"]):
        r = g["residual_shares"]
        nz = r[r.abs() >= 1]
        rows.append({"exchange": ex, "date": d, "n_complete_days": int(len(g)),
                     "exact_pct": round(100 * float((r.abs() < 1).mean()), 3),
                     "n_negative": int((r <= -1).sum()), "n_positive_whole_lot": int(((nz > 0) & (nz % TRADING_UNIT == 0)).sum()),
                     "n_positive_non_whole": int(((nz > 0) & (nz % TRADING_UNIT != 0)).sum()),
                     "residual_type": residual_type(r)})
    q = pd.DataFrame(rows)
    q = q.merge(by_date[["exchange", "date", "date_pattern"]].rename(columns={"date_pattern": "pattern_v1"}),
                on=["exchange", "date"], how="outer")
    exc = cov[cov["incomplete"]].groupby(["exchange", "date"])["report"].agg(lambda s: "|".join(sorted(s))) \
        .rename("incomplete_reports").reset_index()
    cmin = cov[cov["checked"]].groupby(["exchange", "date"])["coverage_ratio"].min().rename("min_coverage_ratio") \
        .reset_index()
    q = q.merge(exc, on=["exchange", "date"], how="left").merge(cmin, on=["exchange", "date"], how="left")
    st = cov.groupby(["exchange", "date"]).agg(requests=("report", "size"),
                                              non_200=("http_status", lambda s: int((s != 200).sum()))).reset_index()
    q = q.merge(st, on=["exchange", "date"], how="left")
    if len(raw):
        rv = raw.groupby(["exchange", "date"]).agg(raw_checked=("sha_ok", "size"),
                                                   raw_bad=("sha_ok", lambda s: int((~s.astype(bool)).sum()))).reset_index()
        q = q.merge(rv, on=["exchange", "date"], how="left")
    else:
        q["raw_checked"], q["raw_bad"] = 0, 0
    if len(rep):
        rp = rep[rep["cells_differ"] > 0].groupby(["exchange", "date"])["report"] \
            .agg(lambda s: "|".join(sorted(s))).rename("reparse_differs").reset_index()
        q = q.merge(rp, on=["exchange", "date"], how="left")
    else:
        q["reparse_differs"] = np.nan
    q["incomplete_reports"] = q["incomplete_reports"].fillna("")
    q["reference_completeness"] = np.where(q["requests"].isna(), "NO_REFERENCE",
                                           np.where(q["incomplete_reports"] != "", "INCOMPLETE", "COMPLETE"))
    q["raw_evidence_status"] = np.select(
        [q["requests"].isna(), q["non_200"].fillna(0) > 0, q["raw_bad"].fillna(0) > 0, q["raw_checked"].fillna(0) > 0],
        ["MISSING", "REQUEST_FAILED", "HASH_MISMATCH", "HASH_VERIFIED"], default="PRESENT_NOT_HASH_CHECKED")
    n = q["n_complete_days"].fillna(0)
    q["identity_status"] = np.select(
        [q["reference_completeness"] == "NO_REFERENCE", n < MIN_COMPLETE_DAYS, q["n_negative"] > 0,
         q["exact_pct"] >= VERIFIED_EXACT_PCT],
        ["NO_REFERENCE", "INSUFFICIENT_COMPLETE_DAYS", "OVERCOUNT_PRESENT", "VERIFIED"], default="NO_OVERCOUNT")
    q["category_v1"] = np.where(q["pattern_v1"] != "formula_unverified", q["pattern_v1"], np.select(
        [(q["residual_type"] == "WHOLE_LOT_POSITIVE"), q["n_negative"] > 0, q["residual_type"] == "MIXED_WHOLE_NONWHOLE"],
        ["A_whole_lot_only_below60", "B_negative", "C_mixed"], default="D_non_whole"))
    q["exclusion_reason"] = np.select(
        [q["identity_status"] == "NO_REFERENCE", q["raw_evidence_status"].isin(["REQUEST_FAILED", "HASH_MISMATCH"]),
         q["identity_status"] == "INSUFFICIENT_COMPLETE_DAYS", q["identity_status"] == "OVERCOUNT_PRESENT",
         q["identity_status"] == "NO_OVERCOUNT"],
        ["no official reference", "official raw evidence failed", "fewer than 30 complete days",
         "negative residuals on complete days (over-count or over-subtraction)",
         "identity < 99 % exact (Level A excluded; Level B allowed)"], default="")
    q["verified_at"] = checked_at
    return q.sort_values(["exchange", "date"]).reset_index(drop=True)


def component_suspects(recon: pd.DataFrame, q: pd.DataFrame) -> pd.DataFrame:
    """For exchange-dates with non-whole residuals: share of non-whole symbols lacking each component,
    vs the share among exact symbols (a component missing only for the non-whole ones is the suspect)."""
    rep_cols = [c for c in recon.columns if c.startswith("rep_")]
    tgt = q[q["residual_type"].isin(["NON_WHOLE", "MIXED_WHOLE_NONWHOLE"])][["exchange", "date"]]
    cd = recon[recon["complete_day"] & ~recon["unit_unverified"]].merge(tgt, on=["exchange", "date"])
    rows = []
    for (ex, d), g in cd.groupby(["exchange", "date"]):
        r = g["residual_shares"]
        nw = g[(r > 0) & (r % TRADING_UNIT != 0)]
        ex_ = g[r.abs() < 1]
        best, gap = None, 0.0
        for c in rep_cols:
            if g[c].notna().sum() == 0:
                continue
            a = float(nw[c].isna().mean()) if len(nw) else np.nan
            b = float(ex_[c].isna().mean()) if len(ex_) else np.nan
            if pd.notna(a) and pd.notna(b) and a - b > gap:
                best, gap = c[4:], a - b
        rows.append({"exchange": ex, "date": d, "n_non_whole": int(len(nw)),
                     "non_whole_median_shares": float(nw["residual_shares"].median()) if len(nw) else np.nan,
                     "suspect_component_absent": best, "absent_share_gap": round(gap, 3)})
    return pd.DataFrame(rows, columns=["exchange", "date", "n_non_whole", "non_whole_median_shares",
                                       "suspect_component_absent", "absent_share_gap"])


def classify_v2(recon: pd.DataFrame, q: pd.DataFrame) -> pd.DataFrame:
    x = recon.merge(q[["exchange", "date", "identity_status", "reference_completeness"]], on=["exchange", "date"],
                    how="left")
    r = x["residual_shares"]
    ok = x["identity_status"].isin(["VERIFIED", "NO_OVERCOUNT"])
    level = np.where(x["identity_status"] == "VERIFIED", "official_upper_bound@verified_exchange_date",
                     "official_upper_bound@no_overcount_exchange_date")
    whole = (r >= TRADING_UNIT) & ((r % TRADING_UNIT) == 0)
    has_missing = x["missing_slots"] > 0
    x["class_v2"] = np.select(
        [~has_missing, x["unit_unverified"], x["official_total"].isna(),
         x["identity_status"] == "OVERCOUNT_PRESENT", ~ok,
         r <= -1, (r >= 0) & (r < TRADING_UNIT),
         whole & (x["reference_completeness"] == "COMPLETE"),
         x["reference_completeness"] != "COMPLETE"],
        ["NO_MISSING_BARS", "UNIT_UNVERIFIED_EXCLUDED", "UNRESOLVED_NO_OFFICIAL_RECORD",
         "UNRESOLVED_OVERCOUNT_EXCHANGE_DATE", "UNRESOLVED_IDENTITY_INSUFFICIENT",
         "UNRESOLVED_VOLUME_EXCESS", NO_TRADE, DAY_LEVEL, "UNRESOLVED_REFERENCE_INCOMPLETE"],
        default="UNRESOLVED_RESIDUAL_NOT_WHOLE_LOT")
    x["evidence_status"] = np.select([x["class_v2"] == "NO_MISSING_BARS", x["class_v2"].isin([NO_TRADE, DAY_LEVEL])],
                                     ["NOT_APPLICABLE", "CANDIDATE"], default="UNRESOLVED")
    x["evidence_level"] = np.where(x["class_v2"].isin([NO_TRADE, DAY_LEVEL]), level, "")
    x["reason_v2"] = np.select(
        [x["class_v2"] == NO_TRADE, x["class_v2"] == DAY_LEVEL],
        ["0 <= official regular - stored 5m < 1 lot and no over-count on this exchange-date",
         "stored 5m short by whole lots; reference complete; not located to a bar"], default="")
    return x


# ------------------------------------------------------------------------------------------------ phase 3
def symbol_day_quality(days: pd.DataFrame, x: pd.DataFrame, totals: pd.DataFrame, q: pd.DataFrame) -> pd.DataFrame:
    keep = ["symbol", "date", "exchange", "official_total", "official_regular", "m_volume_shares",
            "residual_shares", "unit_unverified", "volume_bound_class", "class_v2", "evidence_status",
            "evidence_level", "reason_v2", "identity_status"]
    s = days[["symbol", "date", "expected_policy", "liquidity_bucket", "expected_slots", "real_bars",
              "missing_slots", "day_status", "manifest_seq"]].merge(x[keep], on=["symbol", "date"], how="left")
    exmap = x.groupby("symbol")["exchange"].agg(lambda e: e.mode().iat[0] if len(e.mode()) else "unknown")
    s["exchange"] = s["exchange"].fillna(s["symbol"].map(exmap)).fillna("unknown")
    s["session_type"] = s["expected_policy"]
    # point-in-time: the previous trading day of the exchange (official reports known at 20:00 that day)
    cal = q[["exchange", "date"]].drop_duplicates().sort_values(["exchange", "date"])
    cal["prev_trading_date"] = cal.groupby("exchange")["date"].shift(1)
    s = s.merge(cal, on=["exchange", "date"], how="left")
    pt = totals.rename(columns={"date": "prev_trading_date", "official_total": "prev_official_total_shares"})
    s = s.merge(pt[["symbol", "exchange", "prev_trading_date", "prev_official_total_shares"]],
                on=["symbol", "exchange", "prev_trading_date"], how="left")
    s["pit_eligible"] = s["prev_trading_date"].notna() & s["prev_official_total_shares"].notna()
    r = s["residual_shares"]
    ok = s["identity_status"].isin(["VERIFIED", "NO_OVERCOUNT"])
    s["official_regular_volume"] = s["official_regular"]
    s["fubon_intraday_volume"] = s["m_volume_shares"]
    s["intraday_volume_shortfall"] = np.where(ok & (r >= TRADING_UNIT) & ((r % TRADING_UNIT) == 0), r,
                                              np.where(ok & (r >= 0) & (r < TRADING_UNIT), 0.0, np.nan))
    s["volume_reliable"] = np.select([~ok | s["unit_unverified"].fillna(True).astype(bool),
                                      (r >= 0) & (r < TRADING_UNIT)], ["UNKNOWN", "TRUE"], default="FALSE")
    s["price_reliable"] = "UNKNOWN"      # no official price comparison in v0.1
    s["missing_bar_evidence_status"] = np.where(s["missing_slots"].fillna(0) == 0, "NONE_MISSING",
                                                s["class_v2"].fillna("NOT_RECONCILED"))
    normal = s["session_type"] == "normal_template"
    full = (s["missing_slots"] == 0) & (s["real_bars"] == s["expected_slots"])
    exact = r.abs() < 1
    unit_ok = ~s["unit_unverified"].fillna(True).astype(bool) & s["official_total"].notna()
    a_rules = [("session not normal", normal), ("not all expected bars", full),
               ("unit unverified or no official record", unit_ok),
               ("exchange-date identity not VERIFIED", s["identity_status"] == "VERIFIED"),
               ("symbol residual not exactly 0", exact), ("no previous-day official record (PIT)", s["pit_eligible"])]
    b_rules = [("session not normal", normal), ("unit unverified or no official record", unit_ok),
               ("exchange-date identity not VERIFIED/NO_OVERCOUNT", ok),
               ("missing bars without no-trade candidate evidence",
                (s["missing_slots"] == 0) | (s["class_v2"] == NO_TRADE)),
               ("intraday volume not reliable", s["volume_reliable"] == "TRUE"),
               ("no previous-day official record (PIT)", s["pit_eligible"])]
    s["eligible_level_A"] = np.logical_and.reduce([m.fillna(False).astype(bool) for _, m in a_rules])
    s["eligible_level_B"] = np.logical_and.reduce([m.fillna(False).astype(bool) for _, m in b_rules])
    s["level_B_basis"] = np.where(s["eligible_level_B"] & (s["missing_slots"] > 0),
                                  "CANDIDATE_NO_TRADE:" + s["evidence_level"].fillna(""), "")

    def first_fail(rules):
        out = pd.Series("", index=s.index, dtype=object)
        for name, m in reversed(rules):
            out = out.where(m.fillna(False).astype(bool), name)
        return out
    s["exclusion_reason_A"] = first_fail(a_rules)
    s["exclusion_reason_B"] = first_fail(b_rules)
    s["exclusion_reason"] = s["exclusion_reason_B"].where(s["exclusion_reason_B"] != "", s["exclusion_reason_A"])
    s = s.rename(columns={"real_bars": "real_bar_count", "missing_slots": "missing_bar_count"})
    cols = ["symbol", "exchange", "date", "session_type", "liquidity_bucket", "real_bar_count", "missing_bar_count",
            "official_total", "official_regular_volume", "fubon_intraday_volume", "residual_shares",
            "intraday_volume_shortfall", "missing_bar_evidence_status", "evidence_status", "evidence_level",
            "volume_reliable", "price_reliable", "prev_trading_date", "prev_official_total_shares", "pit_eligible",
            "eligible_level_A", "eligible_level_B", "level_B_basis", "exclusion_reason_A", "exclusion_reason_B",
            "exclusion_reason", "identity_status", "volume_bound_class", "manifest_seq"]
    return s[cols].rename(columns={"volume_bound_class": "class_v1"})


def shortfall_report(s: pd.DataFrame) -> dict[str, pd.DataFrame]:
    v = s[(s["session_type"] == "normal_template") & s["identity_status"].isin(["VERIFIED", "NO_OVERCOUNT"])
          & (s["official_regular_volume"] > 0)].copy()
    v["month"] = v["date"].str[:7]
    v["year"] = v["date"].str[:4]
    v["short"] = v["intraday_volume_shortfall"].fillna(0) > 0
    v["complete_short"] = v["short"] & (v["missing_bar_count"] == 0)

    def agg(g):
        return g.agg(symbol_days=("symbol", "size"), short_symbol_days=("short", "sum"),
                     complete_day_short=("complete_short", "sum"),
                     shortfall_shares=("intraday_volume_shortfall", "sum"),
                     official_regular_shares=("official_regular_volume", "sum")).reset_index()
    out = {}
    for k, keys in {"by_year": ["year", "exchange"], "by_month": ["month", "exchange"],
                    "by_liquidity_2026": ["liquidity_bucket", "exchange"]}.items():
        src = v[v["year"] == "2026"] if k.endswith("2026") else v
        t = agg(src.groupby(keys))
        t["short_pct_symbol_days"] = (100 * t["short_symbol_days"] / t["symbol_days"]).round(3)
        t["shortfall_pct_volume"] = (100 * t["shortfall_shares"] / t["official_regular_shares"]).round(4)
        out[k] = t
    top = v[(v["year"] == "2026") & v["short"]].groupby(["symbol", "exchange"]).agg(
        short_days=("date", "size"), shortfall_shares=("intraday_volume_shortfall", "sum")).reset_index() \
        .sort_values("shortfall_shares", ascending=False).head(100)
    out["top_symbols_2026"] = top
    return out


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def run(ledger_dir: Path, recon_dir: Path, dataset_root: Path, out_root: Path, data_root: Path | None = None,
        sample_frac: float = 0.05, log=print) -> Path:
    checked_at = datetime.now(TAIPEI).isoformat(timespec="seconds")
    out = Path(out_root) / f"eligibility_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    if data_root:
        from ..config import load_config
        from ..paths import Layout
        from .runner import _guard_out_dir
        _guard_out_dir(Layout.from_config(load_config(None, {"data_root": str(data_root)})), out)
    out.mkdir(parents=True)
    log("phase 1: loading inputs")
    inp = load_inputs(ledger_dir, recon_dir)
    rq, comp, shards = shard_inventory(dataset_root)
    p1 = reproduce(inp, rq, shards)
    seq = None
    if data_root:
        from ..config import load_config
        from ..paths import Layout
        from .reader import load_published
        lay = Layout.from_config(load_config(None, {"data_root": str(data_root)}))
        seq = load_published("fubon_5m", lay.dataset_dirs["fubon_5m"]).seq
    p1["published_fubon_5m_seq"] = seq
    (out / "phase1_reproduce.json").write_text(json.dumps(p1, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log(f"phase 1: ok={p1['ok']} issues={p1['issues']}")
    if not p1["ok"]:
        log("reproduction failed: no re-classification (see phase1_reproduce.json)")
        return out
    log("phase 2: coverage, raw evidence, re-parse, residual structure")
    cov = coverage(rq)
    unver = set(inp["by_date"].loc[inp["by_date"]["date_pattern"] == "formula_unverified", "date"])
    raw = verify_raw(rq, unver, sample_frac)
    bd = inp["by_date"]
    pre = cov[cov["incomplete"]]["date"]
    d_dates = set(bd.loc[(bd["date_pattern"] == "formula_unverified")
                         & (bd["whole_lot_residual_pct"].fillna(0) < 50), "date"]) | set(pre)
    rep = reparse_compare(rq, d_dates)
    q = exchange_date_quality(inp["recon"], bd, cov, raw, rep, checked_at)
    sus = component_suspects(inp["recon"], q)
    q = q.merge(sus, on=["exchange", "date"], how="left")
    x = classify_v2(inp["recon"], q)
    log("phase 3: products")
    totals = official_totals(comp)
    s = symbol_day_quality(inp["days"], x, totals, q)
    q.to_parquet(out / "exchange_date_quality.parquet", index=False)
    s.to_parquet(out / "symbol_day_quality.parquet", index=False)
    cov.to_csv(out / "reference_coverage.csv", index=False, encoding="utf-8-sig")
    raw.to_csv(out / "raw_hash_check.csv", index=False, encoding="utf-8-sig")
    rep.to_csv(out / "reparse_compare.csv", index=False, encoding="utf-8-sig")
    # deliverable 1: class diff
    mx = x[x["missing_slots"] > 0]
    diff = mx.groupby(["volume_bound_class", "class_v2"]).agg(symbol_days=("symbol", "size"),
                                                              missing_slots=("missing_slots", "sum")).reset_index()
    diff.to_csv(out / "class_diff_v1_v2.csv", index=False, encoding="utf-8-sig")
    v2 = mx.groupby(["class_v2", "evidence_status"]).agg(symbol_days=("symbol", "size"),
                                                         missing_slots=("missing_slots", "sum")).reset_index()
    v2["pct_slots"] = (100 * v2["missing_slots"] / v2["missing_slots"].sum()).round(3)
    v2.to_csv(out / "class_v2_summary.csv", index=False, encoding="utf-8-sig")
    # deliverable 2: unresolved exchange-dates + minimum evidence
    unres = q[q["identity_status"] != "VERIFIED"].copy()
    mx = mx.assign(_u=np.where(mx["evidence_status"] == "UNRESOLVED", mx["missing_slots"], 0),
                   _c=np.where(mx["evidence_status"] == "CANDIDATE", mx["missing_slots"], 0))
    slots = mx.groupby(["exchange", "date"]).agg(unresolved_slots=("_u", "sum"), candidate_slots=("_c", "sum")) \
        .reset_index()
    unres = unres.merge(slots, on=["exchange", "date"], how="left")
    unres["minimum_evidence_needed"] = np.select(
        [unres["identity_status"] == "NO_REFERENCE", unres["reference_completeness"] == "INCOMPLETE",
         unres["identity_status"] == "OVERCOUNT_PRESENT", unres["identity_status"] == "INSUFFICIENT_COMPLETE_DAYS"],
        ["fetch the official reports of this exchange-date (1 shard, 11 requests)",
         "re-fetch the incomplete report(s) listed for this date (1 request each)",
         "explain the negative residual symbols: official revision notice, Fubon D candle for those symbols, "
         "FinLab daily volume; until then zero residuals on this date are not evidence",
         "none available offline (too few complete days)"],
        default="review only: no-overcount rule applies (Level B, not Level A)")
    unres.to_csv(out / "unresolved_exchange_dates.csv", index=False, encoding="utf-8-sig")
    refetch = cov[cov["incomplete"]][["exchange", "date", "report", "http_status", "parsed_rows", "neighbour_median"]]
    miss = pd.DataFrame({"exchange": "ALL", "date": p1["reference_shards"]["missing_or_incomplete"],
                         "report": "all batch reports"})
    pd.concat([miss, refetch], ignore_index=True).to_csv(out / "refetch_minimal.csv", index=False, encoding="utf-8-sig")
    # deliverable 3: 2026 shortfall report
    rpt = shortfall_report(s)
    for k, t in rpt.items():
        t.to_csv(out / f"volume_shortfall_{k}.csv", index=False, encoding="utf-8-sig")
    # deliverable 4: eligibility counts + selection comparison
    n = s[s["session_type"] == "normal_template"].copy()
    n["year"] = n["date"].str[:4]
    cnt = n.groupby(["year", "exchange"]).agg(symbol_days=("symbol", "size"), level_A=("eligible_level_A", "sum"),
                                             level_B=("eligible_level_B", "sum")).reset_index()
    cnt["level_A_pct"] = (100 * cnt["level_A"] / cnt["symbol_days"]).round(2)
    cnt["level_B_pct"] = (100 * cnt["level_B"] / cnt["symbol_days"]).round(2)
    cnt.to_csv(out / "eligibility_counts.csv", index=False, encoding="utf-8-sig")
    sel = n.groupby(["liquidity_bucket"]).agg(symbol_days=("symbol", "size"), level_A=("eligible_level_A", "sum"),
                                             level_B=("eligible_level_B", "sum")).reset_index()
    sel["level_A_pct"] = (100 * sel["level_A"] / sel["symbol_days"]).round(2)
    sel["level_B_pct"] = (100 * sel["level_B"] / sel["symbol_days"]).round(2)
    sel.to_csv(out / "selection_by_liquidity.csv", index=False, encoding="utf-8-sig")
    reasons = pd.concat([n.loc[~n["eligible_level_A"], "exclusion_reason_A"].value_counts().rename("level_A"),
                         n.loc[~n["eligible_level_B"], "exclusion_reason_B"].value_counts().rename("level_B")], axis=1) \
        .fillna(0).astype(int)
    reasons.to_csv(out / "exclusion_reasons.csv", encoding="utf-8-sig")
    neg_rate = float((inp["recon"].loc[inp["recon"]["complete_day"] & ~inp["recon"]["unit_unverified"],
                                       "residual_shares"] <= -1).mean())
    files = {f: _sha(out / f) for f in ("exchange_date_quality.parquet", "symbol_day_quality.parquet")}
    excl_ed = q.loc[~q["identity_status"].isin(["VERIFIED", "NO_OVERCOUNT"]), ["exchange", "date", "identity_status",
                                                                               "exclusion_reason"]]
    lv = q[q["identity_status"] == "VERIFIED"]
    man = {
        "eligibility_version": ELIGIBILITY_VERSION, "code_commit": git_sha(), "created_at": checked_at,
        "inputs": {"ledger_dir": str(ledger_dir), "ledger": p1["ledger"], "published_fubon_5m_seq": seq,
                   "reconciliation_dir": str(recon_dir), "reconciliation": {k: p1["reconciliation"][k] for k in
                                                                         ("git_sha", "recon_version")},
                   "official_reference_root": str(dataset_root), "reference_shards": {
                       k: v for k, v in p1["reference_shards"].items() if k != "missing_or_incomplete"},
                   "reference_shards_missing": p1["reference_shards"]["missing_or_incomplete"]},
        "allowed_date_ranges": {ex: {"first": g["date"].min(), "last": g["date"].max(), "verified_exchange_dates": len(g)}
                                for ex, g in lv.groupby("exchange")},
        "excluded_exchange_dates": excl_ed.to_dict("records"),
        "excluded_symbol_days": {"level_A": reasons["level_A"].to_dict(), "level_B": reasons["level_B"].to_dict()},
        "filter_rules": {
            "level_A": ["session normal", "all expected bars (54)", "unit verified and official record present", "exchange-date identity VERIFIED (>= 99 % exact)",
             "symbol residual exactly 0 (no day-level shortfall)", "previous trading day official record (PIT)"],
            "level_B": ["session normal", "unit verified and official record present",
                        "exchange-date identity VERIFIED or NO_OVERCOUNT",
                        "missing bars only with CANDIDATE_NO_BOARD_LOT_TRADE evidence",
                        "volume_reliable == TRUE (no unlocated intraday shortfall)",
                        "previous trading day official record (PIT)"],
            "rule_v2": "0 <= official regular - stored 5m < 1,000 shares on an exchange-date without negative residuals",
            "pit_note": "eligibility masks use same-day reconciliation (known after 20:00); they are data-availability "
                        "masks, not a tradable universe. Universe filters must use prev_official_total_shares.",
            "price_reliable": "UNKNOWN for all rows in v0.1 (no official price comparison yet)",
            "no_forward_fill": True},
        "assumption_checks": {"negative_residual_rate_complete_days": round(neg_rate, 6)},
        "row_counts": {"exchange_date_quality": int(len(q)), "symbol_day_quality": int(len(s)),
                       "level_A": int(s["eligible_level_A"].sum()), "level_B": int(s["eligible_level_B"].sum())},
        "sha256": files,
        "statement": "Evidence report and derived masks for review. Not research-ready until reviewed.",
    }
    (out / "backtest_eligibility_manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=2, default=str),
                                                            encoding="utf-8")
    md = ["# Eligibility v0.1 review", "", f"- code {git_sha()}, reconciliation {p1['reconciliation']['git_sha']}, "
          f"fubon_5m seq {seq}", f"- negative residual rate on complete days: {neg_rate:.6f}", "",
          "## class v2", "", "```", v2.to_string(index=False), "```", "", "## diff v1 -> v2", "", "```",
          diff.to_string(index=False), "```", "", "## exchange-date identity", "", "```",
          q.groupby(["exchange", "identity_status"]).size().unstack(fill_value=0).to_string(), "```", "",
          "## eligibility", "", "```", cnt.to_string(index=False), "```", "", "## exclusion reasons", "", "```",
          reasons.to_string(), "```", "", "## 2026 shortfall by month", "", "```",
          rpt["by_month"][rpt["by_month"]["month"] >= "2025-07"].to_string(index=False), "```"]
    (out / "REPORT.md").write_text("\n".join(md), encoding="utf-8")
    log(f"written to {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.eligibility")
    p.add_argument("--data-root", default=None)
    p.add_argument("--ledger-dir", required=True)
    p.add_argument("--recon-dir", required=True)
    p.add_argument("--dataset-root", required=True)
    p.add_argument("--out-dir", default="/content/tw_lake_eligibility")
    p.add_argument("--raw-sample-frac", type=float, default=0.05)
    a = p.parse_args(argv)
    out = run(Path(a.ledger_dir), Path(a.recon_dir), Path(a.dataset_root), Path(a.out_dir),
              Path(a.data_root) if a.data_root else None, a.raw_sample_frac)
    if (out / "REPORT.md").exists():
        print((out / "REPORT.md").read_text(encoding="utf-8"))
    else:
        print((out / "phase1_reproduce.json").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
