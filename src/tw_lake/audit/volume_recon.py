"""Volume-bound reconciliation pilot: official daily breakdown vs stored 5m volume (read-only).

python -m tw_lake.audit.volume_recon --probe-dir .../official_volume_probe_... \
    --ledger-dir .../gap_ledger_... --out-dir /content/tw_lake_volume_recon

Order of evidence (nothing is assumed):
1. Identity test on COMPLETE days (every expected 5m slot has a bar). For each exchange and each
   subset S of the available official components (after-hours fixed price, odd lot, block):
       residual_S = official_daily_total - sum(S) - stored_5m_shares
   The subset with the highest exact rate (|residual| < 1 share) is the measured definition of
   "official regular-session volume". It is ``verified`` only if exact on >= 99% of >= 50 complete days.
   FinLab daily shares are compared with the official total too (statistical definition check).
2. Complete-day residuals under that identity are reported (they must be explained, not assigned
   to missing bars).
3. Days with missing slots, only when the identity is verified for that exchange:
       residual = official_regular - stored_5m_shares
       |residual| < trading unit (1000 shares) -> CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND
       residual >= trading unit               -> DAY_LEVEL_MISSING_VOLUME_EVIDENCED (missing shares are
                                                 known for the day; WHICH slot is not -> slots stay UNRESOLVED)
       residual <= -1                          -> VOLUME_EXCESS_UNEXPLAINED
   Otherwise ``UNRESOLVED_IDENTITY_NOT_VERIFIED``. The identity is also checked per exchange and date
   (``identity_by_date``): only dates whose own complete days reconcile (>= 99% exact, >= 30 days) are
   classified; a failing date is an anomaly to explain, not evidence about its missing bars. F13 symbols (trading unit not verified) and
   symbol-days with 5m volume > 5x daily are ``UNIT_UNVERIFIED_EXCLUDED``.
All classes are candidates for review; the gap ledger is not modified.
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha

RECON_VERSION = "0.1.0"
TRADING_UNIT = 1000.0
F13_UNIT_UNVERIFIED = {"00636K", "00668K", "00657K", "00687C"}


def foreign_currency_etf(sym: pd.Series) -> pd.Series:
    """TWSE report notes: an ETF whose 6th code character is K, M, S or C trades in a foreign currency."""
    s = sym.astype(str)
    return s.str.startswith("00") & (s.str.len() == 6) & s.str[5].isin(list("KMSC"))
TOTAL_REPORTS = {"TWSE": ["MI_INDEX_ALLBUT0999"], "TPEX": ["dailyQuotes_EW", "old_daily_close_quotes"]}
COUNT_WORDS = ("筆數", "Transaction")


def component_table(parsed: pd.DataFrame) -> pd.DataFrame:
    """symbol-date x report shares (main share field of each report, summed over rows)."""
    p = parsed[~parsed["field"].str.contains("|".join(COUNT_WORDS))].copy()
    if p.empty:
        return pd.DataFrame(columns=["symbol", "date"])
    # main field per report: exact 成交股數 if present, else the first share field seen
    main = {}
    for rep, g in p.groupby("report"):
        fields = list(dict.fromkeys(g["field"]))
        main[rep] = next((f for f in fields if f.strip().startswith("成交股數")), fields[0])
    p = p[p.apply(lambda r: r["field"] == main[r["report"]], axis=1)]
    w = p.groupby(["symbol", "date", "report"])["shares"].sum().unstack("report")
    w.columns = [f"rep_{c}" for c in w.columns]
    out = w.reset_index()
    out.attrs["main_field"] = main
    return out


def _exchange_of(row_has: pd.DataFrame) -> pd.Series:
    tw = row_has[[c for c in row_has.columns if c in {f"rep_{r}" for r in TOTAL_REPORTS["TWSE"]}]].notna().any(axis=1)
    tp = row_has[[c for c in row_has.columns if c in {f"rep_{r}" for r in TOTAL_REPORTS["TPEX"]}]].notna().any(axis=1)
    return pd.Series(np.select([tw, tp], ["TWSE", "TPEX"], default="unknown"), index=row_has.index)


def _total(x: pd.DataFrame, ex: str) -> pd.Series:
    cols = [f"rep_{r}" for r in TOTAL_REPORTS[ex] if f"rep_{r}" in x.columns]
    if not cols:
        return pd.Series(np.nan, index=x.index)
    return x[cols].bfill(axis=1).iloc[:, 0]


def component_reports(x: pd.DataFrame, ex: str, components: dict) -> list[str]:
    tot = {f"rep_{r}" for r in TOTAL_REPORTS[ex]}
    return [c for c in x.columns if c.startswith("rep_") and c not in tot and components.get(c[4:], ("", ""))[0] == ex]


def identity_tests(x: pd.DataFrame, ex: str, comps: list[str]) -> pd.DataFrame:
    """Exact-rate of residual = total - sum(S) - m5 over complete days, for every subset S."""
    c = x[(x["exchange"] == ex) & x["complete_day"] & ~x["unit_unverified"] & x["official_total"].notna()]
    rows = []
    # block variants are alternatives, not additive: use at most one report per component kind
    for k in range(len(comps) + 1):
        for S in itertools.combinations(comps, k):
            kinds = [s.split("_")[1] if s.startswith("rep_BFIAUU") else s for s in S]
            if len(set(kinds)) < len(kinds):
                continue
            sub = c[list(S)].fillna(0).sum(axis=1) if S else 0.0
            res = c["official_total"] - sub - c["m_volume_shares"]
            rows.append({"exchange": ex, "subtract": "+".join(s[4:] for s in S) or "(none)", "n_complete_days": int(len(c)),
                         "exact_pct": round(100 * float((res.abs() < 1).mean()), 3) if len(c) else None,
                         "within_1_lot_pct": round(100 * float((res.abs() < TRADING_UNIT).mean()), 3) if len(c) else None,
                         "residual_p50": float(res.median()) if len(c) else None,
                         "residual_abs_p99": float(res.abs().quantile(0.99)) if len(c) else None,
                         "_subset": S})
    f = c["finlab_vs_official"] if len(c) else pd.Series(dtype=float)
    rows.append({"exchange": ex, "subtract": "FINLAB_vs_OFFICIAL_TOTAL", "n_complete_days": int(len(c)),
                 "exact_pct": round(100 * float((f.abs() < 1).mean()), 3) if len(c) else None,
                 "within_1_lot_pct": round(100 * float((f.abs() < TRADING_UNIT).mean()), 3) if len(c) else None,
                 "residual_p50": float(f.median()) if len(c) else None,
                 "residual_abs_p99": float(f.abs().quantile(0.99)) if len(c) else None, "_subset": None})
    return pd.DataFrame(rows)


def run_recon(probe_dir: Path, ledger_dir: Path, out_root: Path, min_days: int = 50, min_exact_pct: float = 99.0,
              parsed_file: Path | None = None, min_days_per_date: int = 30) -> Path:
    started = datetime.now(TAIPEI)
    out = Path(out_root) / f"volume_recon_{started:%Y%m%d_%H%M%S}"
    out.mkdir(parents=True, exist_ok=False)
    parsed = pd.read_parquet(Path(parsed_file) if parsed_file else Path(probe_dir) / "parsed_volume.parquet")
    reqs = pd.read_parquet(Path(probe_dir) / "requests.parquet")
    components = {r.report: (r.exchange, r.component) for r in reqs.drop_duplicates("report").itertuples()}
    comp = component_table(parsed)
    dates = sorted(parsed["date"].unique())
    days = pd.read_parquet(Path(ledger_dir) / "gap_day_summary.parquet",
                           filters=[("date", "in", [pd.Timestamp(d) for d in dates])])
    days = days[days["expected_policy"] == "normal_template"].copy()
    days["date"] = pd.to_datetime(days["date"]).dt.strftime("%Y-%m-%d")
    x = days.merge(comp, on=["symbol", "date"], how="left")
    x["exchange"] = _exchange_of(x)
    x["official_total"] = np.nan
    for ex in ("TWSE", "TPEX"):
        m = x["exchange"] == ex
        x.loc[m, "official_total"] = _total(x.loc[m], ex)
    x["finlab_vs_official"] = x["f_volume_shares"] - x["official_total"]
    x["complete_day"] = (x["missing_slots"] == 0) & (x["real_bars"] > 0)
    ratio = x["m_volume_shares"] / x["f_volume_shares"].where(x["f_volume_shares"] > 0)
    x["unit_unverified"] = x["symbol"].isin(F13_UNIT_UNVERIFIED) | foreign_currency_etf(x["symbol"]) | (ratio > 5)

    tests, identity = [], {}
    for ex in ("TWSE", "TPEX"):
        comps = component_reports(x, ex, components)
        t = identity_tests(x, ex, comps)
        tests.append(t)
        cand = t[t["_subset"].notna()].sort_values(["exact_pct", "within_1_lot_pct"], ascending=False)
        if len(cand):
            b = cand.iloc[0]
            identity[ex] = {"subtract": list(b["_subset"]), "exact_pct": b["exact_pct"],
                            "n_complete_days": int(b["n_complete_days"]),
                            "verified": bool(b["n_complete_days"] >= min_days and (b["exact_pct"] or 0) >= min_exact_pct)}
    tt = pd.concat(tests, ignore_index=True).drop(columns="_subset")
    tt.to_csv(out / "identity_tests.csv", index=False, encoding="utf-8-sig")

    x["official_regular"] = np.nan
    for ex, idn in identity.items():
        m = x["exchange"] == ex
        sub = x.loc[m, idn["subtract"]].fillna(0).sum(axis=1) if idn["subtract"] else 0.0
        x.loc[m, "official_regular"] = x.loc[m, "official_total"] - sub
    x["residual_shares"] = x["official_regular"] - x["m_volume_shares"].fillna(0)
    # the identity is verified per exchange AND date: a date whose complete days do not reconcile is an
    # anomaly to investigate, and its missing days are not classified (nothing is hidden: see identity_by_date)
    cd = x[x["complete_day"] & ~x["unit_unverified"] & x["official_regular"].notna()]
    by_date = cd.groupby(["exchange", "date"]).agg(
        n_complete_days=("symbol", "size"),
        exact_pct=("residual_shares", lambda r: round(100 * float((r.abs() < 1).mean()), 3)),
        residual_lots_median_nonzero=("residual_shares", lambda r: float((r[r.abs() >= 1] / TRADING_UNIT).median())
                                      if (r.abs() >= 1).any() else 0.0),
        whole_lot_residual_pct=("residual_shares", lambda r: round(100 * float(((r % TRADING_UNIT) == 0)[r.abs() >= 1].mean()), 1)
                                if (r.abs() >= 1).any() else None)).reset_index()
    by_date["date_verified"] = (by_date["n_complete_days"] >= min_days_per_date) & (by_date["exact_pct"] >= min_exact_pct) \
        & by_date["exchange"].map(lambda e: identity.get(e, {}).get("subtract") is not None)
    by_date.to_csv(out / "identity_by_date.csv", index=False, encoding="utf-8-sig")
    dv = set(zip(by_date.loc[by_date["date_verified"], "exchange"], by_date.loc[by_date["date_verified"], "date"]))
    verified = pd.Series([(e, d) in dv for e, d in zip(x["exchange"], x["date"])], index=x.index)
    has_missing = x["missing_slots"] > 0
    x["volume_bound_class"] = np.select(
        [~has_missing, x["unit_unverified"], x["official_total"].isna(), ~verified,
         x["residual_shares"] <= -1, x["residual_shares"].abs() < TRADING_UNIT, x["residual_shares"] >= TRADING_UNIT],
        ["NO_MISSING_SLOTS", "UNIT_UNVERIFIED_EXCLUDED", "UNRESOLVED_NO_OFFICIAL_RECORD", "UNRESOLVED_IDENTITY_NOT_VERIFIED",
         "VOLUME_EXCESS_UNEXPLAINED", "CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND", "DAY_LEVEL_MISSING_VOLUME_EVIDENCED"],
        default="UNRESOLVED_OTHER")
    x["unlocatable_missing_shares"] = np.where(x["volume_bound_class"] == "DAY_LEVEL_MISSING_VOLUME_EVIDENCED",
                                               x["residual_shares"], 0.0)
    keep = ["symbol", "date", "exchange", "liquidity_bucket", "day_status", "expected_slots", "real_bars", "missing_slots",
            "f_volume_shares", "official_total", "official_regular", "m_volume_shares", "residual_shares",
            "finlab_vs_official", "complete_day", "unit_unverified", "volume_bound_class", "unlocatable_missing_shares",
            *[c for c in x.columns if c.startswith("rep_")]]
    x[keep].to_parquet(out / "volume_recon_symbol_day.parquet", index=False)

    mx = x[has_missing]
    by_class = mx.groupby("volume_bound_class").agg(
        symbol_days=("symbol", "size"), missing_slots=("missing_slots", "sum"),
        unlocatable_missing_shares=("unlocatable_missing_shares", "sum")).reset_index()
    by_liq = mx.groupby(["liquidity_bucket", "volume_bound_class"]).agg(
        symbol_days=("symbol", "size"), missing_slots=("missing_slots", "sum"),
        unlocatable_missing_shares=("unlocatable_missing_shares", "sum")).reset_index()
    by_liq.to_csv(out / "by_liquidity.csv", index=False, encoding="utf-8-sig")
    comp_res = x[x["complete_day"] & ~x["unit_unverified"] & x["official_regular"].notna()].copy()
    comp_res["abs_res"] = comp_res["residual_shares"].abs()
    comp_res.sort_values("abs_res", ascending=False).head(200)[keep].to_csv(
        out / "complete_day_top_residuals.csv", index=False, encoding="utf-8-sig")
    explained = {"CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND", "DAY_LEVEL_MISSING_VOLUME_EVIDENCED"}
    ex_rows = mx[mx["volume_bound_class"].isin(explained)]
    priority = x[(x["liquidity_bucket"] == "ge5000_lots") & has_missing][keep].head(500)
    priority.to_csv(out / "priority_liquid_missing_days.csv", index=False, encoding="utf-8-sig")
    prov = x[x["day_status"] == "provider_missing_evidenced"][keep]
    prov.to_csv(out / "provider_missing_days.csv", index=False, encoding="utf-8-sig")
    summ = {
        "recon_version": RECON_VERSION, "git_sha": git_sha(), "probe_dir": str(probe_dir), "ledger_dir": str(ledger_dir),
        "parsed_file": str(parsed_file) if parsed_file else "parsed_volume.parquet",
        "unit_basis": parsed.groupby(["report", "multiplier_basis"]).size().rename("n").reset_index().to_dict("records")
        if "multiplier_basis" in parsed.columns else None,
        "dates": dates, "symbol_days_in_scope": int(len(x)), "exchange_counts": x["exchange"].value_counts().to_dict(),
        "identity": identity,
        "identity_by_date": by_date.to_dict("records"),
        "finlab_vs_official_on_complete_days": tt[tt["subtract"] == "FINLAB_vs_OFFICIAL_TOTAL"].to_dict("records"),
        "complete_days": {"n": int(len(comp_res)), "exact_pct": round(100 * float((comp_res["abs_res"] < 1).mean()), 3)
                          if len(comp_res) else None,
                          "residual_ge_1_lot": int((comp_res["abs_res"] >= TRADING_UNIT).sum())},
        "missing_days": {
            "symbol_days_with_missing_slots": int(len(mx)),
            "explained_symbol_days": int(len(ex_rows)),
            "explained_missing_slots_candidate_no_trade": int(mx.loc[mx["volume_bound_class"] ==
                                                                     "CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND",
                                                                     "missing_slots"].sum()),
            "day_level_missing_volume_symbol_days": int((mx["volume_bound_class"] == "DAY_LEVEL_MISSING_VOLUME_EVIDENCED").sum()),
            "unlocatable_missing_shares_total": float(mx["unlocatable_missing_shares"].sum()),
            "by_class": by_class.to_dict("records")},
        "provider_missing_days": prov[["symbol", "date", "official_total", "official_regular", "residual_shares",
                                       "volume_bound_class"]].to_dict("records"),
        "f13_excluded_symbol_days": int(x["unit_unverified"].sum()),
        "statement": ("Pilot only. Classes are candidates; the gap ledger keeps every slot UNRESOLVED until the identity "
                      "and units are verified and reviewed. DAY_LEVEL_MISSING_VOLUME_EVIDENCED does not locate the slot."),
    }
    (out / "volume_recon_summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2, default=str),
                                                   encoding="utf-8")
    md = ["# Volume-bound reconciliation pilot", "", f"- {summ['statement']}", "", "## Identity tests (complete days)", "",
          "```", tt.to_string(index=False), "```", "", "## Summary", "", "```json",
          json.dumps({k: v for k, v in summ.items() if k not in ("probe_dir", "ledger_dir")}, ensure_ascii=False,
                     indent=2, default=str), "```"]
    (out / "volume_recon_summary.md").write_text("\n".join(md), encoding="utf-8")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.volume_recon")
    p.add_argument("--probe-dir", required=True)
    p.add_argument("--ledger-dir", required=True)
    p.add_argument("--out-dir", default="/content/tw_lake_volume_recon")
    p.add_argument("--parsed-file", default=None, help="e.g. <probe>/reparse_<ts>/parsed_volume.parquet")
    a = p.parse_args(argv)
    out = run_recon(Path(a.probe_dir), Path(a.ledger_dir), Path(a.out_dir),
                    parsed_file=Path(a.parsed_file) if a.parsed_file else None)
    print((out / "volume_recon_summary.md").read_text(encoding="utf-8"))
    print(f"volume recon written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
