"""README.md, audit_summary.md and audit_summary.json for one audit run."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

LIMITATIONS = [
    "Fubon omits 5-minute intervals without trades; a missing timestamp cannot be proven to be a legal "
    "no-trade interval from 5m data alone (needs tick/odd-lot/after-hours source).",
    "Expected bar counts under disposal call-auction regimes are not asserted; only observed counts are reported.",
    "FinLab daily volume definition (after-hours fixed-price / block / odd-lot inclusion) is not verified "
    "against an official TWSE/TPEx source; volume ratios < band are pending_verification, not explained.",
    "security_categories is a current snapshot; instrument_type/market are not point-in-time. Listing and "
    "delisting are inferred from first/last FinLab price date (reconstructed ex post), not official dates.",
    "has_trade / has_price for date D are known only after D's close; they must be lagged for pre-open selection.",
    "Ingestion did not persist per-bar excluded (emerging-board) rows; only per-window summaries and any "
    "QA-rejected windows in quarantine can be analysed.",
    "This audit runs on one machine; it cannot prove cross-machine/Drive atomicity. See docs/AUDIT.md "
    "fault-injection plan.",
]


def _count(df: pd.DataFrame, col: str, val) -> int:
    return int((df[col] == val).sum()) if len(df) and col in df.columns else 0


def severity_table(tables: dict) -> list[dict]:
    integ = tables["audit_integrity"]
    days = tables["audit_5m_by_symbol_day"]
    rec = tables["audit_ohlcv_reconciliation"]
    gaps = tables["audit_gap_cases"]
    uni = tables["audit_universe_issues"]
    excl = tables["audit_excluded_bars"]
    rows = []
    fails = integ[integ["status"] == "fail"] if len(integ) else integ
    for (ds, chk, sev), g in (fails.groupby(["dataset", "check", "severity"]) if len(fails) else []):
        rows.append({"severity": sev, "area": "A1", "item": f"{ds}:{chk}", "count": int(len(g)),
                     "denominator": f"published partitions of {ds}", "basis": "measured",
                     "example": str(g["partition"].dropna().head(3).tolist())})
    n_days = len(days)
    for st, sev in (("invalid", "critical"), ("no_bars_finlab_traded", "warning"), ("intraday_missing", "warning"),
                    ("bars_without_finlab_trade", "warning"), ("regime_call_auction_unverified", "info"),
                    ("regime_unknown", "info"), ("complete", "info")):
        k = _count(days, "quality_status", st)
        if k:
            rows.append({"severity": sev, "area": "A2", "item": f"5m day {st}", "count": k,
                         "denominator": f"{n_days} audited symbol-days", "basis": "measured",
                         "example": str(days.loc[days["quality_status"] == st, ["symbol", "date"]].head(3)
                                        .astype(str).values.tolist())})
    for cl, sev in (("data_error_candidate", "critical"), ("suspect", "warning"), ("missing_5m_day", "warning"),
                    ("pending_verification", "info")):
        k = _count(rec, "classification", cl)
        if k:
            rows.append({"severity": sev, "area": "A3", "item": f"recon {cl}", "count": k,
                         "denominator": f"{len(rec)} reconciled symbol-days", "basis": "measured",
                         "example": str(rec.loc[rec["classification"] == cl, ["symbol", "date", "flags"]].head(3)
                                        .astype(str).values.tolist())})
    for cl, sev, basis in (("source_missing_suspected", "warning", "inferred"), ("unresolved", "warning", "unknown"),
                           ("no_regular_session_trade_probable", "info", "inferred"),
                           ("trading_regime_disposal", "info", "inferred")):
        k = _count(gaps, "classification", cl)
        if k:
            rows.append({"severity": sev, "area": "A5", "item": f"gap {cl}", "count": k,
                         "denominator": f"{len(gaps)} gap symbol-days (complete_with_gaps + partial)", "basis": basis,
                         "example": str(gaps.loc[gaps["classification"] == cl, ["symbol", "date"]].head(3)
                                        .astype(str).values.tolist())})
    if len(uni):
        for (iss, sev), g in uni.groupby(["issue", "severity"]):
            rows.append({"severity": sev, "area": "A4", "item": iss, "count": int(len(g)),
                         "denominator": "symbols in symbol_master", "basis": "measured",
                         "example": str(g["symbol"].head(3).tolist())})
    if len(excl):
        for risk, g in excl.groupby("risk"):
            sev = "info" if risk == "consistent_with_pre_listing_emerging" else "warning"
            rows.append({"severity": sev, "area": "A4", "item": f"excluded bars {risk}", "count": int(len(g)),
                         "denominator": f"{len(excl)} excluded symbol-windows ({int(excl['bars_excluded'].sum())} bars)",
                         "basis": "measured", "example": str(g["symbol"].head(3).tolist())})
    order = {"critical": 0, "warning": 1, "info": 2}
    return sorted(rows, key=lambda r: (order.get(r["severity"], 3), r["area"], -r["count"]))


def verdict(sev_rows: list[dict]) -> dict:
    crit = [r for r in sev_rows if r["severity"] == "critical"]
    base = "NOT research-ready" if crit else "conditionally usable"
    return {
        "daily_only": f"{base}: use audit_research_ready.ready_daily_only and the PIT caveats in audit_field_provenance",
        "5m_normal_session": f"{base}: strict = ready_5m_normal_session; illiquid symbols: ready_5m_no_trade_tolerant "
                             "(exploratory, missing intervals treated as no-trade only with volume/range evidence); "
                             "no silent gap filling",
        "30m_resample": f"{base}: only from ready_30m_resample=True days; resampling spec is Phase B",
        "critical_items": len(crit),
        "statement": "This audit does not declare the data research-ready; it lists what must be fixed or filtered.",
    }


def write_reports(out_dir: Path, meta: dict, tables: dict, findings: dict, log: list[str]) -> None:
    sev = severity_table(tables)
    v = verdict(sev)
    summary = {"meta": meta, "verdict": v, "severity_table": sev, "findings": findings,
               "limitations": LIMITATIONS, "outputs": sorted(p.name for p in out_dir.glob("*.parquet"))}
    (out_dir / "audit_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str),
                                                 encoding="utf-8")
    md = [f"# Data Quality Audit {meta['audit_id']}", "",
          f"- audit version {meta['audit_version']}, git `{meta['git_sha'][:12]}`, "
          f"started {meta['started_at']} (Asia/Taipei), elapsed {meta['elapsed_sec']} s",
          f"- scope: {meta['scope']}", f"- source unchanged: **{findings['source_unchanged']['ok']}** "
          f"({findings['source_unchanged']['method']})", "", "## Verdict", ""]
    md += [f"- **{k}**: {val}" for k, val in v.items()]
    md += ["", "## Findings by severity", "", "| severity | area | item | count | denominator | basis | example |",
           "|---|---|---|---:|---|---|---|"]
    md += [f"| {r['severity']} | {r['area']} | {r['item']} | {r['count']} | {r['denominator']} | {r['basis']} | "
           f"{r['example'][:80].replace('|', '/')} |" for r in sev]
    md += ["", "## Handoff reconstruction (fubon_5m manifest)", "", "```json",
           json.dumps(findings["handoff_reconstruction"], ensure_ascii=False, indent=2, default=str)[:4000], "```",
           "", "## OHLCV reconciliation", "", "```json",
           json.dumps(findings["recon_summary"], ensure_ascii=False, indent=2, default=str)[:4000], "```",
           "", "## Research readiness (derived, audit output only)", "", "```json",
           json.dumps(findings["readiness"], ensure_ascii=False, indent=2, default=str), "```",
           "", "## Limitations (not provable here)", ""]
    md += [f"- {x}" for x in LIMITATIONS]
    (out_dir / "audit_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    readme = [f"# {meta['audit_id']}", "",
              "Read-only Data Quality Audit (Phase A). Nothing under DATA_ROOT was modified.", "",
              "## Sources (manifest-pointed versions)", "", "| dataset | seq | version_dir | run_id |", "|---|---:|---|---|"]
    readme += [f"| {k} | {s['seq']} | {s['version_dir']} | {s['run_id']} |" for k, s in meta["sources"].items()]
    readme += ["", "## Run", "", f"- commit: `{meta['git_sha']}`", f"- data_root: `{meta['data_root']}`",
               f"- started: {meta['started_at']}  finished: {meta['finished_at']}",
               f"- options: `{json.dumps(meta['options'], ensure_ascii=False)}`", "",
               "Re-run (same code + same manifests => same results):", "", "```bash",
               "python -m tw_lake.audit.cli --data-root \"$DATA_ROOT\" --out-dir /content/tw_lake_audits"
               + (f" --symbols {','.join(meta['options']['symbols'])}" if meta["options"].get("symbols") else ""),
               "```", "", "## Files", ""]
    readme += [f"- `{n}`" for n in summary["outputs"]] + ["", "## Log", "", "```", *log, "```"]
    (out_dir / "README.md").write_text("\n".join(readme) + "\n", encoding="utf-8")
