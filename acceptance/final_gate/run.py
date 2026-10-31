"""FINAL GATE runner - read-only against the pinned lake versions; writes only its own result folder.

python -m acceptance.final_gate.run --data-root /content/drive/MyDrive/00Quant_Research/data \
    [--out-root <lake>/audits/final_gate] [--dates 30] [--k 10] [--seed 20261031]

Verdict (mechanical):
  NO-GO      any pin / SHA / integration check fails, any look-ahead or fake-fill check fails, accounting is
             inconsistent, or the two identical runs differ.
  SCOPED GO  all of the above pass; usable only inside the rows of SCOPE_MATRIX.csv marked USABLE (fail-closed
             exclusions exist, so a plain GO is not claimed).
  GO         as SCOPED GO with no excluded instrument class (not reachable while fund-like limits are UNKNOWN).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from . import core as C
from .probe import Probe, Sources, check_ledger

PINS = {
    "fubon_5m_seq": 68,
    "fubon_5m_version_dir": "versions/000068_20261025_211531_4ceb32f",
    "mask_dir": "audits/quality_acceptance/quality_mask_20261030_014633_fbec159",
    "mask_sha256": "9ec79fe02412c25dae56572a334c167c168e374a438dd71e9f4dc0e138ac6863",
    "mask_commit": "fbec159",
    "evidence_version": "quality_v1.0.0+eligibility_63ffd6f+recon_c111ab2+price_price_check_20261029_235425_4308c60",
    "corporate_actions_v2": "derived/corporate_actions_v2/corporate_actions_v2_20261031_200705_75aed1d",
    "trading_rules": "derived/trading_rules/trading_rules_20261031_222459_c5c5a4b",
    "universe_pit": "audits/universe_pit/universe_pit_20261031_222454_c5c5a4b",
}
RESEARCH_QUARANTINE = [{"symbol": "8101", "start": "2024-08-22", "end": "2024-12-31"}]
FORBIDDEN_OUT = ("fubon_stock_5m", "finlab_stock_daily", "official_volume", "quality_acceptance", "corporate_actions",
                 "trading_rules", "universe_pit", "_manifest")
MIN_CLOSED = {"INTRADAY_PRICE": 20, "INTRADAY_VOLUME": 20, "MULTI_DAY": 10}


def sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def repo_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).parent, text=True).strip()
    except Exception:  # noqa: BLE001
        return "unknown"


# ------------------------------------------------------------------------------------------ pins
def verify_derived(d: Path) -> dict:
    m = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    bad = {f: "MISSING" if not (d / f).exists() else "MISMATCH" for f, h in m["sha256"].items()
           if not (d / f).exists() or sha(d / f) != h}
    return {"dir": str(d), "dataset": m.get("dataset"), "version": m.get("version"),
            "code_commit": m.get("code_commit"), "files": len(m["sha256"]), "sha_mismatch": bad,
            "sha256": m["sha256"], "ok": not bad}


def verify_pins(lake: Path, data_root: Path, integration_sample: int = 50) -> dict:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from tw_lake.audit.integration_check import integration_check
    res: dict = {}
    ptr = json.loads((data_root / "fubon_stock_5m" / "_manifest" / "published.json").read_text(encoding="utf-8"))
    res["fubon_5m"] = {"seq": ptr.get("seq"), "version_dir": ptr.get("version_dir"),
                       "ok": ptr.get("seq") == PINS["fubon_5m_seq"] and ptr.get("version_dir") == PINS["fubon_5m_version_dir"]}
    qdir = lake / PINS["mask_dir"]
    msha = sha(qdir / "quality_mask_symbol_day.parquet")
    ev = pd.read_parquet(qdir / "quality_mask_symbol_day.parquet", columns=["evidence_version"])["evidence_version"].unique()
    ic = integration_check(qdir, data_root, PINS["mask_commit"], PINS["fubon_5m_seq"], integration_sample)
    res["quality_mask"] = {"sha256": msha, "evidence_version": list(ev), "integration_check": ic["verdict"],
                           "integration_failures": ic["failures"], "recount": ic["recount"],
                           "ok": msha == PINS["mask_sha256"] and list(ev) == [PINS["evidence_version"]]
                           and ic["verdict"] == "PASS"}
    for k in ("corporate_actions_v2", "trading_rules", "universe_pit"):
        res[k] = verify_derived(lake / PINS[k])
    res["all_ok"] = all(v["ok"] for v in res.values() if isinstance(v, dict))
    return res


# ------------------------------------------------------------------------------------------ io
def load_sources(lake: Path, data_root: Path) -> Sources:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from tw_lake.audit.reader import load_published
    mask = pd.read_parquet(lake / PINS["mask_dir"] / "quality_mask_symbol_day.parquet",
                           columns=["symbol", "exchange", "date", "liquidity_bucket", "session_type",
                                    "can_use_for_price_backtest", "can_use_for_volume_backtest", "level_A_strict",
                                    "research_tier", "exclusion_reason"])
    mask["date"] = pd.to_datetime(mask["date"])
    panel = pd.read_parquet(lake / PINS["universe_pit"] / "universe_panel.parquet",
                            columns=["symbol", "date", "official_listed", "security_type", "delisted_official",
                                     "halted_official"])
    rules = pd.read_parquet(lake / PINS["trading_rules"] / "trading_rules.parquet")
    ca = pd.read_parquet(lake / PINS["corporate_actions_v2"] / "corporate_actions_v2.parquet")
    pv = load_published("fubon_5m", data_root / "fubon_stock_5m")
    parts = pv.partitions.set_index("partition")["relpath"].to_dict()

    def bars(symbol: str, year: int) -> pd.DataFrame:
        rel = parts.get(f"symbol={symbol}/year={year}")
        if rel is None:
            return pd.DataFrame(columns=["bar_start", "open", "high", "low", "close", "volume_raw"])
        b = pd.read_parquet(pv.data_path(rel), columns=["bar_start", "open", "high", "low", "close", "volume_raw"])
        b["bar_start"] = pd.to_datetime(b["bar_start"]).dt.tz_convert("Asia/Taipei").dt.tz_localize(None)
        return b
    sessions = pd.DatetimeIndex(sorted(rules["date"].unique()))
    return Sources(mask, panel, rules, ca, bars, sessions, RESEARCH_QUARANTINE)


def sample_dates(src: Sources, n: int) -> list[pd.Timestamp]:
    s = src.sessions[(src.sessions >= src.mask["date"].min()) & (src.sessions <= src.mask["date"].max())]
    s = s[1:-30] if len(s) > 40 else s[1:]
    idx = np.unique(np.linspace(0, len(s) - 1, n).round().astype(int))
    return list(s[idx])


# ------------------------------------------------------------------------------------------ scope
def _ret(num: pd.Series, by: pd.Series) -> str:
    g = num.groupby(by).mean()
    return json.dumps({str(k): round(float(v), 4) for k, v in g.items()}, ensure_ascii=False)


def scope_matrix(src: Sources, probe_ok: dict) -> pd.DataFrame:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from tw_lake.reference.official_daily import security_type
    m = src.mask.copy()
    us = pd.Series(m["symbol"].unique())
    m["security_type"] = m["symbol"].map(dict(zip(us, us.map(security_type))))
    st = m[m["security_type"] == "STOCK"]
    q = np.zeros(len(st), dtype=bool)
    for r in RESEARCH_QUARANTINE:
        q |= ((st["symbol"] == r["symbol"]) & (st["date"] >= r["start"]) & (st["date"] <= r["end"])).to_numpy()
    rows = []

    def add(sid, cls, inst, status, cond, excl, base, ok, fill, by_liq, by_year, reasons):
        rows.append({"scope_id": sid, "strategy_class": cls, "instruments": inst, "status": status,
                     "data_and_conditions": cond, "mandatory_exclusions": excl, "base_units": int(base),
                     "usable_units": int(ok), "retention": round(ok / base, 4) if base else None,
                     "retention_by_liquidity": by_liq, "retention_by_year": by_year,
                     "top_exclusion_reasons": reasons, "fill_model": fill})
    fill_intra = ("market order at the open of the first REAL 5m bar at/after the 30m bucket available_at, +1 tick "
                  "slippage clipped to the bar range, 10% participation cap, B4a limits (UNKNOWN fails closed, "
                  "at-limit open does not fill); closing auction never targeted; 0.1425% fee, 0.3% sell tax")
    for sid, cls, col, probe_key, cond in (
            ("S1", "INTRADAY_PRICE_5M_30M", "can_use_for_price_backtest", "INTRADAY_PRICE",
             "fubon_5m seq 68 REAL bars resampled per RESEARCH_REPO_INTERFACE s2; select with previous-session mask "
             "+ B5a membership; same-day mask only invalidates"),
            ("S2", "INTRADAY_VOLUME_STRICT", "can_use_for_volume_backtest", "INTRADAY_VOLUME",
             "as S1 and volume_reconciliation_status == EXACT"),
            ("S1A", "INTRADAY_ROBUSTNESS_LEVEL_A", "level_A_strict", "INTRADAY_PRICE",
             "as S1 with 54/54 bars and zero residual")):
        ok = st[col].astype(bool) & ~q
        reasons = st.loc[~ok, "exclusion_reason"].replace("", "QUARANTINE_8101").value_counts().head(6).to_dict()
        verified = probe_ok.get(probe_key, False)
        add(sid, cls, "common stocks (4-digit codes) TWSE + TPEx", "USABLE" if verified else "NOT_VERIFIED", cond,
            "exclusion_reason != '' (special sessions/disposal, quarantined exchange dates, unresolved gaps, "
            "price not PASS, no official record); 8101 2024-08-22..2024-12-31; B4a UNKNOWN at execution",
            len(st), ok.sum(), fill_intra, _ret(ok, st["liquidity_bucket"]), _ret(ok, st["date"].dt.year),
            json.dumps(reasons, ensure_ascii=False))
    for h in (1, 5, 20):
        r = C.multi_day_retention(src.rules, src.ca, h, RESEARCH_QUARANTINE)
        ok = r["reason"] == "USABLE"
        add(f"M{h}", f"MULTI_DAY_HOLD_{h}_SESSIONS", "common stocks", "USABLE" if probe_ok.get("MULTI_DAY", False)
            else "NOT_VERIFIED",
            "entry/exit on official sessions with usable B4a limit status; every corporate action in (entry, exit] "
            "SUPPORTED and accounted (shares x (1+stock), cash at effective session); raw prices for P&L, "
            "B3a factors only for signals with available_at <= decision time",
            "windows crossing PRICE_ONLY/UNSUPPORTED events, 8101 window, UNKNOWN limits; delisting -> no exit at "
            "last price (INVALIDATED_NO_EXIT_PRICE)", len(r), ok.sum(),
            "next-session open of first REAL 5m bar (same fill model as S1); exit retried up to 5 sessions",
            _ret(ok, r["liquidity_bucket"]), _ret(ok, r["date"].dt.year),
            json.dumps(r.loc[~ok, "reason"].value_counts().to_dict(), ensure_ascii=False))
    fund = src.rules[src.rules["instrument_class"].isin(["ETF", "ETN", "REIT"])]
    fok = ~fund["price_limit_status"].isin(["UNKNOWN_INSTRUMENT_CLASS", "UNKNOWN_NO_REFERENCE", "DERIVED_CONFLICT"])
    add("X1", "ANY", "ETF / ETN / REIT", "EXCLUDED_FAIL_CLOSED", "B4a limit class not officially established "
        "for TWSE fund-like products", "all formal use", len(fund), fok.sum(), "none (fail-closed)",
        _ret(fok, fund["liquidity_bucket"]), _ret(fok, fund["date"].dt.year),
        json.dumps(fund.loc[~fok, "price_limit_status"].value_counts().to_dict(), ensure_ascii=False))
    sp = m[m["session_type"] != "normal_template"]
    add("X2", "ANY", "disposal / special-auction / unknown-regime symbol-days", "EXCLUDED_FAIL_CLOSED",
        "research_tier ISOLATE", "all formal use", len(m), len(sp), "none", "", "",
        json.dumps(sp["session_type"].value_counts().to_dict(), ensure_ascii=False))
    add("X3", "ANY", "8101 2024-08-22..2024-12-31", "EXCLUDED_FAIL_CLOSED", "research quarantine (B3a)",
        "all use", len(st), int(q.sum()), "none", "", "", "{}")
    add("X4", "CLOSING_AUCTION_EXECUTION", "all", "EXCLUDED_FAIL_CLOSED",
        "order cut-off and executability of the 13:30 call auction not verified", "orders targeting 13:30",
        0, 0, "none: closing-price signals execute next session at the earliest", "", "", "{}")
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------------------ blockers
STATIC_BLOCKERS = [
    ("B-01", "B3", "852 corporate actions PRICE_ONLY (652) / UNSUPPORTED (200): TWSE stock-dividend ratios, capital "
     "reductions, splits, rights", "multi-day windows crossing them", "EXCLUDED_FAIL_CLOSED",
     "holding window rejected (INVALIDATED_CORPORATE_ACTION); signal adjustment still valid"),
    ("B-02", "B4", "90,233 fund-like symbol-days (142 TWSE ETF/ETN/REIT) without official limit class",
     "ETF/ETN/REIT", "EXCLUDED_FAIL_CLOSED", "scope X1"),
    ("B-03", "B4", "4 DERIVED_CONFLICT (6821, 6423, 6794, 7780; new listings) + 4,048 UNKNOWN_NO_REFERENCE",
     "those symbol-days", "EXCLUDED_FAIL_CLOSED", "eligibility UNKNOWN -> no fill"),
    ("B-04", "B4", "closing call-auction order cut-off not verified", "closing-auction execution",
     "EXCLUDED_FAIL_CLOSED", "scope X4; closing signals trade next session"),
    ("B-05", "B3", "8101 unexplained 5x jump", "8101 2024-08-22..2024-12-31", "EXCLUDED_FAIL_CLOSED", "scope X3"),
    ("B-06", "B2", "13 quarantined exchange dates (QUARANTINE_FROZEN_V1); disposal/special sessions",
     "those dates / symbol-days", "EXCLUDED_FAIL_CLOSED", "already excluded by quality 1.0.0 mask"),
    ("B-07", "B3", "actual dividend payment dates not available", "cash timing in multi-day accounting",
     "NON_BLOCKING_DISCLOSE", "cash credited at the effective session; disclose"),
    ("B-08", "B5", "TWSE halt history before 2023-09-14 incomplete", "halt flag", "NON_BLOCKING_DISCLOSE",
     "halted days have no official volume / bars -> BLOCKED_NO_TRADE anyway"),
    ("B-09", "B5", "37 symbols stopped without official record (2 stocks: 1589, 4804)", "holdings over their end",
     "NON_BLOCKING_DISCLOSE", "no exit at last price -> INVALIDATED_NO_EXIT_PRICE"),
    ("B-10", "B5", "survivorship: delisted-stock FinLab missing rate 1.29% vs 0.50% active (93.6% odd-lot-only days)",
     "daily FinLab cross-checks", "NON_BLOCKING_DISCLOSE", "5m strategies use fubon bars + B5a membership"),
    ("B-11", "B1", "2026 fubon 5m intraday volume shortfall", "Volume Strict", "EXCLUDED_FAIL_CLOSED",
     "Volume Strict accepts only EXACT reconciliation"),
    ("B-12", "process", "acceptance harness lives in this repo (acceptance/, outside tw_lake) by explicit FINAL GATE "
     "instruction although AGENTS rule 1 keeps engines out", "none on data", "NON_BLOCKING_DISCLOSE",
     "move to the independent Backtest Agent repo when it exists"),
]


def blocker_register(checks: dict, pins: dict) -> pd.DataFrame:
    rows = [dict(zip(["blocker_id", "area", "description", "impact_scope", "severity", "handling"], b),
                 status="OPEN_HANDLED") for b in STATIC_BLOCKERS]
    n = 0
    for k, v in checks.items():
        if not v["passed"]:
            n += 1
            rows.append({"blocker_id": f"G-{n:02d}", "area": "gate", "description": f"check failed: {k}",
                         "impact_scope": "all", "severity": "BLOCKING", "handling": json.dumps(v["detail"])[:300],
                         "status": "OPEN"})
    if not pins.get("all_ok", False):
        rows.append({"blocker_id": "G-PINS", "area": "versions", "description": "pinned version / SHA mismatch",
                     "impact_scope": "all", "severity": "BLOCKING", "handling": "stop", "status": "OPEN"})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------------------ gate
def gate(src: Sources, dates: list, k: int, seed: int, pins: dict) -> dict:
    led1 = Probe(src, seed).run(dates, k)
    led2 = Probe(src, seed).run(dates, k)
    s1, s2 = C.sha_frame(led1), C.sha_frame(led2)
    lc = check_ledger(led1)
    checks = {name: {"passed": bool(p), "n": int(n), "detail": det} for name, (p, n, det) in lc.items()}
    checks["reproducible_same_version_rerun"] = {"passed": s1 == s2, "n": len(led1), "detail": [s1, s2]}
    checks["pins_and_sha"] = {"passed": bool(pins.get("all_ok")), "n": 5, "detail": {
        k2: v.get("ok") for k2, v in pins.items() if isinstance(v, dict)}}
    closed = led1[led1["status"] == "CLOSED"].groupby("strategy").size().to_dict()
    probe_ok = {s: closed.get(s, 0) >= n for s, n in MIN_CLOSED.items()}
    hard = all(c["passed"] for c in checks.values())
    excluded = True                          # fund-like / closing auction / disposal are fail-closed exclusions
    verdict = "NO-GO" if not hard or not any(probe_ok.values()) else ("SCOPED GO" if excluded else "GO")
    return {"ledger": led1, "ledger_sha256": s1, "checks": checks, "closed_by_strategy": closed,
            "status_by_strategy": led1.groupby(["strategy", "status"]).size().rename("n").reset_index(),
            "probe_ok": probe_ok, "verdict": verdict}


def report_md(res: dict, scope: pd.DataFrame, blk: pd.DataFrame, pins: dict, meta: dict) -> str:
    v = res["verdict"]
    usable = scope[scope["status"] == "USABLE"]
    lines = [f"# FINAL READINESS REPORT — tw-market-data-lake → Backtest Agent", "",
             f"**Verdict: {v}**  (run {meta['run_id']}, repo {meta['repo_commit'][:7]})", "",
             "## Answer", "",
             ("Yes, within the USABLE scope rows below; everything else is fail-closed." if v != "NO-GO"
              else "No: at least one hard check failed (see Checks)."), "",
             "## Scope", "", "| id | class | status | usable / base | retention |", "|---|---|---|---|---|"]
    for r in scope.itertuples(index=False):
        lines.append(f"| {r.scope_id} | {r.strategy_class} | {r.status} | {r.usable_units:,} / {r.base_units:,} | "
                     f"{r.retention if r.retention is not None else ''} |")
    lines += ["", "## Hard checks", "", "| check | passed | n |", "|---|---|---|"]
    for k, c in res["checks"].items():
        lines.append(f"| {k} | {'PASS' if c['passed'] else 'FAIL'} | {c['n']} |")
    lines += ["", "## Probe ledger outcome", "", "| strategy | status | n |", "|---|---|---|"]
    for r in res["status_by_strategy"].itertuples(index=False):
        lines.append(f"| {r.strategy} | {r.status} | {r.n} |")
    lines += ["", f"Closed trades per strategy: {res['closed_by_strategy']} (minimum {MIN_CLOSED})",
              f"Ledger SHA-256 (run 1 = run 2): {res['ledger_sha256']}", "", "## Blockers", "",
              "| id | severity | description |", "|---|---|---|"]
    for r in blk.itertuples(index=False):
        lines.append(f"| {r.blocker_id} | {r.severity} | {r.description} |")
    lines += ["", "## Pinned versions", "", "```", json.dumps({k: (v2 if not isinstance(v2, dict) else
              {kk: vv for kk, vv in v2.items() if kk != 'sha256'}) for k, v2 in pins.items()}, ensure_ascii=False,
              indent=1, default=str), "```", "",
              "Probe P&L is not a performance result and was not used for any decision.", ""]
    return "\n".join(lines)


def run(data_root: Path, out_root: Path | None, n_dates: int, k: int, seed: int, log=print) -> Path:
    lake = data_root / "_tw-market-data-lake"
    out_root = out_root or lake / "audits" / "final_gate"
    rid = f"final_gate_{datetime.now():%Y%m%d_%H%M%S}_{repo_commit()[:7]}"
    out = Path(out_root) / rid
    if any(b in out.resolve().parts for b in FORBIDDEN_OUT):
        raise SystemExit(f"refusing to write inside a dataset folder: {out}")
    out.mkdir(parents=True, exist_ok=False)
    log("verifying pins ...")
    pins = verify_pins(lake, data_root)
    log(json.dumps({k2: v.get("ok") for k2, v in pins.items() if isinstance(v, dict)}))
    log("loading pinned sources ...")
    src = load_sources(lake, data_root)
    dates = sample_dates(src, n_dates)
    log(f"probing {len(dates)} sessions x {k} symbols x 3 probes (twice) ...")
    res = gate(src, dates, k, seed, pins)
    log("scope matrix ...")
    scope = scope_matrix(src, res["probe_ok"])
    blk = blocker_register(res["checks"], pins)
    res["ledger"].to_parquet(out / "probe_ledger.parquet", index=False)
    res["ledger"].to_csv(out / "probe_ledger.csv", index=False, encoding="utf-8-sig")
    scope.to_csv(out / "SCOPE_MATRIX.csv", index=False, encoding="utf-8-sig")
    blk.to_csv(out / "BLOCKER_REGISTER.csv", index=False, encoding="utf-8-sig")
    (out / "checks.json").write_text(json.dumps(res["checks"], ensure_ascii=False, indent=2, default=str), "utf-8")
    meta = {"run_id": rid, "repo_commit": repo_commit(), "created_at": datetime.now().isoformat(timespec="seconds"),
            "params": {"dates": [str(d.date()) for d in dates], "k": k, "seed": seed, "horizon": 5,
                       "costs": {"commission": C.COMMISSION, "sell_tax": C.SELL_TAX_STOCK, "slip_ticks": C.SLIP_TICKS,
                                 "participation_cap": C.PARTICIPATION_CAP}}}
    (out / "FINAL_READINESS_REPORT.md").write_text(report_md(res, scope, blk, pins, meta), encoding="utf-8")
    files = sorted(p for p in out.iterdir() if p.is_file())
    (out / "run_manifest.json").write_text(json.dumps({
        **meta, "verdict": res["verdict"], "pins": pins, "ledger_sha256": res["ledger_sha256"],
        "closed_by_strategy": res["closed_by_strategy"], "probe_ok": res["probe_ok"],
        "outputs_sha256": {p.name: sha(p) for p in files},
        "statement": "read-only acceptance run; nothing in the lake was modified"},
        ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log(f"\n======== FINAL GATE: {res['verdict']} ========")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m acceptance.final_gate.run")
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--out-root", default="")
    ap.add_argument("--dates", type=int, default=30)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=20261031)
    a = ap.parse_args(argv)
    out = run(Path(a.data_root), Path(a.out_root) if a.out_root else None, a.dates, a.k, a.seed)
    print("written:", out)
    print((out / "FINAL_READINESS_REPORT.md").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
