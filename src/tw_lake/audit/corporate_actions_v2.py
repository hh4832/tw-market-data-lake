"""B3a - corporate actions v2: an immutable derived dataset built only from archived official evidence (offline).

python -m tw_lake.audit.corporate_actions_v2 --dataset-root .../reference/corporate_actions/v1 \
    --data-root /content/drive/MyDrive/00Quant_Research/data --out-root .../derived/corporate_actions_v2

Per official event (TWSE TWT49U / TWTAUU / TWTB8U / TWTCAU, TPEx exDailyQ / revivt / pvChgRslt):

  official_effective_date     the date printed in the official table (legal effective date; never moved)
  effective_market_session    first market session (any symbol traded) on or after it - official dates can be
                              market closures (typhoon days 2023-08-03, 2024-07-24/25, 2024-10-02/03, ...)
  first_observed_trade_date   first session on or after effective_market_session on which THIS symbol traded
                              (FinLab raw volume > 0); diagnostic only, it does not move the factor
  price_factor                official reference price / official pre-event close. Semantics (backward
                              adjustment): signal_adjusted_price(t) = raw_price(t) x product of price_factor of
                              every event with effective_market_session > t (and available_at <= as-of time).
                              raw prices stay unadjusted for execution.
  available_at                effective_market_session 08:30 Asia/Taipei. This is a POLICY (the reference price is
                              the opening basis of that session), recorded in available_at_policy; the historical
                              publication time (source_published_at) is unknown and stays null. retrieved_at is
                              the real retrieval time of the archived response.
  accounting_status           SUPPORTED   price factor + share ratio + cash per share all official, no subscription
                                          right involved
                              PRICE_ONLY  price factor official; position quantity / cash flow not derivable from
                                          official fields (TWSE stock dividends, capital reductions, par changes,
                                          ETF splits whose ratio is only implied by prices)
                              UNSUPPORTED cash capital increase / subscription right involved, reference above the
                                          pre-event close for a stock dividend, or no market session in range
Also: diagnostics ledger (FinLab adjustment timing per event), FinLab jumps without an official event, the research
quarantine (separate from the B2 frozen exchange-date list) and a manifest with SHA-256 of every output.
Nothing in the lake, the reference dataset or the quality mask is modified.
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
from . import corporate_events as CE
from .stagelog import Stages

V2_VERSION = "2.0.0"
AVAILABLE_AT_POLICY = ("effective_market_session 08:30 Asia/Taipei: the official reference price is the opening basis "
                       "of that session; historical publication time not observed")
RESEARCH_QUARANTINE = [
    {"symbol": "8101", "start": "2024-08-22", "end": "2024-12-31",
     "reason": "no trade 2024-08-22..2024-11-18 then resumption at 10.45 after 1.90 with 13 limit-up sessions; "
               "no official corporate-action or halt record in reference/corporate_actions/v1; FinLab adjusted "
               "factor 5.0 on 2024-11-19 unexplained",
     "scope": "research signals, valuation and any holding across the window", "separate_from": "B2 QUARANTINE_FROZEN_V1"},
]
OUT_COLUMNS = ["event_id", "exchange", "symbol", "name", "event_type", "official_effective_date",
               "effective_market_session", "session_shifted", "first_observed_trade_date", "prev_close", "ref_price",
               "price_factor", "limit_up", "limit_down", "cash_per_share", "stock_per_share", "subscription_shares",
               "subscription_price", "accounting_status", "accounting_reason", "available_at", "available_at_policy",
               "source_published_at", "retrieved_at", "report", "raw_sha256", "in_research_quarantine"]


# ------------------------------------------------------------------------------------------ evidence
def retrieved_map(root: Path) -> dict[str, str]:
    """raw sha256 -> retrieved_at of the archived response (request.json of each shard)."""
    out = {}
    for rj in (Path(root) / "shards").glob("report=*/period=*/request.json"):
        try:
            r = json.loads(rj.read_text(encoding="utf-8"))
            out[r["sha256"]] = r.get("retrieved_at")
        except Exception:  # noqa: BLE001
            continue
    return out


def tpex_subscription(wide: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """TPEx exDailyQ subscription fields per (symbol, event date)."""
    w = wide.get("TPEX_exDailyQ_b")
    if w is None or not len(w):
        return pd.DataFrame(columns=["symbol", "official_effective_date", "subscription_shares", "subscription_price",
                                     "subscription_per_1000"])
    g = lambda c: w[c].map(CE.num) if c in w.columns else pd.Series(np.nan, index=w.index)  # noqa: E731
    return pd.DataFrame({"symbol": w["代號"].map(CE._symbol), "official_effective_date": w["除權息日期"].map(CE.parse_tw_date),
                         "subscription_shares": g("現金增資股數"), "subscription_price": g("現金增資認購價"),
                         "subscription_per_1000": g("按持股比例仟股認購")}).drop_duplicates(["symbol", "official_effective_date"])


def classify(e: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    t, ex = e["event_type"], e["exchange"]
    cash, stock = e["cash_per_share"], e["stock_per_share"]
    rights = (e["subscription_shares"].fillna(0) > 0) | (e["subscription_price"].fillna(0) > 0) \
        | (e["subscription_per_1000"].fillna(0) > 0)
    no_session = e["effective_market_session"].isna() | ~e["price_factor"].between(1e-3, 1e3)
    stockish = t.isin(["stock_dividend", "stock_and_cash_dividend"])
    conds = [
        no_session,
        rights,
        stockish & (e["price_factor"] > 1),
        (t == "cash_dividend") & cash.gt(0) & stock.fillna(0).eq(0),
        stockish & (ex == "TPEX") & stock.notna() & cash.notna(),
        stockish,
        t == "cash_dividend",
        t == "capital_reduction",
        t.isin(["etf_split", "etf_reverse_split", "par_value_change"]),
    ]
    status = ["UNSUPPORTED", "UNSUPPORTED", "UNSUPPORTED", "SUPPORTED", "SUPPORTED", "PRICE_ONLY", "PRICE_ONLY",
              "PRICE_ONLY", "PRICE_ONLY"]
    reason = ["no market session in the observed calendar (event after the last FinLab date)",
              "cash capital increase / subscription right: holder wealth depends on subscription",
              "stock dividend with reference above pre-event close implies a subscription component",
              "official cash per share, no stock or subscription component",
              "official stock per 1000 shares and cash per share (TPEx), no subscription",
              "stock ratio or cash split not in the official result table (TWSE detail endpoint not archived)",
              "cash per share not available",
              "exchange ratio and refund per share not in the official result table",
              "share ratio only implied by prices, no official ratio field"]
    return (pd.Series(np.select(conds, status, default="UNSUPPORTED"), index=e.index),
            pd.Series(np.select(conds, reason, default="unclassified event"), index=e.index))


def build_v2(ev: pd.DataFrame, wide: dict[str, pd.DataFrame], cal: pd.DatetimeIndex, raw: pd.DataFrame | None,
             retrieved: dict[str, str]) -> pd.DataFrame:
    e = ev.rename(columns={"event_date": "official_effective_date", "sha256": "raw_sha256",
                           "cash_dividend": "cash_per_share"}).copy()
    e["stock_per_share"] = e["stock_per_1000"] / 1000.0
    e = e.merge(tpex_subscription(wide), on=["symbol", "official_effective_date"], how="left")
    pos = cal.searchsorted(e["official_effective_date"].values, side="left")
    e["effective_market_session"] = [cal[i] if i < len(cal) else pd.NaT for i in pos]
    e["session_shifted"] = e["effective_market_session"].notna() & (e["effective_market_session"]
                                                                     != e["official_effective_date"])
    if raw is not None and len(raw):
        tr = raw[raw["volume_shares"].fillna(0) > 0][["symbol", "date"]].sort_values("date")
        x = e[["symbol", "effective_market_session"]].dropna().reset_index().sort_values("effective_market_session")
        m = pd.merge_asof(x, tr.rename(columns={"date": "first_observed_trade_date"}),
                          left_on="effective_market_session", right_on="first_observed_trade_date", by="symbol",
                          direction="forward")
        e["first_observed_trade_date"] = m.set_index("index")["first_observed_trade_date"].reindex(e.index)
    else:
        e["first_observed_trade_date"] = pd.NaT
    e["price_factor"] = e["ref_price"] / e["prev_close"]
    e["accounting_status"], e["accounting_reason"] = classify(e)
    e["available_at"] = e["effective_market_session"].dt.strftime("%Y-%m-%dT08:30:00+08:00")
    e["available_at_policy"] = AVAILABLE_AT_POLICY
    e["source_published_at"] = None
    e["retrieved_at"] = e["raw_sha256"].map(retrieved)
    key = e["exchange"] + "|" + e["symbol"] + "|" + e["event_type"] + "|" + e["official_effective_date"].dt.strftime("%Y-%m-%d")
    e["event_id"] = key.map(lambda k: hashlib.sha256(k.encode()).hexdigest()[:16])
    e["in_research_quarantine"] = in_quarantine(e["symbol"], e["official_effective_date"])
    return e[OUT_COLUMNS].sort_values(["symbol", "official_effective_date"]).reset_index(drop=True)


def in_quarantine(symbols: pd.Series, dates: pd.Series) -> pd.Series:
    out = pd.Series(False, index=symbols.index)
    for q in RESEARCH_QUARANTINE:
        out |= (symbols == q["symbol"]) & (dates >= pd.Timestamp(q["start"])) & (dates <= pd.Timestamp(q["end"]))
    return out


# ------------------------------------------------------------------------------------------ semantics
def signal_adjusted(prices: pd.DataFrame, events: pd.DataFrame, as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """Reference implementation of the contract semantics for ONE symbol: prices(date, raw) -> adds
    `cum_factor` and `signal_adjusted_price`. Only events with available_at <= as_of are applied (as_of=None: all).
    The research repo must reproduce this; it is not applied to any stored price."""
    p = prices.sort_values("date").copy()
    ev = events.dropna(subset=["effective_market_session"])
    if as_of is not None:
        avail = pd.to_datetime(ev["available_at"], utc=True).dt.tz_convert(TAIPEI).dt.tz_localize(None)
        ev = ev[avail <= as_of]
    ev = ev.sort_values("effective_market_session", ascending=False)
    p["cum_factor"] = 1.0
    for r in ev.itertuples(index=False):
        p.loc[p["date"] < r.effective_market_session, "cum_factor"] *= r.price_factor
    p["signal_adjusted_price"] = p["raw"] * p["cum_factor"]
    return p


def holding_allowed(events: pd.DataFrame, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> tuple[bool, str]:
    """Formal cross-event holding gate: every event of the symbol with effective_market_session in (start, end]
    must be SUPPORTED, and no research quarantine may overlap."""
    ev = events[(events["symbol"] == symbol) & (events["effective_market_session"] > start)
                & (events["effective_market_session"] <= end)]
    bad = ev[ev["accounting_status"] != "SUPPORTED"]
    if len(bad):
        return False, f"{len(bad)} event(s) not SUPPORTED: {','.join(sorted(set(bad['accounting_status'])))}"
    for q in RESEARCH_QUARANTINE:
        if q["symbol"] == symbol and start <= pd.Timestamp(q["end"]) and end >= pd.Timestamp(q["start"]):
            return False, "research quarantine " + q["start"] + ".." + q["end"]
    return True, ""


# ------------------------------------------------------------------------------------------ diagnostics
def diagnostics(v2: pd.DataFrame, jumps: pd.DataFrame, data_start: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per event: when FinLab adjusted (if at all) relative to the three dates; FinLab jumps without an event."""
    j = jumps.rename(columns={"jump_date": "finlab_jump_date", "implied_factor": "finlab_implied_factor"})
    d = v2[["event_id", "symbol", "event_type", "official_effective_date", "effective_market_session",
            "first_observed_trade_date", "price_factor", "accounting_status"]].copy()
    cand = d.merge(j, on="symbol", how="left")
    cand = cand[(cand["finlab_jump_date"] >= cand["official_effective_date"])
                & (cand["finlab_jump_date"] <= cand["official_effective_date"] + pd.Timedelta(days=31))]
    ref = cand["effective_market_session"].fillna(cand["official_effective_date"])
    cand = cand.assign(_dist=(cand["finlab_jump_date"] - ref).abs()).sort_values(["_dist", "finlab_jump_date"])
    cand = cand.drop_duplicates("event_id").drop_duplicates(["symbol", "finlab_jump_date"])   # one jump per event
    d = d.merge(cand[["event_id", "finlab_jump_date", "finlab_implied_factor"]], on="event_id", how="left")
    d["factor_rel_err"] = (d["finlab_implied_factor"] / d["price_factor"] - 1).abs()
    d["timing_status"] = np.select(
        [d["official_effective_date"] < data_start, d["finlab_jump_date"].isna(),
         d["finlab_jump_date"] == d["official_effective_date"], d["finlab_jump_date"] == d["effective_market_session"],
         d["finlab_jump_date"] == d["first_observed_trade_date"]],
        ["OUT_OF_FINLAB_SCOPE", "NO_FINLAB_ADJUSTMENT", "MATCH_OFFICIAL_DATE", "MATCH_EFFECTIVE_SESSION",
         "MATCH_FIRST_OBSERVED_TRADE"], default="UNEXPLAINED_TIMING")
    used = set(zip(d["symbol"], d["finlab_jump_date"]))
    free = np.array([(s, x) not in used for s, x in zip(jumps["symbol"], jumps["jump_date"])], dtype=bool)
    orphan = jumps[(jumps["jump_date"] >= data_start).to_numpy() & free].copy()
    orphan["in_research_quarantine"] = in_quarantine(orphan["symbol"], orphan["jump_date"])
    return d, orphan


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def run(dataset_root: Path, out_root: Path, data_root: Path | None = None, log=print) -> Path:
    out = Path(out_root) / f"corporate_actions_v2_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    for bad in ("fubon_stock_5m", "finlab_stock_daily", "official_volume", "quality_acceptance"):
        if bad in out.resolve().parts:
            raise SystemExit(f"refusing to write inside {bad}: {out}")
    out.mkdir(parents=True, exist_ok=False)
    st = Stages(out, "corporate_actions_v2", {"dataset_root": str(dataset_root), "data_root": str(data_root),
                                              "version": V2_VERSION}, log=log)
    summary: dict = {"version": V2_VERSION}
    with st.stage("evidence") as rec:
        wide = CE.load_wide(dataset_root)
        ev = CE.build_events(wide)
        raw = adj = None
        if data_root:
            raw, adj = CE.load_lake(Path(data_root), log=log)
        cal = CE.trading_calendar(raw) if raw is not None else pd.DatetimeIndex(sorted(ev["event_date"].unique()))
        rec.update(events=len(ev))
    with st.stage("build") as rec:
        v2 = build_v2(ev, wide, cal, raw, retrieved_map(dataset_root))
        v2.to_parquet(out / "corporate_actions_v2.parquet", index=False)
        pd.DataFrame(RESEARCH_QUARANTINE).to_csv(out / "research_quarantine.csv", index=False, encoding="utf-8-sig")
        summary["accounting_status"] = v2.groupby(["exchange", "event_type", "accounting_status"]).size() \
            .rename("n").reset_index().to_dict("records")
        summary["accounting_status_total"] = v2["accounting_status"].value_counts().to_dict()
        summary["session_shifted"] = int(v2["session_shifted"].sum())
        summary["first_trade_after_session"] = int((v2["first_observed_trade_date"]
                                                    > v2["effective_market_session"]).sum())
        rec.update(rows=len(v2))
    if raw is not None:
        with st.stage("diagnostics") as rec:
            jumps = CE.adjusted_jumps(raw, adj)
            diag, orphan = diagnostics(v2, jumps, pd.Timestamp("2023-05-01"))
            diag.to_parquet(out / "diagnostics_ledger.parquet", index=False)
            orphan.to_csv(out / "finlab_jumps_without_event.csv", index=False, encoding="utf-8-sig")
            summary["timing_status"] = diag["timing_status"].value_counts().to_dict()
            summary["timing_status_by_type"] = diag.groupby(["event_type", "timing_status"]).size().rename("n") \
                .reset_index().to_dict("records")
            summary["finlab_jumps_without_event"] = {"n": int(len(orphan)),
                                                     "in_research_quarantine": int(orphan["in_research_quarantine"].sum())}
            rec.update(orphans=len(orphan))
    (out / "adjustment_semantics.json").write_text(json.dumps({
        "price_factor": "official reference price / official pre-event close",
        "signal_adjusted_price": "raw_price(t) x product(price_factor of events with effective_market_session > t "
                                 "and available_at <= as_of)",
        "raw_execution_price": "unadjusted fubon_5m / FinLab raw; never multiplied",
        "position_accounting": "only accounting_status == SUPPORTED events; PRICE_ONLY / UNSUPPORTED block formal "
                               "cross-event holding", "available_at_policy": AVAILABLE_AT_POLICY,
        "research_quarantine": RESEARCH_QUARANTINE}, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    files = sorted(p for p in out.iterdir() if p.is_file() and p.name not in ("run_info.json", "manifest.json"))
    (out / "manifest.json").write_text(json.dumps({
        "dataset": "corporate_actions_v2", "version": V2_VERSION, "code_commit": git_sha(),
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"), "source": str(dataset_root),
        "sha256": {p.name: _sha(p) for p in files},
        "statement": "derived from archived official responses; immutable; lake, reference and masks untouched"},
        ensure_ascii=False, indent=2), encoding="utf-8")
    st.finish(**{k: v for k, v in summary.items() if not isinstance(v, list)})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tw_lake.audit.corporate_actions_v2")
    ap.add_argument("--dataset-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--data-root", default="")
    a = ap.parse_args(argv)
    out = run(Path(a.dataset_root), Path(a.out_root), Path(a.data_root) if a.data_root else None)
    print("written:", out)
    print((out / "summary.json").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
