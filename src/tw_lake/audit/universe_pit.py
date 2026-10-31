"""B5a - historical point-in-time universe reconciliation (offline).

python -m tw_lake.audit.universe_pit --official-root .../reference/official_volume/v1 \
    --ca-root .../reference/corporate_actions/v1 --data-root /content/drive/MyDrive/00Quant_Research/data \
    [--gap-ledger .../audits/gap_ledger_.../gap_day_summary.parquet] --out-root .../audits/universe_pit

Panel: one row per (symbol, date) over the archived official calendar that appears in ANY of: the official daily
tables (TWSE MI_INDEX ALLBUT0999 / TPEx dailyQuotes EW), FinLab raw daily, fubon 5m (via the gap ledger).
Separate flags - never collapsed into one "in universe" bit:
  official_listed     present in the official table of `exchange` that session
  official_traded     official volume > 0
  halted_official     inside a TWSE TWTAWU / TPEx sprc suspension interval, or TPEx chtm 停止交易 that day
  finlab_price        FinLab raw close present;  fubon_bars: real 5m bars present (if the gap ledger is given)
  delisted_official   on / after an official termination date of that exchange (board transfers excluded)
coverage_class (the reason a source is missing, or why it is extra):
  OK_TRADED_PRICED | OK_NO_TRADE_NO_PRICE | NO_TRADE_PRICE_PRESENT | HALTED_NO_PRICE | HALTED_PRICE_PRESENT
  | OFFICIAL_TRADED_PRICE_MISSING | PRICE_AFTER_OFFICIAL_DELISTING | PRICE_BEFORE_OFFICIAL_LISTING
  | PRICE_WITHOUT_OFFICIAL_ROW | OFFICIAL_TABLE_MISSING
Per symbol: first/last official listed date, official listing / delisting dates and reports, last_observed_price_date
(kept separate from the official delisting date), termination_status. No holding is ever liquidated at the last
available price: valuation_status() reports DELISTED_NO_EXIT_PRICE / UNPRICED_* instead.
Nothing in the lake, the reference datasets, the quality mask or can_use_* / research_tier is modified.
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
from ..reference.official_daily import load_official_daily, security_type
from ..run_context import git_sha
from . import corporate_events as CE
from .bars import liquidity_bucket
from .stagelog import Stages

PIT_VERSION = "1.0.0"
PANEL_COLUMNS = ["symbol", "date", "exchange", "security_type", "official_listed", "official_traded", "halted_official",
                 "finlab_price", "finlab_traded", "fubon_bars", "delisted_official", "liquidity_bucket",
                 "coverage_class"]
SYMBOL_COLUMNS = ["symbol", "security_type", "exchanges", "first_official_date", "last_official_date",
                  "official_listing_date", "official_delisting_date", "delisting_report", "transfer_dates",
                  "last_observed_price_date", "last_official_traded_date", "termination_status", "official_days",
                  "official_traded_days", "price_missing_traded_days", "price_after_delisting_days"]


# ------------------------------------------------------------------------------------------ official events
def halt_days(wide: dict[str, pd.DataFrame], cal: pd.DatetimeIndex) -> pd.DataFrame:
    """symbol, date for every session inside an official suspension interval [suspend, resume)."""
    out = []
    for rep, sc, a, b in (("TWSE_TWTAWU", "證券代號", "暫停交易日期", "恢復交易日期"),
                          ("TPEX_sprc", "有價證券代號", "暫停交易", "恢復交易")):
        w = wide.get(rep)
        if w is None or not len(w) or sc not in w.columns:
            continue
        for s, x, y in zip(w[sc].map(CE._symbol), w[a].map(CE.parse_tw_date), w[b].map(CE.parse_tw_date)):
            if x is None or pd.isna(x):
                continue
            y = y if y is not None and pd.notna(y) else cal.max() + pd.Timedelta(days=1)
            for d in cal[(cal >= x) & (cal < y)]:
                out.append((s, d, rep))
    ch = CE.chtm_flags(wide)
    ch = ch[ch["halted"].astype(bool)]
    out += [(s, d, "TPEX_chtm") for s, d in zip(ch["symbol"], ch["date"])]
    return pd.DataFrame(out, columns=["symbol", "date", "halt_source"]).drop_duplicates(["symbol", "date"])


def terminations(li: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Official delistings split into terminations and board transfers (delisted on one exchange and listed on the
    other on the same date)."""
    d = li[li["event"] == "delisted"][["symbol", "exchange", "date", "report"]]
    a = li[li["event"].isin(["listed", "listed_current_isin"])][["symbol", "exchange", "date"]]
    t = d.merge(a, on=["symbol", "date"], suffixes=("_from", "_to"))
    t = t[t["exchange_from"] != t["exchange_to"]].drop_duplicates(["symbol", "date"])
    keys = set(zip(t["symbol"], t["date"]))
    term = d[np.array([(s, x) not in keys for s, x in zip(d["symbol"], d["date"])], dtype=bool)]
    return term.sort_values("date").drop_duplicates("symbol", keep="last"), t


def first_listing(li: pd.DataFrame) -> dict:
    x = li[li["event"].isin(["listed", "listed_current_isin"])]
    return {} if x.empty else pd.to_datetime(x["date"]).groupby(x["symbol"]).min().to_dict()


# ------------------------------------------------------------------------------------------ panel
def build_panel(od: pd.DataFrame, raw: pd.DataFrame | None, li: pd.DataFrame, halts: pd.DataFrame,
                bars: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    od = od.copy()
    od["volume_shares"] = pd.to_numeric(od["volume_shares"], errors="coerce")
    cal = pd.DatetimeIndex(sorted(od["date"].unique()))
    tables = set(zip(od["exchange"], od["date"]))
    o = od[["symbol", "date", "exchange", "volume_shares"]].assign(official_listed=True)
    o["official_traded"] = o["volume_shares"].fillna(0) > 0
    o = o.drop_duplicates(["symbol", "date"])
    parts = [o]
    if raw is not None and len(raw):
        r = raw[(raw["date"] >= cal.min()) & (raw["date"] <= cal.max())]
        r = r.assign(finlab_price=r["close"].notna(),
                     finlab_traded=pd.to_numeric(r.get("volume_shares", r["close"]), errors="coerce").fillna(0) > 0)
        parts.append(r[["symbol", "date", "finlab_price", "finlab_traded"]])
    if bars is not None and len(bars):
        b = bars[(bars["date"] >= cal.min()) & (bars["date"] <= cal.max())]
        parts.append(b[["symbol", "date"]].assign(fubon_bars=b["real_bars"].fillna(0) > 0))
    p = parts[0]
    for x in parts[1:]:
        p = p.merge(x.drop_duplicates(["symbol", "date"]), on=["symbol", "date"], how="outer")
    for c in ("official_listed", "official_traded", "finlab_price", "finlab_traded", "fubon_bars"):
        p[c] = p[c].fillna(False).astype(bool) if c in p.columns else False
    p = p[p["date"].isin(cal)]
    # exchange for rows without an official row: the symbol's nearest official exchange (date-specific otherwise)
    p = p.sort_values(["symbol", "date"])
    p["exchange"] = p.groupby("symbol")["exchange"].transform(lambda s: s.ffill().bfill())
    p["security_type"] = p["symbol"].map(security_type)
    hk = set(zip(halts["symbol"], halts["date"]))
    p["halted_official"] = [(s, d) in hk for s, d in zip(p["symbol"], p["date"])]
    term, transfers = terminations(li)
    tmap = dict(zip(term["symbol"], term["date"]))
    texch = dict(zip(term["symbol"], term["exchange"]))
    p["delisted_date"] = pd.to_datetime(p["symbol"].map(tmap))
    p["delisted_official"] = p["delisted_date"].notna() & (p["date"] >= p["delisted_date"]) \
        & (p["exchange"] == p["symbol"].map(texch))
    p["listing_date"] = pd.to_datetime(p["symbol"].map(first_listing(li)))
    p["table_present"] = [(e, d) in tables for e, d in zip(p["exchange"], p["date"])]
    p["liquidity_bucket"] = liquidity_bucket((p["volume_shares"].fillna(0) / 1000).where(p["official_traded"])) \
        .replace("no_finlab_trade", "no_trade")
    ol, ot, fp, h = p["official_listed"], p["official_traded"], p["finlab_price"], p["halted_official"]
    p["coverage_class"] = np.select(
        [ol & ot & fp, ol & h & ~fp, ol & h & fp, ol & ~ot & ~fp, ol & ~ot & fp, ol & ot & ~fp,
         ~ol & fp & p["delisted_official"], ~ol & fp & p["listing_date"].notna() & (p["date"] < p["listing_date"]),
         ~ol & p["exchange"].notna() & ~p["table_present"], ~ol & fp],
        ["OK_TRADED_PRICED", "HALTED_NO_PRICE", "HALTED_PRICE_PRESENT", "OK_NO_TRADE_NO_PRICE",
         "NO_TRADE_PRICE_PRESENT", "OFFICIAL_TRADED_PRICE_MISSING", "PRICE_AFTER_OFFICIAL_DELISTING",
         "PRICE_BEFORE_OFFICIAL_LISTING", "OFFICIAL_TABLE_MISSING", "PRICE_WITHOUT_OFFICIAL_ROW"],
        default="NO_SOURCE_SIGNAL")
    p = p.reset_index(drop=True)
    return p[PANEL_COLUMNS + ["delisted_date", "listing_date"]], transfers


def symbol_table(p: pd.DataFrame, li: pd.DataFrame, transfers: pd.DataFrame, last_day: pd.Timestamp,
                 stop_gap_sessions: int = 20) -> pd.DataFrame:
    cal = pd.DatetimeIndex(sorted(p["date"].unique()))
    term, _ = terminations(li)
    g = p.groupby("symbol")
    off = p[p["official_listed"]].groupby("symbol")["date"]
    s = pd.DataFrame({
        "security_type": g["security_type"].first(),
        "exchanges": p[p["official_listed"]].groupby("symbol")["exchange"].agg(lambda x: ",".join(sorted(set(x)))),
        "first_official_date": off.min(), "last_official_date": off.max(),
        "last_observed_price_date": p[p["finlab_price"]].groupby("symbol")["date"].max(),
        "last_official_traded_date": p[p["official_traded"]].groupby("symbol")["date"].max(),
        "official_days": g["official_listed"].sum(), "official_traded_days": g["official_traded"].sum(),
        "price_missing_traded_days": g["coverage_class"].agg(lambda x: int((x == "OFFICIAL_TRADED_PRICE_MISSING").sum())),
        "price_after_delisting_days": g["coverage_class"].agg(lambda x: int((x == "PRICE_AFTER_OFFICIAL_DELISTING").sum())),
    }).reset_index()
    s["official_listing_date"] = pd.to_datetime(s["symbol"].map(first_listing(li)))
    s["official_delisting_date"] = s["symbol"].map(dict(zip(term["symbol"], term["date"])))
    s["delisting_report"] = s["symbol"].map(dict(zip(term["symbol"], term["report"])))
    tr: dict[str, list] = {}
    for sy, dt in zip(transfers["symbol"], transfers["date"]):
        tr.setdefault(sy, []).append(pd.Timestamp(dt).strftime("%Y-%m-%d"))
    s["transfer_dates"] = [",".join(sorted(tr.get(x, []))) for x in s["symbol"]]
    pos_last = cal.searchsorted(s["last_official_date"].fillna(cal.min()))
    stopped = s["last_official_date"].notna() & ((len(cal) - 1 - pos_last) >= stop_gap_sessions)
    s["termination_status"] = np.select(
        [s["official_delisting_date"].notna(), stopped, s["last_official_date"].isna()],
        ["DELISTED_OFFICIAL", "STOPPED_NO_OFFICIAL_RECORD", "NEVER_IN_OFFICIAL_TABLES"], default="ACTIVE")
    return s[SYMBOL_COLUMNS].sort_values("symbol").reset_index(drop=True)


def valuation_status(p: pd.DataFrame, symbol: str, date: pd.Timestamp) -> str:
    """How a held position may be valued on `date`. Never returns a liquidation at the last available price."""
    x = p[(p["symbol"] == symbol) & (p["date"] == date)]
    if x.empty:
        known = p.loc[p["symbol"] == symbol, "delisted_date"].dropna()
        if len(known) and date >= known.iloc[0]:
            return "DELISTED_NO_EXIT_PRICE"
        return "UNPRICED_NO_SOURCE"
    r = x.iloc[0]
    if r["delisted_official"]:
        return "DELISTED_NO_EXIT_PRICE"
    if r["halted_official"]:
        return "UNPRICED_HALTED"
    if r["finlab_price"] and r["official_listed"]:
        return "PRICED"
    if r["official_listed"]:
        return "UNPRICED_LISTED_NO_TRADE"
    return "UNPRICED_NO_OFFICIAL_ROW"


def coverage(p: pd.DataFrame, sym: pd.DataFrame) -> pd.DataFrame:
    x = p[p["official_traded"]].merge(sym[["symbol", "termination_status"]], on="symbol", how="left")
    x = x.assign(year=x["date"].dt.year)
    return x.groupby(["year", "exchange", "security_type", "liquidity_bucket", "termination_status"]).agg(
        official_traded_days=("symbol", "size"), finlab_priced=("finlab_price", "sum"),
        fubon_bars=("fubon_bars", "sum"), halted=("halted_official", "sum")).reset_index() \
        .assign(price_coverage=lambda d: d["finlab_priced"] / d["official_traded_days"])


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def run(official_root: Path, ca_root: Path, out_root: Path, data_root: Path | None = None,
        gap_ledger: Path | None = None, log=print) -> Path:
    out = Path(out_root) / f"universe_pit_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    for bad in ("fubon_stock_5m", "finlab_stock_daily", "official_volume", "quality_acceptance", "corporate_actions"):
        if bad in out.resolve().parts:
            raise SystemExit(f"refusing to write inside {bad}: {out}")
    out.mkdir(parents=True, exist_ok=False)
    st = Stages(out, "universe_pit", {"official_root": str(official_root), "ca_root": str(ca_root),
                                      "data_root": str(data_root), "gap_ledger": str(gap_ledger),
                                      "version": PIT_VERSION}, log=log)
    summary: dict = {"version": PIT_VERSION}
    with st.stage("evidence") as rec:
        od = load_official_daily(official_root, log=log)
        wide = CE.load_wide(ca_root)
        li = CE.build_listings(wide)
        raw = CE.load_lake(Path(data_root), log=log)[0] if data_root else None
        bars = None
        if gap_ledger and Path(gap_ledger).exists():
            bars = pd.read_parquet(gap_ledger, columns=["symbol", "date", "real_bars"])
            bars["symbol"] = bars["symbol"].astype(str)
            bars["date"] = pd.to_datetime(bars["date"])
        cal = pd.DatetimeIndex(sorted(od["date"].unique()))
        halts = halt_days(wide, cal)
        rec.update(official_rows=len(od), sessions=len(cal), halt_days=len(halts))
        summary["sessions"] = {"n": len(cal), "first": str(cal.min().date()), "last": str(cal.max().date())}
        summary["official_tables"] = od.groupby("exchange")["date"].nunique().to_dict()
    with st.stage("panel") as rec:
        p, transfers = build_panel(od, raw, li, halts, bars)
        sym = symbol_table(p, li, transfers, cal.max())
        p.to_parquet(out / "universe_panel.parquet", index=False)
        sym.to_parquet(out / "universe_symbols.parquet", index=False)
        sym.to_csv(out / "universe_symbols.csv", index=False, encoding="utf-8-sig")
        transfers.to_csv(out / "board_transfers.csv", index=False, encoding="utf-8-sig")
        cov = coverage(p, sym)
        cov.to_csv(out / "coverage_breakdown.csv", index=False, encoding="utf-8-sig")
        summary["coverage_class"] = p["coverage_class"].value_counts().to_dict()
        summary["coverage_class_by_type"] = p.groupby(["security_type", "coverage_class"]).size().rename("n") \
            .reset_index().to_dict("records")
        summary["termination_status"] = sym["termination_status"].value_counts().to_dict()
        tr = p[p["official_traded"]]
        summary["official_traded_symbol_days"] = int(len(tr))
        summary["official_traded_price_missing"] = int((tr["coverage_class"] == "OFFICIAL_TRADED_PRICE_MISSING").sum())
        summary["price_missing_rate_aggregate"] = float(summary["official_traded_price_missing"] / max(len(tr), 1))
        summary["price_missing_rate_by_termination"] = cov.groupby("termination_status")[
            ["official_traded_days", "finlab_priced"]].sum().assign(
            missing_rate=lambda d: 1 - d["finlab_priced"] / d["official_traded_days"]).reset_index().to_dict("records")
        summary["board_transfers"] = int(len(transfers))
        summary["delisted_with_price_after"] = int((sym["price_after_delisting_days"] > 0).sum())
        summary["statement"] = ("aggregate missing rate is not evidence of no survivorship bias; see the breakdown by "
                                "termination status and security type")
        for k, cls in (("official_traded_price_missing", "OFFICIAL_TRADED_PRICE_MISSING"),
                       ("price_after_official_delisting", "PRICE_AFTER_OFFICIAL_DELISTING"),
                       ("price_without_official_row", "PRICE_WITHOUT_OFFICIAL_ROW")):
            p[p["coverage_class"] == cls].to_csv(out / f"{k}.csv", index=False, encoding="utf-8-sig")
        rec.update(rows=len(p), symbols=len(sym))
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    files = sorted(x for x in out.iterdir() if x.is_file() and x.name not in ("run_info.json", "manifest.json"))
    (out / "manifest.json").write_text(json.dumps({
        "dataset": "universe_pit", "version": PIT_VERSION, "code_commit": git_sha(),
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "sources": {"official_daily": str(official_root), "corporate_actions": str(ca_root)},
        "sha256": {x.name: _sha(x) for x in files},
        "statement": "reconciliation only; lake, reference, masks, can_use_* and research_tier untouched"},
        ensure_ascii=False, indent=2), encoding="utf-8")
    st.finish(**{k: v for k, v in summary.items() if isinstance(v, (int, float, str))})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tw_lake.audit.universe_pit")
    ap.add_argument("--official-root", required=True)
    ap.add_argument("--ca-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--data-root", default="")
    ap.add_argument("--gap-ledger", default="")
    a = ap.parse_args(argv)
    out = run(Path(a.official_root), Path(a.ca_root), Path(a.out_root), Path(a.data_root) if a.data_root else None,
              Path(a.gap_ledger) if a.gap_ledger else None)
    print("written:", out)
    print((out / "summary.json").read_text(encoding="utf-8")[:4000])
    return 0


if __name__ == "__main__":
    sys.exit(main())
