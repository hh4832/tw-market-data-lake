"""B4a - price limits and execution eligibility: a separate, versioned trading-rules dataset (offline).

python -m tw_lake.audit.trading_rules --official-root .../reference/official_volume/v1 \
    --ca-root .../reference/corporate_actions/v1 --data-root /content/drive/MyDrive/00Quant_Research/data \
    --out-root .../derived/trading_rules

One row per official symbol-day (TWSE MI_INDEX ALLBUT0999 / TPEx dailyQuotes EW, archived; no request).

price_limit_status (validity of the limit band, independent of trading):
  VERIFIED_OFFICIAL_EVENT     limits printed in the official corporate-action table for this session
  VERIFIED_OFFICIAL_NEXT_DAY  limits TPEx published for this session in the previous session's quotes
  NO_LIMIT_VERIFIED           an official table prints the no-limit sentinel (9999.95 / 0.01) for this session
  NO_LIMIT_NEW_LISTING        first five sessions from an official listing date
  NO_LIMIT_OFFICIAL_CLASS     fund-like symbol whose official tables only ever show the no-limit sentinel
  DERIVED                     tick table x percentage from an official reference: the previous session's official
                              close (symbol traded that session) or, failing that, this session's official close
                              minus official change; instrument class known
  DERIVED_CONFLICT            derived band contradicted by the observed high / low (treated as UNKNOWN)
  UNKNOWN_INSTRUMENT_CLASS    fund-like symbol (ETF / ETN / REIT) without official limit evidence - fail closed
  UNKNOWN_NO_REFERENCE        no official reference for the session; older closes are NEVER carried forward
buy_eligibility / sell_eligibility (daily summary of whether an order could have executed; never a fill promise):
  UNKNOWN (limit status not usable) | BLOCKED_NO_TRADE | BLOCKED_LOCKED_LIMIT_UP / _DOWN (high == low == limit)
  | CONDITIONAL_LIMIT_TOUCHED | CONDITIONAL_NO_INTRADAY_RANGE | ELIGIBLE_SUBJECT_TO_LIQUIDITY
Closing auction: a signal that uses the session's closing price is known only after the closing call auction
(13:25-13:30, possibly delayed to 13:33); it can never execute in that same auction - see earliest_execution().
Nothing in the lake, the reference datasets or the quality mask is modified; can_use_* is not touched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, time
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..reference.official_daily import lake_scope, load_official_daily, security_type
from ..run_context import git_sha
from . import corporate_events as CE
from .bars import liquidity_bucket
from .stagelog import Stages

RULES_VERSION = "1.0.0"
CLOSE_AUCTION_START, REGULAR_CLOSE, DELAYED_CLOSE_LATEST = time(13, 25), time(13, 30), time(13, 33)
USABLE = {"VERIFIED_OFFICIAL_EVENT", "VERIFIED_OFFICIAL_NEXT_DAY", "NO_LIMIT_VERIFIED", "NO_LIMIT_NEW_LISTING",
          "NO_LIMIT_OFFICIAL_CLASS", "DERIVED"}
NO_LIMIT = {"NO_LIMIT_VERIFIED", "NO_LIMIT_NEW_LISTING", "NO_LIMIT_OFFICIAL_CLASS"}
OUT_COLUMNS = ["exchange", "date", "symbol", "instrument_class", "fund_limit_class", "traded", "volume_lots",
               "liquidity_bucket", "official_close", "reference_price", "reference_source", "limit_up", "limit_down",
               "price_limit_status", "finlab_high", "finlab_low", "limit_consistent", "buy_eligibility",
               "sell_eligibility", "raw_sha256"]


def _tick(p: np.ndarray, fund: np.ndarray) -> np.ndarray:
    stock = np.select([p < 10, p < 50, p < 100, p < 500, p < 1000], [0.01, 0.05, 0.1, 0.5, 1.0], default=5.0)
    return np.where(fund, np.where(p < 50, 0.01, 0.05), stock)


def limit_prices_vec(ref, symbols: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """Vectorised corporate_events.limit_prices (same rounding: up floored, down ceiled, tick of the result's band)."""
    ref = np.asarray(ref, dtype=float)
    u = pd.Series(symbols.unique())
    fund = symbols.map(dict(zip(u, u.map(CE.is_fund_like)))).to_numpy(dtype=bool)
    pct = symbols.map(dict(zip(u, u.map(CE.pct_for)))).to_numpy(dtype=float)
    ok = np.isfinite(ref) & (ref > 0)
    r = np.where(ok, ref, 1.0)
    up_raw, dn_raw = r * (1 + pct), r * (1 - pct)
    t = _tick(up_raw, fund)
    up = np.floor(up_raw / t + 1e-9) * t
    t2 = _tick(up, fund)
    up = np.where(t2 != t, np.floor(up_raw / t2 + 1e-9) * t2, up)
    t = _tick(dn_raw, fund)
    dn = np.ceil(dn_raw / t - 1e-9) * t
    t2 = _tick(dn, fund)
    dn = np.where(t2 != t, np.ceil(dn_raw / t2 - 1e-9) * t2, dn)
    return np.where(ok, np.round(up, 2), np.nan), np.where(ok, np.round(dn, 2), np.nan)


def _is_sentinel(up, dn) -> np.ndarray:
    return (np.asarray(up, dtype=float) >= CE.NO_LIMIT_UP) & (np.asarray(dn, dtype=float) <= CE.NO_LIMIT_DOWN)


def fund_limit_classes(ev_limits: pd.DataFrame, od: pd.DataFrame) -> dict[str, str]:
    """Fund-like symbol -> LIMITED / NO_LIMIT / CONFLICT, from official evidence only (corporate-action table limits
    and TPEx next-day limits). Symbols absent from the map are UNKNOWN (fail closed)."""
    a = ev_limits[["symbol", "limit_up", "limit_down"]]
    b = od[["symbol", "next_limit_up", "next_limit_down"]].rename(columns={"next_limit_up": "limit_up",
                                                                         "next_limit_down": "limit_down"})
    x = pd.concat([a, b], ignore_index=True).dropna()
    x = x[x["symbol"].map(CE.is_fund_like) & (x["limit_up"] > 0)]
    x["nolim"] = _is_sentinel(x["limit_up"], x["limit_down"])
    g = x.groupby("symbol")["nolim"].agg(["all", "any"])
    return {s: ("NO_LIMIT" if r["all"] else "LIMITED" if not r["any"] else "CONFLICT") for s, r in g.iterrows()}


def event_limits(ev: pd.DataFrame, cal: pd.DatetimeIndex) -> pd.DataFrame:
    """Official corporate-action limits keyed on the market session they apply to."""
    e = ev.copy()
    for c in ("limit_up", "limit_down", "ref_price"):
        e[c] = pd.to_numeric(e[c], errors="coerce").astype(float)
    e["event_date"] = pd.to_datetime(e["event_date"])
    e = e.dropna(subset=["limit_up", "limit_down", "event_date"])
    pos = cal.searchsorted(e["event_date"].values, side="left")
    e["date"] = [cal[i] if i < len(cal) else pd.NaT for i in pos]
    return e.dropna(subset=["date"]).drop_duplicates(["symbol", "date"])[["symbol", "date", "limit_up", "limit_down",
                                                                         "ref_price"]]


def build_rules(od: pd.DataFrame, ev: pd.DataFrame, listings: pd.DataFrame, raw: pd.DataFrame | None = None
                ) -> pd.DataFrame:
    od = od.sort_values(["symbol", "date"]).reset_index(drop=True)
    for c in ("volume_shares", "close", "change", "reference_price", "next_reference", "next_limit_up",
              "next_limit_down"):
        od[c] = pd.to_numeric(od[c], errors="coerce").astype(float)
    cal = pd.DatetimeIndex(sorted(od.loc[od["volume_shares"].fillna(0) > 0, "date"].unique()))
    prev_session = pd.Series(cal[:-1], index=cal[1:])
    d = od.copy()
    d["traded"] = (d["volume_shares"].fillna(0) > 0) & d["close"].notna()
    g = d.groupby("symbol")
    for c in ("date", "close", "traded", "next_limit_up", "next_limit_down", "next_reference"):
        d["p_" + c] = g[c].shift(1)
    d["prev_session"] = d["date"].map(prev_session)
    contiguous = d["p_date"].notna() & (d["p_date"] == d["prev_session"])
    us = pd.Series(d["symbol"].unique())
    d["instrument_class"] = d["symbol"].map(dict(zip(us, us.map(security_type))))
    el = event_limits(ev, cal)
    d = d.merge(el.rename(columns={"limit_up": "ev_up", "limit_down": "ev_dn", "ref_price": "ev_ref"}),
                on=["symbol", "date"], how="left")
    fcls = fund_limit_classes(el, od)
    fund = d["symbol"].map(dict(zip(us, us.map(CE.is_fund_like)))).astype(bool)
    d["fund_limit_class"] = np.where(fund, d["symbol"].map(fcls).fillna("UNKNOWN"), "NOT_FUND")
    lst = listings[listings["event"].isin(["listed", "listed_current_isin"])].groupby("symbol")["date"].max()
    ld = pd.to_datetime(d["symbol"].map(lst))
    li_pos = np.where(ld.notna(), cal.searchsorted(ld.fillna(cal.min()).to_numpy()), -10**9)
    new_listing = ld.notna() & (d["date"] >= ld) & ((cal.searchsorted(d["date"].to_numpy()) - li_pos) < 5)

    nx_up = d["p_next_limit_up"].where(contiguous)
    nx_dn = d["p_next_limit_down"].where(contiguous)
    prev_ref = d["p_close"].where(contiguous & d["p_traded"].fillna(False).astype(bool))
    ref = prev_ref.fillna(d["reference_price"])
    d["reference_source"] = np.select(
        [d["ev_ref"].notna(), nx_up.notna(), prev_ref.notna(), d["reference_price"].notna()],
        ["OFFICIAL_EVENT_TABLE", "OFFICIAL_NEXT_DAY", "PREV_SESSION_OFFICIAL_CLOSE", "OFFICIAL_CLOSE_MINUS_CHANGE"],
        default="NONE")
    d["reference_price"] = np.select([d["ev_ref"].notna(), nx_up.notna()],
                                     [d["ev_ref"], d["p_next_reference"].where(contiguous)], default=ref)
    der_up, der_dn = limit_prices_vec(ref, d["symbol"])
    ev_has, nx_has = d["ev_up"].notna(), nx_up.notna()
    status = np.select(
        [ev_has & _is_sentinel(d["ev_up"], d["ev_dn"]), ev_has,
         nx_has & _is_sentinel(nx_up, nx_dn), nx_has,
         new_listing, fund & d["fund_limit_class"].eq("NO_LIMIT"),
         fund & ~d["fund_limit_class"].eq("LIMITED"), ~np.isnan(der_up)],
        ["NO_LIMIT_VERIFIED", "VERIFIED_OFFICIAL_EVENT", "NO_LIMIT_VERIFIED", "VERIFIED_OFFICIAL_NEXT_DAY",
         "NO_LIMIT_NEW_LISTING", "NO_LIMIT_OFFICIAL_CLASS", "UNKNOWN_INSTRUMENT_CLASS", "DERIVED"],
        default="UNKNOWN_NO_REFERENCE")
    d["limit_up"] = np.select([ev_has, nx_has], [d["ev_up"], nx_up], default=der_up)
    d["limit_down"] = np.select([ev_has, nx_has], [d["ev_dn"], nx_dn], default=der_dn)
    nolim = pd.Series(status).isin(NO_LIMIT).to_numpy() | (pd.Series(status) == "UNKNOWN_INSTRUMENT_CLASS").to_numpy()
    d.loc[nolim, ["limit_up", "limit_down"]] = np.nan
    if raw is not None and len(raw):
        r = raw[["symbol", "date", "high", "low"]].rename(columns={"high": "finlab_high", "low": "finlab_low"})
        d = d.merge(r, on=["symbol", "date"], how="left")
    else:
        d["finlab_high"] = d["finlab_low"] = np.nan
    has_rng = d["finlab_high"].notna() & d["finlab_low"].notna() & d["limit_up"].notna()
    inside = (d["finlab_high"] <= d["limit_up"] + 1e-6) & (d["finlab_low"] >= d["limit_down"] - 1e-6)
    d["limit_consistent"] = np.where(has_rng, np.where(inside, "TRUE", "FALSE"), "UNKNOWN")
    status = np.where((status == "DERIVED") & (d["limit_consistent"] == "FALSE"), "DERIVED_CONFLICT", status)
    d["price_limit_status"] = status
    d["buy_eligibility"], d["sell_eligibility"] = eligibility(d)
    d["volume_lots"] = d["volume_shares"].fillna(0) / 1000.0
    d["liquidity_bucket"] = liquidity_bucket(d["volume_lots"].where(d["traded"])).replace("no_finlab_trade", "no_trade")
    d["official_close"] = d["close"]
    return d[OUT_COLUMNS].sort_values(["date", "exchange", "symbol"]).reset_index(drop=True)


def eligibility(d: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    usable = d["price_limit_status"].isin(USABLE)
    limited = d["limit_up"].notna()
    rng = d["finlab_high"].notna() & d["finlab_low"].notna()
    flat = rng & np.isclose(d["finlab_high"], d["finlab_low"])
    out = []
    for lim, blocked in ((d["limit_up"], "BLOCKED_LOCKED_LIMIT_UP"), (d["limit_down"], "BLOCKED_LOCKED_LIMIT_DOWN")):
        side_px = d["finlab_high"] if blocked.endswith("UP") else d["finlab_low"]
        locked = limited & flat & np.isclose(d["finlab_high"], lim)
        touched = limited & rng & np.isclose(side_px, lim)
        out.append(np.select(
            [~usable, ~d["traded"], locked, touched, ~rng],
            ["UNKNOWN", "BLOCKED_NO_TRADE", blocked, "CONDITIONAL_LIMIT_TOUCHED", "CONDITIONAL_NO_INTRADAY_RANGE"],
            default="ELIGIBLE_SUBJECT_TO_LIQUIDITY"))
    return out[0], out[1]


def earliest_execution(signal_inputs_at: pd.Timestamp, sessions: pd.DatetimeIndex) -> tuple[pd.Timestamp, str]:
    """Earliest order-execution opportunity for a signal whose LAST input is observed at signal_inputs_at (Taipei,
    naive). Inputs observed at or after the closing call auction start (13:25) - including the closing price,
    which may be fixed as late as 13:33 - cannot trade in that auction: the next session's opening auction."""
    day = signal_inputs_at.normalize()
    t = signal_inputs_at.time()
    if day in sessions and t < time(9, 0):
        return day + pd.Timedelta(hours=9), "OPENING_AUCTION_SAME_DAY"
    if day in sessions and t < CLOSE_AUCTION_START:
        return signal_inputs_at, "CONTINUOUS_OR_CLOSING_AUCTION_SAME_DAY_NOT_GUARANTEED"
    nxt = sessions[sessions > day]
    if not len(nxt):
        return pd.NaT, "NO_FUTURE_SESSION"
    return nxt[0] + pd.Timedelta(hours=9), "OPENING_AUCTION_NEXT_SESSION"


def counts(rules: pd.DataFrame) -> pd.DataFrame:
    r = rules.assign(year=rules["date"].dt.year)
    return r.groupby(["year", "exchange", "instrument_class", "liquidity_bucket", "price_limit_status"]).size() \
        .rename("symbol_days").reset_index()


def validation(rules: pd.DataFrame) -> dict:
    """Derived-rule check on sessions where an official band exists: recompute from the reference that the DERIVED
    path would have used and compare."""
    v = rules[rules["price_limit_status"].isin(["VERIFIED_OFFICIAL_EVENT", "VERIFIED_OFFICIAL_NEXT_DAY"])]
    cu, cd = limit_prices_vec(v["reference_price"], v["symbol"])
    up = np.isclose(cu, v["limit_up"], atol=1e-6)
    dn = np.isclose(cd, v["limit_down"], atol=1e-6)
    out = {}
    for st in ("VERIFIED_OFFICIAL_EVENT", "VERIFIED_OFFICIAL_NEXT_DAY"):
        m = (v["price_limit_status"] == st).to_numpy()
        out[st] = {"n": int(m.sum()), "limit_down_match": float(dn[m].mean()) if m.any() else None,
                   "both_match": float((up & dn)[m].mean()) if m.any() else None}
    der = rules[rules["price_limit_status"].isin(["DERIVED", "DERIVED_CONFLICT"])]
    out["derived_vs_observed_range"] = der["limit_consistent"].value_counts().to_dict()
    return out


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def run(official_root: Path, ca_root: Path, out_root: Path, data_root: Path | None = None, log=print) -> Path:
    out = Path(out_root) / f"trading_rules_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    for bad in ("fubon_stock_5m", "finlab_stock_daily", "official_volume", "quality_acceptance", "corporate_actions"):
        if bad in out.resolve().parts:
            raise SystemExit(f"refusing to write inside {bad}: {out}")
    out.mkdir(parents=True, exist_ok=False)
    st = Stages(out, "trading_rules", {"official_root": str(official_root), "ca_root": str(ca_root),
                                       "data_root": str(data_root), "version": RULES_VERSION}, log=log)
    summary: dict = {"version": RULES_VERSION}
    with st.stage("evidence") as rec:
        raw = CE.load_lake(Path(data_root), log=log)[0] if data_root else None
        lake_syms = set(raw["symbol"].unique()) if raw is not None else set()
        od = load_official_daily(official_root, log=log, symbol_filter=lake_scope(lake_syms))
        summary["out_of_scope_official_rows"] = int(od.attrs.get("out_of_scope_rows", 0))
        wide = CE.load_wide(ca_root)
        ev = CE.build_events(wide) if wide else pd.DataFrame(columns=CE.EVENT_COLUMNS)
        li = CE.build_listings(wide)
        if raw is not None:
            raw = raw[["symbol", "date", "high", "low"]]
        rec.update(official_rows=len(od), events=len(ev))
    with st.stage("build") as rec:
        rules = build_rules(od, ev, li, raw)
        rules.to_parquet(out / "trading_rules.parquet", index=False)
        c = counts(rules)
        c.to_csv(out / "status_counts.csv", index=False, encoding="utf-8-sig")
        summary["status_total"] = rules["price_limit_status"].value_counts().to_dict()
        summary["status_by_instrument"] = rules.groupby(["instrument_class", "price_limit_status"]).size() \
            .rename("n").reset_index().to_dict("records")
        summary["status_by_year_exchange"] = rules.assign(year=rules["date"].dt.year) \
            .groupby(["year", "exchange", "price_limit_status"]).size().rename("n").reset_index().to_dict("records")
        summary["status_by_liquidity"] = rules.groupby(["liquidity_bucket", "price_limit_status"]).size() \
            .rename("n").reset_index().to_dict("records")
        summary["reference_source"] = rules["reference_source"].value_counts().to_dict()
        summary["fund_limit_class_symbols"] = rules.drop_duplicates("symbol")["fund_limit_class"].value_counts().to_dict()
        summary["buy_eligibility"] = rules["buy_eligibility"].value_counts().to_dict()
        summary["sell_eligibility"] = rules["sell_eligibility"].value_counts().to_dict()
        summary["validation"] = validation(rules)
        rules[rules["price_limit_status"] == "DERIVED_CONFLICT"].to_csv(out / "derived_conflicts.csv", index=False,
                                                                       encoding="utf-8-sig")
        rec.update(rows=len(rules))
    (out / "execution_semantics.json").write_text(json.dumps({
        "closing_auction": {"start": "13:25", "regular_close": "13:30", "delayed_close_latest": "13:33"},
        "same_auction_execution_from_closing_signal": "never",
        "earliest_execution_after_closing_signal": "next session opening auction (09:00), fill not guaranteed",
        "limit_validity_vs_execution": "price_limit_status describes the band; buy/sell_eligibility describes whether "
                                       "an order could have executed; a usable band does not imply a fill",
        "fail_closed": "UNKNOWN_* and DERIVED_CONFLICT -> eligibility UNKNOWN; no carry-forward of older closes"},
        ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    files = sorted(p for p in out.iterdir() if p.is_file() and p.name not in ("run_info.json", "manifest.json"))
    (out / "manifest.json").write_text(json.dumps({
        "dataset": "trading_rules", "version": RULES_VERSION, "code_commit": git_sha(),
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "sources": {"official_daily": str(official_root), "corporate_actions": str(ca_root)},
        "sha256": {p.name: _sha(p) for p in files},
        "statement": "derived from archived official responses; immutable; lake, reference and masks untouched"},
        ensure_ascii=False, indent=2), encoding="utf-8")
    st.finish(**{k: v for k, v in summary.items() if isinstance(v, (int, str))})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tw_lake.audit.trading_rules")
    ap.add_argument("--official-root", required=True)
    ap.add_argument("--ca-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--data-root", default="")
    a = ap.parse_args(argv)
    out = run(Path(a.official_root), Path(a.ca_root), Path(a.out_root), Path(a.data_root) if a.data_root else None)
    print("written:", out)
    print((out / "summary.json").read_text(encoding="utf-8")[:4000])
    return 0


if __name__ == "__main__":
    sys.exit(main())
