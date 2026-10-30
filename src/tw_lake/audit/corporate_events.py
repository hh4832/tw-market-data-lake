"""B3 / B4 / B5 offline audit of the official corporate-action and listing reference (no network).

python -m tw_lake.audit.corporate_events --dataset-root .../reference/corporate_actions/v1 \
    --data-root /content/drive/MyDrive/00Quant_Research/data --out-dir /content/tw_lake_corp_events [--with-5m]

Stages
  events     normalise the saved official tables into one event table (ex-dividend / ex-right / capital
             reduction / par-value change / ETF split or reverse split) and one listing table (listed /
             delisted dates). Dates in ROC or AD formats are parsed; every row keeps report + raw sha256.
  limits     B4: recompute the official limit-up / limit-down prices from the official opening reference
             with the tick table (stock rule vs ETF rule) and report the match rate per instrument class.
  lake       (needs --data-root) B3: official pre-event close vs FinLab raw close of the previous priced day;
             FinLab raw high/low on the event day inside the official limits; FinLab adjusted-price factor
             jumps vs official events. B5: official listing / delisting dates vs FinLab first / last price
             dates; symbols the lake infers as delisted without an official delisting record.
  5m         (--with-5m) first stored 5m bar of each event day vs the official limits / opening reference.
Writes only to its own new output folder. Nothing in the lake, the reference dataset or the masks changes.
known_at of an event is taken as the open of its effective date (the official reference price is the
opening basis of that day); announcement dates are not available historically and are not assumed.
"""
from __future__ import annotations

import argparse
import gzip
import json
import math
import re
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from ..reference import corporate_actions as CA
from .stagelog import Stages

EVENTS_VERSION = "0.2.0"
EVENT_COLUMNS = ["exchange", "symbol", "name", "event_type", "event_date", "prev_close", "ref_price", "open_ref",
                 "limit_up", "limit_down", "cash_dividend", "stock_per_1000", "price_factor", "detail",
                 "known_at", "report", "period", "sha256"]
LISTING_COLUMNS = ["exchange", "symbol", "name", "event", "date", "detail", "report", "period", "sha256"]


# ------------------------------------------------------------------------------------------- parsing
def parse_tw_date(v) -> pd.Timestamp | None:
    """'112年05月17日' '112/05/17' '1120710' '112-02-10' '100.01.03' '2023/05/17' '20230517' -> Timestamp."""
    s = re.sub(r"<[^>]+>", "", str(v or "")).strip()
    m = re.fullmatch(r"(\d{2,4})\D+(\d{1,2})\D+(\d{1,2})\D*", s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
    else:
        digits = re.sub(r"\D", "", s)
        if len(digits) == 7:
            y, mo, d = int(digits[:3]), int(digits[3:5]), int(digits[5:])
        elif len(digits) == 8:
            y, mo, d = int(digits[:4]), int(digits[4:6]), int(digits[6:])
        else:
            return None
    if y < 1911:
        y += 1911
    try:
        return pd.Timestamp(date(y, mo, d))
    except ValueError:
        return None


def num(v) -> float:
    s = re.sub(r"<[^>]+>", "", str(v if v is not None else "")).replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return np.nan


def _symbol(v) -> str:
    return re.split(r"[\s　]+", re.sub(r"<[^>]+>", "", str(v)).strip())[0]


# ------------------------------------------------------------------------------------------- B4 ticks
def tick_stock(p: float) -> float:
    """TWSE 升降單位 for stocks (and, per the 2005 notice, beneficiary certificates)."""
    for lim, t in ((10, 0.01), (50, 0.05), (100, 0.1), (500, 0.5), (1000, 1.0)):
        if p < lim:
            return t
    return 5.0


def tick_etf(p: float) -> float:
    """Candidate ETF rule: 0.01 below 50, 0.05 from 50 (to be confirmed by the match rate)."""
    return 0.01 if p < 50 else 0.05


def is_fund_like(symbol: str) -> bool:
    """ETF ('00...'), REIT beneficiary securities ('01...T') and ETN ('02...'): the 0.01 / 0.05 tick table."""
    s = str(symbol)
    return s.startswith(("00", "02")) or (s.startswith("01") and s.endswith("T"))


def tick_for(symbol: str):
    return tick_etf if is_fund_like(symbol) else tick_stock


def pct_for(symbol: str) -> float:
    """Daily limit: 10 %; leveraged ETFs ('00...L', 2x) observed at 20 % on their split resumption days."""
    s = str(symbol)
    return 0.20 if s.startswith("00") and s.endswith("L") else 0.10


NO_LIMIT_UP, NO_LIMIT_DOWN = 9999.95, 0.01       # how the official tables show "no price limit"


def limit_prices(ref: float, tick=tick_stock, pct: float = 0.10) -> tuple[float, float]:
    """Limit up rounded down to a valid tick, limit down rounded up; the tick of the band the result lies in."""
    if not (ref and ref > 0 and math.isfinite(ref)):
        return np.nan, np.nan
    up_raw, dn_raw = ref * (1 + pct), ref * (1 - pct)
    t = tick(up_raw)
    up = math.floor(up_raw / t + 1e-9) * t
    if tick(up) != t:
        t = tick(up)
        up = math.floor(up_raw / t + 1e-9) * t
    t = tick(dn_raw)
    dn = math.ceil(dn_raw / t - 1e-9) * t
    if tick(dn) != t:
        t = tick(dn)
        dn = math.ceil(dn_raw / t - 1e-9) * t
    return round(up, 2), round(dn, 2)


# ------------------------------------------------------------------------------------------- load
def load_wide(root: Path) -> dict[str, pd.DataFrame]:
    """report -> wide frame (one row per table row; columns = official field names; + period, sha256).
    When a period exists in several dated copies (current month / snapshots) the newest copy is used."""
    shards: dict[tuple[str, str], Path] = {}
    for sd in sorted((Path(root) / "shards").glob("report=*/period=*")):
        if not CA._done(sd):
            continue
        rep, period = sd.parent.name.split("=", 1)[1], sd.name.split("=", 1)[1]
        base = re.sub(r"_asof_.*$", "", period)
        base = "snapshot" if base.startswith("snapshot_") else base
        prev = shards.get((rep, base))
        if prev is None or sd.name > prev.name:
            shards[(rep, base)] = sd
    frames: dict[str, list[pd.DataFrame]] = {}
    for (rep, _b), sd in sorted(shards.items()):
        r = json.loads((sd / "request.json").read_text(encoding="utf-8"))
        bf = next(sd.glob("body.*.gz"))
        lg = CA.flatten(rep, sd.name.split("=", 1)[1], "html" if ".html." in bf.name else "json",
                        gzip.decompress(bf.read_bytes()), r["sha256"])
        if len(lg):
            frames.setdefault(rep, []).append(lg)
    return widen(pd.concat([x for v in frames.values() for x in v], ignore_index=True)) if frames else {}


def widen(lg: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Long rows (report, period, table, row, field, value, sha256) -> report -> wide frame."""
    out = {}
    for rep, g in lg.groupby("report"):
        out[rep] = g.pivot_table(index=["period", "table", "row", "sha256"], columns="field", values="value",
                                 aggfunc="first").reset_index()
    return out


def _ev(w: pd.DataFrame, rep: str, exchange: str, etype, date_c, sym_c, name_c, prev_c, ref_c, open_c, up_c, dn_c,
        cash=None, stock=None, detail=None) -> pd.DataFrame:
    if w is None or not len(w):
        return pd.DataFrame(columns=EVENT_COLUMNS)
    g = lambda c: w[c] if c in w.columns else pd.Series(np.nan, index=w.index)  # noqa: E731
    e = pd.DataFrame({
        "exchange": exchange, "symbol": g(sym_c).map(_symbol), "name": g(name_c).astype(str).str.strip(),
        "event_type": etype(w) if callable(etype) else etype, "event_date": g(date_c).map(parse_tw_date),
        "prev_close": g(prev_c).map(num), "ref_price": g(ref_c).map(num), "open_ref": g(open_c).map(num),
        "limit_up": g(up_c).map(num), "limit_down": g(dn_c).map(num),
        "cash_dividend": cash(w) if callable(cash) else np.nan,
        "stock_per_1000": g(stock).map(num) if stock else np.nan,
        "detail": g(detail).astype(str).str.strip() if detail else "",
        "report": rep, "period": w["period"], "sha256": w["sha256"]})
    e["price_factor"] = e["ref_price"] / e["prev_close"]
    e["known_at"] = e["event_date"].dt.strftime("%Y-%m-%dT08:30:00+08:00")
    return e[EVENT_COLUMNS]


def _twse_div_type(w):
    return w["權/息"].astype(str).str.strip().map({"息": "cash_dividend", "權": "stock_dividend",
                                                   "權息": "stock_and_cash_dividend"}).fillna("ex_right_other")


def _tpex_div_type(w):
    return w["權/息"].astype(str).str.strip().map({"除息": "cash_dividend", "除權": "stock_dividend",
                                                   "除權息": "stock_and_cash_dividend"}).fillna("ex_right_other")


def _split_type(w):
    t = w["分割(反分割)"].astype(str).str.strip() if "分割(反分割)" in w.columns else pd.Series("", index=w.index)
    ratio = w["恢復買賣參考價"].map(num) / w["停止買賣前收盤價格"].map(num)
    guess = np.where(ratio < 1, "etf_split", np.where(ratio > 1, "etf_reverse_split", "etf_split_unknown"))
    return np.where(t == "分割", "etf_split", np.where(t == "反分割", "etf_reverse_split", guess))


def build_events(wide: dict[str, pd.DataFrame]) -> pd.DataFrame:
    w = wide.get
    parts = [
        _ev(w("TWSE_TWT49U"), "TWSE_TWT49U", "TWSE", _twse_div_type, "資料日期", "股票代號", "股票名稱",
            "除權息前收盤價", "除權息參考價", "開盤競價基準", "漲停價格", "跌停價格",
            cash=lambda x: np.where(x["權/息"].astype(str).str.strip() == "息", x["權值+息值"].map(num), np.nan)),
        _ev(w("TPEX_exDailyQ_b"), "TPEX_exDailyQ_b", "TPEX", _tpex_div_type, "除權息日期", "代號", "名稱",
            "除權息前收盤價", "除權息參考價", "開始交易基準價", "漲停價", "跌停價",
            cash=lambda x: x["現金股利"].map(num), stock="每仟股無償配股"),
        _ev(w("TWSE_TWTAUU"), "TWSE_TWTAUU", "TWSE", "capital_reduction", "恢復買賣日期", "股票代號", "名稱",
            "停止買賣前收盤價格", "恢復買賣參考價", "開盤競價基準", "漲停價格", "跌停價格", detail="減資原因"),
        _ev(w("TPEX_revivt_b"), "TPEX_revivt_b", "TPEX", "capital_reduction", "恢復買賣日期", "股票代號", "名稱",
            "最後交易日之收盤價格", "減資恢復買賣開始日參考價格", "開始交易基準價", "漲停價格", "跌停價格",
            detail="減資原因"),
        _ev(w("TWSE_TWTB8U"), "TWSE_TWTB8U", "TWSE", "par_value_change", "恢復買賣日期", "股票代號", "名稱",
            "停止買賣前收盤價格", "恢復買賣參考價", "開盤競價基準", "漲停價格", "跌停價格"),
        _ev(w("TPEX_pvChgRslt"), "TPEX_pvChgRslt", "TPEX", "par_value_change", "恢復買賣日期", "證券代號", "證券名稱",
            "最後交易日之收盤價格", "恢復買賣開始參考價", "開始交易基準價", "漲停價格", "跌停價格"),
        _ev(w("TWSE_TWTCAU"), "TWSE_TWTCAU", "TWSE", _split_type, "恢復買賣日期", "ETF代號", "名稱",
            "停止買賣前收盤價格", "恢復買賣參考價", "開盤競價基準", "漲停價格", "跌停價格"),
    ]
    for rep in ("TPEX_etfSplitRslt", "TPEX_etfRvsRslt"):        # no rows in 2023-2026 so far; fields unseen
        if w(rep) is not None and len(w(rep)):
            x = w(rep)
            cols = list(x.columns)
            pick = lambda *keys: next((c for c in cols if any(k in c for k in keys)), None)  # noqa: E731
            parts.append(_ev(x, rep, "TPEX", "etf_split" if "Split" in rep else "etf_reverse_split",
                             pick("恢復買賣日期", "日期"), pick("代號"), pick("名稱"), pick("收盤"),
                             pick("參考價"), pick("基準"), pick("漲停"), pick("跌停")))
    ev = pd.concat([p for p in parts if len(p)], ignore_index=True)
    ev = ev[ev["symbol"].str.len() > 0].drop_duplicates(["exchange", "symbol", "event_type", "event_date"])
    return ev.sort_values(["event_date", "exchange", "symbol"]).reset_index(drop=True)


def build_listings(wide: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = []

    def add(rep, exchange, event, sym_c, date_c, name_c=None, detail_c=None, sym_fn=_symbol):
        x = wide.get(rep)
        if x is None or not len(x) or sym_c not in x.columns or date_c not in x.columns:
            return
        out.append(pd.DataFrame({
            "exchange": exchange, "symbol": x[sym_c].map(sym_fn), "event": event,
            "name": x[name_c].astype(str).str.strip() if name_c and name_c in x.columns else "",
            "date": x[date_c].map(parse_tw_date),
            "detail": x[detail_c].astype(str).str.strip() if detail_c and detail_c in x.columns else "",
            "report": rep, "period": x["period"], "sha256": x["sha256"]}))
    add("TWSE_suspendListing", "TWSE", "delisted", "上市編號", "終止上市日期", "公司名稱")
    add("TPEX_deListed_y", "TPEX", "delisted", "股票代號", "終止上櫃日期", "公司名稱", "終止上櫃原因")
    add("TWSE_newlisting", "TWSE", "listed", "公司代號", "股票上市買賣日期", "公司簡稱", "承銷價")
    add("TPEX_latest_y", "TPEX", "listed", "股票代號", "上櫃日期", "公司名稱")
    for rep, ex in (("ISIN_TWSE", "TWSE"), ("ISIN_TPEX", "TPEX")):
        x = wide.get(rep)
        if x is not None and len(x) and "上市日" in x.columns:
            x = x[x["上市日"].astype(str).str.match(r"^\d{4}/\d{2}/\d{2}$")]
            add_df = pd.DataFrame({
                "exchange": ex, "symbol": x["有價證券代號及名稱"].map(_symbol), "event": "listed_current_isin",
                "name": x["有價證券代號及名稱"].astype(str).str.split(r"[\s　]+", n=1).str[-1],
                "date": x["上市日"].map(parse_tw_date), "detail": x.get("CFICode", pd.Series("", index=x.index)),
                "report": rep, "period": x["period"], "sha256": x["sha256"]})
            out.append(add_df)
    if not out:
        return pd.DataFrame(columns=LISTING_COLUMNS)
    li = pd.concat(out, ignore_index=True)
    li = li[li["symbol"].str.len() > 0].drop_duplicates(["exchange", "symbol", "event", "date"])
    return li[LISTING_COLUMNS].sort_values(["date", "symbol"]).reset_index(drop=True)


# ------------------------------------------------------------------------------------------- checks
def check_limits(ev: pd.DataFrame) -> pd.DataFrame:
    """B4: official event-day limits vs a recomputation with the tick table of the instrument class.
    Observed convention: limit down from the official reference price; limit up from the reference price or
    (stock dividends / rights) from the opening reference. Remaining mismatches are listed, not explained away."""
    e = ev.copy()
    e["instrument"] = np.where(e["symbol"].map(is_fund_like), "fund_like", "stock")
    e["no_price_limit"] = (e["limit_up"] >= NO_LIMIT_UP) & (e["limit_down"] <= NO_LIMIT_DOWN)
    e["has_official_limits"] = (e["limit_up"] > 0) & (e["limit_down"] > 0) & ~e["no_price_limit"]
    ref = [limit_prices(r, tick_for(s), pct_for(s)) for r, s in zip(e["ref_price"], e["symbol"])]
    opn = [limit_prices(r, tick_for(s), pct_for(s)) for r, s in zip(e["open_ref"], e["symbol"])]
    e["calc_up_ref"], e["calc_dn_ref"] = [x[0] for x in ref], [x[1] for x in ref]
    e["calc_up_open"] = [x[0] for x in opn]
    up_ref = np.isclose(e["calc_up_ref"], e["limit_up"], atol=1e-6)
    up_open = np.isclose(e["calc_up_open"], e["limit_up"], atol=1e-6)
    dn_ref = np.isclose(e["calc_dn_ref"], e["limit_down"], atol=1e-6)
    e["limit_up_basis"] = np.where(up_ref, "ref_price", np.where(up_open, "open_ref", "unexplained"))
    e["limit_rule_match"] = (up_ref | up_open) & dn_ref
    return e


def limit_summary(e: pd.DataFrame) -> pd.DataFrame:
    x = e[e["has_official_limits"]]
    return x.groupby(["instrument", "exchange", "event_type"]).agg(
        events=("symbol", "size"), rule_match=("limit_rule_match", "mean"),
        up_from_open_ref=("limit_up_basis", lambda s: float((s == "open_ref").mean()))).reset_index()


def normal_day_limits(raw: pd.DataFrame, ev: pd.DataFrame, li: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """B4 on ordinary days: limits from the previous close with the instrument's tick table and percentage;
    FinLab raw high / low must stay inside. Event days (official limits apply), symbols the official tables show
    without limits, and the first five trading days after a listing are excluded and counted separately."""
    r = raw[raw["close"].notna() & raw["high"].notna()][["symbol", "date", "high", "low", "close"]] \
        .sort_values(["symbol", "date"]).copy()
    r["prev_close"] = r.groupby("symbol")["close"].shift(1)
    r["n_day"] = r.groupby("symbol").cumcount()
    r = r[r["prev_close"] > 0]
    evk = set(zip(ev["symbol"], ev["event_date"]))
    nolim = set(ev.loc[(ev["limit_up"] >= NO_LIMIT_UP) & (ev["limit_down"] <= NO_LIMIT_DOWN), "symbol"])
    listed = li[li["event"] == "listed"].groupby("symbol")["date"].max()
    r["listing_date"] = r["symbol"].map(listed)
    r["class"] = np.select(
        [[(s, d) in evk for s, d in zip(r["symbol"], r["date"])], r["symbol"].isin(nolim),
         r["listing_date"].notna() & (r["date"] >= r["listing_date"]) & (r["n_day"] < 5)],
        ["event_day", "no_limit_symbol", "first_5_days_after_listing"], default="ordinary")
    o = r[r["class"] == "ordinary"].copy()
    lp = [limit_prices(p, tick_for(s), pct_for(s)) for p, s in zip(o["prev_close"], o["symbol"])]
    o["limit_up"], o["limit_down"] = [x[0] for x in lp], [x[1] for x in lp]
    o["violation"] = (o["high"] > o["limit_up"] + 1e-6) | (o["low"] < o["limit_down"] - 1e-6)
    o["instrument"] = np.where(o["symbol"].map(is_fund_like), "fund_like", "stock")
    summ = pd.concat([
        r.groupby("class").size().rename("symbol_days").reset_index().assign(instrument="all"),
        o.groupby("instrument").agg(symbol_days=("symbol", "size"), violations=("violation", "sum"))
         .reset_index().assign(**{"class": "ordinary"})], ignore_index=True)
    return summ, o[o["violation"]]


def check_prev_close(ev: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    """Official pre-event close vs FinLab raw close of the last priced day before the event date."""
    r = raw[raw["close"].notna()][["symbol", "date", "close", "high", "low"]].sort_values("date")
    e = ev.sort_values("event_date").copy()
    prev = pd.merge_asof(e[["symbol", "event_date"]].assign(t=e["event_date"] - pd.Timedelta(days=1)).sort_values("t"),
                         r.rename(columns={"date": "prev_date", "close": "finlab_prev_close"})[
                             ["symbol", "prev_date", "finlab_prev_close"]].sort_values("prev_date"),
                         left_on="t", right_on="prev_date", by="symbol", direction="backward")
    e = e.merge(prev.drop(columns="t").drop_duplicates(["symbol", "event_date"]), on=["symbol", "event_date"],
                how="left")
    day = r.rename(columns={"date": "event_date", "close": "finlab_close", "high": "finlab_high", "low": "finlab_low"})
    e = e.merge(day, on=["symbol", "event_date"], how="left")
    e["prev_close_match"] = np.where(e["finlab_prev_close"].isna() | e["prev_close"].isna(), "UNKNOWN",
                                     np.where(np.isclose(e["prev_close"], e["finlab_prev_close"], atol=1e-6),
                                              "TRUE", "FALSE"))
    lim = (e["limit_up"] > 0) & (e["limit_down"] > 0) & e["finlab_high"].notna()
    e["event_day_within_limits"] = np.where(~lim, "UNKNOWN", np.where(
        (e["finlab_high"] <= e["limit_up"] + 1e-6) & (e["finlab_low"] >= e["limit_down"] - 1e-6), "TRUE", "FALSE"))
    return e


def adjusted_jumps(raw: pd.DataFrame, adj: pd.DataFrame, tol: float = 1e-4) -> pd.DataFrame:
    """FinLab adjusted/raw ratio per symbol-day; a change of the ratio between consecutive priced days is an
    adjustment event of the FinLab snapshot (date = first day on the new basis)."""
    m = raw[["symbol", "date", "close"]].merge(adj[["symbol", "date", "adj_close"]], on=["symbol", "date"])
    m = m[(m["close"] > 0) & (m["adj_close"] > 0)].sort_values(["symbol", "date"])
    m["ratio"] = m["adj_close"] / m["close"]
    m["prev_ratio"] = m.groupby("symbol")["ratio"].shift(1)
    m["implied_factor"] = m["prev_ratio"] / m["ratio"]          # adj convention: back-adjusts history
    j = m[(m["prev_ratio"].notna()) & ((m["implied_factor"] - 1).abs() > tol)]
    return j[["symbol", "date", "implied_factor"]].rename(columns={"date": "jump_date"}).reset_index(drop=True)


def match_jumps(ev: pd.DataFrame, jumps: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    e = ev.merge(jumps.rename(columns={"jump_date": "event_date"}), on=["symbol", "event_date"], how="left")
    e["finlab_adjusted"] = e["implied_factor"].notna()
    keys = set(zip(ev["symbol"], ev["event_date"]))
    jumps = jumps.copy()
    jumps["official_event"] = [(s, d) in keys for s, d in zip(jumps["symbol"], jumps["jump_date"])]
    return e, jumps


def check_listings(li: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    """B5: official listing / delisting dates vs FinLab first / last priced dates (2023-05 onwards scope)."""
    p = raw[raw["close"].notna()].groupby("symbol")["date"].agg(first_price_date="min", last_price_date="max")
    x = li[li["event"].isin(["listed", "delisted"])].merge(p, left_on="symbol", right_index=True, how="left")
    x["in_finlab"] = x["last_price_date"].notna()
    d = x["event"] == "delisted"
    x["status"] = np.select(
        [~x["in_finlab"], d & (x["last_price_date"] >= x["date"]), d & (x["last_price_date"] < x["date"]),
         ~d & (x["first_price_date"] < x["date"]), ~d & (x["first_price_date"] >= x["date"])],
        ["NOT_IN_FINLAB", "PRICE_ON_OR_AFTER_DELISTING", "OK_LAST_PRICE_BEFORE_DELISTING",
         "PRICE_BEFORE_LISTING", "OK_FIRST_PRICE_ON_OR_AFTER_LISTING"], default="UNKNOWN")
    x["gap_days"] = np.where(d, (x["date"] - x["last_price_date"]).dt.days, (x["first_price_date"] - x["date"]).dt.days)
    return x


def inferred_without_official(raw: pd.DataFrame, li: pd.DataFrame, last_day: pd.Timestamp, gap_days: int = 30
                              ) -> pd.DataFrame:
    """Symbols whose FinLab prices stop >= gap_days before the end and have no official delisting record."""
    p = raw[raw["close"].notna()].groupby("symbol")["date"].max().rename("last_price_date").reset_index()
    p = p[p["last_price_date"] < last_day - pd.Timedelta(days=gap_days)]
    off = set(li.loc[li["event"] == "delisted", "symbol"])
    p["official_delisting"] = p["symbol"].isin(off)
    p["code_class"] = np.select([p["symbol"].str.startswith("00"), p["symbol"].str.fullmatch(r"\d{4}")],
                                ["etf_like", "four_digit"], default="other")
    return p


# ------------------------------------------------------------------------------------------- B3 v0.2
def trading_calendar(raw: pd.DataFrame) -> pd.DatetimeIndex:
    """Market trading days = dates on which any symbol traded (FinLab raw, volume > 0)."""
    v = raw["volume_shares"] if "volume_shares" in raw.columns else raw["close"]
    return pd.DatetimeIndex(sorted(raw.loc[v.fillna(0) > 0, "date"].unique()))


def adjustment_factors(ev: pd.DataFrame, cal: pd.DatetimeIndex) -> pd.DataFrame:
    """Official price-adjustment table (replaces the FinLab adjusted snapshot).

    effective_date = first market trading day on or after the official date (official dates can fall on market
    closures, e.g. typhoon days; FinLab then adjusts on the next trading day). price_factor = official reference
    price / official pre-event close: multiply prices BEFORE effective_date by it to put them on the new basis.
    share_factor_est = 1 / price_factor for pure stock dividends, splits and reverse splits (shares held scale
    inversely; estimated, cash parts and rights subscriptions are not modelled) and NaN otherwise. known_at =
    effective_date 08:30 (the reference price is the opening basis of that day). Nothing here is applied to any
    stored price."""
    e = ev.copy()
    pos = cal.searchsorted(e["event_date"].values, side="left")
    e["effective_date"] = [cal[i] if i < len(cal) else pd.NaT for i in pos]
    e["date_shifted"] = e["effective_date"].notna() & (e["effective_date"] != e["event_date"])
    e["price_factor"] = e["ref_price"] / e["prev_close"]
    pure = e["event_type"].isin(["stock_dividend", "etf_split", "etf_reverse_split", "par_value_change"])
    e["share_factor_est"] = np.where(pure, 1 / e["price_factor"], np.nan)
    e["known_at"] = e["effective_date"].dt.strftime("%Y-%m-%dT08:30:00+08:00")
    e["factor_valid"] = e["price_factor"].between(1e-3, 1e3) & e["effective_date"].notna()
    cols = ["exchange", "symbol", "name", "event_type", "event_date", "effective_date", "date_shifted", "prev_close",
            "ref_price", "price_factor", "share_factor_est", "cash_dividend", "stock_per_1000", "limit_up",
            "limit_down", "known_at", "factor_valid", "report", "sha256"]
    return e[cols].sort_values(["symbol", "effective_date"]).reset_index(drop=True)


def explain_jumps(jumps: pd.DataFrame, af: pd.DataFrame, since: str = "2023-05-01") -> pd.DataFrame:
    """FinLab adjustment jumps (in scope) -> matched on the official effective date / unexplained."""
    j = jumps[jumps["jump_date"] >= pd.Timestamp(since)].copy()
    keys = set(zip(af["symbol"], af["effective_date"]))
    raw_keys = set(zip(af["symbol"], af["event_date"]))
    j["status"] = np.select([[(s, d) in raw_keys for s, d in zip(j["symbol"], j["jump_date"])],
                             [(s, d) in keys for s, d in zip(j["symbol"], j["jump_date"])]],
                            ["MATCH_OFFICIAL_DATE", "MATCH_EFFECTIVE_DATE"], default="UNEXPLAINED")
    return j


def official_rows(wide: dict[str, pd.DataFrame], symbols: list[str]) -> pd.DataFrame:
    """Every official row (any report) that mentions one of the symbols - for case review (e.g. 8101)."""
    out = []
    for rep, w in wide.items():
        hit = w.apply(lambda r: any(re.split(r"[\s\u3000,]+", str(v).strip())[0] in symbols for v in r.values), axis=1)
        if hit.any():
            x = w[hit].copy()
            x.insert(0, "report", rep)
            out.append(x.astype(str))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def chtm_flags(wide: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """TPEx daily altered-trading list -> symbol, date, flags (Y when set)."""
    w = wide.get("TPEX_chtm")
    if w is None or not len(w):
        return pd.DataFrame(columns=["symbol", "date", "altered", "split_session", "managed", "cycle_min", "halted"])
    g = lambda c: w[c].fillna("").astype(str).str.strip() if c in w.columns else pd.Series("", index=w.index)  # noqa: E731
    return pd.DataFrame({"symbol": g("證券代號"), "date": pd.to_datetime(w["period"].str[:10], errors="coerce"),
                         "altered": g("變更交易").ne(""), "split_session": g("分盤交易").ne(""),
                         "managed": g("屬管理股票").ne(""), "cycle_min": g("分盤或管理股票撮合循環時間(分鐘)").map(num),
                         "halted": g("停止交易").ne("")})


def enrich_violations(viol: pd.DataFrame, raw: pd.DataFrame, cal: pd.DatetimeIndex, chtm: pd.DataFrame,
                      af: pd.DataFrame) -> pd.DataFrame:
    """Ordinary-day limit violations with the most likely explanation; nothing is excluded here."""
    v = viol.copy()
    r = raw[raw["close"].notna()][["symbol", "date"]].sort_values(["symbol", "date"])
    r["prev_priced"] = r.groupby("symbol")["date"].shift(1)
    v = v.merge(r, on=["symbol", "date"], how="left")
    ci = {d: i for i, d in enumerate(cal)}
    v["trading_days_since_prev_price"] = [ci.get(d, np.nan) - ci.get(p, np.nan) if pd.notna(p) else np.nan
                                          for d, p in zip(v["date"], v["prev_priced"])]
    v = v.merge(chtm, on=["symbol", "date"], how="left")
    ev_near = af.groupby("symbol")["effective_date"].apply(list).to_dict()
    v["official_event_within_5d"] = [any(abs((d - x).days) <= 5 for x in ev_near.get(s, [])) for s, d in
                                     zip(v["symbol"], v["date"])]
    v["move_vs_prev_close"] = v["close"] / v["prev_close"] - 1
    v["likely_cause"] = np.select(
        [v["instrument"] == "fund_like",
         v["trading_days_since_prev_price"] > 1,
         v["official_event_within_5d"],
         v[["altered", "split_session", "managed", "halted"]].fillna(False).any(axis=1)],
        ["fund_like_probably_no_price_limit", "previous_trading_day_without_price",
         "official_event_nearby", "tpex_altered_or_managed_trading"], default="unexplained")
    return v


def transfer_check(mask_path: Path, li: pd.DataFrame) -> pd.DataFrame:
    """Board transfers (delisted from one exchange and listed on the other on the same date): the quality mask's
    exchange must be the old exchange before and the new exchange from the transfer date."""
    d = li[li["event"] == "delisted"][["symbol", "exchange", "date"]]
    a = li[li["event"] == "listed"][["symbol", "exchange", "date"]]
    t = d.merge(a, on=["symbol", "date"], suffixes=("_from", "_to"))
    t = t[t["exchange_from"] != t["exchange_to"]]
    if not len(t) or not Path(mask_path).exists():
        return t.assign(symbol_days=0, wrong_exchange=0)
    m = pd.read_parquet(mask_path, columns=["symbol", "exchange", "date", "can_use_for_price_backtest"])
    m["date"] = pd.to_datetime(m["date"])
    rows = []
    for r in t.itertuples(index=False):
        x = m[m["symbol"] == r.symbol]
        exp = np.where(x["date"] < r.date, r.exchange_from, r.exchange_to)
        bad = x["exchange"].values != exp
        rows.append({"symbol": r.symbol, "transfer_date": r.date, "from": r.exchange_from, "to": r.exchange_to,
                     "symbol_days": int(len(x)), "wrong_exchange": int(bad.sum()),
                     "wrong_and_usable": int((bad & x["can_use_for_price_backtest"].values).sum())})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------------------- lake io
def load_lake(data_root: Path, start: str = "2023-01-01", log=print) -> tuple[pd.DataFrame, pd.DataFrame]:
    from ..config import load_config
    from ..paths import Layout
    from .reader import load_published
    lay = Layout.from_config(load_config(None, {"data_root": str(data_root)}))
    out = []
    for name in ("finlab_daily_raw", "finlab_daily_adjusted"):
        pv = load_published(name, lay.dataset_dirs[name])
        y0 = int(start[:4])
        keep = [r for r in pv.partitions.itertuples(index=False)
                if not re.search(r"year=(\d{4})", str(r.partition))
                or int(re.search(r"year=(\d{4})", str(r.partition)).group(1)) >= y0]
        frames = [pd.read_parquet(pv.data_path(r.relpath)) for r in keep]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        df["symbol"] = df["symbol"].astype(str)
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        df = df[df["date"] >= pd.Timestamp(start)]
        log(f"{name}@seq={pv.seq}: {len(df)} rows")
        out.append(df)
    return out[0], out[1]


def first_bars(data_root: Path, ev: pd.DataFrame, workers: int = 16, log=print) -> pd.DataFrame:
    """First stored 5m bar (and day high/low) of each event day, read from the published fubon_5m."""
    from concurrent.futures import ThreadPoolExecutor
    from ..config import load_config
    from ..paths import Layout
    from .reader import load_published
    lay = Layout.from_config(load_config(None, {"data_root": str(data_root)}))
    pv5 = load_published("fubon_5m", lay.dataset_dirs["fubon_5m"])
    parts = pv5.partitions.copy()
    parts["symbol"] = parts["partition"].str.extract(r"symbol=([^/]+)")[0]
    parts["year"] = parts["partition"].str.extract(r"year=(\d+)")[0].astype(int)
    need = ev.assign(year=ev["event_date"].dt.year)[["symbol", "year", "event_date"]]
    sel = parts.merge(need[["symbol", "year"]].drop_duplicates(), on=["symbol", "year"])
    want = need.groupby(["symbol", "year"])["event_date"].apply(set).to_dict()
    log(f"5m: {len(sel)} partitions for {len(need)} events (seq {pv5.seq})")

    def one(rec):
        df = pd.read_parquet(pv5.data_path(rec.relpath), columns=["bar_start", "open", "high", "low", "close"])
        df["date"] = df["bar_start"].dt.tz_localize(None).dt.normalize() if df["bar_start"].dt.tz is not None \
            else df["bar_start"].dt.normalize()
        df = df[df["date"].isin(want[(rec.symbol, rec.year)])].sort_values("bar_start")
        if not len(df):
            return None
        g = df.groupby("date")
        return pd.DataFrame({"symbol": rec.symbol, "event_date": g["open"].first().index, "bar5_first_open":
                             g["open"].first().values, "bar5_high": g["high"].max().values,
                             "bar5_low": g["low"].min().values, "bar5_n": g.size().values})
    with ThreadPoolExecutor(max_workers=workers) as pool:
        res = [r for r in pool.map(one, list(sel.itertuples(index=False))) if r is not None]
    return pd.concat(res, ignore_index=True) if res else pd.DataFrame(
        columns=["symbol", "event_date", "bar5_first_open", "bar5_high", "bar5_low", "bar5_n"])


# ------------------------------------------------------------------------------------------- run
def run(dataset_root: Path, out_root: Path, data_root: Path | None = None, with_5m: bool = False,
        mask_path: Path | None = None, trace_symbols: tuple = ("8101",), log=print) -> Path:
    out = Path(out_root) / f"corporate_events_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    out.mkdir(parents=True, exist_ok=False)
    st = Stages(out, "corporate_events", {"dataset_root": str(dataset_root), "data_root": str(data_root),
                                          "with_5m": with_5m, "mask_path": str(mask_path),
                                          "events_version": EVENTS_VERSION}, log=log)
    summary: dict = {}
    with st.stage("events") as rec:
        wide = load_wide(dataset_root)
        ev = build_events(wide)
        li = build_listings(wide)
        ev.to_parquet(out / "corporate_events.parquet", index=False)
        li.to_parquet(out / "listing_events.parquet", index=False)
        rec.update(events=len(ev), listings=len(li))
        summary["events_by_type"] = ev.groupby(["exchange", "event_type"]).size().rename("n").reset_index() \
            .to_dict("records")
        summary["listing_by_type"] = li.groupby(["exchange", "event"]).size().rename("n").reset_index() \
            .to_dict("records")
    with st.stage("limits") as rec:
        lc = check_limits(ev)
        ls = limit_summary(lc)
        ls.to_csv(out / "limit_rule_match.csv", index=False, encoding="utf-8-sig")
        mm = lc[lc["has_official_limits"] & ~lc["limit_rule_match"]]
        mm.to_csv(out / "limit_rule_mismatches.csv", index=False, encoding="utf-8-sig")
        summary["limit_rule_match_overall"] = float(lc.loc[lc["has_official_limits"], "limit_rule_match"].mean())
        summary["no_price_limit_events"] = int(lc["no_price_limit"].sum())
        summary["limit_rule_match"] = ls.to_dict("records")
        rec["mismatches"] = int(len(mm))
    if data_root:
        with st.stage("lake") as rec:
            raw, adj = load_lake(Path(data_root), log=log)
            pc = check_prev_close(ev, raw)
            jumps = adjusted_jumps(raw, adj)
            pc, jumps = match_jumps(pc, jumps)
            scope = pc["event_date"] >= raw["date"].min() + pd.Timedelta(days=7)
            pc.to_parquet(out / "events_checked.parquet", index=False)
            jumps.to_parquet(out / "finlab_adjustment_jumps.parquet", index=False)
            lk = check_listings(li, raw)
            lk.to_csv(out / "listing_checks.csv", index=False, encoding="utf-8-sig")
            inf = inferred_without_official(raw, li, raw["date"].max())
            nds, ndv = normal_day_limits(raw[raw["date"] >= pd.Timestamp("2023-05-01")], ev, li)
            nds.to_csv(out / "ordinary_day_limit_check.csv", index=False, encoding="utf-8-sig")
            ndv.to_csv(out / "ordinary_day_limit_violations.csv", index=False, encoding="utf-8-sig")
            summary["ordinary_day_limits"] = nds.to_dict("records")
            inf.to_csv(out / "inferred_delisted_vs_official.csv", index=False, encoding="utf-8-sig")
            s = pc[scope]
            summary["prev_close_match"] = s["prev_close_match"].value_counts().to_dict()
            summary["event_day_within_limits"] = s["event_day_within_limits"].value_counts().to_dict()
            summary["finlab_adjusted_on_event"] = s.groupby("event_type")["finlab_adjusted"].mean().round(4).to_dict()
            summary["finlab_jumps_with_official_event"] = jumps["official_event"].value_counts().to_dict()
            summary["listing_checks"] = lk[lk["date"] >= pd.Timestamp("2023-05-01")].groupby(
                ["exchange", "event", "status"]).size().rename("n").reset_index().to_dict("records")
            summary["inferred_delisted_without_official"] = inf[~inf["official_delisting"]].groupby(
                "code_class").size().to_dict()
            rec.update(events_in_scope=int(scope.sum()), jumps=len(jumps))
        with st.stage("b3_b5_v2") as rec:
            cal = trading_calendar(raw)
            af = adjustment_factors(ev, cal)
            af.to_parquet(out / "official_adjustment_factors.parquet", index=False)
            ej = explain_jumps(jumps, af)
            ej.to_csv(out / "finlab_jumps_explained.csv", index=False, encoding="utf-8-sig")
            summary["finlab_jumps_since_2023_05"] = ej["status"].value_counts().to_dict()
            summary["official_dates_shifted_to_next_trading_day"] = int(af["date_shifted"].sum())
            ch = chtm_flags(wide)
            ev2 = enrich_violations(ndv, raw, cal, ch, af)
            ev2.to_csv(out / "ordinary_day_limit_violations_explained.csv", index=False, encoding="utf-8-sig")
            summary["ordinary_day_violation_causes"] = ev2.groupby(["instrument", "likely_cause"]).size() \
                .rename("n").reset_index().to_dict("records")
            tr = official_rows(wide, list(trace_symbols))
            tr.to_csv(out / "trace_official_rows.csv", index=False, encoding="utf-8-sig")
            px = raw[raw["symbol"].isin(trace_symbols)].sort_values("date")
            px.to_csv(out / "trace_finlab_prices.csv", index=False, encoding="utf-8-sig")
            summary["trace_symbols"] = {"symbols": list(trace_symbols), "official_rows": int(len(tr))}
            if mask_path:
                tc = transfer_check(Path(mask_path), li)
                tc.to_csv(out / "board_transfer_mask_check.csv", index=False, encoding="utf-8-sig")
                summary["board_transfers"] = tc.to_dict("records")
            rec.update(adjustment_rows=len(af), violations=len(ev2))
        if with_5m:
            with st.stage("5m") as rec:
                fb = first_bars(Path(data_root), pc[scope], log=log)
                x = pc[scope].merge(fb, on=["symbol", "event_date"], how="left")
                lim = (x["limit_up"] > 0) & x["bar5_high"].notna()
                x["bar5_within_limits"] = np.where(~lim, "UNKNOWN", np.where(
                    (x["bar5_high"] <= x["limit_up"] + 1e-6) & (x["bar5_low"] >= x["limit_down"] - 1e-6),
                    "TRUE", "FALSE"))
                x.to_parquet(out / "events_checked_5m.parquet", index=False)
                summary["bar5_within_limits"] = x["bar5_within_limits"].value_counts().to_dict()
                rec["events_with_5m"] = int(x["bar5_high"].notna().sum())
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    st.finish(**{k: v for k, v in summary.items() if not isinstance(v, list)})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tw_lake.audit.corporate_events")
    ap.add_argument("--dataset-root", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--data-root", default="")
    ap.add_argument("--with-5m", action="store_true")
    ap.add_argument("--mask", default="", help="quality_mask_symbol_day.parquet for the board-transfer check")
    ap.add_argument("--trace", default="8101", help="comma-separated symbols to trace through all official rows")
    a = ap.parse_args(argv)
    out = run(Path(a.dataset_root), Path(a.out_dir), Path(a.data_root) if a.data_root else None, a.with_5m,
              Path(a.mask) if a.mask else None, tuple(x for x in a.trace.split(",") if x))
    print("written:", out)
    print((out / "summary.json").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
