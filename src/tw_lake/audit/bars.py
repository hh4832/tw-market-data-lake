"""A2 - 5m completeness & timestamp semantics, one row per symbol/date.

Expected timestamps are *learned*, not assumed: for each trading date the union of bar times of
the template symbols (liquid stocks/ETFs, e.g. 2330/0050/006208) defines the normal-session
template for that date. A date without template bars has an unknown expectation
(``regime_unknown``); call-auction (disposal) regimes have an unknown expected count. Fubon
omits 5-minute intervals without trades, so a missing timestamp on an illiquid symbol cannot be
told apart from a source gap using 5m data alone; such rows are ``intraday_missing`` with
``missing_cause = unverified_no_trade_or_source_gap``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SESSION_OPEN, SESSION_CLOSE, CLOSE_AUCTION = "09:00", "13:30", "13:30"
LIQUIDITY_BUCKETS = [(-np.inf, 50, "lt50_lots"), (50, 500, "50_500_lots"), (500, 5000, "500_5000_lots"),
                     (5000, np.inf, "ge5000_lots")]
MAX_LIST = 30

DAY_COLUMNS = [
    "symbol", "date", "expected_policy", "regime", "auction_interval_min", "actual_bar_count",
    "unique_bar_ts", "duplicate_bars", "expected_bar_count", "missing_count", "missing_timestamps",
    "unexpected_count", "unexpected_timestamps", "intraday_completeness_ratio", "has_close_auction_bar",
    "close_auction_flat", "ohlc_invalid_bars", "nonpositive_price_bars", "negative_volume_bars",
    "zero_volume_bars", "not_trading_day", "finlab_volume_lots", "liquidity_bucket", "regime_unknown",
    "missing_cause", "quality_status", "partition", "manifest_seq",
]


def liquidity_bucket(lots: pd.Series) -> pd.Series:
    out = pd.Series("no_finlab_trade", index=lots.index, dtype=object)
    for lo, hi, name in LIQUIDITY_BUCKETS:
        out[(lots > max(lo, 0)) & (lots <= hi)] = name
    return out


def _hhmm(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%H:%M")


def build_templates(frames: list[pd.DataFrame]) -> dict:
    """date -> frozenset of HH:MM present in ANY template symbol that date; plus per-date agreement."""
    if not frames:
        return {}
    t = pd.concat(frames, ignore_index=True)
    t = t.assign(date=t["bar_start"].dt.tz_localize(None).dt.normalize(), hhmm=_hhmm(t["bar_start"]))
    out = {}
    for d, g in t.groupby("date"):
        out[d] = frozenset(g["hhmm"])
    return out


def template_report(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=["date", "symbols", "union_bars", "min_bars", "max_bars", "agree"])
    t = pd.concat(frames, ignore_index=True)
    t = t.assign(date=t["bar_start"].dt.tz_localize(None).dt.normalize())
    c = t.groupby(["date", "symbol"]).size().unstack()
    u = t.assign(hhmm=_hhmm(t["bar_start"])).groupby("date")["hhmm"].nunique()
    return pd.DataFrame({"symbols": c.notna().sum(axis=1), "union_bars": u, "min_bars": c.min(axis=1),
                         "max_bars": c.max(axis=1), "agree": c.min(axis=1) == c.max(axis=1)}).reset_index()


def _audit_symbol_partition_reference(df: pd.DataFrame, symbol: str, finlab_day: pd.DataFrame, templates: dict,
                           regime: pd.DataFrame, trading_days: set, partition: str, seq) -> pd.DataFrame:
    """df: one 5m partition (one symbol, one year). finlab_day: FinLab raw rows of this symbol
    (date, volume_shares, ...) for the same year. regime: trading_regime_daily rows of the symbol."""
    x = df.copy()
    x["date"] = x["bar_start"].dt.tz_localize(None).dt.normalize()
    x["hhmm"] = _hhmm(x["bar_start"])
    minute = x["bar_start"].dt.minute
    off_grid = (minute % 5 != 0) | (x["bar_start"].dt.second != 0)
    out_session = (x["hhmm"] < SESSION_OPEN) | (x["hhmm"] > SESSION_CLOSE)
    o, h, lo, c = x["open"], x["high"], x["low"], x["close"]
    x["_ohlc_bad"] = (lo > o) | (lo > c) | (h < o) | (h < c) | (lo > h)
    x["_nonpos"] = (o <= 0) | (h <= 0) | (lo <= 0) | (c <= 0)
    x["_negvol"] = x["volume_raw"] < 0
    x["_zerovol"] = x["volume_raw"] == 0
    x["_dup"] = x.duplicated("bar_start", keep="first")
    x["_flat"] = (o == h) & (h == lo) & (lo == c)

    fd = finlab_day.copy()
    fd["date"] = pd.to_datetime(fd["date"]).dt.normalize()
    fd = fd[fd["volume_shares"].fillna(0) > 0]
    days = sorted(set(x["date"]) | set(fd["date"]))
    reg = regime.set_index("date") if len(regime) else pd.DataFrame()
    vol_by_day = fd.set_index("date")["volume_shares"] if len(fd) else pd.Series(dtype=float)
    rows = []
    groups = dict(tuple(x.groupby("date"))) if len(x) else {}
    for d in days:
        g = groups.get(d)
        n = 0 if g is None else len(g)
        times = set() if g is None else set(g["hhmm"])
        tmpl = templates.get(d)
        regime_name, interval = "normal", np.nan
        if len(reg) and d in reg.index:
            rr = reg.loc[d]
            regime_name, interval = rr["regime"], rr["auction_interval_min"]
        if regime_name != "normal":
            policy, expected = "call_auction_unknown_count", np.nan
        elif tmpl is None:
            policy, expected = "no_template_for_date", np.nan
        else:
            policy, expected = "normal_session_template", float(len(tmpl))
        missing = sorted(tmpl - times) if (tmpl is not None and regime_name == "normal" and n) else []
        unexpected = set()
        if g is not None:
            unexpected = set(g.loc[off_grid.loc[g.index] | out_session.loc[g.index], "hhmm"])
            if tmpl is not None and regime_name == "normal":
                unexpected |= times - tmpl
        lots = vol_by_day.get(d, np.nan) / 1000.0 if len(vol_by_day) else np.nan
        not_td = d not in trading_days
        rows.append({
            "symbol": symbol, "date": d, "expected_policy": policy, "regime": regime_name,
            "auction_interval_min": interval, "actual_bar_count": n,
            "unique_bar_ts": 0 if g is None else int(g["bar_start"].nunique()),
            "duplicate_bars": 0 if g is None else int(g["_dup"].sum()),
            "expected_bar_count": expected, "missing_count": len(missing),
            "missing_timestamps": "|".join(missing[:MAX_LIST]) + ("|..." if len(missing) > MAX_LIST else ""),
            "unexpected_count": len(unexpected),
            "unexpected_timestamps": "|".join(sorted(unexpected)[:MAX_LIST]),
            "intraday_completeness_ratio": (len(times & tmpl) / len(tmpl)) if (policy == "normal_session_template" and n) else np.nan,
            "has_close_auction_bar": bool(CLOSE_AUCTION in times),
            "close_auction_flat": bool(g is not None and g.loc[g["hhmm"] == CLOSE_AUCTION, "_flat"].all()
                                       and CLOSE_AUCTION in times),
            "ohlc_invalid_bars": 0 if g is None else int(g["_ohlc_bad"].sum()),
            "nonpositive_price_bars": 0 if g is None else int(g["_nonpos"].sum()),
            "negative_volume_bars": 0 if g is None else int(g["_negvol"].sum()),
            "zero_volume_bars": 0 if g is None else int(g["_zerovol"].sum()),
            "not_trading_day": bool(not_td and n > 0),
            "finlab_volume_lots": lots, "regime_unknown": policy == "no_template_for_date",
            "partition": partition, "manifest_seq": seq,
        })
    out = pd.DataFrame(rows, columns=[c for c in DAY_COLUMNS if c not in ("liquidity_bucket", "missing_cause", "quality_status")])
    return classify_days(out)


_TMPL_CACHE: dict = {}


def _template_frame(templates: dict) -> tuple[pd.DataFrame, pd.Series]:
    key = id(templates)
    hit = _TMPL_CACHE.get(key)
    if hit is None or hit[0] is not templates:
        rows = [(d, t) for d, ts in templates.items() for t in ts]
        tf = pd.DataFrame(rows, columns=["date", "hhmm"])
        tf["date"] = pd.to_datetime(tf["date"])
        hit = (templates, tf, tf.groupby("date").size())
        _TMPL_CACHE.clear()
        _TMPL_CACHE[key] = hit
    return hit[1], hit[2]


def _join_list(s: pd.Series) -> str:
    v = sorted(set(s))
    return "|".join(v[:MAX_LIST]) + ("|..." if len(v) > MAX_LIST else "")


def audit_symbol_partition(df: pd.DataFrame, symbol: str, finlab_day: pd.DataFrame, templates: dict,
                           regime: pd.DataFrame, trading_days: set, partition: str, seq) -> pd.DataFrame:
    """Vectorized version of _audit_symbol_partition_reference (same output, ~10x faster).
    df: one 5m partition (one symbol, one year). finlab_day: FinLab raw rows of this symbol for the
    same year (date, volume_shares). regime: trading_regime_daily rows of the symbol."""
    x = df[["bar_start", "open", "high", "low", "close", "volume_raw"]].copy()
    x["date"] = x["bar_start"].dt.tz_localize(None).dt.normalize()
    x["hhmm"] = _hhmm(x["bar_start"])
    o, h, lo, c = x["open"], x["high"], x["low"], x["close"]
    x["_ohlc_bad"] = (lo > o) | (lo > c) | (h < o) | (h < c) | (lo > h)
    x["_nonpos"] = (o <= 0) | (h <= 0) | (lo <= 0) | (c <= 0)
    x["_negvol"] = x["volume_raw"] < 0
    x["_zerovol"] = x["volume_raw"] == 0
    x["_dup"] = x.duplicated("bar_start", keep="first")
    x["_flat"] = (o == h) & (h == lo) & (lo == c)
    x["_offgrid"] = (x["bar_start"].dt.minute % 5 != 0) | (x["bar_start"].dt.second != 0) | \
                    (x["hhmm"] < SESSION_OPEN) | (x["hhmm"] > SESSION_CLOSE)

    fd = finlab_day.copy()
    fd["date"] = pd.to_datetime(fd["date"]).dt.normalize()
    fd = fd[fd["volume_shares"].fillna(0) > 0]
    days = sorted(set(x["date"]) | set(fd["date"]))
    if not days:
        return pd.DataFrame(columns=DAY_COLUMNS)
    t = pd.DataFrame({"date": pd.to_datetime(days)})
    if len(regime):
        rg = regime[["date", "regime", "auction_interval_min"]].copy()
        rg["date"] = pd.to_datetime(rg["date"])
        t = t.merge(rg.drop_duplicates("date"), on="date", how="left")
    else:
        t["regime"], t["auction_interval_min"] = np.nan, np.nan
    t["regime"] = t["regime"].fillna("normal")
    tf, tn = _template_frame(templates)
    t["tmpl_n"] = t["date"].map(tn)
    has_tmpl = t["tmpl_n"].notna()
    normal = t["regime"] == "normal"
    t["expected_policy"] = np.select([~normal, ~has_tmpl], ["call_auction_unknown_count", "no_template_for_date"],
                                     default="normal_session_template")
    t["expected_bar_count"] = np.where(t["expected_policy"] == "normal_session_template", t["tmpl_n"], np.nan)

    g = x.groupby("date")
    agg = pd.DataFrame({
        "actual_bar_count": g.size(), "unique_bar_ts": g["bar_start"].nunique(),
        "duplicate_bars": g["_dup"].sum(), "ohlc_invalid_bars": g["_ohlc_bad"].sum(),
        "nonpositive_price_bars": g["_nonpos"].sum(), "negative_volume_bars": g["_negvol"].sum(),
        "zero_volume_bars": g["_zerovol"].sum(),
    }) if len(x) else pd.DataFrame(columns=["actual_bar_count", "unique_bar_ts", "duplicate_bars", "ohlc_invalid_bars",
                                            "nonpositive_price_bars", "negative_volume_bars", "zero_volume_bars"])
    t = t.merge(agg, left_on="date", right_index=True, how="left")
    for col in ("actual_bar_count", "unique_bar_ts", "duplicate_bars", "ohlc_invalid_bars", "nonpositive_price_bars",
                "negative_volume_bars", "zero_volume_bars"):
        t[col] = t[col].fillna(0).astype("int64")
    ca = x[x["hhmm"] == CLOSE_AUCTION].groupby("date")["_flat"].all() if len(x) else pd.Series(dtype=bool)
    t["has_close_auction_bar"] = t["date"].isin(ca.index)
    t["close_auction_flat"] = t["date"].map(ca).fillna(False).astype(bool)

    check_days = t.loc[(t["expected_policy"] == "normal_session_template"), "date"]
    xt = x.merge(tf.assign(_in=True), on=["date", "hhmm"], how="left")
    xt["_in"] = xt["_in"].fillna(False).astype(bool)
    unexp = xt["_offgrid"] | (xt["date"].isin(check_days) & ~xt["_in"])
    ue = xt[unexp].groupby("date")["hhmm"]
    t["unexpected_count"] = t["date"].map(ue.nunique()).fillna(0).astype("int64")
    t["unexpected_timestamps"] = t["date"].map(ue.agg(lambda s: "|".join(sorted(set(s))[:MAX_LIST]))).fillna("")

    with_bars = t.loc[(t["expected_policy"] == "normal_session_template") & (t["actual_bar_count"] > 0), "date"]
    tm = tf[tf["date"].isin(with_bars)].merge(x[["date", "hhmm"]].drop_duplicates().assign(_have=True),
                                              on=["date", "hhmm"], how="left")
    miss = tm[tm["_have"].isna()].groupby("date")["hhmm"]
    t["missing_count"] = t["date"].map(miss.size()).fillna(0).astype("int64")
    t["missing_timestamps"] = t["date"].map(miss.agg(_join_list)).fillna("")
    have_ratio = (t["tmpl_n"] - t["missing_count"]) / t["tmpl_n"]
    t["intraday_completeness_ratio"] = np.where(t["date"].isin(with_bars), have_ratio, np.nan)

    vol = fd.groupby("date")["volume_shares"].sum() / 1000.0 if len(fd) else pd.Series(dtype=float)
    t["finlab_volume_lots"] = t["date"].map(vol)
    td = pd.DatetimeIndex(pd.to_datetime(list(trading_days))) if trading_days else pd.DatetimeIndex([])
    t["not_trading_day"] = ~t["date"].isin(td) & (t["actual_bar_count"] > 0)
    t["regime_unknown"] = t["expected_policy"] == "no_template_for_date"
    t["symbol"], t["partition"], t["manifest_seq"] = symbol, partition, seq
    cols = [c for c in DAY_COLUMNS if c not in ("liquidity_bucket", "missing_cause", "quality_status")]
    return classify_days(t[cols])


def classify_days(t: pd.DataFrame) -> pd.DataFrame:
    if len(t) == 0:
        return pd.DataFrame(columns=DAY_COLUMNS)
    t = t.copy()
    t["liquidity_bucket"] = liquidity_bucket(t["finlab_volume_lots"])
    finlab_traded = t["finlab_volume_lots"].fillna(0) > 0
    invalid = (t["duplicate_bars"] > 0) | (t["ohlc_invalid_bars"] > 0) | (t["nonpositive_price_bars"] > 0) | \
              (t["negative_volume_bars"] > 0) | (t["unexpected_count"] > 0) | t["not_trading_day"]
    no_bars = t["actual_bar_count"] == 0
    conds = [
        invalid,
        no_bars & finlab_traded,
        ~no_bars & ~finlab_traded,
        t["regime"] != "normal",
        t["expected_policy"] == "no_template_for_date",
        t["missing_count"] > 0,
    ]
    names = ["invalid", "no_bars_finlab_traded", "bars_without_finlab_trade", "regime_call_auction_unverified",
             "regime_unknown", "intraday_missing"]
    t["quality_status"] = np.select(conds, names, default="complete")
    t["missing_cause"] = np.where(t["quality_status"] == "intraday_missing", "unverified_no_trade_or_source_gap",
                                  np.where(t["quality_status"] == "no_bars_finlab_traded", "see_gap_cases", ""))
    return t[DAY_COLUMNS]


def daily_from_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild daily OHLCV from 5m bars of one symbol (close = last bar = 13:30 closing auction when present)."""
    if len(df) == 0:
        return pd.DataFrame(columns=["symbol", "date", "m_open", "m_high", "m_low", "m_close", "m_volume_raw",
                                     "m_bars", "volume_unit", "m_first_bar", "m_last_bar"])
    x = df.sort_values("bar_start").assign(date=lambda z: z["bar_start"].dt.tz_localize(None).dt.normalize())
    g = x.groupby(["symbol", "date"], observed=True)
    return pd.DataFrame({
        "m_open": g["open"].first(), "m_high": g["high"].max(), "m_low": g["low"].min(),
        "m_close": g["close"].last(), "m_volume_raw": g["volume_raw"].sum(), "m_bars": g.size(),
        "volume_unit": g["volume_unit"].first(), "m_first_bar": g["bar_start"].first().dt.strftime("%H:%M"),
        "m_last_bar": g["bar_start"].last().dt.strftime("%H:%M"),
    }).reset_index()
