"""research_ready flags per symbol/date - derived audit output only (never written back to sources).

Strict flags (v0.1, unchanged)
ready_daily_only        FinLab traded that day and the 5m reconciliation shows no data_error candidate
                        for it (daily research itself does not need 5m; this flag only covers the 5m era)
ready_5m_normal_session 5m day is complete against the learned template (every template timestamp
                        present), valid, normal regime, partition integrity ok, reconciliation ok or
                        pending_verification
ready_30m_resample      same as ready_5m_normal_session (13:30 closing-auction bar goes to the
                        13:00-13:30 bucket, see docs/AUDIT.md)

No-trade-tolerant flags (v0.2, exploratory)
Fubon omits 5-minute intervals without trades, so the strict flag rejects almost every day of an
illiquid symbol even when nothing is lost. The tolerant flag accepts an ``intraday_missing`` day
when the evidence says the missing intervals carried no trades:
  * reconciliation is ``ok`` - 5m volume within the band of FinLab daily volume, open/close equal,
    and the 5m high/low equal the daily high/low (a missing bar that contained the day's extreme
    would show up as ``daily_range_wider_than_5m`` -> pending_verification -> rejected);
  * the 13:30 closing-auction bar is present;
  * partition integrity ok and normal regime (intraday_missing already implies both valid bars and
    a normal-session template for the date);
  * missing_count <= the allowance of the day's liquidity bucket. A missing interval on a liquid
    symbol is implausible as "no trade", so liquid buckets allow few or none (None = no cap; the
    volume/range evidence alone decides).
``ready_5m_no_trade_tolerant`` is a superset of ``ready_5m_normal_session``. A 30m bucket whose
5m bars are all absent is then a no-trade bucket (carry the previous close, volume 0), so
``ready_30m_no_trade_tolerant`` equals the 5m tolerant flag.
The allowances are exploratory defaults; audit_5m_missing_by_liquidity is the calibration table.
v0.2.1 defaults (calibrated on audit_20261025_163404_e53478a): ge5000=0, 500-5000=10, 50-500 and <50
uncapped, 13:30 auction bar not required (recon ok already requires last 5m close == daily close).
Evidence: in the 500-5000 and 50-500 buckets the share of recon-ok days stays at 90-97% from 0 to
31+ missing bars, i.e. more missing bars do not mean more lost volume.
A symbol/date that is not ready is not necessarily wrong, and a ready one is not certified.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

READINESS_VERSION = "0.2.1"

# liquidity bucket (bars.LIQUIDITY_BUCKETS, by FinLab daily lots) -> max missing template timestamps
DEFAULT_MISSING_ALLOWANCE: dict[str, int | None] = {
    "ge5000_lots": 0,
    "500_5000_lots": 10,
    "50_500_lots": None,
    "lt50_lots": None,
}
BUCKET_ORDER = ["ge5000_lots", "500_5000_lots", "50_500_lots", "lt50_lots", "no_finlab_trade"]

READY_COLUMNS = ["symbol", "date", "quality_status", "recon_classification", "partition_ok",
                 "liquidity_bucket", "missing_count", "has_close_auction_bar",
                 "ready_daily_only", "ready_5m_normal_session", "ready_30m_resample", "not_ready_reason",
                 "ready_5m_no_trade_tolerant", "ready_30m_no_trade_tolerant", "tolerant_not_ready_reason"]

DAY_INPUT_COLUMNS = ["symbol", "date", "quality_status", "partition", "finlab_volume_lots", "regime",
                     "liquidity_bucket", "missing_count", "has_close_auction_bar"]

HIST_EDGES = [0, 1, 2, 3, 4, 6, 11, 16, 21, 31, 55]
HIST_LABELS = ["0", "1", "2", "3", "4-5", "6-10", "11-15", "16-20", "21-30", "31-54"]


def parse_allowance(spec: str | None) -> dict[str, int | None]:
    """'ge5000_lots=0,500_5000_lots=3,50_500_lots=15,lt50_lots=none' -> dict (unknown keys rejected)."""
    out = dict(DEFAULT_MISSING_ALLOWANCE)
    if not spec:
        return out
    for item in spec.split(","):
        k, _, v = item.strip().partition("=")
        if k not in out:
            raise ValueError(f"unknown liquidity bucket {k!r}; expected one of {sorted(out)}")
        out[k] = None if v.strip().lower() in ("none", "inf", "") else int(v)
    return out


def _bucket_from_lots(lots: pd.Series) -> pd.Series:
    from .bars import liquidity_bucket
    return liquidity_bucket(lots)


def research_ready(day_audit: pd.DataFrame, recon: pd.DataFrame, bad_partitions: set[str],
                   allowance: dict[str, int | None] | None = None,
                   require_close_auction: bool = False) -> pd.DataFrame:
    """require_close_auction=False: an illiquid day without a 13:30 auction trade is accepted when the
    reconciliation is ok (which already requires the last 5m close == FinLab close)."""
    if len(day_audit) == 0:
        return pd.DataFrame(columns=READY_COLUMNS)
    allowance = dict(DEFAULT_MISSING_ALLOWANCE if allowance is None else allowance)
    d = day_audit[[c for c in DAY_INPUT_COLUMNS if c in day_audit.columns]].copy()
    if "liquidity_bucket" not in d:
        d["liquidity_bucket"] = _bucket_from_lots(d["finlab_volume_lots"])
    if "missing_count" not in d:
        d["missing_count"] = 0
    if "has_close_auction_bar" not in d:
        d["has_close_auction_bar"] = False
    r = recon[["symbol", "date", "classification"]].rename(columns={"classification": "recon_classification"}) \
        if len(recon) else pd.DataFrame(columns=["symbol", "date", "recon_classification"])
    for t in (d, r):
        t["date"] = pd.to_datetime(t["date"])
        t["symbol"] = t["symbol"].astype(str)
    x = d.merge(r, on=["symbol", "date"], how="left")
    x["partition_ok"] = ~x["partition"].isin(bad_partitions)
    x["missing_count"] = pd.to_numeric(x["missing_count"], errors="coerce").fillna(0).astype("int64")
    x["has_close_auction_bar"] = x["has_close_auction_bar"].fillna(False).astype(bool)
    traded = x["finlab_volume_lots"].fillna(0) > 0
    x["ready_daily_only"] = traded & (x["recon_classification"] != "data_error_candidate")

    # ---- strict (v0.1) ---------------------------------------------------------------------
    good_recon = x["recon_classification"].isin(["ok", "pending_verification"])
    x["ready_5m_normal_session"] = (x["quality_status"] == "complete") & good_recon & x["partition_ok"]
    x["ready_30m_resample"] = x["ready_5m_normal_session"]
    x["not_ready_reason"] = np.select(
        [~x["partition_ok"], x["quality_status"] != "complete", ~good_recon],
        ["partition_integrity_failure", "5m_" + x["quality_status"].astype(str),
         "recon_" + x["recon_classification"].astype(str)],
        default="")

    # ---- no-trade tolerant (v0.2) ----------------------------------------------------------
    cap = x["liquidity_bucket"].map(lambda b: allowance.get(b, 0))
    cap_num = pd.to_numeric(cap, errors="coerce")                       # None -> NaN = no cap
    within = cap_num.isna() | (x["missing_count"] <= cap_num.fillna(0))
    missing_day = x["quality_status"] == "intraday_missing"
    recon_ok = x["recon_classification"] == "ok"
    close_ok = x["has_close_auction_bar"] | (not require_close_auction)
    tol_missing = missing_day & x["partition_ok"] & recon_ok & close_ok & within
    x["ready_5m_no_trade_tolerant"] = x["ready_5m_normal_session"] | tol_missing
    x["ready_30m_no_trade_tolerant"] = x["ready_5m_no_trade_tolerant"]
    x["tolerant_not_ready_reason"] = np.select(
        [x["ready_5m_no_trade_tolerant"],
         ~x["partition_ok"],
         ~missing_day & (x["quality_status"] != "complete"),
         ~recon_ok,                                                     # incl. complete days with suspect recon
         ~close_ok,
         ~within],
        ["", "partition_integrity_failure", "5m_" + x["quality_status"].astype(str),
         "recon_" + x["recon_classification"].astype(str),
         "no_close_auction_bar", "missing_over_allowance_" + x["liquidity_bucket"].astype(str)],
        default="other")
    return x[READY_COLUMNS]


def missing_by_liquidity(ready: pd.DataFrame) -> pd.DataFrame:
    """Calibration table: per liquidity bucket, the distribution of missing_count on days that have
    5m bars and a normal-session template (complete + intraday_missing), with how many of them have
    a clean reconciliation."""
    cols = ["liquidity_bucket", "missing_bin", "days", "recon_ok_days", "with_close_auction_days", "share_of_bucket"]
    if len(ready) == 0:
        return pd.DataFrame(columns=cols)
    x = ready[ready["quality_status"].isin(["complete", "intraday_missing"])].copy()
    if len(x) == 0:
        return pd.DataFrame(columns=cols)
    x["missing_bin"] = pd.cut(x["missing_count"].clip(upper=54), HIST_EDGES, right=False, labels=HIST_LABELS)
    x["recon_ok"] = x["recon_classification"] == "ok"
    g = x.groupby(["liquidity_bucket", "missing_bin"], observed=True)
    t = pd.DataFrame({"days": g.size(), "recon_ok_days": g["recon_ok"].sum(),
                      "with_close_auction_days": g["has_close_auction_bar"].sum()}).reset_index()
    t["share_of_bucket"] = (t["days"] / t.groupby("liquidity_bucket")["days"].transform("sum")).round(4)
    t["missing_bin"] = t["missing_bin"].astype(str)
    order = {b: i for i, b in enumerate(BUCKET_ORDER)}
    t = t.sort_values(["liquidity_bucket", "missing_bin"],
                      key=lambda s: s.map(order) if s.name == "liquidity_bucket" else s.map(HIST_LABELS.index))
    for c in ("days", "recon_ok_days", "with_close_auction_days"):
        t[c] = t[c].astype("int64")
    return t[cols].reset_index(drop=True)


def _pct(k, n):
    return round(100.0 * k / n, 2) if n else None


def readiness_summary(ready: pd.DataFrame, allowance: dict[str, int | None] | None = None,
                      require_close_auction: bool = False) -> dict:
    if len(ready) == 0:
        return {"rows": 0}
    n = len(ready)
    out = {"readiness_version": READINESS_VERSION,
           "denominator": "symbol-days in the audited 5m scope (FinLab trade day or 5m bars present)",
           "rows": int(n)}
    for c in ("ready_daily_only", "ready_5m_normal_session", "ready_30m_resample",
              "ready_5m_no_trade_tolerant", "ready_30m_no_trade_tolerant"):
        if c in ready:
            k = int(ready[c].sum())
            out[c] = {"count": k, "pct": _pct(k, n)}
    out["not_ready_reasons"] = {k: int(v) for k, v in ready.loc[ready["not_ready_reason"] != "", "not_ready_reason"]
                                .value_counts().items()}
    if "ready_5m_no_trade_tolerant" in ready:
        out["missing_allowance"] = dict(DEFAULT_MISSING_ALLOWANCE if allowance is None else allowance)
        out["require_close_auction"] = require_close_auction
        out["tolerant_not_ready_reasons"] = {
            k: int(v) for k, v in ready.loc[ready["tolerant_not_ready_reason"] != "", "tolerant_not_ready_reason"]
            .value_counts().items()}
        by = {}
        for b, g in ready.groupby("liquidity_bucket"):
            m = g["quality_status"] == "intraday_missing"
            by[b] = {"rows": int(len(g)),
                     "strict_pct": _pct(int(g["ready_5m_normal_session"].sum()), len(g)),
                     "tolerant_pct": _pct(int(g["ready_5m_no_trade_tolerant"].sum()), len(g)),
                     "intraday_missing_days": int(m.sum()),
                     "missing_count_p50": float(g.loc[m, "missing_count"].median()) if m.any() else None,
                     "missing_count_p90": float(g.loc[m, "missing_count"].quantile(0.9)) if m.any() else None}
        out["by_liquidity_bucket"] = {b: by[b] for b in BUCKET_ORDER if b in by}
        per_sym = ready.groupby("symbol")["ready_5m_no_trade_tolerant"].mean()
        out["symbols_by_tolerant_ready_share"] = {
            k: int(v) for k, v in pd.cut(per_sym, [-0.01, 0.1, 0.5, 0.9, 1.0],
                                         labels=["le10pct", "10_50pct", "50_90pct", "gt90pct"])
            .value_counts().sort_index().items()}
    return out
