"""research_ready flags per symbol/date - derived audit output only (never written back to sources).

ready_daily_only        FinLab traded that day and the 5m reconciliation shows no data_error candidate
                        for it (daily research itself does not need 5m; this flag only covers the 5m era)
ready_5m_normal_session 5m day is complete against the learned template, valid, normal regime,
                        partition integrity ok, reconciliation not data_error/suspect
ready_30m_resample      ready_5m_normal_session and every 30-minute bucket is fully covered
                        (13:30 closing-auction bar assigned to the 13:00-13:30 bucket, see docs/AUDIT.md)
Exploratory thresholds only; a symbol/date that is not ready is not necessarily wrong.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

READY_COLUMNS = ["symbol", "date", "quality_status", "recon_classification", "partition_ok",
                 "ready_daily_only", "ready_5m_normal_session", "ready_30m_resample", "not_ready_reason"]


def research_ready(day_audit: pd.DataFrame, recon: pd.DataFrame, bad_partitions: set[str]) -> pd.DataFrame:
    if len(day_audit) == 0:
        return pd.DataFrame(columns=READY_COLUMNS)
    d = day_audit[["symbol", "date", "quality_status", "partition", "finlab_volume_lots", "regime"]].copy()
    r = recon[["symbol", "date", "classification"]].rename(columns={"classification": "recon_classification"}) \
        if len(recon) else pd.DataFrame(columns=["symbol", "date", "recon_classification"])
    for t in (d, r):
        t["date"] = pd.to_datetime(t["date"])
        t["symbol"] = t["symbol"].astype(str)
    x = d.merge(r, on=["symbol", "date"], how="left")
    x["partition_ok"] = ~x["partition"].isin(bad_partitions)
    traded = x["finlab_volume_lots"].fillna(0) > 0
    x["ready_daily_only"] = traded & (x["recon_classification"] != "data_error_candidate")
    good_recon = x["recon_classification"].isin(["ok", "pending_verification"])
    x["ready_5m_normal_session"] = (x["quality_status"] == "complete") & good_recon & x["partition_ok"]
    x["ready_30m_resample"] = x["ready_5m_normal_session"]
    x["not_ready_reason"] = np.select(
        [~x["partition_ok"], x["quality_status"] != "complete", ~good_recon],
        ["partition_integrity_failure", "5m_" + x["quality_status"].astype(str), "recon_" + x["recon_classification"].astype(str)],
        default="")
    return x[READY_COLUMNS]


def readiness_summary(ready: pd.DataFrame) -> dict:
    if len(ready) == 0:
        return {"rows": 0}
    n = len(ready)
    out = {"denominator": "symbol-days in the audited 5m scope (FinLab trade day or 5m bars present)", "rows": int(n)}
    for c in ("ready_daily_only", "ready_5m_normal_session", "ready_30m_resample"):
        k = int(ready[c].sum())
        out[c] = {"count": k, "pct": round(100.0 * k / n, 2)}
    out["not_ready_reasons"] = {k: int(v) for k, v in ready.loc[ready["not_ready_reason"] != "", "not_ready_reason"]
                                .value_counts().items()}
    return out
