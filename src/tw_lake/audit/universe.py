"""A4 - universe / listing / classification provenance, ETF rule spot checks, excluded bars.

Nothing in the published universe is claimed to be point-in-time unless it is: classification
comes from a *current* security_categories snapshot; listing spans are inferred ex post from
the first/last FinLab price date; has_trade is only known after that day's close.
"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd

FIELD_PROVENANCE = [
    # column, provenance, known_at, usable_for_same_day_pre_open_selection, note
    ("has_price", "observed_at_date", "after close of date (FinLab daily publish)", False, ""),
    ("has_trade", "observed_at_date", "after close of date", False, "use previous trading day's value for pre-open selection"),
    ("tradable_status", "observed_at_date", "after close of date", False, "derived from has_trade"),
    ("listing_status", "reconstructed_ex_post", "end of dataset (uses last price date)", False,
     "inferred from first/last FinLab price date; not an official listing/delisting date"),
    ("listing_status_latest", "reconstructed_ex_post", "audit time", False, "delisted_inferred = last price older than N trading days"),
    ("first_price_date", "reconstructed_ex_post", "known only from history", False, "proxy for listing date; not official"),
    ("last_price_date", "reconstructed_ex_post", "known only after the fact", False, "survivorship-sensitive"),
    ("instrument_type", "current_snapshot_applied_to_history", "classification_asof", False,
     "security_categories snapshot + '00'-prefix rule; not PIT"),
    ("market", "current_snapshot_applied_to_history", "classification_asof", False, "board transfers (TPEx->TWSE) not reflected"),
    ("classification_source", "metadata", "classification_asof", True, ""),
    ("trading_regime (disposal)", "known_asof", "announcement date (disposal_information.date)", True,
     "effective from 處置開始時間; known after announcement"),
]


def provenance_table() -> pd.DataFrame:
    return pd.DataFrame(FIELD_PROVENANCE, columns=["column", "provenance", "known_at",
                                                   "usable_for_same_day_pre_open_selection", "note"])


def etf_subtype(sym: str) -> str:
    s = str(sym)
    if not s.startswith("00"):
        return ""
    suffix = s[-1] if s[-1].isalpha() else ""
    return {"L": "leveraged", "R": "inverse", "B": "bond", "U": "futures_or_commodity", "A": "active",
            "K": "suffix_K", "T": "suffix_T"}.get(suffix, "plain" if not suffix else f"suffix_{suffix}")


def classification_issues(master: pd.DataFrame, categories: pd.DataFrame | None) -> pd.DataFrame:
    rows = []
    m = master.copy()
    m["symbol"] = m["symbol"].astype(str)
    cats = None
    if categories is not None and len(categories) and "stock_id" in categories.columns:
        cats = categories.assign(stock_id=categories["stock_id"].astype(str).str.strip()).drop_duplicates(
            "stock_id", keep="last").set_index("stock_id")
    n_not_pit = int((m.get("classification_is_pit", pd.Series(False, index=m.index)) == False).sum())  # noqa: E712
    if n_not_pit:
        rows.append({"symbol": "*", "issue": "classification_not_pit", "severity": "warning",
                     "detail": f"{n_not_pit} of {len(m)} symbols classified from a current snapshot "
                               f"(asof={m.get('classification_asof', pd.Series(['?'])).iloc[0]}) applied to history"})
    for r in m.itertuples(index=False):
        sym = r.symbol
        sub = etf_subtype(sym)
        cat = cats.loc[sym, "category"] if cats is not None and sym in cats.index else None
        mkt = cats.loc[sym, "market"] if cats is not None and sym in cats.index else None
        if sym.startswith("00") and r.instrument_type != "etf":
            rows.append({"symbol": sym, "issue": "00_prefix_not_etf", "severity": "warning",
                         "detail": f"instrument_type={r.instrument_type}"})
        if sym.startswith("00") and cat is not None and "etf" not in str(cat).lower() and str(mkt) != "etf":
            rows.append({"symbol": sym, "issue": "etf_by_code_rule_overrides_category", "severity": "info",
                         "detail": f"category={cat} market={mkt} subtype={sub}"})
        if getattr(r, "classification_source", "") == "code_rule_inferred":
            rows.append({"symbol": sym, "issue": "classified_by_code_rule_only", "severity": "info",
                         "detail": f"not in current security_categories; type={r.instrument_type}"})
        if getattr(r, "listing_status_latest", "") == "delisted_inferred":
            rows.append({"symbol": sym, "issue": "delisting_inferred_from_price_gap", "severity": "info",
                         "detail": f"last_price_date={getattr(r, 'last_price_date', '')}; reconstructed_ex_post"})
        if sub and sub != "plain":
            rows.append({"symbol": sym, "issue": f"etf_subtype_{sub}", "severity": "info",
                         "detail": "special ETF (leverage/inverse/bond/futures/active): different price/volume regime"})
    return pd.DataFrame(rows, columns=["symbol", "issue", "severity", "detail"])


def load_excluded_summaries(runs_dir: Path) -> pd.DataFrame:
    frames = []
    for f in sorted(glob.glob(str(runs_dir / "*" / "excluded_outside_listing_5m.*"))):
        if f.endswith(".csv") and Path(f).with_suffix(".parquet").exists():
            continue
        try:
            df = pd.read_parquet(f) if f.endswith(".parquet") else pd.read_csv(f, dtype={"symbol": str})
        except Exception:  # noqa: BLE001
            continue
        frames.append(df.assign(run_id=Path(f).parent.name))
    if not frames:
        return pd.DataFrame()
    x = pd.concat(frames, ignore_index=True)
    x["symbol"] = x["symbol"].astype(str)
    return x


def excluded_bars_audit(excl: pd.DataFrame, trading_days: list) -> pd.DataFrame:
    """Per symbol/window exclusion summary with distance to the listing boundary (trading days)."""
    if len(excl) == 0:
        return pd.DataFrame()
    td = pd.DatetimeIndex(pd.to_datetime(sorted(trading_days)))
    x = excl.copy()
    first_ex = pd.to_datetime(x["first_excluded"].astype(str).str[:10])
    last_ex = pd.to_datetime(x["last_excluded"].astype(str).str[:10])
    fpd = pd.to_datetime(x["first_price_date"], errors="coerce")
    lpd = pd.to_datetime(x["last_price_date"], errors="coerce")
    pos = lambda s: td.searchsorted(s.values)  # noqa: E731
    x["side"] = np.where(last_ex < fpd, "before_listing", np.where(first_ex > lpd, "after_last_price", "overlap"))
    x["trading_days_last_excluded_to_first_price"] = np.where(x["side"] == "before_listing", pos(fpd) - pos(last_ex), np.nan)
    x["adjacent_to_boundary_le5d"] = x["trading_days_last_excluded_to_first_price"].le(5)
    x["session_end_hhmm"] = x["last_excluded"].astype(str).str[11:16]
    x["emerging_session_evidence"] = x["session_end_hhmm"] > "13:30"
    x["risk"] = np.select(
        [x["side"] == "overlap", x["side"] == "after_last_price",
         x["adjacent_to_boundary_le5d"] & ~x["emerging_session_evidence"]],
        ["excluded_inside_listing_span", "excluded_after_last_price_review", "adjacent_without_emerging_evidence"],
        default="consistent_with_pre_listing_emerging")
    x["note"] = "per-bar excluded rows were not persisted by ingestion; only per-window summaries exist"
    return x


def rejected_window_bars(quarantine_dir: Path) -> pd.DataFrame:
    """Raw bars kept in quarantine: QA-rejected windows and (since B7) every excluded bar."""
    frames = []
    files = glob.glob(str(quarantine_dir / "*" / "fubon_5m_qa_rejected" / "*.parquet")) + \
        glob.glob(str(quarantine_dir / "*" / "fubon_5m_excluded_outside_listing" / "*.parquet"))   # B7
    for f in sorted(files):
        try:
            df = pd.read_parquet(f)
        except Exception:  # noqa: BLE001
            continue
        frames.append(df.assign(source_file=Path(f).name, quarantine_run=Path(f).parents[1].name))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def listing_boundary_days(rejected: pd.DataFrame, master: pd.DataFrame, trading_days: list, k: int = 5) -> pd.DataFrame:
    """For quarantined raw windows: per-date bar stats in the k trading days around first_price_date."""
    if len(rejected) == 0:
        return pd.DataFrame()
    td = pd.DatetimeIndex(pd.to_datetime(sorted(trading_days)))
    fpd = master.set_index(master["symbol"].astype(str))["first_price_date"]
    x = rejected.copy()
    x["date"] = pd.to_datetime(x["bar_start"]).dt.tz_localize(None).dt.normalize()
    x["hhmm"] = pd.to_datetime(x["bar_start"]).dt.strftime("%H:%M")
    rows = []
    for (sym, d), g in x.groupby(["symbol", "date"]):
        if sym not in fpd.index or pd.isna(fpd[sym]):
            continue
        dist = int(td.searchsorted(d) - td.searchsorted(pd.Timestamp(fpd[sym])))
        if abs(dist) > k:
            continue
        rows.append({"symbol": sym, "date": d, "trading_days_from_first_price_date": dist, "bars": len(g),
                     "first_bar": g["hhmm"].min(), "last_bar": g["hhmm"].max(),
                     "volume_raw_sum": float(g["volume_raw"].sum()),
                     "regime_guess": "emerging_board_session" if g["hhmm"].max() > "13:30" else "listed_session"})
    return pd.DataFrame(rows)
