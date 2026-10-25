"""FinLab ingestion jobs: raw daily (append-only, immutable versions), adjusted snapshot,
security_categories, disposal/attention history, point-in-time universe."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

import pandas as pd

from . import SCHEMA_VERSION
from .calendar import TradingCalendar
from .errors import RequiredDatasetMissing
from .finlab_source import (FinlabSource, discover_datasets, load_adjusted_daily, load_raw_daily, plain,
                            to_storable)
from .normalize import merge_with_conflicts
from .run_context import RunContext
from .snapshots import publish_snapshot
from .store import DatasetStore, StoreContext, upsert_partition_records
from .universe import (UNIVERSE_V2_PROVENANCE, classify, minute_targets, symbol_master_pit, universe_daily,
                       universe_daily_v2)

RAW_VALUE_COLS = ["open", "high", "low", "close", "volume_shares", "turnover"]
CHANGES_COLUMNS = ["detected_run_id", "partition", "key", "field", "published_value", "source_value"]


@dataclass
class FinlabResult:
    raw: pd.DataFrame
    calendar: TradingCalendar
    master: pd.DataFrame
    targets: pd.DataFrame
    summaries: dict = field(default_factory=dict)
    disposal_discovery: dict = field(default_factory=dict)


def prepare_store(rc: RunContext, store: DatasetStore) -> DatasetStore:
    """Parameter/schema check + recovery scan (quarantine staging, verify published files)."""
    store.check_params()
    rep = store.recovery_scan(deep=bool(rc.info.get("deep_verify")), apply=not rc.info.get("dry_run"))
    rc.info.setdefault("recovery", []).append(rep)
    issues = rep["missing_files"] + rep["corrupt_files"]
    if issues or rep["staging_quarantined"] or rep["pointer_missing_with_versions"]:
        rc.log.warning("recovery %s: %s", store.name, json.dumps(
            {k: (v if not isinstance(v, list) else v[:10]) for k, v in rep.items()}, ensure_ascii=False))
    return store


def _year_parts(df: pd.DataFrame, date_col: str = "date") -> dict[str, pd.DataFrame]:
    years = pd.to_datetime(df[date_col]).dt.year
    return {f"year={y:04d}": g.reset_index(drop=True) for y, g in df.groupby(years)}


def ingest_raw_daily(rc: RunContext, sctx: StoreContext, raw: pd.DataFrame) -> tuple[DatasetStore, dict]:
    """Append new rows; never overwrite published values. Changed history -> changes_log."""
    store = DatasetStore("finlab_daily_raw", sctx, key_cols=["date", "symbol"], schema_version=SCHEMA_VERSION,
                         params={"source": "finlab", "fields": rc.cfg.finlab["raw_fields"], "adjusted": False})
    prepare_store(rc, store)
    tables = store.tables()
    parts = tables["partitions"]
    changes_old = tables.get("changes_log", pd.DataFrame(columns=CHANGES_COLUMNS))
    new_records, change_rows, delta = [], [], []
    removed_total = 0
    for part, new in sorted(_year_parts(raw).items()):
        old = store.read_partition(part)
        merged, n_added, conflicts = merge_with_conflicts(old, new, ["date", "symbol"], RAW_VALUE_COLS)
        if old is not None:
            removed = len(old.merge(new[["date", "symbol"]], on=["date", "symbol"], how="left", indicator=True)
                          .query("_merge == 'left_only'"))
            removed_total += removed
        if len(conflicts):
            for _, c in conflicts.iterrows():
                for col in RAW_VALUE_COLS:
                    oc, nc = f"old_{col}", f"new_{col}"
                    if oc in conflicts.columns and (pd.notna(c.get(oc)) or pd.notna(c.get(nc))):
                        change_rows.append({"detected_run_id": rc.run_id, "partition": part, "key": c["key"],
                                            "field": col, "published_value": c.get(oc), "source_value": c.get(nc)})
        if old is None or n_added > 0:
            rec = store.publish_partition(merged, part)
            new_records.append(rec)
            delta.append({"dataset": "finlab_daily_raw", "partition": part, "rows_added": int(n_added),
                          "min_date": str(new["date"].min().date()), "max_date": str(new["date"].max().date())})
    changes = pd.DataFrame(change_rows, columns=CHANGES_COLUMNS)
    for c in ("published_value", "source_value"):
        changes[c] = changes[c].astype("float64")
    summary = {"partitions_written": len(new_records), "rows_added": int(sum(d["rows_added"] for d in delta)),
               "source_changes_detected": int(len(changes)), "rows_missing_in_source_now": int(removed_total),
               "data_max_date": str(raw["date"].max().date())}
    if new_records or len(changes):
        all_changes = pd.concat([changes_old, changes], ignore_index=True) if len(changes_old) else changes
        ptr = store.commit({"partitions": upsert_partition_records(parts, new_records), "changes_log": all_changes},
                           {"data_max_date": summary["data_max_date"], "asof": rc.clock.now().date().isoformat()})
        summary["seq"] = ptr["seq"]
    else:
        summary["seq"] = (store.pointer() or {}).get("seq")
    summary["delta"] = delta
    return store, summary


def run_finlab_readonly(rc: RunContext, src: FinlabSource) -> FinlabResult:
    """Dry-run / smoke: compute calendar, master and targets in memory; write nothing."""
    cfg = rc.cfg
    asof = rc.clock.now().date().isoformat()
    with rc.stage("finlab (read-only): load raw daily"):
        raw = load_raw_daily(src, cfg.finlab["raw_fields"])
        cal = TradingCalendar(raw)
    with rc.stage("finlab (read-only): classification + targets"):
        cats = plain(src.get("security_categories"))
        classification = classify(cats, sorted(raw["symbol"].unique()), asof)
        master = symbol_master_pit(raw, classification, cal.trading_days, int(cfg.finlab["delist_gap_trading_days"]))
        targets = minute_targets(master, cfg.minute_start)
    return FinlabResult(raw=raw, calendar=cal, master=master, targets=targets,
                        summaries={"minute_targets": {"n": int(len(targets))}})


def run_finlab(rc: RunContext, sctx: StoreContext, src: FinlabSource) -> FinlabResult:
    cfg = rc.cfg
    asof = rc.clock.now().date().isoformat()
    summaries: dict = {}

    with rc.stage("finlab: load raw daily"):
        raw_src = load_raw_daily(src, cfg.finlab["raw_fields"])
        rc.log.info("raw daily rows=%d symbols=%d dates=%s..%s", len(raw_src), raw_src["symbol"].nunique(),
                    raw_src["date"].min().date(), raw_src["date"].max().date())

    with rc.stage("finlab: publish raw daily (append-only)"):
        raw_store, summaries["finlab_daily_raw"] = ingest_raw_daily(rc, sctx, raw_src)
        raw = raw_store.read_all()            # published research version (old values win)
        raw["symbol"] = raw["symbol"].astype(str)
        cal = TradingCalendar(raw)

    with rc.stage("finlab: adjusted snapshot (content-hash dedupe)"):
        adj = load_adjusted_daily(src)
        adj_store = DatasetStore("finlab_daily_adjusted", sctx, ["date", "symbol"], SCHEMA_VERSION,
                                 params={"source": "finlab", "method": "TWMarket.get_trading_price(adj=True)"})
        prepare_store(rc, adj_store)
        summaries["finlab_daily_adjusted"] = publish_snapshot(
            adj_store, _year_parts(adj), asof, {"data_max_date": str(adj["date"].max().date())})

    with rc.stage("finlab: security_categories snapshot"):
        cats = plain(src.get("security_categories"))
        cat_store = DatasetStore("security_categories", sctx, ["stock_id"], SCHEMA_VERSION,
                                 params={"source": "finlab", "dataset": "security_categories"})
        prepare_store(rc, cat_store)
        summaries["security_categories"] = publish_snapshot(
            cat_store, {"snapshot": to_storable(cats)}, asof, {"note": "current snapshot, not point-in-time"},
            all_columns_key=True)

    with rc.stage("finlab: disposal / attention history"):
        found = discover_datasets(src, cfg.finlab["disposal_dataset_candidates"])
        disc = {k: {kk: vv for kk, vv in v.items() if kk != "frame"} for k, v in found.items()}
        ok = {k: v["frame"] for k, v in found.items() if v.get("ok")}
        rc.log.info("disposal/attention candidates: %s", json.dumps({k: v.get("ok") for k, v in disc.items()}))
        if not ok and cfg.finlab.get("require_disposal_dataset", True):
            raise RequiredDatasetMissing(
                "no disposal/attention dataset could be loaded from FinLab; candidates tried: "
                + ", ".join(cfg.finlab["disposal_dataset_candidates"]) + ". Fix the names in config (see smoke report).")
        if ok:
            disp_store = DatasetStore("disposal_attention", sctx, ["_dummy"], SCHEMA_VERSION,
                                      params={"source": "finlab", "kind": "disposal_attention_history"})
            prepare_store(rc, disp_store)
            parts = {f"dataset={re.sub(r'[^A-Za-z0-9_]+', '_', k)}": to_storable(v) for k, v in ok.items()}
            summaries["disposal_attention"] = publish_snapshot(
                disp_store, parts, asof, {"discovery": disc}, all_columns_key=True)

    with rc.stage("universe: point-in-time universe + symbol master"):
        classification = classify(cats, sorted(raw["symbol"].unique()), asof)
        master = symbol_master_pit(raw, classification, cal.trading_days,
                                   int(cfg.finlab["delist_gap_trading_days"]))
        uni = universe_daily(raw, master, cal.trading_days)
        uni_store = DatasetStore("universe_daily", sctx, ["date", "symbol"], SCHEMA_VERSION,
                                 params={"rules": "v1", "classification": "security_categories_current+code_rule"})
        prepare_store(rc, uni_store)
        summaries["universe_daily"] = publish_snapshot(
            uni_store, _year_parts(uni), asof, {"classification_asof": asof},
            extra_tables={"symbol_master": master})
        if "universe_daily_v2" in cfg.raw["datasets"]:      # B5: additive derived dataset, v1 untouched
            uni2_store = DatasetStore("universe_daily_v2", sctx, ["date", "symbol"], SCHEMA_VERSION,
                                      params={"rules": "v2", "derived_from": "universe_daily v1",
                                              "classification": "security_categories_current+code_rule"})
            prepare_store(rc, uni2_store)
            summaries["universe_daily_v2"] = publish_snapshot(
                uni2_store, _year_parts(universe_daily_v2(uni)), asof,
                {"classification_asof": asof, "field_provenance": UNIVERSE_V2_PROVENANCE,
                 "pre_open_selection_columns": ["has_price_prev_day", "has_trade_prev_day",
                                                "listed_trading_days_before"]})
        targets = minute_targets(master, cfg.minute_start)
        summaries["minute_targets"] = {
            "n": int(len(targets)), "by_type": targets["instrument_type"].value_counts().to_dict(),
            "delisted_inferred": int((targets["listing_status_latest"] == "delisted_inferred").sum()),
            "code_rule_inferred": int((targets["classification_source"] == "code_rule_inferred").sum()),
        }

    return FinlabResult(raw=raw, calendar=cal, master=master, targets=targets, summaries=summaries,
                        disposal_discovery=disc)
