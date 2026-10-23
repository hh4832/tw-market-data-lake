"""Fubon 5m ingestion: repair-first planning, fetch, QA gate, immutable publish, manifest commit."""
from __future__ import annotations

import itertools
import json
import time
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from tqdm.auto import tqdm

from . import SCHEMA_VERSION
from .calendar import as_date
from .errors import (ApiBadRequest, ApiNotFound, ConfigError, IntegrityError, RequestBudgetExceeded,
                     RetriesExhausted)
from .fubon_source import FubonCaller, MarketClient, RateLimiter, RequestBudget, RetryPolicy, candle_params
from .jobs_finlab import FinlabResult, prepare_store
from .normalize import coerce_minute_schema, merge_with_conflicts, normalize_candles
from .qa import WindowQA, daily_from_minute, reconcile_daily, summarize, window_qa
from .run_context import RunContext
from .store import DatasetStore, StoreContext, upsert_partition_records
from .windows import (COMPLETE, COMPLETE_WITH_GAPS, CONFLICT, DATA_DONE, FAILED, PARTIAL, FetchTask,
                      classify_no_data, coverage_by_symbol, ensure_windows, normalize_windows, plan_tasks,
                      pre_request_skip)

CONFLICT_COLUMNS = ["detected_run_id", "symbol", "partition", "key", "detail"]


@dataclass
class TaskOutcome:
    task: FetchTask
    kind: str                    # data | no_data | failed | qa_fail | conflict | write_failed
    status: str | None = None    # for no_data
    df: pd.DataFrame | None = None
    error_class: str | None = None
    error_msg: str | None = None
    qa: WindowQA | None = None
    n_bars_window: int = 0
    requested: bool = True


@dataclass
class FubonJob:
    rc: RunContext
    sctx: StoreContext
    client: MarketClient | None
    fin: FinlabResult
    symbols: list[str] | None = None
    max_requests: int | None = None
    dry_run: bool = False
    retry_conflicts: bool = False
    retry_exhausted: bool = False
    sleep: callable = time.sleep
    monotonic: callable = time.monotonic
    # state
    request_log: list[dict] = field(default_factory=list)
    conflict_rows: list[dict] = field(default_factory=list)
    delta_rows: list[dict] = field(default_factory=list)
    minute_daily: list[pd.DataFrame] = field(default_factory=list)
    recon_ranges: dict = field(default_factory=dict)
    outcome_counts: dict = field(default_factory=dict)

    # ------------------------------------------------------------------------------------
    def run(self) -> dict:
        rc, cfg, f = self.rc, self.rc.cfg, self.rc.cfg.fubon
        cal = self.fin.calendar
        self.store = store = DatasetStore("fubon_5m", self.sctx, ["bar_start"], SCHEMA_VERSION,
                                          params=cfg.minute_params())
        prepare_store(rc, store)
        meta = store.meta()
        if meta.get("window_months") and int(meta["window_months"]) != int(f["window_months"]):
            raise ConfigError(f"fubon.window_months={f['window_months']} differs from published "
                              f"{meta['window_months']}; windows cannot be re-cut in place")
        tables = store.tables()
        self.parts = tables["partitions"]
        windows = normalize_windows(tables.get("coverage_by_window"))
        self.conflicts_log = tables.get("conflicts_log", pd.DataFrame(columns=CONFLICT_COLUMNS))

        # recovery: published file missing/corrupt -> drop record, mark that symbol-year FAILED
        if store.bad_partitions:
            for part in store.bad_partitions:
                sym, yr = part.split("/")[0].split("=")[1], int(part.split("/")[1].split("=")[1])
                m = (windows["symbol"] == sym) & (windows["year"] == yr)
                windows.loc[m, ["status", "error_class"]] = [FAILED, "published_file_missing_or_corrupt"]
                windows.loc[m, "covered_to"] = pd.NaT
            self.parts = self.parts[~self.parts["partition"].isin(store.bad_partitions)].reset_index(drop=True)

        targets = self.fin.targets
        if self.symbols:
            unknown = sorted(set(self.symbols) - set(targets["symbol"]))
            if unknown:
                rc.log.warning("symbols not in 5m target universe (skipped): %s", unknown)
            targets = targets[targets["symbol"].isin(self.symbols)]
        self.targets = targets.set_index("symbol")
        self.latest = latest = cal.latest_available_date(rc.clock, f["data_ready_hhmm"])
        windows, new_keys = ensure_windows(windows, list(targets["symbol"]), cfg.minute_start, latest,
                                           int(f["window_months"]))
        tasks, stats = plan_tasks(windows, new_keys, latest, int(f["max_window_attempts"]),
                                  self.retry_conflicts, self.retry_exhausted, list(targets["symbol"]))
        n_req = sum(1 for t in tasks if not pre_request_skip(cal, t.symbol, t.fetch_from, t.fetch_to))
        plan_df = pd.DataFrame([t.__dict__ for t in tasks])
        rc.add_table("plan", plan_df)
        est_min = n_req / max(1, int(f["historical_requests_per_min"]))
        summary = {"latest_available_date": latest.isoformat(), "targets": int(len(targets)),
                   "plan": stats, "tasks": len(tasks), "estimated_requests": n_req,
                   "estimated_minutes": round(est_min, 1)}
        rc.log.info("5m plan: %s", json.dumps(summary, ensure_ascii=False))
        if self.dry_run:
            summary["dry_run"] = True
            return summary

        self.windows = windows
        self.widx = {(s, w): i for i, (s, w) in enumerate(zip(windows["symbol"], windows["window_id"]))}
        self.caller = FubonCaller(
            RateLimiter(int(f["historical_requests_per_min"]), clock=self.monotonic, sleep=self.sleep),
            RetryPolicy(int(f["max_attempts_per_request"]), float(f["rate_limit_wait_sec"]),
                        float(f["backoff_base_sec"]), float(f["backoff_max_sec"])),
            RequestBudget(self.max_requests), sleep=self.sleep)

        stop_reason, dirty, since_commit = None, False, 0
        last_hb = self.monotonic()
        commit_every = int(cfg.manifest["commit_every_symbols"])
        repair = [t for t in tasks if t.reason == "repair"]
        later = [t for t in tasks if t.reason != "repair"]
        bar = tqdm(total=len(tasks), desc="5m windows", unit="win", mininterval=5)
        try:
            for phase, ptasks in (("repair", repair), ("update", later)):
                for sym, grp in itertools.groupby(ptasks, key=lambda t: t.symbol):
                    grp = list(grp)
                    outcomes, budget_hit = self._fetch_symbol(sym, grp)
                    if outcomes:
                        self._write_symbol(sym, outcomes)
                        dirty, since_commit = True, since_commit + 1
                    bar.update(len(outcomes))
                    if self.monotonic() - last_hb > 60:
                        last_hb = self.monotonic()
                        if self.sctx.lock is not None:
                            self.sctx.lock.heartbeat()
                        rc.log.info("heartbeat phase=%s symbol=%s requests=%d done=%d/%d", phase, sym,
                                    self.caller.budget.used, bar.n, len(tasks))
                    if since_commit >= commit_every:
                        self._commit()
                        dirty, since_commit = False, 0
                    if budget_hit:
                        stop_reason = "request_budget_reached"
                        break
                if stop_reason:
                    break
        finally:
            bar.close()
            if dirty:
                self._commit()

        summary.update({
            "stop_reason": stop_reason, "requests_used": self.caller.budget.used,
            "outcomes": self.outcome_counts, "conflicts": len(self.conflict_rows),
            "new_bars": int(sum(d["bars_added"] for d in self.delta_rows)),
            "seq": (store.pointer() or {}).get("seq"),
        })
        self._artifacts()
        summary["reconciliation"] = self.recon_summary
        return summary

    # ------------------------------------------------------------------------------------
    def _log_calls(self, t: FetchTask, n_rows: int | None):
        for r in self.caller.records:
            self.request_log.append({
                "run_id": self.rc.run_id, "symbol": t.symbol, "window_id": t.window_id,
                "from": t.fetch_from.isoformat(), "to": t.fetch_to.isoformat(), "attempt": r.attempt,
                "status": r.status, "error": r.error, "latency_ms": round(r.latency_ms, 1),
                "n_rows": n_rows if r.status == "ok" else None,
                "ts": self.rc.clock.now().isoformat(),
            })

    def _fetch_symbol(self, sym: str, tasks: list[FetchTask]) -> tuple[list[TaskOutcome], bool]:
        cfg, f, cal = self.rc.cfg, self.rc.cfg.fubon, self.fin.calendar
        itype = self.targets.loc[sym, "instrument_type"]
        unit = f["volume_unit_by_type"].get(itype, "lot")
        out: list[TaskOutcome] = []
        for t in tasks:
            skip = pre_request_skip(cal, sym, t.fetch_from, t.fetch_to)
            if skip:
                out.append(TaskOutcome(t, "no_data", status=skip, requested=False))
                continue
            params = candle_params(sym, t.fetch_from, t.fetch_to, f["timeframe"], list(f["fields"]))
            try:
                resp = self.caller.call(self.client.historical_candles, params)
                rows = (resp or {}).get("data") or [] if isinstance(resp, dict) else []
                self._log_calls(t, len(rows))
                if not rows:
                    cls = classify_no_data(cal, sym, t.fetch_from, t.fetch_to)
                    out.append(TaskOutcome(t, "no_data", status=cls) if cls else
                               TaskOutcome(t, "failed", error_class="unknown_empty",
                                           error_msg="empty response although FinLab shows trades"))
                    continue
                df = normalize_candles(rows, sym, unit, f["timeframe"], f["bar_label"])
                out.append(TaskOutcome(t, "data", df=df))
            except ApiNotFound as e:
                self._log_calls(t, None)
                cls = classify_no_data(cal, sym, t.fetch_from, t.fetch_to)
                out.append(TaskOutcome(t, "no_data", status=cls) if cls else
                           TaskOutcome(t, "failed", error_class="unknown_404", error_msg=str(e)[:300]))
            except ApiBadRequest as e:
                self._log_calls(t, None)
                out.append(TaskOutcome(t, "failed", error_class="bad_request", error_msg=str(e)[:300]))
            except RetriesExhausted as e:
                self._log_calls(t, None)
                out.append(TaskOutcome(t, "failed", error_class="retries_exhausted", error_msg=str(e)[:300]))
            except RequestBudgetExceeded:
                self._log_calls(t, None)
                return out, True
        return out, False

    def _write_symbol(self, sym: str, outcomes: list[TaskOutcome]) -> None:
        f, cal = self.rc.cfg.fubon, self.fin.calendar
        for o in outcomes:
            if o.kind == "data":
                o.qa = window_qa(o.df, cal, sym, o.task.fetch_from, o.task.fetch_to,
                                 f["session_start"], f["session_end"])
                if not o.qa.ok:
                    o.kind, o.error_class, o.error_msg = "qa_fail", "qa_hard_fail", ",".join(o.qa.hard_failures)
        data = [o for o in outcomes if o.kind == "data"]
        for year, grp in itertools.groupby(sorted(data, key=lambda o: o.task.year), key=lambda o: o.task.year):
            grp = list(grp)
            part = f"symbol={sym}/year={year:04d}"
            old = coerce_minute_schema(self.store.read_partition(part))
            new = coerce_minute_schema(pd.concat([o.df for o in grp], ignore_index=True))
            new = new.drop_duplicates(subset=["bar_start"], keep="last")
            merged, n_added, conflicts = merge_with_conflicts(old, new)
            merged = coerce_minute_schema(merged)
            if len(conflicts):
                for o in grp:
                    o.kind, o.error_class = "conflict", "conflict_with_published"
                for _, c in conflicts.iterrows():
                    self.conflict_rows.append({"detected_run_id": self.rc.run_id, "symbol": sym, "partition": part,
                                               "key": c["key"], "detail": json.dumps(
                                                   {k: (None if pd.isna(v) else v) for k, v in c.items() if k != "key"},
                                                   default=str)})
                continue
            if old is None or n_added > 0:
                try:
                    rec = self.store.publish_partition(merged, part)
                except IntegrityError as e:
                    for o in grp:
                        o.kind, o.error_class, o.error_msg = "write_failed", "write_verify_failed", str(e)[:300]
                    continue
                self.parts = upsert_partition_records(self.parts, [rec])
            bar_dates = merged["bar_start"].dt.date
            for o in grp:
                w = self.windows.loc[self.widx[(sym, o.task.window_id)]]
                ws, we = w["window_start"].date(), w["window_end"].date()
                o.n_bars_window = int(((bar_dates >= ws) & (bar_dates <= we)).sum())
            self.delta_rows.append({"symbol": sym, "partition": part, "bars_added": int(n_added),
                                    "min_bar": str(new["bar_start"].min()), "max_bar": str(new["bar_start"].max())})
            self.minute_daily.append(daily_from_minute(new))
            lo, hi = self.recon_ranges.get(sym, (None, None))
            a, b = min(o.task.fetch_from for o in grp), max(o.task.fetch_to for o in grp)
            self.recon_ranges[sym] = (a if lo is None else min(lo, a), b if hi is None else max(hi, b))
        for o in outcomes:
            self._apply(o)

    def _apply(self, o: TaskOutcome) -> None:
        t, w, i = o.task, self.windows, self.widx[(o.task.symbol, o.task.window_id)]
        row = w.loc[i]
        now = self.rc.clock.now().isoformat()
        key = o.kind if o.kind != "no_data" else f"no_data:{o.status}"
        self.outcome_counts[key] = self.outcome_counts.get(key, 0) + 1
        w.at[i, "last_attempt_run_id"] = self.rc.run_id
        w.at[i, "updated_at"] = now
        prev_status = row["status"]
        cov = as_date(row["covered_to"])
        if o.kind == "no_data":
            if prev_status in DATA_DONE and t.reason == "extend":
                pass  # nothing new in the extension range (e.g. no trades); data status unchanged
            else:
                w.at[i, "status"] = o.status
                w.at[i, "n_bars"] = 0
            w.at[i, "covered_to"] = pd.Timestamp(max(t.fetch_to, cov) if cov else t.fetch_to)
            w.at[i, "attempts"] = 0
            w.at[i, "error_class"] = None
            w.at[i, "error_msg"] = None
            w.at[i, "last_success_run_id"] = self.rc.run_id
            return
        if o.kind == "data":
            new_gaps = sorted(o.qa.gap_days)
            prior = json.loads(row["gap_days"]) if row["gap_days"] else []
            in_range = [g for g in prior if t.fetch_from.isoformat() <= g <= t.fetch_to.isoformat()]
            outside = [g for g in prior if g not in in_range] if t.reason == "extend" else []
            combined = sorted(set(outside) | set(new_gaps))
            limit = int(self.rc.cfg.fubon["gap_retry_limit"])
            if not combined:
                status, ga = COMPLETE, 0
            elif prev_status == COMPLETE_WITH_GAPS and set(new_gaps) <= set(prior):
                status, ga = COMPLETE_WITH_GAPS, int(row["gap_attempts"])
            else:
                ga = int(row["gap_attempts"]) + 1
                status = COMPLETE_WITH_GAPS if (set(new_gaps) == set(in_range) and ga >= limit) else PARTIAL
            w.at[i, "status"] = status
            w.at[i, "gap_days"] = json.dumps(combined) if combined else None
            w.at[i, "gap_attempts"] = ga
            w.at[i, "n_bars"] = o.n_bars_window
            w.at[i, "covered_to"] = pd.Timestamp(max(t.fetch_to, cov) if (cov and t.reason == "extend") else t.fetch_to)
            w.at[i, "error_class"] = None if status != PARTIAL else "source_gaps"
            w.at[i, "error_msg"] = None if status != PARTIAL else f"{len(new_gaps)} FinLab trade day(s) without bars"
            w.at[i, "attempts"] = 0 if status != PARTIAL else int(row["attempts"]) + 1
            if status != PARTIAL:
                w.at[i, "last_success_run_id"] = self.rc.run_id
            return
        status = {"failed": FAILED, "qa_fail": PARTIAL, "conflict": CONFLICT, "write_failed": PARTIAL}[o.kind]
        w.at[i, "status"] = status
        w.at[i, "error_class"] = o.error_class
        w.at[i, "error_msg"] = o.error_msg
        w.at[i, "attempts"] = int(row["attempts"]) + 1

    def _commit(self) -> None:
        f = self.rc.cfg.fubon
        cov_sym = coverage_by_symbol(self.windows, self.parts, self.fin.targets, self.latest,
                                     int(f["max_window_attempts"]))
        new_conf = pd.DataFrame(self.conflict_rows, columns=CONFLICT_COLUMNS)
        conf = pd.concat([self.conflicts_log, new_conf], ignore_index=True).drop_duplicates() \
            if len(self.conflicts_log) else new_conf
        meta = {"window_months": int(f["window_months"]), "minute_start": self.rc.cfg.minute_start.isoformat(),
                "latest_available_date": self.latest.isoformat(), "data_max_date": self.latest.isoformat(),
                "bar_label_assumption": f["bar_label"], "fields_requested": list(f["fields"]),
                "turnover_note": "null when Fubon minute responses omit turnover; never derived"}
        self.store.commit({"partitions": self.parts, "coverage_by_window": self.windows,
                           "coverage_by_symbol": cov_sym, "conflicts_log": conf}, meta)
        self.rc.log.info("manifest committed: fubon_5m seq=%s", (self.store.pointer() or {}).get("seq"))

    def _artifacts(self) -> None:
        rc = self.rc
        rc.add_table("request_log", pd.DataFrame(self.request_log))
        rc.add_table("delta_report_5m", pd.DataFrame(self.delta_rows))
        if self.conflict_rows:
            rc.add_table("conflicts_5m", pd.DataFrame(self.conflict_rows))
        md = pd.concat(self.minute_daily, ignore_index=True) if self.minute_daily else pd.DataFrame()
        md_by = dict(tuple(md.groupby("symbol"))) if len(md) else {}
        raw = self.fin.raw
        raw = raw[(raw["date"] >= pd.Timestamp(rc.cfg.minute_start)) & raw["symbol"].isin(list(self.recon_ranges))]
        raw_by = dict(tuple(raw[["date", "symbol", "high", "low", "volume_shares"]].groupby("symbol")))
        empty_raw = raw.iloc[0:0][["date", "symbol", "high", "low", "volume_shares"]]
        recs = []
        q = rc.cfg.qa
        for sym, (a, b) in sorted(self.recon_ranges.items()):
            part = md_by.get(sym, md.iloc[0:0] if len(md) else md)
            recs.append(reconcile_daily(part, raw_by.get(sym, empty_raw), self.fin.calendar, [sym], a, b,
                                        (float(q["volume_ratio_ok_low"]), float(q["volume_ratio_ok_high"])),
                                        float(q["price_tolerance"])))
        rec = pd.concat(recs, ignore_index=True) if recs else pd.DataFrame()
        rc.add_table("qa_reconciliation_5m", rec)
        self.recon_summary = summarize(rec)
