"""Phase A audit orchestration. Read-only on DATA_ROOT; all outputs go to a new audit folder."""
from __future__ import annotations

import random
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..clock import TAIPEI
from ..config import Config
from ..paths import Layout
from ..run_context import git_sha, package_versions
from . import AUDIT_VERSION
from . import bars as A2
from . import gaps as A5
from . import integrity as A1
from . import recon as A3
from . import universe as A4
from .readiness import missing_by_liquidity, readiness_summary, research_ready
from .reader import PublishedVersion, control_files, diff_fingerprints, fingerprint, load_published
from .regime import matrix_agreement, trading_regime_daily
from .report import write_reports

EMPTY_5M = pd.DataFrame({"symbol": pd.Series(dtype=object),
                         "bar_start": pd.Series(dtype="datetime64[ns, Asia/Taipei]"),
                         **{c: pd.Series(dtype=float) for c in ("open", "high", "low", "close", "volume_raw")},
                         "volume_unit": pd.Series(dtype=object)})


class AuditWriteRefused(RuntimeError):
    pass


@dataclass
class AuditOptions:
    out_root: Path
    symbols: list[str] | None = None
    max_partitions: int | None = None
    deep_verify: bool = False
    sha_sample: int = 20
    template_symbols: tuple[str, ...] = ("2330", "0050", "006208")
    volume_ratio_band: tuple[float, float] = (0.95, 1.02)
    missing_allowance: dict | None = None      # readiness v0.2; None = readiness.DEFAULT_MISSING_ALLOWANCE
    publish_dir: Path | None = None
    seed: int = 20261025
    progress: bool = True


@dataclass
class AuditContext:
    cfg: Config
    layout: Layout
    opts: AuditOptions
    audit_id: str
    out_dir: Path
    started: datetime
    log: list[str] = field(default_factory=list)
    timings: dict = field(default_factory=dict)

    def note(self, msg: str) -> None:
        line = f"{datetime.now(TAIPEI).isoformat(timespec='seconds')} {msg}"
        self.log.append(line)
        print(line, flush=True)


def _guard_out_dir(layout: Layout, out_dir: Path) -> None:
    """Refuse to write into any source dataset folder, protected folder or ingestion system folder."""
    o = out_dir.resolve()
    forbidden = [*layout.dataset_dirs.values(), *layout.protected, layout.locks, layout.runs, layout.staging,
                 layout.quarantine, layout.smoke]
    for f in forbidden:
        f = Path(f).resolve()
        if o == f or f in o.parents:
            raise AuditWriteRefused(f"audit output must not be inside {f}")
    if o.exists() and any(o.iterdir()):
        raise AuditWriteRefused(f"audit output folder already exists and is not empty: {o}")


def _read(pv: PublishedVersion, rec):
    return pd.read_parquet(pv.data_path(rec.relpath))


def _iter(items, enabled, desc):
    if not enabled:
        return items
    try:
        from tqdm.auto import tqdm
        return tqdm(items, desc=desc, mininterval=30)   # one line per ~30 s when streamed
    except Exception:  # noqa: BLE001
        return items


def run_audit(cfg: Config, opts: AuditOptions) -> Path:
    layout = Layout.from_config(cfg)
    started = datetime.now(TAIPEI)
    audit_id = f"audit_{started:%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    out_dir = Path(opts.out_root) / audit_id
    _guard_out_dir(layout, out_dir)
    if opts.publish_dir is not None:
        _guard_out_dir(layout, Path(opts.publish_dir) / audit_id)
    if not layout.data_root.exists():
        raise FileNotFoundError(f"DATA_ROOT not found (is Drive mounted?): {layout.data_root}")
    out_dir.mkdir(parents=True)
    ctx = AuditContext(cfg, layout, opts, audit_id, out_dir, started)
    t0 = time.monotonic()

    # ---- source fingerprint (before) -------------------------------------------------
    pvs = {name: load_published(name, root) for name, root in layout.dataset_dirs.items()}
    hash_set, stat_set = control_files({n: pv.version_dir for n, pv in pvs.items()}, layout.dataset_dirs,
                                       layout.system_dir)
    fp_before = {**fingerprint(hash_set, hash_small_bytes=50_000_000), **fingerprint(stat_set)}
    ctx.note(f"fingerprinted {len(hash_set)} manifest/pointer files (sha256) + {len(stat_set)} system entries "
             f"(stat); protected folders are not read  [{time.monotonic() - t0:.0f}s]")
    sources = {n: {"seq": pv.seq, "version_dir": (pv.pointer or {}).get("version_dir"),
                   "run_id": (pv.pointer or {}).get("run_id"), "committed_at": (pv.pointer or {}).get("committed_at")}
               for n, pv in pvs.items()}

    # ---- A1 stat-level integrity (parallel; 5m scope follows --symbols) -------------------
    rng = random.Random(opts.seed)
    integ: list[dict] = []
    data_fp_before: dict = {}
    for name, pv in pvs.items():
        integ += A1.version_checks(pv)
        parts = list(pv.partitions["partition"]) if len(pv.partitions) else []
        only = None
        if name == "fubon_5m" and opts.symbols:
            only = {p for p in parts if p.split("/")[0].split("=")[1] in set(opts.symbols)}
            parts = sorted(only)
        sample = set(rng.sample(parts, min(opts.sha_sample, len(parts)))) if parts else set()
        integ += A1.stat_checks(pv, deep=opts.deep_verify, sha_sample=sample, only_partitions=only,
                                fp_out=data_fp_before)
        ctx.note(f"A1 {name}: {len(parts)} partitions checked  [{time.monotonic() - t0:.0f}s]")
    data_paths = list(data_fp_before)

    # ---- FinLab raw daily (5m era) -----------------------------------------------------
    raw_pv = pvs["finlab_daily_raw"]
    y0 = cfg.minute_start.year
    raw_frames, schema = [], {}
    for rec in raw_pv.partitions.itertuples(index=False):
        if int(rec.partition.split("=")[1]) < y0:
            continue
        try:
            df = _read(raw_pv, rec)
        except Exception as e:  # noqa: BLE001
            integ.append(A1.unreadable(raw_pv, rec, e))
            continue
        integ += A1.read_checks(raw_pv, rec, df, schema)
        raw_frames.append(df)
    raw = pd.concat(raw_frames, ignore_index=True) if raw_frames else pd.DataFrame(
        columns=["date", "symbol", "open", "high", "low", "close", "volume_shares", "turnover"])
    raw["symbol"] = raw["symbol"].astype(str)
    raw["date"] = pd.to_datetime(raw["date"]).dt.normalize()
    raw = raw[raw["date"] >= pd.Timestamp(cfg.minute_start)]
    trading_days = sorted(raw.loc[raw["volume_shares"].fillna(0) > 0, "date"].unique())
    td_set = set(pd.to_datetime(trading_days))
    raw_by_sym = dict(tuple(raw.groupby("symbol")))
    ctx.note(f"FinLab raw loaded: {len(raw)} rows {len(trading_days)} trading days  [{time.monotonic() - t0:.0f}s]")

    calendar_source = "finlab_raw_daily"
    # ---- disposal -> trading_regime_daily ------------------------------------------------
    disp_pv = pvs["disposal_attention"]
    disp = {}
    for rec in disp_pv.partitions.itertuples(index=False):
        try:
            disp[rec.partition.split("=", 1)[1]] = _read(disp_pv, rec)
        except Exception as e:  # noqa: BLE001
            integ.append(A1.unreadable(disp_pv, rec, e))
    regime = trading_regime_daily(disp.get("disposal_information"), trading_days)
    regime_checks = [matrix_agreement(regime, disp.get(k), k) for k in ("etl_disposal_stock_filter",)]
    regime_by_sym = dict(tuple(regime.groupby("symbol"))) if len(regime) else {}
    ctx.note(f"trading_regime_daily: {len(regime)} symbol-days under disposal regimes")

    # ---- A2/A3 5m pass ----------------------------------------------------------------------
    pv5 = pvs["fubon_5m"]
    parts5 = pv5.partitions.copy()
    parts5["symbol"] = parts5["partition"].str.extract(r"symbol=([^/]+)")[0]
    parts5["year"] = parts5["partition"].str.extract(r"year=(\d+)")[0].astype(int)
    bad = {r["partition"] for r in integ if r["dataset"] == "fubon_5m" and r["status"] == "fail" and r["partition"]}
    tmpl_frames = []
    for rec in parts5[parts5["symbol"].isin(opts.template_symbols)].itertuples(index=False):
        if rec.partition in bad:
            continue
        try:
            tmpl_frames.append(_read(pv5, rec))
        except Exception:  # noqa: BLE001
            pass
    templates = A2.build_templates(tmpl_frames)
    tmpl_report = A2.template_report(tmpl_frames)
    if not trading_days and templates:          # FinLab raw unreadable: fall back, and say so
        trading_days = sorted(templates)
        td_set = set(trading_days)
        calendar_source = "5m_template_dates_fallback (FinLab raw unavailable)"
        regime = trading_regime_daily(disp.get("disposal_information"), trading_days)
        regime_by_sym = dict(tuple(regime.groupby("symbol"))) if len(regime) else {}
    ctx.note(f"templates learned for {len(templates)} dates from {opts.template_symbols}  [{time.monotonic() - t0:.0f}s]")

    sel = parts5 if not opts.symbols else parts5[parts5["symbol"].isin(opts.symbols)]
    if opts.max_partitions:
        sel = sel.head(opts.max_partitions)
    day_frames, md_frames, schema5 = [], [], {}

    def load(rec):
        if rec.partition in bad:
            return rec, None, None
        try:
            return rec, _read(pv5, rec), None
        except Exception as e:  # noqa: BLE001
            return rec, None, e

    from concurrent.futures import ThreadPoolExecutor
    recs = list(sel.itertuples(index=False))
    bar = _iter(range(len(recs)), opts.progress, "5m partitions")
    it = iter(bar)
    with ThreadPoolExecutor(max_workers=16) as ex:            # bounded prefetch: Drive reads are latency bound
        for i in range(0, len(recs), 64):
            for rec, df, err in ex.map(load, recs[i:i + 64]):
                next(it, None)
                sym, yr = rec.symbol, rec.year
                fd = raw_by_sym.get(sym, raw.iloc[0:0])
                fd = fd[fd["date"].dt.year == yr]
                if err is not None:
                    integ.append(A1.unreadable(pv5, rec, err))
                    bad.add(rec.partition)
                if df is None:
                    df = EMPTY_5M
                else:
                    integ += A1.read_checks(pv5, rec, df, schema5)
                day_frames.append(A2.audit_symbol_partition(df, sym, fd, templates,
                                                            regime_by_sym.get(sym, regime.iloc[0:0]),
                                                            td_set, rec.partition, pv5.seq))
                if len(df):
                    md_frames.append(A2.daily_from_bars(df))
    day_audit = pd.concat(day_frames, ignore_index=True) if day_frames else pd.DataFrame(columns=A2.DAY_COLUMNS)
    minute_daily = pd.concat(md_frames, ignore_index=True) if md_frames else A2.daily_from_bars(EMPTY_5M)
    audited_syms = set(sel["symbol"])
    rec_df = A3.reconcile(minute_daily, raw[raw["symbol"].isin(audited_syms)], td_set, opts.volume_ratio_band)
    ctx.note(f"A2/A3 done: {len(sel)} partitions, {len(day_audit)} symbol-days  [{time.monotonic() - t0:.0f}s]")

    # ---- A4 universe ----------------------------------------------------------------------
    uni = pvs["universe_daily"]
    master = uni.tables.get("symbol_master", pd.DataFrame())
    cats = None
    cat_pv = pvs["security_categories"]
    for rec in cat_pv.partitions.itertuples(index=False):
        try:
            cats = _read(cat_pv, rec)
        except Exception as e:  # noqa: BLE001
            integ.append(A1.unreadable(cat_pv, rec, e))
    uni_issues = A4.classification_issues(master, cats) if len(master) else pd.DataFrame()
    excl = A4.excluded_bars_audit(A4.load_excluded_summaries(layout.runs), trading_days)
    rejected = A4.rejected_window_bars(layout.quarantine)
    boundary = A4.listing_boundary_days(rejected, master, trading_days) if len(master) else pd.DataFrame()

    # ---- A5 gaps ---------------------------------------------------------------------------
    windows = pv5.tables.get("coverage_by_window", pd.DataFrame())
    gap_cases = A5.classify_gaps(A5.explode_gap_days(windows), raw, regime, pv5.seq) if len(windows) else pd.DataFrame()
    partial_plan = A5.partial_window_plan(windows) if len(windows) else pd.DataFrame()
    integ_df = pd.DataFrame(integ, columns=A1.COLUMNS)
    backfill = A5.backfill_candidates(integ_df, windows) if len(windows) else pd.DataFrame()

    # ---- readiness -----------------------------------------------------------------------
    ready = research_ready(day_audit, rec_df, bad, opts.missing_allowance)

    # ---- outputs ---------------------------------------------------------------------------
    tables = {
        "audit_integrity": integ_df,
        "audit_5m_by_symbol_day": day_audit,
        "audit_5m_templates": tmpl_report,
        "audit_ohlcv_reconciliation": rec_df,
        "audit_ohlcv_top100": A3.top_anomalies(rec_df),
        "audit_universe_issues": uni_issues,
        "audit_field_provenance": A4.provenance_table(),
        "audit_excluded_bars": excl,
        "audit_listing_boundary_days": boundary,
        "audit_gap_cases": gap_cases,
        "audit_partial_windows": partial_plan,
        "audit_backfill_candidates": backfill,
        "trading_regime_daily": regime,
        "audit_research_ready": ready,
        "audit_5m_missing_by_liquidity": missing_by_liquidity(ready),
    }
    for name, df in tables.items():
        if df is not None and len(df.columns):
            df.to_parquet(out_dir / f"{name}.parquet", index=False)
    tables["audit_ohlcv_top100"].to_csv(out_dir / "audit_ohlcv_top100.csv", index=False, encoding="utf-8-sig")

    fp_after = {**fingerprint(hash_set, hash_small_bytes=50_000_000), **fingerprint(stat_set)}
    changes = diff_fingerprints(fp_before, fp_after)
    data_changes = diff_fingerprints(data_fp_before, fingerprint(data_paths, hash_small_bytes=0))
    findings = {
        "handoff_reconstruction": A1.reconstruct_handoff(pv5),
        "recon_summary": A3.summarize(rec_df, day_audit),
        "readiness": readiness_summary(ready, opts.missing_allowance),
        "regime_matrix_agreement": regime_checks,
        "source_unchanged": {"control_files": len(hash_set) + len(stat_set), "data_files_stat": len(data_paths),
                             "changed": (changes + data_changes)[:50], "ok": not changes and not data_changes,
                             "method": "pointers + published manifest version: size+mtime+sha256; system entries and "
                                       "audited data files: size+mtime; protected folders not read"},
    }
    meta = {"audit_id": audit_id, "audit_version": AUDIT_VERSION, "git_sha": git_sha(),
            "started_at": started.isoformat(), "finished_at": datetime.now(TAIPEI).isoformat(),
            "elapsed_sec": round(time.monotonic() - t0, 1), "data_root": str(layout.data_root),
            "out_dir": str(out_dir), "sources": sources, "packages": package_versions(),
            "options": {k: (list(v) if isinstance(v, tuple) else str(v) if isinstance(v, Path) else v)
                        for k, v in opts.__dict__.items()},
            "scope": {"partitions_audited": int(len(sel)), "partitions_total": int(len(parts5)),
                      "symbols_audited": int(len(audited_syms)), "finlab_years_from": y0,
                      "trading_calendar_source": calendar_source}}
    write_reports(out_dir, meta, tables, findings, ctx.log)
    if opts.publish_dir is not None:
        dest = Path(opts.publish_dir) / audit_id
        shutil.copytree(out_dir, dest)
        ctx.note(f"copied audit outputs to {dest} (flush/unmount Drive afterwards)")
    return out_dir
