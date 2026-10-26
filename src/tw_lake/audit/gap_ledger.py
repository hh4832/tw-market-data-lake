"""Gap Evidence Ledger v0 - every expected 5m slot gets exactly one status (read-only).

python -m tw_lake.audit.gap_ledger --data-root /content/drive/MyDrive/00Quant_Research/data \
    --out-dir /content/tw_lake_gap_ledger

Denominator
    Symbol-days in the 5m era where FinLab shows a trade or Fubon has bars (same scope as the audit).
    * normal regime + a learned template for the date -> the template slots are expected
      (09:00..13:20 continuous slots + the 13:30 closing-auction event, kept as its own slot kind);
    * disposal call-auction regime -> SPECIAL_AUCTION_OR_HALT: no per-slot expectation is asserted
      (bars are counted as real, missing slots are not counted as gaps);
    * no template for the date -> REGIME_UNKNOWN (no expectation asserted).
    Listed days with FinLab volume 0 are tallied separately (whole-day no trade on the official
    daily record) and are not expanded into slots.

Slot statuses (v0; evidence is what exists today, nothing is upgraded by inference)
    REAL_BAR                      a stored bar at an expected slot
    PROVIDER_MISSING_EVIDENCED    whole day absent from Fubon after repeated identical refetches while
                                  FinLab shows a regular-session close (explicit list; not a no-trade proof)
    UNRESOLVED                    everything else. ``evidence_candidate`` records the strongest available
                                  hint without changing the status:
                                    volume_conserved_exact      FinLab daily shares == sum of 5m shares
                                    volume_shortfall / volume_excess
                                    finlab_close_missing        whole day, FinLab volume but no close
                                    no_finlab_volume
Whether ``volume_conserved_exact`` can become a no-trade proof depends on what FinLab daily volume
contains (regular session only, or also block / after-hours fixed-price / odd lot). The ledger
measures that on days where every expected slot has a bar (``conservation_on_complete_days``) instead
of assuming it.

Logic-leak detectors (pipeline or provider, not market inactivity)
    systemic slots   (date, slot) where many liquid symbols (>= 5000 lots that day) miss the same slot
    date spikes      dates where the share of traded symbols without any bar jumps
    unexpected bars  bars outside the template (already an audit invariant; repeated here per day)
    API spot check   ``tw_lake.audit.api_spotcheck`` re-requests a stratified sample read-only and
                     compares the provider response with the stored bars slot by slot.
Outputs go to a new folder outside DATA_ROOT. Request ids / response hashes do not exist for the
historical fetches; those evidence columns stay null (the spot check records them for its sample).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..config import load_config
from ..paths import Layout
from ..run_context import git_sha
from . import bars as A2
from .reader import load_published
from .regime import trading_regime_daily
from .runner import AuditWriteRefused, _guard_out_dir

LEDGER_VERSION = "0.1.0"
CLOSE_AUCTION = "13:30"
UNIT_MULT = {"lot": 1000.0, "share": 1.0, "shares": 1.0}
BUCKETS = ["ge5000_lots", "500_5000_lots", "50_500_lots", "lt50_lots", "no_finlab_trade"]

# Fubon returned nothing for the whole day on 3 identical fetches (run 20261025_211531_4ceb32f) while
# FinLab has a regular-session close. Evidence of a provider gap, not of no trade.
PROVIDER_MISSING_DAYS = {("00720B", "2025-06-23"), ("4716", "2025-06-23"), ("3454", "2026-03-30"),
                         ("6144", "2026-06-23")}
PROVIDER_MISSING_EVIDENCE = "fubon historical candles empty on 3 identical fetches (last run 20261025_211531_4ceb32f); " \
                            "FinLab regular-session close present"

DAY_COLUMNS = ["symbol", "date", "year", "expected_policy", "liquidity_bucket", "expected_slots", "real_bars",
               "missing_slots", "missing_continuous", "missing_close_auction", "missing_first_slot", "missing_runs",
               "longest_run", "unexpected_bars", "m_volume_shares", "f_volume_shares", "diff_shares",
               "volume_ratio", "conservation", "f_close_missing", "last_close_equals_f_close", "day_status",
               "partition", "manifest_seq"]
RUN_COLUMNS = ["symbol", "date", "start_slot", "end_slot", "bar_start_from", "bar_start_to", "n_slots",
               "includes_close_auction", "expected_policy", "status", "evidence_level", "evidence_candidate",
               "evidence_source", "request_id", "response_hash", "manifest_seq", "checked_at", "known_at",
               "notes", "research_eligible"]


def slot_frame(templates: dict) -> pd.DataFrame:
    """date, hhmm, slot_idx (order within the date), slot_kind."""
    rows = [(d, t) for d, ts in templates.items() for t in sorted(ts)]
    tf = pd.DataFrame(rows, columns=["date", "hhmm"])
    tf["date"] = pd.to_datetime(tf["date"])
    tf["slot_idx"] = tf.groupby("date").cumcount().astype("int32")
    tf["slot_kind"] = np.where(tf["hhmm"] == CLOSE_AUCTION, "close_auction", "continuous")
    return tf


def conservation_class(diff_shares: pd.Series, f_shares: pd.Series, m_shares: pd.Series) -> np.ndarray:
    return np.select(
        [f_shares.isna() | (f_shares <= 0), m_shares.fillna(0) <= 0, diff_shares.abs() < 1,
         (diff_shares > 0) & (diff_shares < 1000), diff_shares >= 1000, diff_shares <= -1],
        ["no_finlab_volume", "no_bars", "exact", "shortfall_lt_1_lot", "shortfall", "excess"], default="other")


def ledger_partition(df: pd.DataFrame, symbol: str, finlab_day: pd.DataFrame, tf: pd.DataFrame,
                     regime: pd.DataFrame, partition: str, seq, checked_at: str
                     ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """One 5m partition -> (day rows, missing-run rows, slot rows for cross-section counters).
    finlab_day: date, close, volume_shares rows of this symbol for the partition's year."""
    x = df[["bar_start", "close", "volume_raw", "volume_unit"]].copy()
    x["date"] = x["bar_start"].dt.tz_localize(None).dt.normalize()
    x["hhmm"] = x["bar_start"].dt.strftime("%H:%M")
    x["shares"] = x["volume_raw"] * x["volume_unit"].map(UNIT_MULT).fillna(1000.0)
    x = x.sort_values("bar_start").drop_duplicates("bar_start")

    fd = finlab_day[["date", "close", "volume_shares"]].copy()
    fd["date"] = pd.to_datetime(fd["date"]).dt.normalize()
    fd = fd.drop_duplicates("date")
    traded = fd[fd["volume_shares"].fillna(0) > 0]
    days = sorted(set(x["date"]) | set(traded["date"]))
    if not days:
        return (pd.DataFrame(columns=DAY_COLUMNS), pd.DataFrame(columns=RUN_COLUMNS),
                pd.DataFrame(columns=["date", "hhmm", "liquidity_bucket", "missing"]))
    t = pd.DataFrame({"date": pd.to_datetime(days)})
    t = t.merge(fd.rename(columns={"close": "f_close", "volume_shares": "f_volume_shares"}), on="date", how="left")
    if len(regime):
        rg = regime[["date", "regime"]].copy()
        rg["date"] = pd.to_datetime(rg["date"])
        t = t.merge(rg.drop_duplicates("date"), on="date", how="left")
    else:
        t["regime"] = np.nan
    t["regime"] = t["regime"].fillna("normal")
    tmpl_dates = set(tf["date"].unique()) if len(tf) else set()
    has_tmpl = t["date"].isin(tmpl_dates)
    t["expected_policy"] = np.select([t["regime"] != "normal", ~has_tmpl],
                                     ["special_auction_or_halt", "regime_unknown"], default="normal_template")
    lots = t["f_volume_shares"] / 1000.0
    t["liquidity_bucket"] = A2.liquidity_bucket(lots.where(lots > 0))

    g = x.groupby("date")
    agg = pd.DataFrame({"m_volume_shares": g["shares"].sum(), "n_bars": g.size(),
                        "last_close": g["close"].last()}) if len(x) else \
        pd.DataFrame(columns=["m_volume_shares", "n_bars", "last_close"])
    t = t.merge(agg, left_on="date", right_index=True, how="left")
    t["n_bars"] = t["n_bars"].fillna(0).astype("int64")

    # expected slots for normal-template days
    norm_days = t.loc[t["expected_policy"] == "normal_template", ["date", "liquidity_bucket"]]
    tm = tf[tf["date"].isin(norm_days["date"])].merge(norm_days, on="date", how="left")
    present = x[["date", "hhmm"]].assign(_have=True)
    tm = tm.merge(present, on=["date", "hhmm"], how="left")
    tm["missing"] = tm["_have"].isna()
    off = x.merge(tf[["date", "hhmm"]].assign(_in=True), on=["date", "hhmm"], how="left")
    unexpected = off[off["_in"].isna() & off["date"].isin(norm_days["date"])].groupby("date").size()

    e = tm.groupby("date")
    t["expected_slots"] = t["date"].map(e.size()).fillna(0).astype("int64")
    t["real_bars"] = t["date"].map(e["_have"].count()).fillna(0).astype("int64")
    t["missing_slots"] = t["expected_slots"] - t["real_bars"]
    ms = tm[tm["missing"]]
    t["missing_close_auction"] = t["date"].isin(ms.loc[ms["slot_kind"] == "close_auction", "date"])
    t["missing_continuous"] = t["date"].map(ms[ms["slot_kind"] == "continuous"].groupby("date").size()) \
        .fillna(0).astype("int64")
    t["missing_first_slot"] = t["date"].isin(ms.loc[ms["slot_idx"] == 0, "date"])
    t["unexpected_bars"] = t["date"].map(unexpected).fillna(0).astype("int64")
    # special / unknown regimes: bars are real, nothing is expected per slot
    other = t["expected_policy"] != "normal_template"
    t["real_bars"] = np.where(other, t["n_bars"].to_numpy(), t["real_bars"].to_numpy()).astype("int64")

    # runs of consecutive missing slots
    if len(ms):
        ms = ms.sort_values(["date", "slot_idx"])
        new_run = (ms["date"] != ms["date"].shift()) | (ms["slot_idx"] != ms["slot_idx"].shift() + 1)
        ms = ms.assign(run=new_run.cumsum(), _ca=ms["slot_kind"] == "close_auction")
        r = ms.groupby("run").agg(date=("date", "first"), start_slot=("hhmm", "first"), end_slot=("hhmm", "last"),
                                  n_slots=("hhmm", "size"), includes_close_auction=("_ca", "max"))
    else:
        r = pd.DataFrame(columns=["date", "start_slot", "end_slot", "n_slots", "includes_close_auction"])
    rc = r.groupby("date")["n_slots"]
    t["missing_runs"] = t["date"].map(rc.size()).fillna(0).astype("int64")
    t["longest_run"] = t["date"].map(rc.max()).fillna(0).astype("int64")

    t["diff_shares"] = t["f_volume_shares"] - t["m_volume_shares"].fillna(0)
    t["volume_ratio"] = t["m_volume_shares"] / t["f_volume_shares"].where(t["f_volume_shares"] > 0)
    t["conservation"] = conservation_class(t["diff_shares"], t["f_volume_shares"], t["m_volume_shares"])
    t["f_close_missing"] = t["f_close"].isna() & (t["f_volume_shares"].fillna(0) > 0)
    t["last_close_equals_f_close"] = (t["last_close"] - t["f_close"]).abs() < 1e-9
    dkey = t["date"].dt.strftime("%Y-%m-%d")
    prov_missing = pd.Series([(symbol, d) in PROVIDER_MISSING_DAYS for d in dkey], index=t.index)
    whole_day = (t["expected_policy"] == "normal_template") & (t["n_bars"] == 0)
    t["day_status"] = np.select(
        [t["expected_policy"] == "special_auction_or_halt", t["expected_policy"] == "regime_unknown",
         prov_missing & whole_day, whole_day, t["missing_slots"] == 0],
        ["special_auction_or_halt", "regime_unknown", "provider_missing_evidenced", "unresolved_whole_day",
         "all_expected_slots_real"], default="has_unresolved_slots")
    t["year"] = t["date"].dt.year
    t["symbol"], t["partition"], t["manifest_seq"] = symbol, partition, seq

    # run rows with status + evidence
    if len(r):
        r = r.merge(t[["date", "conservation", "f_close_missing", "day_status"]], on="date", how="left")
        pm = r["day_status"] == "provider_missing_evidenced"
        r["status"] = np.where(pm, "PROVIDER_MISSING_EVIDENCED", "UNRESOLVED")
        r["evidence_level"] = np.where(pm, "provider_repeat_identical+finlab_close", "none")
        cand = np.where(r["day_status"] == "unresolved_whole_day",
                        np.where(r["f_close_missing"], "finlab_close_missing", "whole_day_no_bars"),
                        np.select([r["conservation"] == "exact", r["conservation"].isin(["shortfall", "shortfall_lt_1_lot"]),
                                   r["conservation"] == "excess"],
                                  ["volume_conserved_exact", "volume_shortfall", "volume_excess"],
                                  default=r["conservation"].astype(str)))
        r["evidence_candidate"] = np.where(pm, "", cand)
        r["notes"] = np.where(pm, PROVIDER_MISSING_EVIDENCE, "")
        day_str = r["date"].dt.strftime("%Y-%m-%d")
        r["bar_start_from"] = pd.to_datetime(day_str + " " + r["start_slot"]).dt.tz_localize(TAIPEI)
        r["bar_start_to"] = pd.to_datetime(day_str + " " + r["end_slot"]).dt.tz_localize(TAIPEI)
        r["symbol"] = symbol
        r["expected_policy"] = "normal_template"
        r["evidence_source"] = f"fubon_5m@seq={seq}:{partition}; finlab_daily_raw"
        r["request_id"] = None
        r["response_hash"] = None
        r["manifest_seq"] = seq
        r["checked_at"] = checked_at
        r["known_at"] = day_str + "T16:30:00+08:00"     # needs the full day's daily record
        r["research_eligible"] = False
        r = r[RUN_COLUMNS]
    else:
        r = pd.DataFrame(columns=RUN_COLUMNS)
    slots = tm[["date", "hhmm", "liquidity_bucket", "missing"]]
    return t[DAY_COLUMNS], r, slots


class CrossSection:
    """(date, slot, liquidity bucket) -> expected / missing counters, accumulated without big concats."""

    def __init__(self, tf: pd.DataFrame):
        self.dates = pd.DatetimeIndex(sorted(tf["date"].unique()))
        self.slots = sorted(tf["hhmm"].unique())
        self.si = {s: i for i, s in enumerate(self.slots)}
        self.bi = {b: i for i, b in enumerate(BUCKETS)}
        shape = (len(self.dates), len(self.slots), len(BUCKETS))
        self.expected = np.zeros(shape, dtype=np.int64)
        self.missing = np.zeros(shape, dtype=np.int64)

    def add(self, slots: pd.DataFrame) -> None:
        if len(slots) == 0:
            return
        di = self.dates.get_indexer(slots["date"])
        si = slots["hhmm"].map(self.si).to_numpy()
        bi = slots["liquidity_bucket"].map(self.bi).fillna(self.bi["no_finlab_trade"]).astype(int).to_numpy()
        ok = di >= 0
        np.add.at(self.expected, (di[ok], si[ok], bi[ok]), 1)
        m = ok & slots["missing"].to_numpy()
        np.add.at(self.missing, (di[m], si[m], bi[m]), 1)

    def frame(self) -> pd.DataFrame:
        d, s, b = np.nonzero(self.expected)
        return pd.DataFrame({"date": self.dates[d], "hhmm": np.array(self.slots)[s], "liquidity_bucket": np.array(BUCKETS)[b],
                             "expected": self.expected[d, s, b], "missing": self.missing[d, s, b]})


def systemic_slots(cs: pd.DataFrame, min_liquid: int = 20, share: float = 0.2) -> pd.DataFrame:
    """Slots where at least `share` of the liquid (>= 5000 lots) symbols expected there have no bar."""
    liq = cs[cs["liquidity_bucket"] == "ge5000_lots"].copy()
    liq["missing_share"] = liq["missing"] / liq["expected"]
    return liq[(liq["expected"] >= min_liquid) & (liq["missing_share"] >= share)] \
        .sort_values(["missing_share", "date"], ascending=[False, True]).reset_index(drop=True)


def date_spikes(days: pd.DataFrame, z: float = 4.0) -> pd.DataFrame:
    """Per date: share of FinLab-traded normal-template symbol-days with no bars, and missing-slot share
    of liquid symbols; flags dates far above the median (robust z on MAD)."""
    d = days[days["expected_policy"] == "normal_template"]
    g = d.groupby("date")
    out = pd.DataFrame({
        "symbol_days": g.size(),
        "whole_day_missing": g["day_status"].agg(lambda s: int(s.isin(["unresolved_whole_day",
                                                                         "provider_missing_evidenced"]).sum())),
        "expected_slots": g["expected_slots"].sum(), "missing_slots": g["missing_slots"].sum()})
    liq = d[d["liquidity_bucket"] == "ge5000_lots"].groupby("date")
    out["liquid_missing_share"] = (liq["missing_slots"].sum() / liq["expected_slots"].sum()).reindex(out.index)
    out["whole_day_missing_share"] = out["whole_day_missing"] / out["symbol_days"]
    for c in ("liquid_missing_share", "whole_day_missing_share"):
        v = out[c].fillna(0)
        mad = (v - v.median()).abs().median() or 1e-9
        out[f"{c}_robust_z"] = (v - v.median()) / (1.4826 * mad)
    out["spike"] = (out["liquid_missing_share_robust_z"] > z) | (out["whole_day_missing_share_robust_z"] > z)
    return out.reset_index()


def summarize(days: pd.DataFrame, run_status_slots: dict, cs: pd.DataFrame, zero_volume_days: int,
              sys_slots: pd.DataFrame, spikes: pd.DataFrame) -> dict:
    n = days["expected_policy"] == "normal_template"
    tot = int(days.loc[n, "expected_slots"].sum())
    real = int(days.loc[n, "real_bars"].sum())
    out = {"ledger_version": LEDGER_VERSION,
           "denominator": "normal-template symbol-days x template slots (5m era, FinLab trade day or 5m bars)",
           "symbol_days": {k: int(v) for k, v in days["expected_policy"].value_counts().items()},
           "listed_zero_volume_days_not_expanded": int(zero_volume_days),
           "slots": {"total_expected": tot, "real_bars": real, "missing": tot - real,
                     "missing_pct": round(100 * (tot - real) / tot, 2) if tot else None,
                     "by_status": {k: int(v) for k, v in run_status_slots.items()}},
           "special_auction_or_halt_bars": int(days.loc[days["expected_policy"] == "special_auction_or_halt",
                                                        "real_bars"].sum()),
           "day_status": {k: int(v) for k, v in days["day_status"].value_counts().items()}}
    miss_days = days[n & (days["missing_slots"] > 0)]
    by = {}
    for b in BUCKETS:
        dd = days[n & (days["liquidity_bucket"] == b)]
        if not len(dd):
            continue
        e, m = int(dd["expected_slots"].sum()), int(dd["missing_slots"].sum())
        by[b] = {"symbol_days": int(len(dd)), "expected_slots": e, "missing_slots": m,
                 "missing_pct": round(100 * m / e, 2) if e else None}
    out["by_liquidity"] = by
    yr = days[n].groupby("year").agg(expected=("expected_slots", "sum"), missing=("missing_slots", "sum"))
    out["by_year"] = {int(y): {"expected": int(r.expected), "missing": int(r.missing),
                               "missing_pct": round(100 * r.missing / r.expected, 2) if r.expected else None}
                      for y, r in yr.iterrows()}
    comp = days[n & (days["missing_slots"] == 0) & (days["real_bars"] > 0)]
    out["conservation_on_complete_days"] = _cons(comp)
    out["conservation_on_days_with_missing_slots"] = _cons(miss_days[miss_days["real_bars"] > 0])
    out["conservation_on_complete_days_by_liquidity"] = {b: _cons(comp[comp["liquidity_bucket"] == b])
                                                         for b in BUCKETS if (comp["liquidity_bucket"] == b).any()}
    out["conservation_on_missing_days_by_liquidity"] = {
        b: _cons(miss_days[(miss_days["liquidity_bucket"] == b) & (miss_days["real_bars"] > 0)])
        for b in BUCKETS if (miss_days["liquidity_bucket"] == b).any()}
    out["logic_leak_signals"] = {
        "systemic_slots_liquid_share_ge_20pct": int(len(sys_slots)),
        "spike_dates": int(spikes["spike"].sum()) if len(spikes) else 0,
        "days_with_unexpected_bars": int((days["unexpected_bars"] > 0).sum()),
        "missing_first_slot_days_liquid": int((n & days["missing_first_slot"]
                                               & (days["liquidity_bucket"] == "ge5000_lots")).sum()),
        "missing_close_auction_days_liquid": int((n & days["missing_close_auction"]
                                                  & (days["liquidity_bucket"] == "ge5000_lots")).sum())}
    out["statement"] = ("UNRESOLVED slots are not proven gaps and not proven no-trade; evidence_candidate is a hint "
                        "only. The ledger does not declare the data research-ready.")
    return out


def _cons(d: pd.DataFrame) -> dict:
    if len(d) == 0:
        return {"days": 0}
    vc = d["conservation"].value_counts()
    out = {"days": int(len(d)), "classes": {k: int(v) for k, v in vc.items()},
           "exact_pct": round(100 * vc.get("exact", 0) / len(d), 2)}
    s = d.loc[d["diff_shares"] > 0, "diff_shares"]
    if len(s):
        out["shortfall_shares_p50"] = float(s.median())
        out["shortfall_pct_of_daily_p50"] = float((s / d.loc[s.index, "f_volume_shares"]).median() * 100)
    return out


# ---- orchestration ------------------------------------------------------------------------
def run_ledger(cfg, out_root: Path, symbols: list[str] | None = None, max_partitions: int | None = None,
               template_symbols=("2330", "0050", "006208"), progress: bool = True) -> Path:
    layout = Layout.from_config(cfg)
    started = datetime.now(TAIPEI)
    lid = f"gap_ledger_{started:%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    out = Path(out_root) / lid
    _guard_out_dir(layout, out)
    out.mkdir(parents=True)
    (out / "gap_runs").mkdir()
    t0 = time.monotonic()
    log = []

    def note(msg):
        line = f"{datetime.now(TAIPEI).isoformat(timespec='seconds')} {msg}  [{time.monotonic() - t0:.0f}s]"
        log.append(line)
        print(line, flush=True)

    pvs = {n: load_published(n, layout.dataset_dirs[n]) for n in ("finlab_daily_raw", "disposal_attention", "fubon_5m")}
    raw_pv = pvs["finlab_daily_raw"]
    y0 = cfg.minute_start.year
    frames = [pd.read_parquet(raw_pv.data_path(r.relpath)) for r in raw_pv.partitions.itertuples(index=False)
              if int(r.partition.split("=")[1]) >= y0]
    raw = pd.concat(frames, ignore_index=True)
    raw["symbol"] = raw["symbol"].astype(str)
    raw["date"] = pd.to_datetime(raw["date"]).dt.normalize()
    raw = raw[raw["date"] >= pd.Timestamp(cfg.minute_start)]
    trading_days = sorted(raw.loc[raw["volume_shares"].fillna(0) > 0, "date"].unique())
    raw_by_sym = dict(tuple(raw.groupby("symbol")))
    note(f"FinLab raw: {len(raw)} rows, {len(trading_days)} trading days")

    disp_pv = pvs["disposal_attention"]
    disp = {r.partition.split("=", 1)[1]: pd.read_parquet(disp_pv.data_path(r.relpath))
            for r in disp_pv.partitions.itertuples(index=False)}
    regime = trading_regime_daily(disp.get("disposal_information"), trading_days)
    regime_by_sym = dict(tuple(regime.groupby("symbol"))) if len(regime) else {}

    pv5 = pvs["fubon_5m"]
    parts = pv5.partitions.copy()
    parts["symbol"] = parts["partition"].str.extract(r"symbol=([^/]+)")[0]
    parts["year"] = parts["partition"].str.extract(r"year=(\d+)")[0].astype(int)
    tmpl = [pd.read_parquet(pv5.data_path(r.relpath))
            for r in parts[parts["symbol"].isin(template_symbols)].itertuples(index=False)]
    tf = slot_frame(A2.build_templates(tmpl))
    cs = CrossSection(tf)
    note(f"templates: {tf['date'].nunique()} dates")

    sel = parts if not symbols else parts[parts["symbol"].isin(symbols)]
    if max_partitions:
        sel = sel.head(max_partitions)
    sel_syms = set(sel["symbol"])
    zero_vol = int(((raw["volume_shares"].fillna(0) <= 0) & raw["close"].notna() & raw["symbol"].isin(sel_syms)).sum())
    checked_at = started.isoformat(timespec="seconds")
    day_frames, run_buf, status_slots, part_no = [], [], {}, 0

    def flush_runs():
        nonlocal run_buf, part_no
        if run_buf:
            pd.concat(run_buf, ignore_index=True).to_parquet(out / "gap_runs" / f"part-{part_no:05d}.parquet", index=False)
            part_no += 1
            run_buf = []

    def load(rec):
        try:
            return rec, pd.read_parquet(pv5.data_path(rec.relpath)), None
        except Exception as e:  # noqa: BLE001
            return rec, None, e

    from concurrent.futures import ThreadPoolExecutor
    recs = list(sel.itertuples(index=False))
    try:
        from tqdm.auto import tqdm
        bar = tqdm(total=len(recs), desc="ledger partitions", mininterval=30, disable=not progress)
    except Exception:  # noqa: BLE001
        bar = None
    unreadable = []
    buffered = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        for i in range(0, len(recs), 64):
            for rec, df, err in ex.map(load, recs[i:i + 64]):
                if bar is not None:
                    bar.update(1)
                if err is not None:
                    unreadable.append({"partition": rec.partition, "error": repr(err)[:300]})
                    continue
                fd = raw_by_sym.get(rec.symbol, raw.iloc[0:0])
                fd = fd[fd["date"].dt.year == rec.year]
                d, r, slots = ledger_partition(df, rec.symbol, fd, tf, regime_by_sym.get(rec.symbol, regime.iloc[0:0]),
                                               rec.partition, pv5.seq, checked_at)
                day_frames.append(d)
                cs.add(slots)
                if len(r):
                    for k, v in r.groupby("status")["n_slots"].sum().items():
                        status_slots[k] = status_slots.get(k, 0) + int(v)
                    run_buf.append(r)
                    buffered += len(r)
                    if buffered > 2_000_000:
                        flush_runs()
                        buffered = 0
    flush_runs()
    if bar is not None:
        bar.close()
    days = pd.concat(day_frames, ignore_index=True)
    status_slots["REAL_BAR"] = int(days.loc[days["expected_policy"] == "normal_template", "real_bars"].sum())
    note(f"ledger pass done: {len(recs)} partitions, {len(days)} symbol-days, unreadable={len(unreadable)}")

    csf = cs.frame()
    sys_slots = systemic_slots(csf)
    spikes = date_spikes(days)
    summ = summarize(days, status_slots, csf, zero_vol, sys_slots, spikes)
    summ["unreadable_partitions"] = unreadable
    days.to_parquet(out / "gap_day_summary.parquet", index=False)
    csf.to_parquet(out / "gap_slot_crosssection.parquet", index=False)
    sys_slots.to_parquet(out / "gap_systemic_slots.parquet", index=False)
    spikes.to_parquet(out / "gap_date_spikes.parquet", index=False)
    spikes[spikes["spike"]].to_csv(out / "gap_date_spikes_flagged.csv", index=False, encoding="utf-8-sig")
    meta = {"ledger_id": lid, "ledger_version": LEDGER_VERSION, "git_sha": git_sha(), "started_at": checked_at,
            "finished_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "elapsed_sec": round(time.monotonic() - t0, 1), "data_root": str(layout.data_root),
            "sources": {n: {"seq": pv.seq, "version_dir": (pv.pointer or {}).get("version_dir")} for n, pv in pvs.items()},
            "scope": {"partitions": len(recs), "symbols": len(sel_syms)}, "template_symbols": list(template_symbols)}
    (out / "gap_ledger_summary.json").write_text(json.dumps({"meta": meta, "summary": summ, "log": log},
                                                            ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md = [f"# Gap Evidence Ledger {lid}", "", f"- ledger version {LEDGER_VERSION}, git `{meta['git_sha'][:7]}`, "
          f"fubon_5m seq={pvs['fubon_5m'].seq}", f"- {summ['statement']}", "", "```json",
          json.dumps(summ, ensure_ascii=False, indent=2, default=str), "```"]
    (out / "gap_ledger_summary.md").write_text("\n".join(md), encoding="utf-8")
    note(f"ledger written to {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.gap_ledger")
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--out-dir", default="/content/tw_lake_gap_ledger")
    p.add_argument("--symbols", default=None)
    p.add_argument("--max-partitions", type=int, default=None)
    p.add_argument("--no-progress", action="store_true")
    a = p.parse_args(argv)
    cfg = load_config(a.config, {"data_root": a.data_root} if a.data_root else None)
    try:
        out = run_ledger(cfg, Path(a.out_dir), [s.strip() for s in a.symbols.split(",")] if a.symbols else None,
                         a.max_partitions, progress=not a.no_progress)
    except AuditWriteRefused as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    print(f"gap ledger written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
