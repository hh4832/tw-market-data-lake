"""Provider cross-check for dates whose complete days do not reconcile (read-only, market data only).

python -m tw_lake.audit.provider_crosscheck --data-root ... --recon-dir .../volume_recon_... \
    --out-dir /content/tw_lake_crosscheck [--per-date 20] [--control-per-date 5]

Question: on a day where every expected 5m bar is stored but official regular-session volume is
higher by whole lots, where is the volume lost?
For each sampled symbol-day it re-requests Fubon historical candles for that single day at three
timeframes (D, 5, 1) and compares, in shares:
    official_regular   official total minus the measured components (volume_recon)
    stored_5m          stored bars (published manifest)
    api_5m / api_1m    Fubon intraday candles today (sum of the day)
    api_D              Fubon daily candle (unit recorded both ways; the matching one is reported)
Verdicts
    pipeline_differs        api_5m != stored_5m             -> our stored bars differ from the provider today
    intraday_short_vs_daily api_5m < api_D == official      -> provider intraday candles miss volume its daily has
    one_minute_complete     api_1m == official > api_5m     -> 5m aggregation at the provider loses volume
    provider_feed_short     api_D == api_5m < official      -> provider lacks those trades altogether
    consistent / other
Every response is saved with SHA-256. Writes only to its own output folder; no lake writes; no
trading or account API. Samples: the largest-residual days plus random ones per unverified date,
and residual-0 controls from verified dates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..config import load_config
from ..errors import ApiNotFound
from ..fubon_source import FubonCaller, RateLimiter, RequestBudget, RetryPolicy, candle_params
from ..paths import Layout
from ..run_context import git_sha
from .reader import load_published
from .runner import _guard_out_dir

TIMEFRAMES = ("D", "5", "1")
LOT = 1000.0


def draw(recon: pd.DataFrame, by_date: pd.DataFrame, per_date: int, control_per_date: int, seed: int) -> pd.DataFrame:
    r = recon[(recon["exchange"] == "TWSE") & recon["complete_day"] & ~recon["unit_unverified"]
              & recon["official_regular"].notna()].copy()
    bad = set(by_date.loc[(by_date["exchange"] == "TWSE") & ~by_date["date_verified"], "date"])
    good = set(by_date.loc[(by_date["exchange"] == "TWSE") & by_date["date_verified"], "date"])
    rows = []
    for d, g in r[r["date"].isin(bad) & (r["residual_shares"].abs() >= LOT)].groupby("date"):
        top = g.reindex(g["residual_shares"].abs().sort_values(ascending=False).index).head(per_date // 2)
        rest = g.drop(top.index)
        rnd = rest.sample(min(per_date - len(top), len(rest)), random_state=seed)
        rows += [top.assign(stratum="unverified_top_residual"), rnd.assign(stratum="unverified_random_residual")]
    for d, g in r[r["date"].isin(good) & (r["residual_shares"].abs() < 1)].groupby("date"):
        rows.append(g.sample(min(control_per_date, len(g)), random_state=seed).assign(stratum="verified_control"))
    cols = ["symbol", "date", "stratum", "liquidity_bucket", "official_total", "official_regular", "m_volume_shares",
            "residual_shares"]
    return pd.concat(rows, ignore_index=True)[cols] if rows else pd.DataFrame(columns=cols)


def _sum_volume(rows: list[dict]) -> float:
    return float(sum(float(r.get("volume") or 0) for r in rows))


def verdict(off: float, stored: float, a5: float | None, a1: float | None, aD: float | None) -> str:
    eq = lambda x, y: x is not None and y is not None and abs(x - y) < 1  # noqa: E731
    if a5 is not None and not eq(a5, stored):
        return "pipeline_differs"
    if eq(stored, off):
        return "consistent"
    if eq(a1, off) and a5 is not None and a5 < off:
        return "one_minute_complete"
    if eq(aD, off) and a5 is not None and a5 < off:
        return "intraday_short_vs_daily"
    if eq(aD, a5) and a5 is not None and a5 < off:
        return "provider_feed_short"
    return "other"


def run_crosscheck(cfg, recon_dir: Path, out_root: Path, client, per_date: int = 20, control_per_date: int = 5,
                   seed: int = 20261026, sleep=time.sleep, log=print, sample: pd.DataFrame | None = None,
                   prefix: str = "provider_crosscheck") -> Path:
    layout = Layout.from_config(cfg)
    started = datetime.now(TAIPEI)
    out = Path(out_root) / f"{prefix}_{started:%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    _guard_out_dir(layout, out)
    (out / "responses").mkdir(parents=True)
    if sample is None:              # explicit symbol-day list (targeted checks) or the stratified draw
        recon = pd.read_parquet(Path(recon_dir) / "volume_recon_symbol_day.parquet")
        by_date = pd.read_csv(Path(recon_dir) / "identity_by_date.csv")
        sample = draw(recon, by_date, per_date, control_per_date, seed)
    pv5 = load_published("fubon_5m", layout.dataset_dirs["fubon_5m"])
    parts = pv5.partitions.set_index("partition")
    f = cfg.fubon
    caller = FubonCaller(RateLimiter(int(f["historical_requests_per_min"]), sleep=sleep),
                         RetryPolicy(int(f["max_attempts_per_request"]), float(f["rate_limit_wait_sec"]),
                                     float(f["backoff_base_sec"]), float(f["backoff_max_sec"])),
                         RequestBudget(None), sleep=sleep)
    cache: dict[str, pd.DataFrame] = {}
    rows = []
    for i, s in enumerate(sample.itertuples(index=False), 1):
        part = f"symbol={s.symbol}/year={s.date[:4]}"
        stored_lots = np.nan
        if part in parts.index:
            if part not in cache:
                cache.clear()
                cache[part] = pd.read_parquet(pv5.data_path(parts.at[part, "relpath"]))
            p = cache[part]
            stored_lots = float(p.loc[p["bar_start"].dt.strftime("%Y-%m-%d") == s.date, "volume_raw"].sum())
        rec = {"symbol": s.symbol, "date": s.date, "stratum": s.stratum, "liquidity_bucket": s.liquidity_bucket,
               "official_total": s.official_total, "official_regular": s.official_regular,
               "recon_m5_shares": s.m_volume_shares, "stored_5m_shares": stored_lots * LOT}
        for tf in TIMEFRAMES:
            params = candle_params(s.symbol, s.date, s.date, tf, list(f["fields"]))
            status, data, err = "ok", [], ""
            try:
                resp = caller.call(client.historical_candles, params)
                data = (resp or {}).get("data") or [] if isinstance(resp, dict) else []
            except ApiNotFound as e:
                status, err = "not_found_404", str(e)[:200]
            except Exception as e:  # noqa: BLE001
                status, err = "error", repr(e)[:200]
            body = json.dumps({"params": params, "status": status, "data": data}, ensure_ascii=False, sort_keys=True)
            (out / "responses" / f"{s.symbol}_{s.date}_{tf}.json").write_text(body, encoding="utf-8")
            rec[f"tf{tf}_status"] = status
            rec[f"tf{tf}_rows"] = len(data)
            rec[f"tf{tf}_sha256"] = hashlib.sha256(body.encode("utf-8")).hexdigest()
            rec[f"tf{tf}_volume_raw"] = _sum_volume(data) if status == "ok" else np.nan
        rec["api_5m_shares"] = rec["tf5_volume_raw"] * LOT
        rec["api_1m_shares"] = rec["tf1_volume_raw"] * LOT
        d_raw = rec["tfD_volume_raw"]
        # daily candle unit is not assumed: take the reading (lots or shares) that is closest to official
        cands = [x for x in (d_raw * LOT, d_raw) if pd.notna(x)]
        rec["api_D_shares"] = min(cands, key=lambda x: abs(x - s.official_total)) if cands else np.nan
        rec["api_D_unit"] = ("lot" if pd.notna(d_raw) and rec["api_D_shares"] == d_raw * LOT else "share") \
            if cands else None
        rec["api_D_vs_official_total"] = rec["api_D_shares"] - s.official_total
        rec["api_D_vs_official_regular"] = rec["api_D_shares"] - s.official_regular
        rec["official_regular_minus_api_5m"] = s.official_regular - rec["api_5m_shares"]
        rec["official_regular_minus_api_1m"] = s.official_regular - rec["api_1m_shares"]
        rec["verdict"] = verdict(s.official_regular, rec["stored_5m_shares"],
                                 None if np.isnan(rec["api_5m_shares"]) else rec["api_5m_shares"],
                                 None if np.isnan(rec["api_1m_shares"]) else rec["api_1m_shares"],
                                 None if pd.isna(rec["api_D_shares"]) else rec["api_D_shares"])
        rows.append(rec)
        log(f"[{i}/{len(sample)}] {s.symbol} {s.date} {s.stratum}: {rec['verdict']} "
            f"(official_regular-api5m={rec['official_regular_minus_api_5m']:.0f}, "
            f"official_regular-api1m={rec['official_regular_minus_api_1m']:.0f}, api_D unit={rec['api_D_unit']})")
    res = pd.DataFrame(rows)
    res.to_parquet(out / "crosscheck.parquet", index=False)
    res.to_csv(out / "crosscheck.csv", index=False, encoding="utf-8-sig")
    summ = {"sample": int(len(res)), "git_sha": git_sha(), "recon_dir": str(recon_dir), "fubon_5m_seq": pv5.seq,
            "by_date_verdict": res.groupby(["date", "stratum", "verdict"]).size().rename("n").reset_index()
            .to_dict("records") if len(res) else [],
            "api_D_unit_counts": res["api_D_unit"].value_counts().to_dict() if len(res) else {},
            "statement": "Market-data endpoint only; nothing written to the lake. Verdicts describe where the volume "
                         "difference sits, they do not change the gap ledger."}
    (out / "crosscheck_summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2, default=str),
                                                 encoding="utf-8")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.provider_crosscheck")
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--recon-dir", required=True)
    p.add_argument("--out-dir", default="/content/tw_lake_crosscheck")
    p.add_argument("--per-date", type=int, default=20)
    p.add_argument("--control-per-date", type=int, default=5)
    a = p.parse_args(argv)
    cfg = load_config(a.config, {"data_root": a.data_root} if a.data_root else None)
    from ..fubon_source import RealFubonClient
    client = RealFubonClient.login_from_env()
    out = run_crosscheck(cfg, Path(a.recon_dir), Path(a.out_dir), client, a.per_date, a.control_per_date)
    s = json.loads((out / "crosscheck_summary.json").read_text(encoding="utf-8"))
    print(pd.DataFrame(s["by_date_verdict"]).to_string(index=False))
    print("api_D unit:", s["api_D_unit_counts"])
    print(f"crosscheck written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
