"""Small, bounded follow-up checks after Eligibility v0.1 (each step is explicit; nothing runs by default).

    # 1. offline: the negative-residual (over-count) symbol-days of the 13 B exchange-dates
    python -m tw_lake.audit.targeted_checks overcount --data-root ... --recon-dir ... --eligibility-dir ... --out-dir ...
    # 2. official re-request of the report-dates listed in <eligibility>/refetch_minimal.csv (3 requests)
    python -m tw_lake.audit.targeted_checks official --eligibility-dir ... --dataset-root ... --out-dir ...
    # 3. Fubon market-data re-request (D / 5 / 1) of the over-count symbol-days; opt-in, bounded
    python -m tw_lake.audit.targeted_checks fubon --data-root ... --overcount-dir ... --out-dir ... --max-symbol-days 100

overcount (no network): for each complete day with official_regular - stored 5m <= -1 share:
    excess = stored - official_regular; is it whole lots; does it equal a measured component (a component the
    provider may have put into its intraday bars: fixed-price, odd-lot, block); does the previous / next trading
    day of the same symbol show the opposite residual (volume booked on the wrong day); stored bars after 13:30;
    FinLab daily volume vs the official total. Nothing is re-classified here.
official: re-requests only the listed (report, date) pairs into a NEW evidence folder (raw gzip, SHA-256,
    retrieved_at, known_at) and compares rows / symbols / values with the stored shard. Shards are immutable and
    are not modified; a differing response is reported, not merged.
fubon: historical candles (market data only; no trading / account API), credentials from Colab Secrets.
Writes only to its own output folders.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from .stagelog import Stages, progress
from .volume_recon import TRADING_UNIT, component_table

COMPONENT_COLS = ("rep_BFT41U", "rep_TWT53U", "rep_TWTC7U", "rep_BFIAUU_ALL", "rep_fixPricing", "rep_oddQuote",
                  "rep_odd_Daily", "rep_blockTrade_quote")


def _new_out(out_root: Path, kind: str) -> Path:
    out = Path(out_root) / f"{kind}_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    out.mkdir(parents=True, exist_ok=False)
    return out


def overcount_table(recon: pd.DataFrame) -> pd.DataFrame:
    r = recon.sort_values(["symbol", "date"]).copy()
    g = r.groupby("symbol")
    r["residual_prev_day"] = g["residual_shares"].shift(1)
    r["residual_next_day"] = g["residual_shares"].shift(-1)
    r["prev_date"] = g["date"].shift(1)
    r["next_date"] = g["date"].shift(-1)
    neg = r[r["complete_day"] & ~r["unit_unverified"] & (r["residual_shares"] <= -1)].copy()
    neg["excess_shares"] = -neg["residual_shares"]
    neg["excess_whole_lots"] = (neg["excess_shares"] % TRADING_UNIT) == 0
    eq = []
    for row in neg.itertuples(index=False):
        hit = [c[4:] for c in COMPONENT_COLS if c in neg.columns and pd.notna(getattr(row, c, np.nan))
               and abs(getattr(row, c) - row.excess_shares) < 1]
        eq.append("|".join(hit))
    neg["excess_equals_component"] = eq
    neg["opposite_residual_adjacent_day"] = np.select(
        [(neg["residual_prev_day"] + neg["residual_shares"]).abs() < 1,
         (neg["residual_next_day"] + neg["residual_shares"]).abs() < 1], ["previous_day", "next_day"], default="")
    return neg


def run_overcount(data_root: Path | None, recon_dir: Path, eligibility_dir: Path | None, out_root: Path,
                  log=print) -> Path:
    out = _new_out(out_root, "overcount_diag")
    st = Stages(out, "overcount_diagnosis", {"recon_dir": str(recon_dir), "eligibility_dir": str(eligibility_dir)}, log)
    st.input_file("volume_recon_symbol_day", Path(recon_dir) / "volume_recon_symbol_day.parquet")
    with st.stage("residuals") as s:
        recon = pd.read_parquet(Path(recon_dir) / "volume_recon_symbol_day.parquet")
        neg = overcount_table(recon)
        s["over_count_symbol_days"] = int(len(neg))
    with st.stage("stored_bars") as s:
        neg["n_bars"], neg["bars_after_1330"], neg["bar_1330_volume_shares"] = np.nan, np.nan, np.nan
        if data_root:
            from ..config import load_config
            from ..paths import Layout
            from .reader import load_published
            lay = Layout.from_config(load_config(None, {"data_root": str(data_root)}))
            pv5 = load_published("fubon_5m", lay.dataset_dirs["fubon_5m"])
            parts = pv5.partitions.set_index("partition")
            for i, row in progress(list(neg.iterrows()), "symbol-days"):
                part = f"symbol={row['symbol']}/year={row['date'][:4]}"
                if part not in parts.index:
                    continue
                df = pd.read_parquet(pv5.data_path(parts.at[part, "relpath"]), columns=["bar_start", "volume_raw"])
                d = df[df["bar_start"].dt.strftime("%Y-%m-%d") == row["date"]]
                hh = d["bar_start"].dt.strftime("%H:%M")
                neg.at[i, "n_bars"] = len(d)
                neg.at[i, "bars_after_1330"] = int((hh > "13:30").sum())
                neg.at[i, "bar_1330_volume_shares"] = float(d.loc[hh == "13:30", "volume_raw"].sum() * TRADING_UNIT)
            st.info["inputs"]["fubon_5m_seq"] = pv5.seq
        s["symbol_days_read"] = int(neg["n_bars"].notna().sum())
    cols = ["exchange", "date", "symbol", "liquidity_bucket", "official_total", "official_regular", "m_volume_shares",
            "excess_shares", "excess_whole_lots", "excess_equals_component", "finlab_vs_official",
            "residual_prev_day", "prev_date", "residual_next_day", "next_date", "opposite_residual_adjacent_day",
            "n_bars", "bars_after_1330", "bar_1330_volume_shares", *[c for c in COMPONENT_COLS if c in neg.columns]]
    neg = neg[cols].sort_values(["date", "exchange", "symbol"])
    neg.to_parquet(out / "overcount_symbol_days.parquet", index=False)
    neg.to_csv(out / "overcount_symbol_days.csv", index=False, encoding="utf-8-sig")
    by = neg.groupby(["exchange", "date"]).agg(
        symbol_days=("symbol", "size"), whole_lot=("excess_whole_lots", "sum"),
        equals_component=("excess_equals_component", lambda s: int((s != "").sum())),
        adjacent_opposite=("opposite_residual_adjacent_day", lambda s: int((s != "").sum())),
        finlab_differs=("finlab_vs_official", lambda s: int((s.abs() >= 1).sum())),
        bars_after_1330=("bars_after_1330", "sum"), median_excess=("excess_shares", "median")).reset_index()
    by.to_csv(out / "overcount_by_exchange_date.csv", index=False, encoding="utf-8-sig")
    st.finish(symbol_days=int(len(neg)), by_exchange_date=by.to_dict("records"))
    log(by.to_string(index=False))
    log(f"written to {out}")
    return out


def run_official(eligibility_dir: Path, dataset_root: Path, out_root: Path, getter=None, sleep=None,
                 log=print) -> Path:
    import time

    from ..reference import official_volume as OV
    out = _new_out(out_root, "official_refetch")
    lst = pd.read_csv(Path(eligibility_dir) / "refetch_minimal.csv")
    lst = lst[lst["report"] != "all batch reports"]
    st = Stages(out, "official_refetch", {"eligibility_dir": str(eligibility_dir), "requests": int(len(lst)),
                                          "pairs": lst[["report", "date"]].to_dict("records")}, log)
    eps = {e.report: e for e in OV.ENDPOINTS}
    rows = []
    with st.stage("requests", requests=int(len(lst))):
        for d, g in lst.groupby("date"):
            probe_dir = OV.probe([date.fromisoformat(d)], out, [eps[r] for r in g["report"]],
                                 getter=getter or OV.http_get, sleep=sleep or time.sleep, log=log,
                                 out_path=out / f"date={d}", compress=True)
            new_rq = pd.read_parquet(probe_dir / "requests.parquet")
            new_pv = pd.read_parquet(probe_dir / "parsed_volume.parquet")
            new_comp = component_table(new_pv) if len(new_pv) else pd.DataFrame(columns=["symbol", "date"])
            shard = Path(dataset_root) / "shards" / f"year={d[:4]}" / f"date={d}"
            old_rq = pd.read_parquet(shard / "requests.parquet")
            old_comp = pd.read_parquet(shard / "components.parquet")
            for rep in g["report"]:
                o = old_rq[old_rq["report"] == rep].iloc[0]
                n = new_rq[new_rq["report"] == rep].iloc[0]
                c = f"rep_{rep}"
                a = old_comp.set_index("symbol")[c] if c in old_comp.columns else pd.Series(dtype=float)
                b = new_comp.set_index("symbol")[c] if c in new_comp.columns else pd.Series(dtype=float)
                m = pd.concat([a.rename("old"), b.rename("new")], axis=1)
                differ = ~((m["old"].isna() & m["new"].isna()) | ((m["old"] - m["new"]).abs() < 1e-6))
                rows.append({"date": d, "report": rep, "old_http": int(o.http_status), "new_http": int(n.http_status),
                             "old_sha256": o.sha256, "new_sha256": n.sha256, "same_bytes": o.sha256 == n.sha256,
                             "old_parsed_rows": int(o.parsed_rows), "new_parsed_rows": int(n.parsed_rows),
                             "old_symbols": int(a.notna().sum()), "new_symbols": int(b.notna().sum()),
                             "symbols_value_differs": int(differ.sum()), "new_retrieved_at": n.retrieved_at})
    res = pd.DataFrame(rows)
    res.to_csv(out / "official_refetch_compare.csv", index=False, encoding="utf-8-sig")
    st.finish(compare=res.to_dict("records"))
    log(res.to_string(index=False))
    log(f"written to {out}")
    return out


def run_fubon(data_root: Path, overcount_dir: Path, out_root: Path, max_symbol_days: int = 100, client=None,
              log=print) -> Path:
    from ..config import load_config
    from .provider_crosscheck import run_crosscheck
    neg = pd.read_parquet(Path(overcount_dir) / "overcount_symbol_days.parquet")
    if len(neg) > max_symbol_days:
        raise SystemExit(f"{len(neg)} symbol-days > --max-symbol-days {max_symbol_days}; raise the bound explicitly")
    sample = neg.assign(stratum="overcount", residual_shares=-neg["excess_shares"])[
        ["symbol", "date", "stratum", "liquidity_bucket", "official_total", "official_regular", "m_volume_shares",
         "residual_shares"]]
    cfg = load_config(None, {"data_root": str(data_root)})
    if client is None:
        from ..fubon_source import RealFubonClient
        client = RealFubonClient.login_from_env()
    log(f"Fubon market-data requests: {len(sample)} symbol-days x 3 timeframes = {3 * len(sample)}")
    out = run_crosscheck(cfg, Path("."), Path(out_root), client, sample=sample, prefix="overcount_fubon", log=log)
    x = pd.read_parquet(out / "crosscheck.parquet")
    eq = lambda a, b: (a - b).abs() < 1  # noqa: E731
    x["overcount_verdict"] = np.select(
        [~eq(x["api_5m_shares"], x["stored_5m_shares"]),
         eq(x["api_D_shares"], x["official_total"]) & (x["api_5m_shares"] > x["official_regular"]),
         x["api_D_shares"] > x["official_total"] + 0.5],
        ["pipeline_differs_from_provider_today", "provider_intraday_exceeds_official_regular_daily_matches",
         "provider_daily_exceeds_official_total"], default="other")
    x.to_csv(out / "overcount_fubon_verdicts.csv", index=False, encoding="utf-8-sig")
    log(x["overcount_verdict"].value_counts().to_string())
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.targeted_checks")
    p.add_argument("command", choices=["overcount", "official", "fubon"])
    p.add_argument("--data-root", default=None)
    p.add_argument("--recon-dir", default=None)
    p.add_argument("--eligibility-dir", default=None)
    p.add_argument("--dataset-root", default=None)
    p.add_argument("--overcount-dir", default=None)
    p.add_argument("--out-dir", default="/content/tw_lake_targeted")
    p.add_argument("--max-symbol-days", type=int, default=100)
    a = p.parse_args(argv)
    if a.command == "overcount":
        run_overcount(Path(a.data_root) if a.data_root else None, Path(a.recon_dir),
                      Path(a.eligibility_dir) if a.eligibility_dir else None, Path(a.out_dir))
    elif a.command == "official":
        run_official(Path(a.eligibility_dir), Path(a.dataset_root), Path(a.out_dir))
    else:
        run_fubon(Path(a.data_root), Path(a.overcount_dir), Path(a.out_dir), a.max_symbol_days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
