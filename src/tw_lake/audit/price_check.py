"""Official daily price check of the stored 5m bars (read-only, offline, no network).

python -m tw_lake.audit.price_check --data-root ... --ledger-dir .../gap_ledger_... \
    --dataset-root .../reference/official_volume/v1 --out-dir /content/tw_lake_price_check \
    [--start 2024-07-01 --end 2024-09-30] [--symbols 2330,2317] [--workers 8] [--resume-dir <earlier run>]

Official OHLC come from the raw responses already saved in the official reference shards (TWSE MI_INDEX
ALLBUT0999 "每日收盤行情", TPEx dailyQuotes EW): no request is made. For every symbol-day with stored bars:
    open  = open of the first stored bar        high = max(high)     low = min(low)
    close = close of the last stored bar (13:30 closing auction when present)
and compares with the official 開盤/最高/最低/收盤 (unadjusted both). Missing bars that carry no board-lot
trade cannot change these values; a mismatch is therefore evidence against the stored prices (or a gap with
trades). price_reliable:
    TRUE     all four equal (to 1e-6)
    FALSE    any differs (fields listed in price_mismatch)
    UNKNOWN  no official price (no trade / no record) or no stored bars
Nothing is filled or adjusted. Reads 5m partitions in parallel threads (I/O bound); results are written in
chunks so an interrupted run resumes with --resume-dir. Writes only to its own output folder.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from .stagelog import Stages, progress

PRICE_REPORTS = {"TWSE": "MI_INDEX_ALLBUT0999", "TPEX": "dailyQuotes_EW"}
FIELDS = {"open": ("開盤",), "high": ("最高",), "low": ("最低",), "close": ("收盤價", "收盤")}
TOL = 1e-6


def _num(v):
    s = re.sub(r"<[^>]+>", "", str(v)).replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return np.nan


def parse_official_ohlc(body: bytes) -> pd.DataFrame:
    from ..reference.official_volume import _tables
    obj = json.loads(body.decode("utf-8", errors="replace"))
    for _title, fields, rows in _tables(obj):
        f = [x.strip() for x in fields]
        ci = next((i for i, x in enumerate(f) if x in ("證券代號", "代號", "股票代號")), None)
        idx = {}
        for k, keys in FIELDS.items():
            idx[k] = next((i for key in keys for i, x in enumerate(f)
                           if x.startswith(key) and "指數" not in x and "漲跌" not in x), None)
        if ci is None or any(v is None for v in idx.values()):
            continue
        n = max([ci, *idx.values()])
        recs = [{"symbol": str(r[ci]).strip(), **{k: _num(r[i]) for k, i in idx.items()}}
                for r in rows if isinstance(r, list) and len(r) > n]
        if recs:
            return pd.DataFrame(recs).drop_duplicates("symbol")
    return pd.DataFrame(columns=["symbol", "open", "high", "low", "close"])


def official_prices(dataset_root: Path, start: str | None, end: str | None, workers: int) -> pd.DataFrame:
    from ..reference.official_volume import read_raw
    jobs = []
    for sd in sorted((Path(dataset_root) / "shards").glob("year=*/date=*")):
        d = sd.name.split("=", 1)[1]
        if (start and d < start) or (end and d > end) or not (sd / "_SUCCESS").exists():
            continue
        rq = pd.read_parquet(sd / "requests.parquet")
        for ex, rep in PRICE_REPORTS.items():
            r = rq[(rq["report"] == rep) & (rq["http_status"] == 200)]
            if len(r):
                jobs.append((ex, d, rep, sd / r["raw_file"].iloc[0], r["sha256"].iloc[0]))

    def one(j):
        ex, d, rep, p, sha = j
        t = parse_official_ohlc(read_raw(p))
        return t.assign(exchange=ex, date=d, source_report=rep, raw_sha256=sha)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        frames = list(progress(pool.map(one, jobs), "official OHLC", total=len(jobs)))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out.rename(columns={c: f"official_{c}" for c in ("open", "high", "low", "close")})


def bar_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """Daily OHLC from 5m bars, vectorised (bars sorted by time)."""
    if df.empty:
        return pd.DataFrame(columns=["date", "bar_open", "bar_high", "bar_low", "bar_close", "n_bars", "last_bar"])
    d = df.sort_values("bar_start")
    key = d["bar_start"].dt.strftime("%Y-%m-%d")
    g = d.groupby(key)
    out = pd.DataFrame({"bar_open": g["open"].first(), "bar_high": g["high"].max(), "bar_low": g["low"].min(),
                        "bar_close": g["close"].last(), "n_bars": g.size(),
                        "last_bar": g["bar_start"].last().dt.strftime("%H:%M")})
    return out.rename_axis("date").reset_index()


def stored_prices(data_root: Path, symbols: set[str] | None, years: set[int] | None, chunk_dir: Path,
                  workers: int, log=print) -> pd.DataFrame:
    from ..config import load_config
    from ..paths import Layout
    from .reader import load_published
    lay = Layout.from_config(load_config(None, {"data_root": str(data_root)}))
    pv5 = load_published("fubon_5m", lay.dataset_dirs["fubon_5m"])
    parts = pv5.partitions.copy()
    parts["symbol"] = parts["partition"].str.extract(r"symbol=([^/]+)")[0]
    parts["year"] = parts["partition"].str.extract(r"year=(\d+)")[0].astype(int)
    if symbols:
        parts = parts[parts["symbol"].isin(symbols)]
    if years:
        parts = parts[parts["year"].isin(years)]
    chunk_dir.mkdir(parents=True, exist_ok=True)
    done = set()
    for f in chunk_dir.glob("chunk_*.parquet"):
        done |= set(pd.read_parquet(f, columns=["partition"])["partition"].unique())
    todo = parts[~parts["partition"].isin(done)]
    log(f"5m partitions: {len(parts)} in scope, {len(done)} already done, {len(todo)} to read (seq {pv5.seq})")

    def one(rec):
        df = pd.read_parquet(pv5.data_path(rec.relpath), columns=["bar_start", "open", "high", "low", "close"])
        return bar_ohlc(df).assign(symbol=rec.symbol, partition=rec.partition)
    buf, n_chunk = [], len(list(chunk_dir.glob("chunk_*.parquet")))
    recs = list(todo.itertuples(index=False))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for r in progress(pool.map(one, recs), "5m partitions", total=len(recs)):
            buf.append(r)
            if len(buf) >= 200:
                pd.concat(buf, ignore_index=True).to_parquet(chunk_dir / f"chunk_{n_chunk:05d}.parquet", index=False)
                n_chunk, buf = n_chunk + 1, []
    if buf:
        pd.concat(buf, ignore_index=True).to_parquet(chunk_dir / f"chunk_{n_chunk:05d}.parquet", index=False)
    files = sorted(chunk_dir.glob("chunk_*.parquet"))
    out = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else bar_ohlc(pd.DataFrame())
    out.attrs["fubon_5m_seq"] = pv5.seq
    return out


def compare(bars: pd.DataFrame, off: pd.DataFrame) -> pd.DataFrame:
    x = bars.merge(off, on=["symbol", "date"], how="left")
    has_off = x["official_close"].notna()
    has_bar = x["n_bars"].fillna(0) > 0
    mism = pd.Series("", index=x.index, dtype=object)
    for k in ("open", "high", "low", "close"):
        bad = has_off & has_bar & ~((x[f"bar_{k}"] - x[f"official_{k}"]).abs() < TOL)
        x[f"{k}_match"] = np.where(has_off & has_bar, ~bad, np.nan)
        mism = mism.where(~bad, mism + np.where(mism == "", "", "|") + k)
    x["price_mismatch"] = mism
    x["price_reliable"] = np.select([~has_off | ~has_bar, mism == ""], ["UNKNOWN", "TRUE"], default="FALSE")
    return x


def run(data_root: Path, ledger_dir: Path, dataset_root: Path, out_root: Path, start: str | None = None,
        end: str | None = None, symbols: set[str] | None = None, workers: int = 8,
        resume_dir: Path | None = None, log=print) -> Path:
    out = Path(resume_dir) if resume_dir else Path(out_root) / f"price_check_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    if not resume_dir:
        from ..config import load_config
        from ..paths import Layout
        from .runner import _guard_out_dir
        _guard_out_dir(Layout.from_config(load_config(None, {"data_root": str(data_root)})), out)
    out.mkdir(parents=True, exist_ok=True)
    st = Stages(out, "price_check", {"start": start, "end": end, "symbols": sorted(symbols) if symbols else None,
                                     "workers": workers, "ledger_dir": str(ledger_dir),
                                     "dataset_root": str(dataset_root), "resumed": bool(resume_dir)}, log)
    st.input_file("gap_day_summary", Path(ledger_dir) / "gap_day_summary.parquet", hash_it=False)
    with st.stage("official_prices") as s:
        off = official_prices(dataset_root, start, end, workers)
        s["rows"] = int(len(off))
        off.to_parquet(out / "official_ohlc.parquet", index=False)
    years = set(range(int(start[:4]), int(end[:4]) + 1)) if start and end else None
    with st.stage("stored_5m_prices") as s:
        bars = stored_prices(data_root, symbols, years, out / "chunks", workers, log)
        s["symbol_days"] = int(len(bars))
        st.info["inputs"]["fubon_5m_seq"] = bars.attrs.get("fubon_5m_seq")
    if start:
        bars = bars[bars["date"] >= start]
    if end:
        bars = bars[bars["date"] <= end]
    with st.stage("compare") as s:
        x = compare(bars, off)
        days = pd.read_parquet(Path(ledger_dir) / "gap_day_summary.parquet",
                               columns=["symbol", "date", "liquidity_bucket", "expected_policy", "missing_slots"])
        days["date"] = pd.to_datetime(days["date"]).dt.strftime("%Y-%m-%d")
        x = x.merge(days, on=["symbol", "date"], how="left")
        x.to_parquet(out / "price_check_symbol_day.parquet", index=False)
        s["rows"] = int(len(x))
    x["year"] = x["date"].str[:4]
    summ = x.groupby(["year", "exchange", "price_reliable"], dropna=False).size().unstack(fill_value=0).reset_index()
    summ.to_csv(out / "price_check_by_year.csv", index=False, encoding="utf-8-sig")
    liq = x.groupby(["liquidity_bucket", "price_reliable"], dropna=False).size().unstack(fill_value=0).reset_index()
    liq.to_csv(out / "price_check_by_liquidity.csv", index=False, encoding="utf-8-sig")
    mm = x[x["price_reliable"] == "FALSE"]
    fields = mm["price_mismatch"].value_counts().rename("symbol_days").reset_index()
    fields.to_csv(out / "price_mismatch_fields.csv", index=False, encoding="utf-8-sig")
    mm.sort_values("date").head(2000).to_csv(out / "price_mismatch_examples.csv", index=False, encoding="utf-8-sig")
    total = x["price_reliable"].value_counts().to_dict()
    st.finish(price_reliable=total, mismatch_fields=fields.head(10).to_dict("records"))
    log(json.dumps({"price_reliable": total}, ensure_ascii=False))
    log(summ.to_string(index=False))
    log(fields.head(10).to_string(index=False))
    log(f"written to {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.price_check")
    p.add_argument("--data-root", required=True)
    p.add_argument("--ledger-dir", required=True)
    p.add_argument("--dataset-root", required=True)
    p.add_argument("--out-dir", default="/content/tw_lake_price_check")
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--symbols", default="")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--resume-dir", default=None)
    a = p.parse_args(argv)
    syms = {s.strip() for s in a.symbols.split(",") if s.strip()} or None
    run(Path(a.data_root), Path(a.ledger_dir), Path(a.dataset_root), Path(a.out_dir), a.start, a.end, syms,
        a.workers, Path(a.resume_dir) if a.resume_dir else None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
