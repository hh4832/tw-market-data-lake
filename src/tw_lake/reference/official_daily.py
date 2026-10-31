"""Official daily quote tables (TWSE MI_INDEX ALLBUT0999, TPEx dailyQuotes EW) from the ARCHIVED raw responses of
reference/official_volume/v1 - offline, no request. Shared by the B4a trading-rules and B5a universe audits.

One row per (exchange, date, symbol) present in the official table:
    name, volume_shares, close, change (signed; NaN when the table shows no comparison, e.g. 'X' / 除權息),
    not_compared (TWSE 'X' or TPEx text such as 除息), reference_price (close - change, only when both numeric:
    the official opening reference of that day reconstructed ex post), next_reference / next_limit_up /
    next_limit_down (TPEx publishes them for the next session when the columns exist), raw_sha256.
A symbol listed with no trade appears with volume 0 and close NaN. Nothing is inferred beyond that.
"""
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

from . import official_volume as OV

DAILY_REPORTS = {"TWSE": "MI_INDEX_ALLBUT0999", "TPEX": "dailyQuotes_EW"}
COLUMNS = ["exchange", "date", "symbol", "name", "volume_shares", "close", "change", "not_compared",
           "reference_price", "next_reference", "next_limit_up", "next_limit_down", "source_report", "raw_sha256"]


def security_type(symbol: str) -> str:
    """Security type from the code pattern (TWSE / TPEx numbering conventions); OTHER when no pattern applies."""
    s = str(symbol)
    if s.startswith("02"):
        return "ETN"
    if s.startswith("01") and s.endswith("T"):
        return "REIT"
    if s.startswith("00"):
        return "ETF"
    if s.startswith("91") and len(s) == 6:
        return "TDR"
    if len(s) == 4 and s.isdigit():
        return "STOCK"
    if len(s) == 5 and s[:4].isdigit() and s[4].isalpha():
        return "PREFERRED_OR_SPECIAL"
    return "OTHER"


def _clean(v) -> str:
    return re.sub(r"<[^>]+>", "", str(v if v is not None else "")).replace(",", "").replace("　", " ").strip()


def _num(v) -> float:
    s = _clean(v)
    try:
        return float(s)
    except ValueError:
        return np.nan


def _find(fields: list[str], *keys, exclude=()) -> int | None:
    f = [re.sub(r"\s+", "", x) for x in fields]
    for k in keys:
        for i, x in enumerate(f):
            if x.startswith(k) and not any(e in x for e in exclude):
                return i
    return None


def parse_daily(body: bytes, exchange: str) -> pd.DataFrame:
    obj = json.loads(body.decode("utf-8", errors="replace"))
    for _title, fields, rows in OV._tables(obj):
        ci = _find(fields, "證券代號", "代號", "股票代號")
        cl = _find(fields, "收盤價", "收盤", exclude=("指數",))
        if ci is None or cl is None:
            continue
        nm = _find(fields, "證券名稱", "名稱")
        vol = _find(fields, "成交股數")
        if exchange == "TWSE":
            sign_i, diff_i = _find(fields, "漲跌(+/-)"), _find(fields, "漲跌價差")
        else:
            sign_i, diff_i = None, _find(fields, "漲跌", exclude=("漲跌停", "漲停", "跌停"))
        nref, nup, ndn = _find(fields, "次日參考價"), _find(fields, "次日漲停價"), _find(fields, "次日跌停價")
        recs = []
        for r in rows:
            if not isinstance(r, list) or len(r) <= max(ci, cl):
                continue
            g = lambda i: r[i] if i is not None and i < len(r) else None  # noqa: E731
            close = _num(g(cl))
            if exchange == "TWSE":
                sign = _clean(g(sign_i))
                diff = _num(g(diff_i))
                not_cmp = sign.upper() == "X"
                change = np.nan if not_cmp or np.isnan(diff) else (-diff if sign == "-" else diff)
            else:
                raw = _clean(g(diff_i))
                change = _num(raw)
                not_cmp = bool(raw) and np.isnan(change) and raw not in ("---", "--", "-")
            recs.append({"symbol": _clean(g(ci)), "name": _clean(g(nm)), "volume_shares": _num(g(vol)),
                         "close": close, "change": change, "not_compared": not_cmp,
                         "next_reference": _num(g(nref)), "next_limit_up": _num(g(nup)),
                         "next_limit_down": _num(g(ndn))})
        if recs:
            d = pd.DataFrame(recs)
            d["reference_price"] = np.where(d["close"].notna() & d["change"].notna(), (d["close"] - d["change"]).round(4),
                                            np.nan)
            return d[d["symbol"].str.len() > 0].drop_duplicates("symbol")
    return pd.DataFrame(columns=[c for c in COLUMNS if c not in ("exchange", "date", "source_report", "raw_sha256")])


def load_official_daily(dataset_root: Path, start: str | None = None, end: str | None = None, workers: int = 8,
                        log=print) -> pd.DataFrame:
    jobs = []
    for sd in sorted((Path(dataset_root) / "shards").glob("year=*/date=*")):
        d = sd.name.split("=", 1)[1]
        if (start and d < start) or (end and d > end) or not (sd / "_SUCCESS").exists():
            continue
        rq = pd.read_parquet(sd / "requests.parquet")
        for ex, rep in DAILY_REPORTS.items():
            r = rq[(rq["report"] == rep) & (rq["http_status"] == 200)]
            if len(r):
                jobs.append((ex, d, rep, sd / r["raw_file"].iloc[0], r["sha256"].iloc[0]))

    def one(j):
        ex, d, rep, p, sha = j
        return parse_daily(OV.read_raw(p), ex).assign(exchange=ex, date=d, source_report=rep, raw_sha256=sha)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        frames = list(pool.map(one, jobs))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)
    out["date"] = pd.to_datetime(out["date"])
    log(f"official daily: {len(jobs)} tables, {len(out)} symbol-days")
    return out[COLUMNS]
