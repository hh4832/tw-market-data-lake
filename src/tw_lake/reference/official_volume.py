"""Official TWSE / TPEx daily volume breakdown - pilot probe (evidence collection, no lake writes).

python -m tw_lake.reference.official_volume --dates 2024-05-24,2025-06-23 --out-dir /content/tw_lake_official_volume

Purpose: find out which public exchange reports are available for which dates, in which units,
and collect them as immutable evidence so the volume-bound argument can be tested
(``tw_lake.audit.volume_recon``). Nothing is assumed about which report contains which trades:
the reconciliation measures that on days whose 5m bars are complete.

Every request is stored as received: ``raw/<exchange>/<report>/<date>.json`` plus a row in
``requests.parquet`` (url, params, http status, bytes, sha256, retrieved_at, known_at, parse result).
Parsed rows go to ``parsed_volume.parquet`` (long format; the unit multiplier is recorded per field).
Endpoints are *candidates*: names and parameters of exchange web reports change over time, so a
failing candidate is a finding, not an error. Polite pacing (TWSE asks for <= 3 requests / 5 s).
Writes only into the given out-dir, which must not be inside DATA_ROOT datasets.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from ..clock import TAIPEI

PROBE_VERSION = "0.1.0"
USER_AGENT = "Mozilla/5.0 (tw-market-data-lake evidence probe; contact repo owner)"


@dataclass(frozen=True)
class Endpoint:
    exchange: str          # TWSE | TPEX
    report: str            # stable id used in file names
    component: str         # what we *expect* it to describe; verified later, never assumed
    url: str               # base url
    params: tuple          # ((key, value-template), ...); {d8} YYYYMMDD, {dslash} YYYY/MM/DD, {roc} 113/01/02
    pace_sec: float


def _fmt(t: str, d: date) -> str:
    return t.format(d8=d.strftime("%Y%m%d"), dslash=d.strftime("%Y/%m/%d"),
                    roc=f"{d.year - 1911}/{d:%m}/{d:%d}")


TWSE = "https://www.twse.com.tw/rwd/zh"
TPEX = "https://www.tpex.org.tw/www/zh-tw"
TPEX_OLD = "https://www.tpex.org.tw/web/stock"
ENDPOINTS: list[Endpoint] = [
    # TWSE
    Endpoint("TWSE", "MI_INDEX_ALLBUT0999", "daily_total", f"{TWSE}/afterTrading/MI_INDEX",
             (("date", "{d8}"), ("type", "ALLBUT0999"), ("response", "json")), 4.0),
    Endpoint("TWSE", "BFT41U", "fixed_price_after_hours", f"{TWSE}/afterTrading/BFT41U",
             (("date", "{d8}"), ("selectType", "ALL"), ("response", "json")), 4.0),
    Endpoint("TWSE", "TWT53U", "odd_lot_after_hours", f"{TWSE}/afterTrading/TWT53U",
             (("date", "{d8}"), ("selectType", "ALL"), ("response", "json")), 4.0),
    Endpoint("TWSE", "TWTC7U", "odd_lot_intraday", f"{TWSE}/afterTrading/TWTC7U",
             (("date", "{d8}"), ("selectType", "ALL"), ("response", "json")), 4.0),
    Endpoint("TWSE", "BFIAUU_S", "block", f"{TWSE}/block/BFIAUU",
             (("date", "{d8}"), ("selectType", "S"), ("response", "json")), 4.0),
    Endpoint("TWSE", "BFIAUU_ALL", "block", f"{TWSE}/block/BFIAUU",
             (("date", "{d8}"), ("selectType", "ALL"), ("response", "json")), 4.0),
    # TPEx (new site; candidates)
    Endpoint("TPEX", "dailyQuotes_EW", "daily_total", f"{TPEX}/afterTrading/dailyQuotes",
             (("date", "{dslash}"), ("type", "EW"), ("response", "json")), 2.5),
    Endpoint("TPEX", "fixedPrice", "fixed_price_after_hours", f"{TPEX}/afterTrading/fixedPrice",
             (("date", "{dslash}"), ("response", "json")), 2.5),
    Endpoint("TPEX", "oddLot", "odd_lot", f"{TPEX}/afterTrading/oddLot",
             (("date", "{dslash}"), ("response", "json")), 2.5),
    Endpoint("TPEX", "blockTrade", "block", f"{TPEX}/afterTrading/blockTrade",
             (("date", "{dslash}"), ("response", "json")), 2.5),
    # TPEx (old site; candidates, may redirect or be retired)
    Endpoint("TPEX", "old_daily_close_quotes", "daily_total",
             f"{TPEX_OLD}/aftertrading/daily_close_quotes/stk_quote_result.php",
             (("l", "zh-tw"), ("d", "{roc}"), ("se", "EW"), ("o", "json")), 2.5),
]

CODE_FIELDS = ("證券代號", "股票代號", "代號", "證券代碼", "代碼", "Code", "Security Code")
SHARE_HINTS = ("成交股數", "股數", "成交數量", "成交量", "Volume")
COUNT_HINTS = ("成交筆數", "筆數", "Transaction")


def known_at(d: date) -> str:
    """Conservative availability time for after-close exchange reports (evening of the trade date)."""
    return f"{d.isoformat()}T20:00:00+08:00"


def http_get(url: str, params: dict, timeout: float = 30.0) -> tuple[int, bytes, str, str]:
    full = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), r.headers.get("Content-Type", ""), r.geturl()
    except urllib.error.HTTPError as e:
        return e.code, e.read() if hasattr(e, "read") else b"", "", full
    except Exception as e:  # noqa: BLE001
        return -1, repr(e).encode(), "", full


def _num(v) -> float | None:
    if v is None:
        return None
    s = re.sub(r"<[^>]+>", "", str(v)).replace(",", "").strip()
    if s in ("", "--", "-", "---", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _tables(obj) -> list[tuple[str, list[str], list[list]]]:
    """Collect (title, fields, rows) from the json layouts the two exchanges use."""
    out = []
    if isinstance(obj, dict):
        if isinstance(obj.get("tables"), list):
            for t in obj["tables"]:
                if isinstance(t, dict) and t.get("fields") and isinstance(t.get("data"), list):
                    out.append((str(t.get("title", "")), [str(f) for f in t["fields"]], t["data"]))
        if obj.get("fields") and isinstance(obj.get("data"), list):
            out.append((str(obj.get("title", "")), [str(f) for f in obj["fields"]], obj["data"]))
        for k, v in obj.items():                                  # e.g. fields9/data9 (older TWSE)
            m = re.fullmatch(r"fields(\d+)", k)
            if m and isinstance(obj.get(f"data{m.group(1)}"), list):
                out.append((str(obj.get(f"subtitle{m.group(1)}", k)), [str(f) for f in v], obj[f"data{m.group(1)}"]))
        if isinstance(obj.get("aaData"), list):                   # old TPEx: no field names
            out.append(("aaData", [], obj["aaData"]))
    return out


def unit_multiplier(field: str) -> float:
    return 1000.0 if ("千股" in field or "(仟股)" in field or "仟股" in field) else 1.0


def parse_response(ep: Endpoint, d: date, body: bytes) -> tuple[pd.DataFrame, dict]:
    """Long rows: exchange, report, component, date, symbol, table, field, value, multiplier, shares."""
    cols = ["exchange", "report", "component", "date", "symbol", "table", "field", "value", "multiplier", "shares"]
    info = {"json": False, "tables": 0, "tables_with_code": 0, "rows": 0, "share_fields": [], "stat": None}
    try:
        obj = json.loads(body.decode("utf-8", errors="replace"))
        info["json"] = True
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=cols), info
    if isinstance(obj, dict):
        info["stat"] = obj.get("stat") or obj.get("status")
    rows = []
    tabs = _tables(obj)
    info["tables"] = len(tabs)
    for title, fields, data in tabs:
        ci = next((i for i, f in enumerate(fields) if f.strip() in CODE_FIELDS), None)
        if ci is None:
            continue
        info["tables_with_code"] += 1
        si = [i for i, f in enumerate(fields) if any(h in f for h in SHARE_HINTS + COUNT_HINTS)]
        info["share_fields"] = sorted(set(info["share_fields"]) | {fields[i] for i in si})
        for r in data:
            if not isinstance(r, (list, tuple)) or len(r) <= ci:
                continue
            sym = re.sub(r"<[^>]+>", "", str(r[ci])).strip()
            for i in si:
                if i < len(r):
                    v = _num(r[i])
                    if v is None:
                        continue
                    m = unit_multiplier(fields[i]) if not any(h in fields[i] for h in COUNT_HINTS) else 1.0
                    rows.append((ep.exchange, ep.report, ep.component, d.isoformat(), sym, title, fields[i], v, m, v * m))
    info["rows"] = len(rows)
    return pd.DataFrame(rows, columns=cols), info


def probe(dates: list[date], out_dir: Path, endpoints: list[Endpoint] | None = None, getter=http_get,
          sleep=time.sleep, log=print) -> Path:
    endpoints = endpoints or ENDPOINTS
    started = datetime.now(TAIPEI)
    out = Path(out_dir) / f"official_volume_probe_{started:%Y%m%d_%H%M%S}"
    out.mkdir(parents=True, exist_ok=False)
    reqs, parsed = [], []
    for ep in endpoints:
        for d in dates:
            params = {k: _fmt(v, d) for k, v in ep.params}
            t0 = datetime.now(TAIPEI).isoformat(timespec="seconds")
            status, body, ctype, final_url = getter(ep.url, params)
            sha = hashlib.sha256(body).hexdigest()
            rel = Path("raw") / ep.exchange / ep.report / f"{d.isoformat()}.json"
            (out / rel).parent.mkdir(parents=True, exist_ok=True)
            (out / rel).write_bytes(body)
            df, info = parse_response(ep, d, body) if status == 200 else (pd.DataFrame(), {"json": False})
            reqs.append({"exchange": ep.exchange, "report": ep.report, "component": ep.component, "date": d.isoformat(),
                         "url": ep.url, "params": json.dumps(params, ensure_ascii=False), "final_url": final_url,
                         "http_status": status, "content_type": ctype, "bytes": len(body), "sha256": sha,
                         "raw_file": str(rel), "retrieved_at": t0, "known_at": known_at(d),
                         "json": info.get("json"), "stat": str(info.get("stat")), "tables": info.get("tables", 0),
                         "tables_with_code": info.get("tables_with_code", 0), "parsed_rows": info.get("rows", 0),
                         "share_fields": "|".join(info.get("share_fields", []))})
            if len(df):
                df["sha256"] = sha
                parsed.append(df)
            log(f"{ep.exchange} {ep.report} {d}: http={status} bytes={len(body)} rows={info.get('rows', 0)} "
                f"fields={'|'.join(info.get('share_fields', []))[:120]}")
            sleep(ep.pace_sec)
    rq = pd.DataFrame(reqs)
    rq.to_parquet(out / "requests.parquet", index=False)
    rq.to_csv(out / "requests.csv", index=False, encoding="utf-8-sig")
    pv = pd.concat(parsed, ignore_index=True) if parsed else pd.DataFrame(
        columns=["exchange", "report", "component", "date", "symbol", "table", "field", "value", "multiplier", "shares",
                 "sha256"])
    pv.to_parquet(out / "parsed_volume.parquet", index=False)
    avail = rq.groupby(["exchange", "report", "component"]).agg(
        dates=("date", "size"), http_200=("http_status", lambda s: int((s == 200).sum())),
        parsed_dates=("parsed_rows", lambda s: int((s > 0).sum())), rows_median=("parsed_rows", "median"),
        share_fields=("share_fields", lambda s: " / ".join(sorted({x for x in s if x})))).reset_index()
    avail.to_csv(out / "availability.csv", index=False, encoding="utf-8-sig")
    meta = {"probe_version": PROBE_VERSION, "started_at": started.isoformat(timespec="seconds"),
            "dates": [d.isoformat() for d in dates], "endpoints": [ep.__dict__ for ep in endpoints],
            "statement": "Evidence collection only. Which report contains which trades is measured in volume_recon."}
    (out / "probe_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.reference.official_volume")
    p.add_argument("--dates", required=True, help="YYYY-MM-DD,... (pilot: keep it short)")
    p.add_argument("--out-dir", default="/content/tw_lake_official_volume")
    p.add_argument("--only", default="", help="comma separated report ids to probe")
    a = p.parse_args(argv)
    dates = [date.fromisoformat(x.strip()) for x in a.dates.split(",") if x.strip()]
    eps = [e for e in ENDPOINTS if not a.only or e.report in set(a.only.split(","))]
    out = probe(dates, Path(a.out_dir), eps)
    print(pd.read_csv(out / "availability.csv").to_string(index=False))
    print(f"probe written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
