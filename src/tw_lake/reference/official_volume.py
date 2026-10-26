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
    # TPEx website actions found by --discover-tpex (menu.json pages, 2026-10-26); parameters mirror dailyQuotes
    Endpoint("TPEX", "otc_EW", "daily_total_ex_fixed_price", f"{TPEX}/afterTrading/otc",
             (("date", "{dslash}"), ("type", "EW"), ("response", "json")), 2.5),
    Endpoint("TPEX", "fixPricing", "fixed_price_after_hours", f"{TPEX}/afterTrading/fixPricing",
             (("date", "{dslash}"), ("type", "EW"), ("response", "json")), 2.5),
    Endpoint("TPEX", "oddQuote", "odd_lot_intraday", f"{TPEX}/afterTrading/oddQuote",
             (("date", "{dslash}"), ("type", "EW"), ("response", "json")), 2.5),
    Endpoint("TPEX", "odd", "odd_lot_after_hours", f"{TPEX}/afterTrading/odd",
             (("date", "{dslash}"), ("type", "EW"), ("response", "json")), 2.5),
    Endpoint("TPEX", "odd_noType", "odd_lot_after_hours", f"{TPEX}/afterTrading/odd",
             (("date", "{dslash}"), ("response", "json")), 2.5),
    Endpoint("TPEX", "odd_AL", "odd_lot_after_hours", f"{TPEX}/afterTrading/odd",
             (("date", "{dslash}"), ("type", "AL"), ("response", "json")), 2.5),
    Endpoint("TPEX", "blockTrade_tradeInfo", "block", f"{TPEX}/blockTrade/tradeInfo",
             (("date", "{dslash}"), ("response", "json")), 2.5),
    Endpoint("TPEX", "blockTrade_quote", "block", f"{TPEX}/blockTrade/quote",
             (("date", "{dslash}"), ("response", "json")), 2.5),
    # TPEx (old site; candidates, may redirect or be retired)
    Endpoint("TPEX", "old_daily_close_quotes", "daily_total",
             f"{TPEX_OLD}/aftertrading/daily_close_quotes/stk_quote_result.php",
             (("l", "zh-tw"), ("d", "{roc}"), ("se", "EW"), ("o", "json")), 2.5),
]

CODE_FIELDS = ("證券代號", "股票代號", "代號", "證券代碼", "代碼", "Code", "Security Code")
SHARE_HINTS = ("成交股數", "股數", "成交數量", "成交量", "張數", "Volume")
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
    """Header-declared unit: 千股/仟股/張 = 1,000 shares (board lot; foreign-currency ETFs differ and are
    excluded from reconciliation)."""
    return 1000.0 if ("千股" in field or "仟股" in field or "張" in field) else 1.0


DATE_FIELDS = ("成交日期", "日期")


def _row_date_matches(v, d: date) -> bool:
    s = re.sub(r"[^0-9]", "", str(v))
    roc = f"{d.year - 1911}{d:%m%d}"
    return s in (roc, d.strftime("%Y%m%d"))


AMOUNT_FIELDS = ("成交金額",)
PRICE_FIELDS = ("成交價", "收盤價", "成交均價", "收盤")


def _notes_text(obj) -> str:
    parts = []
    if isinstance(obj, dict):
        for k in ("notes", "hints", "note"):
            v = obj.get(k)
            parts += [str(x) for x in v] if isinstance(v, list) else [str(v)] if v else []
        for t in obj.get("tables", []) or []:
            if isinstance(t, dict):
                for k in ("notes", "hints"):
                    v = t.get(k)
                    parts += [str(x) for x in v] if isinstance(v, list) else [str(v)] if v else []
    return " ".join(parts)


def _row_multiplier(field: str, row, fields: list[str], notes: str) -> tuple[float, str]:
    """Unit multiplier for one value: header text, then amount/(qty*price) implied, then report notes."""
    if unit_multiplier(field) != 1.0:
        return unit_multiplier(field), "header"
    ai = next((i for i, f in enumerate(fields) if f.strip() in AMOUNT_FIELDS), None)
    pi = next((i for i, f in enumerate(fields) if f.strip() in PRICE_FIELDS), None)
    qi = fields.index(field)
    if ai is not None and pi is not None and max(ai, pi, qi) < len(row):
        a, p, q = _num(row[ai]), _num(row[pi]), _num(row[qi])
        if a and p and q:
            ratio = a / (q * p)
            if 800 < ratio < 1250:
                return 1000.0, "implied_amount"
            if 0.8 < ratio < 1.25:
                return 1.0, "implied_amount"
    if "千股" in notes and field.strip() == "成交數量":
        return 1000.0, "report_notes"
    return 1.0, "default"


def parse_response(ep: Endpoint, d: date, body: bytes) -> tuple[pd.DataFrame, dict]:
    """Long rows: exchange, report, component, date, symbol, table, field, value, multiplier, multiplier_basis, shares."""
    cols = ["exchange", "report", "component", "date", "symbol", "table", "field", "value", "multiplier",
            "multiplier_basis", "shares"]
    info = {"json": False, "tables": 0, "tables_with_code": 0, "rows": 0, "share_fields": [], "stat": None,
            "notes": ""}
    try:
        obj = json.loads(body.decode("utf-8", errors="replace"))
        info["json"] = True
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=cols), info
    if isinstance(obj, dict):
        info["stat"] = obj.get("stat") or obj.get("status")
    notes = _notes_text(obj)
    info["notes"] = notes[:2000]
    rows = []
    tabs = _tables(obj)
    info["tables"] = len(tabs)
    for title, fields, data in tabs:
        ci = next((i for i, f in enumerate(fields) if f.strip() in CODE_FIELDS), None)
        if ci is None:
            continue
        info["tables_with_code"] += 1
        di = next((i for i, f in enumerate(fields) if f.strip() in DATE_FIELDS), None)
        if di is not None:                    # multi-day tables (e.g. TPEx blockTrade/tradeInfo): keep the asked date
            n0 = len(data)
            data = [r for r in data if isinstance(r, (list, tuple)) and len(r) > di and _row_date_matches(r[di], d)]
            info["date_filtered"] = f"{len(data)}/{n0}"
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
                    if any(h in fields[i] for h in COUNT_HINTS):
                        m, basis = 1.0, "count"
                    else:
                        m, basis = _row_multiplier(fields[i], r, fields, notes)
                    rows.append((ep.exchange, ep.report, ep.component, d.isoformat(), sym, title, fields[i], v, m,
                                 basis, v * m))
    info["rows"] = len(rows)
    return pd.DataFrame(rows, columns=cols), info


def reparse(probe_dir: Path) -> Path:
    """Re-parse the saved raw responses with the current parser (no network). Writes
    <probe_dir>/reparse_<ts>/parsed_volume.parquet; the original files are not touched."""
    probe_dir = Path(probe_dir)
    rq = pd.read_parquet(probe_dir / "requests.parquet")
    eps = {e.report: e for e in ENDPOINTS}
    out = probe_dir / f"reparse_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}"
    out.mkdir()
    frames, notes = [], []
    for r in rq.itertuples(index=False):
        if r.http_status != 200 or r.report not in eps:
            continue
        df, info = parse_response(eps[r.report], date.fromisoformat(r.date), (probe_dir / r.raw_file).read_bytes())
        notes.append({"report": r.report, "date": r.date, "json": info["json"], "rows": info["rows"],
                      "notes": info.get("notes", "")})
        if len(df):
            df["sha256"] = r.sha256
            frames.append(df)
    (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()).to_parquet(out / "parsed_volume.parquet",
                                                                                  index=False)
    pd.DataFrame(notes).to_csv(out / "report_notes.csv", index=False, encoding="utf-8-sig")
    rq.to_parquet(out / "requests.parquet", index=False)      # lets a reparse folder act as a probe dir
    (out / "reparse_meta.json").write_text(json.dumps({"probe_version": PROBE_VERSION, "source": str(probe_dir)},
                                                      ensure_ascii=False), encoding="utf-8")
    return out


TPEX_DISCOVERY = ["https://www.tpex.org.tw/openapi/swagger.json", "https://www.tpex.org.tw/rsrc/js/main.js"]
TPEX_MENU = "https://www.tpex.org.tw/data/menu/zh-tw/menu.json"      # the site builds its sitemap from this
TPEX_PAGE_KEYWORDS = ("盤後定價", "零股", "鉅額", "收盤行情", "定價")
ACTION_RE = re.compile(r"""["'`]((?:[a-z][A-Za-z]*/)+[a-z][A-Za-z]*)["'`]""")
PAGE_COLUMNS = ["title", "url", "http_status", "actions", "page_scripts", "script_actions"]


def _menu_items(node, trail=()):
    """Yield (title path, url) from the TPEx menu json, whatever the nesting keys are."""
    if isinstance(node, dict):
        title = str(node.get("title") or node.get("name") or "")
        url = node.get("url") or node.get("link") or node.get("href")
        t = trail + ((title,) if title else ())
        if isinstance(url, str) and url:
            yield " > ".join(t), url
        for v in node.values():
            if isinstance(v, (list, dict)):
                yield from _menu_items(v, t)
    elif isinstance(node, list):
        for v in node:
            yield from _menu_items(v, trail)


def _actions(text: str) -> list[str]:
    return sorted({a for a in ACTION_RE.findall(text)
                   if not a.startswith(("rsrc/", "css/", "js/", "zh-tw/", "en-us/", "ja-jp/", "text/", "application/"))})


def discover_tpex(out_dir: Path, getter=http_get, sleep=time.sleep, max_pages: int = 80) -> Path:
    """Find the TPEx website JSON actions (API pattern /www/{LANG}/{ACTION}) for after-hours fixed price,
    odd lot and block trades: read the site menu json, open the matching pages and their page scripts,
    list the action-like strings. The OpenAPI (latest day only, no date parameter) is saved for reference."""
    out = Path(out_dir) / f"tpex_discovery_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}"
    (out / "pages").mkdir(parents=True)
    rows = []
    for url in TPEX_DISCOVERY + [TPEX_MENU]:
        status, body, ctype, final = getter(url, {})
        (out / url.rsplit("/", 1)[1]).write_bytes(body)
        rows.append({"url": url, "http_status": status, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    pd.DataFrame(rows).to_csv(out / "discovery_requests.csv", index=False)
    try:
        menu = json.loads((out / "menu.json").read_bytes().decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001
        menu = {}
    items = [(t, u if u.startswith("http") else "https://www.tpex.org.tw" + (u if u.startswith("/") else "/" + u))
             for t, u in _menu_items(menu)]
    pd.DataFrame(items, columns=["title", "url"]).to_csv(out / "menu_items.csv", index=False, encoding="utf-8-sig")
    wanted = [(t, u) for t, u in dict.fromkeys(items) if any(k in t for k in TPEX_PAGE_KEYWORDS) and u.endswith(".html")]
    pages, seen_scripts = [], {}
    for title, href in wanted[:max_pages]:
        st, b, _, _ = getter(href, {})
        name = re.sub(r"[^A-Za-z0-9]+", "_", href.split("tpex.org.tw/")[-1])[:120]
        (out / "pages" / name).write_bytes(b)
        page = b.decode("utf-8", errors="replace")
        scripts = sorted({x for x in re.findall(r'<script[^>]+src="([^"]+)"', page)
                          if "rsrc/asset" not in x and "googletagmanager" not in x and "cloudflare" not in x})
        sacts = set()
        for sc in scripts:
            u = sc if sc.startswith("http") else "https://www.tpex.org.tw" + sc
            if u not in seen_scripts:
                s2, sb, _, _ = getter(u, {})
                seen_scripts[u] = _actions(sb.decode("utf-8", errors="replace")) if s2 == 200 else []
                sleep(0.5)
            sacts |= set(seen_scripts[u])
        pages.append({"title": title, "url": href, "http_status": st, "actions": "|".join(_actions(page)),
                      "page_scripts": "|".join(scripts), "script_actions": "|".join(sorted(sacts))})
        sleep(1.5)
    pd.DataFrame(pages, columns=PAGE_COLUMNS).to_csv(out / "pages.csv", index=False, encoding="utf-8-sig")
    return out


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
            if status == 200 and not info.get("json"):
                status_note = "soft_404_or_non_json"
            else:
                status_note = ""
            reqs.append({"exchange": ep.exchange, "report": ep.report, "component": ep.component, "date": d.isoformat(),
                         "url": ep.url, "params": json.dumps(params, ensure_ascii=False), "final_url": final_url,
                         "http_status": status, "content_type": ctype, "bytes": len(body), "sha256": sha,
                         "raw_file": str(rel), "retrieved_at": t0, "known_at": known_at(d),
                         "json": info.get("json"), "note": status_note, "stat": str(info.get("stat")), "tables": info.get("tables", 0),
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
        columns=["exchange", "report", "component", "date", "symbol", "table", "field", "value", "multiplier",
                 "multiplier_basis", "shares", "sha256"])
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
    p.add_argument("--dates", default="", help="YYYY-MM-DD,... (pilot: keep it short)")
    p.add_argument("--reparse", default="", help="probe dir: re-parse saved raw responses, no network")
    p.add_argument("--discover-tpex", action="store_true", help="save TPEx OpenAPI spec + site scripts, list API paths")
    p.add_argument("--out-dir", default="/content/tw_lake_official_volume")
    p.add_argument("--only", default="", help="comma separated report ids to probe")
    a = p.parse_args(argv)
    if a.reparse:
        out = reparse(Path(a.reparse))
        print(pd.read_csv(out / "report_notes.csv").drop_duplicates("report").to_string(index=False))
        print(f"reparse written to {out}")
        return 0
    if a.discover_tpex:
        out = discover_tpex(Path(a.out_dir))
        pg = pd.read_csv(out / "pages.csv")
        print(f"menu items: {len(pd.read_csv(out / 'menu_items.csv'))}, matching pages: {len(pg)}")
        print(pg[["title", "url", "actions", "script_actions"]].to_string(index=False) if len(pg) else "(no matching pages)")
        print(f"discovery written to {out}")
        return 0
    dates = [date.fromisoformat(x.strip()) for x in a.dates.split(",") if x.strip()]
    eps = [e for e in ENDPOINTS if not a.only or e.report in set(a.only.split(","))]
    out = probe(dates, Path(a.out_dir), eps)
    print(pd.read_csv(out / "availability.csv").to_string(index=False))
    print(f"probe written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
