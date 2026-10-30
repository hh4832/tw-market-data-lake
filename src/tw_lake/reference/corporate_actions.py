"""Official corporate-action and listing reference data (B3 / B5) - discover, probe, resumable fetch, parse.

    python -m tw_lake.reference.corporate_actions discover --out-dir /content/tw_lake_ca
    python -m tw_lake.reference.corporate_actions probe    --out-dir /content/tw_lake_ca [--start 2025-06-01 --end 2025-06-30]
    python -m tw_lake.reference.corporate_actions fetch    --dataset-root .../reference/corporate_actions/v1 \
        --work-dir /content/tw_lake_ca_work --reports TWSE_TWT49U,... [--start 2023-05-01] [--max-minutes 55]
    python -m tw_lake.reference.corporate_actions status   --dataset-root ...
    python -m tw_lake.reference.corporate_actions parse    --dataset-root ... --out-dir /content/tw_lake_ca_parsed

Why three steps. Exchange web-report names and parameters change over time and could not be checked from the
development sandbox, so every endpoint below is a CANDIDATE:
  discover  reads the exchange pages / TPEx menu and lists the API actions the sites themselves use;
  probe     requests every candidate once for a short test window (default 2025-06, which contains many
            ex-dividend dates and the 0050 split) and reports http status, JSON stat, tables, fields, rows;
  fetch     requests only the reports chosen after the probe, month by month from 2023-05, resumable.
Nothing is parsed into events or adjustment factors here: ``parse`` only flattens the saved tables into a long
table (report, period, table, row, field, value) plus one wide table per report, so the field semantics can be
reviewed before any rule uses them.

Layout (dataset root is a new reference dataset; never inside a lake dataset):
    <root>/shards/report=<ID>/period=<YYYY-MM | snapshot_YYYY-MM-DD>/
        body.<json|html>.gz   response as received (gzip); sha256 in request.json is of the body
        request.json          url, params, http status, bytes, sha256, retrieved_at, known_at, stat, tables
        _SUCCESS              written last
    <root>/runs/<run_id>.json
known_at is conservative: the end of the requested period (month end) for range reports, the retrieval date for
current snapshots. Announcement dates inside the rows are the real known_at and are used by later audits.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import sys
import time
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from . import official_volume as OV

CA_VERSION = "0.2.0"
TWSE = "https://www.twse.com.tw/rwd/zh"
TWSE_WWW = "https://www.twse.com.tw"
TPEX = "https://www.tpex.org.tw/www/zh-tw"
TPEX_OLD = "https://www.tpex.org.tw/web/stock"
ISIN = "https://isin.twse.com.tw/isin/C_public.jsp"
FORBIDDEN_PARTS = ("fubon_5m", "fubon_1m", "finlab", "_published", "manifest", "official_volume")
RETRY_WAITS = (10.0, 30.0, 90.0)


@dataclass(frozen=True)
class Endpoint:
    id: str               # stable id used in paths
    exchange: str         # TWSE | TPEX | ISIN
    kind: str             # what we EXPECT it to contain; verified by probe, never assumed
    url: str
    params: tuple         # ((key, template), ...): {s8} {e8} YYYYMMDD, {sslash} {eslash} YYYY/MM/DD, {sroc} {eroc} ROC
    mode: str             # "range" (window, fetched per month) | "year" (one request per year) |
    #                       "day" (one request per weekday) | "snapshot" (current list, no history)
    fmt: str = "json"     # json | html
    pace_sec: float = 4.0
    timeout: float | None = None   # seconds; None = http_get default


def _rng(t: str, s: date, e: date) -> str:
    return t.format(s8=s.strftime("%Y%m%d"), e8=e.strftime("%Y%m%d"), sslash=s.strftime("%Y/%m/%d"),
                    eslash=e.strftime("%Y/%m/%d"), sroc=f"{s.year - 1911}/{s:%m}/{s:%d}",
                    eroc=f"{e.year - 1911}/{e:%m}/{e:%d}", y=str(s.year), yroc=str(s.year - 1911))


R = (("startDate", "{s8}"), ("endDate", "{e8}"), ("response", "json"))
RT = (("startDate", "{sslash}"), ("endDate", "{eslash}"), ("response", "json"))
# Probe 2026-10-30 (ca_probe_20261030_215933) + discover (ca_discovery_20261030_215635):
#   TWSE rwd endpoints below answered with tables; TWT48U and TWTAVU ignore the date window (current lists only);
#   TPEx new-site actions and their form fields were read from the saved TPEx pages (startDate/endDate in
#   YYYY/MM/DD for bulletin/*Rslt|revivt|exDailyQ; company/deListed and company/latest take date=<year>,
#   reason=-1; afterTrading/chtm takes one date). Entries marked "unverified" have not answered yet.
D = (("date", "{sslash}"), ("response", "json"))
CANDIDATES: list[Endpoint] = [
    # --- TWSE corporate actions (B3)
    Endpoint("TWSE_TWT49U", "TWSE", "ex_right_dividend_result", f"{TWSE}/exRight/TWT49U", R, "range"),
    Endpoint("TWSE_TWT48U", "TWSE", "ex_right_dividend_preannounce", f"{TWSE}/exRight/TWT48U", R, "range"),
    Endpoint("TWSE_TWT48U_now", "TWSE", "ex_right_dividend_preannounce", f"{TWSE}/exRight/TWT48U",
             (("response", "json"),), "snapshot"),
    Endpoint("TWSE_TWTAUU", "TWSE", "capital_reduction_resume_ref_price", f"{TWSE}/reducation/TWTAUU", R, "range"),
    Endpoint("TWSE_TWTAUU_b", "TWSE", "capital_reduction_resume_ref_price", f"{TWSE}/reduction/TWTAUU", R, "range"),
    Endpoint("TWSE_TWTAVU", "TWSE", "capital_reduction_preannounce", f"{TWSE}/reducation/TWTAVU", R, "range"),
    Endpoint("TWSE_TWTAVU_now", "TWSE", "capital_reduction_preannounce", f"{TWSE}/reducation/TWTAVU",
             (("response", "json"),), "snapshot"),
    Endpoint("TWSE_TWTCAU", "TWSE", "etf_split_resume_ref_price", f"{TWSE}/split/TWTCAU", R, "range"),
    Endpoint("TWSE_TWTB8U", "TWSE", "par_value_change_resume_ref_price", f"{TWSE}/change/TWTB8U", R, "range"),
    # --- TWSE trading status / listing (B4, B5)
    Endpoint("TWSE_TWTAWU", "TWSE", "suspended_trading", f"{TWSE}/afterTrading/TWTAWU", R, "range"),
    Endpoint("TWSE_suspendListing", "TWSE", "delisted_companies", f"{TWSE}/company/suspendListing",
             (("response", "json"),), "snapshot"),
    Endpoint("TWSE_newlisting", "TWSE", "new_listings", f"{TWSE}/company/newlisting",
             (("response", "json"),), "snapshot"),
    # --- TPEx new site (actions and form fields from the saved pages)
    Endpoint("TPEX_exDailyQ_b", "TPEX", "ex_right_dividend_result", f"{TPEX}/bulletin/exDailyQ", RT, "range",
             pace_sec=2.5),
    Endpoint("TPEX_revivt_b", "TPEX", "capital_reduction_resume_ref_price", f"{TPEX}/bulletin/revivt", RT, "range",
             pace_sec=2.5),
    Endpoint("TPEX_pvChgRslt", "TPEX", "par_value_change_resume_ref_price", f"{TPEX}/bulletin/pvChgRslt", RT,
             "range", pace_sec=2.5),
    Endpoint("TPEX_etfSplitRslt", "TPEX", "etf_split_resume_ref_price", f"{TPEX}/bulletin/etfSplitRslt", RT, "range",
             pace_sec=2.5),
    Endpoint("TPEX_etfRvsRslt", "TPEX", "etf_reverse_split_resume_ref_price", f"{TPEX}/bulletin/etfRvsRslt", RT,
             "range", pace_sec=2.5),
    Endpoint("TPEX_prePost", "TPEX", "ex_right_dividend_preannounce", f"{TPEX}/bulletin/prePost",
             (("response", "json"),), "snapshot", pace_sec=2.5),
    Endpoint("TPEX_decap", "TPEX", "capital_reduction_preannounce", f"{TPEX}/bulletin/decap",
             (("response", "json"),), "snapshot", pace_sec=2.5),
    Endpoint("TPEX_sprc", "TPEX", "suspended_resumed_trading", f"{TPEX}/bulletin/sprc",
             (("response", "json"),), "snapshot", pace_sec=2.5),
    Endpoint("TPEX_deListed_y", "TPEX", "delisted_companies", f"{TPEX}/company/deListed",
             (("code", ""), ("date", "{y}"), ("reason", "-1"), ("response", "json")), "year", pace_sec=2.5),
    Endpoint("TPEX_deListed_yroc", "TPEX", "delisted_companies", f"{TPEX}/company/deListed",
             (("code", ""), ("date", "{yroc}"), ("reason", "-1"), ("response", "json")), "year", pace_sec=2.5),
    Endpoint("TPEX_latest_y", "TPEX", "new_listings", f"{TPEX}/company/latest",
             (("code", ""), ("date", "{y}"), ("response", "json")), "year", pace_sec=2.5),
    Endpoint("TPEX_latest_yroc", "TPEX", "new_listings", f"{TPEX}/company/latest",
             (("code", ""), ("date", "{yroc}"), ("response", "json")), "year", pace_sec=2.5),
    Endpoint("TPEX_chtm", "TPEX", "altered_trading_daily", f"{TPEX}/afterTrading/chtm", D, "day", pace_sec=2.5),
    # --- TPEx old site / earlier guesses
    Endpoint("TPEX_exDailyQ", "TPEX", "ex_right_dividend_result", f"{TPEX}/exright/dailyQuo", RT, "range", pace_sec=2.5),
    Endpoint("TPEX_exDailyQ_old", "TPEX", "ex_right_dividend_result",
             f"{TPEX_OLD}/exright/dailyquo/exDailyQ_result.php",
             (("l", "zh-tw"), ("d", "{sroc}"), ("ed", "{eroc}"), ("o", "json")), "range", pace_sec=2.5),
    Endpoint("TPEX_revivt", "TPEX", "capital_reduction_resume_ref_price", f"{TPEX}/exright/revivt", RT, "range",
             pace_sec=2.5),
    Endpoint("TPEX_revivt_old", "TPEX", "capital_reduction_resume_ref_price",
             f"{TPEX_OLD}/exright/revivt/revivt_result.php",
             (("l", "zh-tw"), ("d", "{sroc}"), ("ed", "{eroc}"), ("o", "json")), "range", pace_sec=2.5),
    # --- current listing masters with official listing dates (B5; current snapshot only, no delisted names)
    Endpoint("ISIN_TWSE", "ISIN", "listed_securities_twse", ISIN, (("strMode", "2"),), "snapshot", "html", 3.0, 180.0),
    Endpoint("ISIN_TPEX", "ISIN", "listed_securities_tpex", ISIN, (("strMode", "4"),), "snapshot", "html", 3.0, 180.0),
]
# fetched by default (`--reports recommended`); a report that fails three periods in a row is skipped, others go on
RECOMMENDED = [
    "TWSE_TWT49U", "TWSE_TWTAUU", "TWSE_TWTB8U", "TWSE_TWTCAU", "TWSE_TWTAWU",
    "TWSE_TWT48U_now", "TWSE_TWTAVU_now", "TWSE_newlisting", "TWSE_suspendListing",
    "TPEX_exDailyQ_b", "TPEX_exDailyQ_old", "TPEX_revivt_b", "TPEX_pvChgRslt", "TPEX_etfSplitRslt", "TPEX_etfRvsRslt",
    "TPEX_prePost", "TPEX_decap", "TPEX_sprc", "TPEX_deListed_y", "TPEX_deListed_yroc", "TPEX_latest_y",
    "TPEX_latest_yroc", "ISIN_TWSE", "ISIN_TPEX", "TPEX_chtm",
]
BY_ID = {e.id: e for e in CANDIDATES}

# pages whose HTML / scripts name the API actions (discover)
TWSE_PAGES = [
    "/zh/announcement/ex-right/twt48u.html", "/zh/announcement/ex-right/twt49u.html",
    "/zh/announcement/reduction/twtauu.html", "/zh/announcement/reduction/twtavu-detail2.html",
    "/zh/announcement/split/twtcau.html", "/zh/page/trading/exchange/TWTAWU.html",
    "/zh/page/trading/exchange/TWTAUU.html", "/zh/page/trading/exchange/TWT49U.html",
    "/zh/listed/suspend-listing.html", "/zh/listed/listed-company/newlisting.html", "/zh/sitemap.html",
]
TPEX_KEYWORDS = ("除權", "除息", "減資", "分割", "面額", "暫停", "停止", "恢復", "終止", "下櫃", "新上櫃", "轉上市",
                 "上櫃公司", "參考價")
TWSE_ACTION_RE = re.compile(r"""(?:rwd/(?:zh|en)/)?\b((?:exRight|reducation|reduction|split|change|afterTrading|company|"""
                            r"""announcement|listed)/[A-Za-z0-9_]+)""")


def known_at_for(ep: Endpoint, end: date, retrieved: datetime) -> str:
    if ep.mode == "snapshot":
        return retrieved.isoformat(timespec="seconds")
    return min(f"{end.isoformat()}T23:59:59+08:00", retrieved.isoformat(timespec="seconds"))


def months(start: date, end: date) -> list[tuple[date, date]]:
    out, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        a = max(date(y, m, 1), start)
        b = min(date(y, m, monthrange(y, m)[1]), end)
        out.append((a, b))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _decode(body: bytes, fmt: str) -> str:
    for enc in (("utf-8", "big5", "cp950") if fmt == "json" else ("cp950", "big5", "utf-8")):
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def summarize(body: bytes, fmt: str) -> dict:
    """http-independent content summary: json?, stat, tables, rows, fields (first table), first row."""
    if fmt == "html":
        txt = _decode(body, "html")
        rows = len(re.findall(r"<tr", txt, flags=re.I))
        return {"json": False, "html": "<table" in txt.lower(), "stat": "", "tables": txt.lower().count("<table"),
                "rows": rows, "fields": "", "first_row": ""}
    try:
        obj = json.loads(_decode(body, "json"))
    except Exception:  # noqa: BLE001
        return {"json": False, "html": b"<html" in body[:500].lower(), "stat": "", "tables": 0, "rows": 0,
                "fields": "", "first_row": ""}
    tabs = OV._tables(obj)
    stat = str(obj.get("stat", obj.get("status", ""))) if isinstance(obj, dict) else ""
    first = next((t for t in tabs if t[2]), tabs[0] if tabs else None)
    return {"json": True, "html": False, "stat": stat[:80], "tables": len(tabs), "rows": sum(len(t[2]) for t in tabs),
            "fields": "|".join(first[1])[:400] if first else "",
            "first_row": json.dumps(first[2][0], ensure_ascii=False)[:400] if first and first[2] else ""}


BAD_STAT = ("錯誤", "error", "參數", "invalid")


def valid(ep: Endpoint, status: int, body: bytes) -> bool:
    """A response worth keeping as evidence: HTTP 200, the expected format, and no parameter-error stat
    (an empty period is valid)."""
    if status != 200:
        return False
    s = summarize(body, ep.fmt)
    if ep.fmt != "json":
        return s["html"]
    return s["json"] and not any(b in s["stat"].lower() for b in BAD_STAT)


def request(ep: Endpoint, s: date, e: date, getter=OV.http_get) -> tuple[dict, bytes]:
    params = {k: _rng(v, s, e) for k, v in ep.params}
    t0 = datetime.now(TAIPEI)
    status, body, ctype, final = getter(ep.url, params) if ep.timeout is None else getter(ep.url, params, ep.timeout)
    rec = {"report": ep.id, "exchange": ep.exchange, "kind": ep.kind, "mode": ep.mode, "start": s.isoformat(),
           "end": e.isoformat(), "url": ep.url, "params": json.dumps(params, ensure_ascii=False), "final_url": final,
           "http_status": status, "content_type": ctype, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
           "retrieved_at": t0.isoformat(timespec="seconds"), "known_at": known_at_for(ep, e, t0),
           **summarize(body, ep.fmt)}
    return rec, body


def retrying(getter=OV.http_get, sleep=time.sleep, waits=RETRY_WAITS, log=print):
    def get(ep: Endpoint, s: date, e: date):
        for i in range(len(waits) + 1):
            rec, body = request(ep, s, e, getter)
            if valid(ep, rec["http_status"], body) or i == len(waits):
                return rec, body
            log(f"  retry {ep.id} {s}..{e}: http={rec['http_status']} -> wait {waits[i]:.0f}s")
            sleep(waits[i])
    return get


def _by_host(eps: list[Endpoint]) -> dict[str, list[Endpoint]]:
    hosts: dict[str, list[Endpoint]] = {}
    for ep in eps:
        hosts.setdefault(ep.exchange, []).append(ep)
    return hosts


# ------------------------------------------------------------------------------------------------ discover
def discover(out_dir: Path, getter=OV.http_get, sleep=time.sleep, log=print) -> Path:
    out = Path(out_dir) / f"ca_discovery_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}"
    (out / "pages").mkdir(parents=True)
    rows, seen = [], {}
    for path in TWSE_PAGES:
        url = TWSE_WWW + path
        st, b, _, _ = getter(url, {})
        name = re.sub(r"[^A-Za-z0-9]+", "_", path)[:120]
        (out / "pages" / f"twse{name}").write_bytes(b)
        page = b.decode("utf-8", errors="replace")
        acts = set(TWSE_ACTION_RE.findall(page))
        for sc in sorted(set(re.findall(r'<script[^>]+src="([^"]+)"', page))):
            if "googletagmanager" in sc:
                continue
            u = sc if sc.startswith("http") else TWSE_WWW + (sc if sc.startswith("/") else "/" + sc)
            if u not in seen:
                s2, sb, _, _ = getter(u, {})
                seen[u] = set(TWSE_ACTION_RE.findall(sb.decode("utf-8", errors="replace"))) if s2 == 200 else set()
                sleep(0.5)
            acts |= seen[u]
        rows.append({"site": "TWSE", "title": "", "url": url, "http_status": st, "actions": "|".join(sorted(acts))})
        log(f"TWSE {path}: http={st} actions={len(acts)}")
        sleep(2.0)
    st, b, _, _ = getter(OV.TPEX_MENU, {})
    (out / "tpex_menu.json").write_bytes(b)
    try:
        items = list(OV._menu_items(json.loads(b.decode("utf-8", errors="replace"))))
    except Exception:  # noqa: BLE001
        items = []
    items = [(t, u if u.startswith("http") else "https://www.tpex.org.tw" + (u if u.startswith("/") else "/" + u))
             for t, u in items]
    wanted = [(t, u) for t, u in dict.fromkeys(items) if any(k in t for k in TPEX_KEYWORDS) and u.endswith(".html")]
    for title, href in wanted[:80]:
        st, b, _, _ = getter(href, {})
        page = b.decode("utf-8", errors="replace")
        (out / "pages" / ("tpex_" + re.sub(r"[^A-Za-z0-9]+", "_", href.split("tpex.org.tw/")[-1])[:120])).write_bytes(b)
        acts = set(OV._actions(page))
        for sc in sorted({x for x in re.findall(r'<script[^>]+src="([^"]+)"', page)
                          if "rsrc/asset" not in x and "googletagmanager" not in x and "cloudflare" not in x}):
            u = sc if sc.startswith("http") else "https://www.tpex.org.tw" + sc
            if u not in seen:
                s2, sb, _, _ = getter(u, {})
                seen[u] = set(OV._actions(sb.decode("utf-8", errors="replace"))) if s2 == 200 else set()
                sleep(0.5)
            acts |= seen[u]
        rows.append({"site": "TPEX", "title": title, "url": href, "http_status": st, "actions": "|".join(sorted(acts))})
        log(f"TPEX {title}: http={st} actions={len(acts)}")
        sleep(1.5)
    pd.DataFrame(rows).to_csv(out / "discovered_actions.csv", index=False, encoding="utf-8-sig")
    return out


# ------------------------------------------------------------------------------------------------ probe
def probe(out_dir: Path, start: date, end: date, ids: list[str] | None = None, getter=OV.http_get,
          sleep=time.sleep, log=print) -> Path:
    eps = [BY_ID[i] for i in ids] if ids else CANDIDATES
    out = Path(out_dir) / f"ca_probe_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}"
    (out / "raw").mkdir(parents=True)
    recs: list[dict] = []

    def host(lst: list[Endpoint]) -> None:
        for ep in lst:
            rec, body = request(ep, start, end, getter)
            (out / "raw" / f"{ep.id}.{ep.fmt}.gz").write_bytes(gzip.compress(body, mtime=0))
            recs.append(rec)
            log(f"{ep.id}: http={rec['http_status']} json={rec['json']} stat={rec['stat'][:30]!r} "
                f"tables={rec['tables']} rows={rec['rows']} fields={rec['fields'][:100]}")
            sleep(ep.pace_sec)

    with ThreadPoolExecutor(max_workers=3) as pool:
        for f in [pool.submit(host, lst) for lst in _by_host(eps).values()]:
            f.result()
    df = pd.DataFrame(recs).sort_values("report")
    df.to_csv(out / "probe_summary.csv", index=False, encoding="utf-8-sig")
    (out / "probe_meta.json").write_text(json.dumps({"ca_version": CA_VERSION, "git_sha": git_sha(),
                                                     "window": [start.isoformat(), end.isoformat()],
                                                     "endpoints": [asdict(e) for e in eps]},
                                                    ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ------------------------------------------------------------------------------------------------ fetch
def _guard(root: Path) -> None:
    parts = {x.lower() for x in Path(root).resolve().parts}
    if any(f in parts for f in FORBIDDEN_PARTS):
        raise SystemExit(f"refusing to write the reference dataset inside another dataset folder: {root}")


def shard(root: Path, ep: Endpoint, period: str) -> Path:
    return Path(root) / "shards" / f"report={ep.id}" / f"period={period}"


def _done(sd: Path) -> bool:
    return all((sd / f).exists() for f in ("_SUCCESS", "request.json")) and any(sd.glob("body.*.gz"))


def plan(ep: Endpoint, start: date, end: date, today: date) -> list[tuple[str, date, date]]:
    if ep.mode == "snapshot":
        return [(f"snapshot_{today.isoformat()}", today, today)]
    if ep.mode == "year":
        return [(f"{y}", date(y, 1, 1), min(date(y, 12, 31), end)) for y in range(start.year, end.year + 1)]
    if ep.mode == "day":
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        return [(d.isoformat(), d, d) for d in days if d.weekday() < 5]
    return [(f"{a:%Y-%m}", a, b) for a, b in months(start, end)]


def period_name(ep: Endpoint, period: str, b: date, today: date) -> str:
    """Periods that are not finished yet are stored under a dated name, so later runs add a newer copy."""
    if ep.mode in ("range", "year") and b >= today:
        return f"{period}_asof_{today.isoformat()}"
    return period


def fetch(root: Path, work_dir: Path, ids: list[str], start: date, end: date, max_minutes: float = 55.0,
          max_failed_in_row: int = 3, getter=OV.http_get, sleep=time.sleep, log=print, today: date | None = None) -> dict:
    root, work_dir = Path(root), Path(work_dir)
    _guard(root)
    today = today or datetime.now(TAIPEI).date()
    t0 = time.monotonic()
    run_id = f"fetch_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}"
    g = retrying(getter, sleep, log=log)
    eps = [BY_ID[i] for i in ids]
    todo = {ep.exchange: [] for ep in eps}
    for ep in eps:
        for period, a, b in plan(ep, start, end, today):
            period = period_name(ep, period, b, today)
            if not _done(shard(root, ep, period)):
                todo[ep.exchange].append((ep, period, a, b))
    rec = {"run_id": run_id, "ca_version": CA_VERSION, "git_sha": git_sha(), "reports": ids,
           "window": [start.isoformat(), end.isoformat()],
           "started_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
           "todo_at_start": sum(len(v) for v in todo.values()), "done": [], "failed": [], "stopped": ""}

    def host(items):
        failed: dict[str, int] = {}
        for ep, period, a, b in items:
            if (time.monotonic() - t0) / 60 > max_minutes:
                rec["stopped"] = rec["stopped"] or f"time budget {max_minutes} min"
                return
            if failed.get(ep.id, 0) >= max_failed_in_row:
                continue                                   # this report is skipped; the others go on
            r, body = g(ep, a, b)
            local = work_dir / f"report={ep.id}" / f"period={period}"
            if local.exists():
                shutil.rmtree(local)
            local.mkdir(parents=True)
            (local / f"body.{ep.fmt}.gz").write_bytes(gzip.compress(body, mtime=0))
            (local / "request.json").write_text(json.dumps(r, ensure_ascii=False, indent=2), encoding="utf-8")
            dest = shard(root, ep, period)
            if dest.exists():
                shutil.rmtree(dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(local, dest)
            shutil.rmtree(local)
            ok = valid(ep, r["http_status"], body)
            if ok:
                (dest / "_SUCCESS").write_text(json.dumps({"finished_at": datetime.now(TAIPEI).isoformat(
                    timespec="seconds"), "git_sha": git_sha(), "ca_version": CA_VERSION}), encoding="utf-8")
                rec["done"].append(f"{ep.id}/{period}")
                failed[ep.id] = 0
            else:
                rec["failed"].append({"report": ep.id, "period": period, "http": r["http_status"], "stat": r["stat"]})
                failed[ep.id] = failed.get(ep.id, 0) + 1
            log(f"{ep.id} {period}: http={r['http_status']} stat={r['stat'][:30]!r} rows={r['rows']} "
                f"{'ok' if ok else 'FAILED'}")
            if failed.get(ep.id, 0) == max_failed_in_row:
                rec.setdefault("skipped_reports", []).append(ep.id)
                log(f"{ep.id}: {max_failed_in_row} failures in a row - report skipped for this run")
            sleep(ep.pace_sec)

    with ThreadPoolExecutor(max_workers=max(1, len(todo))) as pool:
        for f in [pool.submit(host, items) for items in todo.values()]:
            f.result()
    rec["finished_at"] = datetime.now(TAIPEI).isoformat(timespec="seconds")
    (root / "runs").mkdir(parents=True, exist_ok=True)
    (root / "runs" / f"{run_id}.json").write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    return rec


def status(root: Path, ids: list[str], start: date, end: date, today: date | None = None) -> pd.DataFrame:
    today = today or datetime.now(TAIPEI).date()
    rows = []
    for i in ids:
        ep = BY_ID[i]
        periods = plan(ep, start, end, today)
        done = sum(_done(shard(root, ep, period_name(ep, p, b, today))) for p, _a, b in periods)
        rows.append({"report": i, "periods": len(periods), "done": done, "remaining": len(periods) - done})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------------------------ parse
def _html_rows(txt: str) -> list[list[str]]:
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", txt, flags=re.I | re.S):
        cells = [re.sub(r"<[^>]+>", "", c).replace("&nbsp;", " ").replace("　", " ").strip()
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)]
        if cells:
            out.append(cells)
    return out


def flatten(report: str, period: str, fmt: str, body: bytes, sha: str) -> pd.DataFrame:
    """Saved response -> long rows (report, period, table, row, col, field, value). No interpretation."""
    recs = []
    if fmt == "html":
        rows = _html_rows(_decode(body, "html"))
        header = next((r for r in rows if len(r) >= 4), [])
        for ri, r in enumerate(rows):
            if r is header:
                continue
            for ci, v in enumerate(r):
                recs.append((report, period, "html", ri, ci, header[ci] if ci < len(header) else f"c{ci}", v, sha))
    else:
        try:
            obj = json.loads(_decode(body, "json"))
        except Exception:  # noqa: BLE001
            obj = {}
        for ti, (title, fields, rows) in enumerate(OV._tables(obj)):
            for ri, r in enumerate(rows):
                if not isinstance(r, list):
                    continue
                for ci, v in enumerate(r):
                    recs.append((report, period, f"{ti}:{title}"[:80], ri, ci,
                                 fields[ci] if ci < len(fields) else f"c{ci}", str(v), sha))
    return pd.DataFrame(recs, columns=["report", "period", "table", "row", "col", "field", "value", "sha256"])


def parse(root: Path, out_dir: Path) -> Path:
    out = Path(out_dir) / f"ca_parsed_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    out.mkdir(parents=True)
    longs, reqs = [], []
    for sd in sorted((Path(root) / "shards").glob("report=*/period=*")):
        if not _done(sd):
            continue
        r = json.loads((sd / "request.json").read_text(encoding="utf-8"))
        reqs.append(r)
        bf = next(sd.glob("body.*.gz"))
        fmt = "html" if ".html." in bf.name else "json"
        body = gzip.decompress(bf.read_bytes())
        longs.append(flatten(r["report"], sd.name.split("=", 1)[1], fmt, body, r["sha256"]))
    lg = pd.concat(longs, ignore_index=True) if longs else pd.DataFrame(
        columns=["report", "period", "table", "row", "col", "field", "value", "sha256"])
    lg.to_parquet(out / "rows_long.parquet", index=False)
    pd.DataFrame(reqs).to_csv(out / "requests.csv", index=False, encoding="utf-8-sig")
    summ = lg.groupby(["report", "table"]).agg(periods=("period", "nunique"),
                                              rows=("row", lambda s: int(s.shape[0])),
                                              fields=("field", lambda s: "|".join(dict.fromkeys(s))[:500])
                                              ).reset_index() if len(lg) else pd.DataFrame()
    summ.to_csv(out / "tables_summary.csv", index=False, encoding="utf-8-sig")
    for (rep, tab), g in lg.groupby(["report", "table"]) if len(lg) else []:
        w = g.pivot_table(index=["period", "row", "sha256"], columns="field", values="value", aggfunc="first")
        w.reset_index().to_csv(out / f"wide_{rep}_{re.sub(r'[^A-Za-z0-9]+', '_', tab)[:40]}.csv", index=False,
                               encoding="utf-8-sig")
    (out / "parse_meta.json").write_text(json.dumps({"ca_version": CA_VERSION, "git_sha": git_sha(),
                                                     "root": str(root), "shards": len(reqs),
                                                     "statement": "flattened as received; no field interpretation"},
                                                    ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ------------------------------------------------------------------------------------------------ cli
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tw_lake.reference.corporate_actions")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("discover")
    p.add_argument("--out-dir", required=True)
    p = sub.add_parser("probe")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--start", default="2025-06-01")
    p.add_argument("--end", default="2025-06-30")
    p.add_argument("--reports", default="", help="comma-separated ids, 'recommended', or empty for all")
    for name in ("fetch", "status"):
        p = sub.add_parser(name)
        p.add_argument("--dataset-root", required=True)
        p.add_argument("--reports", default="recommended",
                       help="comma-separated endpoint ids, or 'recommended' (see `list`)")
        p.add_argument("--start", default="2023-05-01")
        p.add_argument("--end", default="")
        if name == "fetch":
            p.add_argument("--work-dir", required=True)
            p.add_argument("--max-minutes", type=float, default=55.0)
    p = sub.add_parser("parse")
    p.add_argument("--dataset-root", required=True)
    p.add_argument("--out-dir", required=True)
    p = sub.add_parser("list")
    a = ap.parse_args(argv)
    if a.cmd == "list":
        t = pd.DataFrame([asdict(e) for e in CANDIDATES])[["id", "exchange", "kind", "mode", "url"]]
        t["recommended"] = t["id"].isin(RECOMMENDED)
        print(t.to_string())
        return 0
    if a.cmd == "discover":
        print("written:", discover(Path(a.out_dir)))
        return 0
    if a.cmd == "probe":
        ids = RECOMMENDED if a.reports == "recommended" else ([x for x in a.reports.split(",") if x] or None)
        print("written:", probe(Path(a.out_dir), date.fromisoformat(a.start), date.fromisoformat(a.end), ids))
        return 0
    if a.cmd == "parse":
        print("written:", parse(Path(a.dataset_root), Path(a.out_dir)))
        return 0
    ids = RECOMMENDED if a.reports == "recommended" else [x for x in a.reports.split(",") if x]
    unknown = [i for i in ids if i not in BY_ID]
    if unknown:
        raise SystemExit(f"unknown report ids: {unknown}; see `list`")
    end = date.fromisoformat(a.end) if a.end else datetime.now(TAIPEI).date() - timedelta(days=0)
    if a.cmd == "status":
        print(status(Path(a.dataset_root), ids, date.fromisoformat(a.start), end).to_string(index=False))
        return 0
    rec = fetch(Path(a.dataset_root), Path(a.work_dir), ids, date.fromisoformat(a.start), end, a.max_minutes)
    summary = {k: (len(v) if k in ("done", "failed") else v) for k, v in rec.items()}
    print(json.dumps(summary, ensure_ascii=False))
    print(status(Path(a.dataset_root), ids, date.fromisoformat(a.start), end).to_string(index=False))
    return 0 if not rec.get("skipped_reports") else 1


if __name__ == "__main__":
    sys.exit(main())
