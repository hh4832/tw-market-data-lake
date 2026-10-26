"""Official volume reference dataset - resumable batch fetch + full-period reconciliation.

    # fetch (run as many times as needed; finished dates are skipped)
    python -m tw_lake.reference.official_volume_batch fetch --ledger-dir .../gap_ledger_... \
        --dataset-root .../_tw-market-data-lake/reference/official_volume/v1 --max-minutes 55
    # progress
    python -m tw_lake.reference.official_volume_batch status --ledger-dir ... --dataset-root ...
    # reconcile every finished date against the gap ledger (no network)
    python -m tw_lake.reference.official_volume_batch recon --ledger-dir ... --dataset-root ... --out-dir ...

Layout (one shard per trading date, written locally first, then copied; `_SUCCESS` is written last, so a shard
without it is incomplete and is fetched again):
    <dataset-root>/shards/year=YYYY/date=YYYY-MM-DD/
        raw/<EXCHANGE>/<REPORT>/<date>.json.gz   response body (gzip); sha256 in requests.parquet is of the body
        requests.parquet                          url, params, status, sha256, retrieved_at, known_at, ...
        components.parquet                        symbol x report shares (main share field), the recon input
        unit_basis.csv                            how each value's unit was decided
        _SUCCESS                                  json: finished_at, git sha, endpoints
    <dataset-root>/runs/<run_id>.json             one record per fetch run
Requests: TWSE and TPEx run side by side (one thread per host); each host stays sequential with its pacing.
Failed requests (non-200, or non-JSON where JSON is required) are retried with back-off; a date that still
fails is not marked done; several failed dates in a row stop the run (likely rate limited).
Never writes under any lake dataset; never touches raw Parquet, manifests or published pointers.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha
from . import official_volume as OV

BATCH_VERSION = "1.0.0"
BATCH_REPORTS = {
    "TWSE": ["MI_INDEX_ALLBUT0999", "BFT41U", "TWT53U", "TWTC7U", "BFIAUU_ALL"],
    "TPEX": ["dailyQuotes_EW", "old_daily_close_quotes", "fixPricing", "oddQuote", "odd_Daily", "blockTrade_quote"],
}
JSON_OPTIONAL = {"old_daily_close_quotes"}          # fallback total; may legitimately be non-JSON
RETRY_WAITS = (10.0, 30.0, 90.0)
FORBIDDEN_PARTS = ("fubon_5m", "fubon_1m", "finlab", "_published", "manifest")


def batch_endpoints() -> list[OV.Endpoint]:
    by_id = {e.report: e for e in OV.ENDPOINTS}
    return [by_id[r] for ex in ("TWSE", "TPEX") for r in BATCH_REPORTS[ex]]


def _ok(report: str, status: int, body: bytes) -> bool:
    if status != 200:
        return False
    if report in JSON_OPTIONAL:
        return True
    head = body.lstrip()[:1]
    return head in (b"{", b"[")


def retrying_getter(getter=OV.http_get, sleep=time.sleep, waits=RETRY_WAITS, log=print):
    report_of = {e.url: e.report for e in batch_endpoints()}

    def get(url, params):
        rep = report_of.get(url, "")
        for i in range(len(waits) + 1):
            st, body, ctype, final = getter(url, params)
            if _ok(rep, st, body) or i == len(waits):
                return st, body, ctype, final
            log(f"  retry {rep} {params.get('date') or params.get('d')}: http={st} -> wait {waits[i]:.0f}s")
            sleep(waits[i])
    return get


def trading_dates(ledger_dir: Path, start: str | None = None, end: str | None = None) -> list[date]:
    d = pd.read_parquet(Path(ledger_dir) / "gap_day_summary.parquet", columns=["date"])["date"]
    ds = sorted({pd.Timestamp(x).date() for x in d.unique()})
    return [x for x in ds if (not start or x >= date.fromisoformat(start)) and (not end or x <= date.fromisoformat(end))]


def shard_dir(root: Path, d: date) -> Path:
    return Path(root) / "shards" / f"year={d.year}" / f"date={d.isoformat()}"


def _complete(sd: Path) -> bool:
    # _SUCCESS alone is not trusted (Drive may upload files out of order): the shard's tables must be there too
    return all((sd / f).exists() for f in ("_SUCCESS", "requests.parquet", "components.parquet"))


def done_dates(root: Path) -> set[date]:
    return {date.fromisoformat(p.parent.name.split("=", 1)[1])
            for p in (Path(root) / "shards").glob("year=*/date=*/_SUCCESS") if _complete(p.parent)}


def _guard(root: Path) -> None:
    parts = {x.lower() for x in Path(root).resolve().parts}
    if any(f in parts for f in FORBIDDEN_PARTS):
        raise SystemExit(f"refusing to write the reference dataset inside a lake dataset folder: {root}")


def _finish_shard(local: Path, d: date) -> tuple[bool, str]:
    rq = pd.read_parquet(local / "requests.parquet")
    bad = rq[[not _ok(r.report, r.http_status, b"{" if r.json else b"<") for r in rq.itertuples()]]
    pv = pd.read_parquet(local / "parsed_volume.parquet")
    from ..audit.volume_recon import component_table
    comp = component_table(pv) if len(pv) else pd.DataFrame(columns=["symbol", "date"])
    comp.to_parquet(local / "components.parquet", index=False)
    if len(pv) and "multiplier_basis" in pv.columns:
        pv.groupby(["report", "multiplier_basis"]).size().rename("n").reset_index() \
            .to_csv(local / "unit_basis.csv", index=False, encoding="utf-8-sig")
    (local / "parsed_volume.parquet").unlink()      # derivable from raw with reparse; keeps the dataset small
    if len(bad):
        return False, "failed: " + ",".join(f"{r.report}={r.http_status}" for r in bad.itertuples())
    return True, "ok"


def fetch(ledger_dir: Path, root: Path, work_dir: Path, max_minutes: float = 55.0, start: str | None = None,
          end: str | None = None, newest_first: bool = False, max_failed_in_row: int = 3,
          getter=OV.http_get, sleep=time.sleep, log=print) -> dict:
    root, work_dir = Path(root), Path(work_dir)
    _guard(root)
    t0 = time.monotonic()
    run_id = f"fetch_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}"
    todo = [d for d in trading_dates(ledger_dir, start, end) if d not in done_dates(root)]
    if newest_first:
        todo.reverse()
    eps = batch_endpoints()
    g = retrying_getter(getter, sleep, log=log)
    rec = {"run_id": run_id, "batch_version": BATCH_VERSION, "git_sha": git_sha(), "started_at":
           datetime.now(TAIPEI).isoformat(timespec="seconds"), "todo_at_start": len(todo), "done": [], "failed": []}
    failed_in_row = 0
    for i, d in enumerate(todo, 1):
        if (time.monotonic() - t0) / 60 > max_minutes:
            log(f"time budget reached ({max_minutes} min); stopping before {d}")
            break
        local = work_dir / f"date={d.isoformat()}"
        if local.exists():
            shutil.rmtree(local)
        OV.probe([d], work_dir, eps, getter=g, sleep=sleep, log=lambda *_a: None, out_path=local, compress=True)
        ok, why = _finish_shard(local, d)
        dest = shard_dir(root, d)
        if dest.exists():
            shutil.rmtree(dest)                     # an earlier unfinished copy (no _SUCCESS)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(local, dest)
        shutil.rmtree(local)
        if ok:
            (dest / "_SUCCESS").write_text(json.dumps({"finished_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                                                       "git_sha": git_sha(), "batch_version": BATCH_VERSION,
                                                       "reports": BATCH_REPORTS}, ensure_ascii=False), encoding="utf-8")
            rec["done"].append(d.isoformat())
            failed_in_row = 0
        else:
            rec["failed"].append({"date": d.isoformat(), "why": why})
            failed_in_row += 1
        el = (time.monotonic() - t0) / 60
        log(f"[{i}/{len(todo)}] {d} {why}  ({el:.1f} min, {el / i * 60:.0f} s/date)")
        if failed_in_row >= max_failed_in_row:
            log(f"{failed_in_row} dates failed in a row - stopping (rate limit or site change); rerun later")
            break
    rec["finished_at"] = datetime.now(TAIPEI).isoformat(timespec="seconds")
    (root / "runs").mkdir(parents=True, exist_ok=True)
    (root / "runs" / f"{run_id}.json").write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    return rec


def status(ledger_dir: Path, root: Path) -> dict:
    allds = trading_dates(ledger_dir)
    done = done_dates(root)
    left = [d for d in allds if d not in done]
    return {"trading_dates": len(allds), "done": len(done & set(allds)), "remaining": len(left),
            "first_remaining": left[0].isoformat() if left else None,
            "last_remaining": left[-1].isoformat() if left else None,
            "est_minutes_remaining": round(len(left) * 21 / 60, 1)}


def load_shards(root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    comps, reqs, ub = [], [], []
    for s in sorted((Path(root) / "shards").glob("year=*/date=*/_SUCCESS")):
        sd = s.parent
        if not _complete(sd):
            continue
        comps.append(pd.read_parquet(sd / "components.parquet"))
        r = pd.read_parquet(sd / "requests.parquet")
        r["raw_file"] = str(sd.relative_to(root)) + "/" + r["raw_file"].astype(str)
        reqs.append(r)
        if (sd / "unit_basis.csv").exists():
            ub.append(pd.read_csv(sd / "unit_basis.csv"))
    if not comps:
        raise SystemExit("no finished shards")
    return (pd.concat(comps, ignore_index=True), pd.concat(reqs, ignore_index=True),
            pd.concat(ub, ignore_index=True) if ub else None)


def recon(ledger_dir: Path, root: Path, out_dir: Path) -> Path:
    from ..audit.volume_recon import run_recon
    comp, reqs, ub = load_shards(root)
    return run_recon(None, Path(ledger_dir), Path(out_dir), comp=comp, reqs=reqs, unit_basis=ub)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.reference.official_volume_batch")
    p.add_argument("command", choices=["fetch", "status", "recon"])
    p.add_argument("--ledger-dir", required=True)
    p.add_argument("--dataset-root", required=True)
    p.add_argument("--work-dir", default="/content/tw_lake_official_volume_work")
    p.add_argument("--out-dir", default="/content/tw_lake_volume_recon")
    p.add_argument("--max-minutes", type=float, default=55.0)
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--newest-first", action="store_true")
    a = p.parse_args(argv)
    if a.command == "status":
        print(json.dumps(status(Path(a.ledger_dir), Path(a.dataset_root)), ensure_ascii=False, indent=2))
        return 0
    if a.command == "fetch":
        r = fetch(Path(a.ledger_dir), Path(a.dataset_root), Path(a.work_dir), a.max_minutes, a.start, a.end,
                  a.newest_first)
        print(f"run {r['run_id']}: done {len(r['done'])}, failed {len(r['failed'])}")
        print(json.dumps(status(Path(a.ledger_dir), Path(a.dataset_root)), ensure_ascii=False, indent=2))
        return 0
    out = recon(Path(a.ledger_dir), Path(a.dataset_root), Path(a.out_dir))
    print(f"recon written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
