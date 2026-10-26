"""API spot check - is a missing slot a pipeline (logic) loss or absent at the provider? Read-only.

python -m tw_lake.audit.api_spotcheck --data-root /content/drive/MyDrive/00Quant_Research/data \
    --ledger-dir /content/tw_lake_gap_ledger/gap_ledger_... --out-dir /content/tw_lake_spotcheck

Draws a stratified sample of symbol-days from a gap ledger, re-requests each day from Fubon
historical candles (one request per symbol-day, market-data endpoint only) and compares the
response with the stored bars slot by slot:
    api_only_slots     provider returns a bar we do not store  -> pipeline / logic loss
    stored_only_slots  we store a bar the provider no longer returns
    value_mismatch     same slot, different OHLCV
Every response is saved as JSON with its SHA-256 (request/response evidence for the ledger).
Never writes under DATA_ROOT; never calls trading or account APIs. Needs the Fubon login env vars
(Colab Secrets) and the Fubon SDK, like the runner.
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
from ..normalize import normalize_candles
from ..paths import Layout
from ..run_context import git_sha
from .reader import load_published
from .runner import _guard_out_dir

STRATA = {
    "liquid_with_missing": lambda d: d["liquidity_bucket"].isin(["ge5000_lots", "500_5000_lots"])
    & (d["missing_slots"] > 0) & (d["real_bars"] > 0),
    "shortfall_with_missing": lambda d: d["conservation"].isin(["shortfall"]) & (d["missing_slots"] > 0)
    & (d["real_bars"] > 0),
    "exact_with_missing": lambda d: (d["conservation"] == "exact") & (d["missing_slots"] > 0),
    "whole_day_missing": lambda d: d["day_status"].isin(["unresolved_whole_day", "provider_missing_evidenced"]),
    "control_complete": lambda d: d["day_status"] == "all_expected_slots_real",
}
VALUES = ["open", "high", "low", "close", "volume_raw"]


def draw_sample(days: pd.DataFrame, per_stratum: int, seed: int, extra: list[tuple[str, str]] = ()) -> pd.DataFrame:
    rows = []
    taken = set()
    for name, f in STRATA.items():
        c = days[f(days)]
        c = c.sample(min(per_stratum, len(c)), random_state=seed) if len(c) else c
        for r in c.itertuples(index=False):
            k = (r.symbol, pd.Timestamp(r.date).strftime("%Y-%m-%d"))
            if k not in taken:
                taken.add(k)
                rows.append({"symbol": k[0], "date": k[1], "stratum": name})
    for sym, d in extra:
        if (sym, d) not in taken:
            taken.add((sym, d))
            rows.append({"symbol": sym, "date": d, "stratum": "requested"})
    return pd.DataFrame(rows)


def compare_day(stored: pd.DataFrame, api: pd.DataFrame) -> dict:
    s = stored.assign(hhmm=stored["bar_start"].dt.strftime("%H:%M")).set_index("hhmm")
    a = api.assign(hhmm=api["bar_start"].dt.strftime("%H:%M")).set_index("hhmm")
    s = s[~s.index.duplicated()]
    a = a[~a.index.duplicated()]
    api_only = sorted(set(a.index) - set(s.index))
    stored_only = sorted(set(s.index) - set(a.index))
    both = sorted(set(a.index) & set(s.index))
    mism = []
    for h in both:
        for c in VALUES:
            x, y = s.at[h, c], a.at[h, c]
            if not ((pd.isna(x) and pd.isna(y)) or (pd.notna(x) and pd.notna(y) and abs(float(x) - float(y)) < 1e-9)):
                mism.append(f"{h}:{c}")
    return {"stored_bars": int(len(s)), "api_bars": int(len(a)), "api_only_slots": "|".join(api_only),
            "n_api_only": len(api_only), "stored_only_slots": "|".join(stored_only), "n_stored_only": len(stored_only),
            "value_mismatch": "|".join(mism[:50]), "n_value_mismatch": len(mism),
            "api_volume_raw": float(a["volume_raw"].sum()) if len(a) else 0.0,
            "stored_volume_raw": float(s["volume_raw"].sum()) if len(s) else 0.0}


def run_spotcheck(cfg, ledger_dir: Path, out_root: Path, client, per_stratum: int = 25, seed: int = 20261026,
                  extra: list[tuple[str, str]] = (), sleep=time.sleep) -> Path:
    layout = Layout.from_config(cfg)
    started = datetime.now(TAIPEI)
    out = Path(out_root) / f"spotcheck_{started:%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    _guard_out_dir(layout, out)
    (out / "responses").mkdir(parents=True)
    days = pd.read_parquet(Path(ledger_dir) / "gap_day_summary.parquet")
    sample = draw_sample(days, per_stratum, seed, extra)
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
        stored = pd.DataFrame(columns=["bar_start", *VALUES, "volume_unit"])
        if part in parts.index:
            if part not in cache:
                cache.clear()
                cache[part] = pd.read_parquet(pv5.data_path(parts.at[part, "relpath"]))
            p = cache[part]
            stored = p[p["bar_start"].dt.strftime("%Y-%m-%d") == s.date]
        unit = stored["volume_unit"].iloc[0] if len(stored) else "lot"
        params = candle_params(s.symbol, s.date, s.date, f["timeframe"], list(f["fields"]))
        status, rows_api, err = "ok", [], ""
        t_req = datetime.now(TAIPEI).isoformat(timespec="seconds")
        try:
            resp = caller.call(client.historical_candles, params)
            rows_api = (resp or {}).get("data") or [] if isinstance(resp, dict) else []
        except ApiNotFound as e:
            status, err = "not_found_404", str(e)[:200]
        except Exception as e:  # noqa: BLE001
            status, err = "error", repr(e)[:200]
        body = json.dumps({"params": params, "status": status, "data": rows_api}, ensure_ascii=False, sort_keys=True)
        h = hashlib.sha256(body.encode("utf-8")).hexdigest()
        (out / "responses" / f"{s.symbol}_{s.date}.json").write_text(body, encoding="utf-8")
        api = normalize_candles(rows_api, s.symbol, unit, f["timeframe"], f["bar_label"]) if rows_api else \
            stored.iloc[0:0]
        rec = {"symbol": s.symbol, "date": s.date, "stratum": s.stratum, "request_status": status, "error": err,
               "requested_at": t_req, "request_id": f"{out.name}:{i}", "response_hash": h,
               **compare_day(stored, api)}
        rec["verdict"] = ("logic_loss" if rec["n_api_only"] else
                          "value_mismatch" if rec["n_value_mismatch"] else
                          "stored_not_in_api" if rec["n_stored_only"] else
                          "request_failed" if status == "error" else "consistent")
        rows.append(rec)
        print(f"[{i}/{len(sample)}] {s.symbol} {s.date} {s.stratum}: {rec['verdict']} "
              f"(stored {rec['stored_bars']}, api {rec['api_bars']})", flush=True)
    res = pd.DataFrame(rows)
    res.to_parquet(out / "spotcheck.parquet", index=False)
    res.to_csv(out / "spotcheck.csv", index=False, encoding="utf-8-sig")
    summ = {"sample": int(len(res)), "by_stratum_verdict": res.groupby(["stratum", "verdict"]).size()
            .rename("n").reset_index().to_dict("records"),
            "logic_loss_days": int((res["verdict"] == "logic_loss").sum()),
            "api_only_slots_total": int(res["n_api_only"].sum()),
            "fubon_5m_seq": pv5.seq, "ledger_dir": str(ledger_dir), "git_sha": git_sha(),
            "statement": "consistent = Fubon today returns exactly what is stored; it is not evidence of no trade."}
    (out / "spotcheck_summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2, default=str),
                                                encoding="utf-8")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.api_spotcheck")
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--ledger-dir", required=True)
    p.add_argument("--out-dir", default="/content/tw_lake_spotcheck")
    p.add_argument("--per-stratum", type=int, default=25)
    p.add_argument("--extra", default="", help="SYMBOL:YYYY-MM-DD,... always included")
    a = p.parse_args(argv)
    cfg = load_config(a.config, {"data_root": a.data_root} if a.data_root else None)
    from ..fubon_source import RealFubonClient
    client = RealFubonClient.login_from_env()
    extra = [tuple(x.strip().split(":", 1)) for x in a.extra.split(",") if x.strip()]
    out = run_spotcheck(cfg, Path(a.ledger_dir), Path(a.out_dir), client, a.per_stratum, extra=extra)
    s = json.loads((out / "spotcheck_summary.json").read_text(encoding="utf-8"))
    print(json.dumps(s, ensure_ascii=False, indent=2))
    print(f"spot check written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
