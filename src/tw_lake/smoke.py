"""smoke mode: capability probes for FinLab and Fubon. Writes ONLY to <system>/smoke/<run_id>/."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from .errors import ApiError
from .finlab_source import FinlabSource, discover_datasets
from .fubon_source import FubonCaller, MarketClient, RateLimiter, RequestBudget, RetryPolicy, candle_params
from .jobs_finlab import FinlabResult
from .normalize import normalize_candles
from .run_context import RunContext


def _probe(caller: FubonCaller, client: MarketClient, params: dict) -> dict:
    try:
        resp = caller.call(client.historical_candles, params)
        rows = (resp or {}).get("data") or []
        return {"ok": True, "n_rows": len(rows), "rows": rows, "keys": sorted(rows[0].keys()) if rows else []}
    except ApiError as e:
        return {"ok": False, "error_type": type(e).__name__, "status_code": e.status_code, "error": str(e)[:300]}


def _minute_profile(rows: list[dict], sym: str, cfg, fin: FinlabResult, start: date, end: date) -> dict:
    f = cfg.fubon
    df = normalize_candles(rows, sym, "lot", f["timeframe"], "start")  # raw label, no shift
    if df.empty:
        return {"n_rows": 0}
    d = df["bar_start"].dt.date
    per_day = d.value_counts()
    first_t = df.groupby(d)["bar_start"].min().dt.strftime("%H:%M").value_counts().head(5).to_dict()
    last_t = df.groupby(d)["bar_start"].max().dt.strftime("%H:%M").value_counts().head(5).to_dict()
    exp_days = fin.calendar.symbol_trade_days_between(sym, start, end)
    have = set(d.unique())
    raw = fin.raw[(fin.raw["symbol"] == sym)].copy()
    raw["d"] = raw["date"].dt.date
    vol = df.groupby(d)["volume_raw"].sum()
    j = pd.DataFrame({"m": vol}).join(raw.set_index("d")["volume_shares"], how="inner")
    ratio = (j["m"] * 1000 / j["volume_shares"]).replace([float("inf")], pd.NA).dropna()
    return {
        "n_rows": int(len(df)),
        "first_bar": str(df["bar_start"].min()), "last_bar": str(df["bar_start"].max()),
        "requested_from": str(start), "requested_to": str(end),
        "expected_trade_days": len(exp_days), "days_with_bars": len(have),
        "missing_trade_days_first10": [x.isoformat() for x in exp_days if x not in have][:10],
        "bars_per_day_distribution": {int(k): int(v) for k, v in per_day.value_counts().sort_index().items()},
        "first_bar_time_counts": first_t, "last_bar_time_counts": last_t,
        "turnover_present_ratio": float(df["turnover"].notna().mean()),
        "average_present_ratio": float(df["avg_price_cum"].notna().mean()),
        "timestamp_samples": df["bar_ts_raw"].head(3).tolist(),
        "volume_x1000_vs_finlab_shares_ratio": {
            "median": float(ratio.median()) if len(ratio) else None,
            "p05": float(ratio.quantile(0.05)) if len(ratio) else None,
            "p95": float(ratio.quantile(0.95)) if len(ratio) else None},
    }


def run_smoke(rc: RunContext, finlab_src: FinlabSource, fin: FinlabResult, client: MarketClient | None) -> Path:
    cfg, f = rc.cfg, rc.cfg.fubon
    out = rc.layout.smoke / rc.run_id
    rc.layout.assert_writable(out)
    raw_dir = out / "raw_json"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rep: dict = {"run_id": rc.run_id, "finlab": {}, "fubon": {}, "recommendations": {}}

    with rc.stage("smoke: finlab"):
        r = fin.raw
        rep["finlab"]["raw_daily"] = {"rows": int(len(r)), "symbols": int(r["symbol"].nunique()),
                                      "min_date": str(r["date"].min().date()), "max_date": str(r["date"].max().date())}
        try:
            adj = finlab_src.adjusted("close")
            rep["finlab"]["adjusted_close"] = {"ok": True, "shape": list(adj.shape),
                                               "max_date": str(pd.to_datetime(adj.index).max().date())}
        except Exception as e:  # noqa: BLE001
            rep["finlab"]["adjusted_close"] = {"ok": False, "error": f"{type(e).__name__}: {e}"[:300]}
        cats = finlab_src.get("security_categories")
        rep["finlab"]["security_categories"] = {
            "shape": list(cats.shape), "columns": [str(c) for c in cats.columns],
            "market_counts": cats["market"].astype(str).value_counts().to_dict() if "market" in cats else None,
            "category_top30": cats["category"].astype(str).value_counts().head(30).to_dict() if "category" in cats else None}
        disc = discover_datasets(finlab_src, cfg.finlab["disposal_dataset_candidates"])
        rep["finlab"]["disposal_attention_candidates"] = {k: {kk: vv for kk, vv in v.items() if kk != "frame"}
                                                          for k, v in disc.items()}
        m = fin.master
        rep["finlab"]["minute_targets"] = {
            "n": int(len(fin.targets)), "by_type": fin.targets["instrument_type"].value_counts().to_dict(),
            "delisted_inferred": int((fin.targets["listing_status_latest"] == "delisted_inferred").sum()),
            "not_in_current_security_categories": int((~m["in_current_security_categories"].fillna(False)).sum())}

    if client is not None:
        with rc.stage("smoke: fubon capability probes"):
            caller = FubonCaller(RateLimiter(int(f["historical_requests_per_min"])),
                                 RetryPolicy(int(f["max_attempts_per_request"]), float(f["rate_limit_wait_sec"]),
                                             float(f["backoff_base_sec"]), float(f["backoff_max_sec"])),
                                 RequestBudget(60))
            latest = fin.calendar.latest_available_date(rc.clock, f["data_ready_hhmm"])
            start = latest - timedelta(days=360)
            fields = list(f["fields"])
            for sym in cfg.raw.get("smoke_symbols", ["2330"]):
                res = _probe(caller, client, candle_params(sym, start, latest, f["timeframe"], fields))
                if res.get("ok"):
                    (raw_dir / f"{sym}_360d.json").write_text(json.dumps(res["rows"][:2000], ensure_ascii=False))
                    res["profile"] = _minute_profile(res.pop("rows"), sym, cfg, fin, start, latest)
                rep["fubon"][f"{sym}_360d"] = res
                m1 = _probe(caller, client, candle_params(sym, latest - timedelta(days=30), latest, f["timeframe"], fields))
                m1.pop("rows", None)
                rep["fubon"][f"{sym}_30d"] = m1
            # turnover rejected? retry without it so the report still shows what works
            first = rep["fubon"].get(f"{cfg.raw['smoke_symbols'][0]}_360d", {})
            if not first.get("ok") and first.get("status_code") == 400:
                no_t = [x for x in fields if x != "turnover"]
                r2 = _probe(caller, client, candle_params(cfg.raw["smoke_symbols"][0], start, latest, f["timeframe"], no_t))
                r2.pop("rows", None)
                rep["fubon"]["retry_without_turnover"] = r2
            # holiday window (Lunar New Year 2024-02-08..14 had no trading)
            hol = _probe(caller, client, candle_params(cfg.raw["smoke_symbols"][0], "2024-02-08", "2024-02-14",
                                                       f["timeframe"], fields))
            hol.pop("rows", None)
            rep["fubon"]["holiday_window_2024-02-08_14"] = hol
            # a delisted symbol from the target list
            dl = fin.targets[(fin.targets["listing_status_latest"] == "delisted_inferred")]
            if len(dl):
                s = dl.sort_values("last_price_date").iloc[len(dl) // 2]
                lp = s["last_price_date"].date()
                r3 = _probe(caller, client, candle_params(s["symbol"], lp - timedelta(days=30), lp, f["timeframe"], fields))
                r3.pop("rows", None)
                rep["fubon"]["delisted_probe"] = {"symbol": s["symbol"], "last_price_date": str(lp), **r3}

            # recommendations
            p = rep["fubon"].get(f"{cfg.raw['smoke_symbols'][0]}_360d", {}).get("profile", {})
            if p.get("n_rows"):
                truncated = p["days_with_bars"] < 0.95 * p["expected_trade_days"]
                rep["recommendations"]["row_cap_suspected"] = bool(truncated)
                rep["recommendations"]["window_months"] = 1 if truncated else int(f["window_months"])
                ft = max(p["first_bar_time_counts"], key=p["first_bar_time_counts"].get)
                rep["recommendations"]["bar_label"] = "start" if ft == "09:00" else ("end" if ft == "09:05" else "check")
                rep["recommendations"]["turnover_in_minute_bars"] = p["turnover_present_ratio"] > 0
            rep["fubon"]["requests_used"] = caller.budget.used

    (out / "capability_report.json").write_text(json.dumps(rep, ensure_ascii=False, indent=2, default=str),
                                                encoding="utf-8")
    rc.info["smoke_report"] = str(out / "capability_report.json")
    rc.info["smoke_recommendations"] = rep["recommendations"]
    rc.log.info("smoke recommendations: %s", json.dumps(rep["recommendations"], ensure_ascii=False))
    return out
