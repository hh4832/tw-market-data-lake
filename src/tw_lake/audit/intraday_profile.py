"""Where in the day is regular-session volume missing from the provider's intraday bars? (read-only)

python -m tw_lake.audit.intraday_profile --data-root ... --probe-dir .../official_volume_probe_... \
    --recon-dir .../volume_recon_... --out-dir /content/tw_lake_intraday_profile

Uses TWSE MI_5MINS (market-wide cumulative trade volume every 5 seconds, regular session) saved by the
official-volume probe and the stored Fubon 5m bars of all TWSE symbols in the reconciliation scope.
For each probe date:
    official_bucket(t)  = cum(before t + 5 min) - cum(before t)          (t = 09:00, 09:05, ... 13:25)
    official_close      = cum(last row) - cum(before 13:30)              (13:30 bar: closing auction + later)
    after_1330          = cum(last row) - cum(at 13:30:00), and the timestamps of rows after 13:30:00
    ("before x" = the last snapshot stamped strictly earlier than x, matching bars [t, t + 5 min).)
    fubon_bucket(t)     = sum of stored 5m volume of the TWSE symbols for bar t (13:30 bar = close auction)
The ratio fubon / official per bucket is reported (MI_5MINS covers every TWSE security, the lake only
stocks and ETFs, so the level is < 1; what matters is where the ratio departs from its normal level on
the dates that do not reconcile). Units of MI_5MINS are recorded from the report, not assumed.
Writes only to its own output folder.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from ..clock import TAIPEI
from ..config import load_config
from ..paths import Layout
from ..reference.official_volume import read_raw
from ..run_context import git_sha
from .reader import load_published
from .runner import _guard_out_dir


def _num(v):
    s = re.sub(r"[^0-9.\-]", "", str(v))
    return float(s) if s not in ("", "-", ".") else np.nan


def parse_mi5mins(body: bytes) -> tuple[pd.DataFrame, dict]:
    obj = json.loads(body.decode("utf-8", errors="replace"))
    tables = []
    if isinstance(obj.get("tables"), list):
        tables = [t for t in obj["tables"] if isinstance(t, dict) and t.get("fields")]
    if obj.get("fields"):
        tables.append({"fields": obj["fields"], "data": obj.get("data", [])})
    for t in tables:
        f = [str(x) for x in t["fields"]]
        ti = next((i for i, x in enumerate(f) if "時間" in x), None)
        vi = next((i for i, x in enumerate(f) if "累積成交數量" in x or ("成交" in x and "數量" in x)), None)
        if ti is None or vi is None:
            continue
        rows = [(str(r[ti]).strip(), _num(r[vi])) for r in t.get("data", []) if len(r) > max(ti, vi)]
        df = pd.DataFrame(rows, columns=["time", "cum_volume"]).dropna()
        notes = " ".join(str(x) for x in (obj.get("notes") or []) + [obj.get("hints") or ""])
        return df, {"volume_field": f[vi], "fields": f, "notes": notes[:500]}
    return pd.DataFrame(columns=["time", "cum_volume"]), {"volume_field": None}


def official_buckets(m: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    m = m.copy()
    m["t"] = pd.to_datetime(m["time"], format="%H:%M:%S", errors="coerce")
    m = m.dropna(subset=["t"]).sort_values("t")
    def before(hhmm: str) -> float:            # cumulative volume of snapshots stamped strictly before hhmm
        v = m.loc[m["t"] < pd.to_datetime(hhmm, format="%H:%M"), "cum_volume"]
        return float(v.max()) if len(v) else 0.0
    rows = []
    for k in range(54):
        start = pd.Timestamp("1900-01-01 09:00") + pd.Timedelta(minutes=5 * k)
        if start.strftime("%H:%M") > "13:25":
            break
        end = (start + pd.Timedelta(minutes=5)).strftime("%H:%M")
        rows.append({"hhmm": start.strftime("%H:%M"), "official": before(end) - before(start.strftime("%H:%M"))})
    last = float(m["cum_volume"].iloc[-1]) if len(m) else np.nan
    rows.append({"hhmm": "13:30", "official": last - before("13:30")})
    c1330 = m.loc[m["t"] <= pd.to_datetime("13:30:00", format="%H:%M:%S"), "cum_volume"].max()
    after = m[m["t"] > pd.to_datetime("13:30:00", format="%H:%M:%S")]
    info = {"last_time": m["time"].iloc[-1] if len(m) else None, "cum_last": last,
            "after_1330_volume": float(last - c1330) if pd.notna(c1330) else None,
            "after_1330_rows": int(len(after)), "after_1330_times": "|".join(after["time"].head(20))}
    return pd.DataFrame(rows), info


def run_profile(cfg, probe_dir: Path, recon_dir: Path, out_root: Path, log=print) -> Path:
    layout = Layout.from_config(cfg)
    out = Path(out_root) / f"intraday_profile_{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
    _guard_out_dir(layout, out)
    out.mkdir(parents=True)
    rq = pd.read_parquet(Path(probe_dir) / "requests.parquet")
    rq = rq[(rq["report"] == "MI_5MINS") & (rq["http_status"] == 200)]
    recon = pd.read_parquet(Path(recon_dir) / "volume_recon_symbol_day.parquet")
    by_date = pd.read_csv(Path(recon_dir) / "identity_by_date.csv")
    verified = dict(zip(zip(by_date["exchange"], by_date["date"]), by_date["date_verified"]))
    twse = recon[recon["exchange"] == "TWSE"]
    pv5 = load_published("fubon_5m", layout.dataset_dirs["fubon_5m"])
    parts = pv5.partitions.set_index("partition")
    off_rows, fub_rows, infos = [], [], []
    for r in rq.itertuples(index=False):
        m, minfo = parse_mi5mins(read_raw(Path(probe_dir) / r.raw_file))
        if m.empty:
            infos.append({"date": r.date, "parsed": False, **minfo})
            continue
        ob, binfo = official_buckets(m)
        off_rows.append(ob.assign(date=r.date))
        syms = sorted(twse.loc[twse["date"] == r.date, "symbol"].unique())
        acc = {}
        for i, sym in enumerate(syms):
            part = f"symbol={sym}/year={r.date[:4]}"
            if part not in parts.index:
                continue
            df = pd.read_parquet(pv5.data_path(parts.at[part, "relpath"]), columns=["bar_start", "volume_raw"])
            d = df[df["bar_start"].dt.strftime("%Y-%m-%d") == r.date]
            for h, v in d.groupby(d["bar_start"].dt.strftime("%H:%M"))["volume_raw"].sum().items():
                acc[h] = acc.get(h, 0.0) + float(v)
            if i % 300 == 0:
                log(f"{r.date}: {i}/{len(syms)} symbols")
        fub_rows.append(pd.DataFrame({"hhmm": list(acc), "fubon_lots": list(acc.values())}).assign(date=r.date))
        infos.append({"date": r.date, "parsed": True, "verified": bool(verified.get(("TWSE", r.date), False)),
                      "symbols": len(syms), **minfo, **binfo})
    off = pd.concat(off_rows, ignore_index=True) if off_rows else pd.DataFrame(columns=["hhmm", "official", "date"])
    fub = pd.concat(fub_rows, ignore_index=True) if fub_rows else pd.DataFrame(columns=["hhmm", "fubon_lots", "date"])
    prof = off.merge(fub, on=["date", "hhmm"], how="outer").sort_values(["date", "hhmm"])
    prof["ratio_fubon_lots_per_official"] = prof["fubon_lots"] / prof["official"].where(prof["official"] > 0)
    med = prof[prof["hhmm"] < "13:30"].groupby("date")["ratio_fubon_lots_per_official"].median().rename("day_median_ratio")
    prof = prof.merge(med, on="date", how="left")
    prof["ratio_vs_day_median"] = prof["ratio_fubon_lots_per_official"] / prof["day_median_ratio"]
    prof["verified_date"] = prof["date"].map(lambda d: bool(verified.get(("TWSE", d), False)))
    prof.to_csv(out / "intraday_profile.csv", index=False, encoding="utf-8-sig")
    info = pd.DataFrame(infos)
    info.to_csv(out / "mi5mins_info.csv", index=False, encoding="utf-8-sig")
    close = prof[prof["hhmm"] == "13:30"][["date", "verified_date", "official", "fubon_lots", "ratio_vs_day_median"]]
    summ = {"git_sha": git_sha(), "probe_dir": str(probe_dir), "recon_dir": str(recon_dir),
            "mi5mins": infos, "close_bucket": close.to_dict("records"),
            "worst_buckets": prof.dropna(subset=["ratio_vs_day_median"]).sort_values("ratio_vs_day_median")
            .head(30)[["date", "hhmm", "verified_date", "official", "fubon_lots", "ratio_vs_day_median"]]
            .to_dict("records"),
            "statement": "Market-level localisation only; it does not change any symbol-day classification."}
    (out / "intraday_profile_summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=2, default=str),
                                                       encoding="utf-8")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.intraday_profile")
    p.add_argument("--config", default=None)
    p.add_argument("--data-root", default=None)
    p.add_argument("--probe-dir", required=True)
    p.add_argument("--recon-dir", required=True)
    p.add_argument("--out-dir", default="/content/tw_lake_intraday_profile")
    a = p.parse_args(argv)
    cfg = load_config(a.config, {"data_root": a.data_root} if a.data_root else None)
    out = run_profile(cfg, Path(a.probe_dir), Path(a.recon_dir), Path(a.out_dir))
    s = json.loads((out / "intraday_profile_summary.json").read_text(encoding="utf-8"))
    cols = ["date", "verified", "volume_field", "last_time", "after_1330_volume", "after_1330_rows", "after_1330_times"]
    print(pd.DataFrame(s["mi5mins"]).reindex(columns=cols).to_string(index=False))
    print("\nclosing bucket (13:30 bar vs official 13:25->end):")
    print(pd.DataFrame(s["close_bucket"]).to_string(index=False))
    print("\nbuckets furthest below the day's normal ratio:")
    print(pd.DataFrame(s["worst_buckets"]).head(15).to_string(index=False))
    print(f"profile written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
