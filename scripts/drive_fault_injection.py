"""Google Drive (Colab FUSE) fault-injection tests - ONLY against a separate test folder.

Never point --root/--data-root at the production DATA_ROOT; the script refuses paths that contain
an existing ``fubon_stock_5m/_manifest/published.json`` unless it was created by this script
(marker file ``.fault_injection_root``). No market-data API is called: the pipeline runs use the
offline fakes from tests/conftest.py.

Subcommands (run from the repo root; see notebooks/TW_Market_Data_Lake_Fault_Injection.ipynb):
  write-protocols  write N parquet files with each protocol, record expected size/SHA256 locally
  check-protocols  after flush_and_unmount + mount: compare what Drive really stores
  pipeline         run the real ingestion (store/manifest/lock code) with fakes into the test root
  crash            start a pipeline run and SIGKILL it after S seconds
  two-writers      start two pipeline runs at once; exactly one may write
  stale-lock       plant a lock with an old heartbeat; must block unless --break-stale-lock
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tests"))
MARKER = ".fault_injection_root"
PROTOCOLS = ("rename_copy2", "direct_fsync")


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def guard_root(root: Path) -> Path:
    root = root.resolve()
    if (root / "fubon_stock_5m" / "_manifest" / "published.json").exists() and not (root / MARKER).exists():
        raise SystemExit(f"refusing: {root} looks like a production DATA_ROOT (no {MARKER} marker)")
    if "00Quant_Research/data" in str(root):
        raise SystemExit(f"refusing: {root} is inside the production data folder; use e.g. "
                         "/content/drive/MyDrive/00Quant_Research/_lake_fault_test")
    root.mkdir(parents=True, exist_ok=True)
    (root / MARKER).write_text("created by scripts/drive_fault_injection.py\n")
    return root


# ------------------------------------------------------------------------------- protocols
def _frame(i: int, rows: int) -> pd.DataFrame:
    rng = np.random.default_rng(i)
    ts = pd.date_range("2024-01-02 09:00", periods=rows, freq="5min", tz="Asia/Taipei")
    return pd.DataFrame({"bar_start": ts, "open": rng.random(rows), "high": rng.random(rows),
                         "low": rng.random(rows), "close": rng.random(rows), "volume_raw": rng.random(rows)})


def write_protocols(root: Path, n: int, label: str, expected: Path, tmp: Path) -> None:
    from tw_lake.store import _durable_write
    tmp.mkdir(parents=True, exist_ok=True)
    exp = {}
    for i in range(n):
        local = tmp / f"f{i:04d}.parquet"
        _frame(i, 2000 + 37 * i).to_parquet(local, index=False)
        h, size = sha(local), local.stat().st_size
        for proto in PROTOCOLS:
            final = root / label / proto / "data" / f"f{i:04d}.parquet"
            final.parent.mkdir(parents=True, exist_ok=True)
            if proto == "rename_copy2":                       # protocol used up to commit e04a034
                stg = root / label / proto / "staging" / final.name
                stg.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(local, stg)
                os.replace(stg, final)
            else:                                             # new protocol: exclusive write + fsync, no rename
                _durable_write(local, final)
            assert sha(final) == h                            # what the writer sees (local FUSE cache)
            exp[str(final)] = {"size": size, "sha256": h, "protocol": proto}
    expected.parent.mkdir(parents=True, exist_ok=True)
    expected.write_text(json.dumps(exp, indent=1))
    print(f"wrote {n} files x {len(PROTOCOLS)} protocols under {root / label}; expected -> {expected}")
    print("NOW: drive.flush_and_unmount(); drive.mount(...); then run check-protocols")


def check_protocols(expected: Path) -> int:
    exp = json.loads(expected.read_text())
    rows = []
    for path, e in exp.items():
        p = Path(path)
        if not p.exists():
            st = "missing"
        elif p.stat().st_size == 0:
            st = "zero_byte"
        elif p.stat().st_size != e["size"]:
            st = "size_mismatch"
        elif sha(p) != e["sha256"]:
            st = "sha_mismatch"
        else:
            st = "ok"
        rows.append({"protocol": e["protocol"], "status": st})
    t = pd.DataFrame(rows).value_counts().unstack(fill_value=0)
    print(t.to_string())
    bad_new = int(t.drop(columns=["ok"], errors="ignore").loc["direct_fsync"].sum()) if "direct_fsync" in t.index else -1
    print("RESULT new protocol:", "PASS" if bad_new == 0 else f"FAIL ({bad_new} bad files)")
    return 0 if bad_new == 0 else 1


# ------------------------------------------------------------------------------- pipeline
def run_pipeline(data_root: Path, mode: str, end: str, extra: list[str] | None = None) -> int:
    from conftest import FakeFinlab, FakeFubon
    from tw_lake import cli
    import yaml
    from tw_lake.config import load_config
    cfg = load_config(overrides={"data_root": str(data_root),
                                 "local_tmp": f"/tmp/tw_lake_fault_tmp/{os.getpid()}",
                                 "fubon": {"rate_limit_wait_sec": 0, "backoff_base_sec": 0, "backoff_max_sec": 0},
                                 "manifest": {"commit_every_symbols": 1,   # many commits -> more crash points
                                              "lock_stale_minutes": int(os.environ.get("TW_FAULT_STALE_MIN", "30"))}})
    cfg_path = Path(f"/tmp/tw_lake_fault_cfg_{os.getpid()}.yaml")
    cfg_path.write_text(yaml.safe_dump(cfg.raw, allow_unicode=True), encoding="utf-8")
    fin = FakeFinlab(end=date.fromisoformat(end))
    fub = FakeFubon(fin)
    slow = float(os.environ.get("TW_FAULT_SLOW_SEC", "0"))
    if slow:                                   # make the run long enough to overlap / be killed mid-write
        orig = fub.historical_candles
        fub.historical_candles = lambda params: (time.sleep(slow), orig(params))[1]
    argv = ["--mode", mode, "--config", str(cfg_path), "--data-root", str(data_root),
            "--symbols", "2330,0050,1234,9999,5555", *(extra or [])]
    return cli.main(argv, finlab_factory=lambda: fin, fubon_factory=lambda: fub,
                    sleep=lambda s: None, monotonic=lambda: 0.0)


def _spawn(data_root: Path, mode: str, end: str, extra=(), slow: float = 0.5) -> subprocess.Popen:
    return subprocess.Popen(env=dict(os.environ, TW_FAULT_SLOW_SEC=str(slow)), args=[sys.executable, __file__, "pipeline", "--data-root", str(data_root), "--mode", mode,
                             "--end", end, *([f"--extra={' '.join(extra)}"] if extra else [])],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def crash(data_root: Path, after: float) -> int:
    """Run on a FRESH test root: a pilot that is killed while writing data files / manifests."""
    p = _spawn(data_root, "pilot", "2024-03-15")
    time.sleep(after)
    p.send_signal(signal.SIGKILL)
    p.wait()
    print(f"killed pipeline after {after}s (exit {p.returncode}). Lock left behind:",
          (data_root / "_tw-market-data-lake" / "locks" / "writer.lock").exists())
    print("NOW: flush_and_unmount + mount, then `TW_FAULT_STALE_MIN=0 ... pipeline --mode pilot "
          "--extra=--break-stale-lock` (the killed run is dead), then verify --all")
    return 0


def two_writers(data_root: Path) -> int:
    """Run on a FRESH test root: two overlapping pilot runs."""
    a = _spawn(data_root, "pilot", "2024-03-15")
    time.sleep(3.0)
    b = _spawn(data_root, "pilot", "2024-03-15")
    out_a, out_b = a.communicate()[0], b.communicate()[0]
    blocked = sum("writer lock held" in o for o in (out_a, out_b))
    print(f"exit codes: {a.returncode}, {b.returncode}; blocked_by_lock: {blocked}")
    print("RESULT two writers:", "PASS (second writer refused)" if blocked == 1 else
          "FAIL/CHECK (inspect logs below)")
    if blocked != 1:
        print(out_a[-3000:], "\n-----\n", out_b[-3000:])
    return 0 if blocked == 1 else 1


def stale_lock(data_root: Path) -> int:
    lock = data_root / "_tw-market-data-lake" / "locks" / "writer.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    old = (datetime.now().astimezone() - timedelta(hours=3)).isoformat()
    lock.write_text(json.dumps({"run_id": "dead_run", "host": "x", "pid": 1, "started_at": old, "heartbeat_at": old}))
    rc1 = run_pipeline(data_root, "pilot", "2024-03-15")
    time.sleep(1.1)                                    # run ids have 1-second resolution
    rc2 = run_pipeline(data_root, "pilot", "2024-03-15", ["--break-stale-lock"])
    q = list((data_root / "_tw-market-data-lake" / "quarantine").rglob("stale_lock_dead_run_*"))
    print(f"without flag exit={rc1} (expect 1 blocked); with --break-stale-lock exit={rc2} (expect 0); "
          f"old lock quarantined: {bool(q)}")
    return 0 if (rc1 == 1 and rc2 == 0 and q) else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write-protocols"); w.add_argument("--root", required=True); w.add_argument("--n", type=int, default=200)
    w.add_argument("--label", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    w.add_argument("--expected", default="/content/fault_expected.json"); w.add_argument("--tmp", default="/tmp/tw_lake_fault_local")
    c = sub.add_parser("check-protocols"); c.add_argument("--expected", default="/content/fault_expected.json")
    p = sub.add_parser("pipeline"); p.add_argument("--data-root", required=True); p.add_argument("--mode", default="pilot")
    p.add_argument("--end", default="2024-03-15"); p.add_argument("--extra", default="")
    k = sub.add_parser("crash"); k.add_argument("--data-root", required=True); k.add_argument("--after", type=float, default=8.0)
    t = sub.add_parser("two-writers"); t.add_argument("--data-root", required=True)
    s = sub.add_parser("stale-lock"); s.add_argument("--data-root", required=True)
    a = ap.parse_args()
    if a.cmd == "write-protocols":
        write_protocols(guard_root(Path(a.root)), a.n, a.label, Path(a.expected), Path(a.tmp))
        return 0
    if a.cmd == "check-protocols":
        return check_protocols(Path(a.expected))
    root = guard_root(Path(a.data_root))
    if a.cmd == "pipeline":
        return run_pipeline(root, a.mode, a.end, a.extra.split() if a.extra else None)
    if a.cmd == "crash":
        return crash(root, a.after)
    if a.cmd == "two-writers":
        return two_writers(root)
    return stale_lock(root)


if __name__ == "__main__":
    sys.exit(main())
