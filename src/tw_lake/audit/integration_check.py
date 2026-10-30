"""B1 integration acceptance of a quality_mask run (read-only; writes only its own result file).

python -m tw_lake.audit.integration_check --qdir .../quality_acceptance/quality_mask_<ts>_<sha> \
    --data-root ... --expected-commit fbec159 --expected-seq 68 [--sample 300] [--out-dir ...]

Checks: SHA-256 of every output and input recorded in quality_manifest.json; code commit; fubon_5m seq in the mask
and the currently published pointer; recount of the tier totals; invariants (UNKNOWN never usable, quarantined
dates never usable, volume tier subset of price tier, ...); bar-level spot check of usable symbol-days against
the published 5m partitions. Verdict PASS only when every check passes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..clock import TAIPEI
from ..run_context import git_sha


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def integration_check(qdir, data_root=None, expected_commit=None, expected_seq=68, sample=300, seed=7):
    qdir = Path(qdir)
    res, fails = {}, []
    man = json.loads((qdir / "quality_manifest.json").read_text(encoding="utf-8"))
    # 1. SHA-256 of every output listed in the manifest
    bad = {f: (sha(qdir / f) if (qdir / f).exists() else "MISSING") for f, h in man["sha256"].items()
           if not (qdir / f).exists() or sha(qdir / f) != h}
    res["output_files_checked"] = len(man["sha256"])
    res["output_hash_mismatch"] = bad
    if bad:
        fails.append("output SHA-256 mismatch")
    # 2. input chain: inputs still exist with the recorded hashes
    ib = {k: v["path"] for k, v in man["inputs"].items() if isinstance(v, dict) and v.get("sha256")
          and (not Path(v["path"]).exists() or sha(v["path"]) != v["sha256"])}
    res["input_hash_mismatch"] = ib
    if ib:
        fails.append("input SHA-256 mismatch")
    # 3. commit + source manifest sequence
    res["code_commit"] = man["code_commit"]
    if expected_commit and not man["code_commit"].startswith(expected_commit):
        fails.append("commit differs")
    seqs = man["source_manifest_sequence"]["fubon_5m"]
    res["mask_fubon_5m_seq"] = seqs
    if expected_seq is not None and seqs != [expected_seq]:
        fails.append("mask seq differs")
    if data_root:
        ptr = Path(data_root) / "fubon_stock_5m" / "_manifest" / "published.json"
        cur = json.loads(ptr.read_text(encoding="utf-8")).get("seq") if ptr.exists() else None
        res["published_fubon_5m_seq_now"] = cur
        if expected_seq is not None and cur != expected_seq:
            fails.append(f"published fubon_5m seq is {cur}, mask built on {expected_seq}")
    # 4. recount + invariants
    m = pd.read_parquet(qdir / "quality_mask_symbol_day.parquet")
    rc = {"symbol_days": len(m), "price_backtest": int(m["can_use_for_price_backtest"].sum()),
          "volume_backtest": int(m["can_use_for_volume_backtest"].sum()), "level_A_strict": int(m["level_A_strict"].sum())}
    res["recount"] = rc
    for k, v in rc.items():
        if man["row_counts"].get(k) != v:
            fails.append(f"recount {k}: {v} != {man['row_counts'].get(k)}")
    p = m["can_use_for_price_backtest"]
    inv = {
        "unique_symbol_date": not m.duplicated(["symbol", "date"]).any(),
        "volume_subset_of_price": bool((~m["can_use_for_volume_backtest"] | p).all()),
        "levelA_subset_of_price": bool((~m["level_A_strict"] | p).all()),
        "price_requires_PASS": bool((~p | (m["price_quality_status"] == "PASS")).all()),
        "no_UNKNOWN_in_price": bool((~p | ((m["price_range_match"] == "TRUE") & (m["open_match"] == "TRUE")
                                          & (m["close_match"] == "TRUE"))).all()),
        "no_quarantined_in_price": bool((~p | (m["identity_status"] != "OVERCOUNT_PRESENT")).all()),
        "gap_ok_in_price": bool((~p | m["gap_evidence_status"].isin(
            ["NONE_MISSING", "VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH"])).all()),
        "normal_session_only": bool((~p | (m["session_type"] == "normal_template")).all()),
        "pit_in_price": bool((~p | m["pit_eligible"].fillna(False).astype(bool)).all()),
        "volume_exact": bool((~m["can_use_for_volume_backtest"] | (m["volume_reconciliation_status"] == "EXACT")).all()),
        "levelA_full_bars": bool((~m["level_A_strict"] | (m["missing_bar_count"] == 0)).all()),
        "single_evidence_version": m["evidence_version"].nunique() == 1,
    }
    res["invariants"] = inv
    fails += [f"invariant {k}" for k, ok in inv.items() if not ok]
    # 5. bar-level spot check against the published 5m partitions (sample of usable symbol-days)
    if data_root:
        from .reader import load_published
        pv = load_published("fubon_5m", Path(data_root) / "fubon_stock_5m")
        parts = pv.partitions.set_index("partition")
        s = m[p].sample(min(sample, int(p.sum())), random_state=seed)
        bad_bars = []
        for r in s.itertuples(index=False):
            part = f"symbol={r.symbol}/year={r.date[:4]}"
            df = pd.read_parquet(pv.data_path(parts.at[part, "relpath"]), columns=["bar_start", "volume_raw"])
            d = df[df["bar_start"].dt.strftime("%Y-%m-%d") == r.date]
            vol = float(d["volume_raw"].sum()) * 1000
            if len(d) != r.real_bar_count or abs(vol - r.fubon_intraday_volume) >= 1 \
                    or (r.volume_reconciliation_status == "EXACT" and abs(vol - r.official_regular_volume) >= 1):
                bad_bars.append((r.symbol, r.date, len(d), r.real_bar_count, vol, r.fubon_intraday_volume))
        res["bar_spot_check"] = {"sampled": len(s), "mismatch": bad_bars[:20], "n_mismatch": len(bad_bars)}
        if bad_bars:
            fails.append("bar spot check mismatch")
    res["verdict"] = "PASS" if not fails else "FAIL"
    res["failures"] = fails
    return res



def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tw_lake.audit.integration_check")
    p.add_argument("--qdir", required=True)
    p.add_argument("--data-root", default=None)
    p.add_argument("--expected-commit", default=None)
    p.add_argument("--expected-seq", type=int, default=68)
    p.add_argument("--sample", type=int, default=300)
    p.add_argument("--out-dir", default=None, help="folder for a new result sub-folder (never overwrites)")
    a = p.parse_args(argv)
    r = integration_check(a.qdir, a.data_root, a.expected_commit, a.expected_seq, a.sample)
    r["checker_commit"] = git_sha()
    if a.out_dir:
        out = Path(a.out_dir) / f"{datetime.now(TAIPEI):%Y%m%d_%H%M%S}_{git_sha()[:7]}"
        out.mkdir(parents=True, exist_ok=False)
        (out / "integration_check_result.json").write_text(json.dumps(r, ensure_ascii=False, indent=2, default=str),
                                                          encoding="utf-8")
        r["result_dir"] = str(out)
    print(json.dumps({k: v for k, v in r.items() if k != "invariants"}, ensure_ascii=False, indent=2, default=str))
    for k, ok in r["invariants"].items():
        print(("  OK   " if ok else "  FAIL ") + k)
    print(f"\n================  B1 integration check: {r['verdict']}  ================")
    if r["failures"]:
        print("failures:", r["failures"])
    return 0 if r["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
