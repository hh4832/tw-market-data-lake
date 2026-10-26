"""Gap Evidence Ledger v0 + API spot check on synthetic lakes. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd

from conftest import FakeFubon
from test_audit import build_lake, tree_snapshot
from tw_lake.audit import gap_ledger as GL
from tw_lake.audit.api_spotcheck import run_spotcheck


def ledger(cfg, tmp_path, **kw):
    out = GL.run_ledger(cfg, tmp_path / "ledger", template_symbols=("2330", "0050"), progress=False, **kw)
    s = json.loads((out / "gap_ledger_summary.json").read_text(encoding="utf-8"))["summary"]
    days = pd.read_parquet(out / "gap_day_summary.parquet")
    runs = pd.concat([pd.read_parquet(p) for p in sorted((out / "gap_runs").glob("*.parquet"))], ignore_index=True) \
        if list((out / "gap_runs").glob("*.parquet")) else pd.DataFrame(columns=GL.RUN_COLUMNS)
    return out, s, days.set_index(["symbol", "date"]), runs


def test_ledger_statuses_conservation_and_read_only(tmp_cfg, clock, tmp_path, monkeypatch):
    hole, gone = date(2024, 2, 20), date(2024, 2, 21)
    keep = {"09:00", "09:20", "09:40", "10:00", "10:20", "10:40", "11:00", "11:20", "11:40", "12:00", "12:20",
            "12:40", "13:00", "13:20", "13:30"}

    def setup(f):
        f.drop_bars.update({("0050", hole, "10:05"), ("0050", hole, "10:10"), ("2330", hole, "13:30")})
        f.missing_days["9999"] = {gone}
        f.only_bars[("5555", date(2023, 9, 5))] = keep

    build_lake(tmp_cfg, clock, setup)
    monkeypatch.setattr(GL, "PROVIDER_MISSING_DAYS", {("9999", "2024-02-21")})
    before = tree_snapshot(tmp_cfg.data_root)
    out, s, days, runs = ledger(tmp_cfg, tmp_path)
    assert tree_snapshot(tmp_cfg.data_root) == before                    # read-only

    r = days.loc[("0050", pd.Timestamp(hole))]
    assert r["expected_slots"] == 54 and r["real_bars"] == 52 and r["missing_runs"] == 1 and r["longest_run"] == 2
    assert r["conservation"] == "shortfall" and r["day_status"] == "has_unresolved_slots"
    run = runs[(runs["symbol"] == "0050") & (runs["date"] == pd.Timestamp(hole))].iloc[0]
    assert (run["start_slot"], run["end_slot"], run["n_slots"]) == ("10:05", "10:10", 2)
    assert run["status"] == "UNRESOLVED" and run["evidence_candidate"] == "volume_shortfall"
    assert not run["research_eligible"] and pd.isna(run["response_hash"])
    a = days.loc[("2330", pd.Timestamp(hole))]
    assert a["missing_close_auction"] and a["missing_continuous"] == 0
    w = days.loc[("9999", pd.Timestamp(gone))]
    assert w["day_status"] == "provider_missing_evidenced" and w["real_bars"] == 0
    wr = runs[(runs["symbol"] == "9999") & (runs["date"] == pd.Timestamp(gone))].iloc[0]
    assert wr["status"] == "PROVIDER_MISSING_EVIDENCED" and wr["n_slots"] == 54
    d = days.loc[("5555", pd.Timestamp("2023-09-05"))]
    assert d["day_status"] == "special_auction_or_halt" and d["real_bars"] == 15 and d["missing_slots"] == 0

    # fakes reconcile exactly: every complete day conserves volume to the share
    assert s["conservation_on_complete_days"]["exact_pct"] == 100.0
    sl = s["slots"]
    assert sl["missing"] == sl["by_status"]["UNRESOLVED"] + sl["by_status"]["PROVIDER_MISSING_EVIDENCED"]
    assert sl["total_expected"] == sl["real_bars"] + sl["missing"] == sl["by_status"]["REAL_BAR"] + sl["missing"]
    assert s["logic_leak_signals"]["days_with_unexpected_bars"] == 0
    cs = pd.read_parquet(out / "gap_slot_crosssection.parquet")
    assert cs["expected"].sum() == sl["total_expected"] and cs["missing"].sum() == sl["missing"]


def test_spotcheck_detects_logic_loss(tmp_cfg, clock, tmp_path):
    hole = date(2024, 2, 20)
    fin, _ = build_lake(tmp_cfg, clock, lambda f: f.drop_bars.add(("0050", hole, "10:05")))
    out, s, days, runs = ledger(tmp_cfg, tmp_path)
    honest = FakeFubon(fin)                                               # provider now returns the bar
    before = tree_snapshot(tmp_cfg.data_root)
    sc = run_spotcheck(tmp_cfg, out, tmp_path / "sc", honest, per_stratum=3, extra=[("0050", "2024-02-20")],
                       sleep=lambda _s: None)
    assert tree_snapshot(tmp_cfg.data_root) == before
    res = pd.read_csv(sc / "spotcheck.csv", dtype={"symbol": str}).set_index(["symbol", "date"])
    row = res.loc[("0050", "2024-02-20")]
    assert row["verdict"] == "logic_loss" and row["api_only_slots"] == "10:05" and len(row["response_hash"]) == 64
    assert (sc / "responses" / "0050_2024-02-20.json").exists()
    ctrl = res[res["stratum"] == "control_complete"]
    assert len(ctrl) and (ctrl["verdict"] == "consistent").all()
