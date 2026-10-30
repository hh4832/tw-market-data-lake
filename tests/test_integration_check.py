"""B1 integration check on a synthetic quality-mask run. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import pandas as pd

from tw_lake.audit import eligibility as E
from tw_lake.audit import quality_mask as Q
from tw_lake.audit.integration_check import integration_check
from tests.test_eligibility import build, noop
from tests.test_quality_mask import _price


def _qrun(tmp_path):
    led, root, recon = build(tmp_path)
    el = E.run(led, recon, root, tmp_path / "elig", log=noop)
    sdq = pd.read_parquet(el / "symbol_day_quality.parquet")
    pc = tmp_path / "pc"
    pc.mkdir()
    _price(sdq, {}).to_parquet(pc / "price_check_symbol_day.parquet", index=False)
    return Q.run(led, el, tmp_path / "q", price_check_dir=pc, log=noop)


def test_pass_then_detects_tampering(tmp_path):
    out = _qrun(tmp_path)
    r = integration_check(out, expected_seq=None)
    assert r["verdict"] == "PASS", r["failures"]
    assert all(r["invariants"].values())
    (out / "stats_gap_classification.csv").write_text("tampered", encoding="utf-8")
    r2 = integration_check(out, expected_seq=None)
    assert r2["verdict"] == "FAIL" and "output SHA-256 mismatch" in r2["failures"]


def test_commit_and_seq_mismatch_fail(tmp_path):
    out = _qrun(tmp_path)
    r = integration_check(out, expected_commit="deadbee", expected_seq=68)
    assert "commit differs" in r["failures"] and "mask seq differs" in r["failures"]
