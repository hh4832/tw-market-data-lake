"""Resumable official-volume batch on synthetic responses. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd

from tw_lake.reference import official_volume_batch as B
from tw_lake.reference.official_volume import read_raw
from tests.test_volume_recon import D, fake_get, ledger

D2 = date(2025, 6, 24)


def getter_factory(fail_twse_on=None, calls=None):
    def get(url, params):
        if calls is not None:
            calls.append(url)
        if fail_twse_on and "twse" in url and params.get("date") == fail_twse_on:
            return 200, b"<html>too many requests</html>", "text/html", url
        st, body, ct, u = fake_get(url, params)
        if st != 200:
            return 200, json.dumps({"stat": "ok", "tables": []}).encode(), "application/json", url
        return st, body, ct, u
    return get


def _ledger_two_days(tmp_path):
    led = ledger(tmp_path)
    d = pd.read_parquet(led / "gap_day_summary.parquet")
    d2 = d.copy()
    d2["date"] = pd.Timestamp(D2)
    pd.concat([d, d2]).to_parquet(led / "gap_day_summary.parquet", index=False)
    return led


def test_fetch_is_resumable_and_recon_runs(tmp_path):
    led = _ledger_two_days(tmp_path)
    root = tmp_path / "ref" / "official_volume" / "v1"
    noop = lambda *_a, **_k: None  # noqa: E731
    # run 1: TWSE blocked on D2 -> D done, D2 not marked done
    r1 = B.fetch(led, root, tmp_path / "work", getter=getter_factory(fail_twse_on="20250624"), sleep=noop, log=noop)
    assert r1["done"] == [D.isoformat()] and r1["failed"][0]["date"] == D2.isoformat()
    assert B.status(led, root)["remaining"] == 1
    sd = B.shard_dir(root, D)
    rq = pd.read_parquet(sd / "requests.parquet")
    f = rq[rq["report"] == "MI_INDEX_ALLBUT0999"].iloc[0]
    assert str(f.raw_file).endswith(".json.gz")
    import hashlib
    assert hashlib.sha256(read_raw(sd / f.raw_file)).hexdigest() == f.sha256
    assert not (sd / "parsed_volume.parquet").exists() and (sd / "components.parquet").exists()
    # run 2: only D2 is requested
    calls = []
    r2 = B.fetch(led, root, tmp_path / "work", getter=getter_factory(calls=calls), sleep=noop, log=noop)
    assert r2["done"] == [D2.isoformat()] and B.status(led, root)["remaining"] == 0
    assert len(calls) == len(B.batch_endpoints())
    out = B.recon(led, root, tmp_path / "recon")
    x = pd.read_parquet(out / "volume_recon_symbol_day.parquet").set_index(["symbol", "date"])["volume_bound_class"]
    assert x[("A", "2025-06-23")] == "CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND"
    assert x[("A", "2025-06-24")] == "CANDIDATE_NO_BOARD_LOT_TRADE_BY_VOLUME_BOUND"


def test_time_budget_and_guard(tmp_path):
    led = _ledger_two_days(tmp_path)
    noop = lambda *_a, **_k: None  # noqa: E731
    r = B.fetch(led, tmp_path / "v1", tmp_path / "work", max_minutes=0, getter=getter_factory(), sleep=noop, log=noop)
    assert r["done"] == []
    import pytest
    with pytest.raises(SystemExit):
        B.fetch(led, tmp_path / "fubon_5m" / "x", tmp_path / "work", getter=getter_factory(), sleep=noop, log=noop)
