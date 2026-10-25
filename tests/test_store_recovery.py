"""Publish protocol, interrupted writes, recovery scan, manifest inconsistency. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd
import pytest

from conftest import FakeFinlab, FakeFubon
from test_e2e import run, store, windows
from tw_lake.clock import TAIPEI
from tw_lake.errors import ImmutableViolation, ProtectedPathError
from tw_lake.hashing import content_hash
from tw_lake.paths import Layout
from tw_lake.snapshots import publish_snapshot
from tw_lake.store import DatasetStore, StoreContext, quarantine_system_staging


def mkstore(cfg, clock, run_id="r1", name="security_categories", fault=None):
    ctx = StoreContext(Layout.from_config(cfg), run_id, cfg.local_tmp, clock,
                       fault=fault or (lambda s: None))
    return DatasetStore(name, ctx, ["k"], "1")


def frame(n=5, shift=0.0):
    return pd.DataFrame({"k": [str(i) for i in range(n)], "v": [float(i) + shift for i in range(n)]})


def test_content_hash_ignores_row_and_column_order():
    a = frame()
    b = a.iloc[::-1][["v", "k"]]
    assert content_hash(a, ["k"]) == content_hash(b, ["k"])
    assert content_hash(a, ["k"]) != content_hash(frame(shift=0.5), ["k"])


def test_publish_reuse_and_versions(tmp_cfg, clock):
    s = mkstore(tmp_cfg, clock)
    r1 = s.publish_partition(frame(), "snapshot")
    assert not r1["reused"]
    r2 = s.publish_partition(frame().iloc[::-1], "snapshot")
    assert r2["reused"] and r2["relpath"] == r1["relpath"]
    s.commit({"partitions": pd.DataFrame([r1])})
    r3 = s.publish_partition(frame(shift=1), "snapshot")
    s.commit({"partitions": pd.DataFrame([r3])})
    assert s.pointer()["seq"] == 2
    assert (s.root / r1["relpath"]).exists() and (s.root / r3["relpath"]).exists()
    assert len(list((s.root / "_manifest" / "versions").iterdir())) == 2


def test_never_overwrites_different_content(tmp_cfg, clock):
    s = mkstore(tmp_cfg, clock)
    r = s.publish_partition(frame(), "snapshot")
    s.commit({"partitions": pd.DataFrame([r])})
    fp = s.root / r["relpath"]
    frame(shift=9).to_parquet(fp, index=False)       # tamper a *published* file: same path, different content
    with pytest.raises(ImmutableViolation):
        mkstore(tmp_cfg, clock, run_id="r2").publish_partition(frame(), "snapshot")


@pytest.mark.parametrize("damage", [b"", b"PAR1 truncated"])
def test_unreferenced_partial_file_is_left_alone_and_data_goes_to_fresh_path(tmp_cfg, clock, damage):
    """A crashed (or Drive-lost, 0-byte) write is never trusted, never moved and its path never reused."""
    s = mkstore(tmp_cfg, clock)
    r = s.publish_partition(frame(), "snapshot")     # not committed
    fp = s.root / r["relpath"]
    fp.write_bytes(damage)
    s2 = mkstore(tmp_cfg, clock, run_id="r2")
    r2 = s2.publish_partition(frame(), "snapshot")
    assert not r2["reused"] and r2["relpath"] != r["relpath"]
    assert r2["relpath"].endswith(f"{r['content_hash'][:32]}-r2.parquet")
    assert fp.read_bytes() == damage                  # bad file untouched (reported as orphan later)
    assert (s.root / r2["relpath"]).stat().st_size == r2["file_bytes"]
    s2.commit({"partitions": pd.DataFrame([r2])})
    rep = mkstore(tmp_cfg, clock, run_id="r3").recovery_scan()
    assert r["relpath"] in rep["orphan_files"] and not rep["corrupt_files"]
    r3 = mkstore(tmp_cfg, clock, run_id="r4").publish_partition(frame(), "snapshot")
    assert r3["reused"] is False or r3["relpath"] in (r["relpath"], r2["relpath"])


def test_bad_published_file_is_repaired_to_fresh_path(tmp_cfg, clock):
    s = mkstore(tmp_cfg, clock)
    r = s.publish_partition(frame(), "snapshot")
    s.commit({"partitions": pd.DataFrame([r])})
    (s.root / r["relpath"]).write_bytes(b"")         # 0-byte on Drive
    s2 = mkstore(tmp_cfg, clock, run_id="r2")
    rep = s2.recovery_scan()
    assert rep["corrupt_files"] == ["snapshot"] and "snapshot" in s2.bad_partitions
    r2 = s2.publish_partition(frame(), "snapshot")
    assert r2["relpath"] != r["relpath"] and (s2.root / r["relpath"]).read_bytes() == b""
    s2.commit({"partitions": pd.DataFrame([r2])})
    s3 = mkstore(tmp_cfg, clock, run_id="r3")
    rep3 = s3.recovery_scan()
    assert rep3["corrupt_files"] == []                # still referenced by the old manifest version only
    assert len(s3.read_partition("snapshot", verify=True)) == 5


def test_data_files_are_written_without_rename(tmp_cfg, clock, monkeypatch):
    import os as _os
    import tw_lake.store as st
    calls = []
    real = _os.replace
    monkeypatch.setattr(st.os, "replace", lambda a, b: (calls.append((str(a), str(b))), real(a, b))[1])
    s = mkstore(tmp_cfg, clock)
    s.publish_partition(frame(), "snapshot")
    assert not [c for c in calls if "/data/" in c[1]]   # no rename into data/
    assert not (s.ctx.layout.staging).exists() or not any(s.ctx.layout.staging.rglob("*.parquet"))


@pytest.mark.parametrize("stage", ["after_local_verify", "mid_publish", "after_publish"])
def test_interrupted_data_write_leaves_manifest_untouched(tmp_cfg, clock, stage):
    s = mkstore(tmp_cfg, clock)
    s.commit({"partitions": pd.DataFrame([s.publish_partition(frame(), "snapshot")])})
    ptr_before = s.pointer()

    def boom(st):
        if st == stage:
            raise RuntimeError("simulated crash")
    s2 = mkstore(tmp_cfg, clock, run_id="r2", fault=boom)
    with pytest.raises(RuntimeError):
        s2.publish_partition(frame(shift=3), "snapshot")
    assert s2.pointer() == ptr_before                 # commit point never reached
    s3 = mkstore(tmp_cfg, clock, run_id="r3")
    moved = quarantine_system_staging(s3.ctx.layout, "r3")
    rep = s3.recovery_scan()
    assert rep["missing_files"] == [] and rep["corrupt_files"] == []
    assert moved == []                                # data files no longer pass through staging
    if stage in ("mid_publish", "after_publish"):
        assert len(rep["orphan_files"]) == 1          # partial/complete but never committed: reported, kept
    if stage == "mid_publish":                        # next write goes to a fresh path; partial file untouched
        r = s3.publish_partition(frame(shift=3), "snapshot")
        assert not r["reused"] and r["relpath"].endswith("-r3.parquet")
        assert (s3.root / r["relpath"]).stat().st_size == r["file_bytes"]


def test_interrupted_manifest_commit(tmp_cfg, clock):
    s = mkstore(tmp_cfg, clock)
    s.commit({"partitions": pd.DataFrame([s.publish_partition(frame(), "snapshot")])})

    def boom(st):
        if st in ("manifest_after_staging",):
            raise RuntimeError("crash during manifest commit")
    s2 = mkstore(tmp_cfg, clock, run_id="r2", fault=boom)
    rec = s2.publish_partition(frame(shift=2), "snapshot")
    with pytest.raises(RuntimeError):
        s2.commit({"partitions": pd.DataFrame([rec])})
    assert s2.pointer()["seq"] == 1
    s3 = mkstore(tmp_cfg, clock, run_id="r3")
    rep = s3.recovery_scan()
    assert rep["staging_quarantined"]                 # half-written manifest moved aside
    assert s3.pointer()["seq"] == 1

    def boom2(st):
        if st == "manifest_before_pointer":
            raise RuntimeError("crash before pointer")
    s4 = mkstore(tmp_cfg, clock, run_id="r4", fault=boom2)
    with pytest.raises(RuntimeError):
        s4.commit({"partitions": pd.DataFrame([rec])})
    rep = mkstore(tmp_cfg, clock, run_id="r5").recovery_scan()
    assert rep["uncommitted_versions"]                # version dir exists but pointer did not move
    s6 = mkstore(tmp_cfg, clock, run_id="r6")
    ptr = s6.commit({"partitions": pd.DataFrame([rec])})
    assert ptr["seq"] > max(int(v) for v in rep["uncommitted_versions"])


def test_manifest_inconsistency_triggers_rebuild(tmp_cfg, clock):
    fin = FakeFinlab()
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    s = store(tmp_cfg, clock)
    rel = s.partitions().set_index("partition").loc["symbol=2330/year=2023", "relpath"]
    (s.root / rel).unlink()                           # published file vanished
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    fub = FakeFubon(fin)
    assert run(tmp_cfg, clock, fin, fub, mode="update") == 0
    froms = {c["from"] for c in fub.calls if c["symbol"] == "2330"}
    assert {"2023-05-23", "2023-07-01", "2023-10-01"} <= froms   # whole 2023 rebuilt
    s2 = store(tmp_cfg, clock)
    p = s2.read_partition("symbol=2330/year=2023", verify=True)
    assert p["bar_start"].min().date() == date(2023, 5, 23)
    runs = sorted((tmp_cfg.data_root / "_tw-market-data-lake" / "runs").iterdir())
    info = json.loads((runs[-1] / "run_info.json").read_text())
    assert any("symbol=2330/year=2023" in r["missing_files"] for r in info["recovery"])


def test_corrupt_published_table_fails_closed(tmp_cfg, clock):
    s = mkstore(tmp_cfg, clock)
    s.commit({"partitions": pd.DataFrame([s.publish_partition(frame(), "snapshot")])})
    vdir = s.root / "_manifest" / s.pointer()["version_dir"]
    (vdir / "partitions.parquet").write_bytes(b"garbage")
    from tw_lake.errors import IntegrityError
    with pytest.raises(IntegrityError):
        mkstore(tmp_cfg, clock, run_id="r2").tables()


def test_snapshot_dedupe(tmp_cfg, clock):
    s = mkstore(tmp_cfg, clock, name="finlab_daily_adjusted")
    parts = {"year=2023": frame(), "year=2024": frame(shift=10)}
    r1 = publish_snapshot(s, parts, "2024-03-01")
    assert r1["changed"] and r1["new_files"] == 2
    r2 = publish_snapshot(mkstore(tmp_cfg, clock, "r2", "finlab_daily_adjusted"), parts, "2024-03-02")
    assert not r2["changed"] and r2["seq"] == r1["seq"]
    n_files = len(list((s.root / "data").rglob("*.parquet")))
    parts2 = {"year=2023": frame(), "year=2024": frame(shift=11)}
    r3 = publish_snapshot(mkstore(tmp_cfg, clock, "r3", "finlab_daily_adjusted"), parts2, "2024-03-03")
    assert r3["changed"] and r3["new_files"] == 1 and r3["changed_partitions"] == ["year=2024"]
    assert len(list((s.root / "data").rglob("*.parquet"))) == n_files + 1


def test_adjusted_unchanged_across_runs_writes_nothing(tmp_cfg, clock):
    fin = FakeFinlab()
    assert run(tmp_cfg, clock, fin, FakeFubon(fin)) == 0
    adj = mkstore(tmp_cfg, clock, "x", "finlab_daily_adjusted")
    seq1 = adj.pointer()["seq"]
    files1 = sorted((adj.root / "data").rglob("*.parquet"))
    clock.set(datetime(2024, 3, 29, 18, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin, FakeFubon(fin), mode="update") == 0
    assert adj.pointer()["seq"] == seq1
    assert sorted((adj.root / "data").rglob("*.parquet")) == files1
    # a new dividend rewrites history -> new snapshot version, old one kept
    fin2 = FakeFinlab(adj_factor=0.98)
    clock.set(datetime(2024, 3, 29, 19, 0, tzinfo=TAIPEI))
    assert run(tmp_cfg, clock, fin2, FakeFubon(fin2), mode="update") == 0
    adj2 = mkstore(tmp_cfg, clock, "y", "finlab_daily_adjusted")
    assert adj2.pointer()["seq"] == seq1 + 1
    assert all(f.exists() for f in files1)


def test_protected_folders_cannot_be_written(tmp_cfg):
    lay = Layout.from_config(tmp_cfg)
    with pytest.raises(ProtectedPathError):
        lay.assert_writable(tmp_cfg.data_root / "finlab_0050" / "x.parquet")
    with pytest.raises(ProtectedPathError):
        lay.assert_writable(tmp_cfg.data_root / "somewhere_else" / "x.parquet")
    lay.assert_writable(tmp_cfg.data_root / "fubon_stock_5m" / "data" / "x.parquet")
