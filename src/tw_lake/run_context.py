"""Per-run context: run id, logging, timing, run folder artifacts (never overwritten)."""
from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from . import SCHEMA_VERSION, __version__
from .clock import Clock, run_timestamp
from .config import REPO_ROOT, Config
from .errors import ImmutableViolation
from .paths import Layout

LOGGER_NAME = "tw_lake"


def git_sha() -> str:
    env = os.getenv("TW_LAKE_GIT_SHA")
    if env:
        return env
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True,
                                       stderr=subprocess.DEVNULL).strip()
    except Exception:  # noqa: BLE001
        return "nogit"


def package_versions() -> dict[str, str]:
    out = {"python": sys.version.split()[0], "platform": platform.platform()}
    for mod in ("pandas", "pyarrow", "numpy", "finlab", "fubon_neo", "yaml", "tqdm"):
        try:
            m = __import__(mod)
            out[mod] = getattr(m, "__version__", "unknown")
        except Exception:  # noqa: BLE001
            out[mod] = "not_installed"
    return out


@dataclass
class RunContext:
    cfg: Config
    layout: Layout
    clock: Clock
    mode: str
    run_id: str
    sha: str
    local_dir: Path
    started_monotonic: float
    info: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, pd.DataFrame] = field(default_factory=dict)
    texts: dict[str, str] = field(default_factory=dict)

    @classmethod
    def create(cls, cfg: Config, mode: str, clock: Clock) -> "RunContext":
        sha = git_sha()
        run_id = f"{run_timestamp(clock)}_{sha[:7]}"
        local_dir = Path(cfg.local_tmp) / run_id / "_run"
        local_dir.mkdir(parents=True, exist_ok=True)
        rc = cls(cfg=cfg, layout=Layout.from_config(cfg), clock=clock, mode=mode, run_id=run_id, sha=sha,
                 local_dir=local_dir, started_monotonic=time.monotonic())
        rc._setup_logging()
        rc.info.update({
            "run_id": run_id, "mode": mode, "git_commit": sha, "tw_lake_version": __version__,
            "schema_version": SCHEMA_VERSION, "timezone": "Asia/Taipei",
            "started_at": clock.now().isoformat(), "versions": package_versions(),
            "data_root": str(cfg.data_root), "minute_params": cfg.minute_params(),
            "minute_params_hash": cfg.minute_params_hash(), "status": "running", "stages": [],
        })
        return rc

    # ---------------------------------------------------------------- logging
    def _setup_logging(self) -> None:
        lg = logging.getLogger(LOGGER_NAME)
        lg.setLevel(logging.INFO)
        for h in list(lg.handlers):
            lg.removeHandler(h)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(self.local_dir / "pipeline.log", encoding="utf-8")
        fh.setFormatter(fmt)
        lg.addHandler(sh)
        lg.addHandler(fh)
        lg.propagate = False

    @property
    def log(self) -> logging.Logger:
        return logging.getLogger(LOGGER_NAME)

    def stage(self, name: str):
        rc = self

        class _Stage:
            def __enter__(self_inner):
                self_inner.t0 = time.monotonic()
                rc.log.info("START %s", name)
                return self_inner

            def __exit__(self_inner, et, ev, tb):
                dt = round(time.monotonic() - self_inner.t0, 2)
                status = "DONE" if et is None else "FAIL"
                rc.log.info("%s %s elapsed=%.2fs%s", status, name, dt, "" if et is None else f" error={ev!r}")
                rc.info["stages"].append({"stage": name, "status": status, "elapsed_sec": dt,
                                          "error": None if et is None else repr(ev)[:500]})
                return False

        return _Stage()

    # -------------------------------------------------------------- artifacts
    def add_table(self, name: str, df: pd.DataFrame) -> None:
        self.artifacts[name] = df

    def add_text(self, name: str, text: str) -> None:
        self.texts[name] = text

    def finalize(self, status: str) -> Path:
        """Write the run folder `<system>/runs/<run_id>/` (must not exist) and return it."""
        self.info["status"] = status
        self.info["finished_at"] = self.clock.now().isoformat()
        self.info["elapsed_sec"] = round(time.monotonic() - self.started_monotonic, 2)
        out_local = self.local_dir / "artifacts"
        out_local.mkdir(parents=True, exist_ok=True)
        for name, df in self.artifacts.items():
            if df is None:
                continue
            df.to_parquet(out_local / f"{name}.parquet", index=False)
            df.to_csv(out_local / f"{name}.csv", index=False, encoding="utf-8-sig")
        for name, text in self.texts.items():
            (out_local / name).write_text(text, encoding="utf-8")
        (out_local / "run_info.json").write_text(json.dumps(self.info, ensure_ascii=False, indent=2, default=str),
                                                 encoding="utf-8")
        for h in logging.getLogger(LOGGER_NAME).handlers:
            h.flush()
        shutil.copy2(self.local_dir / "pipeline.log", out_local / "pipeline.log")

        dest = self.layout.runs / self.run_id
        self.layout.assert_writable(dest)
        if dest.exists():
            raise ImmutableViolation(f"run folder already exists: {dest}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(out_local, dest)
        self.log.info("run artifacts: %s", dest)
        return dest
