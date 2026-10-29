"""Stage log + run_info.json for audit runs (START / DONE / FAIL with elapsed seconds and counts)."""
from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from ..clock import TAIPEI
from ..run_context import git_sha, package_versions


class Stages:
    def __init__(self, out: Path, run_kind: str, params: dict, log=print):
        self.out, self.log, self.t0 = Path(out), log, time.monotonic()
        self.info = {"run_kind": run_kind, "run_id": Path(out).name, "git_sha": git_sha(),
                     "started_at": datetime.now(TAIPEI).isoformat(timespec="seconds"), "params": params,
                     "packages": package_versions(), "stages": [], "inputs": {}, "status": "running"}
        self._write()

    def _write(self) -> None:
        (self.out / "run_info.json").write_text(json.dumps(self.info, ensure_ascii=False, indent=2, default=str),
                                                encoding="utf-8")

    @contextmanager
    def stage(self, name: str, **extra):
        t = time.monotonic()
        self.log(f"START {name}")
        rec = {"stage": name, "status": "running", **extra}
        self.info["stages"].append(rec)
        try:
            yield rec
        except Exception as e:
            rec.update(status="FAIL", error=repr(e)[:500], elapsed_sec=round(time.monotonic() - t, 1))
            self.info["status"] = "failed"
            self._write()
            self.log(f"FAIL  {name} after {rec['elapsed_sec']} s: {e!r}")
            raise
        rec.update(status="DONE", elapsed_sec=round(time.monotonic() - t, 1))
        self._write()
        counts = {k: v for k, v in rec.items() if k not in ("stage", "status", "elapsed_sec")}
        self.log(f"DONE  {name} in {rec['elapsed_sec']} s {counts if counts else ''}")

    def input_file(self, label: str, path: Path, hash_it: bool = True) -> None:
        p = Path(path)
        rec = {"path": str(p), "exists": p.exists()}
        if hash_it and p.is_file():
            h = hashlib.sha256()
            with open(p, "rb") as f:
                for b in iter(lambda: f.read(1 << 20), b""):
                    h.update(b)
            rec["sha256"] = h.hexdigest()
        self.info["inputs"][label] = rec
        self._write()

    def finish(self, **summary) -> None:
        self.info.update(status="done", finished_at=datetime.now(TAIPEI).isoformat(timespec="seconds"),
                         elapsed_sec=round(time.monotonic() - self.t0, 1), summary=summary)
        self._write()


def progress(items, desc: str, total: int | None = None):
    try:
        from tqdm.auto import tqdm
        return tqdm(items, desc=desc, total=total, mininterval=20)
    except Exception:  # noqa: BLE001
        return items
