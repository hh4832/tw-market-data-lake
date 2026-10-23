"""Single-writer lock stored on the shared Drive.

Exactly one process may write datasets/manifests at a time (Colab, a local PC and GitHub Actions
could otherwise race on the same Drive). The lock file is created with O_CREAT|O_EXCL; holders
refresh a heartbeat. A lock whose heartbeat is older than `stale_minutes` is reported as stale
and is only broken when the caller explicitly asks (--break-stale-lock); the old lock file is
moved to quarantine, never silently deleted.

Note: Google Drive FUSE does not give cross-machine atomicity guarantees for O_EXCL. The lock
plus the heartbeat check before every manifest commit makes concurrent writers detectable and
fail closed; it is not a distributed consensus protocol.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
from datetime import datetime, timedelta
from pathlib import Path

from .clock import Clock
from .errors import LockHeldError, LockLostError


class WriterLock:
    def __init__(self, lock_dir: Path, run_id: str, clock: Clock, stale_minutes: int = 30,
                 quarantine_dir: Path | None = None, name: str = "writer.lock"):
        self.path = Path(lock_dir) / name
        self.run_id = run_id
        self.clock = clock
        self.stale = timedelta(minutes=stale_minutes)
        self.quarantine_dir = quarantine_dir
        self.held = False

    # -- helpers -------------------------------------------------------------
    def _payload(self, started_at: str | None = None) -> dict:
        now = self.clock.now().isoformat()
        return {
            "run_id": self.run_id,
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": started_at or now,
            "heartbeat_at": now,
        }

    def read(self) -> dict | None:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError):
            return {"run_id": "<unreadable>", "heartbeat_at": None}

    def is_stale(self, info: dict) -> bool:
        hb = info.get("heartbeat_at")
        if not hb:
            return True
        try:
            t = datetime.fromisoformat(hb)
        except ValueError:
            return True
        return self.clock.now() - t > self.stale

    # -- API -----------------------------------------------------------------
    def acquire(self, break_stale: bool = False) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            info = self.read() or {}
            if self.is_stale(info) and break_stale:
                self._quarantine_existing(info)
                return self.acquire(break_stale=False)
            state = "stale" if self.is_stale(info) else "active"
            raise LockHeldError(
                f"writer lock held ({state}) by run_id={info.get('run_id')} host={info.get('host')} "
                f"heartbeat_at={info.get('heartbeat_at')}. "
                + ("Re-run with --break-stale-lock if that run is dead." if state == "stale"
                   else "Wait for it to finish; do not start a second writer.")
            )
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(self._payload(), fh)
        self.held = True

    def _quarantine_existing(self, info: dict) -> None:
        if self.quarantine_dir is None:
            raise LockHeldError("stale lock found but no quarantine dir configured")
        dest = Path(self.quarantine_dir) / f"stale_lock_{info.get('run_id', 'unknown')}_{self.run_id}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(self.path), dest)

    def verify(self) -> None:
        """Fail closed if the lock is no longer ours (removed, broken by someone else)."""
        info = self.read()
        if not self.held or info is None or info.get("run_id") != self.run_id:
            raise LockLostError(f"writer lock lost (now: {info and info.get('run_id')})")

    def heartbeat(self) -> None:
        self.verify()
        info = self.read() or {}
        tmp = self.path.with_name(self.path.name + f".{self.run_id}.tmp")
        tmp.write_text(json.dumps(self._payload(info.get("started_at"))), encoding="utf-8")
        os.replace(tmp, self.path)

    def release(self) -> None:
        if not self.held:
            return
        info = self.read()
        if info and info.get("run_id") == self.run_id:
            self.path.unlink(missing_ok=True)
        self.held = False

    def __enter__(self) -> "WriterLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()
