"""Configuration: YAML defaults + environment + CLI overrides."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "default.yaml"


@dataclass
class Config:
    raw: dict[str, Any]
    data_root: Path
    local_tmp: Path
    minute_start: date
    extra: dict[str, Any] = field(default_factory=dict)

    # -- convenient accessors ------------------------------------------------
    @property
    def fubon(self) -> dict[str, Any]:
        return self.raw["fubon"]

    @property
    def finlab(self) -> dict[str, Any]:
        return self.raw["finlab"]

    @property
    def qa(self) -> dict[str, Any]:
        return self.raw["qa"]

    @property
    def manifest(self) -> dict[str, Any]:
        return self.raw["manifest"]

    def minute_params(self) -> dict[str, Any]:
        """Everything that changes the content of a 5m partition. Part of the cache key."""
        f = self.fubon
        return {
            "provider": "fubon",
            "endpoint": "historical/candles",
            "timeframe": str(f["timeframe"]),
            "fields": list(f["fields"]),
            "adjusted": False,
            "bar_label": f["bar_label"],
            "volume_unit_by_type": dict(f["volume_unit_by_type"]),
        }

    def minute_params_hash(self) -> str:
        blob = json.dumps(self.minute_params(), sort_keys=True, ensure_ascii=False).encode()
        return hashlib.sha256(blob).hexdigest()[:16]


def _deep_update(base: dict, upd: dict) -> dict:
    for k, v in upd.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v
    return base


def load_config(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> Config:
    p = Path(path) if path else DEFAULT_CONFIG_PATH
    with p.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    raw = copy.deepcopy(raw)
    if os.getenv("DATA_ROOT"):
        raw["data_root"] = os.environ["DATA_ROOT"]
    if os.getenv("TW_LAKE_LOCAL_TMP"):
        raw["local_tmp"] = os.environ["TW_LAKE_LOCAL_TMP"]
    if overrides:
        _deep_update(raw, overrides)

    wm = int(raw["fubon"]["window_months"])
    if wm not in (1, 2, 3, 4, 6):
        raise ConfigError("fubon.window_months must divide a year and stay < 12 (1,2,3,4,6)")
    if raw["fubon"]["bar_label"] not in ("start", "end"):
        raise ConfigError("fubon.bar_label must be 'start' or 'end'")

    return Config(
        raw=raw,
        data_root=Path(raw["data_root"]),
        local_tmp=Path(raw["local_tmp"]),
        minute_start=date.fromisoformat(str(raw["fubon"]["minute_start"])),
    )
