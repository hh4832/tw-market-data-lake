"""Drive layout and the write guard that keeps this repo out of folders it does not own."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .errors import ProtectedPathError


@dataclass(frozen=True)
class Layout:
    data_root: Path
    system_dir: Path
    dataset_dirs: dict[str, Path]
    protected: tuple[Path, ...]

    @classmethod
    def from_config(cls, cfg: Config) -> "Layout":
        root = cfg.data_root
        raw = cfg.raw
        return cls(
            data_root=root,
            system_dir=root / raw["system_dir"],
            dataset_dirs={k: root / v for k, v in raw["datasets"].items()},
            protected=tuple(root / p for p in raw.get("protected_existing_folders", [])),
        )

    # -- system folders ------------------------------------------------------
    @property
    def locks(self) -> Path:
        return self.system_dir / "locks"

    @property
    def runs(self) -> Path:
        return self.system_dir / "runs"

    @property
    def staging(self) -> Path:
        return self.system_dir / "staging"

    @property
    def quarantine(self) -> Path:
        return self.system_dir / "quarantine"

    @property
    def smoke(self) -> Path:
        return self.system_dir / "smoke"

    def dataset(self, name: str) -> Path:
        return self.dataset_dirs[name]

    # -- guard ---------------------------------------------------------------
    def owned_roots(self) -> list[Path]:
        return [self.system_dir, *self.dataset_dirs.values()]

    def assert_writable(self, path: Path) -> Path:
        """Raise unless `path` is inside a folder this repo owns and outside protected folders."""
        p = _abs(path)
        for prot in self.protected:
            pr = _abs(prot)
            if p == pr or pr in p.parents:
                raise ProtectedPathError(f"refusing to write inside protected folder: {p}")
        for own in self.owned_roots():
            o = _abs(own)
            if p == o or o in p.parents:
                return p
        raise ProtectedPathError(f"refusing to write outside owned folders: {p}")

    def check_data_root(self) -> None:
        if not self.data_root.exists():
            raise ProtectedPathError(f"DATA_ROOT does not exist (is Drive mounted?): {self.data_root}")
        for prot in self.protected:
            for own in self.owned_roots():
                if _abs(own) == _abs(prot) or _abs(prot) in _abs(own).parents:
                    raise ProtectedPathError(f"owned folder {own} overlaps protected folder {prot}")


def _abs(p: Path | str) -> Path:
    return Path(os.path.abspath(os.fspath(p)))
