"""Install runtime dependencies and the Fubon Neo Linux SDK (not on PyPI).

SDK source, first match wins:
  1. $FUBON_SDK_PATH            a .zip or .whl (e.g. kept on Google Drive)
  2. lib/fubon_neo/*.zip|*.whl  (gitignored; place the official file there manually)
  3. official download URL      https://www.fbs.com.tw/TradeAPI_SDK/fubon_binary/...

Usage (repo root):  python scripts/install_colab_dependencies.py
"""
from __future__ import annotations

import os
import platform
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SDK_DIR = ROOT / "lib" / "fubon_neo"
SDK_VERSION = "2.4.0"
OFFICIAL = {
    "x86_64": f"https://www.fbs.com.tw/TradeAPI_SDK/fubon_binary/fubon_neo-{SDK_VERSION}-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.zip",
    "aarch64": f"https://www.fbs.com.tw/TradeAPI_SDK/fubon_binary/fubon_neo-{SDK_VERSION}-cp37-abi3-manylinux_2_17_aarch64.manylinux2014_aarch64.zip",
}


def run(*args: str) -> None:
    print("+", " ".join(args), flush=True)
    subprocess.run(args, check=True)


def linux_wheel(paths: list[Path]) -> Path | None:
    ok = [p for p in paths if "manylinux" in p.name or "linux_" in p.name]
    return sorted(ok)[-1] if ok else None


def resolve_wheel() -> Path:
    SDK_DIR.mkdir(parents=True, exist_ok=True)
    env = os.getenv("FUBON_SDK_PATH")
    candidates: list[Path] = [Path(env)] if env else []
    candidates += sorted(SDK_DIR.glob("*.whl")) + sorted(SDK_DIR.glob("*.zip"))
    if not candidates:
        arch = platform.machine().lower()
        url = OFFICIAL.get("aarch64" if arch in ("aarch64", "arm64") else "x86_64")
        dest = SDK_DIR / url.rsplit("/", 1)[1]
        print(f"Downloading official Fubon SDK: {url}")
        urllib.request.urlretrieve(url, dest)
        candidates = [dest]
    for c in candidates:
        if c.suffix == ".whl":
            return c
        if c.suffix == ".zip":
            out = SDK_DIR / f"_extracted_{c.stem}"
            with zipfile.ZipFile(c) as zf:
                zf.extractall(out)
            w = linux_wheel(list(out.rglob("fubon_neo-*.whl")))
            if w:
                return w
    raise FileNotFoundError("no Linux fubon_neo wheel found (Windows/macOS wheels cannot be installed here)")


def main() -> None:
    print(f"Python {sys.version.split()[0]} on {platform.platform()}")
    if sys.version_info[:2] != (3, 11):
        print("[WARN] target is Python 3.11; this runtime differs. fubon_neo supports 3.8-3.13 (not 3.14). "
              "Versions are recorded in run_info.json.")
    run(sys.executable, "-m", "pip", "install", "-q", "-r", str(ROOT / "requirements.txt"))
    wheel = resolve_wheel()
    run(sys.executable, "-m", "pip", "install", "-q", str(wheel))
    run(sys.executable, "-m", "pip", "install", "-q", "-e", str(ROOT))
    from fubon_neo.sdk import FubonSDK  # noqa: F401
    import finlab  # noqa: F401
    print("[OK] dependencies installed; fubon_neo and finlab import successfully")


if __name__ == "__main__":
    main()
