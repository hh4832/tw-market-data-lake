"""File SHA256 and stable, library-version-tolerant content hashes for DataFrames.

The content hash is computed over a canonical text rendering (sorted rows, sorted columns,
fixed float/datetime formatting) so that identical data produces the same hash regardless of
row order, column order, parquet writer version, or pandas object/str dtype differences.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

_CHUNK = 1 << 20


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(_CHUNK)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _kind(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "bool"
    if pd.api.types.is_integer_dtype(s):
        return "int"
    if pd.api.types.is_float_dtype(s):
        return "float"
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        return "datetime_tz"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"
    return "str"


def canonicalize(df: pd.DataFrame, key_cols: Sequence[str]) -> pd.DataFrame:
    """Sort rows by key columns (stable) and reset index. Column order is preserved."""
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        raise KeyError(f"key columns missing: {missing}")
    return df.sort_values(list(key_cols), kind="mergesort").reset_index(drop=True)


HASH_VERSION = "2"
_SEP = b"\x1e"


def _datetime_ns(s: pd.Series) -> np.ndarray:
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s.astype("datetime64[ns]").to_numpy().view("int64")


def column_digest_bytes(s: pd.Series, kind: str) -> bytes:
    """Version-independent byte rendering of one column (vectorized; no per-row formatting)."""
    if kind == "float":
        a = s.to_numpy(dtype="float64", na_value=np.nan) + 0.0       # -0.0 -> 0.0
        a[np.isnan(a)] = np.nan                                         # canonical NaN payload
        return np.ascontiguousarray(a).tobytes()
    if kind == "int":
        return np.ascontiguousarray(s.to_numpy(dtype="int64")).tobytes()
    if kind in ("datetime", "datetime_tz"):
        return np.ascontiguousarray(_datetime_ns(s)).tobytes()
    vals = s.astype(object).tolist()
    return "\x1f".join("\x00" if (v is None or (isinstance(v, float) and v != v)) else str(v) for v in vals).encode()


def content_hash(df: pd.DataFrame, key_cols: Sequence[str]) -> str:
    """SHA256 over canonical rows (sorted by key) and columns (sorted by name).
    float/int/datetime columns are hashed from their binary values, everything else as text.
    bool and nullable-int columns are rendered as text so object/bool or Int64/float differences
    from parquet round trips cannot change the hash."""
    d = canonicalize(df, key_cols)
    cols = sorted(d.columns)
    kinds = {}
    h = hashlib.sha256()
    for c in cols:
        s = d[c]
        k = _kind(s)
        if k == "int" and s.isna().any():
            k = "float"
        if k == "bool":
            k, s = "str", s.map(lambda v: None if pd.isna(v) else ("1" if bool(v) else "0"))
        kinds[c] = k
        h.update(str(c).encode() + _SEP + k.encode() + _SEP)
        h.update(column_digest_bytes(s, k))
        h.update(_SEP)
    header = json.dumps({"v": HASH_VERSION, "columns": cols, "kinds": kinds, "n": len(d)}, sort_keys=True).encode()
    return hashlib.sha256(header + h.digest()).hexdigest()


def roundtrip(df: pd.DataFrame) -> pd.DataFrame:
    """What the DataFrame looks like after a parquet write/read (dtypes as stored)."""
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return pd.read_parquet(buf)


def stored_content_hash(df: pd.DataFrame, key_cols: Sequence[str]) -> str:
    """Content hash of the data as it will exist on disk. Use this to compare with published hashes."""
    return content_hash(roundtrip(canonicalize(df, key_cols)), key_cols)
