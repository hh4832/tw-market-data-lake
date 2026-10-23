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


def content_hash(df: pd.DataFrame, key_cols: Sequence[str]) -> str:
    d = canonicalize(df, key_cols)
    cols = sorted(d.columns)
    d = d[cols].copy()
    kinds = {}
    for c in cols:
        k = _kind(d[c])
        kinds[c] = k
        if k == "datetime_tz":
            d[c] = d[c].dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif k == "datetime":
            d[c] = d[c].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        elif k == "float":
            d[c] = d[c].astype("float64")
        elif k == "int":
            if d[c].isna().any():
                d[c] = d[c].astype("float64")
                kinds[c] = "int_nullable"
            else:
                d[c] = d[c].astype("int64")
        elif k == "bool":
            d[c] = d[c].map(lambda v: "" if pd.isna(v) else ("1" if bool(v) else "0"))
        else:
            s_ = d[c].astype(object)
            d[c] = s_.where(s_.notna(), "").astype(str)
    header = json.dumps({"columns": cols, "kinds": kinds, "n": len(d)}, sort_keys=True).encode()
    h = hashlib.sha256(header)
    # chunked rendering keeps memory bounded on multi-million-row tables
    step = 200_000
    for i in range(0, len(d), step):
        part = d.iloc[i : i + step]
        h.update(part.to_csv(index=False, header=False, lineterminator="\n", float_format="%.17g", na_rep="").encode())
    return h.hexdigest()


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
