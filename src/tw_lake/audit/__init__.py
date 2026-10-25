"""Data Quality Audit (Phase A, read-only).

This package never writes, moves or deletes anything under DATA_ROOT. It reads the
manifest-pointed research versions (``_manifest/published.json`` -> ``partitions.parquet``)
and writes its own outputs to a separate, new audit folder (local by default).

Modules
    reader      read-only manifest access + source-unchanged fingerprint
    integrity   A1 storage / manifest integrity
    bars        A2 5m completeness & timestamp semantics (per symbol/date)
    recon       A3 5m -> daily OHLCV reconciliation against FinLab raw daily
    universe    A4 universe / listing / classification provenance + excluded bars
    gaps        A5 gap cases, partial windows, trading_regime_daily
    readiness   research_ready derived flags (audit output only)
    report      README / audit_summary.md / audit_summary.json
    runner      orchestration;  cli  command line
"""

AUDIT_VERSION = "0.1.0"
