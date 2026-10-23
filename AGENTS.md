# AGENTS.md — rules for AI agents working in this repo

1. **Scope**: data ingestion only. Do not add signals, backtests, 30m aggregation outputs, or any order/trading/accounting API call.
2. **Drive safety**: write only through `DatasetStore` / `RunContext` (they call `Layout.assert_writable`). Never read, write, move or delete `finlab_0050`, `finlab_txf_calendar`, `finlab_futures_afterhours`, `fubon_txf`, `IKBR` or anything outside the folders listed in `config/default.yaml`.
3. **Immutability**: published data files and manifest versions are never overwritten or deleted. Changes create new content-addressed files and a new manifest version; `published.json` is the only mutable pointer. Corrupt files go to `quarantine/`.
4. **Commit order**: data files (staging → verify → publish) first, manifest last. A window becomes `complete` only after publish + read-back + QA, and only when the manifest commit succeeds.
5. **No guessing**: no forward-fill, no inferred prices, no turnover derived from price × volume, no mixing adjusted and unadjusted prices. Unexplained 404/empty responses stay `failed`.
6. **Cache keys**: anything that changes 5m content belongs in `Config.minute_params()`; changing it must fail with `ParamsMismatchError`, not silently mix data.
7. **Secrets**: credentials come from environment variables (Colab Secrets / GitHub Secrets) and are never logged, written to run artifacts, or committed.
8. **Tests**: keep tests offline (fakes in `tests/conftest.py`, `SYNTHETIC_SMOKE_ONLY`). Run `python -m pytest` before committing. Tests passing is not evidence of real-data coverage.
9. **Git**: commit/push only when the task explicitly authorizes it; never force-push; do not modify other repositories.
10. **Real API runs** (smoke/pilot/full/update) are executed by the user in Colab, not by agents.
