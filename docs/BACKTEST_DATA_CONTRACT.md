# Backtest Data Contract — tw-market-data-lake 5m（v1.0.0，草案，待審）

本文件規定**獨立回測 repo** 如何使用本 lake 的資料。本 repo 只提供資料與品質遮罩，不實作策略或回測引擎（見 AGENTS.md 規則 1）。

- 機器可讀版：`docs/backtest_data_contract.json`。
- 遮罩檔：`quality_mask_symbol_day.parquet`，由 `tw_lake.audit.quality_mask` 產生。每次執行都寫入新的版本化資料夾，並附 `quality_manifest.json`（含 SHA-256）。

## 1. 輸入與版本

回測必須固定以下版本，並在結果中記錄：

| 項目 | 來源 |
|---|---|
| 5 分 K | `fubon_5m`，以 `source_manifest_sequence` 指定 published 版本（目前為 seq 68） |
| 品質遮罩 | `quality_mask_symbol_day.parquet`，其 `evidence_version` 與 `quality_manifest.json` 的 SHA-256 |
| 官方日資料 | `reference/official_volume/v1`（每日 shard，含原始回應 SHA-256） |

## 2. 樣本層級

| 層級 | 欄位 | 用途 | 條件 |
|---|---|---|---|
| Level A（嚴格對照） | `level_A_strict` | 穩健性檢查 | 正常盤、54 根 K 棒全到、交易所日期 `VERIFIED`、個股殘差 = 0、`price_quality_status = PASS`、前一日有官方紀錄 |
| Level B + Price Gate（主要價格研究） | `level_B_price_gate`（= `can_use_for_price_backtest`） | 價格型策略的主要樣本 | 正常盤、單位已核實、交易所日期 `VERIFIED` 或 `NO_OVERCOUNT` 且未隔離、缺棒只允許 `VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH`、`price_quality_status = PASS`、前一日有官方紀錄 |
| Volume Strict（盤中量能研究） | `volume_strict`（= `can_use_for_volume_backtest`） | 量能型策略 | Level B + Price Gate，且 `volume_reconciliation_status = EXACT` |
| Isolate | `research_tier = ISOLATE` | 不可混入；如需研究須另行建模 | 特殊交易制度（處置分盤集合競價）、隔離日期、未解決異常、無官方紀錄 |
| Exploratory only | `research_tier = EXPLORATORY_ONLY` | 只能探索，不得作為正式結論 | 其餘，例如價格只有區間一致、缺棒只是 `CANDIDATE`、量短少 |

`UNKNOWN` 一律不得視為 `TRUE`。

## 3. 格點型態

缺棒格點**不可**直接 forward-fill 成可成交的 K 棒。

| 型態 | 判定 | 可否成交 | 可否用於指標 |
|---|---|---|---|
| `REAL_BAR` | 5 分 K 存在 | 可以：以下一根真實 K 棒開盤價成交（見第 5 節） | 可以 |
| `NO_TRADE_INTERVAL_EVIDENCED` | 缺棒，且當日 `gap_evidence_status = VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH` | 不可以 | 量 = 0；價格可標示為「上一筆成交價（研究衍生）」，但必須標記 |
| `NO_TRADE_INTERVAL_CANDIDATE` | 缺棒，當日 `gap_evidence_status = CANDIDATE_NO_BOARD_LOT_TRADE` | 不可以 | 僅限探索 |
| `SOURCE_MISSING_INTERVAL` | 缺棒，當日為 `DAY_LEVEL_MISSING_VOLUME`、`UNRESOLVED_*` 或隔離日期 | 不可以 | 不可以（當日整天排除於正式研究） |
| `RESEARCH_DERIVED_PRICE` | 由前收延伸出的價格 | 不可以 | 只能作為帶旗標的衍生欄位，不得當成交價 |
| `EXECUTABLE_SLOT` | `REAL_BAR`，量 > 0，且當日 `can_use_for_price_backtest = TRUE` | 可以 | — |

## 4. 時間點與防前視

1. **K 棒可用時間**：5 分 K `bar_start = t` 的資料要到 `t + 5 分鐘` 才完整。13:30 那根是收盤集合競價，要到 13:30 撮合後才可用。重採樣（例如 30 分 K）必須等所屬全部 5 分 K 都完整後才可產生訊號，未結束的 K 棒不得提前使用。
2. **品質遮罩是當日事後資訊**：遮罩用到當天官方報表，`known_at` 約為交易日 20:00。因此：
   - **選股**只能使用前一交易日（含）以前可得的欄位，例如 `prev_official_total_shares`，或用 `prev_trading_date` 的遮罩。
   - 當日遮罩只能用作「資料可用性」條件，也就是排除整天的資料。不得用來挑選標的。
   - 回測報告必須列出因當日遮罩被排除的交易數與其特徵（見第 6 節）。
3. **不可虛構成交**：
   - 訊號不得以產生訊號那根 K 棒的收盤價保證成交。
   - 成交以下一根 `EXECUTABLE_SLOT` 的開盤價為基準，並加上滑價與手續費、證交稅。
   - 若下一根是無成交格點，順延到下一根真實 K 棒，或依策略規則取消。
4. **開盤價、收盤價**：`open_match` 或 `close_match` 為 `FALSE` 時，以官方日 K 開盤價或收盤價為準（`reference/official_volume/v1` 的原始回應）。5 分 K 的第一根開盤價或最後一根收盤價不得當作官方開盤價或收盤價。

## 5. 隔離與排除

- `quarantine_exchange_dates.csv`：完整日有負殘差的交易所日期，目前 13 個（9 個日曆日）。原因是富邦盤中資料多記整張成交。整個交易所日期排除，同日其他股票**不會**自動升級為可信。
- `anomaly_ledger.csv`：其餘未解決的缺棒，逐段列出，並附所需的下一個證據。
- 外幣 ETF（代號第六碼為 K、M、S、C；一張 = 100 單位）：排除。
- 處置分盤（`has_special_auction = TRUE`）：Isolate，不套用 54 根模板。

## 6. 回測報告必須揭露

- 使用的 `evidence_version`、遮罩 SHA-256、`fubon_5m` seq，以及回測 repo 的 commit。
- 樣本保留率，依層級、年份、交易所、流動性分列（`stats_retention_liquidity_exchange_year.csv`），以及排除原因（`stats_exclusion_reasons.csv`）。
- 因當日遮罩被排除的潛在交易數，以及它們與保留樣本在報酬分布上的比較（樣本選擇偏差）。
- 2026 年盤中量短少偏高（見 `stats_volume_quality.csv`）。量能策略在 2026 年需單獨報告。

## 7. 證據等級說明

`VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH` 是**個股日層級**的推論，依據是官方日成交量上限，加上官方最高價、最低價一致。它不是逐根觀察到的「無成交」。前提與證據見 `acceptance_rule_v2.json`。
