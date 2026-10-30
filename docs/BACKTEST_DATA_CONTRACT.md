# Backtest Data Contract — tw-market-data-lake 5m（v1.1.0，草案，待 B2 最終驗收）

本文件規定**獨立回測 repo** 如何使用本 lake 的資料。本 repo 只提供資料與品質遮罩，不實作策略或回測引擎（見 AGENTS.md 規則 1）。研究 repo 的介面規格見 `docs/RESEARCH_REPO_INTERFACE.md`。

- 機器可讀版：`docs/backtest_data_contract.json`。
- 遮罩檔：`quality_mask_symbol_day.parquet`，由 `tw_lake.audit.quality_mask` 產生。每次執行都寫入新的版本化資料夾，並附 `quality_manifest.json`（含 SHA-256）。
- v1.1.0 只修改文字與語義，適用於既有正式遮罩 `quality_mask_20261030_014633_fbec159`（quality 1.0.0，未重跑、未覆寫）。對正式遮罩的讀法修正見第 9 節勘誤。

## 0. 狀態詞彙（B2 M1）

| 狀態 | 意義 | 是否逐根證明 |
|---|---|---|
| `VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH` | **條件式研究接受**。VERIFIED 指交易所日期與規則證據被接受；CANDIDATE 指缺棒並未逐根證實無成交。條件為第 7 節 P1–P8 全部成立 | **否** |
| `CANDIDATE_NO_BOARD_LOT_TRADE` | 只有量的上限，價格區間未佐證 | 否，僅探索 |
| `DAY_LEVEL_MISSING_VOLUME` | 參考資料完整的日期上，短少整張；不知道是哪一根 | 否，僅探索 |
| `DAY_LEVEL_MISSING_VOLUME_REFETCH_CONSISTENT_ONLY` | 同上，但定價參考只做到「重抓一致」（quality ≥ 1.1.0） | 否，僅探索 |
| `REFETCH_CONSISTENT` | 重新請求 HTTP 200 且數值相同；證明可重現，**不證明完整** | — |
| `REFERENCE_COMPLETE`（欄位值 `COMPLETE`） | 報表經獨立來源或官方公告證實完整 | — |

規則：

- `VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH` **永遠不得**自動轉成逐根層級的 `NO_TRADE_INTERVAL_EVIDENCED`。
- no-trade candidate 不等於 evidenced。
- `UNKNOWN` 一律不得視為 `TRUE`。

## 1. 輸入與版本

回測必須固定以下版本，並在結果中記錄：

| 項目 | 來源 |
|---|---|
| 5 分 K | `fubon_5m`，以 `source_manifest_sequence` 指定 published 版本（目前為 seq 68） |
| 品質遮罩 | `quality_mask_symbol_day.parquet`，其 `evidence_version` 與 `quality_manifest.json` 的 SHA-256（正式版 `9ec79fe0…6863`） |
| 官方日資料 | `reference/official_volume/v1`（每日 shard，含原始回應 SHA-256） |

## 2. 樣本層級

| 層級 | 欄位 | 用途 | 條件 |
|---|---|---|---|
| Level A（嚴格對照） | `level_A_strict` | 穩健性檢查 | 正常盤、54 根 K 棒全到、交易所日期 `VERIFIED`、個股殘差 = 0、`price_quality_status = PASS`、前一日有官方紀錄 |
| Level B + Price Gate | `level_B_price_gate`（= `can_use_for_price_backtest`） | **只限單日（盤中）價格研究**的主要樣本 | 正常盤、單位已核實、交易所日期 `VERIFIED` 或 `NO_OVERCOUNT` 且未隔離、缺棒只允許 `VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH`、`price_quality_status = PASS`、前一日有官方紀錄 |
| Volume Strict（盤中量能研究） | `volume_strict`（= `can_use_for_volume_backtest`） | 量能型策略 | Level B + Price Gate，且 `volume_reconciliation_status = EXACT` |
| Isolate | `research_tier = ISOLATE` | 不可混入；如需研究須另行建模 | 特殊交易制度（處置分盤集合競價）、隔離日期、未解決異常、無官方紀錄 |
| Exploratory only | `research_tier = EXPLORATORY_ONLY` | 只能探索，不得作為正式結論 | 其餘，例如價格只有區間一致、缺棒只是 `CANDIDATE` 或 `DAY_LEVEL_*`、量短少 |

**多日持有**的研究在 B3（公司行動）、B4（漲跌停與升降單位）、B5（樣本空間完整性與存活者偏差）解決前，不屬正式結果。

### 2.1 量能（B2 M3）

- Volume Strict 仍然只接受 `EXACT`。`volume_reliable` 的正式布林語義不變：等於 `volume_reconciliation_status == EXACT`。
- 0.5% 容忍度**不得**用於正式樣本。研究 repo 可自行由既有欄位衍生 `volume_tolerance_exploratory`：狀態屬於 `EXACT`、`WITHIN_ONE_LOT` 或 `SHORTFALL_WHOLE_LOTS`，且 `intraday_volume_shortfall_ratio ≤ 0.005`。
- 這個旗標只能作探索與敏感度分析。它不是遮罩欄位，也不改變任何正式層級。

## 3. 格點型態

缺棒格點**不可**直接 forward-fill 成可成交的 K 棒。

| 型態 | 判定 | 可否成交 | 可否用於指標 |
|---|---|---|---|
| `REAL_BAR` | 5 分 K 存在 | 可以：以下一根真實 K 棒開盤價成交（見第 4 節） | 可以 |
| `NO_TRADE_INTERVAL_ACCEPTED_FOR_RESEARCH` | 缺棒，且當日 `gap_evidence_status = VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH` | 不可以 | 量 = 0；價格只能是帶旗標的「研究衍生」值。屬條件式接受，不是逐根證據 |
| `NO_TRADE_INTERVAL_CANDIDATE` | 缺棒，當日 `gap_evidence_status = CANDIDATE_NO_BOARD_LOT_TRADE` | 不可以 | 僅限探索 |
| `SOURCE_MISSING_INTERVAL` | 缺棒，當日為 `DAY_LEVEL_*`、`UNRESOLVED_*` 或隔離日期 | 不可以 | 不可以（當日整天排除於正式研究） |
| `RESEARCH_DERIVED_PRICE` | 由前收延伸出的價格 | 不可以 | 只能作為帶旗標的衍生欄位，不得當成交價 |
| `EXECUTABLE_SLOT` | `REAL_BAR`，量 > 0，且當日 `can_use_for_price_backtest = TRUE` | 可以 | — |

`NO_TRADE_INTERVAL_EVIDENCED` 保留給將來的逐根證據（例如官方逐筆或官方 5 分資料）。目前沒有任何狀態對應到它。

## 4. 時間點與防前視

1. **K 棒可用時間**：5 分 K `bar_start = t` 的資料要到 `t + 5 分鐘` 才完整。13:30 那根是收盤集合競價，要到 13:30 撮合後才可用。重採樣（例如 30 分 K）必須等所屬全部 5 分 K 都完整後才可產生訊號，未結束的 K 棒不得提前使用。
2. **Level B 是事後資料可用性遮罩（B2 M5）**：遮罩用到當天官方報表，`known_at` 約為交易日 20:00。因此：
   - **選股（PIT universe）與訊號**只能使用前一交易日（含）以前可得的欄位，例如 `prev_official_total_shares`，或 `prev_trading_date` 的遮罩。**不得**看當日遮罩。
   - 當日遮罩只能在事後排除整天的資料，不得用來挑選、排序標的或作為訊號輸入。
   - 因當日遮罩被排除的交易記為 **INVALIDATED**，必須依第 6 節揭露。
3. **不可虛構成交**：
   - 訊號不得以產生訊號那根 K 棒的收盤價保證成交。
   - 成交以下一根 `EXECUTABLE_SLOT` 的開盤價為基準，並加上滑價與手續費、證交稅。
   - 若下一根是無成交格點，順延到下一根真實 K 棒，或依策略規則取消。
4. **官方日 K（B2 M4）**：
   - 原始 5 分 K 的 OHLC **不可**被官方日 K 改寫。
   - 官方日 K 只提供獨立的日頻 view `daily_official`，`known_at` 為當日官方公布後（契約假設 20:00）。它只能用於日頻研究。
   - v1.0.0 的「`open_match` 或 `close_match` 為 FALSE 時以官方開收盤為準」**已刪除**，盤中不得使用。
   - `RANGE_ONLY` 的個股日不得用於正式盤中成交。
   - 5 分 K 的第一根開盤價或最後一根收盤價不等於官方開盤價或收盤價。

## 5. 隔離與排除

### 5.1 凍結隔離清單 v1（B2 M2）

以下 13 個交易所日期（9 個日曆日）整日隔離。原因是富邦盤中資料多記整張成交。程式常數為 `QUARANTINE_FROZEN_V1`，即使將來的計算判為 `VERIFIED` 也維持隔離。

| 交易所 | 日期 |
|---|---|
| TPEX | 2023-08-24、2026-01-27、2026-01-29、2026-05-12 |
| TWSE | 2023-05-25、2023-07-19、2023-08-24、2023-08-28、2023-12-25、2024-02-02、2026-01-27、2026-01-29、2026-05-12 |

- 同日其他股票**不會**自動升級為可信。
- 要解除任何個股或日期的隔離，只能另立新版本，並且需要獨立證據與單獨簽核。不得在原清單上直接修改。

### 5.2 其他排除

- `anomaly_ledger.csv`：其餘未解決的缺棒，逐段列出，並附所需的下一個證據。
- 外幣 ETF（代號第六碼為 K、M、S、C；一張 = 100 單位）：排除。
- 處置分盤（`has_special_auction = TRUE`）：Isolate，不套用 54 根模板。

## 6. 回測報告必須揭露

- 使用的 `evidence_version`、遮罩 SHA-256、`fubon_5m` seq，以及回測 repo 的 commit。
- 樣本保留率，依層級、年份、交易所、流動性分列（`stats_retention_liquidity_exchange_year.csv`），以及排除原因（`stats_exclusion_reasons.csv`）。
- **INVALIDATED 交易（B2 M5）**：當日遮罩造成的排除可能與報酬相關（例如異常日、流動性差的日子較常被排除），因此可能產生樣本選擇偏差。報告必須列出：
  - 筆數與占全部候選交易的比例；
  - 曝險（名目金額、持有時間）；
  - 與保留樣本在流動性、年份、交易所上的差異；
  - 敏感度：排除 INVALIDATED 與以保守代理值計入兩種結果的差距。
- 2026 年盤中量短少偏高（見 `stats_volume_quality.csv`）。量能策略在 2026 年需單獨報告。
- 若使用 `volume_tolerance_exploratory`，必須標為探索結果。

## 7. 證據等級與不確定性（P1–P8）

`VERIFIED_CANDIDATE_ACCEPTED_FOR_RESEARCH` 是**個股日層級**的推論，依據是官方日成交量上限，加上官方最高價、最低價一致。它不是逐根觀察到的「無成交」。

| 前提 | 內容 | 剩餘不確定性 |
|---|---|---|
| P1 官方組成 | 官方日總量 = 一般交易（含 13:30 收盤集合競價）+ 定價 + 零股（盤中、盤後）+ 鉅額 | 組成說明可能變動。若某成分同時不在官方總量與我們的成分表中，無法偵測。FinLab 是同一交易所資料的轉載，**不是獨立真值** |
| P2 單位 | 各成分正確換算為股，一張 = 1,000 股 | 只有已記錄單位判定的成分才檢查過；其他標為 `UNIT_UNVERIFIED_EXCLUDED` |
| P3 無重複計數 | 已存 5 分 K 量不超過真實一般交易量 | 負殘差集中在少數交易所日期（屬同日共同的來源錯誤），個股日不是獨立試驗，**不主張**二項分配或 rule-of-three 上限。小於可偵測負殘差的多記無法排除 |
| P4 日期歸屬 | 成交歸到正確交易日 | 若某筆成交被移到隔日，而隔日剛好也漏了同量的整張，兩天的 r 都可能為 0 |
| P5 不相互抵銷 | r = 0 不會隱藏被多記抵銷的成交 | 見反例 1、2 |
| P6 成分未高估 | 量到的成分不超過真實成分 | 成分高估恰好一張可使 r = 0。BFT41U 重抓一致只是 `REFETCH_CONSISTENT` |
| P7 交易制度 | 正常連續交易 + 13:30 收盤集合競價 | 開盤第一根與 13:30 只有在價格不同時才會被開收盤比對抓到 |
| P8 價格佐證 | 隱藏成交若在區間外，會改變官方最高價或最低價 | 隱藏成交價若在既有高低區間內，可通過此關 |

### 7.1 反例測試規格（GPT 14.2）

`tests/test_rule_v2_counterexamples.py` 以合成資料固定以下情境。測試結果是「規則接受了實際有成交的個股日」，用來防止文件宣稱逐根證明：

1. 缺棒真實有 1,000 股，另一根多記 1,000 股 → r = 0，官方高低一致，判為 ACCEPTED。
2. 缺棒有 1,000 股，某成分高估 1,000 股 → r = 0，判為 ACCEPTED。
3. 隱藏成交價落在既有高低區間內 → 通過 High/Low 關；區間外則會被抓到（對照組）。
4. 邊界：
   - 開盤第一根缺：價格相同時無法偵測；價格不同時由 `open_match` 排除。
   - 13:30 收盤集合競價缺：同上，由 `close_match` 排除。
   - 鄰日歸屬：前一天移來的一張可抵銷隔日漏掉的一張，判為 ACCEPTED。
   - 處置分盤與非標準交易單位：不評估，直接排除。
5. 負殘差集中在隔離日：`negative_residual_clustering` 回報集中程度，`independent_trials_bound_valid = False`，`acceptance_rule_v2.json` 不再出現 rule-of-three 上限。

## 8. 參考資料完整度（B2 M6）

- `REFETCH_CONSISTENT`：重抓結果一致，只證明可重現。
- `REFERENCE_COMPLETE`：經獨立來源或官方公告證實完整。
- 單憑重抓一致，不得認定正式完整。quality ≥ 1.1.0 的程式：
  - `reference_fix` 寫入 `REFETCH_CONSISTENT`；
  - 紀錄檔改名為 `reference_refetch_consistency.csv`；
  - 這些日期上短少整張的個股日判為 `DAY_LEVEL_MISSING_VOLUME_REFETCH_CONSISTENT_ONLY`。
- 這個變更只影響 `gap_evidence_status` 的標籤。`can_use_*`、`level_*` 與 `research_tier` 不變（兩者都屬 EXPLORATORY_ONLY），有測試保證。
- 若將來要改變正式遮罩，必須另立版本與驗收。

## 9. 勘誤：正式遮罩 quality 1.0.0 的讀法

正式遮罩不重跑、不覆寫，讀取時依下列方式解讀：

| 檔案或欄位 | 1.0.0 寫法 | 應讀為 |
|---|---|---|
| `reference_completeness` 在 TWSE 2024-02-05、2025-04-07、2025-04-10 | `COMPLETE` | `REFETCH_CONSISTENT` |
| `reference_completeness_cleared.csv` 的 `cleared = True` | cleared | 一致性異常已釐清（重抓一致），不是完整 |
| 上述三日的 `gap_evidence_status = DAY_LEVEL_MISSING_VOLUME` | DAY_LEVEL | `DAY_LEVEL_MISSING_VOLUME_REFETCH_CONSISTENT_ONLY` |
| `acceptance_rule_v2.json` P1「independent source」 | 獨立來源 | 同源轉載，不是獨立真值 |
| `acceptance_rule_v2.json` P3「rule-of-three 95 % upper bound」 | 統計上限 | 撤回，不主張 |
| 遮罩內附 `BACKTEST_DATA_CONTRACT.md` 第 4.4 節「以官方開收盤為準」 | 盤中可覆寫 | 撤回；只有日頻 `daily_official` view |
| 遮罩內附契約第 3 節 `NO_TRADE_INTERVAL_EVIDENCED` | 逐根證據 | `NO_TRADE_INTERVAL_ACCEPTED_FOR_RESEARCH` |

可交易旗標（`can_use_for_price_backtest`、`can_use_for_volume_backtest`、`level_A_strict`）與筆數均不受影響，因此 B1 PASS 仍然有效。
