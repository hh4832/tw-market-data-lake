# 研究 Repo 介面規格（B2 M7，設計文件，待簽核）

本文件規定**獨立、唯讀**的研究或回測 repo 如何讀取本 lake。30 分 K 重採樣、訊號與回測引擎都在那個 repo 實作，本 repo 不做（AGENTS.md 規則 1）。本輪只鎖定介面，不要求建立完整引擎。

## 1. 固定輸入（pin）

研究 repo 啟動時必須讀取並驗證以下項目。任何一項不符就停止（fail-closed）：

| 項目 | 固定值 |
|---|---|
| `fubon_5m` published seq | 68（`versions/000068_20261025_211531_4ceb32f`） |
| 遮罩資料夾 | `audits/quality_acceptance/quality_mask_20261030_014633_fbec159` |
| `quality_mask_symbol_day.parquet` SHA-256 | `9ec79fe02412c25dae56572a334c167c168e374a438dd71e9f4dc0e138ac6863` |
| `evidence_version` | `quality_v1.0.0+eligibility_63ffd6f+recon_c111ab2+price_price_check_20261029_235425_4308c60` |
| 契約版本 | `docs/backtest_data_contract.json` 的 `version`（目前 `1.1.0-draft`），並記錄本 repo commit |
| 列數 | symbol_days 1,788,899；price 1,733,539；volume 1,707,922；Level A 212,428 |

驗證方式與 B1 相同（`tw_lake.audit.integration_check`）：

- 檢查 SHA-256；
- 確認 `published.json` 的 seq 與遮罩 seq 一致；
- 重算列數；
- 檢查遮罩不變量。

研究 repo 以唯讀方式掛載資料，不得寫回 lake。

## 2. 30 分時間桶

### 2.1 預期格點

正常日盤每天有 54 個預期的 5 分格點：
- 連續交易 53 格，`bar_start` 為 09:00、09:05……13:20；
- 13:30 收盤集合競價 1 格。

13:25 不是格點：13:25–13:30 是收盤前的集合競價委託時段，期間沒有成交。

| 時間桶 | 預期格點（`bar_start`） | 格數 |
|---|---|---|
| `[09:00, 09:30)` … `[12:30, 13:00)` | 每桶 6 格，例如 09:00、09:05……09:25 | 6 × 8 |
| `[13:00, 13:30)`（最後盤中桶） | **只有 13:00、13:05、13:10、13:15、13:20** | 5 |
| `CLOSE_AUCTION` | 13:30（收盤集合競價），獨立成桶，**不**併入 `[13:00, 13:30)` | 1 |

- 只由 `fubon_5m` 的 `REAL_BAR` 重採樣，不使用 forward-fill 的格點。
- 每個時間桶記錄：
  - `n_real_bars` 與 `n_expected_bars`；
  - `has_accepted_gap`：桶內有 `NO_TRADE_INTERVAL_ACCEPTED_FOR_RESEARCH` 格點；
  - `has_source_missing`：桶內有 `SOURCE_MISSING_INTERVAL` 格點。
- 開高低收只由桶內真實 K 棒計算，量為真實 K 棒量的加總。桶內沒有真實 K 棒時，價格為 NaN，不得延用前值。

### 2.2 時間欄位（三者分開）

| 欄位 | 定義 | 用途 |
|---|---|---|
| `bucket_end` | 桶內**最後一個預期格點**的 `bar_start + 5 分鐘`。一般桶為起點 + 30 分；最後盤中桶為 13:25；`CLOSE_AUCTION` 為 13:30 | 時間桶的名目結束時間 |
| `bar_end` | 桶內**最後一根真實 K 棒**的 `bar_start + 5 分鐘`；桶內沒有真實 K 棒時為 NaT | 只供參考與稽核，**不得**用來判斷可用時間 |
| `available_at` | 一般桶與最後盤中桶：`bucket_end`。`CLOSE_AUCTION`：13:30 撮合結果公布之後，契約以 `13:30 + 0` 為下限，研究 repo 可加上延遲但不得提前 | 訊號只能在 `available_at` 當下或之後使用 |

規則：

- `available_at` **不得**由 `bar_end` 推定。最後幾格沒有 K 棒時，要等到 `bucket_end`，才能知道那些格點確實沒有成交；在那之前，「之後不會再有成交」屬於前視。
- `available_at ≥ bucket_end ≥ bar_end` 恆成立。
- 桶內有 `SOURCE_MISSING_INTERVAL` 時，仍以 `bucket_end` 為 `available_at`，但該桶標為不完整，不得用於正式訊號。

## 3. 合成成交場景（引擎驗收用）

引擎建立時，必須先以合成資料通過以下場景，才能讀取真實資料：

| 編號 | 場景 | 預期 |
|---|---|---|
| S1 | 訊號在 30 分桶的 `available_at` 產生 | 以 `available_at` 之後第一個 `EXECUTABLE_SLOT` 的開盤價作為**成交模擬基準**，不用本桶收盤價。這個基準**不是保證成交**：還要加上滑價、手續費與證交稅，並受量能限制與漲跌停可成交性限制（B4 解決前標為非正式）。模擬結果不得宣稱能以該價成交 |
| S2 | 下一根是 `NO_TRADE_INTERVAL_ACCEPTED_FOR_RESEARCH` | 順延到下一根真實 K 棒，或依規則取消；不得以研究衍生價格成交 |
| S3 | 下一根是 `SOURCE_MISSING_INTERVAL` | 不成交；當日若非 Level B 則整天不交易 |
| S4 | 收盤集合競價（`CLOSE_AUCTION`） | (a) 由 `CLOSE_AUCTION` 結果（13:30 才知道）產生的訊號，**不得**在同一次集合競價成交，最早只能在下一交易日第一個 `EXECUTABLE_SLOT`。(b) 由最後盤中桶（`available_at` 13:25）產生、目標為 13:30 集合競價的委託：本 lake 未驗證收盤集合競價的委託截止時間與可執行性，因此一律 **fail-closed**，不成交。策略可事先宣告順延到下一交易日，或取消。研究 repo 若要啟用 (b)，必須先提出截止時間的官方依據並另行簽核。(c) 不得使用未到 `available_at` 的桶 |
| S5 | 當日遮罩把該個股日排除 | 記為 INVALIDATED；選股與訊號完全不讀當日遮罩 |
| S6 | `price_quality_status = RANGE_ONLY` | 不得做正式盤中成交 |
| S7 | 隔離日期（凍結清單 v1） | 不交易，也不以同日其他股票推論 |
| S8 | 多日持有跨越公司行動、漲跌停 | B3 與 B4 解決前，標為非正式結果 |

### 3.1 Known-answer 合成測試

`tests/test_research_interface_spec.py` 以 tests 內的參考實作（spec oracle）固定以下答案。這段程式**只在 tests 內**，不是 lake 的輸出，也不是回測引擎。研究 repo 的引擎必須得到相同答案。

| 測試 | 情境 | 答案 |
|---|---|---|
| K1 提前結束的桶 | `[09:30, 10:00)` 只有 09:30、09:35 兩根真實 K 棒，其餘為已接受的無成交格點 | `bar_end` = 09:40，`bucket_end` = `available_at` = 10:00。09:45 的訊號看不到這個桶；10:00 的訊號可以，成交基準為 10:00 那根的開盤價 |
| K2 最後盤中桶與收盤集合競價分離 | 13:00–13:20 五根加上 13:30 一根 | `[13:00, 13:30)` 的預期格點只有 5 格，`available_at` = 13:25，開高低收量不含 13:30；`CLOSE_AUCTION` 單獨一桶，`available_at` = 13:30 |
| K3 同次集合競價前視成交禁止 | 用 `CLOSE_AUCTION` 結果產生的訊號，或在 13:25 產生、目標為 13:30 的委託 | 前者不得在同日 13:30 成交，順延時為次日 09:00 開盤基準；後者在截止時間未驗證時不成交（fail-closed） |

## 4. Fail-closed 規則

以下任一情況發生時，研究 repo 必須停止，或把該資料標成不可用。不得用猜測補齊：

1. 固定輸入的 SHA-256、seq 或列數不符。
2. 遮罩欄位出現契約未列的值，或 `evidence_version` 不只一個。
3. 需要的欄位為 `UNKNOWN` 或 NaN（一律不視為 TRUE）。
4. 時間桶的 `available_at` 晚於訊號時間，或研究 repo 以 `bar_end` 代替 `available_at`。
5. 個股日不在遮罩中。
6. 契約版本與研究 repo 宣告的版本不同。
7. 委託目標為收盤集合競價，但委託截止時間與可執行性未經驗證（S4）。

## 5. 不在本輪範圍

- 完整回測引擎、策略、績效報告。
- B3 公司行動、B4 漲跌停與升降單位、B5 樣本空間完整性。這三項仍是獨立的 blocking issues。
