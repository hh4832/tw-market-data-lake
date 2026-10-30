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

- 只由 `fubon_5m` 的 `REAL_BAR` 重採樣，不使用 forward-fill 的格點。
- 時間桶以 09:00 為起點，左閉右開：`[09:00, 09:30)`、`[09:30, 10:00)`……`[13:00, 13:30)`。
- **13:30 收盤集合競價那根 5 分 K 獨立成一桶 `CLOSE_AUCTION`**，不併入 `[13:00, 13:30)`。
- 每個時間桶記錄：
  - `n_real_bars` 與 `n_expected_bars`；
  - `has_accepted_gap`：桶內含 `NO_TRADE_INTERVAL_ACCEPTED_FOR_RESEARCH` 格點；
  - `has_source_missing`：桶內含 `SOURCE_MISSING_INTERVAL` 格點。
- 開高低收只由桶內真實 K 棒計算，量為真實 K 棒量的加總。桶內沒有真實 K 棒時，價格為 NaN，不得延用前值。
- `available_at` 為桶內最後一根 5 分 K 的 `bar_start + 5 分鐘`。`CLOSE_AUCTION` 則為 13:30 撮合完成之後。訊號只能在 `available_at` 之後使用。

## 3. 合成成交場景（引擎驗收用）

引擎建立時，必須先以合成資料通過以下場景，才能讀取真實資料：

| 編號 | 場景 | 預期 |
|---|---|---|
| S1 | 訊號在 30 分桶收盤時產生 | 以下一個 `EXECUTABLE_SLOT` 開盤價成交，不用本桶收盤價 |
| S2 | 下一根是 `NO_TRADE_INTERVAL_ACCEPTED_FOR_RESEARCH` | 順延到下一根真實 K 棒，或依規則取消；不得以研究衍生價格成交 |
| S3 | 下一根是 `SOURCE_MISSING_INTERVAL` | 不成交；當日若非 Level B 則整天不交易 |
| S4 | 訊號在 13:25 之後 | 只能在 `CLOSE_AUCTION` 成交或順延到次日開盤，依策略規則；不得使用未完成的桶 |
| S5 | 當日遮罩把該個股日排除 | 記為 INVALIDATED；選股與訊號完全不讀當日遮罩 |
| S6 | `price_quality_status = RANGE_ONLY` | 不得做正式盤中成交 |
| S7 | 隔離日期（凍結清單 v1） | 不交易，也不以同日其他股票推論 |
| S8 | 多日持有跨越公司行動、漲跌停 | B3 與 B4 解決前，標為非正式結果 |

## 4. Fail-closed 規則

以下任一情況發生時，研究 repo 必須停止，或把該資料標成不可用。不得用猜測補齊：

1. 固定輸入的 SHA-256、seq 或列數不符。
2. 遮罩欄位出現契約未列的值，或 `evidence_version` 不只一個。
3. 需要的欄位為 `UNKNOWN` 或 NaN（一律不視為 TRUE）。
4. 時間桶的 `available_at` 晚於訊號時間。
5. 個股日不在遮罩中。
6. 契約版本與研究 repo 宣告的版本不同。

## 5. 不在本輪範圍

- 完整回測引擎、策略、績效報告。
- B3 公司行動、B4 漲跌停與升降單位、B5 樣本空間完整性。這三項仍是獨立的 blocking issues。
