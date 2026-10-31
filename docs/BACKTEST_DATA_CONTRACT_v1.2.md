# Backtest Data Contract — tw-market-data-lake（v1.2.0，草案，待 B3a／B4a／B5a 驗收）

本文件是 v1.1.0 的**增補版**。v1.1.0（`docs/BACKTEST_DATA_CONTRACT.md`、`docs/backtest_data_contract.json`）與正式遮罩 `quality_mask_20261030_014633_fbec159`（quality 1.0.0）**不修改、不重跑、不追溯**。

v1.2 只新增三個獨立、版本化的衍生資料集，以及研究 repo 讀取它們的規則：
- B3a 公司行動 v2；
- B4a 交易規則；
- B5a PIT universe。

- 機器可讀版：`docs/backtest_data_contract_v1.2.json`。
- 本 repo 仍不實作策略或回測引擎（AGENTS.md 規則 1）。
- 在 B3／B4／B5 驗收結論出來之前，不得宣稱任何正式可執行回測績效。

## 0. 與 v1.1.0／quality 1.0.0 的相容性

| 項目 | v1.2 的處理 |
|---|---|
| 遮罩欄位 `can_use_*`、`research_tier`、`level_*` | 不變，語義照 v1.1.0 |
| 單日（盤中）研究 | 照 v1.1.0；v1.2 的資料集**只會再收緊**，不放寬 |
| 多日持有 | 照 v1.1.0 仍不屬正式結果。v1.2 定義的是解除限制的**條件**（第 2–5 節），條件要等驗收通過才生效 |
| B2 凍結隔離清單 `QUARANTINE_FROZEN_V1` | 不變；B3a 研究隔離（8101）是另一份清單，兩者不合併 |
| `UNKNOWN` | 一律不得視為 `TRUE`（同 v1.1.0） |

## 1. 固定來源與 SHA-256

回測必須記錄下列每一項的路徑、版本與 `manifest.json` 中的 SHA-256。

| 資料集 | 位置（Drive `_tw-market-data-lake/` 之下） | 版本 | 證據來源 |
|---|---|---|---|
| 5 分 K | `fubon_5m` published | seq 68 | 不變 |
| 品質遮罩 | `audits/quality_acceptance/quality_mask_20261030_014633_fbec159/` | quality 1.0.0（`9ec79fe0…6863`） | 不變 |
| 公司行動 v2 | `derived/corporate_actions_v2/corporate_actions_v2_<run>/` | `V2_VERSION` 2.0.0 | `reference/corporate_actions/v1` 封存回應（每列帶 `raw_sha256`） |
| 交易規則 | `derived/trading_rules/trading_rules_<run>/` | `RULES_VERSION` 1.0.0 | `reference/official_volume/v1` 封存的 MI_INDEX／dailyQuotes，加上公司行動表 |
| PIT universe | `audits/universe_pit/universe_pit_<run>/` | `PIT_VERSION` 1.0.0 | 同上，加 FinLab raw 與 gap ledger |

- 每個衍生資料夾都是新建、不可覆寫。
- 寫入程式拒絕寫在 raw、遮罩或 reference 目錄之內。
- `manifest.json` 記錄 code commit 與每個檔案的 SHA-256。

## 2. 公司行動：價格、數量與現金流（B3a）

### 2.1 三個日期分開

| 欄位 | 意義 |
|---|---|
| `official_effective_date` | 官方表上印的日期。**永不改動** |
| `effective_market_session` | 該日或之後第一個市場交易時段。官方日期可能是休市日，例如颱風假 |
| `first_observed_trade_date` | 該檔在 `effective_market_session` 當天或之後第一次有成交的日期。只作診斷，**不移動因子** |

### 2.2 價格因子

- 定義：`price_factor = 官方參考價 / 官方除權息（停止買賣）前收盤價`。
- 訊號用還原價：
  `signal_adjusted_price(t) = raw_price(t) × Π price_factor`
  連乘的範圍是所有同時滿足以下兩個條件的事件：
  - `effective_market_session > t`；
  - `available_at ≤ as_of`。
- 參考實作：`tw_lake.audit.corporate_actions_v2.signal_adjusted`。
- 成交與損益一律用**未還原**的原始價格；因子從不寫回任何儲存價格。

### 2.3 會計狀態（逐事件）

| `accounting_status` | 條件 | 可否跨事件持有 |
|---|---|---|
| `SUPPORTED` | 官方欄位齊備：價格因子、配股比例、每股現金都有，而且不涉及任何認購權 | 可以，數量與現金流依官方欄位計算 |
| `PRICE_ONLY` | 只有價格因子可信。例如 TWSE 配股（無比例欄位）、減資、面額變更、ETF 分割 | **不可**做正式跨事件持有；只可用於訊號還原 |
| `UNSUPPORTED` | 涉及現金增資或認購權；配股但參考價高於前收；或在觀測日曆之外 | **不可** |

- 混合型事件（權息、配股加增資）只有在現金、股票、認購欄位**全部**齊備時，才可能是 `SUPPORTED`。
- 正式跨事件持有的閘門：`holding_allowed(events, symbol, entry, exit)`。持有期間內每一個事件都必須是 `SUPPORTED`，而且不可與研究隔離重疊。
- 數量與現金：`shares' = shares × (1 + stock_per_share)`，`cash' = cash + shares × cash_per_share`。
  - 這只適用於 `SUPPORTED`；
  - 現金入帳日照 `effective_market_session`；實際發放日未取得，研究 repo 必須揭露這個簡化。

### 2.4 研究隔離（與 B2 分開）

`research_quarantine.csv` 目前只有一筆：

- **對象：** 8101，2024-08-22 至 2024-12-31。
- **原因：** 停牌後以 1.90→10.45 恢復，連續 13 根漲停，官方沒有任何事件或暫停紀錄；FinLab 在 2024-11-19 的 5.0 倍因子無法解釋。
- **範圍：** 研究訊號、評價、跨期持有一律排除。

## 3. PIT 因子可得時間

| 欄位 | 意義 |
|---|---|
| `available_at` | `effective_market_session` 當天 08:30（Asia/Taipei） |
| `available_at_policy` | 寫明 08:30 是**政策**：參考價是當日開盤基準。它**不是**觀測到的公告時間 |
| `source_published_at` | 歷史公告時間未觀測，一律 `null` |
| `retrieved_at` | 封存回應的實際抓取時間 |

規則：
- 在 `available_at` 之前，任何訊號都不得使用該事件的因子。
- 若研究需要更早的資訊，例如除權息公告日，必須另取官方公告資料，不得用本欄位推回。

## 4. 漲跌停狀態與可成交性（B4a）

### 4.1 `price_limit_status`：漲跌停帶是否有效

| 值 | 可用 | 說明 |
|---|---|---|
| `VERIFIED_OFFICIAL_EVENT` | 是 | 官方公司行動表當日的漲跌停價 |
| `VERIFIED_OFFICIAL_NEXT_DAY` | 是 | 前一時段官方行情公布的「次日漲跌停價」 |
| `NO_LIMIT_VERIFIED`、`NO_LIMIT_OFFICIAL_CLASS` | 是 | 官方表顯示無漲跌停（9999.95／0.01） |
| `NO_LIMIT_NEW_LISTING` | 是 | 依官方上市日，上市後前五個交易時段 |
| `DERIVED` | 是（需揭露） | 以官方參考價套用升降單位與漲跌幅推算，且商品類別已知 |
| `DERIVED_CONFLICT` | **否** | 推算的區間與觀測到的最高價、最低價矛盾 |
| `UNKNOWN_INSTRUMENT_CLASS` | **否** | ETF、ETN、REIT 沒有官方漲跌停證據，失敗即關閉 |
| `UNKNOWN_NO_REFERENCE` | **否** | 沒有官方參考價 |

推算時的參考價只能來自兩個地方：
- 前一市場時段的官方收盤，而且該檔在那個時段有成交；或
- 當日官方收盤減官方漲跌。

**永不**往前補用更早的收盤價。

### 4.2 可成交性與有效性分開

`buy_eligibility`／`sell_eligibility` 是日層級的摘要，**不是成交保證**：

| 值 | 條件 |
|---|---|
| `UNKNOWN` | 漲跌停狀態不可用 |
| `BLOCKED_NO_TRADE` | 當日無成交 |
| `BLOCKED_LOCKED_LIMIT_UP`／`_DOWN` | 最高價 = 最低價 = 漲停（跌停） |
| `CONDITIONAL_LIMIT_TOUCHED` | 觸及漲跌停，只能部分或排隊成交 |
| `CONDITIONAL_NO_INTRADAY_RANGE` | 沒有最高、最低價可判斷 |
| `ELIGIBLE_SUBJECT_TO_LIQUIDITY` | 其餘；仍受流動性限制 |

### 4.3 收盤集合競價

- 收盤集合競價的時間是 13:25–13:30，可能延後收盤到 13:33。
- 使用當日收盤價的訊號，**永遠不得**在同一場收盤競價成交。
- 最早的執行機會是下一個交易時段的開盤競價，而且不保證成交。
- 參考實作：`trading_rules.earliest_execution`。

## 5. 市場成員與下市來源（B5a）

- 每日成員名單 = 官方日行情表中有列出的代號（`official_listed`）。
- 範圍：股票、ETF、ETN、REIT、TDR、特別股，加上 FinLab 有收錄的任何代號；權證、債券等其他代號不在範圍內，只在 `summary.json` 計數。
- 下列欄位分開記錄，不合併成單一的「在 universe 內」：
  - `official_traded`
  - `halted_official`（TWSE TWTAWU、TPEx sprc／chtm）
  - `finlab_price`
  - `fubon_bars`
  - `delisted_official`
- `last_observed_price_date` 與 `official_delisting_date` 分開保存。
- 轉板（同日在一個交易所下市、在另一個上市）不算終止。
- 下市或無價格時**不得**以最後可得價格自動平倉。評價狀態依 `valuation_status`：
  - `DELISTED_NO_EXIT_PRICE`
  - `UNPRICED_HALTED`
  - `UNPRICED_LISTED_NO_TRADE`
  - `UNPRICED_NO_SOURCE`

  研究 repo 必須自行建模出場，例如以官方下市日、清算或另取價格，並揭露所用的方法。
- 0.1% 的總缺漏率**不足以**證明沒有存活者偏差。必須另看依下列維度拆分的缺漏率：
  - 終止狀態（`DELISTED_OFFICIAL`、`STOPPED_NO_OFFICIAL_RECORD`、`ACTIVE`）
  - 證券類型
  - 流動性
  - 年度

## 6. INVALIDATED 交易的會計

v1.1.0 第 4、6 節的 INVALIDATED 規則不變。v1.2 新增下列四種 INVALIDATED 原因，回報時必須分開計數：

1. 持有期間內有 `PRICE_ONLY` 或 `UNSUPPORTED` 事件 → `INVALIDATED_CORPORATE_ACTION`。
2. 進場或出場當日的 `buy_eligibility`／`sell_eligibility` 為 `UNKNOWN` → `INVALIDATED_LIMIT_UNKNOWN`。
3. 持有期間內遇到研究隔離 → `INVALIDATED_RESEARCH_QUARANTINE`。
4. 持有期間內遇到 `DELISTED_NO_EXIT_PRICE` → `INVALIDATED_NO_EXIT_PRICE`。

**揭露要求：**
- 回報各類 INVALIDATED 的筆數。
- 回報它們占總交易的比例。
- 做敏感度分析：比較「排除這些交易」與「以保守代理值計入」兩種結果，並報告差距。

被 `BLOCKED_*` 擋下的委託不得記為成交，應依策略規則延後或放棄，並揭露所用的處理方式。

## 7. 失敗即關閉

| 情況 | 處理 |
|---|---|
| 檔案的 SHA-256 與 manifest 不符 | 拒絕載入 |
| 事件的 `effective_market_session` 為空 | `UNSUPPORTED` |
| 基金類商品無官方漲跌停證據 | `UNKNOWN_INSTRUMENT_CLASS` |
| 前一時段無價格，也沒有官方漲跌停 | `UNKNOWN_NO_REFERENCE`，不往前補值 |
| 推算的區間與觀測矛盾 | `DERIVED_CONFLICT` → `UNKNOWN` |
| 官方表該日缺少 | `OFFICIAL_TABLE_MISSING`，不推論為無成交 |
| 任何 `UNKNOWN` | 不得視為 `TRUE` |

## 8. 待驗收項目（v1.2 生效前）

- B3a、B4a、B5a 三個 Colab 的實際計數，以及 GPT 的 PASS／CONDITIONAL／NO-GO 判定。
- 尚待補充的官方資料：
  - TWSE 配股比例與現金拆分（TWT49U 詳細資料端點）；
  - 減資換股率與退還股款（TWTAVU／TPEx decap 歷史）；
  - 2023-09-14 以前的 TWSE 暫停交易紀錄；
  - TWSE 基金類漲跌停分類；
  - 創新板上市來源；
  - 實際股利發放日。
