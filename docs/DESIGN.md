# tw-market-data-lake 設計（v1.2，2026-10-25）

v1 任務範圍與授權邊界不變：**只抓資料**，不做 30m 聚合、訊號、回測或下單；只寫入本 repo 擁有的 Drive 資料夾；
不修改其他 repository。v1.1 套用 10 項設計修正（下文標示 ①–⑩）。

## 1. 資料集與來源

| dataset key | Drive 路徑（相對 DATA_ROOT） | 來源 | 型態 |
|---|---|---|---|
| `finlab_daily_raw` | `finlab_stock_daily/raw` | FinLab `price:*`（未還原） | append-only，年分區不可變版本 |
| `finlab_daily_adjusted` | `finlab_stock_daily/adjusted` | `TWMarket.get_trading_price(adj=True)` | 快照，content hash 去重 ⑥ |
| `security_categories` | `finlab_stock_reference/security_categories` | FinLab | 目前快照（非 PIT） |
| `disposal_attention` | `finlab_stock_reference/disposal_attention` | FinLab 處置／注意資料集 | 快照；名稱由 smoke 確認，找不到則 fail closed |
| `universe_daily` | `finlab_stock_reference/universe_daily` | 由 raw + 分類衍生 | 快照＋`symbol_master` 表 ② |
| `fubon_symbol_master` | `fubon_stock_reference/symbol_master` | 富邦 `intraday/tickers` 旗標 | 每日快照累積（僅當日狀態） |
| `fubon_5m` | `fubon_stock_5m` | 富邦 `historical/candles` timeframe=5 | `symbol=X/year=YYYY` 不可變版本 |

系統資料夾 `_tw-market-data-lake/`：`locks/`、`runs/`、`staging/`、`quarantine/`、`smoke/`。

## 2. 儲存與發布協定 ③⑤

每個 dataset：

```
<root>/data/<partition>/<content_hash>.parquet        不可變；內容相同 => 同檔名
<root>/_manifest/versions/<seq>_<run_id>/             不可變 manifest 版本（meta.json + 表格）
<root>/_manifest/published.json                       指向目前研究版本（role=current_research_version）
```

寫入順序：本機寫檔 → 讀回逐值驗證 → content hash（以 parquet round trip 後資料定義）→ SHA256 →
**直接以獨占建立（`xb`）+ fsync 寫到 content-addressed 最終路徑（不經 staging、不 rename）** → 驗證 SHA256 →
**最後**寫 manifest 版本並原子替換 `published.json`（fsync 後替換並讀回比對）。

> v1.2（2026-10-25，B1）：稽核發現 copy2→staging→rename 的資料檔在 Drive 上有 83 個成為 0-byte（寫入端讀回時命中
> FUSE 本機快取所以驗證通過；run 結束時的 flush/remount 也沒有救回）。新協定不在 Drive 上 rename 資料檔；
> 未被任何 manifest 引用、內容或大小不對的同名檔（中斷或遺失的寫入）會移到 `quarantine/<run>/<dataset>/orphan_partial/`
> 後重寫；已被引用者照舊拒絕覆蓋（除非 recovery 判定為毀損）。持久性只能在**重新掛載後**驗證：
> `python -m tw_lake.audit.verify`（runner notebook 已自動執行）。故障注入見 `scripts/drive_fault_injection.py`。更新一個年分區就是產生新的檔案與新的 manifest 版本；舊檔永遠保留在磁碟與舊 manifest 中。

Single-writer lock（`locks/writer.lock`，O_EXCL + heartbeat）；每次 manifest commit 前驗證 lock 仍屬本 run。
過期 lock 只能用 `--break-stale-lock` 打破，舊 lock 移到 quarantine。

Recovery scan（每個 dataset、每次 run 開始）：staging 殘留移至 quarantine；驗證 published 版本引用的檔案存在且大小
（`--deep-verify` 時 SHA256）正確；缺檔／毀損的分區會被重建（5m：該 symbol-year 的 window 標 `failed`）；
未被引用的孤兒檔只回報不刪除（內容相同時會被重用）；pointer 未前進的版本回報為 uncommitted。

## 3. Coverage ①④

`coverage_by_window`（真實來源）：`symbol × window`（日曆區塊，預設 6 個月、不跨年；2026-10-23 smoke 顯示 360 天 5m 單次回傳未截斷）。
欄位：`window_start/end, covered_to, status, error_class, error_msg, attempts, gap_days, gap_attempts, n_bars, ...`

狀態：`pending / partial / failed / conflict / complete / complete_with_gaps /
no_data_market_closed / no_data_not_yet_listed / no_data_delisted / no_data_no_trade`。

`coverage_by_symbol`：由 window 表衍生（`complete / needs_update / needs_repair / conflict / failed_exhausted`、
`last_complete_trade_date`、首末 bar）。

規劃順序：**先修復所有 symbol 的 failed、partial、既有 pending（及指定時的 conflict）**，再延伸已完成 window
（從 `covered_to` 當天起重疊一天）與建立新 window。`complete` 只在「資料發布成功＋讀回驗證＋window QA 通過」後設定，
並隨 manifest commit 才成立。

404／空回應分類（依 FinLab 事實）：休市 → `no_data_market_closed`；尚未上市 → `no_data_not_yet_listed`；
已下市 → `no_data_delisted`；上市但當期無成交 → `no_data_no_trade`；其餘 → `failed`（`unknown_404` /
`unknown_empty`），**不會標 complete**。前三種在發送請求前即可判定而略過請求；無成交仍會實際請求驗證。
`unknown_404` 達 `max_window_attempts` 次後列為 exhausted（仍是 failed），需 `--retry-exhausted` 才重試。

Window QA：重複 bar、OHLC 不一致、非正價格、負量、超出請求區間、超出交易時段、非交易日 bar → hard failure（不發布，`partial`）。
FinLab 有成交但無 bar 的日子 → `gap_days`；第一次 `partial`，同樣缺口重抓達 `gap_retry_limit` 次後 → `complete_with_gaps`。
重抓資料與已發布值不同 → `conflict`（保留舊值、不發布、寫 `conflicts_log`，需人工判讀）。

## 4. Point-in-Time Universe ②

`universe_daily` 每列 `date × symbol`（只在該 symbol 的 FinLab 首末價格日之間）：
`has_price`、`has_trade`、`listing_status`（listed）、`tradable_status`（traded / listed_no_trade）、
`listing_status_source=inferred_from_finlab_price`、`instrument_type`、`classification_source`、
`classification_asof`、`classification_is_pit=False`。

`symbol_master`：`first/last_price_date`、`first_price_is_dataset_start`、`listing_status_latest`
（listed / delisted_inferred）、`in_current_security_categories`。不在目前 `security_categories` 的代號以代碼規則推定，
來源標 `code_rule_inferred`。5m 目標＝普通股＋ETF（全部 ETF）且 2023-05-23 後曾有價格。處置期間照常抓。

## 5. 5m raw schema ⑦

`symbol, bar_start(Asia/Taipei), bar_ts_raw, open, high, low, close, volume_raw, volume_unit, avg_price_cum,
turnover, turnover_present, timeframe, bar_label_assumption, adjusted=False, source=fubon`

- `fields` 明確請求 `turnover`；富邦文件只保證日／週／月 K 有 turnover，若分 K 未回傳則存 null 並標 `turnover_present=False`，
  **不以價×量推算**。smoke 回報實際是否提供。
- `volume_unit` 依商品類型（普通股／ETF 分 K = 張）；股數換算只在 QA 中進行。
- 不含抓取時間等易變欄位，使相同來源資料得到相同 content hash；provenance 在 request_log 與 partitions 表。

## 6. QA 對帳 ⑧

`qa_reconciliation_5m`（只涵蓋**本次實際抓取的日期區間**，v1.2 修正）：
`ok`、`no_trade_expected`、`missing_in_source`（FinLab 有成交、無 bar）、
`data_error`（分 K 高低點超出日 K 範圍）、`suspect`（收盤/開盤不一致、分 K 量高於日量、FinLab 無成交卻有 bar）、
`pending_verification`（分 K 量低於門檻或日 K 範圍較寬——**不再**自動歸為 vendor_definition_diff，因 FinLab 日量口徑未經官方驗證）。
單純量的差異不會被判為缺漏；只有 `data_error` 讓 QA status 為 fail。

## 7. 模式

| mode | 寫入 | 說明 |
|---|---|---|
| `smoke` | 只寫 `smoke/<run_id>/` | 能力探測：筆數上限、時間戳語意、收盤競價、turnover、量單位、休市／下市 404 行為、處置資料集名稱 |
| `pilot` | 正式 dataset | 設定檔 pilot 名單＋自動加入下市與代碼推定標的 |
| `full` | 正式 dataset | 需 `--confirm-full`；建議先 `--dry-run` 看估算 |
| `update` | 正式 dataset | 需已有 5m manifest；repair-first 後增量 |

共用旗標：`--dry-run`（不寫 dataset／manifest）、`--symbols`、`--max-requests`、`--break-stale-lock`、
`--retry-conflicts`、`--retry-exhausted`、`--deep-verify`、`--skip-fubon-master`。

## 8. 測試 ⑨

離線（SYNTHETIC_SMOKE_ONLY）：跨年補洞、資料寫入三個階段中斷、manifest commit 中斷（staging 後／pointer 前）、
published 檔案缺失觸發重建、manifest 表毀損 fail closed、兩個 writer 同時啟動、過期 lock、404 分類、
快照去重與還原價改寫、repair-first 順序、預算中斷、衝突保留舊值、FinLab 歷史變動只記錄不覆寫、
dry-run 不寫入、full 需確認、處置資料集缺失 fail closed、smoke 只寫 smoke 資料夾。
CI 在 pandas 2.2 與 3.x 兩種環境執行。

## 9. Smoke 實測結果（2026-10-23，run 20261023_183553_fa5ecc1）

- 360 天 5m（約 1.3 萬筆）單次回傳完整 → 無筆數上限疑慮，`window_months` 改為 6。
- 時間戳為 bar 起點；每日 54 根：09:00–13:20 連續撮合 53 根 ＋ **13:30 一根＝收盤集合競價**（13:25 無 bar）。下游 30m 聚合須把 13:30 視為競價。
- 分 K **沒有 turnover** → 欄位存 null、`turnover_present=False`。
- 5m 量 ×1000 ÷ FinLab 成交股數：2330 中位數 0.87、0050 0.94、6488 0.96 → 推測 FinLab 日量含盤後定價／鉅額／零股等口徑（未經官方驗證；v1.2 起標 `pending_verification`），非缺漏。
- 6488 有 20 天每日僅 15 根 → 處置期間分盤撮合，資料照抓，下游以處置表判讀。
- FinLab ETF 在 `security_categories` 的 market 為 `etf`（category `domestic_etf`／`foreign_etf`）；v1.1.1 修正分類，ETF 納入 5m 目標。
- 處置／注意：`disposal_information`（事件表 8,125 列）、`etl:disposal_stock_filter`、`etl:noticed_stock_filter`（每日布林矩陣）可用；`notice_information` 不存在。
- 已下市股（例 3202，最後價格 2025-04-02）富邦仍有分 K；休市區間回 404 → `no_data_market_closed`。

### Full 第 1 段（2026-10-24，run 20261024_102239_d16da15）

- 9,000 次請求、約 53 次／分；3,326 萬根 bar；無 conflict、無 data_error。
- Recovery 偵測到 pilot 時寫入的 3 個檔案在 Drive 上變成 0 byte（size 檢查），已自動隔離並重寫。
  推測為 Colab Drive FUSE 非同步上傳未完成；notebook 執行後改為 `drive.flush_and_unmount()`。
- 551 個 window 有 source gap（179 檔、3,089 個交易日），集中在低流動性股票：FinLab 有成交量但盤中無成交，
  很可能僅有盤後定價／鉅額成交。依規則重抓 2 次相同後轉 `complete_with_gaps`。
- 23 個 window `bar_outside_session`（皆為非電子類小型股）；v1.1.2 起錯誤訊息記錄違規 bar 時間，
  並把被拒資料存到 quarantine 供判讀，再決定是否放寬時段規則。

### Full 第 2 段（2026-10-24，run 20261024_144318_a9e54e9）

- 8,929 次請求，所有 window 都處理過（pending 0）；無 conflict、無 data_error；recovery 乾淨。
- 第 1 段的 551 個缺日 window 重抓結果相同 → `complete_with_gaps`（規則如預期收斂）。
- 197 個 `bar_outside_session`：違規 bar 落在 13:35–14:55，且集中在各股上市當期的 window →
  富邦也回傳**興櫃時期**（上市前）的分 K（興櫃交易至 15:00、量單位為股）。
  v1.1.3 起只保留 FinLab 首／末價格日之間（上市櫃期間）的 bar，其餘排除並記錄於
  `excluded_outside_listing_5m`；被擋的 window 下一次 update 會重抓修復。

## 10. 尚待真實資料確認

1. 富邦單次回傳筆數上限（決定 `window_months`；改變需在首次 pilot 前）
2. 分 K 時間戳是 bar 起點或終點；13:30 收盤競價呈現方式
3. 分 K 是否回傳 turnover
4. 已下市股票在富邦是否仍有 2023-05 後的分 K
5. FinLab 處置／注意資料集正確名稱
6. FinLab 還原價函式在安裝版本的實際名稱
7. 富邦 API Key 登入函式名稱（`apikey_login` 未經實測；密碼＋憑證登入沿用既有 pipeline 做法）
