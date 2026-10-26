# Data Quality Audit v0.1.0（Phase A，唯讀）

這份文件涵蓋三件事：（1）稽核工具的規格、（2）2026-10-25 已實測的發現（附證據與分母）、（3）Phase B 修正**提案**。
本文件不表示資料已可用於研究（research-ready）。Phase B 的任何程式修改或回補，都要另外取得明確授權才會執行。

## 1. 唯讀保證（怎麼做到、怎麼驗證）

| 保證 | 做法 |
|---|---|
| 不寫 DATA_ROOT | `tw_lake.audit` 沒有任何寫入原始資料的呼叫；所有輸出都寫到 `--out-dir`（預設在 Colab 本機 `/content/tw_lake_audits/<audit_id>/`）。`_guard_out_dir` 會拒絕把輸出寫進 dataset、受保護資料夾、`locks/ runs/ staging/ quarantine/ smoke/`，也拒絕寫進已存在且非空的資料夾。 |
| 選用的 Drive 副本 | 只在 `--publish-to-drive` 時，才在稽核完成後把結果**複製**到新資料夾 `_tw-market-data-lake/audits/<audit_id>/`。這個路徑不屬於 ingestion 會讀寫的範圍，也不會覆蓋任何東西。 |
| 不呼叫 API | 不 import `fubon_source` / `finlab_source`，不需要任何憑證。 |
| 有證據 | 稽核開始與結束各做一次 fingerprint：pointer 與目前發布的 manifest 版本記錄 size、mtime 與 SHA256；locks/staging/quarantine/runs 第一層記錄 size 與 mtime；**受保護資料夾完全不讀**（AGENTS.md 規則 2）；manifest 引用的資料檔記錄 size 與 mtime。比對結果寫在 `audit_summary.json → findings.source_unchanged`。 |
| 測試 | `tests/test_audit.py::test_audit_is_read_only...` 對整個 DATA_ROOT 樹做前後 SHA256 比對，必須完全相同。2026-10-25 的鏡像實測是用另一個非 root 使用者、對唯讀檔案系統執行，執行成功。 |

所有讀取都從 `_manifest/published.json` → `versions/<seq>_<run_id>/partitions.parquet` 開始，不會 glob `data/` 去判斷哪些檔案有效。

## 2. 執行

- Colab：開 `notebooks/TW_Market_Data_Lake_Audit.ipynb`（branch `audit/data-quality-v0.1.0`）。先設 `SYMBOLS="2330,0050,8299"` 試跑，再全量執行。
- CLI：`python -m tw_lake.audit.cli --data-root "$DATA_ROOT" --out-dir /content/tw_lake_audits [--symbols ...] [--deep-verify] [--publish-to-drive]`
- 可重現：同一份程式碼加上同一組 manifest，輸出就相同。SHA 抽樣用固定 seed（`20261025`）。

### 輸出

| 檔案 | 內容 |
|---|---|
| `README.md` | 來源 manifest 的 seq / version_dir / run_id、commit、時間（Asia/Taipei）、參數、data_root、範圍、重跑指令、log |
| `audit_summary.md/.json` | 判定（不宣稱 research-ready）、依 critical/warning/info 排序的發現表（含 count、分母，以及 basis：measured / inferred / unknown）、交班統計重建、對帳摘要、readiness、限制 |
| `audit_integrity.parquet` | A1：每個 dataset/partition/check 的 pass/fail/warn 與 severity |
| `audit_5m_by_symbol_day.parquet` | A2：每個 symbol/date 一列；欄位：`expected_policy`、`actual_bar_count`、`expected_bar_count`（只在可定義時才有值）、`missing_timestamps`、`unexpected_timestamps`、`intraday_completeness_ratio`、`regime_unknown`、`quality_status`、`partition`、`manifest_seq` |
| `audit_5m_templates.parquet` | 每天從模板標的（2330/0050/006208）學到的正常時段時間點，以及這幾檔彼此是否一致 |
| `audit_ohlcv_reconciliation.parquet` / `audit_ohlcv_top100.*` | A3：5 分K重建的日 OHLCV 對 FinLab 原始日K，保留原值、差額與 ratio；異常前 100 筆 |
| `audit_universe_issues.parquet`、`audit_field_provenance.parquet` | A4：分類與上下市問題；每個欄位的 provenance（observed_at_date / reconstructed_ex_post / known_asof）與 known_at |
| `audit_excluded_bars.parquet`、`audit_listing_boundary_days.parquet` | A4：被排除 bars 與上市日的距離及風險標記；quarantine 中的原始窗在上市日前後 ±5 個交易日的情況 |
| `audit_gap_cases.parquet`、`audit_partial_windows.parquet`、`audit_backfill_candidates.parquet` | A5：缺口分類、partial 窗缺口清單與建議回補範圍、0-byte/毀損分區的回補候選與 request 估計 |
| `trading_regime_daily.parquet` | symbol/date/regime/auction_interval/effective_from/effective_to/source/known_at/expected_bar_policy |
| `audit_research_ready.parquet` | 衍生指標：`ready_daily_only` / `ready_5m_normal_session` / `ready_30m_resample` 與不合格原因（只出現在 audit 輸出，不回寫來源） |

## 3. 檢查定義

**A1 儲存與 manifest**
- pointer 與 meta 的 seq / run_id 一致；version_dir 存在。
- manifest 各表的 SHA256 與列數。
- 沒有未提交的 version，沒有 `.staging` 殘留。
- partition 不重複；路徑等於 `data/<partition>/<content_hash[:32]>.parquet`。
- 檔案存在，大小等於 `file_bytes`（**0-byte 直接列為 critical**），SHA256 抽樣或全量。
- 檔案可讀；列數、key 範圍、重複 key、schema 一致。
- 能否只用 manifest 重建交班統計。

**A2 5 分K完整性**
- 期望時間點不寫死成 54 根，而是從模板標的逐日學習。模板缺日時標記 `regime_unknown`；處置的分盤集合競價期間，期望根數一律記為 unknown。
- `quality_status` 依序判定：
  - `invalid`：有重複、不在 5 分鐘格點、在時段外、OHLC 不合理、價格非正、量為負、或落在非交易日；
  - `no_bars_finlab_traded`；
  - `bars_without_finlab_trade`；
  - `regime_call_auction_unverified`；
  - `regime_unknown`；
  - `intraday_missing`（同一天缺任何一根就算）；
  - `complete`。
- 富邦不會輸出沒有成交的 5 分鐘區間（實測：樣本中沒有任何 0 量 bar）。因此「合法的無成交空格」和「來源缺漏」無法只靠 5 分K區分，這類列會標 `missing_cause=unverified_no_trade_or_source_gap`。

**A3 OHLCV 對帳**
- 量的換算依每列的 `volume_unit`（lot ×1000）。
- 分類優先順序：
  1. `data_error_candidate`：5 分K高低點超出日K區間；
  2. `suspect`：收盤或開盤不一致、5 分K量大於日量、FinLab 沒成交但有 bars、或非交易日有 bars；
  3. `missing_5m_day`；
  4. `pending_verification`：ratio 低於門檻，或日K區間比 5 分K寬；
  5. `ok`。
- **不再自動歸為 `vendor_definition_diff`。**門檻（預設 0.95–1.02）只是探索用，並按年份與流動性分層呈現。

**A4 Universe**
- 分類是當前快照套用到歷史，**不是 PIT**。
- 上下市日是用首尾價格日事後推得（`reconstructed_ex_post`）。
- `has_trade` 要到當日收盤後才知道，不能用來做當日開盤前的篩選（`usable_for_same_day_pre_open_selection=False`）。
- ETF 依代號字尾分子類：L 槓桿、R 反向、B 債券、U 期貨/商品、A 主動，以及 D/K/T/C 等。
- 列出 "00" 規則覆蓋 FinLab category 的標的。
- 被排除的 bars：與首個價格日的交易日距離，以及是否有興櫃時段（最後一根 >13:30）的證據。

**A5 缺口與處置**
- `trading_regime_disposal`
- `no_regular_session_trade_probable`：FinLab 量 < 1 張，代表當天不可能有整股成交。
- `source_missing_suspected`：量 ≥ 50 張且沒有處置。
- `unresolved`：其他情況，包含 FinLab 缺值。
- 「兩次相同的 API 結果」只記為 `api_repeat_identical` 證據，不代表完整。

## 4. 2026-10-25 實測發現（稽核前的證據收集）

方法：透過 Google Drive 連接器讀取各 dataset 的 pointer、manifest 表與樣本資料檔，並驗證 SHA256。另外用 Drive API 的 metadata 列出 2026-10-24 09:46Z 之後建立的全部檔案大小。FinLab 年度日K檔超過連接器 10 MB 的上限，**沒有讀到**，所以 A3 的全量對帳要等 Colab 跑稽核。

| # | 嚴重度 | 發現 | 證據 / 分母 | 依據 |
|---|---|---|---|---|
| F1 | **critical** | `fubon_5m@seq=63` 引用的分區中，有 **83 個在 Drive 上是 0-byte**，合計 manifest 列數 335,771 根、83 檔 | 最後一次 run `20261024_174628_684410f` 建立的 198 個分區中有 83 個是 0 byte（Drive API `fileSize=0`；下載其中一檔回傳空內容）。另外 115 個大小與 manifest 相符。年份分布：2023:3、2024:25、2025:24、2026:31 | 實測 |
| F2 | **critical** | `security_categories@seq=4` 的 snapshot 檔是 0-byte | 檔案 `f774e83f…parquet`，manifest 記錄 85,725 bytes，Drive 上是 0 | 實測 |
| F3 | 資訊 | 其餘 manifest 表與樣本資料檔的 SHA256 全部相符 | 7 個 dataset 的 pointer 與 meta；樣本 8 檔（2330 2024/2025、0050、006208、1101、8299 2024/2025、disposal_information） | 實測 |
| F4 | 資訊 | 交班統計可以只靠 manifest 重建，數字完全吻合 | 19,072 個窗；狀態 15,859 / 1,602 / 1,458 / 142 / 8 / 3；61,562,619 根；symbol-year 的 n_bars 與 partition n_rows 不一致 0 筆 | 實測 |
| F5 | **warning**（QA 程式缺陷） | 最後一次 run 的 `qa_reconciliation_5m` 把 30,646 個 symbol-day 判為 `missing_in_source`，其中 **25,324 筆是假陽性** | 對帳區間取了每檔所有 fetch 範圍的最小到最大，但只放入本次抓到的 5 分K，中間沒重抓的窗就被當成缺漏（例：3632 的 2024-01 窗有 955 根，仍被判缺）。只有 5,322 筆對得上 manifest 的 gap_days | 實測 |
| F6 | 資訊 | 高流動性標的每天都是 54 根：09:00–13:20 共 53 根，加 13:30 一根，13:25 沒有 bar。13:30 那根 100% 是 O=H=L=C，量的中位數約為 13:20 的 5.4 倍，**實證上就是收盤集合競價** | 2330 有 485 天；0050、006208、1101 樣本 | 實測 |
| F7 | 資訊 | 處置分盤會讓根數變少：8299 在 2025-10-13～10-27（20 分鐘分盤）每天 15 根；5 分鐘分盤期間仍是 54 根 | 對上 `disposal_information` 的生效期間 | 實測 |
| F8 | 資訊 | 0050 在 2025-06-11～06-17 沒有 5 分K（分割停止交易）；1101 在 2025-08-13 沒有 5 分K | 和 006208 的交易日比對 | 實測；原因屬推測，需要官方公告佐證 |
| F9 | warning | 2254（2023-07 窗）被排除的 674 根 bars，最後一根是 13:20，緊鄰首個價格日 2023-10-20，**沒有興櫃時段的證據**。另外 196 個被排除的窗都有 >13:30 的興櫃時段證據 | `excluded_outside_listing_5m.csv`（run 684410f）共 197 窗 / 406,544 根。逐根被排除的資料沒有保存 | 實測 + 推測 |
| F10 | warning | 每一檔的分類都是當前快照（`classification_is_pit=False`）；383 檔的下市狀態是靠價格中斷推得 | 共 2,789 檔 | 實測 |
| F11 | 資訊 | 3 個 partial 窗：2941 2024-01（7 個缺口日）、7631 2025-01（1 日，而且該年分區正是 F1 的 0-byte 檔之一）、7782 2025-07（1 日） | `coverage_by_window` | 實測 |

**F1 和 F2 的機制推測（待故障注入驗證）**：`publish_partition` 的流程是先 `copy2` 到 Drive 上的 staging，再 `os.replace` 到最終路徑，接著對最終路徑做 SHA256。在 Colab 的 Drive FUSE 上，讀取會命中本機快取，所以驗證會通過；但非同步上傳若遇到「寫入後馬上 rename」，可能只留下 0-byte 的佔位檔。同一次 run 的 manifest 表（用整個目錄 copytree 再 rename）以及 run 產物都有正常上傳。也就是說，**單機上的 SHA256 驗證無法證明檔案在 Drive 端已經持久化**。

**影響**：下一次 `update` 開始時，recovery 會依大小不符偵測到這些檔案，把對應窗標成 FAILED 並重新抓取（約 166 個窗的 request，大約 3 分鐘）。在那之前，研究讀取這 83 個 symbol-year 和 security_categories 都會失敗。

## 5. 無法證明 / 需要外部官方資料

- 一個缺少的 5 分鐘時間點，究竟是沒有成交還是來源缺漏：需要逐筆成交或官方盤中資料。
- FinLab `成交股數` 是否包含盤後定價、鉅額、零股：需要 TWSE/TPEx 官方日成交資料逐項比對。
- 官方的上市、上櫃、下市、轉板日期，以及停止交易日：需要 TWSE/TPEx 的歷史主檔，並建立 `known_at` 模型。
- 處置分盤期間應有幾根 bar：需要撮合規則加上官方撮合紀錄。
- Drive 跨機器的原子性：需要第 6 節的故障注入測試；單機離線測試不能證明。

## 6. 故障注入測試計畫（Drive FUSE，Phase B 前執行）

這些測試都在一個**獨立的測試 DATA_ROOT**（例如 `MyDrive/00Quant_Research/_lake_fault_test/`）進行，絕不碰正式資料。每一項的判準相同：**下一次 run 能偵測並修復，或明確拒絕；研究讀取端絕不會讀到不完整的內容。**

| 情境 | 注入方式 | 驗證 |
|---|---|---|
| 寫入後立刻 rename | 照現行流程寫 200 個小檔，完成後立刻 `flush_and_unmount`，再重新 mount 並用 Drive API 列出大小 | 0-byte 的比例；與「直接寫最終檔名、不 rename」的版本比較 |
| 上傳途中斷線 | 寫入期間用 `kill -9` 結束 kernel / 中斷 runtime | 新機器重新 mount 後，recovery 能分類，pointer 只指向完整的版本 |
| 0-byte 或截斷檔 | 測試根目錄中把已發布檔案截斷 | `update` 的 recovery 加上稽核 A1 都能抓到；讀取端拒讀 |
| 兩個 writer 同時跑 | 兩台 Colab 同時跑 `update` | 第二個因 `LockHeldError` 拒絕；stale lock 只能用 `--break-stale-lock` 強制解除 |
| stale lock | 寫入一個 heartbeat 過期的 lock | 預設拒絕；加旗標後才接手，且舊 lock 會被隔離到 quarantine |
| manifest 發布到一半中斷 | 在 `manifest_after_staging` 與 `manifest_before_pointer` 兩個點注入例外 | pointer 不前進；殘留檔被隔離；下次 run 重建 |
| 跨機器 | A 機寫入後立即中斷，B 機（新 runtime）接續 | B 看到的檔案大小、SHA256 和 manifest 一致；不一致時會進入修復 |

**驗證方法**：除了 FUSE 上的 stat 之外，還要**重新 mount 後**再 stat，或用 Drive API 讀 `fileSize` 交叉確認。

## 7. 5m → 30m 重採樣規格（草案，Phase B 實作）

- 時段：09:00–13:30，切成 9 個 30 分鐘桶：09:00、09:30、…、13:00。最後一桶 13:00–13:30 包含 13:00–13:20 的 5 根，**再加上 13:30 收盤集合競價那根**，共 6 根。
- 桶的時間標記用開始時間；桶在 `end + data_latency` 之後才算可得。即時資料端以富邦 5 分K的完成時間為準，回測時要模擬「桶結束才看得到」。
- 只有桶內每一根 5 分K都存在，該桶才算完整；不完整的桶標 incomplete，不補值、不前推。
- 訊號在桶完成之後才產生；最早可成交的時間是下一個可交易時點。最後一桶的訊號只能在次日開盤執行，或另外定義收盤競價規則。**嚴禁用還沒完成的 K 棒交易。**
- 處置分盤日、`regime_unknown` 日，預設排除在 30m 研究之外（`ready_30m_resample=False`）。

## 8. Phase B 狀態（2026-10-25，branch `phase-b/durable-publish-qa`）

| 項目 | 狀態 | commit |
|---|---|---|
| B1 發布協定：資料檔不 rename（獨占寫入 + fsync）、孤兒/0-byte 檔隔離後重寫、manifest 表 fsync、pointer 讀回比對、重新掛載後驗證 `tw_lake.audit.verify`（runner 自動執行）、故障注入 script/notebook | **已實作；Drive 實測待使用者執行故障注入 notebook** | `77d6e73` |
| B2 回補 83 個空檔 symbol-year（166 窗）+ security_categories | **完成（2026-10-25）**：run `20261025_140026_c169ddf` 重抓後 82 檔仍 0-byte（F12，同路徑重寫）；修正 PR #3 後第二次 update：重新掛載後 verify `fubon_5m seq=67 checked=82 failures=0`、`security_categories seq=6 checked=1 failures=0`，RESULT OK | PR #3 |
| B3 run QA：只對帳本次抓取區間；`vendor_definition_diff` → `pending_verification`；收盤/開盤不一致 → suspect | 已實作 | `17a4969` |
| B4 `coverage_by_window.data_quality_status`（complete_with_gaps = source_gap_confirmed_by_repeat）＋ `research_ready=see_audit` | 已實作（不改變排程行為） | `a572d6b` |
| B5 `universe_daily_v2`（前一交易日欄位、provenance；v1 不動） | 已實作；下次 run 首次發布 | `a86c31f` |
| B6 官方上市/下市/轉板/停止交易日資料源 | 未做：需先選定資料源（TWSE/TPEx 公開資料或付費源） | — |
| B7 被排除 bars 逐根保存到 quarantine | 已實作 | `a572d6b` |
| B8 5m→30m 重採樣 | 未做：AGENTS.md 規則 1 規定本 repo 不產出 30m 聚合；規格見第 7 節，建議放在研究 repo | — |
| 稽核效能：regime / 處置矩陣向量化（~100 s → ~1 s） | 已實作 | `96959f9` |

**F12（2026-10-25 下午，回補 run `20261025_140026_c169ddf`）**：recovery 正確偵測 83 個毀損檔並重抓（165 requests），但修復流程「把壞檔移到 quarantine，再在原路徑寫新檔」的 82 個 5m 檔在 Drive 上**仍是 0-byte**（Drive 上原路徑保留舊 file id、修改時間更新、大小 0）；同一 run 寫到新路徑的 40 個 universe 檔全部正常。結論：在 Drive FUSE 上，**被 rename/move 過的路徑不可再寫**。修正（v1.2.1）：壞檔不動，修復資料寫到 `<hash32>-<run_id>.parquet` 新路徑；故障注入 T1 新增 `move_then_same_path`（舊）與 `keep_bad_fresh_path`（新）兩種協定對照。

**B1 的限制**：「不 rename 就不會出現 0-byte」目前是依證據推測，須以故障注入 notebook 在真實 Drive 上驗證（T1 比較新舊協定；T2/T3 用真實 store 程式碼）。
即使新協定 PASS，每次 run 之後仍以重新掛載後的 verify 為準。

## 9. 全量稽核結果與 readiness v0.2（2026-10-25 晚）

全量稽核 `audit_20261025_163404_e53478a`（5,885 s）：critical 0、integrity 179 項全 pass、來源未變動、partial 窗 0。
5m 嚴格 readiness（`ready_5m_normal_session`）只有 21.83%：1,374,511 / 1,788,899 個 symbol-day 是 `intraday_missing`。
高流動標的接近 100%（2330 99.9%、2317/0050/00878/2603/1101 100%、2454 98%），低流動標的偏低。原因是富邦不輸出無成交的 5 分鐘區間，而不是資料毀損。

**F13（外幣計價 ETF 成交量單位）**：對帳 suspect 5,588 天中，00636K / 00668K / 00657K 各約 817 天、00687C 422 天，旗標 `volume_above_daily`。
這些天開盤與收盤完全相同，5m 成交量剛好是 FinLab 日成交股數的 10 倍，推測是單位或交易單位定義不同。待官方資料查證，查證前這幾檔的 5m 成交量不得使用。
不自動修正。

**readiness v0.2（`tw_lake.audit.readiness`，探索性）**：新增 `ready_5m_no_trade_tolerant` / `ready_30m_no_trade_tolerant`，嚴格旗標保留不變；容忍版是嚴格版的超集合。
`intraday_missing` 日只有在以下條件全部成立時，才把缺的區間視為「無成交」：

| 條件 | 理由 |
|---|---|
| 對帳 `ok`（量比在 band 內；開、收相同；5m 高低 = 日高低） | 缺的 bar 若含當日極值，會變成 `daily_range_wider_than_5m` → 拒絕 |
| （v0.2.0 才要求）有 13:30 收盤集合競價 bar；v0.2.1 預設不要求 | 對帳 ok 已要求 5m 最後收盤 = 日收盤 |
| 分區完整性 OK、正常盤（`intraday_missing` 已隱含 bars 有效且有模板） | |
| `missing_count` ≤ 流動性分層上限（v0.2.1）：ge5000=0、500–5000=10、50–500 與 <50 不設上限 | 高流動標的缺區間不可信為無成交 |

**限制**：量比 band（0.95–1.02）看不到成交量占當日 <5% 的單根缺 bar。所以 500–5000 張的分層容許 3 根是取捨，不是證明。
`audit_5m_missing_by_liquidity` 是校準表，上限可用 `--missing-allowance` 調整。
30m：某 30 分鐘桶的 5m 全缺時，視為無成交桶（沿用前收、量 0）；此規則屬研究 repo 的重採樣規格，本 repo 不產出 30m。

重算不必重跑稽核：`python -m tw_lake.audit.rescore --audit-dir <audit 資料夾>`（只讀稽核輸出，寫入新的 `rescore_*` 資料夾；Audit notebook 第 8–9 格）。

**校準結果（rescore 2026-10-25 19:06，預設上限）**：嚴格 21.83% → 容忍 47.37%。
關鍵觀察：在 500–5000 與 50–500 兩層，缺棒數從 0 根到 30 根以上，對帳 `ok` 的比例都維持在 90–97%，沒有隨缺棒數增加而下降。若缺的 bar 有實際成交，對帳 ok 比例應該會隨缺棒數下降；這個證據支持缺棒多為無成交區間。
例外：ge5000 層缺 31 根以上的 248 天，對帳 ok 只有 40 天，應視為真實異常。
容忍版的主要拒絕原因：分層上限（50–500：391,379；500–5000：181,138）、對帳 pending（168,135）、無 13:30 bar（157,770）。
新增 `--no-require-close-auction`：冷門股常沒有收盤集合競價成交，而對帳 ok 已要求 5m 最後收盤 = 日收盤。

**v0.2.1 預設（rescore 2026-10-25 19:18）**：上限 ge5000=0、500–5000=10、50–500 與 <50 不設上限，不要求 13:30 bar。
容忍版可用率 84.66%（1,514,416 / 1,788,899），嚴格版不變（21.83%）。各分層容忍版：ge5000 92.43%、500–5000 84.74%、50–500 90.79%、<50 67.84%。
<50 張的日子中位數缺 46/54 根，5m 幾乎是空的。技術上可重採樣，但研究價值低，策略端應另設流動性門檻，readiness 不代替流動性篩選。

**缺口日分類修正（2026-10-25）**：FinLab 有成交股數但沒有收盤價，表示當天只有鉅額或盤後定價交易，沒有一般盤中成交。成交股數多為整數張，例如 500,000、700,001、6,000,002 股。
這類日子現在歸為 `no_regular_session_trade_probable`，evidence 會加上 `finlab_close_missing=True`。
以全量稽核資料重算：source_missing_suspected 14 → 3（00720B 2025-06-23、4716 2025-06-23、3454 2026-03-30），unresolved 510 → 1（6144 2026-06-23），其餘歸為無一般盤成交。
經使用者授權，以 `--refetch-gap-days` 重抓這 15 天：原 14 天加 6144，共 14 個視窗。

## 10. 缺棒證據帳本 v0（Gap Evidence Ledger，2026-10-26）

起因：GPT 複核要求，每一個應有行情的 5 分格點沒有 K 棒時，都要有可追溯的原因與證據。84.66% 是研究篩選比例，不是缺口解釋完成率。

`python -m tw_lake.audit.gap_ledger`（唯讀，Gap Ledger notebook 第 1–6 格）

- **分母**：5m 時期中，FinLab 有成交或富邦有 K 棒的股票日。正常盤且有模板的日子，期待模板內所有格點：09:00–13:20 連續格點，加上 13:30 收盤集合競價（獨立的格點類別）。處置分盤日記為 `special_auction_or_halt`，不套 54 根；沒有模板的日子記為 `regime_unknown`。FinLab 成交量為 0 的掛牌日另外計數，不展開成格點。
- **格點狀態**：
  - `REAL_BAR`
  - `PROVIDER_MISSING_EVIDENCED`：明列的 4 天整日缺漏，只證明富邦缺資料，不是無成交證明
  - `UNRESOLVED`：其餘全部
  
  `evidence_candidate` 只是提示，不會改變狀態：`volume_conserved_exact` / `volume_shortfall` / `volume_excess` / `finlab_close_missing` / `whole_day_no_bars`。
- **成交量守恆**：FinLab 日成交股數減去 5m 加總，差值精確到 1 股才算 exact。exact 能否成為「缺棒內無成交」的證據，取決於 FinLab 日量包含哪些交易（一般盤；鉅額、盤後定價、零股是否包含）。帳本在「所有格點都有 K 棒」的日子實測 `conservation_on_complete_days`，不做假設。
- **邏輯遺漏偵測**：
  - systemic slots：同一日同一格點，高流動股有 ≥20% 缺棒
  - date spikes：整日缺漏比例或高流動缺棒比例異常偏高的日期（robust z > 4）
  - 模板外 K 棒
- **證據欄位**：`symbol, date, start_slot, end_slot, bar_start_from/to, n_slots, status, evidence_level, evidence_candidate, evidence_source, request_id, response_hash, manifest_seq, checked_at, known_at, notes, research_eligible`。連續缺棒壓縮成區間儲存，可還原每一個格點。歷史抓取沒有保存 request id 和 response hash，這兩欄為 null。
- **輸出**：`gap_day_summary`（每股票日一列）、`gap_runs/part-*.parquet`、`gap_slot_crosssection`、`gap_systemic_slots`、`gap_date_spikes`、`gap_ledger_summary.json/.md`。

`python -m tw_lake.audit.api_spotcheck`（唯讀 API，notebook 第 7–9 格）

- 從帳本分層抽樣：高流動有缺棒、量不足、量守恆但有缺棒、整日缺漏、完整日對照組，另加指定日。每個股票日向富邦歷史 K 線查詢 1 次，逐格點比對。
- 判定：`logic_loss`（API 有、我們沒存，屬程式遺漏）、`value_mismatch`、`stored_not_in_api`、`consistent`。每個回應都存 JSON 並記 SHA-256。`consistent` 只表示富邦現在給的與已存的一致，不是無成交證據。

**尚未解決**：歷史逐筆證據。富邦/Fugle 歷史端點只有 K 線，逐筆只有當日。要把 `UNRESOLVED` 升級成無成交證據，需要官方日量組成（一般盤、鉅額、盤後定價、零股）或交易所歷史逐筆資料。來源與取得方式待使用者決定。
