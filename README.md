# tw-market-data-lake

台股（上市櫃普通股＋全部 ETF）**資料擷取專用** repo：

- FinLab 日 K（原始價 append-only、還原價版本快照）
- 證券分類快照、處置／注意股歷史、Point-in-Time universe
- 富邦 5 分 K（2023-05-23 起，含處置期間）
- 富邦當日注意／處置旗標快照（只代表當天，非歷史）

資料寫到 Google Drive `00Quant_Research/data/`（`DATA_ROOT`）。不做 30m 聚合、訊號、回測或下單。
設計細節見 [`docs/DESIGN.md`](docs/DESIGN.md)。

## Drive 結構

```
00Quant_Research/data/
├── finlab_stock_daily/{raw,adjusted}/
├── finlab_stock_reference/{security_categories,disposal_attention,universe_daily}/
├── fubon_stock_reference/symbol_master/
├── fubon_stock_5m/                       data/symbol=XXXX/year=YYYY/<hash>.parquet
└── _tw-market-data-lake/{locks,runs,staging,quarantine,smoke}/
```

既有的 `finlab_0050/`、`finlab_txf_calendar/`、`finlab_futures_afterhours/`、`fubon_txf/`、`IKBR/` 不會被讀寫（程式內有寫入防護）。

每個 dataset 的 `_manifest/published.json` 指向目前的研究版本；更早的版本與檔案都保留。

## 使用方式（Colab）

開啟 `notebooks/TW_Market_Data_Lake_Runner.ipynb`，設定 Colab Secrets：
`FINLAB_API_TOKEN`、`FUBON_USER_ID`、`FUBON_PASSWORD`（或 `FUBON_API_KEY`）、`FUBON_CERT_PASSWORD`（選填），
並把 `FUBON_PFX_DRIVE_PATH` 改成你的憑證位置。依序執行：

1. `MODE="smoke"`：只寫 `_tw-market-data-lake/smoke/`。確認 `capability_report.json` 的
   `recommendations`（筆數上限 → `window_months`、`bar_label`、分 K 是否有 turnover）以及處置資料集名稱。
   若需調整，改 `config/default.yaml` 並 commit 後**再**跑 pilot（`window_months` 與請求參數在首次發布後不可更改）。
2. `MODE="pilot"`：約 20 檔全期間，檢查 `runs/<run_id>/qa_summary.md`。
3. `MODE="full"`、`DRY_RUN=True`：看預估請求數與時間。
4. `MODE="full"`、`CONFIRM_FULL=True`：一次回補。可用 `MAX_REQUESTS` 分段，中斷後重跑會從 manifest 接續。
5. 之後任何時間（例如兩個月後）`MODE="update"`：先修復所有失敗／不完整的 window，再從各 window 的
   `covered_to` 當天起補到最新可用日（16:30 前執行時為前一交易日）。

命令列（repo 根目錄，已安裝依賴）：

```bash
python -m tw_lake.cli --mode smoke
python -m tw_lake.cli --mode full --dry-run
python -m tw_lake.cli --mode full --confirm-full --max-requests 2000
python -m tw_lake.cli --mode update
```

結束代碼：`0` 成功、`2` 部分完成（有 failed/partial/conflict 或達請求上限）、`1` 失敗或被 lock 擋下。

**每次寫入後的持久性驗證**（Drive FUSE 的讀取會命中本機快取，同一個掛載內看不出檔案是否真的上傳）：runner notebook
在 run 結束後 `flush_and_unmount` → `mount` → `python -m tw_lake.audit.verify`（本次 run 寫的檔案逐一比對大小與 SHA256）。
`PROBLEMS` 時不要使用那些分區，再跑一次 `update` 會自動重建。故障注入測試：`notebooks/TW_Market_Data_Lake_Fault_Injection.ipynb`
（只寫 `00Quant_Research/_lake_fault_test/`）。

## 「從哪一天開始有新資料」怎麼看

- `fubon_stock_5m/_manifest/versions/<最新>/coverage_by_window.parquet`：每個 symbol × window 的 `status` 與 `covered_to`
- `coverage_by_symbol.parquet`：每檔 `last_complete_trade_date`、`status`
- 每次 run 的 `runs/<run_id>/plan.csv`（這次要抓什麼、為什麼）、`delta_report_5m.csv`（實際新增）、
  `request_log.csv`、`qa_reconciliation_5m.csv`、`qa_summary.md`、`run_info.json`

讀取現行研究版本（範例）：

```python
from tw_lake.config import load_config
from tw_lake.clock import Clock
from tw_lake.paths import Layout
from tw_lake.store import DatasetStore, StoreContext
cfg = load_config()
ctx = StoreContext(Layout.from_config(cfg), "reader", cfg.local_tmp, Clock())
s5 = DatasetStore("fubon_5m", ctx, ["bar_start"], "1", params=cfg.minute_params())
bars = s5.read_partition("symbol=2330/year=2025")
cov = s5.tables()["coverage_by_window"]
```

## 資料品質稽核（唯讀，Phase A）

`notebooks/TW_Market_Data_Lake_Audit.ipynb` 或
`python -m tw_lake.audit.cli --data-root "$DATA_ROOT" --out-dir /content/tw_lake_audits`。
只讀 manifest 指向的已發布版本，不呼叫 API、不寫 DATA_ROOT（`--publish-to-drive` 時才把結果複製到新的
`_tw-market-data-lake/audits/<audit_id>/`）。規格、2026-10-25 實測發現與 Phase B 提案見 [docs/AUDIT.md](docs/AUDIT.md)。

後續稽核與參考資料都是唯讀，或只寫入自己的新資料夾。各 notebook 用途如下：

| Notebook | 模組 | 用途 |
|---|---|---|
| `TW_Market_Data_Lake_Gap_Ledger.ipynb` | `tw_lake.audit.gap_ledger` | 逐格缺棒帳本 |
| `TW_Market_Data_Lake_Official_Volume_Batch.ipynb` | `tw_lake.reference.official_volume_batch` | 官方成交量參考資料（可分段抓取）與全期對帳 |
| `TW_Market_Data_Lake_Eligibility.ipynb` | `tw_lake.audit.eligibility` | 規則 v2 複核、Backtest Eligibility（Level A／B） |
| `TW_Market_Data_Lake_Quality_Acceptance.ipynb` | `tw_lake.audit.quality_mask` | 最終品質驗收、隔離清單、異常帳本、股票日品質遮罩；使用規則見 [docs/BACKTEST_DATA_CONTRACT.md](docs/BACKTEST_DATA_CONTRACT.md)；研究 repo 介面見 [docs/RESEARCH_REPO_INTERFACE.md](docs/RESEARCH_REPO_INTERFACE.md) |
| `TW_Market_Data_Lake_Corporate_Actions.ipynb` | `tw_lake.reference.corporate_actions` | B3／B5：官方除權息、減資、分割、暫停交易與上市主檔。流程為探索、試抓、正式抓取、解析，存到 `reference/corporate_actions/v1` |
| `TW_Market_Data_Lake_Integration_Check.ipynb` | `tw_lake.audit.integration_check` | B1：驗證品質遮罩的 SHA-256、版本、筆數、規則與 K 棒抽查（PASS/FAIL） |
| `TW_Market_Data_Lake_Targeted_Checks.ipynb` | `tw_lake.audit.targeted_checks`、`tw_lake.audit.price_check` | 小量補抓、B 類日期診斷、官方日 K 價格比對 |

## 開發

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

測試全部離線、使用合成資料（`SYNTHETIC_SMOKE_ONLY`）；**測試通過不代表真實資料完整**，真實品質以 smoke／pilot 的 QA 報告為準。

## 已知限制

- 富邦分 K 自 2023-05-23 起；單次查詢 < 1 年；歷史行情 60 次／分（程式預設 55）。
- `security_categories` 是目前快照，套用到歷史時標記 `classification_is_pit=False`。
- 處置／注意歷史以 FinLab 為準；富邦旗標只代表抓取當天。
- 富邦 API Key 登入函式未經實測；預設使用帳號密碼＋憑證（與既有 pipeline 相同）。
- Drive FUSE 不保證跨機器原子性：lock 與 commit 前 heartbeat 驗證能偵測並阻擋並行寫入，但不是分散式共識。
