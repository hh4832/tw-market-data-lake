"""Corporate-action / listing reference fetch: offline tests with a fake HTTP getter. SYNTHETIC_SMOKE_ONLY."""
from __future__ import annotations

import gzip
import json
from datetime import date

import pandas as pd
import pytest

from tw_lake.reference import corporate_actions as CA

T49 = {"stat": "OK", "fields": ["資料日期", "股票代號", "股票名稱", "除權息前收盤價", "除權息參考價", "權值+息值", "權/息"],
       "data": [["114年06月18日", "0050", "元大台灣50", "188.65", "47.16", "0.00", "分割"],
                ["114年06月19日", "2330", "台積電", "1000.00", "995.00", "5.00", "息"]]}
EMPTY = {"stat": "很抱歉，沒有符合條件的資料!"}
ISIN_HTML = ("<table><tr><td>有價證券代號及名稱</td><td>國際證券辨識號碼(ISIN Code)</td><td>上市日</td>"
             "<td>市場別</td><td>產業別</td><td>CFICode</td><td>備註</td></tr>"
             "<tr><td>2330　台積電</td><td>TW0002330008</td><td>1994/09/05</td><td>上市</td><td>半導體業</td>"
             "<td>ESVUFR</td><td></td></tr></table>").encode("cp950")


def fake_getter(calls):
    def get(url, params):
        calls.append((url, dict(params)))
        if "TWT49U" in url:
            body = T49 if params.get("startDate", "").startswith("202506") else EMPTY
            return 200, json.dumps(body, ensure_ascii=False).encode(), "application/json", url
        if "isin" in url:
            return 200, ISIN_HTML, "text/html", url
        if "broken" in url:
            return 500, b"err", "", url
        return 200, b"<html>not found</html>", "text/html", url
    return get


def test_months_and_plan():
    m = CA.months(date(2023, 5, 23), date(2023, 7, 10))
    assert m == [(date(2023, 5, 23), date(2023, 5, 31)), (date(2023, 6, 1), date(2023, 6, 30)),
                 (date(2023, 7, 1), date(2023, 7, 10))]
    snap = CA.plan(CA.BY_ID["ISIN_TWSE"], date(2023, 5, 1), date(2026, 10, 30), date(2026, 10, 30))
    assert snap == [("snapshot_2026-10-30", date(2026, 10, 30), date(2026, 10, 30))]


def test_summary_and_validity():
    ep = CA.BY_ID["TWSE_TWT49U"]
    s = CA.summarize(json.dumps(T49, ensure_ascii=False).encode(), "json")
    assert s["json"] and s["rows"] == 2 and "除權息參考價" in s["fields"]
    assert CA.valid(ep, 200, json.dumps(EMPTY, ensure_ascii=False).encode())       # empty month is valid evidence
    assert not CA.valid(ep, 200, b"<html>soft 404</html>")
    assert not CA.valid(ep, 500, b"{}")
    assert CA.valid(CA.BY_ID["ISIN_TWSE"], 200, ISIN_HTML)


def test_probe_writes_raw_and_summary(tmp_path):
    calls = []
    out = CA.probe(tmp_path, date(2025, 6, 1), date(2025, 6, 30), ["TWSE_TWT49U", "TWSE_TWTAUU", "ISIN_TWSE"],
                   getter=fake_getter(calls), sleep=lambda *_: None, log=lambda *_: None)
    s = pd.read_csv(out / "probe_summary.csv").set_index("report")
    assert s.at["TWSE_TWT49U", "rows"] == 2 and bool(s.at["TWSE_TWT49U", "json"])
    assert not bool(s.at["TWSE_TWTAUU", "json"])                                    # soft 404 recorded, not hidden
    assert (out / "raw" / "TWSE_TWT49U.json.gz").exists()
    p = dict(calls[[u for u, _ in calls].index(CA.BY_ID["TWSE_TWT49U"].url)][1])
    assert p == {"startDate": "20250601", "endDate": "20250630", "response": "json"}


def test_fetch_resumes_and_skips_done(tmp_path):
    root = tmp_path / "reference" / "corporate_actions" / "v1"
    calls = []
    kw = dict(getter=fake_getter(calls), sleep=lambda *_: None, log=lambda *_: None, today=date(2026, 10, 30))
    rec = CA.fetch(root, tmp_path / "w", ["TWSE_TWT49U", "ISIN_TWSE"], date(2025, 5, 1), date(2025, 7, 31), **kw)
    assert len(rec["done"]) == 4 and not rec["failed"]                              # 3 months + 1 snapshot
    sd = CA.shard(root, CA.BY_ID["TWSE_TWT49U"], "2025-06")
    r = json.loads((sd / "request.json").read_text(encoding="utf-8"))
    body = gzip.decompress((sd / "body.json.gz").read_bytes())
    import hashlib
    assert r["sha256"] == hashlib.sha256(body).hexdigest() and r["known_at"] == "2025-06-30T23:59:59+08:00"
    n = len(calls)
    rec2 = CA.fetch(root, tmp_path / "w", ["TWSE_TWT49U", "ISIN_TWSE"], date(2025, 5, 1), date(2025, 7, 31), **kw)
    assert len(calls) == n and rec2["todo_at_start"] == 0                           # nothing re-requested
    st = CA.status(root, ["TWSE_TWT49U"], date(2025, 5, 1), date(2025, 7, 31), today=date(2026, 10, 30))
    assert st.iloc[0]["remaining"] == 0


def test_fetch_failure_is_not_marked_done_and_stops(tmp_path, monkeypatch):
    bad = CA.Endpoint("X_broken", "TWSE", "test", "https://x/broken", CA.R, "range", pace_sec=0)
    monkeypatch.setitem(CA.BY_ID, "X_broken", bad)
    root = tmp_path / "ref"
    rec = CA.fetch(root, tmp_path / "w", ["X_broken"], date(2025, 1, 1), date(2025, 12, 31),
                   getter=fake_getter([]), sleep=lambda *_: None, log=lambda *_: None, today=date(2026, 10, 30))
    assert not rec["done"] and len(rec["failed"]) == 3 and "failures in a row" in rec["stopped"]
    assert not (CA.shard(root, bad, "2025-01") / "_SUCCESS").exists()


def test_current_month_is_dated(tmp_path):
    root = tmp_path / "ref"
    rec = CA.fetch(root, tmp_path / "w", ["TWSE_TWT49U"], date(2026, 10, 1), date(2026, 10, 30),
                   getter=fake_getter([]), sleep=lambda *_: None, log=lambda *_: None, today=date(2026, 10, 30))
    assert rec["done"] == ["TWSE_TWT49U/2026-10_asof_2026-10-30"]


def test_guard_refuses_lake_dataset_paths(tmp_path):
    with pytest.raises(SystemExit):
        CA.fetch(tmp_path / "fubon_5m" / "x", tmp_path / "w", ["TWSE_TWT49U"], date(2025, 1, 1), date(2025, 1, 31),
                 getter=fake_getter([]), sleep=lambda *_: None, log=lambda *_: None)
    with pytest.raises(SystemExit):
        CA.fetch(tmp_path / "official_volume" / "v1", tmp_path / "w", ["TWSE_TWT49U"], date(2025, 1, 1),
                 date(2025, 1, 31), getter=fake_getter([]), sleep=lambda *_: None, log=lambda *_: None)


def test_parse_flattens_without_interpretation(tmp_path):
    root = tmp_path / "ref"
    CA.fetch(root, tmp_path / "w", ["TWSE_TWT49U", "ISIN_TWSE"], date(2025, 6, 1), date(2025, 6, 30),
             getter=fake_getter([]), sleep=lambda *_: None, log=lambda *_: None, today=date(2026, 10, 30))
    out = CA.parse(root, tmp_path / "parsed")
    lg = pd.read_parquet(out / "rows_long.parquet")
    t = lg[lg["report"] == "TWSE_TWT49U"]
    assert set(t["field"]) >= {"股票代號", "除權息參考價"} and (t["value"] == "47.16").any()
    i = lg[lg["report"] == "ISIN_TWSE"]
    assert (i["field"] == "上市日").any() and (i["value"] == "1994/09/05").any()
    assert any(p.name.startswith("wide_TWSE_TWT49U") for p in out.iterdir())
