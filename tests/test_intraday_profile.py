import json

from tw_lake.audit.intraday_profile import official_buckets, parse_mi5mins


def _body(rows):
    return json.dumps({"stat": "OK", "fields": ["時間", "累積委託買進數量", "累積成交筆數", "累積成交數量", "累積成交金額"],
                       "data": rows, "notes": ["單位：交易單位"]}, ensure_ascii=False).encode("utf-8")


def test_parse_picks_cumulative_trade_volume():
    m, info = parse_mi5mins(_body([["09:00:00", "999", "1", "1,000", "5"], ["09:00:05", "999", "2", "1,500", "6"]]))
    assert info["volume_field"] == "累積成交數量"
    assert list(m["cum_volume"]) == [1000.0, 1500.0]


def test_buckets_and_after_close():
    rows = [["09:00:00", "0", "0", "100", "0"], ["09:04:55", "0", "0", "300", "0"], ["09:05:00", "0", "0", "400", "0"],
            ["13:25:00", "0", "0", "5000", "0"], ["13:30:00", "0", "0", "6000", "0"],
            ["13:33:00", "0", "0", "6500", "0"]]
    m, _ = parse_mi5mins(_body(rows))
    b, info = official_buckets(m)
    b = b.set_index("hhmm")["official"]
    assert b["09:00"] == 300          # snapshots stamped in [09:00, 09:05)
    assert b["09:05"] == 100
    assert b["13:25"] == 4600         # [13:25, 13:30)
    assert b["13:30"] == 1500         # 13:30:00 closing auction and anything later
    assert info["after_1330_volume"] == 500
    assert info["after_1330_rows"] == 1 and info["after_1330_times"] == "13:33:00"
    assert abs(b.sum() - 6500) < 1e-9  # buckets conserve the day total
