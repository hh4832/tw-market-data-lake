"""Known-answer tests for docs/RESEARCH_REPO_INTERFACE.md sections 2-3 (B2 M7 follow-up). SYNTHETIC_SMOKE_ONLY.

The bucketing / fill functions below are a minimal SPEC ORACLE that lives only in tests. They are not a lake
output and not a backtest engine (AGENTS.md rule 1); the research repo's engine must reproduce these answers.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

FIVE = timedelta(minutes=5)
CLOSE_AUCTION_START = time(13, 30)


# ---------------------------------------------------------------- spec oracle
def expected_slots(d: date) -> list[datetime]:
    """54 expected slots of a normal day session: 09:00..13:20 continuous (53) + 13:30 closing call."""
    t0 = datetime.combine(d, time(9, 0))
    cont = [t0 + i * FIVE for i in range(53)]
    assert cont[-1].time() == time(13, 20)
    return cont + [datetime.combine(d, CLOSE_AUCTION_START)]


def bucket_of(slot: datetime) -> str:
    if slot.time() == CLOSE_AUCTION_START:
        return "CLOSE_AUCTION"
    m = (slot.hour * 60 + slot.minute - 9 * 60) // 30
    start = datetime.combine(slot.date(), time(9, 0)) + timedelta(minutes=30 * m)
    return start.strftime("%H:%M")


def buckets(d: date, bars: dict[datetime, tuple[float, float, float, float, int]]) -> dict[str, dict]:
    """bars: REAL_BARs only, keyed by bar_start -> (open, high, low, close, volume)."""
    out: dict[str, dict] = {}
    for s in expected_slots(d):
        out.setdefault(bucket_of(s), {"expected": [], "real": []})["expected"].append(s)
    for s in sorted(bars):
        out[bucket_of(s)]["real"].append(s)
    for b in out.values():
        exp, real = b["expected"], b["real"]
        b["bucket_end"] = exp[-1] + (timedelta(0) if exp[-1].time() == CLOSE_AUCTION_START else FIVE)
        b["bar_end"] = (real[-1] + (timedelta(0) if real[-1].time() == CLOSE_AUCTION_START else FIVE)) \
            if real else None
        b["available_at"] = b["bucket_end"]                  # never derived from bar_end
        if real:
            b["open"], b["close"] = bars[real[0]][0], bars[real[-1]][3]
            b["high"] = max(bars[s][1] for s in real)
            b["low"] = min(bars[s][2] for s in real)
            b["volume"] = sum(bars[s][4] for s in real)
    return out


def visible(bk: dict[str, dict], t: datetime) -> list[str]:
    return [k for k, b in bk.items() if b["available_at"] <= t]


def fill_reference(signal_at: datetime, source_bucket: str, days: list[tuple[date, dict]],
                   target: str = "next_executable", close_cutoff_verified: bool = False):
    """Simulation reference (not a guaranteed fill): (fill_slot, reference_open) or None when fail-closed."""
    if target == "close_auction" and not close_cutoff_verified:
        return None                                          # S4(b): cutoff/executability not verified
    for d, bars in days:
        for s in expected_slots(d):
            if s not in bars or bars[s][4] <= 0:
                continue
            if s.time() == CLOSE_AUCTION_START:
                # S4(a): a signal known at/after the 13:30 match cannot fill in that same auction;
                # entering the closing call from the continuous session is S4(b) and is fail-closed.
                continue
            if s >= signal_at:
                return s, bars[s][0]
    return None


def _valid_ohlc(bars) -> bool:
    return all(l <= min(o, c) and h >= max(o, c) and l <= h for o, h, l, c, _ in bars.values())


def flat(d: date, slots, px=100.0, vol=1000):
    return {s: (px, px, px, px, vol) for s in slots}


D = date(2025, 6, 23)
N = date(2025, 6, 24)


def at(d, hh, mm):
    return datetime.combine(d, time(hh, mm))


# ---------------------------------------------------------------- K1 bucket that ends early
def test_k1_bucket_ending_early_is_available_only_at_bucket_end():
    real = [s for s in expected_slots(D) if bucket_of(s) != "09:30"] + [at(D, 9, 30), at(D, 9, 35)]
    bars = flat(D, real)
    bars[at(D, 10, 0)] = (101.0, 101.0, 101.0, 101.0, 1000)
    b = buckets(D, bars)["09:30"]
    assert len(b["expected"]) == 6 and len(b["real"]) == 2
    assert b["bar_end"] == at(D, 9, 40)
    assert b["bucket_end"] == b["available_at"] == at(D, 10, 0)
    assert b["available_at"] > b["bar_end"]
    assert "09:30" not in visible(buckets(D, bars), at(D, 9, 45))   # would be lookahead via bar_end
    assert "09:30" in visible(buckets(D, bars), at(D, 10, 0))
    assert fill_reference(at(D, 10, 0), "09:30", [(D, bars)]) == (at(D, 10, 0), 101.0)


# ---------------------------------------------------------------- K2 last intraday bucket vs closing call
def test_k2_last_intraday_bucket_and_close_auction_are_separate():
    bars = flat(D, expected_slots(D))
    bars[at(D, 13, 20)] = (100.0, 100.5, 100.0, 100.5, 1000)             # valid OHLC: high >= close
    bars[at(D, 13, 30)] = (102.0, 102.0, 102.0, 102.0, 7000)          # closing call
    assert _valid_ohlc(bars)
    bk = buckets(D, bars)
    last = bk["13:00"]
    assert [s.time() for s in last["expected"]] == [time(13, m) for m in (0, 5, 10, 15, 20)]
    assert last["bucket_end"] == last["available_at"] == at(D, 13, 25)
    assert last["close"] == 100.5 and last["high"] == 100.5
    assert last["volume"] == 5 * 1000 and last["high"] < 102.0         # 13:30 not merged
    ca = bk["CLOSE_AUCTION"]
    assert [s.time() for s in ca["expected"]] == [time(13, 30)]
    assert ca["available_at"] == at(D, 13, 30) and ca["close"] == 102.0 and ca["volume"] == 7000
    assert sum(len(b["expected"]) for b in bk.values()) == 54
    assert len(bk) == 10                                               # 9 intraday buckets + CLOSE_AUCTION


# ---------------------------------------------------------------- K3 no same-auction lookahead fill
def test_k3_close_auction_signal_cannot_fill_in_same_auction():
    today = flat(D, expected_slots(D))
    nxt = flat(N, expected_slots(N), px=105.0)
    sig = buckets(D, today)["CLOSE_AUCTION"]["available_at"]
    assert sig == at(D, 13, 30)
    got = fill_reference(sig, "CLOSE_AUCTION", [(D, today), (N, nxt)])
    assert got == (at(N, 9, 0), 105.0)                                 # next day, never D 13:30


def test_k3_order_into_closing_call_is_fail_closed_until_cutoff_verified():
    today = flat(D, expected_slots(D))
    sig = buckets(D, today)["13:00"]["available_at"]
    assert sig == at(D, 13, 25)
    assert fill_reference(sig, "13:00", [(D, today)], target="close_auction",
                          close_cutoff_verified=False) is None
    # declared roll-over to the next session instead of the closing call
    nxt = flat(N, expected_slots(N), px=99.0)
    assert fill_reference(sig, "13:00", [(D, today), (N, nxt)]) == (at(N, 9, 0), 99.0)
