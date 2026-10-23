"""Time helpers. All wall-clock decisions use Asia/Taipei. Injectable for tests."""
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo("Asia/Taipei")


class Clock:
    def now(self) -> datetime:
        return datetime.now(TAIPEI)


class FixedClock(Clock):
    def __init__(self, dt: datetime):
        self._dt = dt if dt.tzinfo else dt.replace(tzinfo=TAIPEI)

    def now(self) -> datetime:
        return self._dt

    def set(self, dt: datetime) -> None:
        self._dt = dt if dt.tzinfo else dt.replace(tzinfo=TAIPEI)


def parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def run_timestamp(clock: Clock) -> str:
    return clock.now().strftime("%Y%m%d_%H%M%S")


def today_taipei(clock: Clock) -> date:
    return clock.now().date()
