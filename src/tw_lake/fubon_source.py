"""Fubon Neo market-data access: login once, rate limiting, retry policy, error mapping.

Only market-data endpoints are used. This module never calls trading or accounting APIs.
"""
from __future__ import annotations

import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from .errors import (ApiBadRequest, ApiError, ApiNotFound, ApiRateLimited, ApiTransient, ConfigError,
                     RequestBudgetExceeded, RetriesExhausted)

log = logging.getLogger(__name__)


class MarketClient(Protocol):
    def historical_candles(self, params: dict) -> dict: ...
    def tickers(self, params: dict) -> dict: ...
    def ticker(self, params: dict) -> dict: ...


# ---- rate limiting ------------------------------------------------------------------
class RateLimiter:
    """Sliding-window limiter: at most `per_minute` calls in any 60 s window."""

    def __init__(self, per_minute: int, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.per_minute = per_minute
        self.clock = clock
        self.sleep = sleep
        self.calls: deque[float] = deque()

    def acquire(self) -> None:
        while True:
            now = self.clock()
            while self.calls and now - self.calls[0] >= 60.0:
                self.calls.popleft()
            if len(self.calls) < self.per_minute:
                self.calls.append(now)
                return
            self.sleep(60.0 - (now - self.calls[0]) + 0.05)


@dataclass
class RequestBudget:
    max_requests: int | None = None
    used: int = 0

    def take(self) -> None:
        if self.max_requests is not None and self.used >= self.max_requests:
            raise RequestBudgetExceeded(f"request budget {self.max_requests} reached")
        self.used += 1


@dataclass
class RetryPolicy:
    max_attempts: int = 5
    rate_limit_wait_sec: float = 61.0
    backoff_base_sec: float = 2.0
    backoff_max_sec: float = 60.0


@dataclass
class CallRecord:
    attempt: int
    status: str
    error: str | None
    latency_ms: float


def map_exception(exc: Exception) -> ApiError:
    """Translate SDK exceptions (FugleAPIError has .status_code) into typed errors."""
    if isinstance(exc, ApiError):
        return exc
    code = getattr(exc, "status_code", None)
    try:
        code = int(code) if code is not None else None
    except (TypeError, ValueError):
        code = None
    msg = f"{type(exc).__name__}: {exc}"
    if code == 404:
        return ApiNotFound(msg, 404)
    if code == 429:
        return ApiRateLimited(msg, 429)
    if code == 400:
        return ApiBadRequest(msg, 400)
    if code is not None and 400 < code < 500:
        return ApiBadRequest(msg, code)
    return ApiTransient(msg, code)


@dataclass
class FubonCaller:
    """Wraps one endpoint with limiter + budget + retries. Records every attempt."""
    limiter: RateLimiter
    policy: RetryPolicy
    budget: RequestBudget
    sleep: Callable[[float], None] = time.sleep
    records: list[CallRecord] = field(default_factory=list)

    def call(self, fn: Callable[[dict], dict], params: dict) -> dict:
        self.records = []
        last: ApiError | None = None
        for attempt in range(1, self.policy.max_attempts + 1):
            self.budget.take()
            self.limiter.acquire()
            t0 = time.monotonic()
            try:
                resp = fn(params)
                self.records.append(CallRecord(attempt, "ok", None, (time.monotonic() - t0) * 1000))
                return resp
            except Exception as exc:  # noqa: BLE001 - mapped below
                err = map_exception(exc)
                self.records.append(CallRecord(attempt, type(err).__name__, str(err)[:500],
                                               (time.monotonic() - t0) * 1000))
                if isinstance(err, (ApiNotFound, ApiBadRequest)):
                    raise err
                last = err
                if isinstance(err, ApiRateLimited):
                    self.sleep(self.policy.rate_limit_wait_sec)
                else:
                    self.sleep(min(self.policy.backoff_max_sec, self.policy.backoff_base_sec ** attempt))
        raise RetriesExhausted(f"gave up after {self.policy.max_attempts} attempts: {last}",
                               getattr(last, "status_code", None))


# ---- real SDK client -------------------------------------------------------------------
class RealFubonClient:
    """Thin adapter over fubon_neo. Logs in exactly once per process."""

    def __init__(self, sdk: Any):
        self.sdk = sdk
        self.rest = sdk.marketdata.rest_client.stock

    @classmethod
    def login_from_env(cls) -> "RealFubonClient":
        try:
            from fubon_neo.sdk import FubonSDK  # type: ignore
        except Exception as exc:  # pragma: no cover - needs vendor SDK
            raise ConfigError("fubon_neo SDK not installed; run scripts/install_colab_dependencies.py") from exc
        uid = os.getenv("FUBON_USER_ID")
        cert = os.getenv("FUBON_CERT_PATH")
        cert_pw = os.getenv("FUBON_CERT_PASSWORD") or None
        api_key = os.getenv("FUBON_API_KEY")
        pw = os.getenv("FUBON_PASSWORD")
        if not uid or not cert:
            raise ConfigError("missing FUBON_USER_ID / FUBON_CERT_PATH")
        sdk = FubonSDK()
        if api_key:
            if not hasattr(sdk, "apikey_login"):
                raise ConfigError("FUBON_API_KEY set but this SDK has no apikey_login(); use password login")
            res = sdk.apikey_login(uid, api_key, cert, cert_pw) if cert_pw else sdk.apikey_login(uid, api_key, cert)
        else:
            if not pw:
                raise ConfigError("missing FUBON_PASSWORD (or FUBON_API_KEY)")
            res = sdk.login(uid, pw, cert, cert_pw) if cert_pw else sdk.login(uid, pw, cert)
        if not getattr(res, "is_success", False):
            raise ConfigError(f"Fubon login failed: {getattr(res, 'message', '')}")
        sdk.init_realtime()
        log.info("Fubon login OK (credentials not logged)")
        return cls(sdk)

    def historical_candles(self, params: dict) -> dict:
        return self.rest.historical.candles(**params)

    def tickers(self, params: dict) -> dict:
        return self.rest.intraday.tickers(**params)

    def ticker(self, params: dict) -> dict:
        return self.rest.intraday.ticker(**params)


def candle_params(symbol: str, start, end, timeframe: str, fields: list[str]) -> dict:
    return {"symbol": symbol, "from": str(start), "to": str(end), "timeframe": str(timeframe),
            "fields": ",".join(fields), "sort": "asc"}
