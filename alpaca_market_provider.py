"""Tajum On V150 Alpaca US-stock market-data provider.

Purpose
-------
Use Alpaca as the PRIMARY US-stock market-data source while preserving KIS as a
fallback. The signal engine remains provider-agnostic.

Default plan assumption
-----------------------
- ALPACA_STOCK_FEED=iex        (Basic/free)
- ALPACA_STREAM_SYMBOL_LIMIT=30
Later, when the account is upgraded to a plan with unlimited stream symbols, set
ALPACA_STREAM_SYMBOL_LIMIT=0 and the code does not need to change.

Important
---------
Basic IEX is only the IEX exchange, not the consolidated SIP market. It is suitable
for development / early validation. Production signal quality should be revalidated
when switching to SIP.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

import requests

UTC = timezone.utc
NY = ZoneInfo("America/New_York")

API_KEY = (
    os.getenv("ALPACA_API_KEY", "").strip()
    or os.getenv("APCA_API_KEY_ID", "").strip()
)
API_SECRET = (
    os.getenv("ALPACA_API_SECRET", "").strip()
    or os.getenv("APCA_API_SECRET_KEY", "").strip()
)
DATA_BASE = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets").rstrip("/")
STOCK_FEED = os.getenv("ALPACA_STOCK_FEED", "iex").strip().lower() or "iex"
STREAM_URL = os.getenv(
    "ALPACA_STREAM_URL",
    f"wss://stream.data.alpaca.markets/v2/{STOCK_FEED}",
).strip()

# Free Basic currently permits 30 stock symbols on the websocket.
# 0 means unlimited (for a future upgraded plan).
STREAM_SYMBOL_LIMIT = max(
    0,
    int(os.getenv("ALPACA_STREAM_SYMBOL_LIMIT", "30") or 30),
)
HTTP_TIMEOUT = max(
    3,
    min(int(os.getenv("ALPACA_HTTP_TIMEOUT_SEC", "10") or 10), 30),
)
REST_MIN_INTERVAL_SEC = max(
    0.03,
    min(float(os.getenv("ALPACA_REST_MIN_INTERVAL_SEC", "0.08") or 0.08), 1.0),
)
HISTORICAL_DELAY_MIN = max(
    0,
    min(
        int(
            os.getenv(
                "ALPACA_HISTORICAL_DELAY_MINUTES",
                "16" if STOCK_FEED == "iex" else "0",
            )
            or 0
        ),
        60,
    ),
)

_http_lock = threading.Lock()
_last_http_at = 0.0
_status_lock = threading.Lock()
_status: dict[str, Any] = {
    "configured": bool(API_KEY and API_SECRET),
    "provider": "ALPACA",
    "feed": STOCK_FEED,
    "stream_url": STREAM_URL,
    "stream_symbol_limit": STREAM_SYMBOL_LIMIT,
    "historical_delay_minutes": HISTORICAL_DELAY_MIN,
    "request_count": 0,
    "success_count": 0,
    "error_count": 0,
    "rate_limit_count": 0,
    "last_request_at": None,
    "last_success_at": None,
    "last_error": None,
}


def configured() -> bool:
    return bool(API_KEY and API_SECRET)


def auth_headers() -> dict[str, str]:
    if not configured():
        raise RuntimeError("ALPACA_NOT_CONFIGURED")
    return {
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
        "accept": "application/json",
    }


def status() -> dict[str, Any]:
    with _status_lock:
        return dict(_status)


def _guard() -> None:
    global _last_http_at
    with _http_lock:
        now = time.monotonic()
        wait = REST_MIN_INTERVAL_SEC - (now - _last_http_at)
        if wait > 0:
            time.sleep(wait)
        _last_http_at = time.monotonic()


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _get_bars(
    symbol: str,
    timeframe: str,
    *,
    start: datetime,
    end: datetime,
    limit: int = 10000,
) -> list[dict[str, Any]]:
    if not configured():
        raise RuntimeError("ALPACA_NOT_CONFIGURED")

    symbol = symbol.upper().strip()
    params: dict[str, Any] = {
        "timeframe": timeframe,
        "start": _iso(start),
        "end": _iso(end),
        "limit": min(max(int(limit), 1), 10000),
        "adjustment": "raw",
        "feed": STOCK_FEED,
        "sort": "asc",
    }

    rows: list[dict[str, Any]] = []
    page_token: str | None = None
    # Bounded pagination protects startup from accidental very large history pulls.
    for _page in range(4):
        if page_token:
            params["page_token"] = page_token
        _guard()
        with _status_lock:
            _status["request_count"] = int(_status.get("request_count", 0) or 0) + 1
            _status["last_request_at"] = datetime.now(UTC).isoformat()
        try:
            resp = requests.get(
                f"{DATA_BASE}/v2/stocks/{symbol}/bars",
                headers=auth_headers(),
                params=params,
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code == 429:
                with _status_lock:
                    _status["rate_limit_count"] = int(
                        _status.get("rate_limit_count", 0) or 0
                    ) + 1
                time.sleep(0.8)
                continue
            resp.raise_for_status()
            body = resp.json()
            raw = body.get("bars") or []
            if not isinstance(raw, list):
                raise RuntimeError("Alpaca bars response is not a list")
            rows.extend(_bar_to_row(x) for x in raw if isinstance(x, dict))
            page_token = str(body.get("next_page_token") or "").strip() or None
            with _status_lock:
                _status["success_count"] = int(_status.get("success_count", 0) or 0) + 1
                _status["last_success_at"] = datetime.now(UTC).isoformat()
                _status["last_error"] = None
            if not page_token:
                break
        except Exception as exc:
            with _status_lock:
                _status["error_count"] = int(_status.get("error_count", 0) or 0) + 1
                _status["last_error"] = f"{type(exc).__name__}: {exc}"
            raise

    # Deduplicate by open_time because page boundaries can occasionally overlap.
    dedup = {int(x["open_time"]): x for x in rows}
    return [dedup[k] for k in sorted(dedup)]


def _bar_to_row(item: dict[str, Any]) -> dict[str, Any]:
    ts = str(item.get("t") or "")
    if not ts:
        raise ValueError("Alpaca bar timestamp missing")
    # Python fromisoformat supports microseconds; trim nanoseconds if present.
    norm = ts.replace("Z", "+00:00")
    if "." in norm:
        left, right = norm.split(".", 1)
        suffix = "+00:00" if right.endswith("+00:00") else ""
        frac = right[:-6] if suffix else right
        frac = frac[:6]
        norm = f"{left}.{frac}{suffix}"
    dt = datetime.fromisoformat(norm)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    open_ms = int(dt.astimezone(UTC).timestamp() * 1000)
    return {
        "open_time": open_ms,
        "open": float(item["o"]),
        "high": float(item["h"]),
        "low": float(item["l"]),
        "close": float(item["c"]),
        "volume": float(item.get("v") or 0.0),
        "close_time": open_ms,
    }


def _regular_session_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only 09:30 <= New York time < 16:00 on weekdays.

    Alpaca real-time minute bars explicitly include pre/after-market activity. The
    current Tajum stock model was built on regular-session candles, so both historical
    warm-up and websocket paths use the same regular-session rule.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            dt = datetime.fromtimestamp(int(row["open_time"]) / 1000, tz=UTC).astimezone(NY)
            if dt.weekday() >= 5:
                continue
            hm = dt.hour * 60 + dt.minute
            if 9 * 60 + 30 <= hm < 16 * 60:
                out.append(row)
        except Exception:
            continue
    return out


def warmup_sources(symbol: str) -> dict[str, list[dict[str, Any]]]:
    """Fetch only three source families; higher/lower TFs are derived locally.

    5Min  : enough for 5m / 15m / 30m
    30Min : enough for 1h / 2h / 4h / 6h
    1Day  : enough for 1d / 3d / 1w

    Basic/free accounts have a recent-historical restriction, so the default `end`
    stays 16 minutes behind real time. The live websocket fills the current gap.
    """
    if not configured():
        raise RuntimeError("ALPACA_NOT_CONFIGURED")

    now = datetime.now(UTC)
    end = now - timedelta(minutes=HISTORICAL_DELAY_MIN)
    bars_5m = _regular_session_rows(_get_bars(
        symbol, "5Min",
        start=end - timedelta(days=21),
        end=end,
    ))
    bars_30m = _regular_session_rows(_get_bars(
        symbol, "30Min",
        start=end - timedelta(days=90),
        end=end,
    ))
    daily = _get_bars(
        symbol, "1Day",
        start=end - timedelta(days=420),
        end=end,
    )
    return {"5m": bars_5m, "30m": bars_30m, "1d": daily}


def stream_symbols(symbols: list[str]) -> tuple[list[str], list[str]]:
    """Split current US watchlist into websocket-covered and REST-fallback symbols."""
    unique = sorted({str(x).upper().strip() for x in symbols if str(x).strip()})
    if STREAM_SYMBOL_LIMIT <= 0:
        return unique, []
    return unique[:STREAM_SYMBOL_LIMIT], unique[STREAM_SYMBOL_LIMIT:]
