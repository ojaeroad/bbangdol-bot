"""Tajum On V145 KIS market-data provider.

Purpose
-------
- KRX/US member watchlist symbols can be evaluated without TradingView registration.
- KIS REST APIs are used for stock OHLC warm-up and current prices.
- No trading/order API is used.
- Missing KIS credentials disable only stock direct-calculation; coin engines remain live.

Environment
-----------
KIS_APP_KEY, KIS_APP_SECRET                         required for stock direct calculation
KIS_ENV=real|demo                                   default real
KIS_BASE_URL                                        optional override
KIS_HTTP_TIMEOUT_SEC                                default 8
KIS_HTTP_MIN_INTERVAL_SEC                           default 0.30 (global rate guard)
KIS_CACHE_DIR                                       default /tmp/tajum_kis_cache
KIS_CACHE_TTL_SEC                                   default 21600 (6h)
KIS_DOMESTIC_HISTORY_DAYS                           default 22 (hard max 30)
KIS_DOMESTIC_PAGES_PER_DAY                          default 4 (KRX 390m / 120 rows)
KIS_OVERSEAS_PAGES                                  default 4
KIS_RATE_LIMIT_COOLDOWN_SEC                         default 1.5
KIS_OVERSEAS_MINUTE_CACHE_TTL_SEC                  default 75

V145 notes
----------
- Follows the official KIS domestic minute pagination stop rule: stop when the
  page is shorter than 120 rows or the oldest time reaches 09:00.
- Reuses the oldest time as the next cursor (dedup handles overlap) instead of
  subtracting one minute, which caused 09:20-era 500 responses on some days.
- Stops domestic 1m warm-up as soon as enough aggregated 4h bars exist.
- Retries only transient gateway/rate-limit errors; invalid historical points are
  isolated without repeatedly hammering KIS.

Notes
-----
Domestic KIS minute history is page-oriented 1-minute data. V143 intentionally
caches fetched history on disk so expensive warm-up is not repeated every cycle.
Attach a Render persistent disk and set KIS_CACHE_DIR to that mount for production.
"""
from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import requests

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "").strip()
KIS_ENV = os.getenv("KIS_ENV", "real").strip().lower() or "real"
if KIS_ENV not in {"real", "demo"}:
    KIS_ENV = "real"
KIS_BASE_URL = os.getenv(
    "KIS_BASE_URL",
    "https://openapi.koreainvestment.com:9443" if KIS_ENV == "real" else "https://openapivts.koreainvestment.com:29443",
).rstrip("/")
HTTP_TIMEOUT = max(3, min(int(os.getenv("KIS_HTTP_TIMEOUT_SEC", "8") or 8), 30))
MIN_INTERVAL = max(0.20, min(float(os.getenv("KIS_HTTP_MIN_INTERVAL_SEC", "0.30") or 0.30), 1.0))
CACHE_DIR = Path(os.getenv("KIS_CACHE_DIR", "/tmp/tajum_kis_cache") or "/tmp/tajum_kis_cache")
CACHE_TTL = max(300, min(int(os.getenv("KIS_CACHE_TTL_SEC", "21600") or 21600), 86400 * 7))
DOMESTIC_HISTORY_DAYS = max(5, min(int(os.getenv("KIS_DOMESTIC_HISTORY_DAYS", "22") or 22), 30))
DOMESTIC_PAGES_PER_DAY = max(1, min(int(os.getenv("KIS_DOMESTIC_PAGES_PER_DAY", "4") or 4), 6))
OVERSEAS_PAGES = max(1, min(int(os.getenv("KIS_OVERSEAS_PAGES", "4") or 4), 10))

RATE_LIMIT_COOLDOWN_SEC = max(0.5, min(float(os.getenv("KIS_RATE_LIMIT_COOLDOWN_SEC", "1.5") or 1.5), 10.0))
OVERSEAS_MINUTE_CACHE_TTL = max(45, min(int(os.getenv("KIS_OVERSEAS_MINUTE_CACHE_TTL_SEC", "75") or 75), 300))

_token_lock = threading.Lock()
_token_value = ""
_token_expiry = 0.0
_http_lock = threading.Lock()
_last_http_at = 0.0
_rate_blocked_until = 0.0
_us_exchange_cache: dict[str, str] = {}
_status_lock = threading.Lock()
_status: dict[str, Any] = {
    "configured": bool(KIS_APP_KEY and KIS_APP_SECRET),
    "env": KIS_ENV,
    "base_url": KIS_BASE_URL,
    "cache_dir": str(CACHE_DIR),
    "last_request_at": None,
    "last_success_at": None,
    "last_error": None,
    "last_symbol": None,
    "request_count": 0,
    "success_count": 0,
    "error_count": 0,
    "rate_limit_count": 0,
    "transient_retry_count": 0,
    "cache_hit_count": 0,
    "domestic_warmup_calls": 0,
    "domestic_warmup_days": 0,
}


def configured() -> bool:
    return bool(KIS_APP_KEY and KIS_APP_SECRET)


def status() -> dict[str, Any]:
    with _status_lock:
        return dict(_status)


def _status_update(**kwargs: Any) -> None:
    with _status_lock:
        _status.update(kwargs)


def _rate_guard() -> None:
    """Global KIS request pacer shared by every auto-engine worker thread."""
    global _last_http_at, _rate_blocked_until
    with _http_lock:
        now = time.monotonic()
        wait_interval = MIN_INTERVAL - (now - _last_http_at)
        wait_block = _rate_blocked_until - now
        wait = max(0.0, wait_interval, wait_block)
        if wait > 0:
            time.sleep(wait)
        _last_http_at = time.monotonic()


def _apply_global_backoff(seconds: float) -> None:
    """Extend the shared KIS cooldown so all workers slow down together."""
    global _rate_blocked_until
    delay = max(0.0, float(seconds))
    with _http_lock:
        target = time.monotonic() + delay
        if target > _rate_blocked_until:
            _rate_blocked_until = target


def _access_token() -> str:
    global _token_value, _token_expiry
    if not configured():
        raise RuntimeError("KIS credentials missing: set KIS_APP_KEY and KIS_APP_SECRET")
    now = time.time()
    with _token_lock:
        if _token_value and now < _token_expiry - 120:
            return _token_value
        _rate_guard()
        resp = requests.post(
            f"{KIS_BASE_URL}/oauth2/tokenP",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
            },
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()
        token = str(body.get("access_token") or "").strip()
        if not token:
            raise RuntimeError(f"KIS token missing: {body}")
        expires = int(body.get("expires_in") or 86400)
        _token_value = token
        _token_expiry = now + max(300, expires)
        return token


def _get(path: str, tr_id: str, params: dict[str, Any], *, tr_cont: str = "") -> tuple[dict[str, Any], dict[str, str]]:
    """KIS GET with one shared pacer and bounded, classified retries.

    Important:
    - KIS may return EGW00201 (per-second request limit) inside an HTTP 500 body.
      Detect that business code before treating the response as a generic 500.
    - A rate-limit event slows *all* worker threads, not just the failing request.
    - Generic HTTP 500 is retried once only. 502/503/504 and network timeouts are
      retried at most twice. Permanent business errors fail immediately.
    """
    if not configured():
        raise RuntimeError("KIS_NOT_CONFIGURED")

    last_exc: Exception | None = None
    for attempt in range(3):
        _rate_guard()
        with _status_lock:
            _status["request_count"] = int(_status.get("request_count", 0) or 0) + 1
            _status["last_request_at"] = datetime.now(timezone.utc).isoformat()

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {_access_token()}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
        }
        if tr_cont:
            headers["tr_cont"] = tr_cont

        try:
            resp = requests.get(f"{KIS_BASE_URL}{path}", headers=headers, params=params, timeout=HTTP_TIMEOUT)

            # KIS frequently embeds the real business error in JSON even when HTTP=500.
            body: dict[str, Any] | None = None
            try:
                parsed = resp.json()
                if isinstance(parsed, dict):
                    body = parsed
            except Exception:
                body = None

            msg_cd = str((body or {}).get("msg_cd") or "")
            msg1 = str((body or {}).get("msg1") or (body or {}).get("message") or "")
            rt_cd = str((body or {}).get("rt_cd", ""))

            # EGW00201 = KIS per-second transaction limit.
            if msg_cd == "EGW00201":
                with _status_lock:
                    _status["rate_limit_count"] = int(_status.get("rate_limit_count", 0) or 0) + 1
                delay = RATE_LIMIT_COOLDOWN_SEC * (attempt + 1)
                _apply_global_backoff(delay)
                last_exc = RuntimeError(f"KIS {msg_cd}: {msg1 or 'rate limit'}")
                if attempt < 2:
                    with _status_lock:
                        _status["transient_retry_count"] = int(_status.get("transient_retry_count", 0) or 0) + 1
                    continue
                raise last_exc

            if resp.status_code == 429:
                with _status_lock:
                    _status["rate_limit_count"] = int(_status.get("rate_limit_count", 0) or 0) + 1
                delay = RATE_LIMIT_COOLDOWN_SEC * (attempt + 1)
                _apply_global_backoff(delay)
                last_exc = requests.HTTPError(f"429 Client Error: {resp.text[:240]}", response=resp)
                if attempt < 2:
                    with _status_lock:
                        _status["transient_retry_count"] = int(_status.get("transient_retry_count", 0) or 0) + 1
                    continue
                raise last_exc

            if resp.status_code in {500, 502, 503, 504}:
                last_exc = requests.HTTPError(
                    f"{resp.status_code} Server Error: {resp.text[:240]}", response=resp
                )
                # Plain 500s are often bad historical cursor points; retry once only.
                max_retry_attempt = 1 if resp.status_code == 500 else 2
                if attempt < max_retry_attempt:
                    with _status_lock:
                        _status["transient_retry_count"] = int(_status.get("transient_retry_count", 0) or 0) + 1
                    _apply_global_backoff(0.7 * (attempt + 1))
                    continue
                raise last_exc

            resp.raise_for_status()
            if body is None:
                parsed = resp.json()
                if not isinstance(parsed, dict):
                    raise RuntimeError(f"KIS non-object response: {type(parsed).__name__}")
                body = parsed

            rt_cd = str(body.get("rt_cd", "0"))
            if rt_cd not in {"0", ""}:
                msg_cd = str(body.get("msg_cd") or "")
                msg1 = str(body.get("msg1") or "")
                raise RuntimeError(f"KIS {msg_cd}: {msg1}")

            with _status_lock:
                _status["success_count"] = int(_status.get("success_count", 0) or 0) + 1
                _status["last_success_at"] = datetime.now(timezone.utc).isoformat()
                _status["last_error"] = None
            return body, {str(k).lower(): str(v) for k, v in resp.headers.items()}

        except Exception as exc:
            last_exc = exc
            if isinstance(exc, (requests.ConnectionError, requests.Timeout)) and attempt < 2:
                with _status_lock:
                    _status["transient_retry_count"] = int(_status.get("transient_retry_count", 0) or 0) + 1
                _apply_global_backoff(0.7 * (attempt + 1))
                continue
            break

    with _status_lock:
        _status["error_count"] = int(_status.get("error_count", 0) or 0) + 1
        _status["last_error"] = f"{type(last_exc).__name__}: {last_exc}" if last_exc else "unknown"
    if last_exc:
        raise last_exc
    raise RuntimeError("KIS request failed")


def _num(value: Any) -> float:
    out = float(str(value or "0").replace(",", ""))
    if not math.isfinite(out):
        raise ValueError("non-finite KIS number")
    return out


def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _cache_path(kind: str, symbol: str) -> Path:
    safe = re.sub(r"[^A-Z0-9_-]", "_", symbol.upper())
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{kind}_{safe}.json"


def _cache_load(kind: str, symbol: str, max_age: int | float | None = None) -> list[dict[str, Any]] | None:
    path = _cache_path(kind, symbol)
    try:
        ttl = CACHE_TTL if max_age is None else max(1.0, float(max_age))
        if not path.exists() or time.time() - path.stat().st_mtime > ttl:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            with _status_lock:
                _status["cache_hit_count"] = int(_status.get("cache_hit_count", 0) or 0) + 1
            return data
        return None
    except Exception:
        return None


def _cache_save(kind: str, symbol: str, rows: list[dict[str, Any]]) -> None:
    try:
        path = _cache_path(kind, symbol)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass


def aggregate(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    bucket_ms = int(minutes) * 60_000
    out: list[dict[str, Any]] = []
    cur = None
    cur_bucket = None
    for row in sorted(rows, key=lambda x: int(x["open_time"])):
        bucket = int(row["open_time"]) // bucket_ms * bucket_ms
        if cur is None or bucket != cur_bucket:
            if cur is not None:
                out.append(cur)
            cur_bucket = bucket
            cur = {
                "open_time": bucket,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
                "close_time": int(row.get("close_time", row["open_time"])),
            }
        else:
            cur["high"] = max(float(cur["high"]), float(row["high"]))
            cur["low"] = min(float(cur["low"]), float(row["low"]))
            cur["close"] = float(row["close"])
            cur["volume"] = float(cur["volume"]) + float(row.get("volume", 0.0))
            cur["close_time"] = int(row.get("close_time", row["open_time"]))
    if cur is not None:
        out.append(cur)
    return out


def _day_aggregate(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 1:
        return rows
    out: list[dict[str, Any]] = []
    buf: list[dict[str, Any]] = []
    for row in rows:
        buf.append(row)
        if len(buf) == days:
            out.append({
                "open_time": int(buf[0]["open_time"]),
                "open": float(buf[0]["open"]),
                "high": max(float(x["high"]) for x in buf),
                "low": min(float(x["low"]) for x in buf),
                "close": float(buf[-1]["close"]),
                "volume": sum(float(x.get("volume", 0.0)) for x in buf),
                "close_time": int(buf[-1].get("close_time", buf[-1]["open_time"])),
            })
            buf = []
    return out


def domestic_daily(symbol: str, count_hint: int = 180) -> list[dict[str, Any]]:
    cached = _cache_load("kr_daily", symbol)
    if cached and len(cached) >= min(160, count_hint):
        return cached

    end_day = datetime.now(timezone(timedelta(hours=9))).date()
    while end_day.weekday() >= 5:
        end_day = _previous_weekday(end_day + timedelta(days=1))
    start_day = end_day - timedelta(days=max(360, count_hint * 3))
    all_rows: dict[int, dict[str, Any]] = {}
    current_end = end_day

    for _ in range(4):
        body, _ = _get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_DATE_1": start_day.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": current_end.strftime("%Y%m%d"),
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )
        raw = body.get("output2") or body.get("output") or []
        if not isinstance(raw, list) or not raw:
            break
        page_dates: list[str] = []
        before = len(all_rows)
        for item in raw:
            ds = str(item.get("stck_bsop_date") or "")
            if len(ds) != 8:
                continue
            try:
                dt = datetime.strptime(ds, "%Y%m%d").replace(tzinfo=timezone.utc)
                row = {
                    "open_time": _ms(dt),
                    "open": _num(item.get("stck_oprc")),
                    "high": _num(item.get("stck_hgpr")),
                    "low": _num(item.get("stck_lwpr")),
                    "close": _num(item.get("stck_clpr")),
                    "volume": _num(item.get("acml_vol")),
                    "close_time": _ms(dt),
                }
                all_rows[row["open_time"]] = row
                page_dates.append(ds)
            except Exception:
                continue
        if not page_dates or len(all_rows) == before:
            break
        if len(all_rows) >= count_hint:
            break
        oldest = min(page_dates)
        current_end = datetime.strptime(oldest, "%Y%m%d").date() - timedelta(days=1)
        if current_end <= start_day or len(raw) < 100:
            break

    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save("kr_daily", symbol, rows)
    return rows


def _parse_domestic_minute_item(item: dict[str, Any], fallback_date: str) -> dict[str, Any] | None:
    ds = str(item.get("stck_bsop_date") or fallback_date or "")
    hs = str(item.get("stck_cntg_hour") or item.get("cntg_hour") or "")[:6].zfill(6)
    if len(ds) != 8 or len(hs) != 6:
        return None
    try:
        # KST -> UTC timestamp
        local = datetime.strptime(ds + hs, "%Y%m%d%H%M%S").replace(tzinfo=timezone(timedelta(hours=9)))
        dt = local.astimezone(timezone.utc)
        close = _num(item.get("stck_prpr") or item.get("stck_clpr"))
        return {
            "open_time": _ms(dt),
            "open": _num(item.get("stck_oprc") or close),
            "high": _num(item.get("stck_hgpr") or close),
            "low": _num(item.get("stck_lwpr") or close),
            "close": close,
            "volume": _num(item.get("cntg_vol") or item.get("acml_vol") or 0),
            "close_time": _ms(dt),
        }
    except Exception:
        return None


def _previous_weekday(day):
    day = day - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def _kr_first_query_point(day):
    """Return (query_day, HHMMSS) accepted by KIS domestic minute history.

    Official KIS examples/backtester query historical minutes from 15:30 backwards.
    For today's open market we may start at the current KST minute; before 09:00 we
    use the previous weekday. Exchange holidays are handled by the caller as an
    empty/failed day and skipped without failing the whole symbol.
    """
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    if day.weekday() >= 5:
        return _previous_weekday(day + timedelta(days=1)), "153000"
    if day == now.date():
        hhmmss = now.strftime("%H%M00")
        if hhmmss < "090000":
            return _previous_weekday(day), "153000"
        if hhmmss > "153000":
            return day, "153000"
        return day, hhmmss
    return day, "153000"


def _fetch_domestic_minute_day(symbol: str, day, *, max_pages: int) -> list[dict[str, Any]]:
    """Fetch one KRX business day using the official 120-row cursor rules."""
    query_day, cursor_time = _kr_first_query_point(day)
    ds = query_day.strftime("%Y%m%d")
    out: dict[int, dict[str, Any]] = {}
    seen_cursors: set[str] = set()
    kst = timezone(timedelta(hours=9))

    for _ in range(max_pages):
        if cursor_time in seen_cursors:
            break
        seen_cursors.add(cursor_time)
        body, _ = _get(
            "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
            "FHKST03010230",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_HOUR_1": cursor_time,
                "FID_INPUT_DATE_1": ds,
                "FID_PW_DATA_INCU_YN": "Y",
                "FID_FAKE_TICK_INCU_YN": "",
            },
        )
        with _status_lock:
            _status["domestic_warmup_calls"] = int(_status.get("domestic_warmup_calls", 0) or 0) + 1

        raw = body.get("output2") or []
        if not isinstance(raw, list) or not raw:
            break
        requested_day_rows: list[dict[str, Any]] = []
        for item in raw:
            row = _parse_domestic_minute_item(item, ds)
            if not row:
                continue
            out[int(row["open_time"])] = row
            local_dt = datetime.fromtimestamp(row["open_time"] / 1000, timezone.utc).astimezone(kst)
            if local_dt.strftime("%Y%m%d") == ds:
                requested_day_rows.append(row)

        if not requested_day_rows:
            break
        oldest_dt = min(
            (datetime.fromtimestamp(r["open_time"] / 1000, timezone.utc).astimezone(kst) for r in requested_day_rows),
            key=lambda x: x.time(),
        )
        min_time = oldest_dt.strftime("%H%M%S")
        # Official KIS backtester termination rule.
        if len(raw) < 120 or min_time <= "090000":
            break
        cursor_time = min_time  # one-row overlap is intentional; dict dedups it

    return [out[k] for k in sorted(out)]


def domestic_minutes(symbol: str) -> list[dict[str, Any]]:
    """KRX 1m warm-up with long-lived history + cheap current-session refresh.

    Cold start: backfill only until >=35 aggregated 4h bars exist (normally ~18
    sessions, max four calls per session). Warm cycle: keep that history and refresh
    only the latest session tail (two pages while market is open). This avoids both
    the V143 hundreds-of-calls-per-cycle problem and a six-hour stale minute cache.
    """
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    latest_business_day = now.date()
    while latest_business_day.weekday() >= 5:
        latest_business_day = _previous_weekday(latest_business_day + timedelta(days=1))

    # Historical warm-up can live for days; the active session tail is refreshed
    # separately below. With a Render persistent disk this survives deploys.
    cached = _cache_load("kr_minute", symbol, max_age=86400 * 7) or []
    all_rows: dict[int, dict[str, Any]] = {}
    for row in cached:
        try:
            all_rows[int(row["open_time"])] = row
        except Exception:
            pass

    market_open = (
        now.weekday() < 5
        and now.time() >= datetime.strptime("090000", "%H%M%S").time()
        and now.time() <= datetime.strptime("153000", "%H%M%S").time()
    )
    # Refresh current/latest business session if market is open, or if the cache
    # does not yet contain that session. Two pages cover the live 4h bucket tail.
    cached_dates = set()
    for row in all_rows.values():
        try:
            local_dt = datetime.fromtimestamp(int(row["open_time"]) / 1000, timezone.utc).astimezone(kst)
            cached_dates.add(local_dt.date())
        except Exception:
            pass
    need_latest_day = market_open or latest_business_day not in cached_dates
    if need_latest_day:
        try:
            fresh_pages = 2 if cached else DOMESTIC_PAGES_PER_DAY
            for row in _fetch_domestic_minute_day(symbol, latest_business_day, max_pages=fresh_pages):
                all_rows[int(row["open_time"])] = row
        except (requests.HTTPError, RuntimeError):
            # Do not discard valid cached history if the latest session probe fails.
            pass

    # Cold/incomplete cache: walk backward only until the signal warm-up target is met.
    if len(aggregate([all_rows[k] for k in sorted(all_rows)], 240)) < 35:
        cursor_day = _previous_weekday(latest_business_day)
        sessions_with_data = 0
        calendar_scans = 0
        max_calendar_scans = DOMESTIC_HISTORY_DAYS * 2
        while sessions_with_data < DOMESTIC_HISTORY_DAYS and calendar_scans < max_calendar_scans:
            current_rows = [all_rows[k] for k in sorted(all_rows)]
            if len(aggregate(current_rows, 240)) >= 35:
                break
            calendar_scans += 1
            if cursor_day.weekday() >= 5:
                cursor_day = _previous_weekday(cursor_day + timedelta(days=1))
            try:
                day_rows = _fetch_domestic_minute_day(symbol, cursor_day, max_pages=DOMESTIC_PAGES_PER_DAY)
            except (requests.HTTPError, RuntimeError):
                day_rows = []
            if day_rows:
                for row in day_rows:
                    all_rows[int(row["open_time"])] = row
                sessions_with_data += 1
                with _status_lock:
                    _status["domestic_warmup_days"] = max(
                        int(_status.get("domestic_warmup_days", 0) or 0), sessions_with_data
                    )
            cursor_day = _previous_weekday(cursor_day)

    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save("kr_minute", symbol, rows)
    return rows


def resolve_us_exchange(symbol: str) -> str:
    symbol = symbol.upper().strip()
    cached = _us_exchange_cache.get(symbol)
    if cached:
        return cached
    # Product basic info API resolves market without requiring a pre-built US master.
    for product_type, excd in (("512", "NAS"), ("513", "NYS"), ("529", "AMS")):
        try:
            body, _ = _get(
                "/uapi/overseas-price/v1/quotations/search-info",
                "CTPF1702R",
                {"PRDT_TYPE_CD": product_type, "PDNO": symbol},
            )
            output = body.get("output") or {}
            if isinstance(output, list):
                output = output[0] if output else {}
            # KIS may return rt_cd=0 with an empty record, so require a recognizable code/name.
            text = " ".join(str(v or "") for v in output.values()).upper() if isinstance(output, dict) else ""
            if symbol in text or any(str(output.get(k) or "").strip() for k in ("prdt_name", "prdt_name120", "ovrs_item_name", "hts_eng_isnm")):
                _us_exchange_cache[symbol] = excd
                return excd
        except Exception:
            continue
    # Common default for the user's current tech-heavy watchlist; price lookup will fail visibly if wrong.
    _us_exchange_cache[symbol] = "NAS"
    return "NAS"


def overseas_current_price(symbol: str, exchange: str | None = None) -> float:
    excd = exchange or resolve_us_exchange(symbol)
    body, _ = _get(
        "/uapi/overseas-price/v1/quotations/price",
        "HHDFS00000300",
        {"AUTH": "", "EXCD": excd, "SYMB": symbol.upper()},
    )
    output = body.get("output") or {}
    if isinstance(output, list):
        output = output[0] if output else {}
    for key in ("last", "clos", "stck_prpr"):
        if isinstance(output, dict) and str(output.get(key) or "").strip():
            return _num(output[key])
    raise RuntimeError(f"KIS overseas current price missing {symbol}/{excd}")


def domestic_current_price(symbol: str) -> float:
    body, _ = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "FHKST01010100",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
    )
    output = body.get("output") or {}
    if isinstance(output, list):
        output = output[0] if output else {}
    return _num(output.get("stck_prpr"))


def overseas_minutes(symbol: str, minutes: int, exchange: str | None = None) -> list[dict[str, Any]]:
    excd = exchange or resolve_us_exchange(symbol)
    kind = f"us_{excd}_{minutes}m"
    cached = _cache_load(kind, symbol, max_age=OVERSEAS_MINUTE_CACHE_TTL)
    if cached:
        return cached
    all_rows: dict[int, dict[str, Any]] = {}
    keyb = ""
    next_flag = ""
    for page in range(OVERSEAS_PAGES):
        body, headers = _get(
            "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
            "HHDFS76950200",
            {
                "AUTH": "",
                "EXCD": excd,
                "SYMB": symbol.upper(),
                "NMIN": str(minutes),
                "PINC": "0" if page == 0 else "1",
                "NEXT": next_flag,
                "NREC": "120",
                "FILL": "",
                "KEYB": keyb,
            },
            tr_cont="" if page == 0 else "N",
        )
        raw = body.get("output2") or []
        if not isinstance(raw, list) or not raw:
            break
        parsed = []
        for item in raw:
            ds = str(item.get("tymd") or item.get("xymd") or item.get("date") or "")
            hs = str(item.get("xhms") or item.get("hour") or item.get("time") or "").replace(":", "")[:6].zfill(6)
            if len(ds) != 8:
                continue
            try:
                dt = datetime.strptime(ds + hs, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                close = _num(item.get("last") or item.get("clos"))
                row = {
                    "open_time": _ms(dt),
                    "open": _num(item.get("open") or close),
                    "high": _num(item.get("high") or close),
                    "low": _num(item.get("low") or close),
                    "close": close,
                    "volume": _num(item.get("evol") or item.get("tvol") or 0),
                    "close_time": _ms(dt),
                }
                all_rows[row["open_time"]] = row
                parsed.append(row)
            except Exception:
                continue
        if not parsed:
            break
        oldest = min(parsed, key=lambda x: x["open_time"])
        keyb = (datetime.fromtimestamp(oldest["open_time"] / 1000, timezone.utc) - timedelta(minutes=minutes)).strftime("%Y%m%d%H%M%S")
        next_flag = "1"
        if str(headers.get("tr_cont", "")).upper() not in {"M", "F"} and page > 0:
            break
    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save(kind, symbol, rows)
    return rows


def overseas_daily(symbol: str, exchange: str | None = None) -> list[dict[str, Any]]:
    excd = exchange or resolve_us_exchange(symbol)
    kind = f"us_{excd}_daily"
    cached = _cache_load(kind, symbol)
    if cached:
        return cached
    all_rows: dict[int, dict[str, Any]] = {}

    # KIS official dailyprice continuation keeps the same BYMD and advances with
    # the tr_cont header. V142 changed BYMD to the last returned date, which could
    # jump far backwards and eventually produce 500 responses. Restore the official
    # continuation pattern and use the latest weekday as the query anchor.
    anchor = datetime.now(timezone.utc).date()
    while anchor.weekday() >= 5:
        anchor -= timedelta(days=1)
    bymd = anchor.strftime("%Y%m%d")
    tr_cont = ""
    seen_pages: set[tuple[str, str]] = set()
    for page in range(max(2, OVERSEAS_PAGES)):
        try:
            body, headers = _get(
                "/uapi/overseas-price/v1/quotations/dailyprice",
                "HHDFS76240000",
                {"AUTH": "", "EXCD": excd, "SYMB": symbol.upper(), "GUBN": "0", "BYMD": bymd, "MODP": "1"},
                tr_cont=tr_cont,
            )
        except requests.HTTPError:
            # If the anchor is a US holiday, move back one weekday and retry once
            # only before giving up/using already accumulated rows.
            if page == 0:
                anchor = _previous_weekday(anchor)
                bymd = anchor.strftime("%Y%m%d")
                try:
                    body, headers = _get(
                        "/uapi/overseas-price/v1/quotations/dailyprice",
                        "HHDFS76240000",
                        {"AUTH": "", "EXCD": excd, "SYMB": symbol.upper(), "GUBN": "0", "BYMD": bymd, "MODP": "1"},
                        tr_cont=tr_cont,
                    )
                except requests.HTTPError:
                    break
            else:
                break
        raw = body.get("output2") or []
        if not isinstance(raw, list) or not raw:
            break
        page_dates: list[str] = []
        before = len(all_rows)
        for item in raw:
            ds = str(item.get("xymd") or "")
            if len(ds) != 8:
                continue
            try:
                dt = datetime.strptime(ds, "%Y%m%d").replace(tzinfo=timezone.utc)
                row = {
                    "open_time": _ms(dt),
                    "open": _num(item.get("open")),
                    "high": _num(item.get("high")),
                    "low": _num(item.get("low")),
                    "close": _num(item.get("clos")),
                    "volume": _num(item.get("tvol") or 0),
                    "close_time": _ms(dt),
                }
                all_rows[row["open_time"]] = row
                page_dates.append(ds)
            except Exception:
                continue
        if not page_dates:
            break
        signature = (min(page_dates), max(page_dates))
        if signature in seen_pages or len(all_rows) == before:
            break
        seen_pages.add(signature)
        if len(all_rows) >= 220:  # enough for 31+ weekly bars after 5-day aggregation
            break
        # Official KIS continuation: move BYMD to the last returned trading date
        # and pass tr_cont=N when the response indicates more data.
        oldest_ds = min(page_dates)
        bymd = oldest_ds
        cont = str(headers.get("tr_cont", "")).upper()
        if cont not in {"M", "F"}:
            break
        tr_cont = "N"
    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save(kind, symbol, rows)
    return rows


def rows(symbol: str, timeframe: str) -> tuple[str, list[dict[str, Any]]]:
    """Return (market, OHLC rows) for KRX six-digit or US ticker symbol."""
    symbol = symbol.upper().strip()
    _status_update(last_symbol=symbol)
    domestic = bool(re.fullmatch(r"\d{6}", symbol))
    market = "KOREA" if domestic else "US"
    if timeframe in {"30m", "1h", "4h"}:
        minutes = {"30m": 30, "1h": 60, "4h": 240}[timeframe]
        if domestic:
            return market, aggregate(domestic_minutes(symbol), minutes)
        return market, overseas_minutes(symbol, minutes)
    if timeframe in {"1d", "3d", "1w"}:
        daily = domestic_daily(symbol) if domestic else overseas_daily(symbol)
        if timeframe == "1d":
            return market, daily
        if timeframe == "3d":
            return market, _day_aggregate(daily, 3)
        return market, _day_aggregate(daily, 5)  # trading-week approximation from daily bars
    raise ValueError(f"unsupported KIS stock timeframe: {timeframe}")


def current_price(symbol: str) -> tuple[str, float]:
    symbol = symbol.upper().strip()
    if re.fullmatch(r"\d{6}", symbol):
        return "KOREA", domestic_current_price(symbol)
    return "US", overseas_current_price(symbol)


# Official KIS master-file source used by its own open-trading-api examples.
_KRX_MASTER_URLS = {
    "KOSPI": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
    "KOSDAQ": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
}
_master_cache: tuple[float, list[dict[str, str]]] = (0.0, [])
_master_lock = threading.Lock()


def domestic_master() -> list[dict[str, str]]:
    """All KOSPI/KOSDAQ symbols for app search; public KIS master downloads, no API key."""
    global _master_cache
    now = time.time()
    if _master_cache[1] and now - _master_cache[0] < 21600:
        return list(_master_cache[1])
    with _master_lock:
        if _master_cache[1] and now - _master_cache[0] < 21600:
            return list(_master_cache[1])
        out: dict[str, dict[str, str]] = {}
        for exchange, url in _KRX_MASTER_URLS.items():
            try:
                resp = requests.get(url, timeout=HTTP_TIMEOUT)
                resp.raise_for_status()
                with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                    names = zf.namelist()
                    if not names:
                        continue
                    content = zf.read(names[0])
                for line in content.split(b"\n"):
                    if len(line) < 61:
                        continue
                    code = line[0:9].decode("euc-kr", errors="ignore").strip()
                    name = line[21:61].decode("euc-kr", errors="ignore").strip()
                    if len(code) > 6:
                        code = code[-6:]
                    if re.fullmatch(r"\d{6}", code) and name:
                        out[code] = {"symbol": code, "name": name, "exchange": exchange}
            except Exception:
                continue
        values = sorted(out.values(), key=lambda x: x["symbol"])
        if values:
            _master_cache = (now, values)
        return list(_master_cache[1])


def domestic_name(symbol: str) -> str:
    """Best-effort Korean company name from the official KIS KOSPI/KOSDAQ master."""
    code = str(symbol or "").strip().upper()
    if not re.fullmatch(r"\d{6}", code):
        return ""
    for item in domestic_master():
        if str(item.get("symbol") or "").strip() == code:
            return str(item.get("name") or "").strip()
    return ""
