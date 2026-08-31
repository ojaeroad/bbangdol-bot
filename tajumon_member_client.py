"""Core -> detached Tajum On member/FCM service client.

If TAJUM_MEMBER_SERVICE_URL is empty, the existing in-process member/FCM path remains
active.  This allows a zero-downtime migration: deploy the member service first, verify
it, then set two environment variables on the core service.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any

import requests

BASE_URL = os.getenv("TAJUM_MEMBER_SERVICE_URL", "").strip().rstrip("/")
SECRET = os.getenv("TAJUM_MEMBER_INTERNAL_SECRET", "").strip()
TIMEOUT = max(1.0, min(float(os.getenv("TAJUM_MEMBER_HTTP_TIMEOUT_SEC", "3") or 3), 10.0))
_CACHE_LOCK = threading.Lock()
_CACHE_AT = 0.0
_CACHE_SYMBOLS: list[str] = []


def configured() -> bool:
    return bool(BASE_URL and SECRET)


def _headers() -> dict[str, str]:
    return {"X-Tajum-Member-Secret": SECRET}


def active_symbols(cache_seconds: float = 5.0) -> list[str]:
    global _CACHE_AT, _CACHE_SYMBOLS
    if not configured():
        raise RuntimeError("Tajum member service is not configured")
    now = time.monotonic()
    with _CACHE_LOCK:
        if _CACHE_SYMBOLS and now - _CACHE_AT < max(0.0, cache_seconds):
            return list(_CACHE_SYMBOLS)
    r = requests.get(f"{BASE_URL}/internal/subscriptions/active", headers=_headers(), timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    symbols = [str(x or "").strip().upper() for x in (data.get("symbols") or []) if str(x or "").strip()]
    with _CACHE_LOCK:
        _CACHE_AT = now
        _CACHE_SYMBOLS = symbols
    return list(symbols)


def fanout(payload: dict[str, Any], source: str, cooldown_minutes: int = 5) -> dict[str, Any]:
    if not configured():
        raise RuntimeError("Tajum member service is not configured")
    r = requests.post(
        f"{BASE_URL}/internal/push/fanout",
        headers={**_headers(), "Content-Type": "application/json"},
        json={"payload": payload, "source": source, "cooldown_minutes": int(cooldown_minutes)},
        timeout=max(TIMEOUT, 8.0),
    )
    r.raise_for_status()
    return r.json()


def health() -> dict[str, Any]:
    if not configured():
        return {"configured": False}
    try:
        r = requests.get(f"{BASE_URL}/internal/health", headers=_headers(), timeout=TIMEOUT)
        return {"configured": True, "http_status": r.status_code, **(r.json() if r.content else {})}
    except Exception as exc:
        return {"configured": True, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def proxy(method: str, path: str, *, params: Any = None, json_body: Any = None) -> tuple[dict[str, Any], int]:
    if not configured():
        raise RuntimeError("Tajum member service is not configured")
    r = requests.request(
        method.upper(), f"{BASE_URL}{path}", params=params, json=json_body,
        headers={"Content-Type": "application/json"}, timeout=max(TIMEOUT, 8.0),
    )
    try:
        body = r.json()
    except Exception:
        body = {"ok": False, "error": "invalid_member_service_response"}
    return body, r.status_code
