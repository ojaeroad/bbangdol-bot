"""Firebase Cloud Messaging helper for Tajum On.

Push failures must never interrupt the existing Telegram webhook flow.
Credentials are loaded lazily from Render environment variables and are never
stored in GitHub.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import threading
from typing import Any

import firebase_admin
from firebase_admin import credentials, messaging

log = logging.getLogger("bbangdol-push")

_FIREBASE_LOCK = threading.Lock()
_FIREBASE_APP = None


def _service_account_info() -> dict[str, Any] | None:
    raw_b64 = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON_B64", "").strip()
    if raw_b64:
        decoded = base64.b64decode(raw_b64).decode("utf-8")
        return json.loads(decoded)
    raw_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_json:
        return json.loads(raw_json)
    return None


def _get_firebase_app():
    global _FIREBASE_APP
    if _FIREBASE_APP is not None:
        return _FIREBASE_APP
    with _FIREBASE_LOCK:
        if _FIREBASE_APP is not None:
            return _FIREBASE_APP
        info = _service_account_info()
        if info:
            cred = credentials.Certificate(info)
            _FIREBASE_APP = firebase_admin.initialize_app(cred)
        else:
            # Optional fallback for environments configured with GOOGLE_APPLICATION_CREDENTIALS.
            _FIREBASE_APP = firebase_admin.initialize_app()
        return _FIREBASE_APP


def push_health() -> dict[str, Any]:
    info = None
    try:
        info = _service_account_info()
    except Exception as exc:
        return {"configured": False, "ready": False, "error": f"credential_parse:{type(exc).__name__}"}
    configured = bool(info) or bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip())
    if not configured:
        return {"configured": False, "ready": False, "project_id": None}
    try:
        app = _get_firebase_app()
        return {"configured": True, "ready": True, "project_id": getattr(app, "project_id", None)}
    except Exception as exc:
        log.exception("Firebase push initialization failed")
        return {"configured": True, "ready": False, "error": type(exc).__name__}


def send_push_to_tokens(tokens: list[str], title: str, body: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    clean_tokens: list[str] = []
    seen: set[str] = set()
    for token in tokens or []:
        t = str(token or "").strip()
        if t and t not in seen:
            seen.add(t)
            clean_tokens.append(t)
        if len(clean_tokens) >= 500:
            break
    if not clean_tokens:
        return {"ok": True, "requested": 0, "success": 0, "failure": 0, "failed_tokens": [], "successful_tokens": []}

    app = _get_firebase_app()
    clean_data = {str(k): str(v) for k, v in (data or {}).items() if v is not None}
    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=str(title), body=str(body)),
        data=clean_data,
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(sound="default"),
        ),
        tokens=clean_tokens,
    )
    response = messaging.send_each_for_multicast(message, app=app)
    failed_tokens: list[str] = []
    successful_tokens: list[str] = []
    for idx, item in enumerate(response.responses):
        if item.success:
            successful_tokens.append(clean_tokens[idx])
        else:
            failed_tokens.append(clean_tokens[idx])
    return {
        "ok": response.failure_count == 0,
        "requested": len(clean_tokens),
        "success": int(response.success_count),
        "failure": int(response.failure_count),
        "failed_tokens": failed_tokens,
        "successful_tokens": successful_tokens,
    }
