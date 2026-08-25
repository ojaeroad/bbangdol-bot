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
import re
import threading
from typing import Any

import firebase_admin
from firebase_admin import credentials, messaging

log = logging.getLogger("bbangdol-push")
_FIREBASE_LOCK = threading.Lock()
_FIREBASE_APP = None

_FIXED_SOUND_PROFILES = {
    "system": None,
    "spark": "tajum_spark",
    "cash": "tajum_cash",
    "siren": "tajum_siren",
    "clear": "tajum_clear",
    "soft": "tajum_soft",
    "bright": "tajum_bright",
    "deep": "tajum_deep",
    "pulse": "tajum_pulse",
    "silent": None,
}
_CUSTOM_PROFILE_RE = re.compile(r"^custom_[0-9a-f]{12}$")


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
            _FIREBASE_APP = firebase_admin.initialize_app()
        return _FIREBASE_APP


def push_health() -> dict[str, Any]:
    info = None
    try:
        info = _service_account_info()
    except Exception as exc:
        return {
            "configured": False,
            "ready": False,
            "error": f"credential_parse:{type(exc).__name__}",
        }
    configured = bool(info) or bool(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    )
    if not configured:
        return {"configured": False, "ready": False, "project_id": None}
    try:
        app = _get_firebase_app()
        return {
            "configured": True,
            "ready": True,
            "project_id": getattr(app, "project_id", None),
        }
    except Exception as exc:
        log.exception("Firebase push initialization failed")
        return {"configured": True, "ready": False, "error": type(exc).__name__}


def notification_delivery_profile(
    sound_profile: str,
    vibration_enabled: bool,
) -> dict[str, Any]:
    """Return the Android channel/sound pair matching the app-created channel.

    Custom user sounds use a URI-backed Android NotificationChannel created on
    the device. The server only needs the deterministic channel id; it must not
    send a raw-resource sound name for those profiles.
    """
    profile = str(sound_profile or "clear").strip().lower()[:24]
    if profile not in _FIXED_SOUND_PROFILES and not _CUSTOM_PROFILE_RE.fullmatch(profile):
        profile = "clear"

    vib = bool(vibration_enabled)
    vibration_key = "vib" if vib else "novib"
    channel_id = f"tajum_{profile}_{vibration_key}_v1"

    if _CUSTOM_PROFILE_RE.fullmatch(profile):
        sound = None
    elif profile == "system":
        sound = "default"
    else:
        sound = _FIXED_SOUND_PROFILES.get(profile)

    return {
        "profile": profile,
        "channel_id": channel_id,
        "sound": sound,
        "vibration_enabled": vib,
    }


def send_push_to_tokens(
    tokens: list[str],
    title: str,
    body: str,
    data: dict[str, Any] | None = None,
    *,
    channel_id: str | None = None,
    sound: str | None = "default",
    vibration_enabled: bool | None = True,
) -> dict[str, Any]:
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
        return {
            "ok": True,
            "requested": 0,
            "success": 0,
            "failure": 0,
            "successful_tokens": [],
            "failed_tokens": [],
        }

    app = _get_firebase_app()
    clean_data = {
        str(key): str(value)
        for key, value in (data or {}).items()
        if value is not None
    }

    android_notification_kwargs: dict[str, Any] = {}
    if channel_id:
        android_notification_kwargs["channel_id"] = str(channel_id)
    if sound:
        android_notification_kwargs["sound"] = str(sound)
    if vibration_enabled is not None:
        android_notification_kwargs["default_vibrate_timings"] = bool(vibration_enabled)

    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=str(title), body=str(body)),
        data=clean_data,
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(
                **android_notification_kwargs,
            ),
        ),
        tokens=clean_tokens,
    )

    response = messaging.send_each_for_multicast(message, app=app)
    successful_tokens: list[str] = []
    failed_tokens: list[str] = []
    for index, item in enumerate(response.responses):
        if item.success:
            successful_tokens.append(clean_tokens[index])
        else:
            failed_tokens.append(clean_tokens[index])

    return {
        "ok": response.failure_count == 0,
        "requested": len(clean_tokens),
        "success": int(response.success_count),
        "failure": int(response.failure_count),
        "successful_tokens": successful_tokens,
        "failed_tokens": failed_tokens,
    }
