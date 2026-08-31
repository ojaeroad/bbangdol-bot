"""Tajum On member/FCM service - V147.

This module is deployable as a separate Render Web Service:
    gunicorn -w 2 -b 0.0.0.0:$PORT tajumon_member_service:app

It owns member device registration, watchlist persistence, FCM fan-out and member alert
history.  The core market/signal service can keep the existing public URL and proxy only
these routes to this service after TAJUM_MEMBER_SERVICE_URL is configured.
"""
from __future__ import annotations

import hmac
import logging
import os
from collections import defaultdict
from typing import Any

from flask import Flask, jsonify, request

from firebase_push import notification_delivery_profile, push_health, send_push_to_tokens
from performance_store import (
    app_active_symbols,
    app_device_summary,
    app_devices_for_symbol,
    filter_app_devices_by_push_cooldown,
    recent_app_push_history,
    remove_app_device_token,
    save_app_push_history,
    upsert_app_device,
)

log = logging.getLogger("tajumon-member-service")
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
SERVICE_VERSION = "V147_MEMBER_SERVICE_1"
INTERNAL_SECRET = os.getenv("TAJUM_MEMBER_INTERNAL_SECRET", "").strip()

_GROUP_TIMEFRAMES = {
    "COIN": {
        "SCALP": ["5m", "15m"],
        "SWING": ["30m", "1h"],
        "LONG": ["4h", "6h"],
        "LIFE": ["12h", "1d", "1w"],
    },
    "STOCK": {
        "SWING": ["30m", "1h"],
        "LONG": ["4h", "1d"],
        "LIFE": ["3d", "1w"],
    },
}


def _clean_symbol(value: Any) -> str:
    return str(value or "").strip().upper().replace("BINANCE:", "").replace("UPBIT:", "")[:100]


def _internal_ok() -> bool:
    if not INTERNAL_SECRET:
        return False
    provided = request.headers.get("X-Tajum-Member-Secret", "").strip()
    return bool(provided) and hmac.compare_digest(provided, INTERNAL_SECRET)


def _require_internal():
    if not INTERNAL_SECRET:
        return jsonify({"ok": False, "error": "TAJUM_MEMBER_INTERNAL_SECRET_not_configured"}), 503
    if not _internal_ok():
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None


def _device_allows_group(device: dict[str, Any], payload: dict[str, Any]) -> bool:
    group_key = str(payload.get("group_key", "") or "").strip().upper()
    symbol = _clean_symbol(payload.get("symbol"))
    if not group_key or not symbol:
        return True
    prefs = device.get("enabled_signal_groups")
    if not isinstance(prefs, dict) or not prefs:
        return True
    raw = prefs.get(symbol)
    if raw is None or not isinstance(raw, list):
        return True
    enabled = {str(x or "").strip().upper() for x in raw if str(x or "").strip()}
    return group_key in enabled


@app.get("/ping")
def ping():
    return "pong", 200


@app.get("/version")
def version():
    return jsonify({
        "ok": True,
        "service": "tajumon-member-fcm",
        "version": SERVICE_VERSION,
        "firebase": push_health(),
    }), 200


@app.post("/app/device/register")
def device_register():
    data = request.get_json(silent=True, force=True) or {}
    raw_symbols = data.get("enabled_symbols", [])
    if isinstance(raw_symbols, str):
        raw_symbols = [x.strip() for x in raw_symbols.split(",") if x.strip()]
    if not isinstance(raw_symbols, list):
        return jsonify({"ok": False, "error": "enabled_symbols_must_be_list"}), 400

    raw_groups = data.get("enabled_signal_groups")
    groups = None
    if raw_groups is not None:
        if not isinstance(raw_groups, dict):
            return jsonify({"ok": False, "error": "enabled_signal_groups_must_be_object"}), 400
        groups = {}
        for sym, values in raw_groups.items():
            if isinstance(values, list):
                groups[_clean_symbol(sym)] = [str(v or "").strip().upper() for v in values if str(v or "").strip()]

    try:
        saved = upsert_app_device(
            device_id=str(data.get("device_id", "") or "").strip(),
            fcm_token=str(data.get("fcm_token", "") or "").strip(),
            enabled_symbols=[_clean_symbol(x) for x in raw_symbols if _clean_symbol(x)],
            notifications_enabled=bool(data.get("notifications_enabled", True)),
            platform=str(data.get("platform", "android") or "android").strip(),
            sound_profile=str(data.get("sound_profile", "clear") or "clear").strip().lower(),
            vibration_enabled=bool(data.get("vibration_enabled", True)),
            enabled_signal_groups=groups,
        )
        return jsonify({"ok": True, "persisted": True, "live_subscription": False, "member_service": True, **saved}), 200
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        log.exception("member device register failed")
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/app/push/health")
def member_push_health():
    try:
        devices = app_device_summary()
    except Exception as exc:
        log.exception("member push health DB failed")
        devices = {"database": "error", "error": type(exc).__name__, "device_count": 0, "push_enabled_count": 0}
    return jsonify({"ok": True, "firebase": push_health(), "devices": devices}), 200


@app.get("/app/alerts/recent")
def recent_alerts():
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"ok": False, "error": "empty_device_id"}), 400
    symbols = [_clean_symbol(x) for x in request.args.get("symbols", "").split(",") if _clean_symbol(x)]
    try:
        limit = max(1, min(int(request.args.get("limit", "300") or 300), 300))
    except (TypeError, ValueError):
        limit = 300
    try:
        rows = recent_app_push_history(device_id, symbols or None, limit, 30)
        normalized = []
        for raw in rows:
            row = dict(raw)
            direction = str(row.get("direction", "") or "").upper()
            stage = max(0, min(int(row.get("stage", 0) or 0), 3))
            side = "매수" if direction == "LOW" else "매도"
            row["alert_label"] = (
                f"{side} 대기" if stage == 0
                else f"분할 {'매수' if direction == 'LOW' else '매도'} {stage}차 타점"
            )
            normalized.append(row)
        return jsonify({
            "ok": True,
            "source": "delivered_fcm_history",
            "retention": {"max_count": 300, "max_days": 30},
            "symbols": symbols,
            "count": len(normalized),
            "alerts": normalized,
            "member_service": True,
        }), 200
    except Exception as exc:
        log.exception("member recent alerts failed")
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/internal/subscriptions/active")
def internal_active_subscriptions():
    denied = _require_internal()
    if denied:
        return denied
    try:
        symbols = app_active_symbols()
        return jsonify({"ok": True, "symbol_count": len(symbols), "symbols": symbols}), 200
    except Exception as exc:
        log.exception("active subscription query failed")
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/internal/health")
def internal_health():
    denied = _require_internal()
    if denied:
        return denied
    try:
        return jsonify({"ok": True, "version": SERVICE_VERSION, "devices": app_device_summary(), "firebase": push_health()}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.post("/internal/push/fanout")
def internal_push_fanout():
    denied = _require_internal()
    if denied:
        return denied
    data = request.get_json(silent=True, force=True) or {}
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    source = str(data.get("source", "AUTO") or "AUTO").strip().upper()
    symbol = _clean_symbol(payload.get("symbol"))
    if not symbol or not payload.get("title") or not payload.get("body"):
        return jsonify({"ok": False, "error": "invalid_payload"}), 400

    try:
        devices = [d for d in app_devices_for_symbol(symbol, 500) if _device_allows_group(d, payload)]
        if not devices:
            return jsonify({"ok": True, "success": 0, "failure": 0, "recipients": 0, "history": 0}), 200

        market_type = "COIN" if str(payload.get("market", "") or "").upper() == "COIN" else "STOCK"
        group_key = str(payload.get("group_key", "") or "").strip().upper()
        group_tfs = payload.get("group_timeframes")
        if not isinstance(group_tfs, list) or not group_tfs:
            group_tfs = _GROUP_TIMEFRAMES.get(market_type, {}).get(group_key, [str(payload.get("timeframe", "") or "")])
        cooldown = max(1, min(int(data.get("cooldown_minutes", 5) or 5), 120))
        devices, blocked = filter_app_devices_by_push_cooldown(
            devices, symbol, str(payload.get("side", "") or ""), group_tfs, cooldown
        )
        if not devices:
            return jsonify({"ok": True, "success": 0, "failure": 0, "recipients": 0, "cooldown_blocked": blocked, "history": 0}), 200

        groups: dict[tuple[str, bool], list[dict[str, Any]]] = defaultdict(list)
        for device in devices:
            groups[(str(device.get("sound_profile", "clear") or "clear").lower(), bool(device.get("vibration_enabled", True)))].append(device)

        successful: set[str] = set()
        failed: set[str] = set()
        total_success = total_failure = 0
        push_data = {
            key: payload.get(key, "")
            for key in (
                "symbol", "display", "market", "exchange", "category_key", "category_label",
                "group_key", "group_label", "direction", "side", "timeframe", "stage",
                "alert_label", "signal_price", "route"
            )
        }
        push_data["source"] = source

        for (profile, vibration), group in groups.items():
            delivery = notification_delivery_profile(profile, vibration)
            result = send_push_to_tokens(
                [item["fcm_token"] for item in group],
                str(payload.get("title", "")),
                str(payload.get("body", "")),
                push_data,
                channel_id=delivery["channel_id"],
                sound=delivery["sound"],
                vibration_enabled=delivery["vibration_enabled"],
            )
            total_success += int(result.get("success", 0) or 0)
            total_failure += int(result.get("failure", 0) or 0)
            successful.update(result.get("successful_tokens", []))
            failed.update(result.get("failed_tokens", []))

        for token in failed:
            remove_app_device_token(token)

        deliveries = []
        for device in devices:
            if device.get("fcm_token") not in successful:
                continue
            deliveries.append({
                "device_id": device.get("device_id"),
                "delivery_key": payload.get("delivery_key"),
                "symbol": symbol,
                "display": payload.get("display"),
                "market": payload.get("market"),
                "exchange": payload.get("exchange"),
                "direction": payload.get("direction"),
                "side": payload.get("side"),
                "timeframe": payload.get("timeframe"),
                "stage": payload.get("stage"),
                "alert_label": payload.get("alert_label"),
                "signal_price": payload.get("signal_price"),
                "route": payload.get("route"),
                "occurred_at": payload.get("occurred_at"),
                "source": source,
            })
        history_count = save_app_push_history(deliveries) if deliveries else 0
        return jsonify({
            "ok": True,
            "success": total_success,
            "failure": total_failure,
            "recipients": len(devices),
            "cooldown_blocked": blocked,
            "history": history_count,
        }), 200
    except Exception as exc:
        log.exception("member FCM fanout failed symbol=%s", symbol)
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500
