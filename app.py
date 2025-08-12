# app.py
import os
import json
import logging
import time
from typing import Dict, Any
from flask import Flask, request, jsonify
import requests

# -------------------------------
# Telegram Bot 설정
# -------------------------------
BOT_TOKEN = "7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU"
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Chat ID 매핑 (Pine route → Telegram Chat ID)
ROUTE_TO_CHAT: Dict[str, int] = {
    # OS routes
    "OS_SCALP": -4870905408,
    "OS_SHORT": -4820497789,
    "OS_SWING": -4912298868,
    "OS_LONG":  -1002529014389,  # long 값은 지수표기 아닌 정수 그대로

    # OB routes
    "OB_SWING": -4825365651,
    "OB_LONG":  -4906640026,
}

MAX_LEN = 3900  # 텔레그램 메시지 길이 안전 마진
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# -------------------------------
# 유틸
# -------------------------------
def safe_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    if len(s) > MAX_LEN:
        return s[:MAX_LEN - 20] + "\n...[truncated]"
    return s

def post_telegram(chat_id: int, text: str, parse_mode: str = None) -> Dict[str, Any]:
    payload = {
        "chat_id": chat_id,
        "text": safe_text(text),
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    last_err = None
    for attempt in range(3):
        try:
            r = requests.post(TELEGRAM_API, json=payload, timeout=10)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code} {r.text}"
            time.sleep(0.6)
        except Exception as e:
            last_err = str(e)
            time.sleep(0.6)
    raise RuntimeError(f"sendMessage failed: {last_err}")

def route_to_chat_id(route: str) -> int:
    rid = ROUTE_TO_CHAT.get(route)
    if rid is None:
        raise KeyError(f"Unknown route: {route}")
    return rid

# -------------------------------
# 헬스체크
# -------------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "status": "healthy"})

@app.get("/routes")
def routes_dump():
    return jsonify({"routes": list(ROUTE_TO_CHAT.keys())})

# -------------------------------
# TradingView Webhook 엔드포인트
# -------------------------------
@app.post("/tv")
def tv_webhook():
    try:
        data = request.get_json(silent=True, force=True) or {}
    except Exception:
        raw = request.data.decode("utf-8", errors="ignore")
        try:
            data = json.loads(raw)
        except Exception:
            log.error("Invalid payload (not JSON)")
            return jsonify({"ok": False, "error": "invalid_json"}), 400

    route = str(data.get("route", "")).strip()
    msg    = str(data.get("msg", "")).strip()

    if not route or not msg:
        return jsonify({"ok": False, "error": "missing route or msg"}), 400

    try:
        chat_id = route_to_chat_id(route)
    except KeyError as e:
        log.error(f"[DROP] {e}. payload={data}")
        return jsonify({"ok": False, "error": "unknown_route"}), 200

    try:
        res = post_telegram(chat_id, msg)
        ok = bool(res.get("ok"))
        if not ok:
            log.error(f"TG send failed: {res}")
            return jsonify({"ok": False, "error": "telegram_failed", "detail": res}), 500
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("Telegram send exception")
        return jsonify({"ok": False, "error": "exception", "detail": str(e)}), 500

# -------------------------------
# 로컬 실행
# -------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
