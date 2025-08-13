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
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env is missing")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Chat ID 매핑 (Pine의 route -> Telegram Chat ID)
# 기존 OS/OB 라우팅은 그대로 유지 + 주요지표(AUX_4INDEX) 하드코딩 추가
ROUTE_TO_CHAT: Dict[str, int] = {
    # ===== OS (과매도) =====
    "OS_SCALP": -4870905408,
    "OS_SHORT": -4820497789,
    "OS_SWING": -4912298868,
    "OS_LONG":  -1002529014389,

    # ===== OB (과매수) =====
    "OB_SWING": -4825365651,
    "OB_LONG":  -4906640026,

    # ===== 주요지표 전용 (하드코딩) =====
    "AUX_4INDEX": -4872204876,
}

MAX_LEN = 3900
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

def post_telegram(chat_id: int, text: str, parse_mode: str | None = None) -> Dict[str, Any]:
    payload = {"chat_id": chat_id, "text": safe_text(text)}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    last_err = None
    for _ in range(3):
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
    if route not in ROUTE_TO_CHAT:
        raise KeyError(f"Unknown or unmapped route: {route}")
    return ROUTE_TO_CHAT[route]

# -------------------------------
# 헬스체크 & 라우트 확인
# -------------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "status": "healthy"})

@app.get("/routes")
def routes_dump():
    # 디버깅용: 현재 라우트 -> chat id 매핑 확인
    return jsonify({"routes": ROUTE_TO_CHAT})

# -------------------------------
# TradingView Webhook
# -------------------------------
@app.post("/bot")
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
    msg   = str(data.get("msg", "")).strip()

    if not route or not msg:
        return jsonify({"ok": False, "error": "missing route or msg"}), 400

    try:
        chat_id = route_to_chat_id(route)
    except KeyError as e:
        log.error(f"[DROP] {e}. payload={data}")
        # 200으로 응답해 TV 재시도 폭주 방지
        return jsonify({"ok": False, "error": "unknown_route"}), 200

    try:
        res = post_telegram(chat_id, msg)
        if not bool(res.get("ok")):
            log.error(f"TG send failed: {res}")
            return jsonify({"ok": False, "error": "telegram_failed", "detail": res}), 500
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("Telegram send exception")
        return jsonify({"ok": False, "error": "exception", "detail": str(e)}), 500

if __name__ == "__main__":
    # Render는 PORT 환경변수 제공. 로컬에선 5000 기본.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
