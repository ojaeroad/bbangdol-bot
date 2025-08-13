# app.py
import os
import json
import logging
import time
from typing import Dict, Any
from flask import Flask, request, jsonify
import requests

# -------------------------------
# Telegram Bot 설정 (환경변수)
# -------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env is missing")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# 환경변수에서 정수형 Chat ID를 안전하게 읽는 함수
def read_chat_id(env_name: str) -> int:
    raw = os.getenv(env_name)
    if raw is None or raw.strip() == "":
        raise RuntimeError(f"Missing env: {env_name}")
    s = raw.strip()
    # 스프레드시트 복사 시 발생하는 지수표기 형태 방지
    if "e" in s.lower():
        raise RuntimeError(f"Invalid chat id in {env_name}: exponential form detected ({s})")
    try:
        return int(s)
    except ValueError:
        raise RuntimeError(f"Invalid chat id in {env_name}: {s}")

# -------------------------------
# Pine route → Telegram Chat ID 매핑
# (Render Environment 탭의 KEY 이름과 정확히 일치)
# -------------------------------
ROUTE_TO_CHAT: Dict[str, int] = {
    # 과매도
    "OS_SCALP": read_chat_id("OS_SCALP_CHAT_ID"),
    "OS_SHORT": read_chat_id("OS_SHORT_CHAT_ID"),
    "OS_SWING": read_chat_id("OS_SWING_CHAT_ID"),
    "OS_LONG":  read_chat_id("OS_LONG_CHAT_ID"),

    # 과매수
    "OB_SWING": read_chat_id("OB_SWING_CHAT_ID"),
    "OB_LONG":  read_chat_id("OB_LONG_CHAT_ID"),

    # 주요지표 전용 (1시간 주기 보조지표용)
    "AUX_4INDEX": read_chat_id("MAIN_INDICATOR_CHAT_ID"),
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
# 헬스체크 & 매핑 확인
# -------------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "status": "healthy"})

@app.get("/routes")
def routes_dump():
    # 디버깅용: 현재 라우트 → chat id 매핑 노출(값만 표시)
    return jsonify({"routes": ROUTE_TO_CHAT})

# -------------------------------
# TradingView Webhook 엔드포인트
# -------------------------------
@app.post("/bot")
def tv_webhook():
    # JSON 파싱
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
    # symbol은 로깅/확인용 (필수 아님)
    symbol = str(data.get("symbol", "")).strip()

    if not route or not msg:
        return jsonify({"ok": False, "error": "missing route or msg"}), 400

    try:
        chat_id = route_to_chat_id(route)
    except KeyError as e:
        log.error(f"[DROP] {e}. payload={data}")
        # TradingView 재시도 폭주 방지를 위해 200 반환
        return jsonify({"ok": False, "error": "unknown_route"}), 200

    try:
        res = post_telegram(chat_id, msg)
        if not bool(res.get("ok")):
            log.error(f"TG send failed: {res} (route={route}, symbol={symbol})")
            return jsonify({"ok": False, "error": "telegram_failed", "detail": res}), 500
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("Telegram send exception")
        return jsonify({"ok": False, "error": "exception", "detail": str(e)}), 500

# -------------------------------
if __name__ == "__main__":
    # Render는 PORT 환경변수를 줌. 로컬은 5000 기본.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
