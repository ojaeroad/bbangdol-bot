# app.py
import os, json, time
from typing import Dict, Any
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# === 환경변수 ===
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
# 기본 채널 (필수)
CHAT_ID_DEFAULT = os.getenv("TELEGRAM_CHAT_ID_DEFAULT", "")
# 전략별 채널 (옵션) - 없으면 기본 채널로 전송
CHAT_ID_SCALP = os.getenv("TELEGRAM_CHAT_ID_SCALP", "")   # 예: 스캘프 전용 채팅방
CHAT_ID_SHORT = os.getenv("TELEGRAM_CHAT_ID_SHORT", "")
CHAT_ID_SWING = os.getenv("TELEGRAM_CHAT_ID_SWING", "")
CHAT_ID_LONG  = os.getenv("TELEGRAM_CHAT_ID_LONG", "")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
TELEGRAM_MAX = 4096  # 텔레그램 단일 메시지 길이 제한(문자수 기준)

def choose_chat_id(alert_type: str) -> str:
    t = (alert_type or "").lower()
    if t == "scalp" and CHAT_ID_SCALP: return CHAT_ID_SCALP
    if t == "short" and CHAT_ID_SHORT: return CHAT_ID_SHORT
    if t == "swing" and CHAT_ID_SWING: return CHAT_ID_SWING
    if t == "long"  and CHAT_ID_LONG:  return CHAT_ID_LONG
    return CHAT_ID_DEFAULT

def clamp_text(s: str) -> str:
    """4096자 초과 시 말줄임. (안전 가드)"""
    if s is None:
        return ""
    if len(s) <= TELEGRAM_MAX:
        return s
    # 남김 메시지 표기
    suffix = "\n…(truncated)"
    return s[:TELEGRAM_MAX - len(suffix)] + suffix

def send_telegram(text: str, chat_id: str, parse_mode: str = None, disable_preview: bool = True) -> Dict[str, Any]:
    payload = {
        "chat_id": chat_id,
        "text": clamp_text(text),
        "disable_web_page_preview": disable_preview
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    # 간단 재시도(네트워크 흔들릴 때 대비)
    for attempt in range(3):
        try:
            r = requests.post(TELEGRAM_API, json=payload, timeout=10)
            if r.ok:
                return {"ok": True, "try": attempt + 1}
            # 429 등 속도 제한 대응: 텔레그램이 주는 retry_after 초 대기
            if r.status_code == 429:
                wait = r.json().get("parameters", {}).get("retry_after", 1)
                time.sleep(wait)
                continue
        except Exception as e:
            if attempt < 2:
                time.sleep(1.2)
                continue
            return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "Telegram API error"}

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    if not BOT_TOKEN or not CHAT_ID_DEFAULT:
        return jsonify({"ok": False, "error": "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID_DEFAULT"}), 500

    # TradingView alert body: {"type":"scalp","message":"..."} 형태를 기대
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    # 유연한 키 수용
    alert_type = (data.get("type") or data.get("strategy") or "").strip()
    message    = (data.get("message") or data.get("msg") or "").strip()

    # 필수 검사
    if not message:
        return jsonify({"ok": False, "error": "Missing 'message'"}), 400

    # 채널 선택
    chat_id = choose_chat_id(alert_type)

    # 색상/스타일 X → 그냥 텍스트만 (요구사항: 흰색 텍스트)
    text = message

    res = send_telegram(text=text, chat_id=chat_id)
    code = 200 if res.get("ok") else 502
    return jsonify({"ok": res.get("ok", False), "detail": res}), code

if __name__ == "__main__":
    # Flask 개발 실행 (운영은 gunicorn 권장: gunicorn -w 2 -b 0.0.0.0:8000 app:app)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
