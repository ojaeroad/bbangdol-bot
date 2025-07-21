from flask import Flask, request
import requests
import os
import json

app = Flask(__name__)

# ----------------------------------------
# 1) 시작 시 환경 변수 로드 상태 로깅
#    (TOKEN은 마지막 4자리만 노출)
app.logger.info(f"TOKEN           = {os.environ.get('TOKEN', '')[-4:]}")
app.logger.info(f"SCALP_CHAT_ID   = {os.environ.get('SCALP_CHAT_ID', '')}")
app.logger.info(f"DAYTRADE_CHAT_ID= {os.environ.get('DAYTRADE_CHAT_ID', '')}")
app.logger.info(f"SWING_CHAT_ID   = {os.environ.get('SWING_CHAT_ID', '')}")
app.logger.info(f"LONG_CHAT_ID    = {os.environ.get('LONG_CHAT_ID', '')}")
# ----------------------------------------

# 2) 실제로 사용할 환경 변수
TOKEN = os.environ["TOKEN"]
CHAT_IDS = {
    "scalping":   os.environ["SCALP_CHAT_ID"],
    "daytrade":   os.environ["DAYTRADE_CHAT_ID"],
    "swing":      os.environ["SWING_CHAT_ID"],
    "longterm":   os.environ["LONG_CHAT_ID"],
}

# 3) Webhook 엔드포인트 ("/" 와 "/alert" 둘 다 지원)
@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])
def webhook():
    # 3-1) 원본 페이로드 로깅
    raw = request.get_data(as_text=True)
    app.logger.info(f"▶ Received raw payload: {raw}")

    # 3-2) JSON 파싱
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        app.logger.error(f"Invalid JSON received: {e}")
        return "Bad Request", 400

    strat = data.get("type")       # "scalping", "daytrade", "swing", "longterm"
    text  = data.get("message", "")  # Pine Script에서 보낸 메시지

    # 3-3) 전략별 Chat ID 분기
    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        app.logger.error(f"Unknown strategy: {strat}")
        return "Unknown strategy", 400

    # 3-4) Telegram API 호출 & 응답 로깅
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10
        )
        app.logger.info(f"▶ Telegram API response: {res.status_code} {res.text}")
    except Exception:
        app.logger.exception("‼ Exception during Telegram API call:")
        return "Internal Server Error", 500

    return "OK", 200

# 로컬 디버깅용
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
