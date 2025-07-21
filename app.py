from flask import Flask, request
import requests
import json
import os

app = Flask(__name__)

TOKEN = os.environ.get("TOKEN", "여기에_토큰_입력")

# 환경 변수 또는 하드코딩으로 Chat ID 등록
CHAT_IDS = {
    "scalping":   "-4870905408",   # 스캘핑 전용 방
    "daytrade":   "-4820497789",   # 단타 전용 방
    "swing":      "-4912298868",   # 스윙 전용 방
    "longterm":   "-1002529014389"   # 장기 전용 방
}

@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])
def webhook():
    # TradingView에서 JSON 포맷으로 보냈다고 가정
    data = json.loads(request.get_data(as_text=True))
    strategy = data.get("type")         # "scalping", "daytrade", "swing", "longterm"
    text     = data.get("message")      # 실제 알림 메시지

    chat_id = CHAT_IDS.get(strategy)
    if not chat_id:
        return "Unknown strategy", 400

    res = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text}
    )
    app.logger.info(f"Telegram response: {res.status_code}, {res.text}")
    return "OK", 200
