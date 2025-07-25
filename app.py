from flask import Flask, request
import requests

app = Flask(__name__)

# 여기에 본인의 텔레그램 봇 토큰과 Chat ID를 입력하세요
TOKEN   = "YOUR_TELEGRAM_BOT_TOKEN"
CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

@app.route("/", methods=["POST"])
def webhook():
    # TradingView에서 보낸 메시지를 그대로 Telegram으로 포워딩
    msg = request.get_data(as_text=True)
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": msg}
    )
    return "OK"
