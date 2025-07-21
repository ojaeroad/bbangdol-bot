from flask import Flask, request
import requests

app = Flask(__name__)

TOKEN   = "YOUR_TELEGRAM_BOT_TOKEN"
CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"

@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])   # ← 이 줄 추가
def webhook():
    msg = request.get_data(as_text=True)
    requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": msg}
    )
    return "OK"
