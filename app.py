from flask import Flask, request
import requests
import os
import json

app = Flask(__name__)

# Render Environment → TOKEN 에 설정한 값
TOKEN = os.environ["TOKEN"]

# Render Environment → 아래 네 키에 각 Chat ID 입력
CHAT_IDS = {
    "scalping":   os.environ["SCALP_CHAT_ID"],
    "daytrade":   os.environ["DAYTRADE_CHAT_ID"],
    "swing":      os.environ["SWING_CHAT_ID"],
    "longterm":   os.environ["LONG_CHAT_ID"],
}

@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])
def webhook():
    data = json.loads(request.get_data(as_text=True))
    strat = data.get("type")       # "scalping"/"daytrade"/"swing"/"longterm"
    text  = data.get("message")    # Pine Script에서 보낸 full 메시지

    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        return "Unknown strategy", 400

    res = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text}
    )
    app.logger.info(f"Telegram response: {res.status_code} {res.text}")
    return "OK", 200
