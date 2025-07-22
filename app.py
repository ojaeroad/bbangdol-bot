# app.py
from flask import Flask, request
import os, json, requests

app = Flask(__name__)

TOKEN = os.environ["TOKEN"]
CHAT_IDS = {
    "scalping": os.environ["SCALP_CHAT_ID"],
    "daytrade": os.environ["DAYTRADE_CHAT_ID"],
    "swing":    os.environ["SWING_CHAT_ID"],
    "longterm": os.environ["LONG_CHAT_ID"],
}

@app.route("/alert", methods=["POST"])
def webhook():
    raw = request.get_data(as_text=True)
    app.logger.info(f"⏳ RAW PAYLOAD: {raw}")

    data    = json.loads(raw)
    strat   = data.get("type")
    message = data.get("message")

    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        return "Unknown strategy", 400

    res = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": message}
    )
    app.logger.info(f"Telegram API response: {res.status_code} {res.text}")
    return "OK", 200

if __name__ == "__main__":
    app.run(port=10000)
