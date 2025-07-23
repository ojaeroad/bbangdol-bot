# app.py

from flask import Flask, request
import os, json, requests, traceback

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
    try:
        data = json.loads(raw)
    except Exception as e:
        # JSON parsing 에러 로깅
        app.logger.error(f"❌ JSON parse error: {e}")
        return "Bad JSON", 400

    try:
        strat   = data["type"]
        message = data["message"]
        chat_id = CHAT_IDS.get(strat)
        if not chat_id:
            app.logger.error(f"❌ Unknown strategy: {strat}")
            return "Unknown strategy", 400

        res = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": message}
        )
        app.logger.info(f"✅ Telegram response: {res.status_code} {res.text}")
    except Exception as e:
        # Telegram 전송 중 예외 로깅
        app.logger.error("❌ Exception sending to Telegram:\n" + traceback.format_exc())
        return "Internal error", 500

    return "OK", 200
