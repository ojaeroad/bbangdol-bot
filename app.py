# 25 07 24 master 전략 ver1
from flask import Flask, request
import os, json, requests, traceback

app = Flask(__name__)
TOKEN    = os.environ["TOKEN"]
CHAT_IDS = {
    "scalping": os.environ["SCALP_CHAT_ID"],
    "daytrade": os.environ["DAYTRADE_CHAT_ID"],
    "swing":    os.environ["SWING_CHAT_ID"],
    "longterm": os.environ["LONG_CHAT_ID"],
}

@app.route("/alert", methods=["POST"])
def webhook():
    raw = request.get_data(as_text=True)
    app.logger.info(f"[ALERT RECEIVED] raw payload repr: {raw!r}")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        app.logger.error(f"JSON parse error: {e}")
        return "Bad JSON", 400

    strat = data.get("type")
    msg   = data.get("message")
    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        return "Unknown strat", 400

    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
        )
        if res.status_code != 200:
            app.logger.error(f"Telegram API error: {res.status_code} {res.text}")
    except Exception:
        app.logger.error(traceback.format_exc())
        return "Error", 500

    return "OK", 200

if __name__ == "__main__":
    app.run(port=10000)
