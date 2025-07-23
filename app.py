from flask import Flask, request
import requests, os, json

app = Flask(__name__)

TOKEN = os.environ["TOKEN"]
CHAT_IDS = {
    "scalping": os.environ["SCALP_CHAT_ID"],
    "daytrade": os.environ["DAYTRADE_CHAT_ID"],
    "swing":    os.environ["SWING_CHAT_ID"],
    "longterm": os.environ["LONG_CHAT_ID"],
}

@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])
def webhook():
    data = json.loads(request.get_data(as_text=True))
    strat = data["type"]
    text  = data["message"]

    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        return "Unknown strategy", 400

    res = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10
    )
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
