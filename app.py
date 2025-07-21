from flask import Flask, request
import requests
import os
import json

app = Flask(__name__)

TOKEN = os.environ["TOKEN"]

# 전략별 방 ID를 환경변수로 받아오거나 직접 입력
CHAT_IDS = {
  "scalping":   os.environ["-4870905408"],
  "daytrade":   os.environ["-4820497789"],
  "swing":      os.environ["-4912298868"],
  "longterm":   os.environ["-1002529014389"],
}

@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])
def webhook():
    data = json.loads(request.get_data(as_text=True))
    strat = data.get("type")
    text  = data.get("message")

    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        app.logger.error(f"Unknown strategy: {strat}")
        return "Unknown strategy", 400

    res = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text}
    )
    app.logger.info(f"Tg response: {res.status_code} {res.text}")
    return "OK", 200
