from flask import Flask, request
import os, requests, json

app = Flask(__name__)

# Render 환경변수 설정값
TOKEN = os.environ["TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]   # 모든 알람을 한 방에 받을 때 쓰시던 Chat ID

@app.route("/alert", methods=["POST"])
def webhook():
    data = json.loads(request.get_data(as_text=True))
    strat = data.get("type")
    text  = data.get("message")

    # single-room: CHAT_ID 하나만 사용
    res = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text}
    )
    app.logger.info(f"Telegram response: {res.status_code} {res.text}")
    return "OK", 200
