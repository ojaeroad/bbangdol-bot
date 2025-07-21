from flask import Flask, request
import requests
import os
import json

app = Flask(__name__)

# 1) 환경 변수 출력 (디버깅용)
print("··· ENV ···")
print("TOKEN last4:", os.environ.get("TOKEN", "")[-4:])
print("SCALP_CHAT_ID :", os.environ.get("SCALP_CHAT_ID"))
print("DAYTRADE_CHAT_ID:", os.environ.get("DAYTRADE_CHAT_ID"))
print("SWING_CHAT_ID :", os.environ.get("SWING_CHAT_ID"))
print("LONG_CHAT_ID  :", os.environ.get("LONG_CHAT_ID"))
print("··· /ENV ···")

# 2) 실서비스용 변수
TOKEN    = os.environ["TOKEN"]
CHAT_IDS = {
    "scalping":   os.environ["SCALP_CHAT_ID"],
    "daytrade":   os.environ["DAYTRADE_CHAT_ID"],
    "swing":      os.environ["SWING_CHAT_ID"],
    "longterm":   os.environ["LONG_CHAT_ID"],
}

@app.route("/", methods=["POST"])
@app.route("/alert", methods=["POST"])
def webhook():
    # 3) 들어온 원본 페이로드
    raw = request.get_data(as_text=True)
    print("▶ Received raw payload:", raw)

    # 4) JSON으로 파싱
    try:
        data = json.loads(raw)
    except Exception as e:
        print("‼ JSON decode error:", e)
        return "Bad Request", 400

    strat = data.get("type")
    text  = data.get("message","")

    # 5) 전략별 chat_id 분기
    chat_id = CHAT_IDS.get(strat)
    if not chat_id:
        print("‼ Unknown strategy:", strat)
        return "Unknown strategy", 400

    # 6) Telegram API 호출
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10
        )
        print("▶ Telegram API response:", res.status_code, res.text)
    except Exception as e:
        print("‼ Exception sending to Telegram:", e)
        return "Internal Error", 500

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
