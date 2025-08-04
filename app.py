# 🔧 작성일시: 2025-07-26 08:25 (KST)
# ✅ 변경사항: UTF-8 한글 포함 JSON 수동 디코딩 처리

from flask import Flask, request, jsonify
import requests
import json

app = Flask(__name__)

TELEGRAM_TOKEN = '7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU'

CHAT_IDS = {
    "scalp": "-4870905408",
    "scalp_up": "-4872204876",
    "daytrade": "-4820497789",
    "swing": "-4912298868",
    "longterm": "-1002529014389"
}

def send_telegram_message(chat_id, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    return requests.post(url, json=data)

@app.route("/alert", methods=["POST"])
def alert():
    try:
        # ✅ 1. 원시 바이트 수신 후 UTF-8로 디코딩
        raw_data = request.get_data()
        decoded = raw_data.decode('utf-8')
        print("📥 Decoded Payload:", decoded)

        # ✅ 2. JSON 파싱
        data = json.loads(decoded)

        message = data.get("message", "[⚠️] No message received")
        strategy = data.get("type", "daytrade")
        chat_id = CHAT_IDS.get(strategy)

        if not chat_id:
            return jsonify({"error": f"Unknown strategy: {strategy}"}), 400

        res = send_telegram_message(chat_id, message)
        return jsonify({"status": res.status_code}), 200

    except Exception as e:
        print("❌ Exception:", str(e))
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
