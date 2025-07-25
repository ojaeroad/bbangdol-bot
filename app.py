# 🔧 작성일시: 2025-07-26 08:00 (KST)
# ✅ 기능: TradingView Webhook → Telegram 메시지 전송

from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# 👤 bbangdol_bot 봇 토큰 (외부 노출 금지)
TELEGRAM_TOKEN = '7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU'

# 전략별 Chat ID 매핑
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
        print("🔍 RAW REQUEST DATA:", request.data)

        data = request.get_json(force=True)
        message = data.get("message", "[⚠️] No message received")
        strategy = data.get("type", "daytrade")

        chat_id = CHAT_IDS.get(strategy)
        if not chat_id:
            return jsonify({"error": "Invalid strategy type"}), 400

        response = send_telegram_message(chat_id, message)
        return jsonify({"telegram_status": response.status_code}), 200

    except Exception as e:
        print("❌ Exception:", str(e))
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
