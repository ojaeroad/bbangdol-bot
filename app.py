# 🔧 작성일시: 2025-07-25 23:58 (KST)
# ✅ 기능: TradingView Webhook → Flask 서버 → Telegram 방 자동 전송

from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

# Telegram Bot Token
TELEGRAM_TOKEN = '7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU'

# 각 전략별 Chat ID
CHAT_IDS = {
    "scalp": "-4870905408",
    "scalp_up": "-4872204876",
    "daytrade": "-4820497789",
    "swing": "-4912298868",
    "longterm": "-1002529014389"
}

# 메시지 전송 함수
def send_telegram_message(chat_id, message):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    response = requests.post(url, json=data)
    return response.status_code

# 🔔 Webhook 엔드포인트 (TradingView가 여기에 POST)
@app.route('/alert', methods=['POST'])
def alert():
    try:
        data = request.get_json()
        message = data.get("message", "No message")
        strategy = data.get("type", "daytrade")  # 기본값: daytrade

        # 전략별 chat_id 결정
        chat_id = CHAT_IDS.get(strategy)
        if not chat_id:
            return jsonify({"error": "Unknown strategy"}), 400

        status = send_telegram_message(chat_id, message)
        return jsonify({"status": status}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 서버 실행
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
