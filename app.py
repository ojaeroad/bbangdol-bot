from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

# 텔레그램 봇 토큰과 방(chat_id) 매핑
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_IDS = {
    "scalping": os.environ.get('SCALPING_CHAT_ID'),
    "daytrade": os.environ.get('DAYTRADE_CHAT_ID'),
    "swing": os.environ.get('SWING_CHAT_ID'),
    "long": os.environ.get('LONG_CHAT_ID')
}

# 텔레그램 전송 함수
def send_telegram(chat_id, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    res = requests.post(url, json=payload)
    return res.status_code, res.text

@app.route('/alert', methods=['POST'])
def alert():
    data = request.json
    alert_type = data.get('type', 'scalping')  # 기본값 scalping
    message = data.get('message', '🔥 빵돌이 알람')

    chat_id = CHAT_IDS.get(alert_type)
    if chat_id:
        status, resp = send_telegram(chat_id, message)
        return jsonify({"status": status, "response": resp})
    else:
        return jsonify({"error": "Invalid type or missing chat_id"}), 400

@app.route('/')
def index():
    return "Bangdori Flask Webhook Server is Running! 🚀"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)