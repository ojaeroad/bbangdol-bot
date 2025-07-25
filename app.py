from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")  # 환경변수 설정 권장

CHAT_IDS = {
    "scalp": "-4870905408",
    "scalp_up": "-4872204876",
    "daytrade": "-4820497789",
    "swing": "-4912298868",
    "longterm": "-1002529014389"
}

def send_telegram_message(chat_id, message):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    return requests.post(url, json=data)

@app.route('/alert', methods=['POST'])
def alert():
    try:
        data = request.get_json(force=True)

        if not data:
            return jsonify({"error": "No JSON received"}), 400

        message = data.get("message", "[⚠️] No message in payload.")
        strategy = data.get("type", "daytrade")  # 기본값

        chat_id = CHAT_IDS.get(strategy)
        if not chat_id:
            return jsonify({"error": "Invalid strategy type"}), 400

        response = send_telegram_message(chat_id, message)
        return jsonify({"telegram_status": response.status_code}), 200

    except Exception as e:
        print("🔴 Exception occurred:", e)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
