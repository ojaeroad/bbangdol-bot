from flask import Flask, request
import requests
import json

app = Flask(__name__)

TOKEN = '7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU'

def send_message(chat_id, text):
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    response = requests.post(url, json=payload)
    return response

@app.route('/alert', methods=['POST'])
def alert():
    try:
        data = json.loads(request.data.decode('utf-8'))
        message = data.get('message', '')
        strategy = data.get('type', '')

        # ✅ 전략별 Chat ID 분기 (사용자 정의 이름 적용)
        if strategy == 'scalp':
            chat_id = '-4870905408'
        elif strategy == 'scalp_up':
            chat_id = '-4872204876'
        elif strategy == 'short':
            chat_id = '-4820497789'
        elif strategy == 'swing':
            chat_id = '-4912298868'
        elif strategy == 'long':
            chat_id = '-1002529014389'
        else:
            return 'Invalid strategy type', 400

        res = send_message(chat_id, message)
        return f'Sent with status code: {res.status_code}', 200
    except Exception as e:
        return f'Error: {str(e)}', 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
