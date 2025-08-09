# app.py — hardcoded token/chat IDs + /alert & /webhook 지원, 길이 가드/로깅
import json, time
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# === Telegram 설정 (기존 정상값) ===
TOKEN = "7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU"
CHATMAP = {
    "scalp":    "-4870905408",
    "scalp_up": "-4872204876",
    "short":    "-4820497789",
    "swing":    "-4912298868",
    "long":     "-1002529014389",
}
TELEGRAM_API = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
TELEGRAM_MAX = 4096

def clamp_text(s: str) -> str:
    if not s: return ""
    return s if len(s) <= TELEGRAM_MAX else s[:TELEGRAM_MAX-14] + "\n…(truncated)"

def choose_chat_id(strategy: str) -> str:
    return CHATMAP.get((strategy or "").strip().lower(), "")

def send_telegram(text: str, chat_id: str):
    payload = {"chat_id": chat_id, "text": clamp_text(text), "disable_web_page_preview": True}
    r = requests.post(TELEGRAM_API, json=payload, timeout=10)
    return r

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "time": int(time.time())})

def handle_alert():
    # TradingView: {"type":"scalp","message":"..."}
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        try:
            data = json.loads(request.data.decode("utf-8"))
        except Exception:
            app.logger.warning({"reason": "invalid_json", "raw": request.data[:120]})
            return jsonify({"ok": False, "error": "invalid JSON"}), 400

    strategy = (data.get("type") or data.get("strategy") or "").strip()
    message  = (data.get("message") or data.get("msg") or "").strip()
    if not message:
        app.logger.warning({"reason":"missing_message","keys":list(data.keys())})
        return jsonify({"ok": False, "error": "missing message"}), 400

    chat_id = choose_chat_id(strategy)
    if not chat_id:
        app.logger.warning({"reason":"invalid_strategy","strategy":strategy})
        return jsonify({"ok": False, "error": f"invalid strategy '{strategy}'"}), 400

    try:
        r = send_telegram(message, chat_id)
        app.logger.info({"path": request.path, "strategy": strategy, "len": len(message), "tg_status": r.status_code})
        return jsonify({"ok": r.ok, "status": r.status_code}), (200 if r.ok else 502)
    except Exception as e:
        app.logger.error({"reason":"telegram_send_fail","err":str(e)})
        return jsonify({"ok": False, "error": str(e)}), 502

@app.route("/alert", methods=["POST"])
def alert():
    return handle_alert()

@app.route("/webhook", methods=["POST"])
def webhook():
    return handle_alert()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
