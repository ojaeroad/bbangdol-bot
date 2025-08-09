# app.py (통합/안전 버전)
import os, json, time
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

TOKEN   = os.getenv("7845798196:AAG5NVZQRjNZw0HTFyb3bqXIsvigMFRTpBU", "")          # 필수
CHATMAP = {
    "scalp":     os.getenv("-4870905408", ""),
    "scalp_up":  os.getenv("CHAT_ID_SCALP_UP", ""),
    "short":     os.getenv("-4820497789", ""),
    "swing":     os.getenv("-4912298868", ""),
    "long":      os.getenv("-1002529014389", ""),
}
CHAT_DEFAULT = os.getenv("CHAT_ID_DEFAULT", "")

TG_API = lambda t: f"https://api.telegram.org/bot{t}/sendMessage"
MAXLEN = 4096

def choose_chat_id(strategy: str) -> str:
    s = (strategy or "").strip().lower()
    return CHATMAP.get(s) or CHAT_DEFAULT

def clamp(text: str) -> str:
    if not text:
        return ""
    if len(text) <= MAXLEN:
        return text
    return text[:MAXLEN-14] + "\n…(truncated)"

def send_message(chat_id: str, text: str):
    if not (TOKEN and chat_id and text):
        return {"ok": False, "error": "missing token/chat_id/text"}
    payload = {"chat_id": chat_id, "text": clamp(text)}
    try:
        r = requests.post(TG_API(TOKEN), json=payload, timeout=10)
        return {"ok": r.ok, "status": r.status_code, "resp": r.text[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "time": int(time.time())})

def handle_alert():
    # TradingView는 JSON로 보내야 함: {"type":"scalp","message":"..."}
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        # 폼/텍스트로 오는 예외 케이스도 방어
        try:
            data = json.loads(request.data.decode("utf-8"))
        except Exception:
            return jsonify({"ok": False, "error": "invalid JSON"}), 400

    strategy = (data.get("type") or data.get("strategy") or "").strip()
    message  = (data.get("message") or data.get("msg") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "missing message"}), 400

    chat_id = choose_chat_id(strategy)
    if not chat_id:
        return jsonify({"ok": False, "error": f"no chat id for '{strategy}'"}), 400

    res = send_message(chat_id, message)
    code = 200 if res.get("ok") else 502
    app.logger.info({"path": request.path, "strategy": strategy, "len": len(message), "result": res})
    return jsonify({"ok": res.get("ok", False), "detail": res}), code

# TradingView가 어디로 보내든 받게 두 개 다 오픈
@app.route("/alert", methods=["POST"])
def alert():
    return handle_alert()

@app.route("/webhook", methods=["POST"])
def webhook():
    return handle_alert()

if __name__ == "__main__":
    # 개발 실행: 운영은 gunicorn 권장: gunicorn -w 2 -b 0.0.0.0:8000 app:app
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
