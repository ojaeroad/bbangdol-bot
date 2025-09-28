# app.py  — unified webhook (old routes kept + new accumulation routes)
import os, json, logging, time
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# --- Telegram ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env is missing")
TG_SEND = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
MAX_LEN = 3900

def safe_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= MAX_LEN else s[:MAX_LEN - 20] + "\n...[truncated]"

def post_telegram(chat_id: int | str, text: str, parse_mode: Optional[str] = None) -> Dict[str, Any]:
    payload = {"chat_id": chat_id, "text": safe_text(text)}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    last_err = None
    for _ in range(3):
        try:
            r = requests.post(TG_SEND, json=payload, timeout=10)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code} {r.text}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.5)
    raise RuntimeError(f"sendMessage failed: {last_err}")

# --- env helpers ---
def _read_optional(env_name: str) -> Optional[str]:
    v = os.getenv(env_name)
    return v.strip() if v and v.strip() != "" else None

def build_route_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    def add_if(k: str, envk: str):
        val = _read_optional(envk)
        if val is not None:
            m[k] = val

    # 기존 유지
    add_if("OS_SCALP", "OS_SCALP_CHAT_ID")
    add_if("OS_SHORT", "OS_SHORT_CHAT_ID")
    add_if("OS_LONG",  "OS_LONG_CHAT_ID")
    add_if("OB_LONG",  "OB_LONG_CHAT_ID")
    add_if("OB_SHORT",  "OB_SHORT_CHAT_ID")
    add_if("AUX_4INDEX", "MAIN_INDICATOR_CHAT_ID")

    # 스윙 분리
    add_if("OS_SWINGA", "OS_SWINGA_CHAT_ID")
    add_if("OB_SWINGA", "OB_SWINGA_CHAT_ID")
    add_if("OS_SWINGB", "OS_SWINGB_CHAT_ID")
    add_if("OB_SWINGB", "OB_SWINGB_CHAT_ID")

    # KRW 전용
    add_if("OS_SCALP_KRW", "KRW_SCALP")
    add_if("OS_SHORT_KRW", "KRW_SHORT")
    add_if("OS_SWING_KRW", "KRW_SWING")
    add_if("OS_LONG_KRW",  "KRW_LONG")

    # 매집 전략 -> 신규 키로 변경
    add_if("SHORT",    "child_short")
    add_if("SWINGA",   "child_swinga")
    add_if("SWINGB",   "child_swingb")
    add_if("LONG",     "child_long")

    return m


ROUTE_TO_CHAT: Dict[str, str] = build_route_map()

def route_to_chat_id(route: str) -> Optional[str]:
    return ROUTE_TO_CHAT.get(route)

# --- health & routes ---
@app.get("/health")
def health():
    return jsonify({"ok": True, "status": "healthy", "routes": list(ROUTE_TO_CHAT.keys())})

@app.get("/routes")
def routes_dump():
    return jsonify({"routes": ROUTE_TO_CHAT})

# --- core handler ---
def _handle_payload(route: str, msg: str, symbol: str = ""):
    if not route or not msg:
        return jsonify({"ok": False, "error": "missing route or msg"}), 400
    chat_id = route_to_chat_id(route)
    if not chat_id:
        log.error(f"[DROP] Unknown route={route} (symbol={symbol})")
        return jsonify({"ok": False, "error": "unknown_route"}), 200
    try:
        res = post_telegram(chat_id, msg)
        if not bool(res.get("ok")):
            log.error(f"TG send failed: {res} (route={route}, symbol={symbol})")
            return jsonify({"ok": False, "error": "telegram_failed", "detail": res}), 500
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("Telegram send exception")
        return jsonify({"ok": False, "error": "exception", "detail": str(e)}), 500

# --- old endpoint (kept for backward compatibility) ---
@app.post("/bot")
def tv_webhook_legacy():
    # expects: {"route": "...", "msg": "...", "symbol": "..."}
    data = request.get_json(silent=True, force=True) or {}
    route  = str(data.get("route", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# --- new endpoint for the accumulation script ---
@app.post("/webhook")
def tv_webhook_new():
    # supports both: {"type":"SCALP","message":"..."}  and  {"route":"SCALP","msg":"..."}
    data = request.get_json(silent=True, force=True) or {}
    route  = str(data.get("type", data.get("route", ""))).strip()
    msg    = str(data.get("message", data.get("msg", ""))).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
