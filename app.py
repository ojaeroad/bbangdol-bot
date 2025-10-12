# app.py — unified webhook (old routes kept + new accumulation routes)
import os, json, logging, time, re, hmac, hashlib
from time import time as now
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlencode
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# === Anti-spam settings (60s fixed) ===
COOLDOWN_SEC      = 60
DEDUP_WINDOW_SEC  = 60

_LAST_SENT_AT: Dict[str, float]                 = {}
_RECENT_MSG_HASH: Dict[Tuple[str, int], float]  = {}

_TF_RE = re.compile(r'\b(1w|1d|12h|6h|4h|2h|1h|30m|15m|5m|3m)\b', re.IGNORECASE)

def _extract_signature(msg: str) -> str:
    if not msg:
        return "unknown"
    lines = msg.splitlines()
    for line in lines[1:]:
        m = _TF_RE.search(line)
        if m:
            return m.group(1).lower()
    return "unknown"

def _bucket_key(chat_id: int | str, symbol: str, route: str, msg: str) -> str:
    sig = _extract_signature(msg)
    return f"{chat_id}:{symbol}:{route}:{sig}"

def _can_send_now(bucket: str) -> bool:
    last = _LAST_SENT_AT.get(bucket)
    return (last is None) or (now() - last >= COOLDOWN_SEC)

def _mark_sent(bucket: str):
    _LAST_SENT_AT[bucket] = now()

def _is_duplicate(bucket: str, msg_norm: str) -> bool:
    k = (bucket, hash(msg_norm))
    t = _RECENT_MSG_HASH.get(k)
    if t is not None and (now() - t) < DEDUP_WINDOW_SEC:
        return True
    _RECENT_MSG_HASH[k] = now()
    return False

# --- Telegram base (기존 트뷰용) ---
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN/TELEGRAM_BOT_TOKEN env is missing")
TG_SEND = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
MAX_LEN = 3900

def safe_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= MAX_LEN else s[:MAX_LEN - 20] + "\n...[truncated]"

def post_telegram(chat_id: int | str, text: str, parse_mode: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": safe_text(text)}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(TG_SEND, json=payload, timeout=10)
    return r.json()

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

    # 기존 방들
    add_if("OS_SCALP", "OS_SCALP_CHAT_ID")
    add_if("OS_SHORT", "OS_SHORT_CHAT_ID")
    add_if("OS_LONG",  "OS_LONG_CHAT_ID")
    add_if("OB_LONG",  "OB_LONG_CHAT_ID")
    add_if("OB_SHORT", "OB_SHORT_CHAT_ID")
    add_if("AUX_4INDEX", "MAIN_INDICATOR_CHAT_ID")

    add_if("OS_SWINGA", "OS_SWINGA_CHAT_ID")
    add_if("OB_SWINGA", "OB_SWINGA_CHAT_ID")
    add_if("OS_SWINGB", "OS_SWINGB_CHAT_ID")
    add_if("OB_SWINGB", "OB_SWINGB_CHAT_ID")

    add_if("OS_SCALP_KRW", "KRW_SCALP")
    add_if("OS_SHORT_KRW", "KRW_SHORT")
    add_if("OS_SWING_KRW", "KRW_SWING")
    add_if("OS_LONG_KRW",  "KRW_LONG")

    add_if("LONG_5M",  "LONG_5M")
    add_if("SHORT_5M", "SHORT_5M")
    add_if("LONG_30M", "LONG_30M")
    add_if("SHORT_30M","SHORT_30M")
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
    bucket = _bucket_key(chat_id, symbol, route, msg)
    msg_norm = safe_text(msg)
    if not _can_send_now(bucket):
        log.info(f"[SKIP cooldown] bucket={bucket}")
        return jsonify({"ok": True, "skipped": "cooldown", "bucket": bucket})
    if _is_duplicate(bucket, msg_norm):
        log.info(f"[SKIP dedup] bucket={bucket}")
        return jsonify({"ok": True, "skipped": "dedup", "bucket": bucket})
    try:
        res = post_telegram(chat_id, msg_norm)
        if not bool(res.get("ok")):
            log.error(f"TG send failed: {res} (route={route}, symbol={symbol})")
            return jsonify({"ok": False, "error": "telegram_failed", "detail": res}), 500
        _mark_sent(bucket)
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("Telegram send exception")
        return jsonify({"ok": False, "error": "exception", "detail": str(e)}), 500

# --- old endpoint (legacy) ---
@app.post("/bot")
def tv_webhook_legacy():
    data = request.get_json(silent=True, force=True) or {}
    route  = str(data.get("route", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# --- new accumulation endpoint ---
@app.post("/webhook")
def tv_webhook_new():
    data = request.get_json(silent=True, force=True) or {}
    route  = str(data.get("type", data.get("route", ""))).strip()
    msg    = str(data.get("message", data.get("msg", ""))).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# =========================================================
# === 신규: BNC_POSITION 전용 (bbangdol_bnc_bot) ===
# =========================================================
def post_telegram_with_token(bot_token: str, chat_id: str, text: str) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": safe_text(text)}
    return requests.post(url, json=payload, timeout=10).json()

@app.post("/bnc/dryrun")
def bnc_dryrun():
    secret = os.getenv("BNC_SECRET")
    data = request.get_json(silent=True) or {}
    if secret and data.get("secret") != secret:
        return jsonify({"ok": False, "error": "bad secret"}), 401
    return jsonify({
        "ok": True,
        "chat_id": os.getenv("BNC_CHAT_ID"),
        "bot": "bbangdol_bnc_bot"
    })

@app.post("/bnc")
def bnc_send():
    data = request.get_json(silent=True, force=True) or {}
    secret = os.getenv("BNC_SECRET")
    if secret and data.get("secret") != secret:
        return jsonify({"ok": False, "error": "bad secret"}), 401

    bnc_token = os.getenv("BNC_BOT_TOKEN")
    bnc_chat  = os.getenv("BNC_CHAT_ID")
    if not bnc_token or not bnc_chat:
        return jsonify({"ok": False, "error": "BNC env missing"}), 500

    tag    = str(data.get("tag", "BNC_POSITION")).strip()
    symbol = str(data.get("symbol", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    if not msg:
        return jsonify({"ok": False, "error": "msg missing"}), 400

    header = f"[{tag}] {symbol}" if symbol else f"[{tag}]"
    text   = f"{header}\n{msg}"

    bucket = _bucket_key(bnc_chat, symbol, tag, text)
    msg_norm = safe_text(text)
    if not _can_send_now(bucket):
        return jsonify({"ok": True, "skipped": "cooldown", "bucket": bucket})
    if _is_duplicate(bucket, msg_norm):
        return jsonify({"ok": True, "skipped": "dedup", "bucket": bucket})

    try:
        res = post_telegram_with_token(bnc_token, bnc_chat, msg_norm)
        _mark_sent(bucket)
        return jsonify({"ok": bool(res.get("ok")), "detail": res})
    except Exception as e:
        log.exception("BNC Telegram send exception")
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================================================
# === 신규: Binance USDⓈ-M Futures order endpoint (/bnc/trade) ===
# =========================================================
def _now_ms() -> int:
    return int(time.time() * 1000)

def _sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()

def _binance_base() -> str:
    # 우선순위: BINANCE_FUTURES_BASE > BINANCE_IS_TESTNET 플래그
    base = _read_optional("BINANCE_FUTURES_BASE")
    if base:
        return base
    return "https://testnet.binancefuture.com" if os.getenv("BINANCE_IS_TESTNET", "1") == "1" else "https://fapi.binance.com"

def _binance_post(path: str, params: dict) -> dict:
    base = _binance_base()
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_SECRET_KEY")
    if not api_key or not api_secret:
        raise RuntimeError("BINANCE_API_KEY/SECRET_KEY missing")

    params["timestamp"] = _now_ms()
    params["recvWindow"] = 5000
    q = urlencode(params, doseq=True, safe=":/")
    sig = _sign(q, api_secret)
    url = f"{base}{path}?{q}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}
    r = requests.post(url, headers=headers, timeout=10)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code != 200:
        raise RuntimeError(f"Binance HTTP {r.status_code} {data}")
    return data

def place_market_order(symbol: str, side: str, qty: float, reduce_only: bool = False, position_side: Optional[str] = None, client_id: Optional[str] = None) -> dict:
    params = {
        "symbol": symbol,
        "side": side,                 # BUY / SELL
        "type": "MARKET",
        "quantity": qty,              # 코인 수량
        "reduceOnly": "true" if reduce_only else "false",
    }
    if position_side:
        params["positionSide"] = position_side  # LONG / SHORT (양방향 모드)
    if client_id:
        params["newClientOrderId"] = client_id[:36]
    return _binance_post("/fapi/v1/order", params)

SYM_WHITELIST = set((_read_optional("BNC_SYMBOLS") or "BTCUSDT,ETHUSDT,SOLUSDT").split(","))

@app.post("/bnc/trade")
def bnc_trade():
    """
    Body:
    {
      "secret": "<BNC_SECRET>",
      "symbol": "BTCUSDT",
      "action": "OPEN_LONG" | "CLOSE_LONG" | "OPEN_SHORT" | "CLOSE_SHORT",
      "qty": 0.001,
      "note": "optional"
    }
    """
    data = request.get_json(silent=True, force=True) or {}
    secret = os.getenv("BNC_SECRET")
    if secret and data.get("secret") != secret:
        return jsonify({"ok": False, "error": "bad secret"}), 401

    symbol = str(data.get("symbol", "")).upper()
    action = str(data.get("action", "")).upper()
    qty    = float(data.get("qty", 0) or 0)
    note   = str(data.get("note", ""))

    if symbol not in SYM_WHITELIST:
        return jsonify({"ok": False, "error": f"symbol not allowed: {symbol}"}), 400
    if qty <= 0:
        return jsonify({"ok": False, "error": "qty must be > 0"}), 400
    if action not in {"OPEN_LONG","CLOSE_LONG","OPEN_SHORT","CLOSE_SHORT"}:
        return jsonify({"ok": False, "error": "invalid action"}), 400

    try:
        cid = f"bnc_{symbol}_{action}_{int(qty*1e8)}_{int(now())}"
        if action == "OPEN_LONG":
            res = place_market_order(symbol, "BUY", qty, reduce_only=False, position_side="LONG", client_id=cid)
        elif action == "CLOSE_LONG":
            res = place_market_order(symbol, "SELL", qty, reduce_only=True, position_side="LONG", client_id=cid)
        elif action == "OPEN_SHORT":
            res = place_market_order(symbol, "SELL", qty, reduce_only=False, position_side="SHORT", client_id=cid)
        else:  # CLOSE_SHORT
            res = place_market_order(symbol, "BUY", qty, reduce_only=True, position_side="SHORT", client_id=cid)

        # 텔레그램 확인 메시지(신규 방)
        try:
            bnc_token = os.getenv("BNC_BOT_TOKEN")
            bnc_chat  = os.getenv("BNC_CHAT_ID")
            confirm   = f"[TRADE] {symbol} {action} qty={qty}\norderId={res.get('orderId')}  status={res.get('status')}\n{note}"
            if bnc_token and bnc_chat:
                post_telegram_with_token(bnc_token, bnc_chat, confirm)
        except Exception:
            pass

        return jsonify({"ok": True, "result": res})
    except Exception as e:
        log.exception("bnc_trade error")
        return jsonify({"ok": False, "error": str(e)}), 500
# =========================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
