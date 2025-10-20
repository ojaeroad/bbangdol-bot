# app.py — unified webhook + BNC trade + TG UI (global mode & split-entry)
import os, json, logging, time, re, hmac, hashlib, math
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

# 주기적 청소(메모리 팽창 방지)
_CLEAN_EVERY = 100
_opcount = 0

_TF_RE = re.compile(r'\b(1w|1d|12h|6h|4h|2h|1h|30m|15m|5m|3m)\b', re.IGNORECASE)

def _extract_signature(msg: str) -> str:
    """타임프레임 + 내용 요약 해시로 시그니처 강화(과차단 방지)."""
    if not msg:
        return "unknown"
    m = _TF_RE.search(msg)
    base = m.group(1).lower() if m else "unknown"
    core = re.sub(r'\d+(\.\d+)?', 'N', msg.lower())
    h = hashlib.sha1(core.encode()).hexdigest()[:6]
    return f"{base}:{h}"

def _bucket_key(chat_id: int | str, symbol: str, route: str, msg: str) -> str:
    sig = _extract_signature(msg)
    return f"{chat_id}:{symbol}:{route}:{sig}"

def _can_send_now(bucket: str) -> bool:
    last = _LAST_SENT_AT.get(bucket)
    return (last is None) or (now() - last >= COOLDOWN_SEC)

def _mark_sent(bucket: str):
    _LAST_SENT_AT[bucket] = now()

def _is_duplicate(bucket: str, msg_norm: str) -> bool:
    """DEDUP_WINDOW_SEC 내 동일 버킷/메시지 반복 차단 + 주기적 청소"""
    global _opcount
    k = (bucket, hash(msg_norm))
    t = _RECENT_MSG_HASH.get(k)
    nowt = now()
    if t is not None and (nowt - t) < DEDUP_WINDOW_SEC:
        return True
    _RECENT_MSG_HASH[k] = nowt
    _opcount += 1
    if _opcount % _CLEAN_EVERY == 0:
        cutoff = now() - DEDUP_WINDOW_SEC
        for kk, ts in list(_RECENT_MSG_HASH.items()):
            if ts < cutoff:
                _RECENT_MSG_HASH.pop(kk, None)
    return False

# --- Telegram base ---
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN/TELEGRAM_BOT_TOKEN env is missing")
TG_SEND = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
TG_EDIT = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
TG_ANSW = f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery"
MAX_LEN = 3900

# 가벼운 리트라이 래퍼

def _post_json(url: str, payload: dict, tries: int = 2, timeout: int = 10):
    last_err = None
    for _ in range(tries):
        try:
            return requests.post(url, json=payload, timeout=timeout)
        except Exception as e:
            last_err = e
            time.sleep(0.2)
    raise last_err


def safe_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= MAX_LEN else s[:MAX_LEN - 20] + "\n...[truncated]"


def post_telegram(chat_id: int | str, text: str, parse_mode: Optional[str] = None, reply_markup: Optional[dict] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": safe_text(text)}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    r = _post_json(TG_SEND, payload)
    return r.json()


def edit_message(chat_id: int | str, message_id: int, text: str, reply_markup: Optional[dict] = None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": safe_text(text)}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    _post_json(TG_EDIT, payload)


def answer_callback_query(cq_id: str, text: str = ""):
    _post_json(TG_ANSW, {"callback_query_id": cq_id, "text": text, "show_alert": False})

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

# --- 공용 웹훅 시크릿(선택)
WEBHOOK_SECRET = _read_optional("WEBHOOK_SECRET")

def _require_webhook_secret(d: dict) -> Optional[tuple]:
    """WEBHOOK_SECRET 미설정이면 그대로 통과 → 기존 호환 100%."""
    if WEBHOOK_SECRET and d.get("secret") != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "bad secret"}), 401
    return None

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
    bad = _require_webhook_secret(data)
    if bad: return bad
    route  = str(data.get("route", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# --- new accumulation endpoint ---
@app.post("/webhook")
def tv_webhook_new():
    data = request.get_json(silent=True, force=True) or {}
    bad = _require_webhook_secret(data)
    if bad: return bad
    route  = str(data.get("type", data.get("route", ""))).strip()
    msg    = str(data.get("message", data.get("msg", ""))).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# =========================================================
# === BNC_POSITION 전용 (bbangdol_bnc_bot)
# =========================================================

def post_telegram_with_token(bot_token: str, chat_id: str, text: str, reply_markup: Optional[dict] = None) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": safe_text(text)}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _post_json(url, payload).json()


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
# === Binance USDⓈ-M Futures — helpers
# =========================================================

def _now_ms() -> int:
    return int(time.time() * 1000)


def _sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def _binance_base() -> str:
    base = _read_optional("BINANCE_FUTURES_BASE")
    if base:
        return base
    return "https://testnet.binancefuture.com" if os.getenv("BINANCE_IS_TESTNET", "1") == "1" else "https://fapi.binance.com"


def _binance_get(path: str, params: dict) -> dict:
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
    r = requests.get(url, headers=headers, timeout=10)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code != 200:
        raise RuntimeError(f"Binance HTTP {r.status_code} {data}")
    return data


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


def place_stop_market(symbol: str, side: str, qty: float, stop_price: float, position_side: Optional[str] = None) -> dict:
    # STOP-MARKET (reduceOnly)
    params = {
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "stopPrice": f"{stop_price:.8f}",
        "closePosition": "false",
        "reduceOnly": "true",
        "quantity": qty,
        "timeInForce": "GTC"
    }
    if position_side:
        params["positionSide"] = position_side
    return _binance_post("/fapi/v1/order", params)


def place_trailing(symbol: str, side: str, qty: float, activation_price: float, callback_rate: float, position_side: Optional[str] = None) -> dict:
    params = {
        "symbol": symbol,
        "side": side,
        "type": "TRAILING_STOP_MARKET",
        "activationPrice": f"{activation_price:.8f}",
        "callbackRate": f"{callback_rate:.2f}",
        "reduceOnly": "true",
        "quantity": qty
    }
    if position_side:
        params["positionSide"] = position_side
    return _binance_post("/fapi/v1/order", params)


def get_mark_price(symbol: str) -> float:
    base = _binance_base()
    r = requests.get(f"{base}/fapi/v1/premiumIndex", params={"symbol": symbol}, timeout=10)
    data = r.json()
    return float(data["markPrice"])


def get_account_available_usdt() -> float:
    data = _binance_get("/fapi/v2/balance", {})
    for b in data:
        if b.get("asset") == "USDT":
            # availableBalance: 선물 사용가능 잔고
            return float(b.get("availableBalance", 0))
    return 0.0


def get_symbol_filters(symbol: str) -> dict:
    info = _binance_get("/fapi/v1/exchangeInfo", {})
    for s in info.get("symbols", []):
        if s.get("symbol") == symbol:
            f = {}
            for fil in s.get("filters", []):
                f[fil["filterType"]] = fil
            return f
    return {}


def round_step(value: float, step: float) -> float:
    if step <= 0: return value
    return math.floor(value / step) * step

# =========================================================
# === STATE (설정/포지션/분할)
# =========================================================
STATE = {
    "global_mode": "BOTH",     # BOTH | LONG_ONLY | SHORT_ONLY
    "split_enabled": True,      # 분할 진입 on/off
    "pairs": {
        # "BTCUSDT": {"mode":"LONG","lev":10,"sl":1.0,"trail":{"act":0.6,"cb":0.2},"legs":0}
    }
}
PHASES = [0.20, 0.25, 0.33, 0.50, 1.00]  # 남은 USDT 대비 분할 비율


def get_pair_cfg(sym: str) -> dict:
    d = STATE["pairs"].get(sym, {})
    return {
        "mode": d.get("mode", "BOTH"),
        "lev": d.get("lev", 10),
        "sl": d.get("sl", 1.0),
        "trail": d.get("trail", {"act":0.6,"cb":0.2}),
        "legs": d.get("legs", 0),
    }


def save_pair_cfg(sym: str, cfg: dict):
    STATE["pairs"][sym] = {**get_pair_cfg(sym), **cfg}


def allowed_by_mode(sym: str, side: str) -> bool:
    local = get_pair_cfg(sym)["mode"]
    globalm = STATE["global_mode"]
    eff = local if local in ("LONG","SHORT","BOTH","LONG_ONLY","SHORT_ONLY") else globalm
    eff = {"LONG_ONLY":"LONG","SHORT_ONLY":"SHORT"}.get(eff, eff)
    if eff == "BOTH": return True
    if eff == "LONG": return side == "LONG"
    if eff == "SHORT": return side == "SHORT"
    return True

# =========================================================
# === Telegram UI (inline buttons + force reply)
# =========================================================
UI: Dict[int, dict] = {}  # chat_id -> state


def ui_get(chat_id: int) -> dict:
    return UI.setdefault(chat_id, {"mode":"idle", "cfg":{}})


def ui_reset(chat_id: int):
    UI[chat_id] = {"mode":"idle", "cfg":{}}


def kb_main(cfg: dict) -> dict:
    sym = cfg.get("symbol","미설정")
    mode = cfg.get("dir","BOTH")
    lev = cfg.get("lev","미설정")
    sl  = cfg.get("sl","미설정")
    trail = cfg.get("trail",{})
    trail_txt = f'{trail.get("act","-")}/{trail.get("cb","-")}'
    rows = [
        [{"text": f"① 종목: {sym}", "callback_data": "ADD:SYMBOL"}],
        [{"text": "② 방향 LONG", "callback_data": "ADD:DIR:LONG"},
         {"text": "방향 SHORT", "callback_data": "ADD:DIR:SHORT"},
         {"text": "방향 BOTH", "callback_data": "ADD:DIR:BOTH"}],
        [{"text": f"③ 레버리지: {lev}", "callback_data": "ADD:LEV"}],
        [{"text": f"④ 손절%: {sl}", "callback_data": "ADD:SL"}],
        [{"text": f"⑤ 트레일링(act/cb): {trail_txt}", "callback_data": "ADD:TRAIL"}],
        [{"text": "✅ 저장", "callback_data": "ADD:SAVE"},
         {"text": "↩️ 취소", "callback_data": "ADD:CANCEL"}],
        # --- 글로벌/분할 토글 ---
        [{"text": f"🌐 GLOBAL: {STATE['global_mode']}", "callback_data":"GLOB:MODE"}],
        [{"text": f"🧩 분할진입: {'ON' if STATE['split_enabled'] else 'OFF'}", "callback_data":"SPLIT:TOGGLE"}],
    ]
    return {"inline_keyboard": rows}


def kb_lev() -> dict:
    return {"inline_keyboard":[
        [{"text":"5x","callback_data":"LEV:5"},{"text":"10x","callback_data":"LEV:10"},{"text":"20x","callback_data":"LEV:20"},{"text":"50x","callback_data":"LEV:50"}],
        [{"text":"직접입력","callback_data":"LEV:CUSTOM"},{"text":"⏪ 뒤로","callback_data":"LEV:BACK"}]
    ]}


def kb_sl() -> dict:
    return {"inline_keyboard":[
        [{"text":"0.5%","callback_data":"SL:0.5"},{"text":"1%","callback_data":"SL:1"},{"text":"1.5%","callback_data":"SL:1.5"},{"text":"2%","callback_data":"SL:2"}],
        [{"text":"직접입력","callback_data":"SL:CUSTOM"},{"text":"⏪ 뒤로","callback_data":"SL:BACK"}]
    ]}


def kb_trail() -> dict:
    return {"inline_keyboard":[
        [{"text":"0.6/0.2","callback_data":"TRAIL:0.6:0.2"},
         {"text":"1.0/0.3","callback_data":"TRAIL:1.0:0.3"},
         {"text":"1.5/0.4","callback_data":"TRAIL:1.5:0.4"}],
        [{"text":"직접입력","callback_data":"TRAIL:CUSTOM"},
         {"text":"⏪ 뒤로","callback_data":"TRAIL:BACK"}]
    ]}


def force_reply(ph: str) -> dict:
    return {"force_reply": True, "input_field_placeholder": ph}


@app.post("/tg")
def tg_webhook():
    upd = request.get_json(silent=True) or {}
    msg = upd.get("message") or upd.get("edited_message")
    cq  = upd.get("callback_query")

    if cq:
        chat_id = cq["message"]["chat"]["id"]
        data = cq.get("data","")
        st = ui_get(chat_id)
        answer_callback_query(cq["id"], "")
        if data == "ADD:SYMBOL":
            st["mode"] = "ask_symbol"
            post_telegram(chat_id, "종목 코드를 입력하세요 (예: BTCUSDT)", reply_markup=force_reply("BTCUSDT"))
        elif data.startswith("ADD:DIR:"):
            st["cfg"]["dir"] = data.split(":")[2]
            post_telegram(chat_id, "방향이 설정되었습니다.", reply_markup=kb_main(st["cfg"]))
        elif data == "ADD:LEV":
            st["mode"] = "pick_lev"
            post_telegram(chat_id, "레버리지를 선택하거나 직접 입력하세요.", reply_markup=kb_lev())
        elif data == "ADD:SL":
            st["mode"] = "pick_sl"
            post_telegram(chat_id, "손절 퍼센트를 선택하거나 직접 입력하세요.", reply_markup=kb_sl())
        elif data == "ADD:TRAIL":
            st["mode"] = "pick_trail"
            post_telegram(chat_id, "트레일링 (activate/callback)", reply_markup=kb_trail())
        elif data == "ADD:SAVE":
            cfg = st["cfg"]; sym = cfg.get("symbol")
            if not sym:
                post_telegram(chat_id, "먼저 종목을 입력하세요.", reply_markup=kb_main(st["cfg"]))
                return jsonify({"ok":True})
            mode = cfg.get("dir","BOTH")
            lev  = int(cfg.get("lev",10))
            sl   = float(cfg.get("sl",1.0))
            trail= cfg.get("trail", {"act":0.6,"cb":0.2})
            save_pair_cfg(sym, {"mode":"LONG" if mode=="LONG" else ("SHORT" if mode=="SHORT" else "BOTH"),
                                "lev":lev, "sl":sl, "trail":{"act":float(trail["act"]), "cb":float(trail["cb"])}, "legs":0})
            post_telegram(chat_id, f"✅ 저장 완료\nSYMBOL: {sym}\nDIR: {mode}\nLEV: {lev}x\nSL: {sl}%\nTRAIL: {trail['act']}/{trail['cb']}\n🌐 GLOBAL={STATE['global_mode']}  🧩 SPLIT={'ON' if STATE['split_enabled'] else 'OFF'}", reply_markup=kb_main(st["cfg"]))
        elif data == "ADD:CANCEL":
            ui_reset(chat_id)
            post_telegram(chat_id, "취소했습니다. /add 로 다시 시작하세요.")
        elif data == "LEV:BACK":
            post_telegram(chat_id, "메인으로 돌아갑니다.", reply_markup=kb_main(st["cfg"]))
        elif data == "LEV:CUSTOM":
            st["mode"] = "ask_lev"
            post_telegram(chat_id, "레버리지를 숫자로 입력 (예: 10)", reply_markup=force_reply("10"))
        elif data.startswith("LEV:"):
            st["cfg"]["lev"] = int(data.split(":")[1])
            post_telegram(chat_id, f"레버리지 {st['cfg']['lev']}x 설정", reply_markup=kb_main(st["cfg"]))
        elif data == "SL:BACK":
            post_telegram(chat_id, "메인으로 돌아갑니다.", reply_markup=kb_main(st["cfg"]))
        elif data == "SL:CUSTOM":
            st["mode"] = "ask_sl"
            post_telegram(chat_id, "손절 % 입력 (예: 1)", reply_markup=force_reply("1"))
        elif data.startswith("SL:"):
            st["cfg"]["sl"] = float(data.split(":")[1])
            post_telegram(chat_id, f"손절 {st['cfg']['sl']}% 설정", reply_markup=kb_main(st["cfg"]))
        elif data == "TRAIL:BACK":
            post_telegram(chat_id, "메인으로 돌아갑니다.", reply_markup=kb_main(st["cfg"]))
        elif data == "TRAIL:CUSTOM":
            st["mode"] = "ask_trail_act"
            post_telegram(chat_id, "트레일 활성 % 입력 (예: 0.6)", reply_markup=force_reply("0.6"))
        elif data.startswith("TRAIL:"):
            _, act, cb = data.split(":")
            st["cfg"]["trail"] = {"act": float(act), "cb": float(cb)}
            post_telegram(chat_id, f"트레일링 {act}/{cb} 설정", reply_markup=kb_main(st["cfg"]))
        elif data == "GLOB:MODE":
            # 순환: BOTH -> LONG_ONLY -> SHORT_ONLY -> BOTH
            nxt = {"BOTH":"LONG_ONLY", "LONG_ONLY":"SHORT_ONLY", "SHORT_ONLY":"BOTH"}[STATE["global_mode"]]
            STATE["global_mode"] = nxt
            post_telegram(chat_id, f"🌐 GLOBAL 모드: {STATE['global_mode']}", reply_markup=kb_main(st["cfg"]))
        elif data == "SPLIT:TOGGLE":
            STATE["split_enabled"] = not STATE["split_enabled"]
            post_telegram(chat_id, f"🧩 분할진입: {'ON' if STATE['split_enabled'] else 'OFF'}", reply_markup=kb_main(st["cfg"]))
        return jsonify({"ok": True})

    if msg:
        chat_id = msg["chat"]["id"]
        text = str(msg.get("text","")).strip()
        st = ui_get(chat_id)
        if msg.get("reply_to_message") and st["mode"].startswith("ask_"):
            try:
                if st["mode"] == "ask_symbol":
                    sym = text.upper().replace(" ","")
                    assert sym.endswith("USDT") and len(sym) >= 6
                    st["cfg"]["symbol"] = sym
                    post_telegram(chat_id, f"종목 설정: {sym}", reply_markup=kb_main(st["cfg"]))
                elif st["mode"] == "ask_lev":
                    lev = int(float(text)); assert 1 <= lev <= 125
                    st["cfg"]["lev"] = lev
                    post_telegram(chat_id, f"레버리지 {lev}x 설정", reply_markup=kb_main(st["cfg"]))
                elif st["mode"] == "ask_sl":
                    sl = float(text); assert 0.1 <= sl <= 10
                    st["cfg"]["sl"] = sl
                    post_telegram(chat_id, f"손절 {sl}% 설정", reply_markup=kb_main(st["cfg"]))
                elif st["mode"] == "ask_trail_act":
                    act = float(text); assert 0.1 <= act <= 10
                    st["cfg"].setdefault("trail", {})["act"] = act
                    st["mode"] = "ask_trail_cb"
                    post_telegram(chat_id, "콜백 % 입력 (예: 0.2)", reply_markup=force_reply("0.2"))
                    return jsonify({"ok": True})
                elif st["mode"] == "ask_trail_cb":
                    cb = float(text); assert 0.1 <= cb <= 5
                    st["cfg"].setdefault("trail", {})["cb"] = cb
                    post_telegram(chat_id, f"트레일링 {st['cfg']['trail']['act']}/{cb} 설정", reply_markup=kb_main(st["cfg"]))
                st["mode"] = "idle"
            except Exception:
                post_telegram(chat_id, "입력이 올바르지 않습니다. 다시 시도해 주세요.")
            return jsonify({"ok": True})

        if text in ("/start", "/add"):
            st["mode"] = "idle"
            if "dir" not in st["cfg"]:
                st["cfg"]["dir"] = "BOTH"
            post_telegram(chat_id, "아래 버튼으로 설정하세요.", reply_markup=kb_main(st["cfg"]))
            return jsonify({"ok": True})

        if text == "/list":
            lines = [f"GLOBAL={STATE['global_mode']}  SPLIT={'ON' if STATE['split_enabled'] else 'OFF'}"]
            for s,c in STATE["pairs"].items(): lines.append(f"{s}: {c}")
            post_telegram(chat_id, "SETTINGS\n" + "\n".join(lines))
            return jsonify({"ok": True})

        return jsonify({"ok": True})

    return jsonify({"ok": True})

# =========================================================
# === /bnc/trade : 수량 자동계산 + SL/트레일링 세팅
# =========================================================
# BNC_SYMBOLS이 비어있으면 -> 모든 심볼 허용, 채워두면 -> 해당 심볼만 허용
_raw = _read_optional("BNC_SYMBOLS")
SYM_WHITELIST = set(s.strip().upper() for s in _raw.split(",") if s.strip()) if _raw else None

@app.post("/bnc/trade")
def bnc_trade():
    """
    Body (Pine Stage2):
      {"secret":"<BNC_SECRET>", "symbol":"BTCUSDT", "action":"OPEN_LONG|OPEN_SHORT", "note":"tf=..."}
    qty가 비어있어도 서버가 자동으로 계산.
    """
    data = request.get_json(silent=True, force=True) or {}
    secret = os.getenv("BNC_SECRET")
    if secret and data.get("secret") != secret:
        return jsonify({"ok": False, "error": "bad secret"}), 401

    symbol = str(data.get("symbol", "")).upper()
    action = str(data.get("action", "")).upper()
    note   = str(data.get("note", ""))

    # 화이트리스트가 있을 때만 검사 (없으면 모든 심볼 허용)
    if SYM_WHITELIST and symbol not in SYM_WHITELIST:
        return jsonify({"ok": False, "error": f"symbol not allowed: {symbol}"}), 400

    if action not in {"OPEN_LONG", "CLOSE_LONG", "OPEN_SHORT", "CLOSE_SHORT"}:
        return jsonify({"ok": False, "error": "invalid action"}), 400


    side = "LONG" if "LONG" in action else "SHORT"
    if action.startswith("OPEN") and not allowed_by_mode(symbol, side):
        return jsonify({"ok": True, "skipped": "mode"})

    try:
        # --- 현재 설정/상태 로드
        cfg = get_pair_cfg(symbol)
        legs = cfg["legs"]
        # --- 수량 계산
        price = get_mark_price(symbol)
        avail = get_account_available_usdt()
        lev   = cfg["lev"]

        # 분할 ON이면 PHASES, OFF면 항상 1단계만(=전체)
        if STATE["split_enabled"]:
            phase = PHASES[legs] if legs < len(PHASES) else 0.0
        else:
            phase = 1.0  # 필요 시 0.2로 바꿀 수 있음

        if action.startswith("OPEN"):
            alloc_usdt = avail * phase
            if alloc_usdt <= 0:
                return jsonify({"ok": False, "error": "no available balance"})
            notional = alloc_usdt * lev
            # 심볼 필터에 맞춰 qty 라운딩
            filters = get_symbol_filters(symbol)
            step = float(filters.get("LOT_SIZE", {}).get("stepSize", "0.001"))
            min_qty = float(filters.get("LOT_SIZE", {}).get("minQty", "0.0"))
            qty = max(round_step(notional / price, step), min_qty)
        else:
            # CLOSE_*: 포지션 조회 없이 최소치로 반대 주문(실전은 포지션 조회 권장)
            filters = get_symbol_filters(symbol)
            step = float(filters.get("LOT_SIZE", {}).get("stepSize", "0.001"))
            min_qty = float(filters.get("LOT_SIZE", {}).get("minQty", "0.0"))
            qty = min_qty

        cid = f"bnc_{symbol}_{action}_{int(now())}"

        # === 실행 ===
        if action == "OPEN_LONG":
            res_open = place_market_order(symbol, "BUY", qty, reduce_only=False, position_side="LONG", client_id=cid)
            # SL & Trailing
            sl_pct = float(cfg["sl"])
            sl_price = price * (1 - sl_pct/100.0)
            place_stop_market(symbol, "SELL", qty, stop_price=sl_price, position_side="LONG")
            tr = cfg["trail"]; act = float(tr.get("act", 0.6)); cb=float(tr.get("cb",0.2))
            activation = price * (1 - act/100.0)
            place_trailing(symbol, "SELL", qty, activation_price=activation, callback_rate=cb, position_side="LONG")
            result = res_open
            save_pair_cfg(symbol, {"legs": min(legs+1, len(PHASES))})
        elif action == "OPEN_SHORT":
            res_open = place_market_order(symbol, "SELL", qty, reduce_only=False, position_side="SHORT", client_id=cid)
            sl_pct = float(cfg["sl"])
            sl_price = price * (1 + sl_pct/100.0)
            place_stop_market(symbol, "BUY", qty, stop_price=sl_price, position_side="SHORT")
            tr = cfg["trail"]; act = float(tr.get("act", 0.6)); cb=float(tr.get("cb",0.2))
            activation = price * (1 + act/100.0)
            place_trailing(symbol, "BUY", qty, activation_price=activation, callback_rate=cb, position_side="SHORT")
            result = res_open
            save_pair_cfg(symbol, {"legs": min(legs+1, len(PHASES))})
        elif action == "CLOSE_LONG":
            result = place_market_order(symbol, "SELL", qty, reduce_only=True, position_side="LONG", client_id=cid)
            save_pair_cfg(symbol, {"legs": 0})
        else:  # CLOSE_SHORT
            result = place_market_order(symbol, "BUY", qty, reduce_only=True, position_side="SHORT", client_id=cid)
            save_pair_cfg(symbol, {"legs": 0})

        # 텔레그램 확인 메시지
        try:
            bnc_token = os.getenv("BNC_BOT_TOKEN")
            bnc_chat  = os.getenv("BNC_CHAT_ID")
            confirm   = f"[TRADE] {symbol} {action} qty={qty}\norderId={result.get('orderId')}  status={result.get('status')}\n{note}\n🌐 {STATE['global_mode']}  🧩 SPLIT={'ON' if STATE['split_enabled'] else 'OFF'}  legs={get_pair_cfg(symbol)['legs']}"
            if bnc_token and bnc_chat:
                post_telegram_with_token(bnc_token, bnc_chat, confirm)
        except Exception:
            pass

        return jsonify({"ok": True, "result": result})
    except Exception as e:
        log.exception("bnc_trade error")
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
