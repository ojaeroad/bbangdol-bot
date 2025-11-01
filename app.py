# app.py — unified webhook + BNC trade + TG UI (multi-symbol & risk modes)
import os, json, logging, time, re, hmac, hashlib, math
from time import time as now
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlencode
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# ===== Econ calendar (optional, guarded single-call) =====
# 켜고 싶을 때만: ECON_CAL_ENABLED = "1" / "true" / "on" / "yes"
if os.getenv("ECON_CAL_ENABLED", "0").strip().lower() not in ("0", "false", "", "no", "off"):
    from econ_calendar_tele_bot import init_econ_calendar
    init_econ_calendar(app)

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
    return jsonify({"ok": True, "routes": list(ROUTE_TO_CHAT.keys()), "status": "healthy"})

@app.get("/routes")
def routes_dump():
    return jsonify({"routes": ROUTE_TO_CHAT})

# --- core handler (불꽃타점 등 /bot, /webhook에서 사용) ---
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

# --- old endpoint (legacy for 불꽃타점) ---
@app.post("/bot")
def tv_webhook_legacy():
    data = request.get_json(silent=True, force=True) or {}
    bad = _require_webhook_secret(data)
    if bad: return bad
    route  = str(data.get("route", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# --- new accumulation endpoint (겸용) ---
@app.post("/webhook")
def tv_webhook_new():
    data = request.get_json(silent=True, force=True) or {}
    bad = _require_webhook_secret(data)
    if bad: return bad
    route  = str(data.get("type", data.get("route", ""))).strip()
    msg    = str(data.get("message", data.get("msg", ""))).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

def _is_oneway() -> bool:
    # 기본 HEDGE. 환경변수로 ONEWAY 라고 넣으면 원웨이 처리
    return (os.getenv("BINANCE_POSITION_MODE", "HEDGE").upper() != "HEDGE")


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

# ======================= Binance helpers: symbol & precision =======================

def normalize_binance_symbol(sym: str) -> str:
    """
    TV/내부 저장에는 ETHUSDT.P 같은 것을 쓰더라도,
    바이낸스 API 호출 시에는 .P 등을 제거한 정규 심볼을 사용한다.
    """
    if not sym:
        return sym
    s = sym.strip().upper()
    if s.endswith(".P"):
        s = s[:-2]  # drop trailing ".P"
    # 안전하게 기타 특수문자 제거
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s

def _decimals_from_step(step: float) -> int:
    # 0.01000000 -> 2
    s = f"{step:.16f}".rstrip('0')
    return len(s.split('.')[-1]) if '.' in s else 0

def round_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step

def format_price_for_symbol(symbol: str, raw_price: float) -> str:
    filters = get_symbol_filters(symbol)  # symbol은 정규화된 심볼이어야 함
    tick = float(filters.get("PRICE_FILTER", {}).get("tickSize", "0.01"))
    adj = round_to_step(raw_price, tick)
    dec = _decimals_from_step(tick)
    return f"{adj:.{dec}f}"

def quantize_qty_for_symbol(symbol: str, raw_qty: float) -> float:
    filters = get_symbol_filters(symbol)
    step = float(filters.get("LOT_SIZE", {}).get("stepSize", "0.001"))
    min_qty = float(filters.get("LOT_SIZE", {}).get("minQty", "0.0"))
    qty = round_to_step(raw_qty, step)
    return max(qty, min_qty)

# =========================================================
# === Binance USDⓈ-M Futures — REST
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

def place_market_order(symbol: str, side: str, qty: float,
                       reduce_only: bool = False,
                       position_side: Optional[str] = None,
                       client_id: Optional[str] = None) -> dict:
    params = {
        "symbol": symbol,
        "side": side,               # BUY / SELL
        "type": "MARKET",
        "quantity": qty,            # 코인 수량(필터 반영된 값이어야 함)
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if position_side:
        params["positionSide"] = position_side  # LONG / SHORT (양방향 모드)
    if client_id:
        params["newClientOrderId"] = client_id[:36]
    return _binance_post("/fapi/v1/order", params)

def place_stop_market(symbol: str, side: str, qty: float, stop_price_raw: float,
                      position_side: Optional[str] = None) -> dict:
    stop_price = format_price_for_symbol(symbol, stop_price_raw)
    params = {
        "symbol": symbol,
        "side": side,
        "type": "STOP_MARKET",
        "stopPrice": stop_price,
        "reduceOnly": "true",
        "quantity": qty
    }
    if position_side:
        params["positionSide"] = position_side
    return _binance_post("/fapi/v1/order", params)

def place_trailing(symbol: str, side: str, qty: float, activation_price_raw: float,
                   callback_rate: float, position_side: Optional[str] = None) -> dict:
    activation_price = format_price_for_symbol(symbol, activation_price_raw)
    params = {
        "symbol": symbol,
        "side": side,
        "type": "TRAILING_STOP_MARKET",
        "activationPrice": activation_price,
        "callbackRate": f"{float(callback_rate):.2f}",
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
    if "markPrice" not in data:
        raise RuntimeError(f"premiumIndex error for {symbol}: {data}")
    return float(data["markPrice"])

def get_account_available_usdt() -> float:
    data = _binance_get("/fapi/v2/balance", {})
    for b in data:
        if b.get("asset") == "USDT":
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

# =========================================================
# === STATE & RISK PRESETS (multi-symbol + risk modes)
# =========================================================

RISK_PRESETS = {
    "safe":       {"sl": 1.5, "trail": {"act": 1.5, "cb": 0.4}, "phases": [0.20, 0.25, 0.33, 0.50, 1.00]},
    "normal":     {"sl": 1.0, "trail": {"act": 1.0, "cb": 0.3}, "phases": [0.25, 0.33, 0.50, 1.00]},
    "aggressive": {"sl": 0.7, "trail": {"act": 0.6, "cb": 0.2}, "phases": [0.33, 0.50, 1.00]},
}

def _risk_or_default(name: str) -> str:
    name = (name or "normal").lower()
    return name if name in RISK_PRESETS else "normal"

STATE = {
    "global_mode": "BOTH",    # BOTH | LONG_ONLY | SHORT_ONLY
    "split_enabled": True,    # 분할 진입 on/off
    "pairs": {                # 심볼별 저장소 (사용자 입력 원문 심볼 키로 저장: BTCUSDT.P 등)
        # "BTCUSDT.P": {"dir":"BOTH","lev":10,"sl":1.0,"trail":{"act":0.6,"cb":0.2},"legs":0,"risk":"normal"}
    }
}

def get_pair_cfg(sym_orig: str) -> dict:
    d = STATE["pairs"].get(sym_orig, {})
    return {
        "dir":   d.get("dir", "BOTH"),
        "lev":   d.get("lev", 10),
        "sl":    d.get("sl", 1.0),
        "trail": d.get("trail", {"act":0.6,"cb":0.2}),
        "legs":  d.get("legs", 0),
        "risk":  _risk_or_default(d.get("risk", "normal"))
    }

def save_pair_cfg(sym_orig: str, cfg: dict):
    base = get_pair_cfg(sym_orig)
    base.update(cfg)
    STATE["pairs"][sym_orig] = base

def allowed_by_mode(sym_orig: str, side: str) -> bool:
    local = get_pair_cfg(sym_orig)["dir"]
    globalm = STATE["global_mode"]
    eff = local if local in ("LONG","SHORT","BOTH","LONG_ONLY","SHORT_ONLY") else globalm
    eff = {"LONG_ONLY":"LONG","SHORT_ONLY":"SHORT"}.get(eff, eff)
    if eff == "BOTH": return True
    if eff == "LONG": return side == "LONG"
    if eff == "SHORT": return side == "SHORT"
    return True

def effective_params(sym_orig: str) -> dict:
    """종목 설정 + 리스크 프리셋을 합쳐 실제 주문 파라미터 산출."""
    cfg = get_pair_cfg(sym_orig)
    rkey = cfg["risk"]
    rp = RISK_PRESETS[rkey]
    sl = float(cfg.get("sl") or rp["sl"])
    trail = cfg.get("trail") or rp["trail"]
    phases = rp["phases"]
    return {"sl": sl, "trail": trail, "phases": phases, "lev": cfg["lev"], "dir": cfg["dir"], "risk": rkey, "legs": cfg["legs"]}

# =========================================================
# === Telegram UI (inline buttons + force reply)
# =========================================================
UI: Dict[int, dict] = {}  # chat_id -> state

def ui_get(chat_id: int) -> dict:
    return UI.setdefault(chat_id, {"mode":"idle", "cfg":{}})

def ui_reset(chat_id: int):
    UI[chat_id] = {"mode":"idle", "cfg":{}}

def kb_risk() -> dict:
    return {"inline_keyboard":[
        [{"text":"안전(safe)","callback_data":"RISK:safe"},
         {"text":"보수(normal)","callback_data":"RISK:normal"},
         {"text":"공격(aggressive)","callback_data":"RISK:aggressive"}],
        [{"text":"⏪ 뒤로","callback_data":"RISK:BACK"}]
    ]}

def kb_main(cfg: dict) -> dict:
    sym = cfg.get("symbol","미설정")
    mode = cfg.get("dir","BOTH")
    lev = cfg.get("lev","미설정")
    sl  = cfg.get("sl","미설정")
    trail = cfg.get("trail",{})
    trail_txt = f'{trail.get("act","-")}/{trail.get("cb","-")}'
    risk = cfg.get("risk","normal")
    rows = [
        [{"text": f"① 종목: {sym}", "callback_data": "ADD:SYMBOL"}],
        [{"text": "② 방향 LONG", "callback_data": "ADD:DIR:LONG"},
         {"text": "방향 SHORT", "callback_data": "ADD:DIR:SHORT"},
         {"text": "방향 BOTH", "callback_data": "ADD:DIR:BOTH"}],
        [{"text": f"③ 레버리지: {lev}", "callback_data": "ADD:LEV"}],
        [{"text": f"④ 손절%: {sl}", "callback_data": "ADD:SL"}],
        [{"text": f"⑤ 트레일링(act/cb): {trail_txt}", "callback_data": "ADD:TRAIL"}],
        [{"text": f"⑥ 모드(리스크): {risk}", "callback_data": "ADD:RISK"}],
        [{"text": "✅ 저장", "callback_data": "ADD:SAVE"},
         {"text": "↩️ 취소", "callback_data": "ADD:CANCEL"}],
        [{"text": f"🌐 GLOBAL: {STATE['global_mode']}", "callback_data":"GLOB:MODE"}],
        [{"text": f"🧩 분할진입: {'ON' if STATE['split_enabled'] else 'OFF'}", "callback_data":"SPLIT:TOGGLE"}],
        [{"text": "📜 저장된 종목 보기/열기/삭제", "callback_data":"LIST:OPEN"}]
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
            post_telegram(chat_id, "종목 코드를 입력하세요 (예: BTCUSDT.P 또는 BTCUSDT)", reply_markup=force_reply("BTCUSDT.P"))
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
        elif data == "ADD:RISK":
            st["mode"] = "pick_risk"
            post_telegram(chat_id, "모드를 선택하세요 (안전/보수/공격).", reply_markup=kb_risk())
        elif data == "RISK:BACK":
            post_telegram(chat_id, "메인으로 돌아갑니다.", reply_markup=kb_main(st["cfg"]))
        elif data.startswith("RISK:"):
            st["cfg"]["risk"] = data.split(":")[1]
            post_telegram(chat_id, f"리스크 모드: {st['cfg']['risk']}", reply_markup=kb_main(st["cfg"]))
        elif data == "ADD:SAVE":
            cfg = st["cfg"]; sym = cfg.get("symbol")
            if not sym:
                post_telegram(chat_id, "먼저 종목을 입력하세요.", reply_markup=kb_main(st["cfg"])); return jsonify({"ok":True})
            mode = cfg.get("dir","BOTH")
            lev  = int(cfg.get("lev",10))
            risk = _risk_or_default(cfg.get("risk","normal"))
            sl   = float(cfg.get("sl",0) or 0)
            trail= cfg.get("trail") or {}
            if not sl:
                sl = RISK_PRESETS[risk]["sl"]
            if not trail or "act" not in trail or "cb" not in trail:
                trail = RISK_PRESETS[risk]["trail"]
            save_pair_cfg(sym, {
                "dir":"LONG" if mode=="LONG" else ("SHORT" if mode=="SHORT" else "BOTH"),
                "lev":lev,
                "sl":float(sl),
                "trail":{"act":float(trail["act"]), "cb":float(trail["cb"])},
                "risk": risk,
                "legs":0
            })
            ep = effective_params(sym)
            msgtxt = (f"✅ 저장 완료\nSYMBOL: {sym}\nDIR: {mode}\nLEV: {lev}x\n"
                      f"SL: {ep['sl']}% (risk={risk})\n"
                      f"TRAIL: {ep['trail']['act']}/{ep['trail']['cb']}\n"
                      f"🌐 GLOBAL={STATE['global_mode']}  🧩 SPLIT={'ON' if STATE['split_enabled'] else 'OFF'}")
            post_telegram(chat_id, msgtxt, reply_markup=kb_main(st["cfg"]))
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
            nxt = {"BOTH":"LONG_ONLY", "LONG_ONLY":"SHORT_ONLY", "SHORT_ONLY":"BOTH"}[STATE["global_mode"]]
            STATE["global_mode"] = nxt
            post_telegram(chat_id, f"🌐 GLOBAL 모드: {STATE['global_mode']}", reply_markup=kb_main(st["cfg"]))
        elif data == "SPLIT:TOGGLE":
            STATE["split_enabled"] = not STATE["split_enabled"]
            post_telegram(chat_id, f"🧩 분할진입: {'ON' if STATE['split_enabled'] else 'OFF'}", reply_markup=kb_main(st["cfg"]))
        elif data == "LIST:OPEN":
            if not STATE["pairs"]:
                post_telegram(chat_id, "저장된 종목이 없습니다.", reply_markup=kb_main(st["cfg"]))
            else:
                rows = []
                for s in sorted(STATE["pairs"].keys()):
                    rows.append([
                        {"text": f"열기 {s}", "callback_data": f"LIST:OPEN:{s}"},
                        {"text": "삭제", "callback_data": f"LIST:DEL:{s}"}
                    ])
                rows.append([{"text":"⏪ 뒤로","callback_data":"LIST:BACK"}])
                post_telegram(chat_id, "저장된 종목", reply_markup={"inline_keyboard": rows})
        elif data.startswith("LIST:OPEN:"):
            sym = data.split(":")[2]
            st["cfg"]["symbol"] = sym
            pc = get_pair_cfg(sym)
            st["cfg"]["dir"]   = pc["dir"]
            st["cfg"]["lev"]   = pc["lev"]
            st["cfg"]["sl"]    = pc["sl"]
            st["cfg"]["trail"] = pc["trail"]
            st["cfg"]["risk"]  = pc["risk"]
            post_telegram(chat_id, f"{sym} 불러옴.", reply_markup=kb_main(st["cfg"]))
        elif data.startswith("LIST:DEL:"):
            sym = data.split(":")[2]
            STATE["pairs"].pop(sym, None)
            post_telegram(chat_id, f"{sym} 삭제 완료.", reply_markup=kb_main(st["cfg"]))
        elif data == "LIST:BACK":
            post_telegram(chat_id, "메인으로 돌아갑니다.", reply_markup=kb_main(st["cfg"]))
        return jsonify({"ok": True})

    if msg:
        chat_id = msg["chat"]["id"]
        text = str(msg.get("text","")).strip()
        st = ui_get(chat_id)
        if msg.get("reply_to_message") and st["mode"].startswith("ask_"):
            try:
                if st["mode"] == "ask_symbol":
                    sym = text.upper().replace(" ","")
                    # 저장은 원문 그대로 (BTCUSDT 또는 BTCUSDT.P)
                    assert sym.endswith("USDT") or sym.endswith("USDT.P")
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
            if "risk" not in st["cfg"]:
                st["cfg"]["risk"] = "normal"
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
# === /bnc/trade : 수량 자동계산 + SL/트레일링(리스크 프리셋 반영) + 심볼정규화
# =========================================================
_raw = _read_optional("BNC_SYMBOLS")
SYM_WHITELIST = set(s.strip().upper() for s in _raw.split(",") if s.strip()) if _raw else None

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
    symbol_orig = str(data.get("symbol", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    if not msg:
        return jsonify({"ok": False, "error": "msg missing"}), 400

    header = f"[{tag}] {symbol_orig}" if symbol_orig else f"[{tag}]"
    text   = f"{header}\n{msg}"

    bucket = _bucket_key(bnc_chat, symbol_orig, tag, text)
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

@app.post("/bnc/trade")
def bnc_trade():
    """
    Body (Pine Stage2):
      {"secret":"<BNC_SECRET>", "symbol":"BTCUSDT.P", "action":"OPEN_LONG|OPEN_SHORT|CLOSE_LONG|CLOSE_SHORT", "note":"tf=..."}
    qty는 비워도 서버가 자동 계산.
    """
    data = request.get_json(silent=True, force=True) or {}
    secret = os.getenv("BNC_SECRET")
    if secret and data.get("secret") != secret:
        return jsonify({"ok": False, "error": "bad secret"}), 401

    symbol_orig = str(data.get("symbol", "")).upper()
    base_sym    = normalize_binance_symbol(symbol_orig)
    action = str(data.get("action", "")).upper()
    note   = str(data.get("note", ""))

    if SYM_WHITELIST:
        if (symbol_orig not in SYM_WHITELIST) and (base_sym not in SYM_WHITELIST):
            return jsonify({"ok": False, "error": f"symbol not allowed: {symbol_orig}"}), 400
    if action not in {"OPEN_LONG", "CLOSE_LONG", "OPEN_SHORT", "CLOSE_SHORT"}:
        return jsonify({"ok": False, "error": "invalid action"}), 400

    side = "LONG" if "LONG" in action else "SHORT"
    if action.startswith("OPEN") and not allowed_by_mode(symbol_orig, side):
        return jsonify({"ok": True, "skipped": "mode"})

    try:
        ep   = effective_params(symbol_orig)
        legs = ep["legs"]

        price = get_mark_price(base_sym)
        avail = get_account_available_usdt()
        lev   = ep["lev"]

        phases = ep["phases"]
        if STATE["split_enabled"]:
            phase = phases[legs] if legs < len(phases) else 0.0
        else:
            phase = 1.0

        filters = get_symbol_filters(base_sym)
        step = float(filters.get("LOT_SIZE", {}).get("stepSize", "0.001"))

        if action.startswith("OPEN"):
            alloc_usdt = avail * phase
            if alloc_usdt <= 0:
                return jsonify({"ok": False, "error": "no available balance"})
            notional = alloc_usdt * lev
            raw_qty = notional / price
            qty = quantize_qty_for_symbol(base_sym, raw_qty)
        else:
            qty = quantize_qty_for_symbol(base_sym, 0.0 + step)

        cid = f"bnc_{base_sym}_{action}_{int(now())}"

        ps_long  = None if _is_oneway() else "LONG"
        ps_short = None if _is_oneway() else "SHORT"

        if action == "OPEN_LONG":
            res_open = place_market_order(base_sym, "BUY", qty, reduce_only=False,
                                          position_side=ps_long, client_id=cid)
            sl_pct = float(ep["sl"])
            sl_price = price * (1 - sl_pct/100.0)
            place_stop_market(base_sym, "SELL", qty, stop_price_raw=sl_price,
                              position_side=ps_long)
            tr = ep["trail"]; act = float(tr.get("act")); cb=float(tr.get("cb"))
            activation = price * (1 - act/100.0)
            place_trailing(base_sym, "SELL", qty, activation_price_raw=activation,
                           callback_rate=cb, position_side=ps_long)
            result = res_open
            save_pair_cfg(symbol_orig, {"legs": min(legs+1, len(phases))})

        elif action == "OPEN_SHORT":
            res_open = place_market_order(base_sym, "SELL", qty, reduce_only=False,
                                          position_side=ps_short, client_id=cid)
            sl_pct = float(ep["sl"])
            sl_price = price * (1 + sl_pct/100.0)
            place_stop_market(base_sym, "BUY", qty, stop_price_raw=sl_price,
                              position_side=ps_short)
            tr = ep["trail"]; act = float(tr.get("act")); cb=float(tr.get("cb"))
            activation = price * (1 + act/100.0)
            place_trailing(base_sym, "BUY", qty, activation_price_raw=activation,
                           callback_rate=cb, position_side=ps_short)
            result = res_open
            save_pair_cfg(symbol_orig, {"legs": min(legs+1, len(phases))})

        elif action == "CLOSE_LONG":
            result = place_market_order(base_sym, "SELL", qty, reduce_only=True,
                                        position_side=ps_long, client_id=cid)
            save_pair_cfg(symbol_orig, {"legs": 0})

        else:  # CLOSE_SHORT
            result = place_market_order(base_sym, "BUY", qty, reduce_only=True,
                                        position_side=ps_short, client_id=cid)
            save_pair_cfg(symbol_orig, {"legs": 0})

        try:
            bnc_token = os.getenv("BNC_BOT_TOKEN")
            bnc_chat  = os.getenv("BNC_CHAT_ID")
            confirm   = (f"[TRADE] {symbol_orig}({base_sym}) {action} qty={qty}\n"
                         f"orderId={result.get('orderId')}  status={result.get('status')}\n"
                         f"{note}\n🌐 {STATE['global_mode']}  🧩 SPLIT="
                         f"{'ON' if STATE['split_enabled'] else 'OFF'}  "
                         f"risk={ep['risk']}  legs={get_pair_cfg(symbol_orig)['legs']}")
            if bnc_token and bnc_chat:
                post_telegram_with_token(bnc_token, bnc_chat, confirm)
        except Exception:
            pass

        return jsonify({"ok": True, "result": result})

    except Exception as e:
        log.exception("bbangdol-bot.bnc_trade error")
        return jsonify({"ok": False, "error": str(e)}), 500

# === TradingView → Private /bnc/trade 프록시 ===
@app.post("/tv")
def tv_proxy():
    data = request.get_json(silent=True, force=True) or {}
    # Pine 포맷: {"secret":"...","tag":"BNC_POSITION","symbol":"BTCUSDT.P","tf":"5","p":"...","sig":"LONG_5m"}
    symbol_orig = str(data.get("symbol","")).upper()
    sig    = str(data.get("sig","")).upper()

    if not symbol_orig or not sig:
        return jsonify({"ok": False, "error": "missing symbol/sig"}), 400

    if   sig.startswith("LONG"):  action = "OPEN_LONG"
    elif sig.startswith("SHORT"): action = "OPEN_SHORT"
    else:                         return jsonify({"ok": True, "skipped": "unknown-sig"})

    note = f"tf={data.get('tf','')}, price={data.get('p','')}, sig={sig}"

    private_base = os.getenv("PRIVATE_BASE", "http://bbangdol-bnc-bot-private:10000")
    payload = {
        "secret": os.getenv("BNC_SECRET", ""),
        "symbol": symbol_orig,          # 원문 그대로 전달 (private에서 정규화)
        "action": action,
        "note":   note
    }
    try:
        r = requests.post(f"{private_base}/bnc/trade", json=payload, timeout=10)
        return (r.text, r.status_code, r.headers.items())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500









# =======================
# === BNC DEBUG LAYER ===
# =======================
def _env_true(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1","true","on","yes")

BNC_DEBUG  = _env_true("BNC_DEBUG", "0")   # 켜야만 동작 (기본 OFF)
BNC_DRYRUN = _env_true("BNC_DRYRUN", "0")  # 실주문 막고 시뮬레이션

def _mask(s: str, keep_head: int = 6, keep_tail: int = 4) -> str:
    if not s: return ""
    if len(s) <= keep_head + keep_tail: return "*" * len(s)
    return s[:keep_head] + "…" + s[-keep_tail:]

def _dbg(msg: str):
    if BNC_DEBUG:
        log.info("[BNC-DEBUG] " + msg)

# --- 원함수 보존
__ORIG__ = {}
__ORIG__["_binance_get"]  = _binance_get
__ORIG__["_binance_post"] = _binance_post
__ORIG__["place_market_order"] = place_market_order
__ORIG__["place_stop_market"]  = place_stop_market
__ORIG__["place_trailing"]     = place_trailing
__ORIG__["get_mark_price"]     = get_mark_price
__ORIG__["get_account_available_usdt"] = get_account_available_usdt
__ORIG__["get_symbol_filters"] = get_symbol_filters

# --- 공통 래퍼
def _wrap_http_call(kind: str, fn):
    def _inner(path: str, params: dict):
        if not BNC_DEBUG:
            return fn(path, params)
        try:
            api_key = os.getenv("BINANCE_API_KEY", "")
            base = _binance_base()
            safe_params = {k: ('***' if k in ('signature',) else v) for k, v in params.items()}
            _dbg(f"{kind} → {base}{path}  params={safe_params}  key={_mask(api_key)}")

            res = fn(path, params)

            # 바이낸스 에러 포맷 일원화
            if isinstance(res, dict) and "code" in res and str(res.get("code")).startswith("-"):
                _dbg(f"{kind} ← ERROR code={res.get('code')} msg={res.get('msg')}")
            else:
                # 안전 로그(dict/list/기타 모두 대응)
                if isinstance(res, dict):
                    _dbg(f"{kind} ← OK dict keys={list(res.keys())[:8]}")
                elif isinstance(res, list):
                    first = res[0] if res else None
                    if isinstance(first, dict):
                        _dbg(f"{kind} ← OK list len={len(res)} sample_keys={list(first.keys())[:8]}")
                    else:
                        _dbg(f"{kind} ← OK list len={len(res)}")
                else:
                    _dbg(f"{kind} ← OK type={type(res).__name__}")

            return res
        except Exception as e:
            _dbg(f"{kind} EXC: {e}")
            raise
    return _inner


# _binance_get/post 트레이스
_binance_get  = _wrap_http_call("GET",  __ORIG__["_binance_get"])
_binance_post = _wrap_http_call("POST", __ORIG__["_binance_post"])


# --- 주문류 래퍼 (DRYRUN 지원)
def _wrap_order(name: str, fn):
    def _inner(*args, **kwargs):
        if not BNC_DEBUG:
            return fn(*args, **kwargs)
        try:
            _dbg(f"{name} args={args} kwargs={kwargs}")

            if BNC_DRYRUN and name in ("place_market_order", "place_stop_market", "place_trailing"):
                # 실제 주문 없이 성공 형태 시뮬레이션
                fake = {"orderId": int(now()), "status": "SIMULATED", "dryrun": True, "fn": name}
                _dbg(f"{name} DRYRUN → {fake}")
                return fake

            res = fn(*args, **kwargs)

            # 응답 요약
            if isinstance(res, dict):
                _dbg(f"{name} ← {{'orderId': {res.get('orderId')}, 'status': {res.get('status')}, 'code': {res.get('code')}, 'msg': {res.get('msg')}}}")
            else:
                _dbg(f"{name} ← type={type(res).__name__}")

            return res
        except Exception as e:
            _dbg(f"{name} EXC: {e}")
            raise
    return _inner


place_market_order = _wrap_order("place_market_order", __ORIG__["place_market_order"])
place_stop_market  = _wrap_order("place_stop_market",  __ORIG__["place_stop_market"])
place_trailing     = _wrap_order("place_trailing",     __ORIG__["place_trailing"])

# --- 시세/잔고/필터 조회 트레이스
def _wrap_simple(name: str, fn):
    def _inner(*args, **kwargs):
        if not BNC_DEBUG:
            return fn(*args, **kwargs)
        try:
            _dbg(f"{name} args={args} kwargs={kwargs}")
            res = fn(*args, **kwargs)
            _dbg(f"{name} ← {res}")
            return res
        except Exception as e:
            _dbg(f"{name} EXC: {e}")
            raise
    return _inner

get_mark_price = _wrap_simple("get_mark_price", __ORIG__["get_mark_price"])
get_account_available_usdt = _wrap_simple("get_account_available_usdt", __ORIG__["get_account_available_usdt"])
get_symbol_filters = _wrap_simple("get_symbol_filters", __ORIG__["get_symbol_filters"])

# --- 라우트 단계별 원인 자동표시: /bnc/trade 가 어디서 막히는지 한눈에
__ORIG__["bnc_trade"] = bnc_trade

@app.post("/bnc/trade")
def bnc_trade_with_trace():
    if not BNC_DEBUG:
        return __ORIG__["bnc_trade"]()

    stage = "start"
    try:
        data = request.get_json(silent=True, force=True) or {}
        _dbg(f"REQ /bnc/trade body={data}")

        # 1) 인증
        stage = "auth"
        secret = os.getenv("BNC_SECRET")
        if secret and data.get("secret") != secret:
            _dbg("FAIL at auth: bad secret")
            return jsonify({"ok": False, "stage": stage, "error": "bad secret"}), 401

        # 2) 파라미터 정합
        stage = "params"
        symbol_orig = str(data.get("symbol", "")).upper()
        base_sym    = normalize_binance_symbol(symbol_orig)
        action      = str(data.get("action", "")).upper()
        note        = str(data.get("note", ""))

        if not symbol_orig or not action:
            _dbg("FAIL at params: missing symbol/action")
            return jsonify({"ok": False, "stage": stage, "error": "missing symbol/action"}), 400

        if SYM_WHITELIST and (symbol_orig not in SYM_WHITELIST) and (base_sym not in SYM_WHITELIST):
            _dbg(f"FAIL at params: symbol not allowed {symbol_orig}")
            return jsonify({"ok": False, "stage": stage, "error": f"symbol not allowed: {symbol_orig}"}), 400

        if action not in {"OPEN_LONG", "CLOSE_LONG", "OPEN_SHORT", "CLOSE_SHORT"}:
            _dbg(f"FAIL at params: invalid action {action}")
            return jsonify({"ok": False, "stage": stage, "error": "invalid action"}), 400

        # 3) 모드 제한
        stage = "mode-check"
        side = "LONG" if "LONG" in action else "SHORT"
        if action.startswith("OPEN") and not allowed_by_mode(symbol_orig, side):
            _dbg("SKIP at mode-check: not allowed by mode")
            return jsonify({"ok": True, "stage": stage, "skipped": "mode"})

        # 4) 서버-바이낸스 시계 드리프트
        stage = "time-drift"
        try:
            base = _binance_base()
            t = requests.get(f"{base}/fapi/v1/time", timeout=5).json().get("serverTime")
            drift_ms = abs(int(t) - _now_ms()) if t else None
        except Exception:
            drift_ms = None
        _dbg(f"time drift(ms)={drift_ms}")

        # 5) 시세/잔고/레버리지 파라미터
        stage = "price-balance"
        ep     = effective_params(symbol_orig)
        price  = get_mark_price(base_sym)
        avail  = get_account_available_usdt()
        lev    = ep["lev"]
        phases = ep["phases"]
        legs   = ep["legs"]

        # 6) 수량 계산 (분할 진입 포함)
        stage = "qty-calc"
        if STATE["split_enabled"]:
            phase = phases[legs] if legs < len(phases) else 0.0
        else:
            phase = 1.0

        filters = get_symbol_filters(base_sym)
        step = float(filters.get("LOT_SIZE", {}).get("stepSize", "0.001"))

        if action.startswith("OPEN"):
            alloc_usdt = avail * phase
            if alloc_usdt <= 0:
                _dbg("FAIL at qty-calc: no available balance")
                return jsonify({"ok": False, "stage": stage, "error": "no available balance"})
            notional = alloc_usdt * lev
            raw_qty = notional / price
            qty = quantize_qty_for_symbol(base_sym, raw_qty)
        else:
            qty = quantize_qty_for_symbol(base_sym, 0.0 + step)

        _dbg(f"calc → price={price} avail={avail} lev={lev} phase={phase} qty={qty} legs={legs} split={STATE['split_enabled']}")

        # 7) 주문/청산 실행 (DRYRUN 지원)
        stage = "place-order"
        cid = f"bnc_{base_sym}_{action}_{int(now())}"
        ps_long  = None if _is_oneway() else "LONG"
        ps_short = None if _is_oneway() else "SHORT"

        # 실주문 우회 (Dry-run)
        def _sim(ok=True, extra=None):
            d = {"ok": ok, "stage": stage, "dryrun": True, "action": action, "symbol": base_sym}
            if extra: d.update(extra)
            return jsonify(d)

        if BNC_DRYRUN:
            # 스탭별 성공 시뮬레이션
            _dbg("DRYRUN: skip real exchange calls for order placement")
            # trailing/stop 가정치도 함께 반환
            return _sim(True, {"qty": qty, "note": note, "split": STATE["split_enabled"], "legs": legs, "risk": ep["risk"]})

        if action == "OPEN_LONG":
            res_open = place_market_order(base_sym, "BUY", qty, reduce_only=False, position_side=ps_long, client_id=cid)
            sl_pct = float(ep["sl"]); sl_price = price * (1 - sl_pct/100.0)
            place_stop_market(base_sym, "SELL", qty, stop_price_raw=sl_price, position_side=ps_long)
            tr = ep["trail"]; act = float(tr.get("act")); cb = float(tr.get("cb"))
            activation = price * (1 - act/100.0)
            place_trailing(base_sym, "SELL", qty, activation_price_raw=activation, callback_rate=cb, position_side=ps_long)
            save_pair_cfg(symbol_orig, {"legs": min(legs+1, len(phases))})
            result = res_open

        elif action == "OPEN_SHORT":
            res_open = place_market_order(base_sym, "SELL", qty, reduce_only=False, position_side=ps_short, client_id=cid)
            sl_pct = float(ep["sl"]); sl_price = price * (1 + sl_pct/100.0)
            place_stop_market(base_sym, "BUY", qty, stop_price_raw=sl_price, position_side=ps_short)
            tr = ep["trail"]; act = float(tr.get("act")); cb = float(tr.get("cb"))
            activation = price * (1 + act/100.0)
            place_trailing(base_sym, "BUY", qty, activation_price_raw=activation, callback_rate=cb, position_side=ps_short)
            save_pair_cfg(symbol_orig, {"legs": min(legs+1, len(phases))})
            result = res_open

        elif action == "CLOSE_LONG":
            result = place_market_order(base_sym, "SELL", qty, reduce_only=True, position_side=ps_long, client_id=cid)
            save_pair_cfg(symbol_orig, {"legs": 0})

        else:  # CLOSE_SHORT
            result = place_market_order(base_sym, "BUY", qty, reduce_only=True, position_side=ps_short, client_id=cid)
            save_pair_cfg(symbol_orig, {"legs": 0})

        stage = "notify"
        try:
            bnc_token = os.getenv("BNC_BOT_TOKEN"); bnc_chat  = os.getenv("BNC_CHAT_ID")
            confirm   = (f"[TRADE/DEBUG] {symbol_orig}({base_sym}) {action} qty={qty}\n"
                         f"orderId={result.get('orderId')}  status={result.get('status')}\n"
                         f"{note}\nstage={stage}  drift(ms)={locals().get('drift_ms',None)}")
            if bnc_token and bnc_chat:
                post_telegram_with_token(bnc_token, bnc_chat, confirm)
        except Exception:
            pass

        return jsonify({"ok": True, "stage": stage, "result": result})

    except Exception as e:
        log.exception("bnc_trade_with_trace")
        return jsonify({"ok": False, "stage": stage, "error": str(e)}), 500

# --- 상태 종합 점검: /bnc/diag
@app.get("/bnc/diag")
def bnc_diag():
    """실제 주문 없이 현재 환경/연결/권한/시계/기본심볼 필터를 한 번에 보여줌"""
    try:
        base = _binance_base()
        api_key = os.getenv("BINANCE_API_KEY","")
        is_testnet = "testnet" in base
        drift_ms = None
        try:
            t = requests.get(f"{base}/fapi/v1/time", timeout=5).json().get("serverTime")
            drift_ms = abs(int(t) - _now_ms()) if t else None
        except Exception:
            pass

        ok_balance = True
        bal = 0.0
        err_balance = None
        try:
            bal = get_account_available_usdt()
        except Exception as e:
            ok_balance = False
            err_balance = str(e)

        sym = "BTCUSDT"
        f = {}
        err_filters = None
        try:
            f = get_symbol_filters(sym)
        except Exception as e:
            err_filters = str(e)

        return jsonify({
            "ok": True,
            "binance_base": base,
            "is_testnet": is_testnet,
            "api_key_masked": _mask(api_key),
            "time_drift_ms": drift_ms,
            "balance_ok": ok_balance,
            "available_usdt": bal,
            "filters_ok": err_filters is None,
            "filters_sample_symbol": sym,
            "price_filter_tick": f.get("PRICE_FILTER", {}).get("tickSize"),
            "lot_step": f.get("LOT_SIZE", {}).get("stepSize"),
            "env_flags": {
                "BNC_DEBUG": BNC_DEBUG,
                "BNC_DRYRUN": BNC_DRYRUN,
                "BINANCE_IS_TESTNET": os.getenv("BINANCE_IS_TESTNET",""),
                "BINANCE_POSITION_MODE": os.getenv("BINANCE_POSITION_MODE","HEDGE")
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
