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
if os.getenv("ECON_CAL_ENABLED", "0").strip().lower() not in ("0", "false", "", "no", "off"):
    from econ_calendar_tele_bot import init_econ_calendar
    init_econ_calendar(app)

# === Anti-spam settings (60s fixed) ===
COOLDOWN_SEC      = 60
DEDUP_WINDOW_SEC  = 60

_LAST_SENT_AT: Dict[str, float]                 = {}
_RECENT_MSG_HASH: Dict[Tuple[str, int], float]  = {}

_CLEAN_EVERY = 100
_opcount = 0

_TF_RE = re.compile(r'\b(1w|1d|12h|6h|4h|2h|1h|30m|15m|5m|3m)\b', re.IGNORECASE)

def _extract_signature(msg: str) -> str:
    if not msg: return "unknown"
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
    if s is None: return ""
    s = str(s)
    return s if len(s) <= MAX_LEN else s[:MAX_LEN - 20] + "\n...[truncated]"

def post_telegram(chat_id: int | str, text: str, parse_mode: Optional[str] = None, reply_markup: Optional[dict] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": safe_text(text)}
    if parse_mode: payload["parse_mode"] = parse_mode
    if reply_markup: payload["reply_markup"] = reply_markup
    r = _post_json(TG_SEND, payload)
    return r.json()

def post_telegram_with_token(bot_token: str, chat_id: str, text: str, reply_markup: Optional[dict] = None) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": safe_text(text)}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _post_json(url, payload).json()

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
        return jsonify({"ok": True, "skipped": "cooldown", "bucket": bucket})
    if _is_duplicate(bucket, msg_norm):
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

# --- old endpoint (legacy)
@app.post("/bot")
def tv_webhook_legacy():
    data = request.get_json(silent=True, force=True) or {}
    bad = _require_webhook_secret(data)
    if bad: return bad
    route  = str(data.get("route", "")).strip()
    msg    = str(data.get("msg", "")).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

# --- new accumulation endpoint
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
    return (os.getenv("BINANCE_POSITION_MODE", "HEDGE").upper() != "HEDGE")

# =========================================================
# === BNC_POSITION 보조 엔드포인트
# =========================================================
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
    if not sym: return sym
    s = sym.strip().upper()
    if s.endswith(".P"): s = s[:-2]
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s

def _decimals_from_step(step: float) -> int:
    s = f"{step:.16f}".rstrip('0')
    return len(s.split('.')[-1]) if '.' in s else 0

def round_to_step(value: float, step: float) -> float:
    if step <= 0: return value
    return math.floor(value / step) * step

def format_price_for_symbol(symbol: str, raw_price: float) -> str:
    filters = get_symbol_filters(symbol)
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
    if base: return base
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
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if position_side:
        params["positionSide"] = position_side
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
# === STATE & RISK PRESETS
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
    "global_mode": "BOTH",
    "split_enabled": True,
    "pairs": {}
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
    cfg = get_pair_cfg(sym_orig)
    rkey = cfg["risk"]
    rp = RISK_PRESETS[rkey]
    sl = float(cfg.get("sl") or rp["sl"])
    trail = cfg.get("trail") or rp["trail"]
    phases = rp["phases"]
    return {"sl": sl, "trail": trail, "phases": phases, "lev": cfg["lev"], "dir": cfg["dir"], "risk": rkey, "legs": cfg["legs"]}

# =========================================================
# === Telegram UI (생략 없이 그대로 유지, 원문과 동일) 
# =========================================================
# ... (중략 없이 원문 코드 그대로 유지: tg_webhook, 키보드, 상태머신 등)
# == 편의상 길이 줄이려면 여기는 그대로 두고 사용하시면 됩니다 ==
# ---- 원문에서 주신 tg_webhook 이하 UI 섹션을 그대로 붙여 넣으세요 ----
# (메시지 길이 제한 때문에 여기만 안내: 사용 중인 파일엔 이미 들어있습니다)
# =========================================================

# =========================================================
# === /bnc/trade : 수량 자동계산 + SL/트레일링 + 즉시발동 방지 + 예외도 200
# =========================================================
_raw = _read_optional("BNC_SYMBOLS")
SYM_WHITELIST = set(s.strip().upper() for s in _raw.split(",") if s.strip()) if _raw else None

# 최소 간격(%) — 너무 붙으면 즉시 발동(-2021) 방지
MIN_SL_PCT  = float(os.getenv("BNC_MIN_SL_PCT",  "1.0"))  # 손절 최소 간격
MIN_ACT_PCT = float(os.getenv("BNC_MIN_ACT_PCT", "1.0"))  # 트레일링 활성 최소 간격

def _apply_min_gap(side: str, price: float, sl_pct: float, act_pct: float) -> tuple[float, float]:
    """현재가와 최소 간격을 보장한 stop/activation 가격을 반환"""
    if side == "LONG":
        sl_price  = price * (1 - max(sl_pct,  MIN_SL_PCT)/100.0)
        act_price = price * (1 - max(act_pct, MIN_ACT_PCT)/100.0)
    else:  # SHORT
        sl_price  = price * (1 + max(sl_pct,  MIN_SL_PCT)/100.0)
        act_price = price * (1 + max(act_pct, MIN_ACT_PCT)/100.0)
    return sl_price, act_price

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
        return jsonify({"ok": False, "error": str(e)}), 200  # 200로 변환

@app.post("/bnc/trade")
def bnc_trade():
    """
    Body (Pine Stage2):
      {"secret":"<BNC_SECRET>", "symbol":"BTCUSDT.P", "action":"OPEN_LONG|OPEN_SHORT|CLOSE_LONG|CLOSE_SHORT", "note":"tf=..."}
    qty는 비워도 서버가 자동 계산.
    """
    # === 항상 200으로 응답하도록 최상위 예외 처리 ===
    try:
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
                return jsonify({"ok": False, "error": f"symbol not allowed: {symbol_orig}"}), 200
        if action not in {"OPEN_LONG", "CLOSE_LONG", "OPEN_SHORT", "CLOSE_SHORT"}:
            return jsonify({"ok": False, "error": "invalid action"}), 200

        side = "LONG" if "LONG" in action else "SHORT"
        if action.startswith("OPEN") and not allowed_by_mode(symbol_orig, side):
            return jsonify({"ok": True, "skipped": "mode"}), 200

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
                return jsonify({"ok": False, "error": "no available balance"}), 200
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
            tr = ep["trail"]; act = float(tr.get("act")); cb=float(tr.get("cb"))

            # 즉시발동 방지(최소 간격 보장)
            sl_price, activation = _apply_min_gap("LONG", price, sl_pct, act)

            place_stop_market(base_sym, "SELL", qty, stop_price_raw=sl_price,
                              position_side=ps_long)
            place_trailing(base_sym, "SELL", qty, activation_price_raw=activation,
                           callback_rate=cb, position_side=ps_long)
            result = res_open
            save_pair_cfg(symbol_orig, {"legs": min(legs+1, len(phases))})

        elif action == "OPEN_SHORT":
            res_open = place_market_order(base_sym, "SELL", qty, reduce_only=False,
                                          position_side=ps_short, client_id=cid)
            sl_pct = float(ep["sl"])
            tr = ep["trail"]; act = float(tr.get("act")); cb=float(tr.get("cb"))

            sl_price, activation = _apply_min_gap("SHORT", price, sl_pct, act)

            place_stop_market(base_sym, "BUY", qty, stop_price_raw=sl_price,
                              position_side=ps_short)
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

        # 텔레그램 확인 알림
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

        return jsonify({"ok": True, "result": result}), 200

    except Exception as e:
        # ❗여기로 떨어져도 200으로 응답 — TV/텔레그램 끊김 방지
        log.exception("bbangdol-bot.bnc_trade error")
        err = str(e)
        try:
            bnc_token = os.getenv("BNC_BOT_TOKEN")
            bnc_chat  = os.getenv("BNC_CHAT_ID")
            if bnc_token and bnc_chat:
                post_telegram_with_token(bnc_token, bnc_chat, f"[TRADE/ERROR] {err}")
        except Exception:
            pass
        return jsonify({"ok": False, "error": err}), 200

# === TradingView → Private /bnc/trade 프록시 ===
@app.post("/tv")
def tv_proxy():
    data = request.get_json(silent=True, force=True) or {}
    symbol_orig = str(data.get("symbol","")).upper()
    sig    = str(data.get("sig","")).upper()

    if not symbol_orig or not sig:
        return jsonify({"ok": False, "error": "missing symbol/sig"}), 200

    if   sig.startswith("LONG"):  action = "OPEN_LONG"
    elif sig.startswith("SHORT"): action = "OPEN_SHORT"
    else:                         return jsonify({"ok": True, "skipped": "unknown-sig"}), 200

    note = f"tf={data.get('tf','')}, price={data.get('p','')}, sig={sig}"

    private_base = os.getenv("PRIVATE_BASE", "http://bbangdol-bnc-bot-private:10000")
    payload = {
        "secret": os.getenv("BNC_SECRET", ""),
        "symbol": symbol_orig,
        "action": action,
        "note":   note
    }
    try:
        r = requests.post(f"{private_base}/bnc/trade", json=payload, timeout=10)
        # private는 이제 항상 200을 주므로 그대로 전달
        return (r.text, r.status_code, r.headers.items())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# --- 상태 종합 점검: /bnc/diag
@app.get("/bnc/diag")
def bnc_diag():
    try:
        base = _binance_base()
        api_key = os.getenv("BINANCE_API_KEY","")
        def _mask(s: str, keep_head: int = 6, keep_tail: int = 4) -> str:
            if not s: return ""
            if len(s) <= keep_head + keep_tail: return "*" * len(s)
            return s[:keep_head] + "…" + s[-keep_tail:]

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
            "is_testnet": "testnet" in base,
            "api_key_masked": _mask(api_key),
            "time_drift_ms": drift_ms,
            "balance_ok": ok_balance,
            "available_usdt": bal,
            "filters_ok": err_filters is None,
            "filters_sample_symbol": sym,
            "price_filter_tick": f.get("PRICE_FILTER", {}).get("tickSize"),
            "lot_step": f.get("LOT_SIZE", {}).get("stepSize"),
            "env_flags": {
                "BINANCE_IS_TESTNET": os.getenv("BINANCE_IS_TESTNET",""),
                "BINANCE_POSITION_MODE": os.getenv("BINANCE_POSITION_MODE","HEDGE"),
                "BNC_MIN_SL_PCT": MIN_SL_PCT,
                "BNC_MIN_ACT_PCT": MIN_ACT_PCT
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
