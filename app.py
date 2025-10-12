# app.py  — unified webhook (old routes kept + new accumulation routes)
import os, json, logging, time, re
from time import time as now
from typing import Dict, Any, Optional, Tuple
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# === Anti-spam settings (60s fixed) ===
COOLDOWN_SEC      = 60   # 같은 버킷(=chat_id+symbol+route+대표TF) 최소 간격
DEDUP_WINDOW_SEC  = 60   # 같은 버킷 내 동일 메시지(내용) 중복 방지

_LAST_SENT_AT: Dict[str, float]                 = {}  # bucket -> last sent epoch
_RECENT_MSG_HASH: Dict[Tuple[str, int], float]  = {}  # (bucket, hash(msg_norm)) -> epoch

# 대표 TF 추출 (메시지 본문에서 첫 라인의 가격 다음 줄부터 검색)
_TF_RE = re.compile(r'\b(1w|1d|12h|6h|4h|2h|1h|30m|15m|5m|3m)\b', re.IGNORECASE)

def _extract_signature(msg: str) -> str:
    if not msg:
        return "unknown"
    lines = msg.splitlines()
    # 첫 줄은 "EXCH : SYM : PRICE" 라인이므로 그 다음 줄들만 검사
    for line in lines[1:]:
        m = _TF_RE.search(line)
        if m:
            return m.group(1).lower()  # 1w/1d/.../3m
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
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": safe_text(text)}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    last_err: Optional[str] = None
    delay = 1
    max_retries = 5

    for _ in range(max_retries):
        try:
            r = requests.post(TG_SEND, json=payload, timeout=10)
        except Exception as e:
            last_err = str(e)
            time.sleep(delay)
            delay = min(delay * 2, 8)
            continue

        if r.status_code == 200:
            return r.json()

        try:
            data = r.json()
        except Exception:
            data = {}
        desc = str(data.get("description", ""))

        if r.status_code == 429 and "parameters" in data and "retry_after" in data["parameters"]:
            wait = int(data["parameters"]["retry_after"]) + 1
            time.sleep(wait)
            continue

        if r.status_code == 400 and "chat not found" in desc.lower():
            raise RuntimeError(f"chat_id invalid: {chat_id} ({desc})")

        last_err = f"HTTP {r.status_code} {r.text}"
        time.sleep(delay)
        delay = min(delay * 2, 8)

    raise RuntimeError(f"sendMessage failed after retries: {last_err}")

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
    add_if("OB_SHORT", "OB_SHORT_CHAT_ID")
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

    # 스캘핑 단타
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

# --- core handler (쿨타임/중복필터 포함) ---
def _handle_payload(route: str, msg: str, symbol: str = ""):
    if not route or not msg:
        return jsonify({"ok": False, "error": "missing route or msg"}), 400

    chat_id = route_to_chat_id(route)
    if not chat_id:
        log.error(f"[DROP] Unknown route={route} (symbol={symbol})")
        return jsonify({"ok": False, "error": "unknown_route"}), 200

    # 버킷 선택: chat_id + symbol + route + 대표TF
    bucket = _bucket_key(chat_id, symbol, route, msg)
    msg_norm = safe_text(msg)

    # 60초 쿨타임: 같은 버킷이면 60초에 1회만
    if not _can_send_now(bucket):
        log.info(f"[SKIP cooldown] bucket={bucket}")
        return jsonify({"ok": True, "skipped": "cooldown", "bucket": bucket})

    # 60초 중복 방지: 같은 버킷에서 같은 내용이면 스킵
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
