# app.py — unified webhook + BNC trade + TG UI (multi-symbol & risk modes)
import os, json, logging, time, re, hmac, hashlib, math, threading
import csv
import io
from datetime import datetime, timedelta, timezone
from time import time as now
from typing import Dict, Any, Optional, Tuple
from functools import wraps
from urllib.parse import urlencode
from flask import Flask, request, jsonify, render_template_string, session, redirect, url_for, abort, Response
import requests

# 회원 운영용 성과 분석 DB (기존 텔레그램/자동매매와 독립)
from performance_store import queue_signal_save, health_summary, latest_signals
from performance_analyzer import rebuild_individual_pairs, analysis_summary, latest_analysis_pairs, visual_cycle_data
from performance_group_analyzer import group_analysis_data, group_analysis_market_data, update_settings as update_group_settings
try:
    from performance_automation import (
        automation_status,
        send_latest_cycle_test,
        send_period_report_test,
        start_performance_automation,
    )
    PERFORMANCE_AUTOMATION_IMPORT_ERROR = ""
except Exception as exc:
    PERFORMANCE_AUTOMATION_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

    def start_performance_automation():
        logging.getLogger("bbangdol-bot").exception(
            "Performance automation import failed: %s",
            PERFORMANCE_AUTOMATION_IMPORT_ERROR,
        )
        return False

    def automation_status():
        return {
            "ok": False,
            "import_error": PERFORMANCE_AUTOMATION_IMPORT_ERROR,
        }

    def send_period_report_test(kind):
        raise RuntimeError(PERFORMANCE_AUTOMATION_IMPORT_ERROR)

    def send_latest_cycle_test(market=None, symbol=None):
        raise RuntimeError(PERFORMANCE_AUTOMATION_IMPORT_ERROR)

app = Flask(__name__)
app.jinja_env.globals["symbol_display"] = lambda symbol, exchange=None: symbol_display(symbol, exchange)
app.jinja_env.globals["exchange_only_label"] = lambda exchange=None, market=None: exchange_only_label(exchange, market)
app.jinja_env.globals["price_path_svg"] = lambda position, width=960, height=360: price_path_svg(position, width, height)
app.jinja_env.globals["format_minutes_compact"] = lambda value: _format_minutes_compact(value)
app.secret_key = os.getenv("PERFORMANCE_SESSION_SECRET", "").strip()
if not app.secret_key:
    # Render 환경변수가 아직 없을 때 서버가 죽지는 않게 하되,
    # 반드시 PERFORMANCE_SESSION_SECRET을 등록해야 한다.
    app.secret_key = "CHANGE-ME-PERFORMANCE-SESSION-SECRET"

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 12,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# 성과 자동발송은 별도 데몬 스레드로 실행된다.
# 실패해도 기존 텔레그램 알람과 자동매매 요청에는 영향이 없다.
start_performance_automation()

# ---- Version / Service markers (for live check) ----
APP_VERSION  = os.getenv("APP_VERSION", "dev")
SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown")

# 회원 전용 주간·월간 성과 리포트 공지방.
# 값이 없어도 기존 알람과 서버 실행에는 영향이 없다.
MEMBER_NOTICE_CHAT_ID = os.getenv("MEMBER_NOTICE_1Q", "").strip()

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

@app.get("/version")
def version():
    return jsonify({
        "service": SERVICE_NAME,
        "version": APP_VERSION,
        "member_notice_configured": bool(MEMBER_NOTICE_CHAT_ID),
        "performance_automation_enabled": os.getenv(
            "PERFORMANCE_AUTOMATION_ENABLED", "1"
        ).strip().lower() not in ("0", "false", "off", "no"),
        "economic_calendar_feature": "removed",
    })

@app.get("/whoami")
def whoami():
    return jsonify({"service": SERVICE_NAME})

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
    nowt = now()
    t = _RECENT_MSG_HASH.get(k)
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

# --- (NEW) Telegram webhook helpers ---
TG_WEBHOOK_BASE = os.getenv("TG_WEBHOOK_BASE")  # 예: https://bbangdol-bnc-bot.onrender.com

def _set_webhook() -> dict:
    """TG_WEBHOOK_BASE가 설정된 경우 /tg로 웹훅 등록."""
    if not TG_WEBHOOK_BASE:
        return {"ok": False, "reason": "TG_WEBHOOK_BASE not set"}
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    cb = TG_WEBHOOK_BASE.rstrip("/") + "/tg"
    r = requests.post(url, json={"url": cb, "drop_pending_updates": True}, timeout=10)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "raw": r.text}

def _get_webhook_info() -> dict:
    r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo", timeout=10)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "raw": r.text}

@app.get("/tg/setup")
def tg_setup():
    """수동 웹훅 등록(1회). TG_WEBHOOK_BASE 필요."""
    try:
        res = _set_webhook()
        return jsonify({"ok": True, "setWebhook": res, "webhookInfo": _get_webhook_info()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.get("/tg/webhook")
def tg_webhook_info():
    return jsonify(_get_webhook_info())

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
    # 기존 라우트들
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
    # ===== 빵돌이 별꽃 타점 / BD 8개 그룹 =====
    # Render 환경변수명 권장:
    # BD_BUY_SHORT, BD_BUY_SWING, BD_BUY_LONG, BD_BUY_LIFE
    # BD_SELL_SHORT, BD_SELL_SWING, BD_SELL_LONG, BD_SELL_LIFE
    add_if("BD_BUY_SHORT", "BD_BUY_SHORT")
    add_if("BD_BUY_SWING", "BD_BUY_SWING")
    add_if("BD_BUY_LONG",  "BD_BUY_LONG")
    add_if("BD_BUY_LIFE",  "BD_BUY_LIFE")
    add_if("BD_SELL_SHORT", "BD_SELL_SHORT")
    add_if("BD_SELL_SWING", "BD_SELL_SWING")
    add_if("BD_SELL_LONG",  "BD_SELL_LONG")
    add_if("BD_SELL_LIFE",  "BD_SELL_LIFE")

    # ===== 1Q 대형주 기존 라우트 호환용 =====
    add_if("BUY_SWING_1Q", "BUY_SWING_1Q")
    add_if("SELL_SWING_1Q", "SELL_SWING_1Q")
    add_if("BUY_LONG_1Q", "BUY_LONG_1Q")
    add_if("SELL_LONG_1Q", "SELL_LONG_1Q")
    add_if("BUY_LIFE_1Q", "BUY_LIFE_1Q")
    add_if("SELL_LIFE_1Q", "SELL_LIFE_1Q")
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

# --- 회원 운영용 성과 분석 DB 상태 (민감정보는 노출하지 않음) ---


# =========================================================
# 기간별·진입시간봉별 회원 성과 계산
# =========================================================
TIMEFRAME_ORDER_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "45m": 45,
    "1h": 60,
    "2h": 120,
    "3h": 180,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
}


def _parse_iso_datetime(value):
    if not value:
        return None
    if hasattr(value, "tzinfo"):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _period_start(period_key: str):
    now = datetime.now(timezone.utc)
    if period_key == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period_key == "7d":
        return now - timedelta(days=7)
    if period_key == "30d":
        return now - timedelta(days=30)
    return None


def _cycle_in_period(cycle, start_at):
    if start_at is None:
        return True

    exit_results = cycle.get("exit_results") or []
    if not exit_results:
        return False

    for result in exit_results:
        exit_time = _parse_iso_datetime(
            (result.get("exit") or {}).get("time")
        )
        if exit_time and exit_time >= start_at:
            return True
    return False


def _member_symbol_statistics(symbol_data, period_key="all"):
    start_at = _period_start(period_key)
    cycles = [
        cycle
        for cycle in (symbol_data.get("completed_cycles") or [])
        if _cycle_in_period(cycle, start_at)
    ]

    all_returns = []
    holding_values = []
    entry_tf_map = {}
    exit_tf_map = {}

    for cycle in cycles:
        for result in cycle.get("exit_results") or []:
            exit_obj = result.get("exit") or {}
            exit_tf = exit_obj.get("timeframe") or "unknown"

            result_values = []

            max_tf_return = result.get("max_timeframe_return_pct")
            if max_tf_return is not None:
                result_values.append(float(max_tf_return))

            all_split_return = result.get("all_split_return_pct")
            if all_split_return is not None:
                result_values.append(float(all_split_return))

            max_hold = result.get("max_timeframe_holding_minutes")
            if max_hold is not None:
                holding_values.append(float(max_hold))

            for tf_result in result.get("timeframe_split_results") or []:
                value = tf_result.get("return_pct")
                if value is None:
                    continue

                value = float(value)
                entry_tf = tf_result.get("timeframe") or "unknown"
                tf_bucket = entry_tf_map.setdefault(
                    entry_tf,
                    {
                        "timeframe": entry_tf,
                        "timeframe_minutes": TIMEFRAME_ORDER_MINUTES.get(
                            entry_tf, 999999
                        ),
                        "returns": [],
                        "holding_minutes": [],
                        "entry_count": 0,
                    },
                )
                tf_bucket["returns"].append(value)
                tf_bucket["entry_count"] += int(
                    tf_result.get("entry_count") or 0
                )

                tf_hold = tf_result.get("holding_minutes")
                if tf_hold is not None:
                    tf_bucket["holding_minutes"].append(float(tf_hold))

                result_values.append(value)

            for individual in result.get("individual_results") or []:
                value = individual.get("return_pct")
                if value is None:
                    continue
                value = float(value)
                result_values.append(value)

                entry = individual.get("entry") or {}
                entry_tf = entry.get("timeframe") or "unknown"
                tf_bucket = entry_tf_map.setdefault(
                    entry_tf,
                    {
                        "timeframe": entry_tf,
                        "timeframe_minutes": TIMEFRAME_ORDER_MINUTES.get(
                            entry_tf, 999999
                        ),
                        "returns": [],
                        "holding_minutes": [],
                        "entry_count": 0,
                    },
                )
                tf_bucket["returns"].append(value)
                tf_bucket["entry_count"] += 1

                hold = individual.get("holding_minutes")
                if hold is not None:
                    tf_bucket["holding_minutes"].append(float(hold))

            if result_values:
                exit_bucket = exit_tf_map.setdefault(
                    exit_tf,
                    {
                        "timeframe": exit_tf,
                        "timeframe_minutes": TIMEFRAME_ORDER_MINUTES.get(
                            exit_tf, 999999
                        ),
                        "returns": [],
                    },
                )
                exit_bucket["returns"].extend(result_values)
                all_returns.extend(result_values)

    def finalize_bucket(bucket):
        values = bucket.pop("returns", [])
        holding = bucket.pop("holding_minutes", [])
        wins = [value for value in values if value > 0]
        bucket.update(
            {
                "result_count": len(values),
                "win_rate_pct": (
                    len(wins) / len(values) * 100
                    if values else None
                ),
                "average_return_pct": (
                    sum(values) / len(values)
                    if values else None
                ),
                "best_return_pct": max(values) if values else None,
                "worst_return_pct": min(values) if values else None,
                "average_holding_minutes": (
                    sum(holding) / len(holding)
                    if holding else None
                ),
            }
        )
        return bucket

    entry_timeframes = [
        finalize_bucket(bucket.copy())
        for _, bucket in sorted(
            entry_tf_map.items(),
            key=lambda item: item[1]["timeframe_minutes"],
        )
    ]

    entry_timeframes_1h_plus = [
        item
        for item in entry_timeframes
        if item["timeframe_minutes"] >= 60
    ]

    exit_timeframes = []
    for _, bucket in sorted(
        exit_tf_map.items(),
        key=lambda item: item[1]["timeframe_minutes"],
    ):
        values = bucket.pop("returns", [])
        wins = [value for value in values if value > 0]
        bucket.update(
            {
                "result_count": len(values),
                "win_rate_pct": (
                    len(wins) / len(values) * 100
                    if values else None
                ),
                "average_return_pct": (
                    sum(values) / len(values)
                    if values else None
                ),
                "best_return_pct": max(values) if values else None,
                "worst_return_pct": min(values) if values else None,
            }
        )
        exit_timeframes.append(bucket)

    wins = [value for value in all_returns if value > 0]

    return {
        "has_results": bool(all_returns),
        "period_key": period_key,
        "completed_cycle_count": len(cycles),
        "result_count": len(all_returns),
        "average_return_pct": (
            sum(all_returns) / len(all_returns)
            if all_returns else None
        ),
        "best_return_pct": max(all_returns) if all_returns else None,
        "worst_return_pct": min(all_returns) if all_returns else None,
        "win_rate_pct": (
            len(wins) / len(all_returns) * 100
            if all_returns else None
        ),
        "average_holding_minutes": (
            sum(holding_values) / len(holding_values)
            if holding_values else None
        ),
        "entry_timeframes": entry_timeframes,
        "entry_timeframes_1h_plus": entry_timeframes_1h_plus,
        "exit_timeframes": exit_timeframes,
    }








def _member_group_engine_statistics(analysis_data, period_key="all"):
    """완료 사이클 기준 회원 통계. 청산 시간봉은 비교 시나리오다."""
    start_at = _period_start(period_key)
    positions = analysis_data.get("positions") or []
    completed = []
    cycle_avg_returns = []
    cycle_avg_holding = []
    cycle_adverse = []
    recovery_values = []
    all_results = []
    entry_tf_buckets = {}
    best_detail = None

    for position in positions:
        if not position.get("cycle_closed"):
            continue
        exits = []
        for result in position.get("exit_results") or []:
            exit_time = _parse_iso_datetime(result.get("exit_time"))
            if start_at is not None and (exit_time is None or exit_time < start_at):
                continue
            exits.append(result)
        if not exits:
            continue

        completed.append(position)
        returns = [float(row["return_pct"]) for row in exits if row.get("return_pct") is not None]
        holdings = [float(row["holding_minutes"]) for row in exits if row.get("holding_minutes") is not None]
        adverse = [float(row["signal_adverse_pct"]) for row in exits if row.get("signal_adverse_pct") is not None]
        recoveries = [float(row["recovery_minutes"]) for row in exits if row.get("recovery_minutes") is not None]
        if returns:
            cycle_avg_returns.append(sum(returns) / len(returns))
            all_results.extend(exits)
        if holdings:
            cycle_avg_holding.append(sum(holdings) / len(holdings))
        if adverse:
            cycle_adverse.append(min(adverse))
        recovery_values.extend(recoveries)

        for row in exits:
            if row.get("return_pct") is None:
                continue
            candidate = {
                "return_pct": float(row["return_pct"]),
                "entry_timeframe": position.get("entry_timeframe"),
                "exit_timeframe": row.get("exit_timeframe"),
                "cycle": position.get("position_sequence"),
                "holding_text": row.get("holding_text"),
                "entry_price": position.get("entry_price"),
                "exit_price": row.get("exit_price"),
                "signal_adverse_pct": row.get("signal_adverse_pct"),
            }
            if best_detail is None or candidate["return_pct"] > best_detail["return_pct"]:
                best_detail = candidate

        tf = position.get("entry_timeframe") or "unknown"
        bucket = entry_tf_buckets.setdefault(tf, {
            "timeframe": tf,
            "timeframe_minutes": TIMEFRAME_ORDER_MINUTES.get(tf, 999999),
            "cycle_ids": set(), "cycle_returns": [], "outcome_returns": [],
            "holding": [], "adverse": [], "recovery": [], "entry_count": 0,
        })
        bucket["cycle_ids"].add(position.get("position_sequence"))
        if returns:
            bucket["cycle_returns"].append(sum(returns)/len(returns))
            bucket["outcome_returns"].extend(returns)
        if holdings: bucket["holding"].append(sum(holdings)/len(holdings))
        if adverse: bucket["adverse"].append(min(adverse))
        bucket["recovery"].extend(recoveries)
        bucket["entry_count"] += int(position.get("entry_count") or 0)

    entry_timeframes=[]
    for bucket in sorted(entry_tf_buckets.values(), key=lambda row: row["timeframe_minutes"]):
        cv=bucket["cycle_returns"]; ov=bucket["outcome_returns"]
        entry_timeframes.append({
            "timeframe": bucket["timeframe"], "timeframe_minutes": bucket["timeframe_minutes"],
            "result_count": len(bucket["cycle_ids"]), "cycle_count": len(bucket["cycle_ids"]),
            "entry_count": bucket["entry_count"],
            "win_rate_pct": len([v for v in cv if v>0])/len(cv)*100 if cv else None,
            "average_return_pct": sum(cv)/len(cv) if cv else None,
            "best_return_pct": max(ov) if ov else None, "worst_return_pct": min(ov) if ov else None,
            "average_holding_minutes": sum(bucket["holding"])/len(bucket["holding"]) if bucket["holding"] else None,
            "average_signal_adverse_pct": sum(bucket["adverse"])/len(bucket["adverse"]) if bucket["adverse"] else None,
            "average_recovery_minutes": sum(bucket["recovery"])/len(bucket["recovery"]) if bucket["recovery"] else None,
        })

    wins=[v for v in cycle_avg_returns if v>0]
    return {
        "has_results": bool(completed), "period_key": period_key,
        "completed_cycle_count": len(completed), "result_count": len(completed),
        "outcome_count": len(all_results), "win_count": len(wins),
        "loss_count": len(cycle_avg_returns)-len(wins),
        "average_return_pct": sum(cycle_avg_returns)/len(cycle_avg_returns) if cycle_avg_returns else None,
        "best_return_pct": max((float(row["return_pct"]) for row in all_results), default=None),
        "worst_return_pct": min((float(row["return_pct"]) for row in all_results), default=None),
        "win_rate_pct": len(wins)/len(cycle_avg_returns)*100 if cycle_avg_returns else None,
        "average_holding_minutes": sum(cycle_avg_holding)/len(cycle_avg_holding) if cycle_avg_holding else None,
        "average_signal_adverse_pct": sum(cycle_adverse)/len(cycle_adverse) if cycle_adverse else None,
        "worst_signal_adverse_pct": min(cycle_adverse) if cycle_adverse else None,
        "average_recovery_minutes": sum(recovery_values)/len(recovery_values) if recovery_values else None,
        "best_detail": best_detail,
        "entry_timeframes": entry_timeframes, "entry_timeframes_1h_plus": [], "exit_timeframes": [],
    }


def _format_minutes_compact(value):
    if value is None: return "-"
    total=max(0,int(round(float(value)))); d,rem=divmod(total,1440); h,m=divmod(rem,60)
    parts=[]
    if d: parts.append(f"{d}일")
    if h: parts.append(f"{h}시간")
    if m or not parts: parts.append(f"{m}분")
    return " ".join(parts)


def _aggregate_market_group_stats(ranked_symbols, category_key):
    groups={}
    for symbol in ranked_symbols:
        for group in symbol.get("member_stats",{}).get("entry_groups",[]):
            if not group.get("has_results"): continue
            bucket=groups.setdefault(group["group_key"], {
                "group_key":group["group_key"],"group_label":group["group_label"],
                "returns":[],"wins":[],"holding":[],"adverse":[],"recovery":[],"cycles":0,
            })
            cycles=int(group.get("result_count") or 0); bucket["cycles"]+=cycles
            if group.get("average_return_pct") is not None: bucket["returns"].extend([float(group["average_return_pct"])]*max(cycles,1))
            if group.get("win_rate_pct") is not None: bucket["wins"].extend([float(group["win_rate_pct"])]*max(cycles,1))
            if group.get("average_holding_minutes") is not None: bucket["holding"].extend([float(group["average_holding_minutes"])]*max(cycles,1))
            if group.get("average_signal_adverse_pct") is not None: bucket["adverse"].extend([float(group["average_signal_adverse_pct"])]*max(cycles,1))
            if group.get("average_recovery_minutes") is not None: bucket["recovery"].extend([float(group["average_recovery_minutes"])]*max(cycles,1))
    ordered=[]
    for key in sorted(groups,key=lambda x:ENTRY_GROUP_ORDER[x]):
        b=groups[key]
        ordered.append({
            "group_key":key,"group_label":b["group_label"],"cycles":b["cycles"],
            "average_return_pct":sum(b["returns"])/len(b["returns"]) if b["returns"] else None,
            "win_rate_pct":sum(b["wins"])/len(b["wins"]) if b["wins"] else None,
            "average_holding_minutes":sum(b["holding"])/len(b["holding"]) if b["holding"] else None,
            "average_signal_adverse_pct":sum(b["adverse"])/len(b["adverse"]) if b["adverse"] else None,
            "average_recovery_minutes":sum(b["recovery"])/len(b["recovery"]) if b["recovery"] else None,
        })
    return ordered

def _svg_escape(value):
    return (
        str(value).replace("&", "&amp;").replace("<", "&lt;")
        .replace(">", "&gt;").replace('"', "&quot;")
    )


def price_path_svg(position, width=960, height=360):
    """진입 가격과 각 청산 시간봉 가격만 표시하는 생략 차트."""
    points = []
    for index, entry in enumerate(position.get("entry_points") or [], 1):
        points.append({
            "kind": "entry",
            "label": f"진입{index} · {entry.get('timeframe')}",
            "price": float(entry.get("price") or 0),
        })
    for result in sorted(
        position.get("exit_results") or [],
        key=lambda row: row.get("exit_timeframe_minutes", 999999),
    ):
        points.append({
            "kind": "exit",
            "label": f"청산 · {result.get('exit_timeframe')}",
            "price": float(result.get("exit_price") or 0),
            "return_pct": float(result.get("return_pct") or 0),
        })
    if not points:
        return '<div class="empty-note">가격 데이터 없음</div>'

    prices = [p["price"] for p in points]
    lo, hi = min(prices), max(prices)
    spread = max(hi - lo, abs(hi) * 0.01, 1e-9)
    px, py = 70, 55
    uw, uh = width - px * 2, height - py * 2

    def xp(i):
        return width / 2 if len(points) == 1 else px + uw * i / (len(points) - 1)
    def yp(price):
        return py + (hi - price) / spread * uh

    coords = [(xp(i), yp(p["price"])) for i, p in enumerate(points)]
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
        f'aria-label="진입 및 청산 가격 생략 차트" '
        f'style="background:#101012;border:1px solid #303035;border-radius:14px">',
        f'<rect width="{width}" height="{height}" rx="14" fill="#101012"/>',
        '<text x="24" y="31" fill="#8bd0ff" font-size="16" font-weight="700">'
        '실제 신호 가격 경로 · 중간 캔들 생략</text>',
    ]
    for i in range(len(coords) - 1):
        x1, y1 = coords[i]
        x2, y2 = coords[i + 1]
        parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            'stroke="#61dca3" stroke-width="4" stroke-dasharray="12 10"/>'
        )
        parts.append(
            f'<text x="{(x1+x2)/2:.1f}" y="{(y1+y2)/2-10:.1f}" '
            'text-anchor="middle" fill="#8d8d95" font-size="13">… 중간 과정 생략 …</text>'
        )
    for i, point in enumerate(points):
        x, y = coords[i]
        entry = point["kind"] == "entry"
        color = "#ffd24d" if entry else "#56e69b"
        extra = "" if entry else f' · {point.get("return_pct", 0):+.2f}%'
        parts.extend([
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="9" fill="{color}" stroke="#111" stroke-width="5"/>',
            f'<text x="{x:.1f}" y="{y-20:.1f}" text-anchor="middle" fill="{color}" '
            f'font-size="15" font-weight="700">{_svg_escape(point["label"] + extra)}</text>',
            f'<text x="{x:.1f}" y="{y+29:.1f}" text-anchor="middle" fill="#f4f4f4" '
            f'font-size="14">{_svg_escape(f"{point["price"]:,.8g}")}</text>',
        ])
    parts.append("</svg>")
    return "".join(parts)


def promo_cycle_svg(position, title, width=1080, height=1080):
    exits=sorted(position.get("exit_results") or [],key=lambda r:r.get("exit_timeframe_minutes",999999))
    best=max(exits,key=lambda r:r.get("return_pct",-999999)) if exits else {}
    chart=price_path_svg(position,920,430)
    inner = chart.split(">",1)[1].rsplit("</svg>",1)[0] if "</svg>" in chart else chart
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}">'
        '<rect width="100%" height="100%" fill="#080809"/>'
        '<rect x="35" y="35" width="1010" height="1010" rx="30" fill="#111113" stroke="#343438"/>'
        '<text x="75" y="105" fill="#ffd24d" font-size="30" font-weight="800">결과로 증명하는 타점 알람</text>'
        f'<text x="75" y="175" fill="#ffffff" font-size="48" font-weight="900">{_svg_escape(title)}</text>'
        f'<text x="75" y="225" fill="#8bd0ff" font-size="25">최초 {position.get("entry_timeframe")} · {position.get("entry_count")}회 진입 · 완료 사이클 #{position.get("position_sequence")}</text>'
        f'<svg x="75" y="270" width="930" height="440" viewBox="0 0 960 360">{inner}</svg>'
        '<rect x="75" y="745" width="440" height="210" rx="22" fill="#19191c" stroke="#3a3a3f"/>'
        '<text x="105" y="800" fill="#aaaaaf" font-size="24">최고 청산 수익률</text>'
        f'<text x="105" y="885" fill="#55e69a" font-size="70" font-weight="900">{best.get("return_pct",0):+.2f}%</text>'
        f'<text x="105" y="930" fill="#ffffff" font-size="24">{best.get("exit_timeframe","-")} 청산 · {best.get("holding_text","-")}</text>'
        '<rect x="545" y="745" width="460" height="210" rx="22" fill="#19191c" stroke="#6e5520"/>'
        '<text x="575" y="800" fill="#aaaaaf" font-size="24">신호 기준 최대 역행</text>'
        f'<text x="575" y="875" fill="#ff7878" font-size="56" font-weight="900">{position.get("signal_adverse_pct",0):.2f}%</text>'
        '<text x="575" y="930" fill="#aaaaaf" font-size="20">중간 캔들이 아닌 저장 LOW 신호 기준</text>'
        '<text x="75" y="1010" fill="#77777e" font-size="18">실제 체결가·수수료·슬리피지·세금은 반영되지 않을 수 있습니다.</text>'
        '</svg>'
    )


ENTRY_GROUP_LABELS = {
    "SCALP": "단타",
    "SWING": "스윙",
    "LONG": "장기",
    "LIFE": "인생타점",
}

ENTRY_GROUP_ORDER = {
    "SCALP": 0,
    "SWING": 1,
    "LONG": 2,
    "LIFE": 3,
}

ENTRY_GROUP_TIMEFRAMES = {
    "COIN": {
        "SCALP": {"5m", "15m"},
        "SWING": {"30m", "1h"},
        "LONG": {"4h", "6h"},
        "LIFE": {"12h", "1d", "1w"},
    },
    "STOCK": {
        "SWING": {"30m", "1h"},
        "LONG": {"4h", "6h"},
        "LIFE": {"1d", "1w"},
    },
}


def _member_market_type(category_key):
    return "COIN" if str(category_key).upper() == "COIN" else "STOCK"


def _entry_group_key(category_key, timeframe):
    market_type = _member_market_type(category_key)
    for group_key, timeframes in ENTRY_GROUP_TIMEFRAMES[market_type].items():
        if timeframe in timeframes:
            return group_key
    return None


def _group_entry_timeframe_stats(category_key, timeframe_stats):
    """회원 화면용 큰 카테고리 → 세부 시간봉 구조."""
    market_type = _member_market_type(category_key)
    output = []

    for group_key in sorted(
        ENTRY_GROUP_TIMEFRAMES[market_type],
        key=lambda key: ENTRY_GROUP_ORDER[key],
    ):
        timeframes = ENTRY_GROUP_TIMEFRAMES[market_type][group_key]
        details = [
            dict(item)
            for item in timeframe_stats
            if item.get("timeframe") in timeframes
        ]
        details.sort(
            key=lambda item: item.get("timeframe_minutes", 999999)
        )

        all_count = sum(int(item.get("result_count") or 0) for item in details)
        weighted_return_total = sum(
            float(item.get("average_return_pct") or 0)
            * int(item.get("result_count") or 0)
            for item in details
        )
        weighted_win_total = sum(
            float(item.get("win_rate_pct") or 0)
            * int(item.get("result_count") or 0)
            for item in details
        )
        holding_items = [
            item for item in details
            if item.get("average_holding_minutes") is not None
            and int(item.get("result_count") or 0) > 0
        ]
        weighted_holding_total = sum(
            float(item["average_holding_minutes"])
            * int(item.get("result_count") or 0)
            for item in holding_items
        )
        holding_count = sum(
            int(item.get("result_count") or 0)
            for item in holding_items
        )
        best_values = [
            item.get("best_return_pct")
            for item in details
            if item.get("best_return_pct") is not None
        ]
        worst_values = [
            item.get("worst_return_pct")
            for item in details
            if item.get("worst_return_pct") is not None
        ]

        output.append(
            {
                "group_key": group_key,
                "group_label": ENTRY_GROUP_LABELS[group_key],
                "details": details,
                "has_results": bool(all_count),
                "result_count": all_count,
                "average_return_pct": (
                    weighted_return_total / all_count
                    if all_count else None
                ),
                "win_rate_pct": (
                    weighted_win_total / all_count
                    if all_count else None
                ),
                "best_return_pct": max(best_values) if best_values else None,
                "worst_return_pct": min(worst_values) if worst_values else None,
                "average_holding_minutes": (
                    weighted_holding_total / holding_count
                    if holding_count else None
                ),
                "average_signal_adverse_pct": (
                    sum(
                        float(item.get("average_signal_adverse_pct")) * int(item.get("result_count") or 0)
                        for item in details if item.get("average_signal_adverse_pct") is not None
                    ) / sum(
                        int(item.get("result_count") or 0)
                        for item in details if item.get("average_signal_adverse_pct") is not None
                    )
                    if any(item.get("average_signal_adverse_pct") is not None for item in details) else None
                ),
                "average_recovery_minutes": (
                    sum(
                        float(item.get("average_recovery_minutes")) * int(item.get("result_count") or 0)
                        for item in details if item.get("average_recovery_minutes") is not None
                    ) / sum(
                        int(item.get("result_count") or 0)
                        for item in details if item.get("average_recovery_minutes") is not None
                    )
                    if any(item.get("average_recovery_minutes") is not None for item in details) else None
                ),
            }
        )
    return output


# 회원·관리자 화면 공통 종목 표기
# KRX는 한글 종목명과 종목코드를 항상 함께 표시한다.
KRX_SYMBOL_NAMES = {
    "005930": "삼성전자",
    "000660": "SK하이닉스",
    "005380": "현대차",
    "032830": "삼성생명",
    "373220": "LG에너지솔루션",
    "207940": "삼성바이오로직스",
    "000270": "기아",
    "068270": "셀트리온",
    "105560": "KB금융",
    "055550": "신한지주",
    "035420": "NAVER",
    "035720": "카카오",
    "012450": "한화에어로스페이스",
    "034020": "두산에너빌리티",
    "086520": "에코프로",
    "247540": "에코프로비엠",
    "006400": "삼성SDI",
    "051910": "LG화학",
    "005490": "POSCO홀딩스",
    "028260": "삼성물산",
    "012330": "현대모비스",
    "066570": "LG전자",
    "003670": "포스코퓨처엠",
    "009150": "삼성전기",
    "042700": "한미반도체",
    "000810": "삼성화재",
    "329180": "HD현대중공업",
    "267250": "HD현대",
    "010130": "고려아연",
    "015760": "한국전력",
    "034730": "SK",
    "096770": "SK이노베이션",
    "316140": "우리금융지주",
    "138040": "메리츠금융지주",
    "024110": "기업은행",
    "003550": "LG",
    "017670": "SK텔레콤",
    "030200": "KT",
    "259960": "크래프톤",
    "352820": "하이브",
    "018260": "삼성에스디에스",
    "009540": "HD한국조선해양",
    "011200": "HMM",
    "010140": "삼성중공업",
    "047050": "포스코인터내셔널",
    "042660": "한화오션",
    "010950": "S-Oil",
    "000720": "현대건설",
    "090430": "아모레퍼시픽",
    "161390": "한국타이어앤테크놀로지",
    "011170": "롯데케미칼",
    "271560": "오리온",
    "097950": "CJ제일제당",
    "251270": "넷마블",
    "035250": "강원랜드",
    "036570": "엔씨소프트",
    "326030": "SK바이오팜",
    "302440": "SK바이오사이언스",
    "128940": "한미약품",
}


def _clean_symbol_code(symbol):
    text = str(symbol or "").strip().upper()
    if ":" in text:
        text = text.split(":")[-1]
    if text.endswith(".KS") or text.endswith(".KQ"):
        text = text[:-3]
    return text


def symbol_display(symbol, exchange=None):
    """국장은 '한글 종목명(코드)', 그 외는 코드/티커 그대로 표시."""
    code = _clean_symbol_code(symbol)
    exchange_text = str(exchange or "").upper()
    is_krx = (
        exchange_text in {"KRX", "KOSPI", "KOSDAQ", "KOREA"}
        or (code.isdigit() and len(code) == 6)
    )
    if is_krx:
        name = KRX_SYMBOL_NAMES.get(code)
        return f"{name}({code})" if name else f"종목명 미등록({code})"
    return code


def exchange_only_label(exchange=None, market=None):
    """화면 카드에서는 전략명(1Q/별꽃타점)을 빼고 거래소만 표시."""
    market_text = str(market or "").upper()
    exchange_text = str(exchange or "").upper()
    if market_text == "KOREA" or exchange_text in {"KRX", "KOSPI", "KOSDAQ", "KOREA"}:
        return "KRX"
    if market_text == "US":
        return exchange_text or "US"
    return exchange_text or "COIN"


CATEGORY_DISPLAY_ORDER = {
    "KOREA_1Q": 0,
    "US_1Q": 1,
    "COIN": 2,
}


def _sort_performance_categories(data):
    """관리자·회원 화면 카테고리를 국장 → 미장 → 코인 순으로 정렬."""
    if not data:
        return data

    categories = list(data.get("categories") or [])
    categories.sort(
        key=lambda item: CATEGORY_DISPLAY_ORDER.get(
            item.get("category_key"),
            999,
        )
    )
    data = dict(data)
    data["categories"] = categories
    return data


def _entry_exit_timeframe_matrix(symbol_data, period_key="all"):
    """
    진입 시간봉 × 청산 시간봉 조합별 실제 성과 집계.

    기준:
    - 각 개별 진입(individual_results)을 해당 청산 고점의 시간봉과 연결한다.
    - 평균수익률: 조합별 개별 결과의 산술평균
    - 누적수익률: 조합별 개별 수익률의 단순합
    - 승률: 수익률이 0보다 큰 결과 비율
    - 향후 시장상태(정배열·역배열 등)는 context_breakdown에 확장 가능
    """
    start_at = _period_start(period_key)
    buckets = {}

    for cycle in symbol_data.get("completed_cycles") or []:
        if not _cycle_in_period(cycle, start_at):
            continue

        for result in cycle.get("exit_results") or []:
            exit_obj = result.get("exit") or {}
            exit_tf = exit_obj.get("timeframe") or "unknown"

            for individual in result.get("individual_results") or []:
                entry = individual.get("entry") or {}
                entry_tf = entry.get("timeframe") or "unknown"
                return_pct = individual.get("return_pct")
                if return_pct is None:
                    continue

                holding_minutes = individual.get("holding_minutes")
                key = (entry_tf, exit_tf)
                bucket = buckets.setdefault(
                    key,
                    {
                        "entry_timeframe": entry_tf,
                        "entry_minutes": TIMEFRAME_ORDER_MINUTES.get(
                            entry_tf, 999999
                        ),
                        "exit_timeframe": exit_tf,
                        "exit_minutes": TIMEFRAME_ORDER_MINUTES.get(
                            exit_tf, 999999
                        ),
                        "returns": [],
                        "holding_minutes": [],
                        # 향후 Pine 웹훅에 MA 상태 등이 들어오면 이곳에서 분류한다.
                        "context_breakdown": {},
                    },
                )
                bucket["returns"].append(float(return_pct))
                if holding_minutes is not None:
                    bucket["holding_minutes"].append(
                        float(holding_minutes)
                    )

    rows = []
    for bucket in buckets.values():
        returns = bucket.pop("returns")
        holdings = bucket.pop("holding_minutes")
        wins = [value for value in returns if value > 0]

        bucket.update(
            {
                "result_count": len(returns),
                "win_count": len(wins),
                "loss_count": len(returns) - len(wins),
                "win_rate_pct": (
                    len(wins) / len(returns) * 100
                    if returns else None
                ),
                "average_return_pct": (
                    sum(returns) / len(returns)
                    if returns else None
                ),
                "cumulative_return_pct": (
                    sum(returns) if returns else None
                ),
                "best_return_pct": (
                    max(returns) if returns else None
                ),
                "worst_return_pct": (
                    min(returns) if returns else None
                ),
                "average_holding_minutes": (
                    sum(holdings) / len(holdings)
                    if holdings else None
                ),
            }
        )
        rows.append(bucket)

    rows.sort(
        key=lambda item: (
            item["entry_minutes"],
            item["exit_minutes"],
        )
    )

    entry_timeframes = sorted(
        {row["entry_timeframe"] for row in rows},
        key=lambda tf: TIMEFRAME_ORDER_MINUTES.get(tf, 999999),
    )
    exit_timeframes = sorted(
        {row["exit_timeframe"] for row in rows},
        key=lambda tf: TIMEFRAME_ORDER_MINUTES.get(tf, 999999),
    )

    lookup = {
        (row["entry_timeframe"], row["exit_timeframe"]): row
        for row in rows
    }

    matrix = []
    for entry_tf in entry_timeframes:
        cells = []
        for exit_tf in exit_timeframes:
            cells.append(
                {
                    "exit_timeframe": exit_tf,
                    "stat": lookup.get((entry_tf, exit_tf)),
                }
            )
        matrix.append(
            {
                "entry_timeframe": entry_tf,
                "cells": cells,
            }
        )

    return {
        "rows": rows,
        "matrix": matrix,
        "entry_timeframes": entry_timeframes,
        "exit_timeframes": exit_timeframes,
        "has_results": bool(rows),
        "result_count": sum(row["result_count"] for row in rows),
    }



def _build_member_chart_data(selected_category_data, period_key="all"):
    """
    회원용 시각화 데이터.
    누적곡선은 각 청산 후보의 전체 분할 수익률을 시간순으로 단순 합산한다.
    복리·동시 보유·자금배분은 반영하지 않는다.
    """
    start_at = _period_start(period_key)
    points = []
    timeframe_map = {}
    symbol_rows = []

    if not selected_category_data:
        return {
            "curve": [],
            "curve_polyline": "",
            "curve_min": 0.0,
            "curve_max": 0.0,
            "final_cumulative_pct": None,
            "timeframes": [],
            "symbols": [],
            "max_abs_timeframe_return": 1.0,
            "max_symbol_return": 1.0,
        }

    for symbol_data in selected_category_data.get("symbols") or []:
        stats = _member_symbol_statistics(symbol_data, period_key)
        if stats.get("has_results"):
            symbol_rows.append(
                {
                    "symbol": symbol_data.get("symbol"),
                    "average_return_pct": stats.get("average_return_pct"),
                    "best_return_pct": stats.get("best_return_pct"),
                    "win_rate_pct": stats.get("win_rate_pct"),
                    "result_count": stats.get("result_count", 0),
                }
            )

        for cycle in symbol_data.get("completed_cycles") or []:
            if not _cycle_in_period(cycle, start_at):
                continue

            for result in cycle.get("exit_results") or []:
                exit_obj = result.get("exit") or {}
                exit_time = _parse_iso_datetime(exit_obj.get("time"))
                if not exit_time:
                    continue

                representative_return = result.get("all_split_return_pct")
                if representative_return is None:
                    representative_return = result.get("max_timeframe_return_pct")

                if representative_return is not None:
                    points.append(
                        {
                            "time": exit_time,
                            "return_pct": float(representative_return),
                            "symbol": symbol_data.get("symbol"),
                            "exit_timeframe": exit_obj.get("timeframe") or "unknown",
                        }
                    )

                for tf_result in result.get("timeframe_split_results") or []:
                    value = tf_result.get("return_pct")
                    if value is None:
                        continue

                    tf = tf_result.get("timeframe") or "unknown"
                    bucket = timeframe_map.setdefault(
                        tf,
                        {
                            "timeframe": tf,
                            "timeframe_minutes": TIMEFRAME_ORDER_MINUTES.get(tf, 999999),
                            "returns": [],
                        },
                    )
                    bucket["returns"].append(float(value))

    points.sort(key=lambda item: item["time"])

    cumulative = 0.0
    curve = []
    for index, item in enumerate(points):
        cumulative += item["return_pct"]
        curve.append(
            {
                "index": index,
                "time": item["time"],
                "time_text": item["time"].astimezone(timezone.utc).strftime("%m-%d %H:%M"),
                "return_pct": item["return_pct"],
                "cumulative_pct": cumulative,
                "symbol": item["symbol"],
                "exit_timeframe": item["exit_timeframe"],
            }
        )

    curve_values = [0.0] + [item["cumulative_pct"] for item in curve]
    curve_min = min(curve_values) if curve_values else 0.0
    curve_max = max(curve_values) if curve_values else 0.0

    width = 1000.0
    height = 260.0
    padding_x = 30.0
    padding_y = 24.0
    chart_w = width - padding_x * 2
    chart_h = height - padding_y * 2
    value_range = curve_max - curve_min
    if value_range == 0:
        value_range = 1.0

    curve_polyline_points = []
    total_positions = max(len(curve), 1)
    for idx, item in enumerate(curve):
        x = padding_x + (idx / max(total_positions - 1, 1)) * chart_w
        y = padding_y + ((curve_max - item["cumulative_pct"]) / value_range) * chart_h
        curve_polyline_points.append(f"{x:.2f},{y:.2f}")

    timeframe_rows = []
    for _, bucket in sorted(
        timeframe_map.items(),
        key=lambda item: item[1]["timeframe_minutes"],
    ):
        values = bucket["returns"]
        wins = [value for value in values if value > 0]
        timeframe_rows.append(
            {
                "timeframe": bucket["timeframe"],
                "timeframe_minutes": bucket["timeframe_minutes"],
                "result_count": len(values),
                "average_return_pct": sum(values) / len(values) if values else None,
                "best_return_pct": max(values) if values else None,
                "worst_return_pct": min(values) if values else None,
                "win_rate_pct": len(wins) / len(values) * 100 if values else None,
            }
        )

    symbol_rows.sort(
        key=lambda item: (
            item["average_return_pct"]
            if item["average_return_pct"] is not None
            else float("-inf")
        ),
        reverse=True,
    )

    tf_values = [
        abs(item["average_return_pct"])
        for item in timeframe_rows
        if item["average_return_pct"] is not None
    ]
    symbol_values = [
        abs(item["average_return_pct"])
        for item in symbol_rows
        if item["average_return_pct"] is not None
    ]

    return {
        "curve": curve,
        "curve_polyline": " ".join(curve_polyline_points),
        "curve_min": curve_min,
        "curve_max": curve_max,
        "final_cumulative_pct": cumulative if curve else None,
        "timeframes": timeframe_rows,
        "symbols": symbol_rows,
        "max_abs_timeframe_return": max(tf_values) if tf_values else 1.0,
        "max_symbol_return": max(symbol_values) if symbol_values else 1.0,
    }


# =========================================================
# 회원용 / 관리자용 성과 화면 접근 제어
# =========================================================
PERFORMANCE_ADMIN_USERNAME = os.getenv(
    "PERFORMANCE_ADMIN_USERNAME", "admin"
).strip()
PERFORMANCE_ADMIN_PASSWORD = os.getenv(
    "PERFORMANCE_ADMIN_PASSWORD", ""
).strip()
PERFORMANCE_MEMBER_PASSWORD = os.getenv(
    "PERFORMANCE_MEMBER_PASSWORD", ""
).strip()


def _safe_equals(left: str, right: str) -> bool:
    return hmac.compare_digest(
        str(left or "").encode("utf-8"),
        str(right or "").encode("utf-8"),
    )


def _performance_role() -> str | None:
    role = session.get("performance_role")
    return role if role in {"admin", "member"} else None


def admin_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if _performance_role() != "admin":
            return redirect(
                url_for(
                    "performance_login",
                    role="admin",
                    next=request.full_path.rstrip("?"),
                )
            )
        return view_func(*args, **kwargs)
    return wrapped


def member_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if _performance_role() not in {"admin", "member"}:
            return redirect(
                url_for(
                    "performance_login",
                    role="member",
                    next=request.full_path.rstrip("?"),
                )
            )
        return view_func(*args, **kwargs)
    return wrapped


def _safe_next_url(value: str | None, fallback: str) -> str:
    value = (value or "").strip()
    if value.startswith("/performance/") and not value.startswith("//"):
        return value
    return fallback


@app.route("/performance/login", methods=["GET", "POST"])
def performance_login():
    requested_role = request.args.get("role", "member").strip().lower()
    if requested_role not in {"member", "admin"}:
        requested_role = "member"

    error = ""
    if request.method == "POST":
        requested_role = request.form.get("role", requested_role).strip().lower()
        password = request.form.get("password", "")
        username = request.form.get("username", "").strip()

        if requested_role == "admin":
            configured = bool(
                PERFORMANCE_ADMIN_USERNAME
                and PERFORMANCE_ADMIN_PASSWORD
            )
            valid = (
                configured
                and _safe_equals(username, PERFORMANCE_ADMIN_USERNAME)
                and _safe_equals(password, PERFORMANCE_ADMIN_PASSWORD)
            )
        else:
            configured = bool(PERFORMANCE_MEMBER_PASSWORD)
            valid = (
                configured
                and _safe_equals(password, PERFORMANCE_MEMBER_PASSWORD)
            )

        if valid:
            session.clear()
            session.permanent = True
            session["performance_role"] = requested_role
            target = _safe_next_url(
                request.form.get("next"),
                (
                    "/performance/dashboard"
                    if requested_role == "admin"
                    else "/performance/member"
                ),
            )
            return redirect(target)

        error = (
            "로그인 정보가 올바르지 않습니다."
            if configured
            else "Render 환경변수에 비밀번호가 아직 등록되지 않았습니다."
        )

    next_url = _safe_next_url(
        request.args.get("next"),
        (
            "/performance/dashboard"
            if requested_role == "admin"
            else "/performance/member"
        ),
    )

    return render_template_string(
        """
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{"관리자" if role == "admin" else "회원"}} 로그인</title>
<style>
body{margin:0;background:#0d0d0f;color:#f4f4f4;font-family:Arial,"Noto Sans KR",sans-serif}
.wrap{max-width:430px;margin:9vh auto;padding:22px}
.box{background:#1b1b1e;border:1px solid #38383c;border-radius:18px;padding:26px}
h1{font-size:28px;margin:0 0 8px}
p{color:#aaa;margin:0 0 22px}
label{display:block;color:#8bd0ff;font-weight:bold;margin:15px 0 7px}
input{width:100%;box-sizing:border-box;padding:13px;border-radius:10px;border:1px solid #444;background:#111;color:#fff;font-size:16px}
button{width:100%;margin-top:20px;padding:13px;border:0;border-radius:10px;background:#2c91c9;color:#fff;font-size:17px;font-weight:bold;cursor:pointer}
.error{background:#431f25;color:#ffb4bd;border-radius:9px;padding:11px;margin-bottom:12px}
.switch{text-align:center;margin-top:18px}.switch a{color:#70cfff}
</style>
</head>
<body>
<div class="wrap"><div class="box">
<h1>{{"관리자 전용 로그인" if role == "admin" else "회원 전용 로그인"}}</h1>
<p>
{% if role == "admin" %}
분석 실행, 원본 신호, 사이클 및 관리자 상세 자료에 접근합니다.
{% else %}
회원에게 공개된 성과 요약 화면에 접근합니다.
{% endif %}
</p>
{% if error %}<div class="error">{{error}}</div>{% endif %}
<form method="post">
<input type="hidden" name="role" value="{{role}}">
<input type="hidden" name="next" value="{{next_url}}">
{% if role == "admin" %}
<label>관리자 아이디</label>
<input name="username" autocomplete="username" required>
{% endif %}
<label>비밀번호</label>
<input type="password" name="password" autocomplete="current-password" required>
<button type="submit">로그인</button>
</form>
<div class="switch">
{% if role == "admin" %}
<a href="/performance/login?role=member">회원 로그인으로</a>
{% else %}
<a href="/performance/login?role=admin">관리자 로그인으로</a>
{% endif %}
</div>
</div></div>
</body>
</html>
        """,
        role=requested_role,
        next_url=next_url,
        error=error,
    )


@app.get("/performance/logout")
def performance_logout():
    session.clear()
    return redirect(url_for("performance_login", role="member"))


@app.get("/performance")
def performance_home():
    if _performance_role() == "admin":
        return redirect("/performance/dashboard")
    if _performance_role() == "member":
        return redirect("/performance/member")
    return redirect("/performance/login?role=member")


@app.get("/performance/health")
@admin_required
def performance_health():
    try:
        result = health_summary()
        return jsonify(result), (200 if result.get("ok") else 503)
    except Exception as e:
        log.exception("Performance health check failed")
        return jsonify({"ok": False, "database": "error", "error": str(e)}), 503



# --- 최근 저장 신호 확인 ---
@app.get("/performance/latest")
@admin_required
def performance_latest():
    try:
        try:
            limit = int(request.args.get("limit", "20"))
        except ValueError:
            limit = 20
        rows = latest_signals(limit)
        return jsonify({"ok": True, "count": len(rows), "signals": rows}), 200
    except Exception as e:
        log.exception("Performance latest signals failed")
        return jsonify({"ok": False, "error": str(e)}), 500


# --- 성과 분석 엔진: 각 저점 진입 × 이후 모든 고점 청산 ---
@app.route("/performance/analyze", methods=["GET", "POST"])
@admin_required
def performance_analyze():
    try:
        return jsonify(rebuild_individual_pairs()), 200
    except Exception as e:
        log.exception("Performance analysis failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/performance/analysis/summary")
def performance_analysis_summary():
    try:
        return jsonify(analysis_summary()), 200
    except Exception as e:
        log.exception("Performance analysis summary failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/performance/analysis/latest")
def performance_analysis_latest():
    try:
        try:
            limit = int(request.args.get("limit", "50"))
        except ValueError:
            limit = 50
        rows = latest_analysis_pairs(limit)
        return jsonify({"ok": True, "count": len(rows), "pairs": rows}), 200
    except Exception as e:
        log.exception("Performance latest analysis failed")
        return jsonify({"ok": False, "error": str(e)}), 500



@app.get("/performance/member")
@member_required
def performance_member():
    try:
        try:
            limit = int(request.args.get("limit", "100"))
        except ValueError:
            limit = 100

        selected_category = (
            request.args.get("category", "KOREA_1Q")
            .strip()
            .upper()
        )
        allowed_categories = {"COIN", "KOREA_1Q", "US_1Q"}
        if selected_category not in allowed_categories:
            selected_category = "KOREA_1Q"

        data = _sort_performance_categories(visual_cycle_data(limit))
        selected = next(
            (
                category
                for category in data["categories"]
                if category["category_key"] == selected_category
            ),
            None,
        )

        period_key = request.args.get("period", "all").strip().lower()
        if period_key not in {"today", "7d", "30d", "all"}:
            period_key = "all"

        ranked_symbols = []
        average_ranking = []
        best_ranking = []
        win_rate_ranking = []
        chart_scale = 1.0
        market_group_stats = []
        market_stats = {
            "has_results": False,
            "average_return_pct": None,
            "best_return_pct": None,
            "win_rate_pct": None,
            "average_holding_minutes": None,
            "result_symbol_count": 0,
            "result_count": 0,
            "best_symbol": None,
            "best_symbol_exchange": None,
        }

        if selected:
            category_market = {
                "KOREA_1Q": "KOREA",
                "US_1Q": "US",
                "COIN": "COIN",
            }
            market_analysis = group_analysis_market_data(
                category_market[selected_category]
            )
            analysis_by_symbol = market_analysis.get(
                "symbol_data", {}
            )

            symbol_stats = []
            for item in selected["symbols"]:
                analysis = analysis_by_symbol.get(
                    item.get("symbol"),
                    {
                        "positions": [],
                        "performance_summary": [],
                        "occurrence_stats": [],
                    },
                )
                stats = _member_group_engine_statistics(
                    analysis,
                    period_key,
                )
                stats["entry_groups"] = _group_entry_timeframe_stats(
                    selected_category,
                    stats.get("entry_timeframes") or [],
                )
                enriched = dict(item)
                enriched["member_stats"] = stats
                symbol_stats.append(enriched)

            selected = dict(selected)
            selected["symbols"] = symbol_stats

            ranked_symbols = [
                item for item in selected["symbols"]
                if item["member_stats"]["has_results"]
            ]

            average_ranking = sorted(
                ranked_symbols,
                key=lambda item: (
                    item["member_stats"]["average_return_pct"]
                    if item["member_stats"]["average_return_pct"] is not None
                    else float("-inf")
                ),
                reverse=True,
            )[:5]

            best_ranking = sorted(
                ranked_symbols,
                key=lambda item: (
                    item["member_stats"]["best_return_pct"]
                    if item["member_stats"]["best_return_pct"] is not None
                    else float("-inf")
                ),
                reverse=True,
            )[:5]

            win_rate_ranking = sorted(
                ranked_symbols,
                key=lambda item: (
                    item["member_stats"]["win_rate_pct"]
                    if item["member_stats"]["win_rate_pct"] is not None
                    else float("-inf"),
                    item["member_stats"]["result_count"],
                ),
                reverse=True,
            )[:5]

            chart_values = [
                abs(item["member_stats"]["average_return_pct"])
                for item in ranked_symbols
                if item["member_stats"]["average_return_pct"] is not None
            ]
            chart_scale = max(chart_values) if chart_values else 1.0

            all_market_returns = []
            market_holding = []
            for item in ranked_symbols:
                stats = item["member_stats"]
                if stats["average_return_pct"] is not None:
                    all_market_returns.append(stats["average_return_pct"])
                if stats["average_holding_minutes"] is not None:
                    market_holding.append(stats["average_holding_minutes"])

            total_results = sum(
                item["member_stats"]["result_count"]
                for item in ranked_symbols
            )
            weighted_wins = sum(
                (
                    item["member_stats"]["win_rate_pct"] / 100
                    * item["member_stats"]["result_count"]
                )
                for item in ranked_symbols
                if item["member_stats"]["win_rate_pct"] is not None
            )

            market_group_stats = _aggregate_market_group_stats(
                ranked_symbols, selected_category
            )
            market_stats = {
                "has_results": bool(ranked_symbols),
                "average_return_pct": (
                    sum(all_market_returns) / len(all_market_returns)
                    if all_market_returns else None
                ),
                "best_return_pct": (
                    max(
                        item["member_stats"]["best_return_pct"]
                        for item in ranked_symbols
                        if item["member_stats"]["best_return_pct"] is not None
                    )
                    if ranked_symbols else None
                ),
                "win_rate_pct": (
                    weighted_wins / total_results * 100
                    if total_results else None
                ),
                "average_holding_minutes": (
                    sum(market_holding) / len(market_holding)
                    if market_holding else None
                ),
                "result_symbol_count": len(ranked_symbols),
                "result_count": total_results,
                "best_symbol": (
                    max(
                        ranked_symbols,
                        key=lambda item: (
                            item["member_stats"]["best_return_pct"]
                            if item["member_stats"]["best_return_pct"] is not None
                            else float("-inf")
                        ),
                    )["symbol"]
                    if ranked_symbols else None
                ),
                "best_symbol_exchange": (
                    max(
                        ranked_symbols,
                        key=lambda item: (
                            item["member_stats"]["best_return_pct"]
                            if item["member_stats"]["best_return_pct"] is not None
                            else float("-inf")
                        ),
                    ).get("exchange")
                    if ranked_symbols else None
                ),
            }

        return render_template_string(
            """
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>회원 성과 리포트</title>
<style>
:root{--bg:#0e0e0f;--card:#1b1b1d;--line:#353539;--blue:#7ed2ff;--green:#55e69a;--yellow:#ffc857;--red:#ff7878}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:#f5f5f5;font-family:Arial,"Noto Sans KR",sans-serif;padding:20px}
.header{display:flex;justify-content:space-between;align-items:center;gap:15px;flex-wrap:wrap;margin-bottom:18px}
h1{margin:0;font-size:32px}.logout{color:#aaa}
.tabs{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px}
.tabs a{padding:10px 15px;border:1px solid #404045;background:#242427;color:#75ceff;border-radius:999px;text-decoration:none}
.tabs a.active{background:#14405b;border-color:#67c8ff;color:#fff;font-weight:bold}
.market{background:#171719;border:1px solid #303034;border-left:5px solid var(--blue);border-radius:14px;padding:18px;margin-bottom:14px}
.market-head{display:flex;justify-content:space-between;gap:15px;align-items:center;flex-wrap:wrap}
.market h2{font-size:27px;margin:0}.badges{display:flex;gap:8px;flex-wrap:wrap}
.badge{background:#29292c;border-radius:999px;padding:8px 12px}
.summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:14px 0 20px}
.metric,.symbol{display:block;color:#f5f5f5;text-decoration:none;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px}
.title{color:var(--blue);font-weight:bold;margin-bottom:8px}.value{font-size:25px;font-weight:bold}
.pos{color:var(--green)}.warn{color:var(--yellow)}.muted{color:#aaa}.metric-sub{margin-top:8px;color:#aaa;font-size:13px}.group-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;margin-top:14px}.group-card{background:#141416;border:1px solid #303035;border-radius:12px;padding:13px}.group-card.life{border-color:#8c6b20;box-shadow:0 0 0 1px rgba(255,200,87,.12)}.group-title{display:flex;justify-content:space-between;gap:8px;align-items:center;color:var(--blue);font-size:19px;font-weight:bold;margin-bottom:10px}.group-card.life .group-title{color:var(--yellow)}.tf-row{display:grid;grid-template-columns:55px 55px 70px 85px 85px 80px;gap:7px;padding:8px 0;border-bottom:1px solid #29292d;font-size:13px;align-items:center}.tf-row:last-child{border-bottom:0}.tf-head{color:#aaa;font-size:12px}.trust-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:0 0 20px}.trust-card{background:#171719;border:1px solid #353539;border-radius:14px;padding:16px}.trust-card.life{border-color:#9d7b1f;background:linear-gradient(145deg,#211b0b,#171719)}.trust-title{font-size:20px;font-weight:800;color:var(--blue);display:flex;justify-content:space-between}.trust-card.life .trust-title{color:var(--yellow)}.trust-value{font-size:28px;font-weight:900;color:var(--green);margin:12px 0}.trust-meta{display:grid;grid-template-columns:1fr 1fr;gap:7px;color:#bbb;font-size:13px}.life-hero{border:1px solid #92751d;background:linear-gradient(145deg,#211c0d,#151517);border-radius:17px;padding:20px;margin-bottom:20px}.life-hero h3{margin:0;color:var(--yellow);font-size:25px}.cycle-flow{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:12px}.flow-step{background:#101012;border:1px solid #303035;border-radius:12px;padding:12px}.flow-step strong{display:block;color:var(--blue);margin-bottom:7px}.flow-step.adverse{border-color:#6b3434}.flow-step.adverse strong{color:var(--red)}.flow-step.exit{border-color:#285b43}.flow-step.exit strong{color:var(--green)}.status-DUE{color:#ffcf55}.status-NEAR{color:#7ed2ff}.status-EARLY{color:#aaa}.status-WAIT{color:#888}
.ranking-wrap{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:13px;margin:0 0 20px}
.ranking{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px;min-width:0}
.ranking h3{margin:0 0 12px;color:var(--blue);font-size:18px}
.rank-row{display:grid;grid-template-columns:28px minmax(0,1fr) auto;gap:8px;align-items:center;padding:9px 0;border-bottom:1px solid #2c2c30}
.rank-row:last-child{border-bottom:0}
.rank-no{width:25px;height:25px;border-radius:50%;background:#2b2b2f;display:flex;align-items:center;justify-content:center;font-size:12px}
.rank-symbol{font-weight:bold;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.rank-value{font-weight:bold;color:var(--green);white-space:nowrap}
.chart-section{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;margin-bottom:20px}
.chart-section h3{margin:0 0 14px;color:var(--blue)}
.bar-row{display:grid;grid-template-columns:110px minmax(100px,1fr) 75px;gap:10px;align-items:center;margin:11px 0}
.bar-name{font-weight:bold;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-track{height:18px;border-radius:999px;background:#101012;overflow:hidden;border:1px solid #2d2d31}
.bar-fill{height:100%;min-width:3px;border-radius:999px;background:linear-gradient(90deg,#2495c7,#55e69a)}
.bar-fill.negbar{background:linear-gradient(90deg,#a63b4a,#ff7878)}
.bar-value{text-align:right;font-weight:bold;white-space:nowrap}
.symbols{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:13px}
.symbol{min-width:0}
.symbol h3{font-size:24px;margin:0 0 13px;overflow-wrap:anywhere}
.values{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px}
.mini{background:#121214;border-radius:9px;padding:10px;min-width:0;overflow:hidden}
.mini span{display:block;color:#aaa;font-size:12px;margin-bottom:5px;overflow-wrap:anywhere}
.mini b{font-size:18px;display:block;overflow-wrap:anywhere}
.notice{background:#1b1b1d;border:1px solid var(--line);border-radius:14px;padding:22px;color:#aaa}
.disclaimer{margin-top:22px;color:#777;font-size:13px;line-height:1.5}
@media(max-width:1100px){.ranking-wrap{grid-template-columns:1fr}.summary{grid-template-columns:repeat(2,1fr)}}
@media(max-width:760px){.values{grid-template-columns:repeat(2,minmax(0,1fr))}.bar-row{grid-template-columns:85px minmax(80px,1fr) 65px}}
@media(max-width:560px){body{padding:11px}.summary{grid-template-columns:1fr}h1{font-size:26px}.symbols{grid-template-columns:1fr}.bar-row{grid-template-columns:72px minmax(60px,1fr) 58px;font-size:13px}}
</style>
</head>
<body>
<div class="header">
<div>
<h1>회원용 성과 리포트</h1>
<div class="muted">공개용 요약 화면</div>
</div>
<div style="display:flex;gap:12px;align-items:center">
<a class="logout" href="/performance/member/image-preview?category={{selected_category}}&period={{period_key}}">홍보 이미지 미리보기</a>
<a class="logout" href="/performance/member/charts?category={{selected_category}}&period={{period_key}}">성과 그래프</a>
<a class="logout" href="/performance/logout">로그아웃</a>
</div>
</div>

<div class="tabs">
{% for category in data.categories %}
<a class="{{'active' if category.category_key == selected_category else ''}}"
href="/performance/member?category={{category.category_key}}&period={{period_key}}">
{{category.category_label}} · {{category.symbol_count}}종목
</a>
{% endfor %}
</div>

<div class="tabs" style="margin-top:-6px">
<a class="{{'active' if period_key == 'today' else ''}}"
href="/performance/member?category={{selected_category}}&period=today">오늘</a>
<a class="{{'active' if period_key == '7d' else ''}}"
href="/performance/member?category={{selected_category}}&period=7d">최근 7일</a>
<a class="{{'active' if period_key == '30d' else ''}}"
href="/performance/member?category={{selected_category}}&period=30d">최근 30일</a>
<a class="{{'active' if period_key == 'all' else ''}}"
href="/performance/member?category={{selected_category}}&period=all">전체</a>
</div>

{% if selected %}
<div class="market">
<div class="market-head">
<h2>{{selected.category_label}}</h2>
<div class="badges">
<span class="badge">종목 {{selected.symbol_count}}</span>
<span class="badge pos">완료 {{selected.completed_cycle_count}}</span>
<span class="badge warn">청산 대기 {{selected.open_low_count}}</span>
</div>
</div>
</div>

<div class="summary">
<div class="metric">
<div class="title">평균 수익률</div>
<div class="value {{'pos' if market_stats.average_return_pct is not none and market_stats.average_return_pct >= 0 else 'muted'}}">
{% if market_stats.average_return_pct is not none %}
{{'%.2f'|format(market_stats.average_return_pct)}}%
{% else %}결과 대기{% endif %}
</div>
<div class="metric-sub">
{% if market_stats.result_symbol_count %}
{{market_stats.result_symbol_count}}개 종목의 결과 평균
{% else %}완료 종목 없음{% endif %}
</div>
</div>
<div class="metric">
<div class="title">최고 수익률</div>
<div class="value {{'pos' if market_stats.best_return_pct is not none and market_stats.best_return_pct >= 0 else 'muted'}}">
{% if market_stats.best_return_pct is not none %}
{{'%.2f'|format(market_stats.best_return_pct)}}%
{% else %}결과 대기{% endif %}
</div>
<div class="metric-sub">
{% if market_stats.best_symbol %}
{{symbol_display(market_stats.best_symbol, market_stats.best_symbol_exchange)}} 최대
{% set best_item = (best_ranking[0] if best_ranking else none) %}
{% if best_item and best_item.member_stats.best_detail %}
<br>{{best_item.member_stats.best_detail.entry_timeframe}} 진입 → {{best_item.member_stats.best_detail.exit_timeframe}} 청산
{% endif %}
{% else %}완료 종목 없음{% endif %}
</div>
</div>
<div class="metric">
<div class="title">승률</div>
<div class="value {{'pos' if market_stats.win_rate_pct is not none and market_stats.win_rate_pct >= 50 else 'muted'}}">
{% if market_stats.win_rate_pct is not none %}
{{'%.1f'|format(market_stats.win_rate_pct)}}%
{% else %}결과 대기{% endif %}
</div>
</div>
<div class="metric">
<div class="title">평균 보유시간</div>
<div class="value">
{% if market_stats.average_holding_minutes is not none %}
{{'%.0f'|format(market_stats.average_holding_minutes)}}분
{% else %}결과 대기{% endif %}
</div>
</div>
</div>

{% if selected.symbol_count %}

{% if ranked_symbols %}
<div class="trust-grid">
{% for group in market_group_stats %}
<div class="trust-card {{'life' if group.group_key == 'LIFE' else ''}}">
<div class="trust-title"><span>{{group.group_label}}</span><span>{{group.cycles}}사이클</span></div>
<div class="trust-value">{% if group.average_return_pct is not none %}{{'%.2f'|format(group.average_return_pct)}}%{% else %}-{% endif %}</div>
<div class="trust-meta">
<span>승률 {{'%.1f'|format(group.win_rate_pct)}}%</span>
<span>보유 {{format_minutes_compact(group.average_holding_minutes)}}</span>
<span>신호 기준 역행 {% if group.average_signal_adverse_pct is not none %}{{'%.2f'|format(group.average_signal_adverse_pct)}}%{% else %}-{% endif %}</span>
<span>회복 {{format_minutes_compact(group.average_recovery_minutes)}}</span>
</div></div>
{% endfor %}
</div>
{% set life = market_group_stats|selectattr('group_key','equalto','LIFE')|list %}
<div class="life-hero">
<h3>★ 인생타점 집중 보기</h3>
{% if life %}
<p>평균 {{'%.2f'|format(life[0].average_return_pct)}}% · 승률 {{'%.1f'|format(life[0].win_rate_pct)}}% · 평균 보유 {{format_minutes_compact(life[0].average_holding_minutes)}} · 신호 기준 평균 최대 역행 {{'%.2f'|format(life[0].average_signal_adverse_pct)}}%</p>
{% else %}<p class="muted">인생타점 완료 사이클이 쌓이면 이곳에 최우선으로 표시됩니다.</p>{% endif %}
</div>
<div class="ranking-wrap">
<div class="ranking">
<h3>평균수익률 TOP 5</h3>
{% for s in average_ranking %}
<div class="rank-row">
<span class="rank-no">{{loop.index}}</span>
<span class="rank-symbol">{{symbol_display(s.symbol, s.exchange)}}</span>
<span class="rank-value">{{'%.2f'|format(s.member_stats.average_return_pct)}}%</span>
</div>
{% endfor %}
</div>

<div class="ranking">
<h3>최고수익률 TOP 5</h3>
{% for s in best_ranking %}
<div class="rank-row">
<span class="rank-no">{{loop.index}}</span>
<span class="rank-symbol">{{symbol_display(s.symbol, s.exchange)}}</span>
<span class="rank-value">{{'%.2f'|format(s.member_stats.best_return_pct)}}%</span>
</div>
{% endfor %}
</div>

<div class="ranking">
<h3>승률 TOP 5</h3>
{% for s in win_rate_ranking %}
<div class="rank-row">
<span class="rank-no">{{loop.index}}</span>
<span class="rank-symbol">{{symbol_display(s.symbol, s.exchange)}}</span>
<span class="rank-value">{{'%.1f'|format(s.member_stats.win_rate_pct)}}%</span>
</div>
{% endfor %}
</div>
</div>

<div class="chart-section">
<h3>종목별 평균수익률 비교</h3>
{% for s in ranked_symbols|sort(attribute='symbol') %}
{% set avg = s.member_stats.average_return_pct %}
<div class="bar-row">
<div class="bar-name">{{symbol_display(s.symbol, s.exchange)}}</div>
<div class="bar-track">
<div class="bar-fill {{'negbar' if avg < 0 else ''}}"
style="width:{{(avg|abs / chart_scale * 100) if chart_scale else 0}}%"></div>
</div>
<div class="bar-value {{'pos' if avg >= 0 else ''}}">{{'%.2f'|format(avg)}}%</div>
</div>
{% endfor %}
</div>



<details class="chart-section trust-method">
<summary style="cursor:pointer;font-size:19px;font-weight:bold;color:var(--blue)">통계 계산 기준·신뢰 안내</summary>
<div style="margin-top:12px;line-height:1.75;color:#d7d7da">
<div>• <b>완료 사이클 1회</b>: 첫 진입부터 최초 유효 고점 청산까지입니다. 청산 후 다음 LOW부터 새 사이클입니다.</div>
<div>• <b>평균수익률</b>: 사이클마다 허용된 청산 시간봉 결과의 평균을 구한 뒤, 완료 사이클끼리 평균합니다.</div>
<div>• <b>최고수익률</b>: 저장된 청산 시간봉 비교 결과 중 최고값이며, 해당 종목·진입 시간봉·청산 시간봉을 함께 표시합니다.</div>
<div>• <b>신호 기준 최대 역행</b>: 전체 캔들 저가가 아니라 청산 전 저장된 LOW 신호 가격 중 최저값 기준입니다.</div>
<div>• 수수료·슬리피지·세금·실제 주문 체결 오차는 반영되지 않을 수 있습니다.</div>
<div>• 발생 주기의 ‘근접/초과’는 과거 평균 간격 비교이며 다음 신호를 보장하지 않습니다.</div>
</div>
</details>

<a class="chart-section promo-preview-link"
   href="/performance/member/image-preview?category={{selected_category}}&period={{period_key}}"
   style="display:block;text-decoration:none;color:#fff;border-color:#6f5a20;background:linear-gradient(145deg,#211c0e,#171719)">
<div style="display:flex;justify-content:space-between;gap:16px;align-items:center;flex-wrap:wrap">
<div>
<div style="color:var(--yellow);font-size:15px;font-weight:bold">홍보 이미지 자동 생성 미리보기</div>
<div style="font-size:25px;font-weight:bold;margin-top:7px">결과로 증명하는 타점 알람</div>
<div class="muted" style="margin-top:7px">현재 수익률·승률·보유기간이 들어간 이미지형 화면을 바로 확인합니다.</div>
</div>
<div style="font-size:42px;font-weight:900;color:var(--green)">
{% if market_stats.best_return_pct is not none %}+{{'%.2f'|format(market_stats.best_return_pct)}}%{% else %}미리보기{% endif %}
</div>
</div>
<div style="margin-top:16px;height:90px;border-radius:12px;background:linear-gradient(165deg,transparent 44%,rgba(86,230,155,.30) 45%,rgba(86,230,155,.30) 49%,transparent 50%),#111113;position:relative;overflow:hidden">
<div style="position:absolute;left:10%;top:63%;width:12px;height:12px;border-radius:50%;background:#ffd24d"></div>
<div style="position:absolute;left:35%;top:50%;width:12px;height:12px;border-radius:50%;background:#ffd24d"></div>
<div style="position:absolute;left:60%;top:36%;width:12px;height:12px;border-radius:50%;background:#ffd24d"></div>
<div style="position:absolute;right:10%;top:18%;width:14px;height:14px;border-radius:50%;background:#56e69b"></div>
</div>
<div style="margin-top:10px;color:var(--blue);font-weight:bold">클릭해서 전체 이미지 디자인 보기 →</div>
</a>


<details class="chart-section">
<summary style="cursor:pointer;font-size:20px;font-weight:bold;color:var(--blue)">
포지션별·세부 시간봉별 수익률 보기
</summary>
<div style="margin-top:14px">
{% for s in ranked_symbols|sort(attribute='symbol') %}
<details style="margin-bottom:10px;background:#111113;border-radius:11px;padding:12px">
<summary style="cursor:pointer;font-weight:bold;font-size:17px">{{symbol_display(s.symbol, s.exchange)}}</summary>
<div class="group-grid">
{% for group in s.member_stats.entry_groups %}
<div class="group-card {{'life' if group.group_key == 'LIFE' else ''}}">
<div class="group-title">
<span>{{group.group_label}}</span>
<span class="{{'pos' if group.average_return_pct is not none and group.average_return_pct >= 0 else 'muted'}}">
{% if group.average_return_pct is not none %}평균 {{'%.2f'|format(group.average_return_pct)}}%{% else %}결과 대기{% endif %}
</span>
</div>
{% if group.details %}
<div class="tf-row tf-head">
<span>시간봉</span><span>사이클</span><span>승률</span><span>평균수익</span><span>최고수익</span><span>보유</span>
</div>
{% for stat in group.details %}
<div class="tf-row">
<span>{{stat.timeframe}}</span>
<span>{{stat.result_count}}</span>
<span>{% if stat.win_rate_pct is not none %}{{'%.1f'|format(stat.win_rate_pct)}}%{% else %}-{% endif %}</span>
<span class="{{'pos' if stat.average_return_pct is not none and stat.average_return_pct >= 0 else ''}}">
{% if stat.average_return_pct is not none %}{{'%.2f'|format(stat.average_return_pct)}}%{% else %}-{% endif %}
</span>
<span class="{{'pos' if stat.best_return_pct is not none and stat.best_return_pct >= 0 else ''}}">
{% if stat.best_return_pct is not none %}{{'%.2f'|format(stat.best_return_pct)}}%{% else %}-{% endif %}
</span>
<span>{% if stat.average_holding_minutes is not none %}{{'%.0f'|format(stat.average_holding_minutes)}}분{% else %}-{% endif %}</span>
</div>
{% endfor %}
{% else %}
<div class="muted">해당 포지션 결과 대기</div>
{% endif %}
</div>
{% endfor %}
</div>
</details>
{% endfor %}
</div>
</details>

<details class="chart-section">
<summary style="cursor:pointer;font-size:20px;font-weight:bold;color:var(--yellow)">
인생타점 성과만 별도로 보기
</summary>
<div style="margin-top:14px">
{% set life_ns = namespace(has_any=false) %}
{% for s in ranked_symbols|sort(attribute='symbol') %}
{% for group in s.member_stats.entry_groups %}
{% if group.group_key == 'LIFE' and group.has_results %}
{% set life_ns.has_any = true %}
<div class="group-card life" style="margin-bottom:10px">
<div class="group-title">
<span>{{symbol_display(s.symbol, s.exchange)}} · 인생타점</span>
<span class="{{'pos' if group.average_return_pct is not none and group.average_return_pct >= 0 else ''}}">
평균 {{'%.2f'|format(group.average_return_pct)}}%
</span>
</div>
<div class="tf-row tf-head">
<span>시간봉</span><span>사이클</span><span>승률</span><span>평균수익</span><span>최고수익</span><span>보유</span>
</div>
{% for stat in group.details %}
<div class="tf-row">
<span>{{stat.timeframe}}</span>
<span>{{stat.result_count}}</span>
<span>{{'%.1f'|format(stat.win_rate_pct)}}%</span>
<span class="{{'pos' if stat.average_return_pct >= 0 else ''}}">{{'%.2f'|format(stat.average_return_pct)}}%</span>
<span class="{{'pos' if stat.best_return_pct >= 0 else ''}}">{{'%.2f'|format(stat.best_return_pct)}}%</span>
<span>{% if stat.average_holding_minutes is not none %}{{'%.0f'|format(stat.average_holding_minutes)}}분{% else %}-{% endif %}</span>
</div>
{% endfor %}
</div>
{% endif %}
{% endfor %}
{% endfor %}
{% if not life_ns.has_any %}
<div class="notice">선택 기간에는 인생타점 완료 성과가 아직 없습니다.</div>
{% endif %}
</div>
</details>

{% endif %}

<div class="symbols">
{% for s in selected.symbols %}
<a class="symbol" href="/performance/member/symbol?category={{selected_category}}&symbol={{s.symbol}}">
<h3>{{symbol_display(s.symbol, s.exchange)}}</h3>
<div class="values">
<div class="mini"><span>최고수익</span><b class="{{'pos' if s.member_stats.best_return_pct is not none and s.member_stats.best_return_pct >= 0 else 'muted'}}">
{% if s.member_stats.best_return_pct is not none %}{{'%.2f'|format(s.member_stats.best_return_pct)}}%{% else %}대기{% endif %}
</b></div>
<div class="mini"><span>평균수익</span><b class="{{'pos' if s.member_stats.average_return_pct is not none and s.member_stats.average_return_pct >= 0 else 'muted'}}">
{% if s.member_stats.average_return_pct is not none %}{{'%.2f'|format(s.member_stats.average_return_pct)}}%{% else %}대기{% endif %}
</b></div>
<div class="mini"><span>승률</span><b>
{% if s.member_stats.win_rate_pct is not none %}{{'%.1f'|format(s.member_stats.win_rate_pct)}}%{% else %}대기{% endif %}
</b></div>
<div class="mini"><span>평균 보유</span><b>
{% if s.member_stats.average_holding_minutes is not none %}{{'%.0f'|format(s.member_stats.average_holding_minutes)}}분{% else %}대기{% endif %}
</b></div>
</div>
</a>
{% endfor %}
</div>
{% else %}
<div class="notice">현재 수집된 해당 시장 신호가 없습니다.</div>
{% endif %}
{% endif %}

<div class="disclaimer">
표시 수익률은 저장된 알람 신호를 가정 진입·청산 방식으로 계산한 통계이며,
실제 체결가격·수수료·슬리피지·세금은 반영되지 않을 수 있습니다.
</div>
</body>
</html>
            """,
            data=data,
            selected=selected,
            selected_category=selected_category,
            ranked_symbols=ranked_symbols,
            average_ranking=average_ranking,
            best_ranking=best_ranking,
            win_rate_ranking=win_rate_ranking,
            chart_scale=chart_scale,
            period_key=period_key,
            market_stats=market_stats,
            market_group_stats=market_group_stats,
        ), 200

    except Exception as exc:
        log.exception("Member performance dashboard failed")
        return jsonify({"ok": False, "error": str(exc)}), 500





@app.get("/performance/member/symbol")
@member_required
def performance_member_symbol():
    category = request.args.get("category", "KOREA_1Q").strip().upper()
    symbol = request.args.get("symbol", "").strip().upper()

    category_market = {
        "KOREA_1Q": "KOREA",
        "US_1Q": "US",
        "COIN": "COIN",
    }
    if category not in category_market:
        category = "KOREA_1Q"

    data = group_analysis_data(
        market=category_market[category],
        symbol=symbol,
    )

    if not data.get("symbol"):
        return render_template_string("""
<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>종목 상세 성과</title>
<style>
body{background:#0e0e0f;color:#f4f4f4;font-family:Arial,"Noto Sans KR",sans-serif;padding:20px}
a{color:#76ceff}.card{background:#1b1b1d;border:1px solid #343438;border-radius:14px;padding:18px}
</style></head><body>
<a href="/performance/member?category={{category}}">← 종목 목록</a>
<div class="card"><h2>종목 데이터가 없습니다.</h2></div>
</body></html>
""", category=category), 404


    member_stats = _member_group_engine_statistics(data, "all")
    member_stats["entry_groups"] = _group_entry_timeframe_stats(
        category, member_stats.get("entry_timeframes") or []
    )

    return render_template_string("""
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{symbol_display(data.symbol, 'KRX' if data.market == 'KOREA' else '')}} 상세 성과</title>
<style>
:root{--bg:#0e0e0f;--card:#1b1b1d;--line:#343438;--blue:#7ed2ff;--green:#55e69a;--red:#ff7878;--muted:#aaa}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:#f5f5f5;font-family:Arial,"Noto Sans KR",sans-serif;padding:18px}
a{color:#78d1ff;text-decoration:none}
h1{font-size:29px;margin:15px 0 8px}
.card{background:var(--card);border:1px solid var(--line);border-radius:15px;padding:16px;margin:14px 0}
.badges{display:flex;gap:8px;flex-wrap:wrap}.badge{background:#29292c;border-radius:999px;padding:8px 12px}
.scroll{overflow-x:auto}table{width:100%;border-collapse:collapse;min-width:850px}
th,td{padding:10px;border-bottom:1px solid #303034;text-align:left}
th{color:var(--blue)}.pos{color:var(--green);font-weight:bold}.neg{color:var(--red);font-weight:bold}.muted{color:var(--muted)}
details{background:#141416;border:1px solid #303035;border-radius:11px;padding:12px;margin:10px 0}
summary{cursor:pointer;font-weight:bold}.trust-summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}.trust-mini{background:#111113;border:1px solid #303035;border-radius:12px;padding:13px}.trust-mini span{display:block;color:var(--muted);font-size:13px}.trust-mini b{display:block;font-size:22px;margin-top:6px}.cycle-flow{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:12px}.flow-step{background:#101012;border:1px solid #303035;border-radius:12px;padding:12px}.flow-step strong{display:block;color:var(--blue);margin-bottom:7px}.flow-step.adverse strong{color:var(--red)}.flow-step.exit strong{color:var(--green)}
</style>
</head>
<body>
<a href="/performance/member?category={{category}}">← 종목 목록으로</a>
<h1>{{symbol_display(data.symbol, 'KRX' if data.market == 'KOREA' else '')}}</h1>
<div class="badges">
<span class="badge">{{exchange_only_label('KRX' if data.market == 'KOREA' else '', data.market)}}</span>
<span class="badge">진입 최대 {{data.settings.entry_split_limit}}회</span>
<span class="badge">최근 평균 {{data.settings.recent_interval_count}}회</span>
</div>

<div class="card">
<h2>회원 체감 핵심 요약</h2>
<div class="trust-summary">
<div class="trust-mini"><span>완료 사이클</span><b>{{member_stats.completed_cycle_count}}회</b></div>
<div class="trust-mini"><span>평균 수익</span><b class="pos">{% if member_stats.average_return_pct is not none %}{{'%.2f'|format(member_stats.average_return_pct)}}%{% else %}-{% endif %}</b></div>
<div class="trust-mini"><span>승률</span><b>{% if member_stats.win_rate_pct is not none %}{{'%.1f'|format(member_stats.win_rate_pct)}}%{% else %}-{% endif %}</b></div>
<div class="trust-mini"><span>신호 기준 평균 최대 역행</span><b class="neg">{% if member_stats.average_signal_adverse_pct is not none %}{{'%.2f'|format(member_stats.average_signal_adverse_pct)}}%{% else %}-{% endif %}</b></div>
<div class="trust-mini"><span>역행 후 평균 회복</span><b>{{format_minutes_compact(member_stats.average_recovery_minutes)}}</b></div>
<div class="trust-mini"><span>최고 사례</span><b class="pos">{% if member_stats.best_detail %}{{member_stats.best_detail.entry_timeframe}}→{{member_stats.best_detail.exit_timeframe}} {{'%.2f'|format(member_stats.best_detail.return_pct)}}%{% else %}-{% endif %}</b></div>
</div>
<p class="muted">※ 최대 역행은 전체 캔들 저가가 아니라 저장된 LOW 신호 가격 기준입니다.</p>
</div>

<div class="card">
<h2>포지션별 성과</h2>
<div class="trust-summary">
{% for group in member_stats.entry_groups %}
<div class="trust-mini" style="{{'border-color:#92751d' if group.group_key == 'LIFE' else ''}}"><span>{{group.group_label}} · {{group.result_count}}사이클</span><b class="pos">{% if group.average_return_pct is not none %}{{'%.2f'|format(group.average_return_pct)}}%{% else %}-{% endif %}</b><small>승률 {% if group.win_rate_pct is not none %}{{'%.1f'|format(group.win_rate_pct)}}%{% else %}-{% endif %} · 보유 {{format_minutes_compact(group.average_holding_minutes)}}</small></div>
{% endfor %}
</div></div>

<div class="card">
<h2>진입 시간봉 × 청산 시간봉별 성과</h2>
<div class="scroll">
<table>
<tr>
<th>최초 진입 시간봉</th><th>청산 시간봉</th><th>완료 사이클</th><th>평균수익</th>
<th>최고수익</th><th>최저수익</th><th>승률</th><th>평균 보유기간</th>
</tr>
{% for row in data.performance_summary %}
<tr>
<td>{{row.entry_timeframe}}</td>
<td>{{row.exit_timeframe}}</td>
<td>{{row.trade_count}}사이클</td>
<td class="{{'pos' if row.average_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.average_return_pct)}}%</td>
<td class="{{'pos' if row.best_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.best_return_pct)}}%</td>
<td class="{{'pos' if row.worst_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.worst_return_pct)}}%</td>
<td>{{'%.1f'|format(row.win_rate_pct)}}%</td>
<td>{{row.average_holding_text}}</td>
</tr>
{% else %}
<tr><td colspan="8" class="muted">완료된 진입·청산 조합이 아직 없습니다.</td></tr>
{% endfor %}
</table>
</div>
</div>

<div class="card">
<h2>타점 발생 주기</h2>
<p class="muted">발생 상태는 최근 N회 또는 누적 평균 간격 대비 마지막 발생 후 경과시간을 비교한 참고값이며, 다음 신호를 보장하지 않습니다.</p>
<div class="scroll">
<table>
<tr><th>그룹</th><th>시간봉</th><th>누적 발생</th><th>누적 평균</th>
<th>최근 {{data.settings.recent_interval_count}}회 평균</th><th>최단</th><th>최장</th><th>마지막 발생 후</th><th>현재 상태</th></tr>
{% for row in data.occurrence_stats %}
<tr><td>{{row.group_label}}</td><td>{{row.timeframe}}</td><td>{{row.occurrence_count}}회</td>
<td>{{row.overall_average_text}}</td><td>{{row.recent_average_text}}</td>
<td>{{row.minimum_text}}</td><td>{{row.maximum_text}}</td><td>{{row.elapsed_text}}</td><td class="status-{{row.readiness_level}}">{{row.readiness_label}}</td></tr>
{% else %}
<tr><td colspan="9" class="muted">발생 주기 데이터가 아직 없습니다.</td></tr>
{% endfor %}
</table>
</div>
</div>

<div class="card">
<h2>사이클별 체감 흐름</h2>
<p class="muted">진입 → 신호 기준 최대 역행 → 청산 시간봉별 결과를 한눈에 비교합니다.</p>
{% for position in data.positions|reverse %}
{% if position.cycle_closed and position.exit_results %}
<details>
<summary>사이클 #{{position.position_sequence}} · 최초 {{position.entry_timeframe}} · {{position.entry_count}}회 진입</summary>
<div class="cycle-flow">
{% for entry in position.entry_points %}<div class="flow-step"><strong>진입 {{loop.index}} · {{entry.timeframe}}</strong><div>{{entry.price}}</div><small>{{entry.time}}</small></div>{% endfor %}
<div class="flow-step adverse"><strong>신호 기준 최대 역행</strong><div>{{'%.2f'|format(position.signal_adverse_pct)}}%</div><small>전체 캔들 MDD가 아닌 LOW 신호 기준</small></div>
{% for exit in position.exit_results %}<div class="flow-step exit"><strong>{{exit.exit_timeframe}} 청산</strong><div>{{'%.2f'|format(exit.return_pct)}}%</div><small>{{exit.holding_text}} · 회복 {{exit.recovery_text}}</small></div>{% endfor %}
</div>
<p><a href="/performance/member/cycle-image?category={{category}}&symbol={{data.symbol}}&cycle={{position.position_sequence}}" target="_blank">이미지 열기·PNG 저장·공유 →</a></p>
</details>
{% endif %}
{% else %}<p class="muted">완료 사이클이 없습니다.</p>{% endfor %}
</div>

<div class="card">
<h2>실제 신호 가격 차트</h2>
<p class="muted">진입 가격과 각 청산 시간봉 가격만 표시하고 중간 캔들은 생략한다.</p>
{% for position in data.positions|reverse %}
{% if position.cycle_closed and position.exit_results %}
<details>
<summary>사이클 #{{position.position_sequence}} · 최초 {{position.entry_timeframe}} · {{position.entry_count}}회 진입</summary>
{{price_path_svg(position)|safe}}
</details>
{% endif %}
{% else %}<p class="muted">완료 사이클이 아직 없습니다.</p>{% endfor %}
</div>

<div class="card">
<h2>실제 백데이터: 진입·청산 시각</h2>
{% for position in data.positions|reverse %}
<details>
<summary>최초 {{position.entry_timeframe}} · {{position.entry_count}}회 진입 · 평균가 {{position.entry_price}}</summary>
<p><b>실제 진입 구성:</b> {{position.entry_source_summary}}<br>
첫 진입: {{position.entry_first_time}}<br>
마지막 진입: {{position.entry_last_time}}</p>
<div class="scroll"><table>
<tr><th>청산 시간봉</th><th>청산 시각</th><th>청산가</th><th>보유기간</th><th>수익률</th></tr>
{% for exit in position.exit_results %}
<tr><td>{{exit.exit_timeframe}}</td><td>{{exit.exit_time}}</td><td>{{exit.exit_price}}</td>
<td>{{exit.holding_text}}</td><td class="{{'pos' if exit.return_pct >= 0 else 'neg'}}">{{'%.3f'|format(exit.return_pct)}}%</td></tr>
{% else %}
<tr><td colspan="5" class="muted">아직 유효한 청산 신호가 없습니다.</td></tr>
{% endfor %}
</table></div>
</details>
{% else %}
<p class="muted">생성된 진입 포지션이 없습니다.</p>
{% endfor %}
</div>
</body>
</html>
""", data=data, category=category, member_stats=member_stats), 200



@app.get("/performance/member/cycle-image.svg")
@member_required
def performance_member_cycle_image():
    category=request.args.get("category","KOREA_1Q").strip().upper()
    symbol=request.args.get("symbol","").strip().upper()
    try: cycle_no=int(request.args.get("cycle","0"))
    except ValueError: cycle_no=0
    market={"KOREA_1Q":"KOREA","US_1Q":"US","COIN":"COIN"}.get(category,"KOREA")
    data=group_analysis_data(market=market,symbol=symbol)
    position=next((row for row in data.get("positions",[]) if int(row.get("position_sequence") or 0)==cycle_no),None)
    if not position: return Response("cycle not found",status=404,mimetype="text/plain")
    title=symbol_display(data.get("symbol"),"KRX" if market=="KOREA" else "")
    svg=promo_cycle_svg(position,title)
    disposition = "attachment" if request.args.get("download") == "1" else "inline"
    safe_symbol = re.sub(r"[^A-Za-z0-9_.-]", "_", symbol) or "symbol"
    return Response(
        svg,
        mimetype="image/svg+xml",
        headers={"Content-Disposition": f'{disposition}; filename="{safe_symbol}_{cycle_no}.svg"'},
    )



@app.get("/performance/member/cycle-image")
@member_required
def performance_member_cycle_image_view():
    category = request.args.get("category", "KOREA_1Q").strip().upper()
    symbol = request.args.get("symbol", "").strip().upper()
    cycle = request.args.get("cycle", "0").strip()
    svg_url = url_for(
        "performance_member_cycle_image",
        category=category,
        symbol=symbol,
        cycle=cycle,
    )
    return render_template_string("""
<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>사이클 홍보 이미지</title>
<style>
body{margin:0;background:#09090a;color:#fff;font-family:Arial,"Noto Sans KR",sans-serif;padding:18px}
.wrap{max-width:1100px;margin:auto}.actions{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0 18px}
button,a.btn{border:1px solid #45454b;background:#202024;color:#fff;border-radius:10px;padding:11px 15px;font-size:15px;text-decoration:none;cursor:pointer}
.primary{border-color:#2fa56f!important;background:#176a48!important}.imagebox{background:#111113;border:1px solid #35353a;border-radius:16px;padding:10px}
.imagebox img{width:100%;display:block;border-radius:12px}.note{color:#aaa;font-size:13px;line-height:1.6;margin-top:14px}
</style></head><body><div class="wrap">
<a href="javascript:history.back()" style="color:#72ceff;text-decoration:none">← 이전 화면</a>
<h1>사이클 홍보 이미지</h1>
<div class="actions">
<a class="btn" href="{{svg_url}}" download="{{symbol}}_cycle_{{cycle}}.svg">SVG 저장</a>
<button class="primary" id="pngBtn">PNG 저장</button>
<button id="shareBtn">공유하기</button>
</div>
<div class="imagebox"><img id="cycleImage" src="{{svg_url}}" alt="사이클 성과 이미지"></div>
<div class="note">
통계 이미지는 저장된 진입·청산 신호 가격을 사용합니다. 중간 캔들은 생략되며,
수수료·슬리피지·세금은 실제 체결 결과와 다를 수 있습니다. 공유하기 버튼은 지원되는 모바일에서
공유 메뉴를 열며 Telegram을 선택할 수 있습니다.
</div>
<canvas id="canvas" style="display:none"></canvas>
<script>
const img = document.getElementById('cycleImage');
const canvas = document.getElementById('canvas');
async function svgToPngBlob(){
  const response = await fetch(img.src, {credentials:'same-origin'});
  if(!response.ok) throw new Error('이미지를 불러오지 못했습니다.');
  const svgText = await response.text();
  const blob = new Blob([svgText], {type:'image/svg+xml;charset=utf-8'});
  const url = URL.createObjectURL(blob);
  try {
    const raster = new Image();
    await new Promise((resolve,reject)=>{raster.onload=resolve;raster.onerror=reject;raster.src=url;});
    canvas.width = 1080; canvas.height = 1080;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle='#080809'; ctx.fillRect(0,0,canvas.width,canvas.height);
    ctx.drawImage(raster,0,0,canvas.width,canvas.height);
    return await new Promise(resolve=>canvas.toBlob(resolve,'image/png',0.95));
  } finally { URL.revokeObjectURL(url); }
}
document.getElementById('pngBtn').addEventListener('click', async()=>{
  try{
    const blob=await svgToPngBlob();
    const url=URL.createObjectURL(blob); const a=document.createElement('a');
    a.href=url; a.download='{{symbol}}_cycle_{{cycle}}.png'; a.click();
    setTimeout(()=>URL.revokeObjectURL(url),2000);
  }catch(e){alert(e.message||'PNG 저장 실패');}
});
document.getElementById('shareBtn').addEventListener('click', async()=>{
  try{
    const blob=await svgToPngBlob();
    const file=new File([blob],'{{symbol}}_cycle_{{cycle}}.png',{type:'image/png'});
    if(navigator.canShare && navigator.canShare({files:[file]})){
      await navigator.share({title:'타점 성과 이미지',text:'결과로 증명하는 타점 알람',files:[file]});
    } else if(navigator.share){
      await navigator.share({title:'타점 성과 이미지',url:location.href});
    } else { alert('이 브라우저는 공유 기능을 지원하지 않습니다. PNG 저장을 이용해주세요.'); }
  }catch(e){ if(e.name!=='AbortError') alert(e.message||'공유 실패'); }
});
</script></div></body></html>
""", svg_url=svg_url, symbol=symbol, cycle=cycle), 200


@app.get("/performance/member/image-preview")
@member_required
def performance_member_image_preview():
    category = request.args.get("category", "KOREA_1Q").strip().upper()
    period_key = request.args.get("period", "all").strip().lower()
    if category not in {"KOREA_1Q", "US_1Q", "COIN"}:
        category = "KOREA_1Q"
    if period_key not in {"today", "7d", "30d", "all"}:
        period_key = "all"

    data = _sort_performance_categories(visual_cycle_data(100))
    selected = next(
        (
            item for item in data["categories"]
            if item["category_key"] == category
        ),
        None,
    )

    cards = []
    if selected:
        category_market = {
            "KOREA_1Q": "KOREA",
            "US_1Q": "US",
            "COIN": "COIN",
        }
        market_analysis = group_analysis_market_data(
            category_market[category]
        )
        analysis_by_symbol = market_analysis.get(
            "symbol_data", {}
        )

        for item in selected["symbols"]:
            stats = _member_group_engine_statistics(
                analysis_by_symbol.get(
                    item.get("symbol"),
                    {"positions": []},
                ),
                period_key,
            )
            if not stats["has_results"]:
                continue
            stats["entry_groups"] = _group_entry_timeframe_stats(
                category,
                stats.get("entry_timeframes") or [],
            )
            enriched = dict(item)
            enriched["group_analysis"] = analysis_by_symbol.get(
                item.get("symbol"), {"positions": []}
            )
            enriched["member_stats"] = stats
            cards.append(enriched)

    cards.sort(
        key=lambda item: item["member_stats"]["best_return_pct"] or -999999,
        reverse=True,
    )

    return render_template_string("""
<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>수익률 홍보 이미지 미리보기</title>
<style>
:root{--bg:#09090a;--card:#18181b;--line:#36363b;--green:#56e69b;--yellow:#ffd24d;--blue:#72ceff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#fff;font-family:Arial,"Noto Sans KR",sans-serif;padding:20px}
a{color:var(--blue);text-decoration:none}.toolbar{max-width:1080px;margin:auto auto 16px;display:flex;gap:14px;flex-wrap:wrap}
.canvas{max-width:1080px;min-height:1080px;margin:auto;background:linear-gradient(145deg,#101012,#050506);border:1px solid #25252a;border-radius:24px;padding:46px}
.eyebrow{color:var(--yellow);font-size:20px;font-weight:bold}.headline{font-size:50px;line-height:1.15;margin:12px 0 30px}.headline strong{color:var(--green)}
.hero{display:grid;grid-template-columns:1.3fr .7fr;gap:18px}.chartbox,.metric{background:var(--card);border:1px solid var(--line);border-radius:20px;padding:24px}
.chartbox{min-height:390px;position:relative;overflow:hidden}.fake-chart{position:absolute;inset:85px 25px 55px;background:linear-gradient(165deg,transparent 48%,rgba(86,230,155,.25) 49%,rgba(86,230,155,.25) 52%,transparent 53%)}
.line{position:absolute;left:8%;right:7%;top:58%;height:5px;background:linear-gradient(90deg,#72ceff,#56e69b);transform:rotate(-11deg);border-radius:99px;box-shadow:0 0 18px rgba(86,230,155,.45)}
.dot{position:absolute;width:17px;height:17px;border-radius:50%;background:var(--yellow);box-shadow:0 0 0 6px rgba(255,210,77,.15)}.d1{left:18%;top:69%}.d2{left:39%;top:60%}.d3{left:61%;top:50%}.sell{right:12%;top:31%;background:var(--green)}
.chart-label{position:absolute;font-size:15px;color:#bbb}.l1{left:12%;top:77%}.l2{left:34%;top:68%}.l3{left:57%;top:58%}.ls{right:8%;top:22%;color:var(--green)}
.bigreturn{font-size:60px;color:var(--green);font-weight:900;margin:5px 0}.sub{color:#aaa}.metric-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:18px}.metric{padding:18px}.metric b{display:block;font-size:26px;margin-top:6px}.life{border-color:#725b1c;background:linear-gradient(145deg,#221d10,#151515)}
.list{margin-top:20px;display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.symbol{background:#141416;border:1px solid #303034;border-radius:15px;padding:16px}.symbol b{font-size:20px}.symbol .ret{color:var(--green);font-size:28px;font-weight:bold;margin-top:8px}
.note{max-width:1080px;margin:14px auto;color:#aaa;font-size:14px}
@media(max-width:750px){body{padding:8px}.canvas{padding:22px;min-height:auto}.headline{font-size:32px}.hero{grid-template-columns:1fr}.list{grid-template-columns:1fr}.bigreturn{font-size:45px}}
</style></head>
<body>
<div class="toolbar">
<a href="/performance/member?category={{category}}&period={{period_key}}">← 회원 성과 화면</a>
<span>현재는 HTML 디자인 미리보기입니다. 다음 단계에서 PNG 자동 생성·텔레그램 전송을 연결합니다.</span>
</div>
<div class="canvas">
<div class="eyebrow">결과로 증명하는 타점 알람</div>
<div class="headline">발목에서 잡고,<br><strong>인생타점으로 수익을 키우다</strong></div>
{% if cards %}
{% set top = cards[0] %}
<div class="hero">
<div class="chartbox">
<div style="font-size:28px;font-weight:bold">{{symbol_display(top.symbol, top.exchange)}}</div>
<div class="sub">진입 ①②③ → 청산 구간 시각화 예시</div>
{% set completed_positions = top.group_analysis.positions|selectattr('cycle_closed')|list %}
{% if completed_positions %}
{{price_path_svg(completed_positions[-1])|safe}}
{% else %}<div class="sub" style="padding:50px 0">완료 사이클 가격 데이터 대기</div>{% endif %}
</div>
<div>
<div class="metric life">
<div class="sub">최고 수익률</div>
<div class="bigreturn">{{'%.2f'|format(top.member_stats.best_return_pct)}}%</div>
<div class="sub">{{period_key}} 기준 · 저장 신호 백데이터</div>
</div>
<div class="metric-grid">
<div class="metric"><span class="sub">평균 수익</span><b>{{'%.2f'|format(top.member_stats.average_return_pct)}}%</b></div>
<div class="metric"><span class="sub">승률</span><b>{{'%.1f'|format(top.member_stats.win_rate_pct)}}%</b></div>
<div class="metric"><span class="sub">평균 보유</span><b>{{'%.0f'|format(top.member_stats.average_holding_minutes or 0)}}분</b></div>
<div class="metric"><span class="sub">완료 사이클</span><b>{{top.member_stats.completed_cycle_count}}회</b></div>
</div>
</div>
</div>
<div class="list">
{% for item in cards[:3] %}
<div class="symbol">
<b>{{symbol_display(item.symbol, item.exchange)}}</b>
<div class="ret">{{'%.2f'|format(item.member_stats.best_return_pct)}}%</div>
<div class="sub">평균 {{'%.2f'|format(item.member_stats.average_return_pct)}}% · 승률 {{'%.1f'|format(item.member_stats.win_rate_pct)}}%</div>
</div>
{% endfor %}
</div>
{% else %}
<div class="metric">완료 성과가 쌓이면 이 화면에 실제 수치가 자동으로 들어갑니다.</div>
{% endif %}
</div>
<div class="note">이 화면은 홍보 이미지의 레이아웃 검토용이다. 실제 캔들 차트 데이터 연결과 PNG 저장 버튼은 다음 작업에서 추가한다.</div>
</body></html>
""", cards=cards, category=category, period_key=period_key), 200


@app.get("/performance/member/charts")
@member_required
def performance_member_charts():
    try:
        selected_category = (
            request.args.get("category", "KOREA_1Q")
            .strip()
            .upper()
        )
        if selected_category not in {"COIN", "KOREA_1Q", "US_1Q"}:
            selected_category = "KOREA_1Q"

        period_key = request.args.get("period", "all").strip().lower()
        if period_key not in {"today", "7d", "30d", "all"}:
            period_key = "all"

        data = _sort_performance_categories(visual_cycle_data(1000))
        selected = next(
            (
                category
                for category in data["categories"]
                if category["category_key"] == selected_category
            ),
            None,
        )

        chart = _build_member_chart_data(selected, period_key)

        return render_template_string(
            """
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>회원 성과 그래프</title>
<style>
:root{--bg:#0e0e0f;--card:#1b1b1d;--line:#353539;--blue:#7ed2ff;--green:#55e69a;--yellow:#ffc857;--red:#ff7878}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:#f5f5f5;font-family:Arial,"Noto Sans KR",sans-serif;padding:20px}
a{color:var(--blue)}
.header{display:flex;justify-content:space-between;align-items:center;gap:15px;flex-wrap:wrap;margin-bottom:18px}
h1{margin:0;font-size:32px}.muted{color:#aaa}
.tabs{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px}
.tabs a{padding:10px 15px;border:1px solid #404045;background:#242427;color:#75ceff;border-radius:999px;text-decoration:none}
.tabs a.active{background:#14405b;border-color:#67c8ff;color:#fff;font-weight:bold}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:18px}
.metric,.panel{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;min-width:0}
.metric .label{color:var(--blue);font-weight:bold;margin-bottom:8px}
.metric .value{font-size:26px;font-weight:bold}
.pos{color:var(--green)}.neg{color:var(--red)}
.panel{margin-bottom:18px}
.panel h2{margin:0 0 15px;font-size:22px;color:var(--blue)}
.chartbox{background:#111113;border:1px solid #2f2f33;border-radius:12px;padding:10px;overflow-x:auto}
.chartbox svg{width:100%;min-width:700px;height:280px;display:block}
.axis{stroke:#3f3f44;stroke-width:1}.curve{fill:none;stroke:#55e69a;stroke-width:4;stroke-linejoin:round;stroke-linecap:round}
.bar-row{display:grid;grid-template-columns:110px minmax(100px,1fr) 80px 70px;gap:10px;align-items:center;margin:12px 0}
.bar-name{font-weight:bold}
.bar-track{height:18px;border-radius:999px;background:#101012;border:1px solid #2d2d31;overflow:hidden}
.bar-fill{height:100%;min-width:3px;background:linear-gradient(90deg,#2495c7,#55e69a);border-radius:999px}
.bar-fill.red{background:linear-gradient(90deg,#a63b4a,#ff7878)}
.value{text-align:right;font-weight:bold}.count{text-align:right;color:#aaa}
table{width:100%;border-collapse:collapse;min-width:760px}
th,td{text-align:left;padding:10px;border-bottom:1px solid #303034}
th{color:var(--blue)}
.notice{padding:20px;color:#aaa;background:#151517;border-radius:10px}
.foot{color:#777;font-size:13px;line-height:1.6;margin-top:18px}
@media(max-width:1000px){.grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:650px){body{padding:11px}.grid{grid-template-columns:1fr}.bar-row{grid-template-columns:70px minmax(70px,1fr) 62px 48px;font-size:13px}h1{font-size:26px}}
</style>
</head>
<body>
<div class="header">
<div>
<h1>회원 성과 그래프</h1>
<div class="muted">{{selected.category_label if selected else "성과 데이터"}}</div>
</div>
<div style="display:flex;gap:12px">
<a href="/performance/member?category={{selected_category}}&period={{period_key}}">요약 화면</a>
<a href="/performance/logout">로그아웃</a>
</div>
</div>

<div class="tabs">
{% for category in data.categories %}
<a class="{{'active' if category.category_key == selected_category else ''}}"
href="/performance/member/charts?category={{category.category_key}}&period={{period_key}}">
{{category.category_label}} · {{category.symbol_count}}종목
</a>
{% endfor %}
</div>

<div class="tabs">
<a class="{{'active' if period_key == 'today' else ''}}" href="/performance/member/charts?category={{selected_category}}&period=today">오늘</a>
<a class="{{'active' if period_key == '7d' else ''}}" href="/performance/member/charts?category={{selected_category}}&period=7d">최근 7일</a>
<a class="{{'active' if period_key == '30d' else ''}}" href="/performance/member/charts?category={{selected_category}}&period=30d">최근 30일</a>
<a class="{{'active' if period_key == 'all' else ''}}" href="/performance/member/charts?category={{selected_category}}&period=all">전체</a>
</div>

<div class="grid">
<div class="metric"><div class="label">청산 결과 수</div><div class="value">{{chart.curve|length}}건</div></div>
<div class="metric"><div class="label">단순 누적 수익률</div><div class="value {{'pos' if chart.final_cumulative_pct is not none and chart.final_cumulative_pct >= 0 else 'neg'}}">
{% if chart.final_cumulative_pct is not none %}{{'%.2f'|format(chart.final_cumulative_pct)}}%{% else %}결과 대기{% endif %}
</div></div>
<div class="metric"><div class="label">최고 누적 구간</div><div class="value {{'pos' if chart.curve_max >= 0 else 'neg'}}">{{'%.2f'|format(chart.curve_max)}}%</div></div>
<div class="metric"><div class="label">최저 누적 구간</div><div class="value {{'pos' if chart.curve_min >= 0 else 'neg'}}">{{'%.2f'|format(chart.curve_min)}}%</div></div>
</div>

<div class="panel">
<h2>기간별 단순 누적 수익률 곡선</h2>
{% if chart.curve %}
<div class="chartbox">
<svg viewBox="0 0 1000 280" preserveAspectRatio="none" aria-label="누적 수익률 그래프">
<line x1="30" y1="24" x2="30" y2="236" class="axis"/>
<line x1="30" y1="236" x2="970" y2="236" class="axis"/>
<polyline points="{{chart.curve_polyline}}" class="curve"/>
<text x="38" y="20" fill="#aaa" font-size="15">최고 {{'%.2f'|format(chart.curve_max)}}%</text>
<text x="38" y="260" fill="#aaa" font-size="15">최저 {{'%.2f'|format(chart.curve_min)}}%</text>
<text x="820" y="260" fill="#aaa" font-size="15">청산 결과 {{chart.curve|length}}건</text>
</svg>
</div>
{% else %}
<div class="notice">선택 기간에는 그래프로 표시할 완료 성과가 없습니다.</div>
{% endif %}
</div>

<div class="panel">
<h2>진입 시간봉별 평균수익률</h2>
{% if chart.timeframes %}
{% for row in chart.timeframes %}
<div class="bar-row">
<div class="bar-name">{{row.timeframe}}</div>
<div class="bar-track"><div class="bar-fill {{'red' if row.average_return_pct < 0 else ''}}" style="width:{{(row.average_return_pct|abs / chart.max_abs_timeframe_return * 100) if chart.max_abs_timeframe_return else 0}}%"></div></div>
<div class="value {{'pos' if row.average_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.average_return_pct)}}%</div>
<div class="count">{{row.result_count}}건</div>
</div>
{% endfor %}
{% else %}
<div class="notice">선택 기간에는 시간봉별 완료 성과가 없습니다.</div>
{% endif %}
</div>

<div class="panel">
<h2>진입 시간봉별 승률·수익률 상세</h2>
{% if chart.timeframes %}
<div style="overflow-x:auto">
<table>
<tr><th>진입 시간봉</th><th>결과 수</th><th>승률</th><th>평균수익</th><th>최고수익</th><th>최저수익</th></tr>
{% for row in chart.timeframes %}
<tr>
<td>{{row.timeframe}}</td>
<td>{{row.result_count}}</td>
<td>{{'%.1f'|format(row.win_rate_pct)}}%</td>
<td class="{{'pos' if row.average_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.average_return_pct)}}%</td>
<td class="{{'pos' if row.best_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.best_return_pct)}}%</td>
<td class="{{'pos' if row.worst_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.worst_return_pct)}}%</td>
</tr>
{% endfor %}
</table>
</div>
{% else %}
<div class="notice">시간봉 상세 데이터가 없습니다.</div>
{% endif %}
</div>

<div class="panel">
<h2>종목별 평균수익률 순위</h2>
{% if chart.symbols %}
{% for row in chart.symbols %}
<div class="bar-row">
<div class="bar-name">{{row.symbol}}</div>
<div class="bar-track"><div class="bar-fill {{'red' if row.average_return_pct < 0 else ''}}" style="width:{{(row.average_return_pct|abs / chart.max_symbol_return * 100) if chart.max_symbol_return else 0}}%"></div></div>
<div class="value {{'pos' if row.average_return_pct >= 0 else 'neg'}}">{{'%.2f'|format(row.average_return_pct)}}%</div>
<div class="count">{{row.result_count}}건</div>
</div>
{% endfor %}
{% else %}
<div class="notice">선택 기간에는 종목별 완료 성과가 없습니다.</div>
{% endif %}
</div>

<div class="foot">
누적 수익률 곡선은 각 청산 후보의 전체 분할 수익률을 시간순으로 단순 합산한 값이다.
복리, 동시 포지션, 자금 배분, 실제 체결가격, 수수료, 슬리피지 및 세금은 반영하지 않는다.
</div>
</body>
</html>
            """,
            data=data,
            selected=selected,
            selected_category=selected_category,
            period_key=period_key,
            chart=chart,
        ), 200

    except Exception as exc:
        log.exception("Member performance chart failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/performance/debug/automation-status")
@admin_required
def performance_automation_status():
    try:
        return jsonify(automation_status()), 200
    except Exception as exc:
        log.exception("Performance automation status failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/performance/debug/send-weekly", methods=["GET", "POST"])
@admin_required
def performance_debug_send_weekly():
    try:
        result = send_period_report_test("weekly")
        return jsonify(result), 200
    except Exception as exc:
        log.exception("Weekly performance test send failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/performance/debug/send-monthly", methods=["GET", "POST"])
@admin_required
def performance_debug_send_monthly():
    try:
        result = send_period_report_test("monthly")
        return jsonify(result), 200
    except Exception as exc:
        log.exception("Monthly performance test send failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/performance/debug/send-latest-cycle", methods=["GET", "POST"])
@admin_required
def performance_debug_send_latest_cycle():
    try:
        market = request.values.get("market", "").strip().upper() or None
        symbol = request.values.get("symbol", "").strip().upper() or None
        result = send_latest_cycle_test(market=market, symbol=symbol)
        return jsonify(result), 200
    except Exception as exc:
        log.exception("Latest cycle test send failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/performance/export.csv")
@admin_required
def performance_export_csv():
    try:
        period_key = request.args.get("period", "all").strip().lower()
        if period_key not in {"today", "7d", "30d", "all"}:
            period_key = "all"

        category_key = request.args.get("category", "KOREA_1Q").strip().upper()
        if category_key not in {"COIN", "KOREA_1Q", "US_1Q"}:
            category_key = "COIN"

        data = _sort_performance_categories(visual_cycle_data(1000))
        category = next(
            (
                item for item in data["categories"]
                if item["category_key"] == category_key
            ),
            None,
        )

        output = io.StringIO()
        output.write("\ufeff")
        writer = csv.writer(output)
        writer.writerow(
            [
                "카테고리",
                "종목",
                "기간",
                "진입 시간봉",
                "결과 수",
                "승률(%)",
                "평균수익률(%)",
                "최고수익률(%)",
                "최저수익률(%)",
                "평균보유시간(분)",
            ]
        )

        if category:
            for symbol in category["symbols"]:
                stats = _member_symbol_statistics(symbol, period_key)
                for row in stats["entry_timeframes"]:
                    writer.writerow(
                        [
                            category["category_label"],
                            symbol["symbol"],
                            period_key,
                            row["timeframe"],
                            row["result_count"],
                            (
                                round(row["win_rate_pct"], 4)
                                if row["win_rate_pct"] is not None else ""
                            ),
                            (
                                round(row["average_return_pct"], 6)
                                if row["average_return_pct"] is not None else ""
                            ),
                            (
                                round(row["best_return_pct"], 6)
                                if row["best_return_pct"] is not None else ""
                            ),
                            (
                                round(row["worst_return_pct"], 6)
                                if row["worst_return_pct"] is not None else ""
                            ),
                            (
                                round(row["average_holding_minutes"], 2)
                                if row["average_holding_minutes"] is not None else ""
                            ),
                        ]
                    )

        filename = (
            f"performance_{category_key}_{period_key}_"
            f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        )
        return Response(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )
    except Exception as exc:
        log.exception("Performance CSV export failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/performance/dashboard")
@admin_required
def performance_dashboard():
    try:
        try:
            limit = int(request.args.get("limit", "100"))
        except ValueError:
            limit = 100

        selected_category = (
            request.args.get("category", "KOREA_1Q")
            .strip()
            .upper()
        )
        allowed_categories = {"COIN", "KOREA_1Q", "US_1Q"}
        if selected_category not in allowed_categories:
            selected_category = "KOREA_1Q"

        data = _sort_performance_categories(visual_cycle_data(limit))
        selected = next(
            (
                category
                for category in data["categories"]
                if category["category_key"] == selected_category
            ),
            None,
        )

        market_stats = {
            "has_results": False,
            "average_return_pct": None,
            "best_return_pct": None,
            "win_rate_pct": None,
            "average_holding_minutes": None,
            "result_symbol_count": 0,
            "result_count": 0,
            "best_symbol": None,
            "best_symbol_exchange": None,
        }

        if selected:
            category_market = {
                "KOREA_1Q": "KOREA",
                "US_1Q": "US",
                "COIN": "COIN",
            }
            market_analysis = group_analysis_market_data(
                category_market[selected_category]
            )
            analysis_by_symbol = market_analysis.get("symbol_data", {})

            enriched_symbols = []
            for item in selected.get("symbols") or []:
                analysis = analysis_by_symbol.get(
                    item.get("symbol"),
                    {"positions": [], "performance_summary": [], "occurrence_stats": []},
                )
                enriched = dict(item)
                enriched["group_analysis"] = analysis
                enriched["member_stats"] = _member_group_engine_statistics(
                    analysis,
                    "all",
                )
                enriched_symbols.append(enriched)

            selected = dict(selected)
            selected["symbols"] = enriched_symbols

            result_symbols = [
                item
                for item in enriched_symbols
                if item["member_stats"]["has_results"]
            ]

            average_values = [
                item["member_stats"]["average_return_pct"]
                for item in result_symbols
                if item["member_stats"]["average_return_pct"] is not None
            ]
            holding_values = [
                item["member_stats"]["average_holding_minutes"]
                for item in result_symbols
                if item["member_stats"]["average_holding_minutes"] is not None
            ]
            best_values = [
                item["member_stats"]["best_return_pct"]
                for item in result_symbols
                if item["member_stats"]["best_return_pct"] is not None
            ]

            total_results = sum(
                item["member_stats"]["result_count"]
                for item in result_symbols
            )
            weighted_wins = sum(
                (
                    item["member_stats"]["win_rate_pct"] / 100
                    * item["member_stats"]["result_count"]
                )
                for item in result_symbols
                if item["member_stats"]["win_rate_pct"] is not None
            )

            market_stats = {
                "has_results": bool(result_symbols),
                "average_return_pct": (
                    sum(average_values) / len(average_values)
                    if average_values else None
                ),
                "best_return_pct": (
                    max(best_values) if best_values else None
                ),
                "win_rate_pct": (
                    weighted_wins / total_results * 100
                    if total_results else None
                ),
                "average_holding_minutes": (
                    sum(holding_values) / len(holding_values)
                    if holding_values else None
                ),
                "result_symbol_count": len(result_symbols),
                "result_count": total_results,
                "best_symbol": (
                    max(
                        result_symbols,
                        key=lambda item: item["member_stats"]["best_return_pct"]
                        if item["member_stats"]["best_return_pct"] is not None
                        else float("-inf"),
                    )["symbol"]
                    if result_symbols else None
                ),
                "best_symbol_exchange": (
                    max(
                        result_symbols,
                        key=lambda item: item["member_stats"]["best_return_pct"]
                        if item["member_stats"]["best_return_pct"] is not None
                        else float("-inf"),
                    ).get("exchange")
                    if result_symbols else None
                ),
            }

        selected_symbol_name = (
            request.args.get("symbol", "")
            .strip()
            .upper()
        )
        selected_symbol = None
        entry_exit_matrix = {
            "rows": [],
            "matrix": [],
            "entry_timeframes": [],
            "exit_timeframes": [],
            "has_results": False,
            "result_count": 0,
        }
        if selected and selected_symbol_name:
            selected_symbol = next(
                (
                    item
                    for item in selected["symbols"]
                    if item["symbol"].upper() == selected_symbol_name
                ),
                None,
            )
            if selected_symbol:
                analysis = selected_symbol.get("group_analysis") or {}
                rows = analysis.get("performance_summary") or []
                entry_exit_matrix = {
                    "rows": rows,
                    "matrix": [],
                    "entry_timeframes": [],
                    "exit_timeframes": [],
                    "has_results": bool(rows),
                    "result_count": selected_symbol["member_stats"]["completed_cycle_count"],
                }

        return render_template_string("""
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>회원 운영용 성과 분석</title>
<style>
:root{--bg:#0e0e0f;--card:#1b1b1d;--line:#333;--blue:#8bd0ff;--green:#5ee39a;--yellow:#ffc857;--red:#ff7676}
*{box-sizing:border-box}
body{font-family:Arial,"Noto Sans KR",sans-serif;background:var(--bg);color:#f4f4f4;margin:0;padding:20px}
h1{margin:4px 0 10px;font-size:34px}h2{margin:0 0 12px;font-size:28px}
a{color:#73c9ff}.toplinks{margin-bottom:18px}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:18px;margin:16px 0}
.summary{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.badge{background:#29292c;border-radius:999px;padding:8px 13px}
.ok{color:var(--green)}.warn{color:var(--yellow)}.neg{color:var(--red)}.muted{color:#aaa}.blue{color:var(--blue)}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:12px 0}
.metric{background:#151517;border:1px solid #303033;border-radius:12px;padding:14px}
.metric .title{color:var(--blue);font-weight:bold;margin-bottom:8px}.metric .value{font-size:20px;font-weight:bold}
table{width:100%;border-collapse:collapse;margin-top:10px;font-size:14px;min-width:760px}
th,td{border-bottom:1px solid var(--line);padding:9px;text-align:left;vertical-align:top}
th{color:var(--blue)}
details{margin:10px 0;background:#141416;border-radius:10px;padding:11px}
summary{cursor:pointer;font-weight:bold}
.small{font-size:12px;color:#aaa}.pos{color:var(--green);font-weight:bold}
.mode-title{font-size:18px;color:var(--blue);margin:14px 0 4px}
.category-nav{display:flex;gap:10px;flex-wrap:wrap;margin:16px 0 22px}
.category-nav a{background:#242427;border:1px solid #3a3a3d;border-radius:999px;padding:9px 14px;text-decoration:none}
.category-nav a.active-category{background:#14405b;border-color:#67c8ff;color:#fff;font-weight:bold}
.category-head{display:flex;justify-content:space-between;gap:15px;align-items:center;margin:32px 0 10px;padding:14px 16px;background:#171719;border-left:5px solid var(--blue);border-radius:10px}
.category-head h2{margin:0}
.category-summary{display:flex;gap:9px;flex-wrap:wrap}
.empty-note{color:#999;padding:14px 0}
.market-performance{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:14px 0 20px}
.symbol-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:14px}
.symbol-card{display:block;background:#1b1b1d;border:1px solid #343438;border-radius:14px;padding:16px;text-decoration:none;color:#f4f4f4}
.symbol-card:hover{border-color:#69c9ff;transform:translateY(-1px)}
.symbol-card-head{display:flex;justify-content:space-between;gap:12px;align-items:center}
.symbol-name{font-size:24px;font-weight:bold}
.symbol-result-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:14px}
.symbol-result{background:#131315;border-radius:9px;padding:10px}
.symbol-result .label{font-size:12px;color:#aaa;margin-bottom:5px}
.symbol-result .number{font-size:18px;font-weight:bold}
.back-link{display:inline-block;margin:4px 0 14px;padding:8px 12px;border-radius:999px;background:#242427;text-decoration:none}
.matrix-wrap{overflow-x:auto;margin-top:12px}
.tf-matrix{border-collapse:separate;border-spacing:5px;min-width:760px}
.tf-matrix th{background:#121214;border:none;text-align:center;position:sticky;top:0}
.tf-matrix td{border:none;padding:0;min-width:125px}
.matrix-cell{display:block;background:#151517;border:1px solid #303035;border-radius:10px;padding:10px;text-align:center;min-height:88px}
.matrix-cell.empty{color:#666;display:flex;align-items:center;justify-content:center}
.matrix-cell .main{font-size:20px;font-weight:bold;margin-bottom:5px}
.matrix-cell .sub{font-size:12px;color:#aaa;line-height:1.45}
.analysis-note{background:#141416;border-left:4px solid var(--yellow);padding:12px 14px;border-radius:8px;color:#bbb;margin:14px 0}
@media(max-width:1100px){.market-performance{grid-template-columns:repeat(3,1fr)}}
@media(max-width:900px){.market-performance{grid-template-columns:repeat(2,1fr)}}
@media(max-width:650px){.market-performance,.symbol-result-grid{grid-template-columns:1fr}}
@media(max-width:800px){.grid{grid-template-columns:1fr}body{padding:10px}h1{font-size:27px}}
</style>
</head>
<body>
<h1>회원 운영용 성과 분석</h1>
<div class="toplinks">
<a href="/performance/health">DB 상태</a> ·
<a href="/performance/latest">최근 신호</a> ·
<a href="/performance/analyze">분석 실행</a> ·
<a href="/performance/cycles">사이클 JSON</a> ·
<a href="/performance/member">회원 화면 미리보기</a> ·
<a href="/performance/member/charts?category={{selected_category}}&period=all">회원 그래프 미리보기</a> ·
<a href="/performance/export.csv?category={{selected_category}}&period=all">CSV 다운로드</a> ·
<a href="/performance/logout">로그아웃</a>
<span style="margin-left:10px;color:#ffbf69;font-weight:bold">관리자 전용</span>
</div>

<div class="category-nav">
{% for category in data.categories %}
<a
href="/performance/dashboard?category={{category.category_key}}"
class="{{'active-category' if category.category_key == selected_category else ''}}"
>
{{category.category_label}} · 종목 {{category.symbol_count}}
</a>
{% endfor %}
</div>

{% if selected %}
<section id="{{selected.anchor}}">
<div class="category-head">
<h2>{{selected.category_label}}</h2>
<div class="category-summary">
<span class="badge">종목 {{selected.symbol_count}}</span>
<span class="badge ok">완료 Cycle {{selected.completed_cycle_count}}</span>
<span class="badge warn">청산 대기 {{selected.open_low_count}}</span>
</div>
</div>

<div class="market-performance">
<div class="metric">
<div class="title">시장 평균수익</div>
<div class="value {{'pos' if market_stats.average_return_pct is not none and market_stats.average_return_pct >= 0 else 'muted'}}">
{% if market_stats.average_return_pct is not none %}
{{'%.2f'|format(market_stats.average_return_pct)}}%
{% else %}결과 대기{% endif %}
</div>
<div class="small" style="margin-top:7px">{{market_stats.result_symbol_count}}개 종목의 완료 사이클 평균</div>
</div>
<div class="metric">
<div class="title">시장 최고수익</div>
<div class="value {{'pos' if market_stats.best_return_pct is not none and market_stats.best_return_pct >= 0 else 'muted'}}">
{% if market_stats.best_return_pct is not none %}
{{'%.2f'|format(market_stats.best_return_pct)}}%
{% else %}결과 대기{% endif %}
</div>
</div>
<div class="metric">
<div class="title">평균 보유시간</div>
<div class="value">
{% if market_stats.average_holding_minutes is not none %}
{{'%.0f'|format(market_stats.average_holding_minutes)}}분
{% else %}결과 대기{% endif %}
</div>
</div>
<div class="metric">
<div class="title">시장 승률</div>
<div class="value {{'pos' if market_stats.win_rate_pct is not none and market_stats.win_rate_pct >= 50 else 'neg'}}">
{% if market_stats.win_rate_pct is not none %}
{{'%.1f'|format(market_stats.win_rate_pct)}}%
{% else %}결과 대기{% endif %}
</div>
</div>
<div class="metric">
<div class="title">성과 발생 종목</div>
<div class="value">{{market_stats.result_symbol_count}} / {{selected.symbol_count}}</div>
</div>
</div>

{% if selected.symbol_count == 0 %}
<div class="card"><div class="empty-note">
현재 저장된 {{selected.category_label}} 신호가 없습니다.
</div></div>
{% elif not selected_symbol %}
<div class="symbol-list">
{% for s in selected.symbols %}
<a class="symbol-card" href="/performance/dashboard?category={{selected_category}}&symbol={{s.symbol}}">
<div class="symbol-card-head">
<div class="symbol-name">{{symbol_display(s.symbol, s.exchange)}}</div>
<div class="small">{{exchange_only_label(s.exchange)}}</div>
</div>
<div class="symbol-result-grid">
<div class="symbol-result">
<div class="label">최고수익</div>
<div class="number {{'pos' if s.member_stats.best_return_pct is not none and s.member_stats.best_return_pct >= 0 else 'muted'}}">
{% if s.member_stats.best_return_pct is not none %}
{{'%.2f'|format(s.member_stats.best_return_pct)}}%
{% else %}대기{% endif %}
</div>
</div>
<div class="symbol-result">
<div class="label">평균수익</div>
<div class="number {{'pos' if s.member_stats.average_return_pct is not none and s.member_stats.average_return_pct >= 0 else 'muted'}}">
{% if s.member_stats.average_return_pct is not none %}
{{'%.2f'|format(s.member_stats.average_return_pct)}}%
{% else %}대기{% endif %}
</div>
</div>
<div class="symbol-result">
<div class="label">평균 보유</div>
<div class="number">
{% if s.member_stats.average_holding_minutes is not none %}
{{'%.0f'|format(s.member_stats.average_holding_minutes)}}분
{% else %}대기{% endif %}
</div>
</div>
<div class="symbol-result">
<div class="label">승률</div>
<div class="number {{'pos' if s.member_stats.win_rate_pct is not none and s.member_stats.win_rate_pct >= 50 else 'neg'}}">
{% if s.member_stats.win_rate_pct is not none %}
{{'%.1f'|format(s.member_stats.win_rate_pct)}}%
{% else %}대기{% endif %}
</div>
</div>
</div>
<div class="summary" style="margin-top:12px;margin-bottom:0">
<span class="badge ok">완료 {{s.completed_cycle_count}}</span>
<span class="badge warn">청산대기 {{s.open_low_count}}</span>
</div>
</a>
{% endfor %}
</div>
{% else %}
<a class="back-link" href="/performance/dashboard?category={{selected_category}}">← 종목 목록으로</a>
{% set s = selected_symbol %}

<div class="card">
<h2>{{symbol_display(s.symbol, s.exchange)}} <span class="small">{{exchange_only_label(s.exchange)}}</span></h2>

<div class="summary">
<span class="badge">저점 {{s.low_count}}</span>
<span class="badge">고점 {{s.high_count}}</span>
<span class="badge ok">완료 사이클 {{s.completed_cycle_count}}</span>
<span class="badge warn">청산 대기 저점 {{s.open_low_count}}</span>
<span class="badge">진입 전 고점 {{s.high_only_count}}</span>
</div>

{% if s.performance_summary.has_results %}
<div class="grid">
<div class="metric">
<div class="title">종목 승률</div>
<div class="value {{'pos' if s.member_stats.win_rate_pct >= 50 else 'neg'}}">
{{'%.1f'|format(s.member_stats.win_rate_pct)}}%
</div>
<div class="small">승 {{s.member_stats.win_count}} · 패 {{s.member_stats.loss_count}}</div>
</div>
<div class="metric">
<div class="title">최고 / 최저 수익</div>
<div class="value">
<span class="pos">{{'%.2f'|format(s.member_stats.best_return_pct)}}%</span>
<span class="small"> / </span>
<span class="{{'pos' if s.performance_summary.worst_return_pct >= 0 else 'neg'}}">{{'%.2f'|format(s.member_stats.worst_return_pct)}}%</span>
</div>
</div>
<div class="metric">
<div class="title">완료 사이클</div>
<div class="value">{{s.member_stats.completed_cycle_count}}회</div>
</div>
</div>


<details open>
<summary>진입 시간봉 × 청산 시간봉 완료 사이클 성과</summary>
<div class="analysis-note">
관리자 설정의 진입 최대 횟수로 평균 진입가를 계산한다.
최초 유효 고점 청산 후 다음 LOW부터 새 사이클로 센다.
같은 사이클의 여러 청산 시간봉은 비교 시나리오이며 사이클 수를 중복 증가시키지 않는다.
</div>
{% if entry_exit_matrix.has_results %}
<table>
<tr><th>최초 진입 시간봉</th><th>청산 시간봉</th><th>완료 사이클</th><th>평균수익</th><th>최고수익</th><th>최저수익</th><th>승률</th><th>평균 보유</th></tr>
{% for stat in entry_exit_matrix.rows %}
<tr>
<td>{{stat.entry_timeframe}}</td><td>{{stat.exit_timeframe}}</td><td>{{stat.trade_count}}회</td>
<td class="{{'pos' if stat.average_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(stat.average_return_pct)}}%</td>
<td class="{{'pos' if stat.best_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(stat.best_return_pct)}}%</td>
<td class="{{'pos' if stat.worst_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(stat.worst_return_pct)}}%</td>
<td>{{'%.1f'|format(stat.win_rate_pct)}}%</td><td>{{stat.average_holding_text}}</td>
</tr>
{% endfor %}
</table>
{% else %}<div class="empty-note">완료된 사이클이 아직 없습니다.</div>{% endif %}
</details>

<details>
<summary>실제 사이클별 진입·청산 가격 차트</summary>
{% for position in s.group_analysis.positions|reverse %}
{% if position.cycle_closed and position.exit_results %}
<div class="card">
<div class="small">사이클 #{{position.position_sequence}} · 최초 {{position.entry_timeframe}} · 평균 진입가 {{position.entry_price}}</div>
{{price_path_svg(position)|safe}}
</div>
{% endif %}
{% else %}<div class="empty-note">표시할 완료 사이클이 없습니다.</div>{% endfor %}
</details>

{% if s.open_cycle_preview %}
<div class="mode-title">현재 진행 중인 진입 구간</div>
<div class="grid">
<div class="metric">
<div class="title">최대시간봉 진입 후보</div>
<div class="value">{{s.open_cycle_preview.max_timeframe_entry.timeframe}} · {{s.open_cycle_preview.max_timeframe_entry.price}}</div>
<div class="small">{{s.open_cycle_preview.max_timeframe_entry.signal_no}}</div>
</div>
<div class="metric">
<div class="title">전체 분할진입 평균가</div>
<div class="value">{{s.open_cycle_preview.all_split_average_price}}</div>
<div class="small">총 {{s.open_cycle_preview.entry_count}}회 분할</div>
</div>
<div class="metric">
<div class="title">수익률 · 보유시간</div>
<div class="value warn">청산 고점 대기</div>
<div class="small">고점 신호가 발생하면 자동 계산</div>
</div>
</div>

<details>
<summary>시간봉별 분할진입 평균가</summary>
<table>
<tr><th>시간봉</th><th>진입 횟수</th><th>평균 진입가</th><th>마지막 진입 시각</th></tr>
{% for tf in s.open_cycle_preview.timeframe_splits %}
<tr>
<td>{{tf.timeframe}}</td>
<td>{{tf.entry_count}}</td>
<td>{{tf.average_entry_price}}</td>
<td>{{tf.last_entry_time}}</td>
</tr>
{% endfor %}
</table>
</details>

<details>
<summary>개별 진입 {{s.open_cycle_preview.entry_count}}건</summary>
<table>
<tr><th>신호번호</th><th>시간봉</th><th>가격</th><th>시각</th></tr>
{% for e in s.open_cycle_preview.individual_entries %}
<tr><td>{{e.signal_no}}</td><td>{{e.timeframe}}</td><td>{{e.price}}</td><td>{{e.time}}</td></tr>
{% endfor %}
</table>
</details>
{% endif %}

{% for c in s.completed_cycles %}
<details>
<summary>완료 Cycle {{c.cycle_no}} · 진입 {{c.entry_count}}회 · 청산후보 {{c.exit_count}}회</summary>

<div class="grid">
<div class="metric">
<div class="title">최대시간봉 진입</div>
<div class="value">{{c.entry_preview.max_timeframe_entry.timeframe}} · {{c.entry_preview.max_timeframe_entry.price}}</div>
</div>
<div class="metric">
<div class="title">전체 분할진입 평균가</div>
<div class="value">{{c.entry_preview.all_split_average_price}}</div>
<div class="small">{{c.entry_preview.entry_count}}회 분할</div>
</div>
<div class="metric">
<div class="title">청산 후보</div>
<div class="value">{{c.exit_count}}건</div>
</div>
</div>

<div class="mode-title">청산 후보별 진입 방식 비교</div>
<div style="overflow-x:auto">
<table>
<tr>
<th>청산 시간봉</th>
<th>청산가</th>
<th>TF 관계</th>
<th>최대TF</th>
<th>전체분할</th>
<th>시간봉별 분할</th>
<th>개별 평균</th>
<th>개별 최고</th>
<th>개별 최저</th>
<th>평균 보유</th>
</tr>
{% for r in c.exit_results %}
<tr>
<td>{{r.exit.timeframe}}</td>
<td>{{r.exit.price}}</td>
<td>{{r.relation_to_max_entry}}</td>
<td class="{{'pos' if r.max_timeframe_return_pct >= 0 else 'neg'}}">
{{'%.3f'|format(r.max_timeframe_return_pct)}}%
</td>
<td class="{{'pos' if r.all_split_return_pct >= 0 else 'neg'}}">
{{'%.3f'|format(r.all_split_return_pct)}}%
</td>
<td>
{% for tf in r.timeframe_split_results %}
<div>
<span class="blue">{{tf.timeframe}}</span>
<span class="{{'pos' if tf.return_pct >= 0 else 'neg'}}">
{{'%.3f'|format(tf.return_pct)}}%
</span>
</div>
{% endfor %}
</td>
<td class="{{'pos' if r.individual_summary.average_return_pct >= 0 else 'neg'}}">
{{'%.3f'|format(r.individual_summary.average_return_pct)}}%
</td>
<td class="{{'pos' if r.individual_summary.maximum_return_pct >= 0 else 'neg'}}">
{{'%.3f'|format(r.individual_summary.maximum_return_pct)}}%
</td>
<td class="{{'pos' if r.individual_summary.minimum_return_pct >= 0 else 'neg'}}">
{{'%.3f'|format(r.individual_summary.minimum_return_pct)}}%
</td>
<td>{{'%.0f'|format(r.individual_summary.average_holding_minutes)}}분</td>
</tr>
{% endfor %}
</table>
</div>

{% for r in c.exit_results %}
<details>
<summary>
상세 보기 · 청산 {{r.exit.timeframe}} · {{r.exit.price}} ·
최대TF <span class="{{'pos' if r.max_timeframe_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(r.max_timeframe_return_pct)}}%</span> ·
전체분할 <span class="{{'pos' if r.all_split_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(r.all_split_return_pct)}}%</span>
</summary>

<table>
<tr><th>청산 신호</th><th>청산 관계</th><th>최대TF 수익률</th><th>최대TF 보유</th><th>전체분할 수익률</th><th>전체분할 보유</th></tr>
<tr>
<td>{{r.exit.signal_no}} / {{r.exit.timeframe}} / {{r.exit.price}}</td>
<td>{{r.relation_to_max_entry}}</td>
<td class="{{'pos' if r.max_timeframe_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(r.max_timeframe_return_pct)}}%</td>
<td>{{r.max_timeframe_holding_minutes}}분</td>
<td class="{{'pos' if r.all_split_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(r.all_split_return_pct)}}%</td>
<td>{{r.all_split_holding_minutes}}분</td>
</tr>
</table>

<div class="mode-title">시간봉별 분할진입 결과</div>
<table>
<tr><th>시간봉</th><th>진입 횟수</th><th>평균가</th><th>수익률</th><th>보유시간</th></tr>
{% for tf in r.timeframe_split_results %}
<tr>
<td>{{tf.timeframe}}</td><td>{{tf.entry_count}}</td><td>{{tf.average_entry_price}}</td>
<td class="{{'pos' if tf.return_pct >= 0 else 'neg'}}">{{'%.3f'|format(tf.return_pct)}}%</td>
<td>{{tf.holding_minutes}}분</td>
</tr>
{% endfor %}
</table>

<details>
<summary>각 개별 진입 결과 {{r.individual_results|length}}건</summary>
<table>
<tr><th>진입 신호</th><th>시간봉</th><th>진입가</th><th>수익률</th><th>보유시간</th></tr>
{% for item in r.individual_results %}
<tr>
<td>{{item.entry.signal_no}}</td><td>{{item.entry.timeframe}}</td><td>{{item.entry.price}}</td>
<td class="{{'pos' if item.return_pct >= 0 else 'neg'}}">{{'%.3f'|format(item.return_pct)}}%</td>
<td>{{item.holding_minutes}}분</td>
</tr>
{% endfor %}
</table>
</details>
</details>
{% endfor %}
</details>
{% endfor %}

{% if s.high_only %}
<details>
<summary>진입 저점 전에 발생한 고점 {{s.high_only_count}}건</summary>
<table>
<tr><th>신호번호</th><th>시간봉</th><th>가격</th><th>시각</th></tr>
{% for e in s.high_only %}
<tr><td>{{e.signal_no}}</td><td>{{e.timeframe}}</td><td>{{e.price}}</td><td>{{e.time}}</td></tr>
{% endfor %}
</table>
</details>
{% endif %}
</div>
{% endif %}
</section>
{% endif %}
{% endif %}
</body>
</html>
        """,
        data=data,
        selected=selected,
        selected_category=selected_category,
        selected_symbol=selected_symbol,
        selected_symbol_name=selected_symbol_name,
        market_stats=market_stats,
        entry_exit_matrix=entry_exit_matrix,
        ), 200

    except Exception as exc:
        log.exception("Performance dashboard failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/performance/group-analysis", methods=["GET", "POST"])
@admin_required
def performance_group_analysis():
    if request.method == "POST":
        try:
            recent_n = int(request.form.get("recent_interval_count", "5"))
            entry_split_limit = int(request.form.get("entry_split_limit", "3"))
            update_group_settings(
                recent_interval_count=recent_n,
                entry_split_limit=entry_split_limit,
            )
            flash("최근 평균 횟수와 진입 최대 횟수를 저장했습니다.")
        except Exception as exc:
            flash(f"설정 저장 실패: {exc}")
        market = request.form.get("market", "KOREA")
        symbol = request.form.get("symbol", "")
        return redirect(f"/performance/group-analysis?market={market}&symbol={symbol}")

    market = request.args.get("market", "KOREA").upper()
    symbol = request.args.get("symbol", "")
    data = group_analysis_data(market=market, symbol=symbol)

    return render_template_string("""
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>포지션 그룹 성과 분석</title>
<style>
:root{--bg:#09090b;--card:#17171a;--line:#303036;--text:#f4f4f5;--muted:#a1a1aa;--yellow:#facc15}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Arial,sans-serif}
.wrap{max-width:1450px;margin:auto;padding:22px}.top{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
a{color:#fde047;text-decoration:none}.tab,.btn{padding:10px 14px;border-radius:9px;border:1px solid var(--line);background:#202024;color:white}
.tab.on{background:#854d0e;border-color:#eab308}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:17px;margin-top:15px}
table{width:100%;border-collapse:collapse;margin-top:10px;min-width:900px}th,td{padding:10px;border-bottom:1px solid #2d2d32;text-align:left}
th{color:#d4d4d8}.scroll{overflow-x:auto}.pos{color:#4ade80}.neg{color:#fb7185}.muted{color:var(--muted);font-size:13px}
input,select{background:#101012;color:white;border:1px solid #3f3f46;border-radius:8px;padding:9px}
details{background:#121215;border:1px solid #2c2c31;border-radius:11px;padding:12px;margin-top:10px}
summary{cursor:pointer;font-weight:bold}
</style>
</head>
<body><div class="wrap">
<div class="top">
<a class="btn" href="/performance/dashboard">← 기존 관리자 대시보드</a>
<h1 style="margin:0 14px 0 0">포지션 그룹 성과 분석</h1>
{% for item in data.markets %}
<a class="tab {{'on' if item == data.market else ''}}" href="?market={{item}}">{{ {'KOREA':'국장','US':'미장','COIN':'코인'}[item] }}</a>
{% endfor %}
</div>

<div class="card">
<form method="get" class="top">
<input type="hidden" name="market" value="{{data.market}}">
<label>종목
<select name="symbol" onchange="this.form.submit()">
{% for item in data.symbols %}
<option value="{{item}}" {{'selected' if item == data.symbol else ''}}>{{symbol_display(item, 'KRX' if data.market == 'KOREA' else '')}}</option>
{% endfor %}
</select></label>
</form>
{% if not data.symbol %}<p class="muted">해당 시장에 저장된 종목 신호가 아직 없습니다.</p>{% endif %}
</div>

<div class="card">
<h2>분석 설정</h2>
<form method="post" class="top">
<input type="hidden" name="market" value="{{data.market}}">
<input type="hidden" name="symbol" value="{{data.symbol or ''}}">
<label>최근 평균 횟수
<input type="number" min="1" max="100" name="recent_interval_count" value="{{data.settings.recent_interval_count}}">
</label>
<label>진입 최대 횟수
<input type="number" min="1" max="10" name="entry_split_limit" value="{{data.settings.entry_split_limit}}">
</label>
<button class="btn" type="submit">저장 후 재계산</button>
<span class="muted">포지션별 진입 쿨타임 {{data.settings.entry_cooldown_minutes}}분 · 시간봉 자체 쿨타임은 발생주기 통계에만 적용</span>
</form>
</div>

{% if data.symbol %}
<div class="card">
<h2>{{symbol_display(data.symbol, 'KRX' if data.market == 'KOREA' else '')}} 매수 그룹 → 매도 그룹 누적 성과</h2>
<div class="scroll"><table>
<tr><th>매수 그룹</th><th>최초 매수 시간봉</th><th>매도 그룹</th><th>매도 시간봉</th><th>완료</th><th>평균수익</th><th>최고</th><th>최저</th><th>승률</th><th>평균 보유</th></tr>
{% for row in data.performance_summary %}
<tr><td>{{row.entry_group_label}}</td><td>{{row.entry_timeframe}}</td>
<td>{{row.exit_group_label}}</td><td>{{row.exit_timeframe}}</td><td>{{row.trade_count}}사이클</td>
<td class="{{'pos' if row.average_return_pct >= 0 else 'neg'}}">{{'%.3f'|format(row.average_return_pct)}}%</td>
<td>{{'%.3f'|format(row.best_return_pct)}}%</td><td>{{'%.3f'|format(row.worst_return_pct)}}%</td>
<td>{{'%.1f'|format(row.win_rate_pct)}}%</td><td>{{row.average_holding_text}}</td></tr>
{% else %}<tr><td colspan="10">완료된 시간봉별 포지션이 아직 없습니다.</td></tr>{% endfor %}
</table></div>
</div>

<div class="card">
<h2>타점 발생 주기</h2>
<div class="scroll"><table>
<tr><th>그룹</th><th>시간봉</th><th>누적 발생</th><th>누적 평균</th><th>최근 {{data.settings.recent_interval_count}}회 평균</th><th>최단</th><th>최장</th><th>마지막 발생 후</th></tr>
{% for row in data.occurrence_stats %}
<tr><td>{{row.group_label}}</td><td>{{row.timeframe}}</td><td>{{row.occurrence_count}}회</td>
<td>{{row.overall_average_text}}</td><td>{{row.recent_average_text}}</td><td>{{row.minimum_text}}</td><td>{{row.maximum_text}}</td><td>{{row.elapsed_text}}</td></tr>
{% else %}<tr><td colspan="8">주기를 계산할 저점 신호가 없습니다.</td></tr>{% endfor %}
</table></div>
</div>

<div class="card">
<h2>실제 백데이터: 진입·청산 시각</h2>
{% for position in data.positions|reverse %}
<details>
<summary>최초 {{position.entry_timeframe}} 포지션 · {{position.entry_count}}회 진입 · 평균가 {{position.entry_price}}</summary>
<p><b>실제 진입 구성:</b> {{position.entry_source_summary}}<br>
첫 진입 {{position.entry_first_time}}<br>마지막 진입 {{position.entry_last_time}}<br>
상태: {{'3회 진입 완료' if position.entry_complete else '유효 진입만 계산'}}</p>
<div class="scroll"><table>
<tr><th>청산 그룹</th><th>청산 시간봉</th><th>청산 시각</th><th>청산가</th><th>보유기간</th><th>수익률</th></tr>
{% for exit in position.exit_results %}
<tr><td>{{exit.exit_group_label}}</td><td>{{exit.exit_timeframe}}</td><td>{{exit.exit_time}}</td><td>{{exit.exit_price}}</td>
<td>{{exit.holding_text}}</td><td class="{{'pos' if exit.return_pct >= 0 else 'neg'}}">{{'%.3f'|format(exit.return_pct)}}%</td></tr>
{% else %}<tr><td colspan="6">아직 유효한 첫 고점 청산이 없습니다.</td></tr>{% endfor %}
</table></div>
</details>
{% else %}<p class="muted">생성된 매수 포지션이 없습니다.</p>{% endfor %}
</div>
{% endif %}
</div></body></html>
""", data=data), 200


@app.get("/performance/cycles")
@admin_required
def performance_cycles_json():
    try:
        try:
            limit = int(request.args.get("limit", "30"))
        except ValueError:
            limit = 30
        return jsonify(visual_cycle_data(limit)), 200
    except Exception as e:
        log.exception("Performance cycles failed")
        return jsonify({"ok": False, "error": str(e)}), 500

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
        return jsonify({"ok": True, "skipped": "cooldown", "bucket": bucket}), 200

    if _is_duplicate(bucket, msg_norm):
        return jsonify({"ok": True, "skipped": "dedup", "bucket": bucket}), 200

    def _send_telegram_background():
        try:
            res = post_telegram(chat_id, msg_norm)
            if not bool(res.get("ok")):
                log.error(f"TG send failed: {res} (route={route}, symbol={symbol})")
                return
            _mark_sent(bucket)
            log.info(f"TG sent ok route={route} symbol={symbol}")
        except Exception:
            log.exception(f"Telegram send exception route={route} symbol={symbol}")

    threading.Thread(target=_send_telegram_background, daemon=True).start()

    return jsonify({"ok": True, "queued": True}), 200

# --- old endpoint (legacy for 불꽃타점) ---
@app.post("/bot")
def tv_webhook_legacy():
    data = request.get_json(silent=True, force=True) or {}
    bad = _require_webhook_secret(data)
    if bad: return bad
    # 통계 저장은 별도 스레드에서 실행. 실패해도 기존 텔레그램 전송에는 영향 없음.
    queue_signal_save(data)
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
    # /webhook 경로도 동일하게 원본 신호를 저장한다.
    queue_signal_save(data)
    route  = str(data.get("type", data.get("route", ""))).strip()
    msg    = str(data.get("message", data.get("msg", ""))).strip()
    symbol = str(data.get("symbol", "")).strip()
    return _handle_payload(route, msg, symbol)

def _is_oneway() -> bool:
    # 기본 HEDGE. 환경변수로 ONEWAY 라고 넣으면 원웨이 처리
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
    """
    TV/내부 저장에는 ETHUSDT.P 같은 것을 쓰더라도,
    바이낸스 API 호출 시에는 .P 등을 제거한 정규 심볼을 사용한다.
    """
    if not sym:
        return sym
    s = sym.strip().upper()
    if s.endswith(".P"):
        s = s[:-2]
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s

def _decimals_from_step(step: float) -> int:
    s = f"{step:.16f}".rstrip('0')
    return len(s.split('.')[-1]) if '.' in s else 0

def round_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
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
    "pairs": {}               # "BTCUSDT.P": {...}
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
def ui_get(chat_id: int) -> dict: return UI.setdefault(chat_id, {"mode":"idle", "cfg":{}})
def ui_reset(chat_id: int): UI[chat_id] = {"mode":"idle", "cfg":{}}

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
                post_telegram(chat_id, "먼저 종목을 입력하세요.", reply_markup=kb_main(st["cfg"]))
                return jsonify({"ok":True})
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
            msgtxt = (f"✅ 저장 완료\nSYMBOL: {sym}\nDIR: {mode}\nLEV: {ep['lev']}x\n"
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
            for s,c in STATE["pairs"].items():
                lines.append(f"{s}: {c}")
            post_telegram(chat_id, "SETTINGS\n" + "\n".join(lines))
            return jsonify({"ok": True})

        return jsonify({"ok": True})

    return jsonify({"ok": True})

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

def _unsupported_symbol_reason(base_sym: str) -> Optional[str]:
    """선물 미상장/지원 불가 심볼 여부를 간단히 탐지."""
    try:
        f = get_symbol_filters(base_sym)
        if not f:
            return "unsupported symbol on Binance Futures"
        if "PRICE_FILTER" not in f or "LOT_SIZE" not in f:
            return "missing filters (PRICE_FILTER/LOT_SIZE)"
    except Exception as e:
        return f"filter check error: {e}"
    return None

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
        return jsonify({"ok": False, "error": str(e)}), 200

@app.post("/bnc/trade")
def bnc_trade():
    """
    Body (Pine Stage2):
      {"secret":"<BNC_SECRET>", "symbol":"BTCUSDT.P", "action":"OPEN_LONG|OPEN_SHORT|CLOSE_LONG|CLOSE_SHORT", "note":"tf=..."}
    qty는 비워도 서버가 자동 계산.
    """
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

        reason = _unsupported_symbol_reason(base_sym)
        if reason:
            try:
                bnc_token = os.getenv("BNC_BOT_TOKEN"); bnc_chat = os.getenv("BNC_CHAT_ID")
                if bnc_token and bnc_chat:
                    post_telegram_with_token(bnc_token, bnc_chat, f"[TRADE/SKIP] {symbol_orig} → {base_sym}\nReason: {reason}")
            except Exception:
                pass
            return jsonify({"ok": True, "skipped": "unsupported", "reason": reason}), 200

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
    # 새 포맷: {"symbol":"BTCUSDT.P","side":"BUY"}
    # 구 포맷: {"symbol":"BTCUSDT.P","sig":"LONG_5m"}

    symbol_orig = str(data.get("symbol", "")).upper()
    side        = str(data.get("side", "")).upper()
    sig         = str(data.get("sig", "")).upper()

    if not symbol_orig:
        return jsonify({"ok": False, "error": "missing symbol"}), 200

    action = None

    if side:
        if side in ("BUY", "LONG"):
            action = "OPEN_LONG"
        elif side in ("SELL", "SHORT"):
            action = "OPEN_SHORT"
        else:
            return jsonify({"ok": False, "error": f"unsupported side: {side}"}), 200
    elif sig:
        if sig.startswith("LONG"):
            action = "OPEN_LONG"
        elif sig.startswith("SHORT"):
            action = "OPEN_SHORT"
        else:
            return jsonify({"ok": True, "skipped": "unknown-sig"}), 200

    if not action:
        return jsonify({"ok": False, "error": "missing side/sig"}), 200

    note = f"tf={data.get('tf','')}, price={data.get('p','')}, side={side or sig}"

    private_base = os.getenv("PRIVATE_BASE", "http://bbangdol-bnc-bot-private:10000")
    payload = {
        "secret": os.getenv("BNC_SECRET", ""),
        "symbol": symbol_orig,
        "action": action,
        "note":   note
    }
    try:
        r = requests.post(f"{private_base}/bnc/trade", json=payload, timeout=10)
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
    if os.getenv("TG_SET_WEBHOOK_ON_BOOT", "0").lower() in ("1","true","on","yes"):
        try:
            app.logger.info(f"Setting Telegram webhook to {TG_WEBHOOK_BASE}/tg ...")
            _set_webhook()
        except Exception:
            app.logger.exception("setWebhook on boot failed")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
