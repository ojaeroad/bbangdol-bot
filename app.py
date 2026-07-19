# app.py — unified webhook + BNC trade + TG UI (multi-symbol & risk modes)
import os, json, logging, time, re, hmac, hashlib, math, threading
from time import time as now
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlencode
from flask import Flask, request, jsonify, render_template_string
import requests

# 회원 운영용 성과 분석 DB (기존 텔레그램/자동매매와 독립)
from performance_store import queue_signal_save, health_summary, latest_signals
from performance_analyzer import rebuild_individual_pairs, analysis_summary, latest_analysis_pairs, visual_cycle_data

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bbangdol-bot")

# ---- Version / Service markers (for live check) ----
APP_VERSION  = os.getenv("APP_VERSION", "dev")
SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown")

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

@app.get("/version")
def version():
    return jsonify({"service": SERVICE_NAME, "version": APP_VERSION})

@app.get("/whoami")
def whoami():
    return jsonify({"service": SERVICE_NAME})

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
@app.get("/performance/health")
def performance_health():
    try:
        result = health_summary()
        return jsonify(result), (200 if result.get("ok") else 503)
    except Exception as e:
        log.exception("Performance health check failed")
        return jsonify({"ok": False, "database": "error", "error": str(e)}), 503



# --- 최근 저장 신호 확인 ---
@app.get("/performance/latest")
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


@app.get("/performance/dashboard")
def performance_dashboard():
    try:
        try:
            limit = int(request.args.get("limit", "30"))
        except ValueError:
            limit = 30

        data = visual_cycle_data(limit)

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
table{width:100%;border-collapse:collapse;margin-top:10px;font-size:14px}
th,td{border-bottom:1px solid var(--line);padding:9px;text-align:left;vertical-align:top}
th{color:var(--blue)}
details{margin:10px 0;background:#141416;border-radius:10px;padding:11px}
summary{cursor:pointer;font-weight:bold}
.small{font-size:12px;color:#aaa}.pos{color:var(--green);font-weight:bold}
.mode-title{font-size:18px;color:var(--blue);margin:14px 0 4px}
.category-nav{display:flex;gap:10px;flex-wrap:wrap;margin:16px 0 22px}
.category-nav a{background:#242427;border:1px solid #3a3a3d;border-radius:999px;padding:9px 14px;text-decoration:none}
.category-head{display:flex;justify-content:space-between;gap:15px;align-items:center;margin:32px 0 10px;padding:14px 16px;background:#171719;border-left:5px solid var(--blue);border-radius:10px}
.category-head h2{margin:0}
.category-summary{display:flex;gap:9px;flex-wrap:wrap}
.empty-note{color:#999;padding:14px 0}
@media(max-width:800px){.grid{grid-template-columns:1fr}body{padding:10px}h1{font-size:27px}}
</style>
</head>
<body>
<h1>회원 운영용 성과 분석</h1>
<div class="toplinks">
<a href="/performance/health">DB 상태</a> ·
<a href="/performance/latest">최근 신호</a> ·
<a href="/performance/analyze">분석 실행</a> ·
<a href="/performance/cycles">사이클 JSON</a>
</div>

<div class="category-nav">
{% for category in data.categories %}
<a href="#{{category.anchor}}">
{{category.category_label}} · 종목 {{category.symbol_count}}
</a>
{% endfor %}
</div>

{% for category in data.categories %}
<section id="{{category.anchor}}">
<div class="category-head">
<h2>{{category.category_label}}</h2>
<div class="category-summary">
<span class="badge">종목 {{category.symbol_count}}</span>
<span class="badge">신호 {{category.signal_count}}</span>
<span class="badge ok">완료 Cycle {{category.completed_cycle_count}}</span>
<span class="badge warn">청산 대기 {{category.open_low_count}}</span>
</div>
</div>

{% if category.symbol_count == 0 %}
<div class="card">
<div class="empty-note">
현재 저장된 {{category.category_label}} 신호가 없습니다.<br>
해당 1Q TradingView 알람이 웹훅으로 들어오면 종목과 성과가 자동 표시됩니다.
</div>
</div>
{% endif %}

{% for s in category.symbols %}
<div class="card">
<h2>{{s.symbol}} <span class="small">{{s.strategy}} / {{s.exchange}}</span></h2>

<div class="summary">
<span class="badge">저점 {{s.low_count}}</span>
<span class="badge">고점 {{s.high_count}}</span>
<span class="badge ok">완료 사이클 {{s.completed_cycle_count}}</span>
<span class="badge warn">청산 대기 저점 {{s.open_low_count}}</span>
<span class="badge">진입 전 고점 {{s.high_only_count}}</span>
</div>

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

{% for r in c.exit_results %}
<details>
<summary>
청산 {{r.exit.timeframe}} · {{r.exit.price}} ·
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
{% endfor %}
</section>
{% endfor %}
</body>
</html>
        """, data=data), 200

    except Exception as exc:
        log.exception("Performance dashboard failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

@app.get("/performance/cycles")
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
