# econ_calendar_tele_bot.py
# -*- coding: utf-8 -*-
"""
TE 경제 캘린더 알림 (안정화·스케줄러 유지판)

ENV:
  ECON_CAL_ENABLED            : "1"이면 활성, "0"/미설정이면 비활성
  ECON_TG_TOKEN | TELEGRAM_BOT_TOKEN
  ECON_CHAT_ID  | TELEGRAM_CHAT_ID
  TE_AUTH                     : "email:apikey" (미설정시 "guest:guest")
  ECON_COUNTRIES              : 기본 "United States"
  ECON_IMPORTANCE             : 기본 "2,3"
  ECON_PREVIEW_TIMES          : 기본 "08:55,20:55" (Asia/Singapore)
  ECON_POLL_SEC               : 기본 60
  ECON_RELEASE_LOOKAHEAD_MIN  : 기본 5
  ECON_ADMIN_KEY              : /econ/preview_now?key=...
"""

import os, time, logging, random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone, utc

try:
    from flask import Blueprint, request
except Exception:
    Blueprint = None
    request = None

log = logging.getLogger("econ-calendar")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# ── ENV/상수 ─────────────────────────────────────────────────────────
ENABLED = os.getenv("ECON_CAL_ENABLED", "0") not in ("0", "false", "False", "", None)

ASIA_SG = timezone("Asia/Singapore")
TE_BASE = "https://api.tradingeconomics.com/calendar"
TE_AUTH = os.getenv("TE_AUTH", "guest:guest")

TG_TOKEN = os.getenv("ECON_TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("ECON_CHAT_ID")  or os.getenv("TELEGRAM_CHAT_ID", "")

COUNTRIES     = [s.strip() for s in os.getenv("ECON_COUNTRIES", "United States").split(",") if s.strip()]
IMPORTANCE    = [s.strip() for s in os.getenv("ECON_IMPORTANCE", "2,3").split(",") if s.strip()]
PREVIEW_TIMES = [s.strip() for s in os.getenv("ECON_PREVIEW_TIMES", "08:55,20:55").split(",") if s.strip()]

POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))
RELEASE_LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))
ADMIN_KEY = os.getenv("ECON_ADMIN_KEY", "")

# 재시도/백오프 HTTP 세션
def _build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=3, connect=3, read=3, backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    ad = HTTPAdapter(max_retries=r, pool_connections=10, pool_maxsize=10)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s

HTTP = _build_session()
REQUEST_TIMEOUT = (4, 12)  # (connect, read)

# 쿨다운/중복 방지
_LAST_QUERY_AT: Dict[str, float] = {}
_COOLDOWN_SEC = 300  # 5분
def _cd_key(d1: str, d2: str, countries: List[str], imp: List[str]) -> str:
    return f"{d1}|{d2}|{','.join(countries)}|{','.join(imp)}"

# ── 유틸 ──────────────────────────────────────────────────────────────
def _sg_now() -> datetime:
    return datetime.now(ASIA_SG)

def _to_sg(dt_utc_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(dt_utc_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(dt_utc_str, "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=utc)
    return dt.astimezone(ASIA_SG)

def _day_range_sg(dt_sg: datetime) -> Tuple[str, str]:
    """TE 호출용 날짜 범위(YYYY-MM-DD); 시간 미포함"""
    d1 = dt_sg.strftime("%Y-%m-%d")
    d2 = (dt_sg + timedelta(days=1)).strftime("%Y-%m-%d")
    return d1, d2

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT or not text:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=(3, 10),
        )
    except Exception as e:
        log.error("telegram send failed: %s", repr(e))

# ── TE 호출 (안정화) ──────────────────────────────────────────────────
def _fetch_day(d1_ymd: str, d2_ymd: str) -> List[Dict[str, Any]]:
    """TE: 날짜는 YYYY-MM-DD만 사용; 나머지 필터는 클라이언트에서."""
    params = {
        "c": TE_AUTH,
        "format": "json",
        "country": ",".join(COUNTRIES),       # 공백 포함 그대로; requests가 인코딩
        "d1": d1_ymd,
        "d2": d2_ymd,
        "importance": ",".join(IMPORTANCE),
    }
    try:
        # 인스턴스 충돌 완화용 지터
        time.sleep(random.uniform(0, 0.8))
        r = HTTP.get(TE_BASE, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            log.warning("TE payload not list: %s", type(data))
            return []
        return data
    except requests.HTTPError as e:
        log.error("TE fetch error: %s", str(e))
        return []
    except Exception as e:
        log.error("TE fetch exception: %s", repr(e))
        return []

def fetch_events_range_sg(d1_sg: datetime, d2_sg: datetime) -> List[Dict[str, Any]]:
    """SGT 윈도우(fine-grain)를 TE day API로 가져와서 로컬 필터."""
    # 5분 쿨다운
    d1_ymd, d2_ymd = _day_range_sg(d1_sg)
    cdkey = _cd_key(d1_ymd, d2_ymd, COUNTRIES, IMPORTANCE)
    now = time.time()
    if now - _LAST_QUERY_AT.get(cdkey, 0) < _COOLDOWN_SEC:
        log.info("cooldown skip: %s", cdkey)
    raw = _fetch_day(d1_ymd, d2_ymd)
    _LAST_QUERY_AT[cdkey] = now

    # 국가/시간 정밀 필터
    out = []
    for e in raw:
        if e.get("Country") not in COUNTRIES:
            continue
        try:
            t_sg = _to_sg(e.get("Date") or e.get("DateTime"))
        except Exception:
            continue
        if d1_sg <= t_sg < d2_sg:
            out.append(e)
    out.sort(key=lambda x: x.get("Date", ""))
    return out

def fetch_events_24h(now_sg: datetime) -> List[Dict[str, Any]]:
    return fetch_events_range_sg(now_sg, now_sg + timedelta(hours=24))

# ── 분류/메시지 ──────────────────────────────────────────────────────
IMPORTANT_KEYWORDS = [
    "CPI","Core CPI","PCE","Core PCE","FOMC","GDP","Non-Farm","Unemployment","Retail Sales",
    "Fed Interest Rate Decision","Fed Press Conference","Minutes"
]
SPEECH_KEYWORDS = ["speech","speaks","remarks","press conference","testifies","testimony","hearing"]
SPEECH_FIGURES  = ["Powell","Federal Reserve","Fed","FOMC","Yellen","Lagarde","ECB","BOE","BOJ","SNB","Waller","Williams","Kashkari"]

CRYPTO_SCENARIOS = {
    "CPI": ("실제치가 예상보다 높음 → <b>단기 약세</b>","예상 부합","예상보다 낮음 → <b>우호적</b>"),
    "Core CPI": ("상회 → <b>약세</b>","부합","하회 → <b>우호적</b>"),
    "PCE": ("상회 → <b>약세</b>","부합","하회 → <b>우호적</b>"),
    "Core PCE": ("상회 → <b>약세</b>","부합","하회 → <b>우호적</b>"),
    "NFP": ("상회 → 수익률↑ → <b>압박</b>","부합","하회 → 달러↓ → <b>우호적</b>"),
    "Unemployment Rate": ("하락 → 과열 → <b>압박</b>","부합","상승 → <b>우호적</b>"),
    "Retail Sales": ("상회 → 수요 견조 → <b>압박</b>","부합","하회 → 둔화 → <b>우호적</b>"),
    "GDP": ("상회 → 긴축 장기화 우려 → <b>중립~약세</b>","부합","하회 → 둔화 기대 → <b>우호적</b>"),
    "FOMC": ("매파 → <b>약세</b>","중립","비둘기 → <b>우호적</b>")
}

def is_speech(e: Dict[str, Any]) -> bool:
    title = (e.get("Event") or e.get("Category") or "").lower()
    if any(k in title for k in SPEECH_KEYWORDS):
        return True
    if any(p.lower() in title for p in SPEECH_FIGURES) and any(k in title for k in ["speech","remarks","speaks","press","testifies","testimony","hearing"]):
        return True
    return False

def _classify_event(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    for k in IMPORTANT_KEYWORDS:
        if k.lower() in title.lower():
            return k
    return "FOMC" if "fed" in title.lower() or "fomc" in title.lower() else ""

def _scenario_text(key: str):
    default = ("상회 → 위험자산 <b>압박</b>", "부합", "하회 → 위험자산 <b>우호적</b>")
    return CRYPTO_SCENARIOS.get(key, default)

def build_preview(events: List[Dict[str, Any]], now_sg: datetime) -> str:
    lines = ["<b>🇺🇸 24h 경제이벤트 (사전 시나리오)</b>\n"]
    count = 0
    for e in events:
        title = (e.get("Event") or e.get("Category") or "").strip()
        evttime = _to_sg(e.get("Date") or e.get("DateTime"))
        if is_speech(e):
            lines.append(f"🕒 {evttime.strftime('%m/%d %H:%M')} — 🎤 {title}")
            lines.append("   • 매파 ↘ / 비둘기 ↗\n")
        else:
            forecast, previous = e.get("Forecast"), e.get("Previous")
            key = _classify_event(e)
            up, eq, dn = _scenario_text(key)
            lines.append(f"🕒 {evttime.strftime('%m/%d %H:%M')} — {title}")
            core = []
            if forecast not in (None, ""): core.append(f"예상 {forecast}")
            if previous not in (None, ""): core.append(f"이전 {previous}")
            if core: lines.append("   • " + ", ".join(core))
            lines.append(f"   • 상회: {up}")
            lines.append(f"   • 부합: {eq}")
            lines.append(f"   • 하회: {dn}\n")
        count += 1
        if count >= 12:
            break
    if count == 0:
        lines.append("(24시간 내 고중요 이벤트 없음)")
    # 텔레그램 메시지 길이 보호
    return "\n".join(lines)[:3500]

def build_release_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    t = _to_sg(e.get("Date") or e.get("DateTime"))
    actual, forecast, previous = e.get("Actual"), e.get("Forecast"), e.get("Previous")
    verdict, detail = "중립", "발표 확인"
    key = _classify_event(e)
    up, eq, dn = _scenario_text(key)
    if actual not in (None, "") and forecast not in (None, ""):
        try:
            a = float(str(actual).replace('%','').replace(',',''))
            f = float(str(forecast).replace('%','').replace(',',''))
            diff = a - f
            thr = 0.1 if any(k in key for k in ["CPI","PCE"]) else 0.001
            if abs(diff) <= thr: verdict, detail = "예상치 부합", eq
            elif diff > 0:      verdict, detail = "예상치 상회", up
            else:               verdict, detail = "예상치 하회", dn
        except Exception:
            pass
    info = []
    if actual not in (None, ""):   info.append(f"실제 {actual}")
    if forecast not in (None, ""): info.append(f"예상 {forecast}")
    if previous not in (None, ""): info.append(f"이전 {previous}")
    body = [f"<b>🇺🇸 {title}</b>", f"⏱ {t.strftime('%m/%d %H:%M')} SGT"]
    if info: body.append("📊 " + ", ".join(info))
    body.append(f"💡 해석: <b>{verdict}</b> — {detail}")
    return "\n".join(body)[:1000]

def build_speech_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    t = _to_sg(e.get("Date") or e.get("DateTime"))
    bullets = [
        "• 매파(긴축/인플레 강조) → 달러·수익률 ↑ → <b>압박</b>",
        "• 비둘기(완화/인하 시사) → 달러·수익률 ↓ → <b>우호적</b>",
    ]
    return "\n".join([f"<b>🎤 {title}</b>", f"⏱ {t.strftime('%m/%d %H:%M')} SGT", *bullets])

# ── 잡 ───────────────────────────────────────────────────────────────
_sent_release_keys: Dict[str, float] = {}

def send_preview_job():
    now_sg = _sg_now()
    evts = fetch_events_24h(now_sg)
    # 간단 가중치 정렬
    def score(e):
        t = (e.get("Event") or e.get("Category") or "").lower()
        s = 0
        if is_speech(e): s -= 200
        for i, k in enumerate(IMPORTANT_KEYWORDS):
            if k.lower() in t: s -= (100 - i)
        return s
    evts.sort(key=score)
    msg = build_preview(evts, now_sg)
    if msg: tg_send(msg)

def poll_releases_job():
    now_sg = _sg_now()
    window_end = now_sg + timedelta(minutes=RELEASE_LOOKAHEAD_MIN)
    evts = fetch_events_range_sg(now_sg - timedelta(minutes=1), window_end)
    for e in evts:
        dt = _to_sg(e.get("Date") or e.get("DateTime"))
        # 발표치 등장
        if dt <= now_sg + timedelta(seconds=5) and e.get("Actual"):
            key = f"REL|{e.get('Event')}|{e.get('Date')}|{e.get('Actual')}"
            if key not in _sent_release_keys:
                _sent_release_keys[key] = time.time()
                tg_send(build_release_note(e))
            continue
        # 연설 시작
        if dt <= now_sg + timedelta(seconds=5) and is_speech(e):
            key = f"SPEECH|{e.get('Event')}|{e.get('Date')}"
            if key not in _sent_release_keys:
                _sent_release_keys[key] = time.time()
                tg_send(build_speech_note(e))

def clean_sent_cache_job():
    now = time.time()
    for k in list(_sent_release_keys.keys()):
        if now - _sent_release_keys[k] > 86400:
            _sent_release_keys.pop(k, None)

_scheduler: BackgroundScheduler = None
_bp = None

def init_econ_calendar(app=None):
    """ENABLED이 아니면 아무 것도 하지 않음."""
    global _scheduler, _bp
    if not ENABLED:
        log.info("econ calendar disabled by ENV (ECON_CAL_ENABLED=0)")
        return None
    if _scheduler:
        return _scheduler

    # 스케줄러 시작
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 사전 프리뷰(매일 고정 시각)
    for t in PREVIEW_TIMES:
        hh, mm = t.split(":")
        _scheduler.add_job(send_preview_job, CronTrigger(hour=int(hh), minute=int(mm)))

    # 실적/연설 모니터링 (지터 부여)
    jitter = random.randint(0, 5)
    _scheduler.add_job(poll_releases_job, "interval", seconds=POLL_SEC + jitter)

    # 캐시 청소
    _scheduler.add_job(clean_sent_cache_job, "interval", minutes=30)

    _scheduler.start()

    # 수동 트리거 (옵션)
    if app is not None and Blueprint is not None:
        _bp = Blueprint("econ", __name__)
        @_bp.get("/econ/preview_now")
        def _preview_now():
            if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
                return "forbidden", 403
            send_preview_job()
            return "ok", 200
        app.register_blueprint(_bp)

    log.info(
        "econ calendar started: enabled=1, preview=%s, poll=%ss(+jitter), importance=%s, TE=%s",
        PREVIEW_TIMES, POLL_SEC, IMPORTANCE, "custom" if TE_AUTH!="guest:guest" else "guest"
    )
    return _scheduler
