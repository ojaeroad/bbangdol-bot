# econ_calendar_tele_bot.py
# -*- coding: utf-8 -*-
"""
TradingEconomics 경제 캘린더 알림 (안정화·fail-safe 버전)

ENV
  ECON_CAL_ENABLED            : "1"이면 활성(기본 0=비활성)
  ECON_TG_TOKEN | TELEGRAM_BOT_TOKEN
  ECON_CHAT_ID  | TELEGRAM_CHAT_ID
  TE_AUTH                     : "email:apikey" 또는 "guest:guest"
  ECON_COUNTRIES              : 기본 "United States"
  ECON_IMPORTANCE             : 기본 "2,3"
  ECON_PREVIEW_TIMES          : 기본 "08:55,20:55" (Asia/Singapore)
  ECON_POLL_SEC               : 기본 60
  ECON_RELEASE_LOOKAHEAD_MIN  : 기본 5 (분)
  ECON_ADMIN_KEY              : /econ/preview_now?key=... 보호용
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

# ─────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────
log = logging.getLogger("econ-calendar")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

# ─────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────
ENABLED = os.getenv("ECON_CAL_ENABLED", "0").strip().lower() not in ("0","false","","no","off")

TE_AUTH = os.getenv("TE_AUTH", "guest:guest")

TG_TOKEN = os.getenv("ECON_TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("ECON_CHAT_ID")  or os.getenv("TELEGRAM_CHAT_ID", "")

COUNTRIES  = [s.strip() for s in os.getenv("ECON_COUNTRIES", "United States").split(",") if s.strip()]
IMPORTANCE = [s.strip() for s in os.getenv("ECON_IMPORTANCE", "2,3").split(",") if s.strip()]
PREVIEW_TIMES = [s.strip() for s in os.getenv("ECON_PREVIEW_TIMES", "08:55,20:55").split(",") if s.strip()]

POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))
LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))
ADMIN_KEY = os.getenv("ECON_ADMIN_KEY", "")

ASIA_SG = timezone("Asia/Singapore")

# ─────────────────────────────────────────────────────────────
# HTTP Session (fail-safe)
#  - 5xx/429이면 스킵(재시도 1회), timeout 짧게
# ─────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=1, connect=1, read=1,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False
    )
    ad = HTTPAdapter(max_retries=r, pool_connections=8, pool_maxsize=8)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s

HTTP = _build_session()
TE_BASE = "https://api.tradingeconomics.com/calendar"
REQUEST_TIMEOUT = (5, 10)   # (connect, read)

# ─────────────────────────────────────────────────────────────
# Util
# ─────────────────────────────────────────────────────────────
def _sg_now() -> datetime:
    return datetime.now(ASIA_SG)

def _to_sg(dt_utc_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(dt_utc_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(dt_utc_str, "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=utc)
    return dt.astimezone(ASIA_SG)

def _ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT or not text:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={
                "chat_id": TG_CHAT,
                "text": text[:3500],
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=(3, 10),
        )
    except Exception as e:
        log.info("telegram send skipped: %s", e)

# ─────────────────────────────────────────────────────────────
# Fetch (에러 억제)
#   - d1/d2는 날짜(YYYY-MM-DD)만 사용
#   - 5xx/429 → 조용히 [] 반환 (로그 INFO 한 줄)
# ─────────────────────────────────────────────────────────────
def fetch_day(d1: datetime, d2: datetime) -> List[Dict[str, Any]]:
    params = {
        "c": TE_AUTH,
        "format": "json",
        "country": ",".join(COUNTRIES),
        "importance": ",".join(IMPORTANCE),
        "d1": _ymd(d1),
        "d2": _ymd(d2),
    }
    try:
        # 인스턴스 동시 호출 완화용 지터
        time.sleep(random.uniform(0, 0.6))
        r = HTTP.get(TE_BASE, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code in (429, 500, 502, 503, 504):
            log.info("econ-cal skip: HTTP %s", r.status_code)
            return []
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.info("econ-cal transient error ignored: %s", e)
        return []

def fetch_window_sg(start_sg: datetime, end_sg: datetime) -> List[Dict[str, Any]]:
    """SGT 윈도우 범위를 day API로 가져와 로컬 필터."""
    raw = fetch_day(start_sg, end_sg)
    out = []
    for e in raw:
        try:
            t = _to_sg(e.get("Date") or e.get("DateTime"))
        except Exception:
            continue
        if start_sg <= t < end_sg and (e.get("Country") in COUNTRIES):
            out.append(e | {"_sg_time": t})
    out.sort(key=lambda x: x.get("_sg_time"))
    return out

# ─────────────────────────────────────────────────────────────
# Message builders
# ─────────────────────────────────────────────────────────────
def build_preview(events: List[Dict[str, Any]]) -> str:
    lines = ["<b>🇺🇸 24h 경제 이벤트 (사전)</b>\n"]
    count = 0
    for e in events:
        title = (e.get("Event") or e.get("Category") or "").strip() or "Unknown"
        tt = e.get("_sg_time") or _to_sg(e.get("Date") or e.get("DateTime"))
        info = []
        if e.get("Forecast") not in (None, ""): info.append(f"예상 {e['Forecast']}")
        if e.get("Previous") not in (None, ""): info.append(f"이전 {e['Previous']}")
        core = (" — " + ", ".join(info)) if info else ""
        lines.append(f"🕒 {tt.strftime('%m/%d %H:%M')} — {title}{core}")
        count += 1
        if count >= 12:
            break
    if count == 0:
        lines.append("(24시간 내 고중요 이벤트 없음)")
    return "\n".join(lines)

def build_release_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    tt = e.get("_sg_time") or _to_sg(e.get("Date") or e.get("DateTime"))
    actual, forecast, previous = e.get("Actual"), e.get("Forecast"), e.get("Previous")
    info = []
    if actual not in (None, ""):   info.append(f"실제 {actual}")
    if forecast not in (None, ""): info.append(f"예상 {forecast}")
    if previous not in (None, ""): info.append(f"이전 {previous}")
    core = ("📊 " + ", ".join(info)) if info else "발표 확인"
    return "\n".join([
        f"<b>📢 {title}</b>",
        f"⏱ {tt.strftime('%m/%d %H:%M')} SGT",
        core
    ])

def build_speech_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    tt = e.get("_sg_time") or _to_sg(e.get("Date") or e.get("DateTime"))
    return "\n".join([
        f"<b>🎤 연설/발언</b>",
        f"{title}",
        f"⏱ {tt.strftime('%m/%d %H:%M')} SGT",
        "• 매파 톤 → 달러/수익률 ↑ → 위험자산 압박",
        "• 비둘기 톤 → 달러/수익률 ↓ → 위험자산 우호"
    ])

def _is_speech(e: Dict[str, Any]) -> bool:
    name = (e.get("Event") or e.get("Category") or "").lower()
    return any(k in name for k in ("speech","speaks","remarks","press","testifies","testimony","hearing"))

# ─────────────────────────────────────────────────────────────
# Jobs & state
# ─────────────────────────────────────────────────────────────
_sent_keys: Dict[str, float] = {}

def send_preview_job():
    now = _sg_now()
    evts = fetch_window_sg(now, now + timedelta(hours=24))
    if evts:
        tg_send(build_preview(evts))

def poll_releases_job():
    now = _sg_now()
    # 발표 직전~직후 5분 윈도우 감시
    evts = fetch_window_sg(now - timedelta(minutes=1), now + timedelta(minutes=LOOKAHEAD_MIN))
    for e in evts:
        tt = e.get("_sg_time") or now
        key = f"{e.get('Event')}|{e.get('Date')}|{e.get('Actual')}"
        # ① 실제치가 있으면 '발표'로 간주
        if e.get("Actual") not in (None, ""):
            if key not in _sent_keys:
                _sent_keys[key] = time.time()
                tg_send(build_release_note(e))
            continue
        # ② 연설 시작 안내
        if _is_speech(e) and (tt <= now + timedelta(seconds=5)):
            k2 = f"SPEECH|{e.get('Event')}|{e.get('Date')}"
            if k2 not in _sent_keys:
                _sent_keys[k2] = time.time()
                tg_send(build_speech_note(e))

def clean_cache_job():
    now = time.time()
    for k in list(_sent_keys.keys()):
        if now - _sent_keys[k] > 86400:
            _sent_keys.pop(k, None)

# ─────────────────────────────────────────────────────────────
# Init entry
# ─────────────────────────────────────────────────────────────
_scheduler: BackgroundScheduler | None = None
_bp = None

def init_econ_calendar(app=None):
    """app.py 에서 조건부로 호출됨. ENABLED=0이면 아무 것도 안함."""
    global _scheduler, _bp
    if not ENABLED:
        log.info("econ calendar disabled by ENV (ECON_CAL_ENABLED=0)")
        return None
    if _scheduler:
        return _scheduler

    # APScheduler
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 미리보기: 지정 시각들
    for t in PREVIEW_TIMES:
        try:
            hh, mm = [int(x) for x in t.split(":")]
            _scheduler.add_job(send_preview_job, CronTrigger(hour=hh, minute=mm))
        except Exception:
            log.warning("invalid ECON_PREVIEW_TIMES entry ignored: %s", t)

    # 실시간 폴링: 지터 부여
    _scheduler.add_job(poll_releases_job, "interval", seconds=POLL_SEC + random.randint(0, 5))
    _scheduler.add_job(clean_cache_job, "interval", minutes=30)
    _scheduler.start()

    # 수동 트리거 엔드포인트(선택)
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
