# econ_calendar_tele_bot.py
# -*- coding: utf-8 -*-
"""
TE 경제 캘린더 알림 (안정화·스케줄러 유지판, fail-safe)
"""

import os, time, logging, random, requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
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

# ── 기본 로거 ─────────────────────────────────────────────
log = logging.getLogger("econ-calendar")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# ── ENV ───────────────────────────────────────────────────
ENABLED = os.getenv("ECON_CAL_ENABLED", "0").lower() not in ("0", "false", "", "no", "off")
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

# ── HTTP 세션 (fail-safe retry) ─────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(total=1, connect=1, read=1, backoff_factor=1.0,
              status_forcelist=(429, 500, 502, 503, 504),
              allowed_methods=("GET",))
    ad = HTTPAdapter(max_retries=r)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s

HTTP = _build_session()

# ── UTIL ───────────────────────────────────────────────────
def _sg_now() -> datetime:
    return datetime.now(ASIA_SG)

def _to_sg(utc_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=utc)
    return dt.astimezone(ASIA_SG)

def tg_send(text: str):
    if not TG_TOKEN or not TG_CHAT or not text:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"},
            timeout=(3, 10)
        )
    except Exception as e:
        log.warning(f"telegram send fail: {e}")

# ── FETCH (에러 억제형) ─────────────────────────────────────
def _fetch_calendar(d1: datetime, d2: datetime) -> List[Dict[str, Any]]:
    base = "https://api.tradingeconomics.com/calendar"
    params = {
        "c": TE_AUTH,
        "format": "json",
        "country": ",".join(COUNTRIES),
        "importance": ",".join(IMPORTANCE),
        "d1": d1.strftime("%Y-%m-%d"),
        "d2": d2.strftime("%Y-%m-%d")
    }
    try:
        time.sleep(random.uniform(0, 0.6))
        r = HTTP.get(base, params=params, timeout=(5, 10))
        if r.status_code >= 500 or r.status_code == 429:
            log.info(f"econ-cal skip ({r.status_code})")
            return []
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.info(f"econ-cal transient error ignored ({e})")
        return []

# ── PREVIEW 메시지 ─────────────────────────────────────────
def build_preview(events: List[Dict[str, Any]]) -> str:
    lines = ["<b>🇺🇸 24h 주요 이벤트</b>\n"]
    for e in events[:10]:
        name = e.get("Event") or e.get("Category") or "Unknown"
        t = _to_sg(e.get("Date") or e.get("DateTime"))
        lines.append(f"🕒 {t.strftime('%m/%d %H:%M')} — {name}")
    if len(lines) == 1:
        lines.append("(이벤트 없음)")
    return "\n".join(lines)

# ── JOBS ───────────────────────────────────────────────────
_sent: Dict[str, float] = {}

def send_preview_job():
    now_sg = _sg_now()
    evts = _fetch_calendar(now_sg, now_sg + timedelta(days=1))
    if evts:
        tg_send(build_preview(evts))

def poll_releases_job():
    now_sg = _sg_now()
    evts = _fetch_calendar(now_sg - timedelta(hours=1), now_sg + timedelta(hours=1))
    for e in evts:
        key = f"{e.get('Event')}|{e.get('Date')}"
        if key in _sent:
            continue
        _sent[key] = time.time()
        tg_send(f"📢 발표 예정: {e.get('Event')} ({e.get('Country')})")

def clean_sent_job():
    now = time.time()
    for k, t in list(_sent.items()):
        if now - t > 86400:
            _sent.pop(k, None)

# ── MAIN INIT ──────────────────────────────────────────────
_scheduler = None

def init_econ_calendar(app=None):
    global _scheduler
    if not ENABLED:
        log.info("econ calendar disabled by ENV (ECON_CAL_ENABLED=0)")
        return
    if _scheduler:
        return _scheduler

    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))
    for t in PREVIEW_TIMES:
        hh, mm = t.split(":")
        _scheduler.add_job(send_preview_job, CronTrigger(hour=int(hh), minute=int(mm)))
    _scheduler.add_job(poll_releases_job, "interval", seconds=POLL_SEC + random.randint(0, 5))
    _scheduler.add_job(clean_sent_job, "interval", minutes=30)
    _scheduler.start()

    if app and Blueprint:
        bp = Blueprint("econ", __name__)
        @bp.get("/econ/preview_now")
        def _preview_now():
            if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
                return "forbidden", 403
            send_preview_job()
            return "ok", 200
        app.register_blueprint(bp)

    log.info(f"econ calendar started: preview={PREVIEW_TIMES}, poll={POLL_SEC}s, TE_AUTH={'custom' if TE_AUTH!='guest:guest' else 'guest'}")
    return _scheduler
