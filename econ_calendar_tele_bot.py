# econ_calendar.py
# -*- coding: utf-8 -*-
"""
TradingEconomics 경제 캘린더 알림 (프리뷰 + 발표 후 요약)

기능
  1) 매일 지정된 시각(복수 가능)에 24시간 프리뷰 전송
  2) 상시 폴링으로 중요 지표 / 연설 발표 시점 탐지
     - 실제치(Actual) 나오면 요약 알림
     - 연설(speech) 이벤트는 시작 직전에 안내
  3) 같은 이벤트를 여러 번 보내지 않도록 24h 캐시

ENV
  ECON_CAL_ENABLED            : "1"이면 활성(기본 0=비활성)

  # TradingEconomics 인증
  TE_AUTH                     : "email:apikey" (우선)
  ECON_API_KEY                : 없을 때 대체로 사용
                                둘 다 없으면 guest:guest

  # Telegram
  ECON_TG_TOKEN               : 우선 사용
  TELEGRAM_BOT_TOKEN          : 위가 없을 때 fallback
  ECON_CHAT_ID                : 우선 사용
  TELEGRAM_CHAT_ID            : 위가 없을 때 fallback

  # 필터 (로컬 필터용)
  ECON_COUNTRIES              : 기본 "United States,Japan"
  ECON_IMPORTANCE             : 기본 "2,3"
  ECON_PREVIEW_TIMES          : 기본 "08:55,20:55" (Asia/Singapore 기준)

  # 기타 동작 옵션
  ECON_POLL_SEC               : 기본 60  (poll 주기, 초)
  ECON_RELEASE_LOOKAHEAD_MIN  : 기본 5   (발표 직후 몇 분까지 감시할지)
  ECON_ADMIN_KEY              : /econ/preview_now?key=... 보호용 키 (선택)
  ECON_RAW_TTL_SEC            : TE 원본 응답 캐시 TTL (기본 45초)
"""

import os
import time
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

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
ENABLED = os.getenv("ECON_CAL_ENABLED", "0").strip().lower() not in (
    "0", "false", "", "no", "off"
)

# TradingEconomics 인증: TE_AUTH > ECON_API_KEY > guest:guest
_te_auth_env = os.getenv("TE_AUTH") or os.getenv("ECON_API_KEY") or "guest:guest"
TE_AUTH = _te_auth_env.strip() or "guest:guest"

# Telegram
TG_TOKEN = os.getenv("ECON_TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("ECON_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")

# 필터 (로컬 필터용)
COUNTRIES = [
    s.strip()
    for s in os.getenv("ECON_COUNTRIES", "United States,Japan").split(",")
    if s.strip()
]
IMPORTANCE = [
    s.strip()
    for s in os.getenv("ECON_IMPORTANCE", "2,3").split(",")
    if s.strip()
]
PREVIEW_TIMES = [
    s.strip()
    for s in os.getenv("ECON_PREVIEW_TIMES", "08:55,20:55").split(",")
    if s.strip()
]

POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))
LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))
ADMIN_KEY = os.getenv("ECON_ADMIN_KEY", "")
RAW_TTL_SEC = int(os.getenv("ECON_RAW_TTL_SEC", "45"))

ASIA_SG = timezone("Asia/Singapore")

# ─────────────────────────────────────────────────────────────
# HTTP Session
# ─────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=1,
        connect=1,
        read=1,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=r, pool_connections=8, pool_maxsize=8)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s


HTTP = _build_session()
TE_BASE = "https://api.tradingeconomics.com/calendar"
REQUEST_TIMEOUT = (5, 10)  # (connect, read)

# ─────────────────────────────────────────────────────────────
# Util
# ─────────────────────────────────────────────────────────────
def _sg_now() -> datetime:
    return datetime.now(ASIA_SG)


def _to_sg(dt_utc_str: str) -> datetime:
    """TradingEconomics ISO 문자열을 Asia/Singapore 로 변환."""
    try:
        dt = datetime.fromisoformat(dt_utc_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(dt_utc_str, "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=utc)
    return dt.astimezone(ASIA_SG)


def _ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def tg_send(text: str) -> None:
    """텔레그램 전송 — 오류는 조용히 로그만 남기고 무시."""
    if not TG_TOKEN or not TG_CHAT or not text:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={
                "chat_id": TG_CHAT,
                "text": text[:3500],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=(3, 10),
        )
    except Exception as e:
        log.info("telegram send skipped: %s", e)

# ─────────────────────────────────────────────────────────────
# Fetch (무료 계정 호환)
#   - TE API에는 날짜/국가/중요도 파라미터를 넣지 않음
#   - 전체 캘린더를 받은 뒤 로컬에서 필터링
# ─────────────────────────────────────────────────────────────
_last_raw_events: List[Dict[str, Any]] = []
_last_raw_ts: float = 0.0


def fetch_day(d1: datetime, d2: datetime) -> List[Dict[str, Any]]:
    """무료 계정 호환용: TE API에는 날짜/국가/중요도 파라미터를 넣지 않고
    전체 캘린더를 받아온 뒤, 이후 단계에서 로컬 필터링만 수행한다."""
    global _last_raw_events, _last_raw_ts
    now_ts = time.time()
    if _last_raw_events and (now_ts - _last_raw_ts) < RAW_TTL_SEC:
        return _last_raw_events

    params = {
        "c": TE_AUTH,
        "format": "json",
    }
    try:
        time.sleep(random.uniform(0, 0.6))
        r = HTTP.get(TE_BASE, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code in (429, 500, 502, 503, 504):
            log.info("econ-cal skip: HTTP %s", r.status_code)
            return _last_raw_events
        data = r.json()
        if isinstance(data, list):
            _last_raw_events = data
            _last_raw_ts = now_ts
            return _last_raw_events
        log.info("econ-cal unexpected payload type: %s", type(data))
        return _last_raw_events
    except Exception as e:
        log.info("econ-cal transient error ignored: %s", e)
        return _last_raw_events


def fetch_window_sg(start_sg: datetime, end_sg: datetime) -> List[Dict[str, Any]]:
    """SGT 윈도우 범위를 전체 캘린더에서 로컬 필터."""
    raw = fetch_day(start_sg, end_sg)
    out: List[Dict[str, Any]] = []
    for e in raw:
        try:
            t = _to_sg(e.get("Date") or e.get("DateTime"))
        except Exception:
            continue

        if not (start_sg <= t < end_sg):
            continue

        country = (e.get("Country") or "").strip()
        if COUNTRIES and country and (country not in COUNTRIES):
            continue

        imp_val = str(e.get("Importance", "")).strip()
        if IMPORTANCE and imp_val and (imp_val not in IMPORTANCE):
            continue

        e["_sg_time"] = t
        out.append(e)

    out.sort(key=lambda x: x.get("_sg_time"))
    return out

# ─────────────────────────────────────────────────────────────
# Message builders
# ─────────────────────────────────────────────────────────────
def _crypto_generic_hint() -> str:
    """
    암호화폐 영향에 대한 아주 일반적인 힌트 텍스트.
    (지표 개별 해석까지는 하지 않고, 방향성만 간단히 안내)
    """
    return (
        "\n\n"
        "📌 <b>코인 시장 참고</b>\n"
        "• 물가·고용 등 지표가 예상보다 <b>강하게</b> 나오면 → 달러·금리 ↑ → 위험자산(주식·코인)에는 단기적으로 부담.\n"
        "• 지표가 예상보다 <b>약하게</b> 나오면 → 달러·금리 ↓ → 위험자산에는 단기적으로 우호적인 편."
    )


def build_preview(events: List[Dict[str, Any]]) -> str:
    lines = ["<b>📅 24시간 경제 이벤트 (사전)</b>\n"]
    count = 0
    for e in events:
        title = (e.get("Event") or e.get("Category") or "").strip() or "Unknown"
        tt = e.get("_sg_time") or _to_sg(e.get("Date") or e.get("DateTime"))
        info = []
        if e.get("Forecast") not in (None, ""):
            info.append(f"예상 {e['Forecast']}")
        if e.get("Previous") not in (None, ""):
            info.append(f"이전 {e['Previous']}")
        core = " — " + ", ".join(info) if info else ""
        country = e.get("Country") or ""
        imp = e.get("Importance", "")
        imp_txt = f"[{country} / 중요도 {imp}]" if country or imp else ""
        lines.append(
            f"🕒 {tt.strftime('%m/%d %H:%M')} {imp_txt}\n"
            f"   {title}{core}"
        )
        count += 1
        if count >= 12:
            break
    if count == 0:
        lines.append("(24시간 내 고중요 이벤트 없음)")
    lines.append(_crypto_generic_hint())
    return "\n".join(lines)


def build_release_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    tt = e.get("_sg_time") or _to_sg(e.get("Date") or e.get("DateTime"))
    actual, forecast, previous = (
        e.get("Actual"),
        e.get("Forecast"),
        e.get("Previous"),
    )

    info = []
    if actual not in (None, ""):
        info.append(f"실제 {actual}")
    if forecast not in (None, ""):
        info.append(f"예상 {forecast}")
    if previous not in (None, ""):
        info.append(f"이전 {previous}")
    core = "📊 " + ", ".join(info) if info else "발표 확인"

    base = "\n".join(
        [
            f"<b>📢 {title}</b>",
            f"⏱ {tt.strftime('%m/%d %H:%M')} SGT",
            core,
        ]
    )
    return base + _crypto_generic_hint()


def build_speech_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    tt = e.get("_sg_time") or _to_sg(e.get("Date") or e.get("DateTime"))
    return "\n".join(
        [
            "<b>🎤 연설/발언</b>",
            title,
            f"⏱ {tt.strftime('%m/%d %H:%M')} SGT",
            "• 매파 톤 → 달러/수익률 ↑ → 위험자산(주식·코인) 압박",
            "• 비둘기 톤 → 달러/수익률 ↓ → 위험자산(주식·코인) 우호",
        ]
    )


def _is_speech(e: Dict[str, Any]) -> bool:
    name = (e.get("Event") or e.get("Category") or "").lower()
    return any(
        k in name
        for k in (
            "speech",
            "speaks",
            "remarks",
            "press",
            "testifies",
            "testimony",
            "hearing",
        )
    )

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
    evts = fetch_window_sg(
        now - timedelta(minutes=1),
        now + timedelta(minutes=LOOKAHEAD_MIN),
    )
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
    now_ts = time.time()
    for k in list(_sent_keys.keys()):
        if now_ts - _sent_keys[k] > 86400:  # 24h
            _sent_keys.pop(k, None)

# ─────────────────────────────────────────────────────────────
# Init entry
# ─────────────────────────────────────────────────────────────
_scheduler: BackgroundScheduler | None = None
_bp = None


def init_econ_calendar(app=None):
    """
    app.py 에서 조건부로 호출되는 진입점.

    예)
      from econ_calendar_tele_bot import init_econ_calendar
      init_econ_calendar(app)
    """
    global _scheduler, _bp
    if not ENABLED:
        log.info("econ calendar disabled by ENV (ECON_CAL_ENABLED=0)")
        return None
    if _scheduler:
        return _scheduler

    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 프리뷰 스케줄
    for t in PREVIEW_TIMES:
        try:
            hh, mm = [int(x) for x in t.split(":")]
            _scheduler.add_job(send_preview_job, CronTrigger(hour=hh, minute=mm))
        except Exception:
            log.warning("invalid ECON_PREVIEW_TIMES entry ignored: %s", t)

    # 실시간 폴링
    _scheduler.add_job(
        poll_releases_job,
        "interval",
        seconds=POLL_SEC + random.randint(0, 5),
    )
    _scheduler.add_job(clean_cache_job, "interval", minutes=30)
    _scheduler.start()

    # 수동 트리거
    if app is not None and Blueprint is not None:
        _bp = Blueprint("econ", __name__)

        @_bp.get("/econ/preview_now")
        def _preview_now():
            if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
                return "forbidden", 403
            send_preview_job()
            return "ok", 200

        @_bp.get("/econ/health")
        def _health():
            return (
                {
                    "ok": True,
                    "enabled": True,
                    "countries": COUNTRIES,
                    "importance": IMPORTANCE,
                    "preview_times": PREVIEW_TIMES,
                    "poll_sec": POLL_SEC,
                    "raw_ttl_sec": RAW_TTL_SEC,
                },
                200,
            )

        app.register_blueprint(_bp)

    log.info(
        "econ calendar started: enabled=1, preview=%s, poll=%ss(+jitter), importance=%s, TE=%s",
        PREVIEW_TIMES,
        POLL_SEC,
        IMPORTANCE,
        "custom" if TE_AUTH != "guest:guest" else "guest",
    )
    return _scheduler


if __name__ == "__main__":
    if not ENABLED:
        print("ECON_CAL_ENABLED=0 이라서 동작하지 않습니다.")
    else:
        print("econ_calendar: 백그라운드 스케줄러 시작 (단독 모드)")
        init_econ_calendar(None)
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            print("stopped.")
