# econ_calendar_tele_bot.py
# -*- coding: utf-8 -*-
"""
TradingEconomics 경제 캘린더 알림 (프리뷰 + 20분 전 상세 설명 + 발표 후 요약)

기능
  1) 매일 지정된 시각(복수 가능)에 24시간 프리뷰 전송
     - 각 이벤트 라인 앞에 중요도 이모티콘(💎/⭐️/⚡️)
     - 각 이벤트 바로 아래에, 예상치 대비 실적치 3단계 시나리오(상회/부합/하회)가 줄마다 표시
  2) 각 이벤트 약 20분 전에 상세 설명 + 3단계 시나리오 전송
     - 메시지 맨 앞에 중요도 이모티콘 포함
  3) 실제 값(Actual)이 나오면 결과 요약 + 암호화폐 영향 코멘트 전송
  4) 같은 이벤트에 대해 20분 전 / 결과 요약은 각각 24h에 1회만 전송 (프리뷰는 매번 전송)

ENV
  ECON_CAL_ENABLED            : "1"이면 활성(기본 0=비활성)

  # TradingEconomics 인증
  TE_AUTH                     : "email:apikey" (유료 계정에서만 사용, 없으면 public endpoint)
  ECON_API_KEY                : TE_AUTH 대신 쓸 수 있는 별칭

  # Telegram
  ECON_TG_TOKEN               : 텔레그램 봇 토큰 (없으면 TELEGRAM_BOT_TOKEN 사용)
  ECON_CHAT_ID                : 텔레그램 chat_id (없으면 TELEGRAM_CHAT_ID 사용)

  # 필터
  ECON_COUNTRIES              : "United States,Japan" 처럼 쉼표 구분 국가 목록
  ECON_IMPORTANCE             : "2,3" (기본) — 중요도 필터
  ECON_PREVIEW_TIMES          : "07:00,13:00,19:00" 처럼 로컬(Asia/Singapore) 시각들
  ECON_POLL_SEC               : 실시간 폴링 주기(초) 기본 60
  ECON_RELEASE_LOOKAHEAD_MIN  : 결과 감지용 앞 시간(분) 기본 5
  ECON_RAW_TTL_SEC            : 원시 응답 캐시 TTL (기본 45초)
  ECON_PREVIEW_KEY            : /econ/preview_now 호출용 간단한 비밀키(?key=...)
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone, utc

try:
    # Flask는 app.py 쪽에서 이미 사용 중
    from flask import request
except Exception:  # Render 환경 등에서만
    request = None  # type: ignore

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 설정/환경변수
# ─────────────────────────────────────────────────────────────

ASIA_SG = timezone("Asia/Singapore")

ENABLED = os.getenv("ECON_CAL_ENABLED", "0").strip().lower() not in (
    "0",
    "false",
    "",
    "no",
    "off",
)

# TradingEconomics 인증(선택)
_te_auth_env = (os.getenv("TE_AUTH") or os.getenv("ECON_API_KEY") or "").strip()
TE_AUTH = _te_auth_env  # 비어 있으면 public endpoint 사용

# Telegram
TG_TOKEN = os.getenv("ECON_TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("ECON_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")

# 필터
COUNTRIES = [
    s.strip()
    for s in os.getenv("ECON_COUNTRIES", "United States,Japan").split(",")
    if s.strip()
]
IMPORTANCE = [
    s.strip() for s in os.getenv("ECON_IMPORTANCE", "2,3").split(",") if s.strip()
]
PREVIEW_TIMES = [
    s.strip()
    for s in os.getenv("ECON_PREVIEW_TIMES", "07:00,13:00,19:00").split(",")
    if s.strip()
]

POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))
LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))
RAW_TTL_SEC = int(os.getenv("ECON_RAW_TTL_SEC", "45"))

DETAIL_BEFORE_MIN = 20  # 이벤트 20분 전 상세 설명

# ─────────────────────────────────────────────────────────────
# HTTP 세션
# ─────────────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
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


def _strip(s: Any) -> str:
    return (str(s) if s is not None else "").strip()


def _is_number_like(v: Any) -> bool:
    if v is None:
        return False
    try:
        float(str(v).replace(",", ""))
        return True
    except Exception:
        return False


def _safe_float(v: Any) -> Optional[float]:
    if not _is_number_like(v):
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def importance_icon(importance: Any) -> str:
    """중요도에 따른 이모티콘 반환."""
    s = _strip(importance)
    if s == "3":
        return "💎"
    if s == "2":
        return "⭐️"
    if s == "1":
        return "⚡️"
    return "⚡️"


# ─────────────────────────────────────────────────────────────
# 간단 캐시
# ─────────────────────────────────────────────────────────────

class TTLCache:
    def __init__(self, ttl_sec: int):
        self.ttl = ttl_sec
        self.store: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str):
        now = time.time()
        v = self.store.get(key)
        if not v:
            return None
        ts, data = v
        if now - ts > self.ttl:
            self.store.pop(key, None)
            return None
        return data

    def set(self, key: str, value: Any):
        self.store[key] = (time.time(), value)


raw_cache = TTLCache(RAW_TTL_SEC)
# 이벤트별 중복 방지: 24시간 (프리뷰는 사용 안 함)
sent_cache = TTLCache(60 * 60 * 24)

# ─────────────────────────────────────────────────────────────
# TradingEconomics fetch
# ─────────────────────────────────────────────────────────────

def fetch_day(d1: datetime, d2: datetime) -> List[Dict[str, Any]]:
    # 무료(API Free) 기본: 인증 파라미터 없이 public endpoint 사용
    params = {
        "f": "json",
        "country": ",".join(COUNTRIES),
        "importance": ",".join(IMPORTANCE),
        "d1": _ymd(d1),
        "d2": _ymd(d2),
    }
    # 유료 계정에서 email:apikey 를 지정한 경우에만 인증 파라미터 추가
    if TE_AUTH:
        params["c"] = TE_AUTH
    try:
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
    """SG 기준 start~end 사이의 이벤트를 모두 가져오기."""
    d1 = (start_sg - timedelta(days=1)).astimezone(utc)
    d2 = (end_sg + timedelta(days=1)).astimezone(utc)

    cache_key = f"{_ymd(d1)}::{_ymd(d2)}"
    cached = raw_cache.get(cache_key)
    if cached is not None:
        raw = cached
    else:
        raw = fetch_day(d1, d2)
        raw_cache.set(cache_key, raw)

    events: List[Dict[str, Any]] = []
    for e in raw:
        try:
            dt = e.get("Date") or e.get("DateTime")
            if not dt:
                continue
            tt = _to_sg(dt)
            if not (start_sg <= tt <= end_sg):
                continue
            country = _strip(e.get("Country"))
            importance = str(e.get("Importance") or "")
            if COUNTRIES and country not in COUNTRIES:
                continue
            if IMPORTANCE and importance not in IMPORTANCE:
                continue
            e["_sg_time"] = tt
            events.append(e)
        except Exception:
            continue
    return events


# ─────────────────────────────────────────────────────────────
# 텔레그램 전송
# ─────────────────────────────────────────────────────────────

def _tg_api(method: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not TG_TOKEN or not TG_CHAT:
        return None
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    try:
        r = HTTP.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("telegram error: %s", e)
        return None


def send_text(msg: str, parse_mode: Optional[str] = None):
    payload: Dict[str, Any] = {"chat_id": TG_CHAT, "text": msg}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    return _tg_api("sendMessage", payload)


# ─────────────────────────────────────────────────────────────
# 메시지 빌더 (3단계 시나리오 포함)
# ─────────────────────────────────────────────────────────────

SCENARIO_BRIEF_MULTI = (
    "   • 상회 → 암호화폐에 긍정적, 단기 급등 가능\n"
    "   • 부합 → 암호화폐에 긍정적, 단기 상승 가능\n"
    "   • 하회 → 암호화폐에 부정적, 단기 급락 가능"
)


def scenario_detail_text(title: str, importance: Any) -> str:
    """이벤트 20분 전 상세 설명용 텍스트."""
    icon = importance_icon(importance)
    lines = [
        f"{icon} *{title}* 발표 20분 전 안내",
        "",
        "🔍 *왜 중요한가?*",
        "최근 시장에서 해당 지표는 금리 경로와 달러 강세/약세를 가르는 핵심 변수로 취급되며,",
        "결과에 따라 비트코인·알트코인 등 암호화폐의 단기 방향성이 크게 바뀔 수 있습니다.",
        "",
        "📌 *해석 가이드 (예상치 대비 실제치 기준)*",
        "• 상회(실제치 > 예상치)",
        "  → 암호화폐에 *긍정적*, 단기 급등 가능성이 커집니다.",
        "",
        "• 부합(실제치 ≈ 예상치)",
        "  → 암호화폐에 *긍정적*, 완만한 단기 상승 흐름을 기대할 수 있습니다.",
        "",
        "• 하회(실제치 < 예상치)",
        "  → 암호화폐에 *부정적*, 단기적으로 충격 하락이 나올 수 있습니다.",
        "",
        "※ 실제 시장 반응은 동시에 발표되는 다른 지표, 뉴스, 유동성 상황에 따라 달라질 수 있으니 ",
        "   과도한 레버리지는 피하는 것이 좋습니다.",
    ]
    return "\n".join(lines)


def _crypto_generic_hint() -> str:
    return (
        "\n\n💡 *참고*\n"
        "- 지표 결과는 다른 뉴스/자금 흐름과 함께 해석해야 하며,\n"
        "  위 내용은 방향성을 이해하기 위한 간단한 가이드일 뿐입니다."
    )


def build_preview(events: List[Dict[str, Any]]) -> str:
    if not events:
        return "📆 향후 24시간 내 고중요 경제지표/이벤트 없음"

    events = sorted(events, key=lambda e: e["_sg_time"])
    lines = ["📆 *향후 24시간 경제 캘린더(중요 이벤트)*\n"]

    count = 0
    for e in events:
        country = _strip(e.get("Country"))
        title = _strip(e.get("Event") or e.get("Category"))
        imp = str(e.get("Importance") or "")
        icon = importance_icon(imp)
        tt = e["_sg_time"]

        ref = _strip(e.get("Reference"))
        ref_dt = _strip(e.get("ReferenceDate"))
        core = ""
        if ref:
            core += f" ({ref}"
            if ref_dt:
                core += f", 기준일 {ref_dt}"
            core += ")"

        imp_txt = f"[{country} / 중요도 {imp}]" if country or imp else ""
        lines.append(
            f"{icon} {tt.strftime('%m/%d %H:%M')} {imp_txt}\n"
            f"   {title}{core}"
        )
        # 각 이벤트 바로 아래에 3단계 시나리오 여러 줄
        lines.append(SCENARIO_BRIEF_MULTI)
        count += 1
        if count >= 20:  # 너무 길어지지 않도록 안전 장치
            break

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

    info_line = ", ".join(info) if info else "값 정보 없음"

    # 숫자이면 3단계 해석
    hint = ""
    a = _safe_float(actual)
    f = _safe_float(forecast)
    p = _safe_float(previous)

    if a is not None and f is not None:
        if a > f * 1.01:
            hint = (
                "✅ *상회(실제치 > 예상치)*\n"
                "   → 암호화폐에 긍정적, 단기 급등 가능성이 있는 결과입니다."
            )
        elif a < f * 0.99:
            hint = (
                "⚠️ *하회(실제치 < 예상치)*\n"
                "   → 암호화폐에 부정적, 단기 충격 하락이 나올 수 있는 결과입니다."
            )
        else:
            hint = (
                "✅ *부합(실제치 ≈ 예상치)*\n"
                "   → 암호화폐에 긍정적, 점진적인 단기 상승 흐름을 기대할 수 있습니다."
            )
    elif a is not None and p is not None:
        if a > p * 1.01:
            hint = (
                "✅ *상회(실제치 > 이전치)*\n"
                "   → 암호화폐에 긍정적, 단기 급등 가능성이 있는 결과입니다."
            )
        elif a < p * 0.99:
            hint = (
                "⚠️ *하회(실제치 < 이전치)*\n"
                "   → 암호화폐에 부정적, 단기 충격 하락이 나올 수 있는 결과입니다."
            )
        else:
            hint = (
                "✅ *부합(실제치 ≈ 이전치)*\n"
                "   → 암호화폐에 긍정적, 완만한 상승 쪽으로 해석될 수 있습니다."
            )

    lines = [
        f"📊 *{title}* 발표 결과",
        f"🕒 {_to_sg(str(tt)).strftime('%m/%d %H:%M')} (Asia/Singapore 기준)",
        f"ℹ️ {info_line}",
    ]
    if hint:
        lines.append("\n" + hint)
    lines.append(_crypto_generic_hint())
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# 스케줄러 잡
# ─────────────────────────────────────────────────────────────

def _event_id(e: Dict[str, Any]) -> str:
    key_parts = [
        _strip(e.get("Country")),
        _strip(e.get("Event") or e.get("Category")),
        _strip(e.get("ReferenceDate") or e.get("Reference")),
    ]
    return "::".join(key_parts)


def send_preview_job():
    now = _sg_now()
    end = now + timedelta(hours=24)
    events = fetch_window_sg(now, end)
    msg = build_preview(events)
    send_text(msg, parse_mode="Markdown")


def poll_releases_job():
    now = _sg_now()
    # 넉넉하게 앞뒤로 잡아서 한 번에 처리
    window_start = now - timedelta(minutes=DETAIL_BEFORE_MIN + 5)
    window_end = now + timedelta(minutes=LOOKAHEAD_MIN)
    events = fetch_window_sg(window_start, window_end)

    for e in events:
        ev_id = _event_id(e)
        if not ev_id:
            continue
        tt: datetime = e.get("_sg_time") or _to_sg(
            e.get("Date") or e.get("DateTime")
        )
        delta_min = (tt - now).total_seconds() / 60.0
        actual = e.get("Actual")
        is_speech = str(e.get("Category") or "").lower().find("speech") >= 0

        # 1) 이벤트 20분 전 상세 설명 (아직 Actual 없음)
        if actual in (None, "") and 18 <= delta_min <= 22:
            pre_key = ev_id + "::pre20"
            if not sent_cache.get(pre_key):
                title = _strip(e.get("Event") or e.get("Category"))
                msg = scenario_detail_text(title, e.get("Importance"))
                send_text(msg, parse_mode="Markdown")
                sent_cache.set(pre_key, True)
            # 20분 전 설명은 보내고 나서도 결과 알림을 위해 계속 진행

        # 2) 연설(speech) 5분 전 안내
        if is_speech and actual in (None, "") and 0 <= delta_min <= LOOKAHEAD_MIN:
            speech_key = ev_id + "::speech"
            if not sent_cache.get(speech_key):
                title = _strip(e.get("Event") or e.get("Category"))
                country = _strip(e.get("Country"))
                icon = importance_icon(e.get("Importance"))
                msg = (
                    f"{icon} *주요 연설 예정 안내*\n"
                    f"🕒 {tt.strftime('%m/%d %H:%M')} (Asia/Singapore)\n"
                    f"국가: {country}\n"
                    f"제목: {title}\n\n"
                    "연설 내용에 따라 기대 인플레이션/금리 전망이 바뀌면 "
                    "비트코인 등 암호화폐 가격에도 영향을 줄 수 있습니다."
                )
                send_text(msg, parse_mode="Markdown")
                sent_cache.set(speech_key, True)
            continue  # 연설은 Actual 이 따로 안 나오는 경우가 많아서 여기까지만

        # 3) 일반 지표 결과 발표 직후 (Actual 존재)
        if actual not in (None, ""):
            res_key = ev_id + "::result"
            if sent_cache.get(res_key):
                continue
            # 실제 발표 시점 근처(조금 과거/미래 허용)
            if -10 <= delta_min <= LOOKAHEAD_MIN:
                msg = build_release_note(e)
                send_text(msg, parse_mode="Markdown")
                sent_cache.set(res_key, True)


# ─────────────────────────────────────────────────────────────
# Flask endpoint 함수
# ─────────────────────────────────────────────────────────────

def econ_health() -> str:
    """상태 확인용 엔드포인트 핸들러."""
    now = _sg_now()
    body = {
        "enabled": ENABLED,
        "ok": bool(ENABLED and TG_TOKEN and TG_CHAT),
        "countries": COUNTRIES,
        "importance": IMPORTANCE,
        "preview_times": PREVIEW_TIMES,
        "poll_sec": POLL_SEC,
        "raw_ttl_sec": RAW_TTL_SEC,
        "detail_before_min": DETAIL_BEFORE_MIN,
        "now": now.isoformat(),
        "tz": "Asia/Singapore",
        "te_auth_mode": "custom" if TE_AUTH else "guest",
    }
    return json.dumps(body, ensure_ascii=False, indent=2)


def econ_preview_now() -> str:
    """강제 프리뷰 테스트용 엔드포인트."""
    if request is None:
        return "request unavailable"
    key = request.args.get("key", "")
    env_key = os.getenv("ECON_PREVIEW_KEY", "")
    if env_key and key != env_key:
        return "forbidden", 403  # Flask가 튜플을 응답으로 처리
    send_preview_job()
    return "ok"


# ─────────────────────────────────────────────────────────────
# 초기화 (app.py 에서 호출)
# ─────────────────────────────────────────────────────────────

_scheduler: Optional[BackgroundScheduler] = None


def init_econ_calendar(app) -> Optional[BackgroundScheduler]:
    """app.py 에서 한 번만 호출.

    예)
        from econ_calendar_tele_bot import init_econ_calendar
        ...
        init_econ_calendar(app)
    """
    global _scheduler
    if not ENABLED:
        log.info("econ_calendar disabled (ECON_CAL_ENABLED=0)")
        return None
    if _scheduler:
        return _scheduler

    # Flask 라우트 등록
    try:
        if app is not None:
            # 중복 등록 방지
            vf = getattr(app, "view_functions", {})
            if "econ_health" not in vf:
                app.add_url_rule("/econ/health", "econ_health", econ_health, methods=["GET"])
            if "econ_preview_now" not in vf:
                app.add_url_rule("/econ/preview_now", "econ_preview_now", econ_preview_now, methods=["GET"])
            log.info("econ_calendar routes registered: /econ/health, /econ/preview_now")
    except Exception as e:
        log.warning("failed to register econ_calendar routes: %s", e)

    if not TG_TOKEN or not TG_CHAT:
        log.warning("econ_calendar enabled, but TG_TOKEN / TG_CHAT missing")
    else:
        log.info("econ_calendar Telegram: chat=%s", TG_CHAT)

    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 프리뷰: 지정 시각들 (매번 전송, 캐시 사용 안 함)
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
        seconds=POLL_SEC,
        jitter=10,
    )

    _scheduler.start()
    log.info(
        "econ_calendar started: poll=%ss, countries=%s, importance=%s, auth_mode=%s",
        POLL_SEC,
        COUNTRIES,
        IMPORTANCE,
        "custom" if TE_AUTH else "guest",
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
                time.sleep(5)
        except KeyboardInterrupt:
            print("종료합니다.")
