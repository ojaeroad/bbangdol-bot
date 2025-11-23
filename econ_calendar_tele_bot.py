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
  TE_AUTH                     : "email:apikey" (우선, 유료 계정에서만 사용)
  ECON_API_KEY                : TE_AUTH 대신 쓸 수 있는 별칭
                                둘 다 비어 있으면 public endpoint(무인증) 사용

  # Telegram
  ECON_TG_TOKEN               : 텔레그램 봇 토큰 (없으면 TELEGRAM_BOT_TOKEN 사용)
  ECON_CHAT_ID                : 텔레그램 chat_id (없으면 TELEGRAM_CHAT_ID 사용)

  # 필터
  ECON_COUNTRIES              : "United States,Japan" 처럼 쉼표 구분 국가 목록
  ECON_IMPORTANCE             : "2,3" (기본) — 중요도 필터
  ECON_PREVIEW_TIMES          : "07:00,13:00,19:00" 처럼 로컬(Asia/Seoul or SG) 시각들
  ECON_POLL_SEC               : 실시간 폴링 주기(초) 기본 60
  ECON_RELEASE_LOOKAHEAD_MIN  : 앞으로 몇 분 안의 이벤트를 "곧 발표"로 볼지 (기본 5분)
  ECON_RAW_TTL_SEC            : 원시 응답 캐시 TTL (기본 45초)

주의
  - 무료 API Free 계정 기준으로, 인증 없이 public calendar endpoint 사용
  - 유료 계정에서 email:apikey 를 넣으면 자동으로 인증 파라미터 추가
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
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

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 설정/환경변수
# ─────────────────────────────────────────────────────────────

ASIA_SG = timezone("Asia/Singapore")  # 인도에서 쓰기 편하게 SG 기준
ENABLED = os.getenv("ECON_CAL_ENABLED", "0").strip().lower() not in (
    "0", "false", "", "no", "off"
)

# TradingEconomics 인증(선택): 유료 계정에서만 사용.
# 무료(API Free) 계정은 TE_AUTH/ECON_API_KEY 를 비워 두고,
# 비인증 public endpoint 를 사용한다.
_te_auth_env = (os.getenv("TE_AUTH") or os.getenv("ECON_API_KEY") or "").strip()
TE_AUTH = _te_auth_env  # 빈 문자열이면 인증 파라미터를 붙이지 않는다.

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
    s.strip()
    for s in os.getenv("ECON_IMPORTANCE", "2,3").split(",")
    if s.strip()
]
PREVIEW_TIMES = [
    s.strip()
    for s in os.getenv("ECON_PREVIEW_TIMES", "07:00,13:00,19:00").split(",")
    if s.strip()
]

POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))
LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))
RAW_TTL_SEC = int(os.getenv("ECON_RAW_TTL_SEC", "45"))

# ─────────────────────────────────────────────────────────────
# HTTP 세션 (재시도 포함)
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


def _safe_float(v: Any) -> float | None:
    if not _is_number_like(v):
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# 간단 캐시 (메모리)
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
sent_cache = TTLCache(60 * 60 * 24)  # 24h 중복 방지용

# ─────────────────────────────────────────────────────────────
# TradingEconomics fetch
# ─────────────────────────────────────────────────────────────
# fetch_day / fetch_window_sg
#   - 무료 계정: 인증 파라미터 없이 f=json 사용
#   - 유료 계정: TE_AUTH 있으면 c=TE_AUTH 붙여서 사용
#   - 날짜는 UTC 기준 Date 필드를 쓰되, 쿼리에는 날짜(YYYY-MM-DD)만 사용
#   - 5xx/429 → 조용히 [] 반환 (로그 INFO 한 줄)
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
    """SG 기준 start~end 사이의 이벤트를 모두 가져오기 (raw + filter)."""
    # d1/d2는 UTC 날짜 기준으로 조금 넉넉하게 잡는다.
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
            # 국가/중요도 필터는 fetch_day에서 이미 걸었지만, 혹시 모르니 한 번 더
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

def _tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any] | None:
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


def send_text(msg: str, parse_mode: str | None = None):
    payload: Dict[str, Any] = {"chat_id": TG_CHAT, "text": msg}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    return _tg_api("sendMessage", payload)


# ─────────────────────────────────────────────────────────────
# 메시지 빌더
# ─────────────────────────────────────────────────────────────

def _crypto_generic_hint() -> str:
    return (
        "\n\n💡 *암호화폐 영향 일반 가이드*\n"
        "- 예상보다 *강한 지표* (실제치 > 예상치): 위험자산(비트코인 등)에 단기 하락 압력 가능\n"
        "- 예상보다 *약한 지표* (실제치 < 예상치): 완화 기대 → 위험자산에 우호적일 수 있음\n"
        "- 결과가 *예상과 비슷*하면, 이미 시장에 반영돼 변동성이 제한될 수 있음"
    )


def build_preview(events: List[Dict[str, Any]]) -> str:
    if not events:
        return "📆 향후 24시간 내 고중요 경제지표/이벤트 없음"

    # 시간순 정렬
    events = sorted(events, key=lambda e: e["_sg_time"])
    lines = ["📆 *향후 24시간 경제 캘린더(중요 이벤트)*\n"]

    count = 0
    for e in events:
        country = _strip(e.get("Country"))
        title = _strip(e.get("Event") or e.get("Category"))
        imp = str(e.get("Importance") or "")
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

    info_line = ", ".join(info) if info else "값 정보 없음"

    # 숫자이면 방향성 코멘트 생성
    hint = ""
    a = _safe_float(actual)
    f = _safe_float(forecast)
    p = _safe_float(previous)

    # 간단 로직: 물가/고용처럼 "강한 지표 = 긴축/달러강세 → BTC 하락 압력" 가정
    if a is not None and f is not None:
        if a > f * 1.01:
            hint = (
                "📉 실제치가 *예상보다 강하게* 나왔습니다.\n"
                "   → 위험자산(비트코인 등)에 단기 하락 압력 가능성을 염두에 두세요."
            )
        elif a < f * 0.99:
            hint = (
                "📈 실제치가 *예상보다 약하게* 나왔습니다.\n"
                "   → 긴축 완화 기대가 커질 수 있어, 위험자산(비트코인 등)에 우호적일 수 있습니다."
            )
        else:
            hint = (
                "⚖️ 실제치가 *예상과 거의 비슷*합니다.\n"
                "   → 이미 시장에 상당 부분 반영되었을 수 있으며, 변동성은 제한될 수 있습니다."
            )
    elif a is not None and p is not None:
        if a > p * 1.01:
            hint = (
                "📉 실제치가 *이전 값보다 강하게* 나왔습니다.\n"
                "   → 전반적으로 긴축/달러강세 쪽 신호로 해석될 수 있어, 비트코인에는 부담일 수 있습니다."
            )
        elif a < p * 0.99:
            hint = (
                "📈 실제치가 *이전 값보다 약하게* 나왔습니다.\n"
                "   → 완화적 신호로 받아들여질 수 있어, 비트코인에는 우호적일 수 있습니다."
            )

    lines = [
        f"📊 *{title}* 발표 결과",
        f"🕒 {_to_sg(str(tt)).strftime('%m/%d %H:%M')} (Asia/Singapore 기준)",
        f"ℹ️ {info_line}",
    ]
    if hint:
        lines.append("\n" + hint)
    else:
        lines.append(_crypto_generic_hint())
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# 스케줄러 잡
# ─────────────────────────────────────────────────────────────

def send_preview_job():
    now = _sg_now()
    end = now + timedelta(hours=24)
    events = fetch_window_sg(now, end)
    msg = build_preview(events)
    send_text(msg, parse_mode="Markdown")


def poll_releases_job():
    now = _sg_now()
    window_end = now + timedelta(minutes=LOOKAHEAD_MIN)
    events = fetch_window_sg(now - timedelta(minutes=5), window_end)

    for e in events:
        # 이벤트 고유 ID 비슷하게 구성
        key_parts = [
            _strip(e.get("Country")),
            _strip(e.get("Event") or e.get("Category")),
            _strip(e.get("ReferenceDate") or e.get("Reference")),
        ]
        ev_id = "::".join(key_parts)
        if not ev_id:
            continue

        # 이미 보낸 이벤트는 스킵
        if sent_cache.get(ev_id):
            continue

        actual = e.get("Actual")
        is_speech = str(e.get("Category") or "").lower().find("speech") >= 0

        # 연설(speech)은 시작 직전에 한 번 안내
        if is_speech and not actual:
            # 발표 시점 5분 전 안에 들어온 것만 공지
            tt = e.get("_sg_time")
            if tt and now <= tt <= window_end:
                title = _strip(e.get("Event") or e.get("Category"))
                country = _strip(e.get("Country"))
                msg = (
                    f"🗣 *주요 연설 예정 안내*\n"
                    f"🕒 {tt.strftime('%m/%d %H:%M')} (Asia/Singapore)\n"
                    f"국가: {country}\n"
                    f"제목: {title}\n\n"
                    "연설 내용에 따라 달러/금리 기대가 바뀌면 비트코인에도 영향을 줄 수 있습니다."
                )
                send_text(msg, parse_mode="Markdown")
                sent_cache.set(ev_id, True)
            continue

        # 일반 지표는 Actual 나왔을 때만 알림
        if actual in (None, ""):
            continue

        msg = build_release_note(e)
        send_text(msg, parse_mode="Markdown")
        sent_cache.set(ev_id, True)


# ─────────────────────────────────────────────────────────────
# Flask Blueprint 통합
# ─────────────────────────────────────────────────────────────

econ_bp = Blueprint("econ_calendar", __name__) if Blueprint else None
_scheduler: BackgroundScheduler | None = None


@econ_bp.route("/econ/health", methods=["GET"]) if econ_bp else lambda *a, **k: None
def econ_health():
    """상태 확인용 엔드포인트 /econ/health"""
    now = _sg_now()
    return json.dumps(
        {
            "enabled": ENABLED,
            "ok": bool(ENABLED and TG_TOKEN and TG_CHAT),
            "countries": COUNTRIES,
            "importance": IMPORTANCE,
            "preview_times": PREVIEW_TIMES,
            "poll_sec": POLL_SEC,
            "raw_ttl_sec": RAW_TTL_SEC,
            "now": now.isoformat(),
            "tz": "Asia/Singapore",
            "te_auth_mode": "custom" if TE_AUTH else "guest",
        },
        ensure_ascii=False,
        indent=2,
    )


@econ_bp.route("/econ/preview_now", methods=["GET"]) if econ_bp else lambda *a, **k: None
def preview_now():
    """강제 프리뷰 테스트용 엔드포인트 /econ/preview_now?key=..."""
    # 간단 보호용 key
    key = request.args.get("key") if request else None
    env_key = os.getenv("ECON_PREVIEW_KEY", "")
    if env_key and key != env_key:
        return "forbidden", 403
    send_preview_job()
    return "ok"


def init_econ_calendar(app) -> BackgroundScheduler | None:
    global _scheduler
    if not ENABLED:
        log.info("econ_calendar disabled (ECON_CAL_ENABLED=0)")
        return None
    if _scheduler:
        return _scheduler

    if not TG_TOKEN or not TG_CHAT:
        log.warning("econ_calendar enabled, but TG_TOKEN / TG_CHAT missing")
    else:
        log.info("econ_calendar Telegram: chat=%s", TG_CHAT)

    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 미리보기: 지정 시각들
    for t in PREVIEW_TIMES:
        try:
            hh, mm = [int(x) for x in t.split(":")]
            _scheduler.add_job(send_preview_job, CronTrigger(hour=hh, minute=mm))
        except Exception:
            log.warning("invalid ECON_PREVIEW_TIMES entry ignored: %s", t)

    # 실시간 폴링: 지터 부여
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


# 이 파일을 단독으로 실행했을 때도 동작하게 옵션 제공 (디버그용)
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
