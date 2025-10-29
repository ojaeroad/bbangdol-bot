# econ_calendar_tele_bot.py
# -*- coding: utf-8 -*-
"""
미국 중심 고중요 이벤트(지표/연설/회의) 사전·실적 알림 + 수동 트리거 엔드포인트 제공.

필요 ENV:
  # 텔레그램
  ECON_TG_TOKEN | TELEGRAM_BOT_TOKEN : 텔레그램 봇 토큰
  ECON_CHAT_ID  | TELEGRAM_CHAT_ID   : 텔레그램 방 ID (예: -4904606442)

  # TradingEconomics (무료 키 발급 권장)
  TE_AUTH                    : "이메일:API키" (미설정시 guest:guest로 동작하나 FOMC/연설 일부 누락)
  ECON_COUNTRIES             : 기본 "United States"
  ECON_IMPORTANCE            : 기본 "2,3" (guest에서도 최대한 커버)
  ECON_PREVIEW_TIMES         : 기본 "08:55,20:55" (Asia/Singapore 기준)
  ECON_POLL_SEC              : 기본 60  (발표 감시 주기)
  ECON_RELEASE_LOOKAHEAD_MIN : 기본 5   (앞으로 N분 내 일정 감시)

  # (선택) 수동 트리거 보호키
  ECON_ADMIN_KEY            : /econ/preview_now 호출 시 ?key=
"""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from urllib.parse import urlencode, quote_plus

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone, utc

# 플라스크 블루프린트(선택: app 전달 시 수동 트리거 라우트 활성)
try:
    from flask import Blueprint, request
except Exception:
    Blueprint = None
    request = None

log = logging.getLogger("econ-calendar")

ASIA_SG = timezone("Asia/Singapore")
TE_BASE = "https://api.tradingeconomics.com/calendar"
TE_AUTH = os.getenv("TE_AUTH", "guest:guest")  # 무료 계정 발급 시: "email:apikey"

# token/chat id는 두 이름 중 하나만 있어도 동작 (호환)
TG_TOKEN = os.getenv("ECON_TG_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("ECON_CHAT_ID")  or os.getenv("TELEGRAM_CHAT_ID", "")

COUNTRIES     = [s.strip() for s in os.getenv("ECON_COUNTRIES", "United States").split(",") if s.strip()]
# 무료 guest 환경에서도 놓치지 않도록 기본을 "2,3"으로 상향
IMPORTANCE    = [s.strip() for s in os.getenv("ECON_IMPORTANCE", "2,3").split(",") if s.strip()]
PREVIEW_TIMES = [s.strip() for s in os.getenv("ECON_PREVIEW_TIMES", "08:55,20:55").split(",") if s.strip()]

POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))
RELEASE_LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))
ADMIN_KEY = os.getenv("ECON_ADMIN_KEY", "")

_sent_release_keys: Dict[str, float] = {}

IMPORTANT_KEYWORDS = [
    "CPI", "Core CPI", "PCE", "Core PCE", "FOMC", "GDP", "Non-Farm", "Unemployment", "Retail Sales",
    "Fed Interest Rate Decision", "Fed Press Conference", "Minutes"
]
SPEECH_KEYWORDS = ["speech", "speaks", "remarks", "press conference", "testifies", "testimony", "hearing"]
SPEECH_FIGURES  = ["Powell", "Federal Reserve", "Fed", "FOMC", "Yellen", "Lagarde", "ECB", "BOE", "BOJ", "SNB",
                   "Waller", "Williams", "Kashkari"]

CRYPTO_SCENARIOS = {
    "CPI": (
        "실제치가 예상보다 높음 → 인플레 재확인·완화 지연 → <b>단기 약세</b>",
        "예상 부합 → 변동성 제한적",
        "예상보다 낮음 → 완화 기대 상승 → <b>우호적</b>"
    ),
    "Core CPI": ("핵심 인플레 상회 → <b>약세</b>", "예상 부합 → 제한적", "하회 → <b>우호적</b>"),
    "PCE": ("상회 → 완화 지연 우려 → <b>약세</b>", "부합 → 제한적", "하회 → <b>우호적</b>"),
    "Core PCE": ("상회 → <b>약세</b>", "부합 → 제한적", "하회 → <b>우호적</b>"),
    "NFP": ("고용 상회 → 수익률↑ → <b>압박</b>", "부합 → 제한적", "하회 → 달러↓ → <b>우호적</b>"),
    "Unemployment Rate": ("하락 → 과열 신호 → <b>압박</b>", "부합 → 제한적", "상승 → 위험자산 <b>우호적</b>"),
    "Retail Sales": ("상회 → 수요 견조 → <b>압박</b>", "부합 → 제한적", "하회 → 둔화·완화 기대 → <b>우호적</b>"),
    "GDP": ("상회 → 긴축 장기화 우려 → <b>중립~약세</b>", "부합 → 제한적", "하회 → 둔화·완화 기대 → <b>우호적</b>"),
    "FOMC": ("매파(상회) → <b>약세</b>", "중립(부합) → 제한적", "비둘기(하회) → <b>우호적</b>")
}

# ── 유틸 ──────────────────────────────────────────────────────────────
def _sg_now() -> datetime:
    return datetime.now(ASIA_SG)

def _to_sg(dt_utc_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(dt_utc_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(dt_utc_str, "%Y-%m-%dT%H:%M:%S")
    return dt.astimezone(ASIA_SG)

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        log.warning("TG env not set; skip send")
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=15)
    except Exception as e:
        log.exception("telegram send failed: %s", e)

# ── API 호출 ──────────────────────────────────────────────────────────
def fetch_events_range(d1_sg: datetime, d2_sg: datetime) -> List[Dict[str, Any]]:
    base = f"{TE_BASE}?c={quote_plus(TE_AUTH)}&format=json"
    params = {
        "country": ",".join(COUNTRIES),
        "d1": d1_sg.astimezone(utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "d2": d2_sg.astimezone(utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "importance": ",".join(IMPORTANCE),
    }
    url = f"{base}&{urlencode(params)}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        raw = r.json()
        # 국가 필터
        cand = [e for e in raw if e.get("Country") in COUNTRIES]
        # 날짜 필터 (간헐적 과거 끼임 방지)
        events = []
        for e in cand:
            try:
                t_sg = _to_sg(e.get("Date"))
            except Exception:
                continue
            if d1_sg <= t_sg < d2_sg:
                events.append(e)
        # 정렬
        events.sort(key=lambda x: x.get("Date", ""))
        return events
    except Exception as e:
        log.exception("fetch_events error: %s", e)
        return []

def fetch_events_24h(now_sg: datetime) -> List[Dict[str, Any]]:
    return fetch_events_range(now_sg, now_sg + timedelta(hours=24))

# ── 분류/메시지 ──────────────────────────────────────────────────────
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

def _scenario_text(key: str) -> Tuple[str, str, str]:
    default = ("상회 → 위험자산 <b>압박</b>", "부합 → 제한적", "하회 → 위험자산 <b>우호적</b>")
    return CRYPTO_SCENARIOS.get(key, default)

def build_preview(events: List[Dict[str, Any]], now_sg: datetime) -> str:
    lines = ["<b>🇺🇸 오늘/내일 24h 주요 경제이벤트 (사전 시나리오 포함)</b>\n"]
    count = 0
    for e in events:
        title = (e.get("Event") or e.get("Category") or "").strip()
        evttime = _to_sg(e.get("Date"))
        if is_speech(e):
            lines.append(f"🕒 {evttime.strftime('%m/%d %H:%M')} — 🎤 {title}")
            lines.append("   • 매파 ↘ 위험자산, 비둘기 ↗ 우호적")
            lines.append("   • 가이던스/금리 경로·발언 톤 주목\n")
        else:
            forecast, previous = e.get("Forecast"), e.get("Previous")
            key = _classify_event(e)
            up, eq, dn = _scenario_text(key)
            lines.append(f"🕒 {evttime.strftime('%m/%d %H:%M')} — {title}")
            core = []
            if forecast is not None: core.append(f"예상 {forecast}")
            if previous is not None: core.append(f"이전 {previous}")
            if core: lines.append("   • " + ", ".join(core))
            lines.append(f"   • 상회: {up}")
            lines.append(f"   • 부합: {eq}")
            lines.append(f"   • 하회: {dn}\n")
        count += 1
        if count >= 12:
            break
    if count == 0:
        lines.append("(24시간 내 고중요 이벤트 없음)")
    return "\n".join(lines).strip()

def build_release_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    t = _to_sg(e.get("Date"))
    actual, forecast, previous = e.get("Actual"), e.get("Forecast"), e.get("Previous")
    verdict, detail = "중립", "발표 확인"
    key = _classify_event(e)
    up, eq, dn = _scenario_text(key)

    if actual is not None and forecast is not None:
        try:
            a = float(str(actual).replace('%','').replace(',',''))
            f = float(str(forecast).replace('%','').replace(',',''))
            diff = a - f
            thr = 0.1 if any(k in key for k in ["CPI","PCE"]) else 0.001
            if abs(diff) <= thr: verdict, detail = "예상치 부합", eq
            elif diff > 0:      verdict, detail = "예상치 상회", up
            else:               verdict, detail = "예상치 하회", dn
        except Exception:
            verdict, detail = "발표", eq

    info = []
    if actual is not None:   info.append(f"실제 {actual}")
    if forecast is not None: info.append(f"예상 {forecast}")
    if previous is not None: info.append(f"이전 {previous}")

    body = [f"<b>🇺🇸 {title}</b>", f"⏱ {t.strftime('%m/%d %H:%M')} 발표"]
    if info: body.append("📊 " + ", ".join(info))
    body.append(f"💡 해석: <b>{verdict}</b> — {detail}")
    return "\n".join(body)

def build_speech_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    t = _to_sg(e.get("Date"))
    bullets = [
        "• 매파(긴축 장기·인플레 지속) → 달러/수익률 ↑ → 위험자산 <b>압박</b>",
        "• 비둘기(완화 시사·인하 경로) → 달러/수익률 ↓ → 위험자산 <b>우호적</b>",
        "• 가이던스·밸런스시트·금융여건 언급 비중 확인"
    ]
    return "\n".join([f"<b>🎤 {title}</b>", f"⏱ {t.strftime('%m/%d %H:%M')} 시작 (SGT)", "💡 포인트:", *bullets])

# ── 스케줄러/작업 ─────────────────────────────────────────────────────
def send_preview_job():
    now_sg = _sg_now()
    evts = fetch_events_24h(now_sg)

    def score(e):
        t = (e.get("Event") or e.get("Category") or "").lower()
        s = 0
        if is_speech(e): s -= 200
        for i, k in enumerate(IMPORTANT_KEYWORDS):
            if k.lower() in t: s -= (100 - i)
        return s

    evts.sort(key=score)
    tg_send(build_preview(evts, now_sg))

def poll_releases_job():
    now_sg = _sg_now()
    window_end = now_sg + timedelta(minutes=RELEASE_LOOKAHEAD_MIN)
    evts = fetch_events_range(now_sg - timedelta(minutes=1), window_end)
    for e in evts:
        dt = _to_sg(e.get("Date"))
        # 지표 결과
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
            continue

def clean_sent_cache_job():
    now = time.time()
    for k in list(_sent_release_keys.keys()):
        if now - _sent_release_keys[k] > 86400:
            _sent_release_keys.pop(k, None)

_scheduler: BackgroundScheduler = None
_bp = None

def init_econ_calendar(app=None):
    global _scheduler, _bp
    if _scheduler:
        return _scheduler

    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 사전 프리뷰 (매일 설정 시간)
    for t in PREVIEW_TIMES:
        hh, mm = t.split(":")
        _scheduler.add_job(send_preview_job, CronTrigger(hour=int(hh), minute=int(mm)))
    # 실적/연설 모니터링
    _scheduler.add_job(poll_releases_job, "interval", seconds=POLL_SEC)
    # 캐시 청소
    _scheduler.add_job(clean_sent_cache_job, "interval", minutes=30)
    _scheduler.start()

    # 수동 트리거 엔드포인트 (선택)
    if app is not None and Blueprint is not None:
        _bp = Blueprint("econ", __name__)
        @_bp.get("/econ/preview_now")
        def _preview_now():
            if ADMIN_KEY and request.args.get("key") != ADMIN_KEY:
                return "forbidden", 403
            send_preview_job()
            return "ok", 200
        app.register_blueprint(_bp)

    log.info("econ calendar started: preview=%s, poll=%ss, importance=%s, TE=%s",
             PREVIEW_TIMES, POLL_SEC, IMPORTANCE, "custom" if TE_AUTH!="guest:guest" else "guest")
    return _scheduler
