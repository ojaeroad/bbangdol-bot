# econ_calendar_tele_bot.py
# -*- coding: utf-8 -*-
"""
미국 경제지표를 Trading Economics API(guest:guest)에서 가져와
- 매일 08:55, 20:55 (Asia/Singapore) 에 '앞으로 24시간 내' 주요 이벤트 미리보기(예상치 포함)
- 각 이벤트별로 '상회/부합/하회 시' 암호화폐 영향 시나리오(전문가 톤) 동봉
- 발표 시각 모니터링(매 1분) 후 '실제치가 나온 즉시' 결과 해석 코멘트와 함께 텔레그램으로 전송
- 주요 연설(파월 등) 시작 시각에 '연설 해석 가이드' 즉시 전송

Render의 기존 Flask app.py 에서:
from econ_calendar_tele_bot import init_econ_calendar
...
app = Flask(__name__)
init_econ_calendar(app)

환경변수:
  ECON_TG_TOKEN       : 텔레그램 봇 토큰 (bbangdol_bot 등)
  ECON_CHAT_ID        : 보낼 채팅방 ID (예: -4904606442)
  ECON_COUNTRIES      : 기본 'United States' (쉼표구분 다중국가 가능)
  ECON_IMPORTANCE     : 중요도(예: 3 또는 2,3)
  ECON_PREVIEW_TIMES  : '08:55,20:55' (Asia/Singapore 기준)
  TE_AUTH             : TradingEconomics 인증 (기본 guest:guest)

필요 패키지: requests, pytz, apscheduler
"""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone, utc

log = logging.getLogger("econ-calendar")

ASIA_SG = timezone("Asia/Singapore")
TE_BASE = "https://api.tradingeconomics.com/calendar"
TE_AUTH = os.getenv("TE_AUTH", "guest:guest")  # ex) 'guest:guest' 또는 'key:secret'

TG_TOKEN = os.getenv("ECON_TG_TOKEN", "")
TG_CHAT  = os.getenv("ECON_CHAT_ID", "")

COUNTRIES     = [s.strip() for s in os.getenv("ECON_COUNTRIES", "United States").split(",") if s.strip()]
IMPORTANCE    = [s.strip() for s in os.getenv("ECON_IMPORTANCE", "3").split(",") if s.strip()]
PREVIEW_TIMES = [s.strip() for s in os.getenv("ECON_PREVIEW_TIMES", "08:55,20:55").split(",") if s.strip()]

# 발표 감시 윈도우
POLL_SEC = int(os.getenv("ECON_POLL_SEC", "60"))                 # 60초마다
RELEASE_LOOKAHEAD_MIN = int(os.getenv("ECON_RELEASE_LOOKAHEAD_MIN", "5"))  # 5분 이내 일정 감시

_sent_release_keys: Dict[str, float] = {}  # 중복 방지 (event id + release time)

# === 공통 유틸 ===

def _sg_now() -> datetime:
    return datetime.now(ASIA_SG)

def _to_sg(dt_utc_str: str) -> datetime:
    # TE의 날짜는 ISO 또는 '%Y-%m-%dT%H:%M:%S' 형식
    try:
        dt = datetime.fromisoformat(dt_utc_str.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.strptime(dt_utc_str, "%Y-%m-%dT%H:%M:%S")
    return dt.astimezone(ASIA_SG)

def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT:
        log.warning("TG env not set; skip send")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": TG_CHAT,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=15)
    except Exception as e:
        log.exception("telegram send failed: %s", e)

# === 경제지표 호출 ===

def fetch_events_24h(now_sg: datetime) -> List[Dict[str, Any]]:
    d1 = now_sg
    d2 = now_sg + timedelta(hours=24)
    return fetch_events_range(d1, d2)

def fetch_events_range(d1_sg: datetime, d2_sg: datetime) -> List[Dict[str, Any]]:
    params = {
        "country": ",".join(COUNTRIES),  # 다중국가: 쉼표 구분
        "d1": d1_sg.astimezone(utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "d2": d2_sg.astimezone(utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "importance": ",".join(IMPORTANCE),
        "c": TE_AUTH,            # ✅ TE 인증은 쿼리스트링으로
        "format": "json"
    }
    url = TE_BASE
    try:
        r = requests.get(url, params=params, timeout=20)  # ✅ auth 파라미터 제거
        r.raise_for_status()
        data = r.json()
        # TE 필드 예: { 'Country', 'Category', 'Event', 'Date', 'Actual', 'Previous', 'Forecast' }
        data = [e for e in data if e.get("Country") in COUNTRIES]
        data.sort(key=lambda x: x.get("Date", ""))
        return data
    except Exception as e:
        log.exception("fetch_events error: %s", e)
        return []

# === 시나리오 엔진 ===

CRYPTO_SCENARIOS = {
    # 카테고리/이벤트 키워드 → (상회, 부합, 하회)
    "CPI": (
        "실제치가 예상치보다 높음 → 인플레 재확인·금리완화 기대 약화 → <b>암호화폐 단기 약세</b>",
        "예상치 부합 → 불확실성 축소 → <b>변동성 제한적</b>",
        "실제치가 예상치보다 낮음 → 인플레 완화 기대 → <b>암호화폐 우호적</b>"
    ),
    "Core CPI": (
        "핵심 인플레 상회 → 연준 매파 리스크 확대 → <b>약세</b>",
        "예상 부합 → 영향 제한적",
        "핵심 인플레 하회 → 연준 완화 기대 상승 → <b>우호적</b>"
    ),
    "PCE": (
        "PCE 상회 → 완화 지연 우려 → <b>약세</b>",
        "예상 부합 → 제한적",
        "PCE 하회 → 완화 기대 강화 → <b>우호적</b>"
    ),
    "Core PCE": (
        "Core 상회 → 매파적 해석 → <b>약세</b>",
        "예상 부합 → 제한적",
        "Core 하회 → 완화 기대 → <b>우호적</b>"
    ),
    "NFP": (
        "고용 서프라이즈(상회) → 임금·수요 견조→ 수익률 ↑ → <b>압박</b>",
        "예상 부합 → 제한적",
        "부진(하회) → 달러·수익률 ↓ → <b>우호적</b> (단, 실업률 급등 시 리스크)"
    ),
    "Unemployment Rate": (
        "실업률 하락(상회 해석) → 경기 과열 신호 → <b>압박</b>",
        "예상 부합 → 제한적",
        "실업률 상승(하회 해석) → 달러 약세 기대로 <b>우호적</b> (급등은 위험회피)"
    ),
    "Retail Sales": (
        "소매 상회 → 수요 견조→ 수익률 ↑ → <b>압박</b>",
        "예상 부합 → 제한적",
        "소매 하회 → 수요 둔화→ 완화 기대 → <b>우호적</b>"
    ),
    "GDP": (
        "성장률 상회 → 긴축 장기화 우려 → <b>중립~약세</b>",
        "예상 부합 → 제한적",
        "성장률 하회 → 둔화·완화 기대 → <b>우호적</b>"
    ),
    "FOMC": (
        "매파적(점도표/가이던스 상회) → <b>약세</b>",
        "중립(예상 부합) → 제한적",
        "비둘기파적(완화 신호) → <b>우호적</b>"
    )
}

IMPORTANT_KEYWORDS = [
    "CPI", "Core CPI", "PCE", "Core PCE", "FOMC", "GDP", "Non-Farm", "Unemployment", "Retail Sales"
]

# ✅ 연설 감지를 위한 키워드
SPEECH_KEYWORDS = [
    "speech", "speaks", "remarks", "press conference", "testifies", "testimony", "hearing"
]
SPEECH_FIGURES = [  # 중앙은행/주요 인물·기관
    "Powell", "Federal Reserve", "Fed", "FOMC", "Yellen",
    "ECB", "Lagarde", "BOE", "SNB", "BOJ", "Kuroda", "Kashkari", "Waller", "Williams"
]

def is_speech(evt: Dict[str, Any]) -> bool:
    title = (evt.get("Event") or evt.get("Category") or "").lower()
    if any(k in title for k in SPEECH_KEYWORDS):
        return True
    if any(p.lower() in title for p in SPEECH_FIGURES) and any(k in title for k in ["speech","remarks","speaks","press","testifies","testimony","hearing"]):
        return True
    return False

def _classify_event(evt: Dict[str, Any]) -> str:
    title = (evt.get("Event") or evt.get("Category") or "").strip()
    for k in IMPORTANT_KEYWORDS:
        if k.lower() in title.lower():
            return k
    return ""

def _scenario_text(key: str) -> Tuple[str, str, str]:
    # 기본 fallback
    default = (
        "실제치가 <b>예상치 상회</b> → 달러·수익률 ↑ → 위험자산 <b>압박</b>",
        "<b>예상치 부합</b> → 변동성 제한적",
        "실제치가 <b>예상치 하회</b> → 완화 기대 ↑ → 위험자산 <b>우호적</b>"
    )
    return CRYPTO_SCENARIOS.get(key, default)

# === 프리뷰(사전) 메시지 ===

def build_speech_preview_lines(e: Dict[str, Any]) -> List[str]:
    title = (e.get("Event") or e.get("Category") or "").strip()
    evttime = _to_sg(e.get("Date"))
    bullets = [
        "   • 매파 신호(인플레 지속·긴축 장기화) → 달러·수익률 ↑ → 암호화폐 <b>압박</b>",
        "   • 비둘기 신호(완화 시사·인하 경로) → 달러·수익률 ↓ → 암호화폐 <b>우호적</b>",
        "   • 포워드 가이던스/밸런스시트/금융여건 언급 주목"
    ]
    return [f"🕒 {evttime.strftime('%m/%d %H:%M')} — 🎤 {title}", *bullets, ""]

def build_preview(events: List[Dict[str, Any]], now_sg: datetime) -> str:
    lines = []
    header = "<b>🇺🇸 오늘/내일 24h 주요 경제이벤트 (사전 시나리오 포함)</b>\n"
    lines.append(header)
    count = 0
    for e in events:
        if is_speech(e):
            lines.extend(build_speech_preview_lines(e))
        else:
            title = (e.get("Event") or e.get("Category") or "").strip()
            evttime = _to_sg(e.get("Date"))
            forecast = e.get("Forecast")
            previous = e.get("Previous")
            key = _classify_event(e)
            up, eq, dn = _scenario_text(key)

            lines.append(f"🕒 {evttime.strftime('%m/%d %H:%M')} — {title}")
            core = []
            if forecast is not None:
                core.append(f"예상 {forecast}")
            if previous is not None:
                core.append(f"이전 {previous}")
            if core:
                lines.append("   • " + ", ".join(core))
            lines.append(f"   • 상회: {up}")
            lines.append(f"   • 부합: {eq}")
            lines.append(f"   • 하회: {dn}")
            lines.append("")
        count += 1
        if count >= 12:
            break
    if count == 0:
        lines.append("(24시간 내 고중요 이벤트 없음)")
    return "\n".join(lines).strip()

# === 결과(실적) 메시지 ===

def build_release_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    t = _to_sg(e.get("Date"))
    actual = e.get("Actual")
    forecast = e.get("Forecast")
    previous = e.get("Previous")

    # 평가
    verdict = "중립"
    detail = "발표 확인"

    key = _classify_event(e)
    up, eq, dn = _scenario_text(key)

    if actual is not None and forecast is not None:
        try:
            a = float(str(actual).replace('%','').replace(',',''))
            f = float(str(forecast).replace('%','').replace(',',''))
            # 단순 판정: 0.05~0.1pp 내는 부합 처리
            diff = a - f
            thr = 0.1 if any(k in key for k in ["CPI","PCE"]) else 0.001
            if abs(diff) <= thr:
                verdict = "예상치 부합"
                detail = eq
            elif diff > 0:
                verdict = "예상치 상회"
                detail = up
            else:
                verdict = "예상치 하회"
                detail = dn
        except Exception:
            verdict = "발표"
            detail = eq

    body = [
        f"<b>🇺🇸 {title}</b>",
        f"⏱ {t.strftime('%m/%d %H:%M')} 발표",
    ]
    info = []
    if actual is not None:
        info.append(f"실제 {actual}")
    if forecast is not None:
        info.append(f"예상 {forecast}")
    if previous is not None:
        info.append(f"이전 {previous}")
    if info:
        body.append("📊 " + ", ".join(info))

    body.append(f"💡 해석: <b>{verdict}</b> — {detail}")
    return "\n".join(body)

# ✅ 연설(시작 시각) 메시지

def build_speech_note(e: Dict[str, Any]) -> str:
    title = (e.get("Event") or e.get("Category") or "").strip()
    t = _to_sg(e.get("Date"))
    bullets = [
        "• 매파 신호(인플레 지속·긴축 장기화·higher for longer) → 달러·수익률 ↑ → 암호화폐 <b>압박</b>",
        "• 비둘기 신호(완화 시사·금리인하 경로·유동성 강조) → 달러·수익률 ↓ → 암호화폐 <b>우호적</b>",
        "• 포워드 가이던스·밸런스시트·금융여건 언급 비중 주목",
        "• 헤드라인 직후 5~15분 변동성 확대 가능 — 초기 과민반응의 되돌림 리스크"
    ]
    body = [
        f"<b>🎤 {title}</b>",
        f"⏱ {t.strftime('%m/%d %H:%M')} 시작 (Asia/Singapore)",
        "💡 해석 가이드:",
        *bullets
    ]
    return "\n".join(body)

# === 스케줄러 ===

def send_preview_job():
    now_sg = _sg_now()
    evts = fetch_events_24h(now_sg)

    # 고중요 키워드 우선 정렬 (+ 연설은 최상위 가중)
    def score(e):
        t = (e.get("Event") or e.get("Category") or "").lower()
        s = 0
        if is_speech(e):
            s -= 200  # 연설 우선
        for i, k in enumerate(IMPORTANT_KEYWORDS):
            if k.lower() in t:
                s -= (100 - i)
        return s

    evts.sort(key=score)
    msg = build_preview(evts, now_sg)
    tg_send(msg)

def poll_releases_job():
    now_sg = _sg_now()
    window_end = now_sg + timedelta(minutes=RELEASE_LOOKAHEAD_MIN)
    evts = fetch_events_range(now_sg - timedelta(minutes=1), window_end)

    for e in evts:
        dt = _to_sg(e.get("Date"))

        # 1) 지표: 실제치가 있으면 즉시 해석 노트 전송
        if dt <= now_sg + timedelta(seconds=5) and e.get("Actual"):
            key = f"REL|{e.get('Event')}|{e.get('Date')}|{e.get('Actual')}"
            if key not in _sent_release_keys:
                _sent_release_keys[key] = time.time()
                tg_send(build_release_note(e))
            continue

        # 2) 연설: 시작 시각 도달 시 '연설 해석 가이드' 전송
        if dt <= now_sg + timedelta(seconds=5) and is_speech(e):
            key = f"SPEECH|{e.get('Event')}|{e.get('Date')}"
            if key not in _sent_release_keys:
                _sent_release_keys[key] = time.time()
                tg_send(build_speech_note(e))
            continue

def clean_sent_cache_job():
    now = time.time()
    keys = list(_sent_release_keys.keys())
    for k in keys:
        if now - _sent_release_keys[k] > 86400:  # 24h 보존
            _sent_release_keys.pop(k, None)

_scheduler: BackgroundScheduler = None

def init_econ_calendar(app=None):
    global _scheduler
    if _scheduler:
        return _scheduler

    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    _scheduler = BackgroundScheduler(timezone=str(ASIA_SG))

    # 사전 프리뷰 알림 (매일 특정 시각)
    for t in PREVIEW_TIMES:
        hh, mm = t.split(":")
        _scheduler.add_job(send_preview_job, CronTrigger(hour=int(hh), minute=int(mm)))

    # 실적/연설 감시 (매 1분)
    _scheduler.add_job(poll_releases_job, "interval", seconds=POLL_SEC)

    # 캐시 청소 (30분마다)
    _scheduler.add_job(clean_sent_cache_job, "interval", minutes=30)

    _scheduler.start()
    log.info("econ calendar scheduler started: preview=%s, poll=%ss", PREVIEW_TIMES, POLL_SEC)
    return _scheduler

# ── 예시: 기존 Flask app.py ──
# from econ_calendar_tele_bot import init_econ_calendar
# app = Flask(__name__)
# init_econ_calendar(app)
# app.run()
