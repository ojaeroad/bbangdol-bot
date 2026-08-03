
"""성과 사이클 이미지·주간·월간 리포트 자동 발송.

기존 실시간 알람 전송 로직과 독립적으로 동작한다.
오류가 발생해도 기존 /bot, /webhook, 자동매매 흐름을 중단하지 않는다.
"""

from __future__ import annotations

import io
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import requests
from PIL import Image, ImageDraw, ImageFont

from performance_store import load_candles, archive_cycle_chart, finish_candle_watch, candle_watch_status
from performance_group_analyzer import (
    EXIT_GROUPS,
    GROUP_LABEL,
    MARKET_GROUPS,
    group_analysis_market_data,
)

log = logging.getLogger("bbangdol-performance-automation")

DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()
BOT_TOKEN = (
    os.getenv("BOT_TOKEN", "").strip()
    or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
)
MEMBER_NOTICE_ENV = "MEMBER_NOTICE_1Q"
NY = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc

POLL_SECONDS = max(30, int(os.getenv("PERFORMANCE_AUTOMATION_POLL_SECONDS", "60")))

def _automation_enabled() -> bool:
    return os.getenv(
        "PERFORMANCE_AUTOMATION_ENABLED", "1"
    ).strip().lower() not in {"0", "false", "off", "no"}

SEND_COIN_SCALP = os.getenv("PERFORMANCE_SEND_COIN_SCALP", "0").strip().lower() not in {"0", "false", "off", "no"}


def _coin_scalp_report_mode() -> str:
    """include: 코인 통합, separate: 코인 본편+단타 별도, exclude: 단타 제외."""
    mode = os.getenv("PERFORMANCE_REPORT_COIN_SCALP_MODE", "separate").strip().lower()
    return mode if mode in {"include", "separate", "exclude"} else "separate"

MARKET_LABEL = {
    "KOREA": "국장",
    "US": "미장",
    "COIN": "코인",
}

# 수익률 결과는 매수방이 아니라 해당 매도 그룹 방으로 발송한다.
EXIT_CHAT_ENV = {
    ("COIN", "SWING"): "BD_SELL_SWING",
    ("COIN", "LONG"): "BD_SELL_LONG",
    ("COIN", "LIFE"): "BD_SELL_LIFE",
    ("KOREA", "SWING"): "SELL_SWING_1Q",
    ("KOREA", "LONG"): "SELL_LONG_1Q",
    ("KOREA", "LIFE"): "SELL_LIFE_1Q",
    ("US", "SWING"): "SELL_SWING_1Q",
    ("US", "LONG"): "SELL_LONG_1Q",
    ("US", "LIFE"): "SELL_LIFE_1Q",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS performance_delivery_log (
    delivery_key VARCHAR(300) PRIMARY KEY,
    delivery_type VARCHAR(40) NOT NULL,
    market VARCHAR(20),
    symbol VARCHAR(100),
    destination_env VARCHAR(100),
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_performance_delivery_type_time
ON performance_delivery_log(delivery_type, delivered_at);

CREATE TABLE IF NOT EXISTS performance_automation_state (
    state_key VARCHAR(100) PRIMARY KEY,
    state_value BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _connect():
    if not DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")
    return psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        connect_timeout=8,
        application_name="bbangdol-performance-automation",
    )


def ensure_schema() -> None:
    with _connect() as conn:
        conn.execute(SCHEMA_SQL)


def _current_max_high_signal_id() -> int:
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM performance_signals WHERE signal_type='HIGH'"
        ).fetchone()
    return int(row[0] or 0)


def _get_state(key: str) -> int | None:
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT state_value FROM performance_automation_state WHERE state_key=%s",
            (key,),
        ).fetchone()
    return int(row[0]) if row else None


def _set_state(key: str, value: int) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO performance_automation_state(state_key, state_value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (state_key) DO UPDATE
            SET state_value=EXCLUDED.state_value, updated_at=NOW()
            """,
            (key, int(value)),
        )


def _bootstrap_or_get_high_watermark() -> tuple[int, bool]:
    key = "last_processed_high_signal_id"
    existing = _get_state(key)
    if existing is not None:
        return existing, False
    current = _current_max_high_signal_id()
    _set_state(key, current)
    log.warning(
        "performance automation baseline initialized high_signal_id=%s; historical results skipped",
        current,
    )
    return current, True


def _claim(
    delivery_key: str,
    delivery_type: str,
    market: str | None,
    symbol: str | None,
    destination_env: str,
) -> bool:
    """DB 원자적 선점. Gunicorn 프로세스가 여러 개여도 한 번만 발송."""
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            """
            INSERT INTO performance_delivery_log(
                delivery_key, delivery_type, market, symbol, destination_env
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (delivery_key) DO NOTHING
            RETURNING delivery_key
            """,
            (delivery_key, delivery_type, market, symbol, destination_env),
        ).fetchone()
    return bool(row)


def _release(delivery_key: str) -> None:
    """전송 실패 시 다음 검사에서 재시도할 수 있도록 선점 해제."""
    try:
        with _connect() as conn:
            conn.execute(
                "DELETE FROM performance_delivery_log WHERE delivery_key=%s",
                (delivery_key,),
            )
    except Exception:
        log.exception("delivery claim release failed key=%s", delivery_key)


_FONT_LOCK = threading.Lock()
_FONT_PATHS: dict[str, str] = {}
_FONT_ERROR: str | None = None
_FONT_DIR = Path("/tmp/bbangdol-fonts")
_FONT_URLS = {
    "regular": "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/Korean/NotoSansCJKkr-Regular.otf",
    "bold": "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/Korean/NotoSansCJKkr-Bold.otf",
}


def _prepare_korean_fonts() -> dict[str, str]:
    """한글 폰트를 찾거나 공식 Noto CJK 저장소에서 /tmp로 준비한다."""
    global _FONT_ERROR
    if _FONT_PATHS.get("regular") and _FONT_PATHS.get("bold"):
        return dict(_FONT_PATHS)
    with _FONT_LOCK:
        if _FONT_PATHS.get("regular") and _FONT_PATHS.get("bold"):
            return dict(_FONT_PATHS)
        system_candidates = {
            "regular": [
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                "/usr/share/fonts/opentype/noto/NotoSansCJKkr-Regular.otf",
                "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            ],
            "bold": [
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
                "/usr/share/fonts/opentype/noto/NotoSansCJKkr-Bold.otf",
                "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
            ],
        }
        for weight, candidates in system_candidates.items():
            for candidate in candidates:
                if Path(candidate).exists():
                    _FONT_PATHS[weight] = candidate
                    break
        try:
            _FONT_DIR.mkdir(parents=True, exist_ok=True)
            for weight, url in _FONT_URLS.items():
                if _FONT_PATHS.get(weight):
                    continue
                target = _FONT_DIR / f"NotoSansCJKkr-{weight}.otf"
                if not target.exists() or target.stat().st_size < 1_000_000:
                    response = requests.get(url, timeout=45)
                    response.raise_for_status()
                    target.write_bytes(response.content)
                # 실제 Pillow 로딩으로 파일 유효성 확인
                ImageFont.truetype(str(target), 24)
                _FONT_PATHS[weight] = str(target)
            _FONT_ERROR = None
        except Exception as exc:
            _FONT_ERROR = str(exc)
            log.exception("Korean font preparation failed")
        if not (_FONT_PATHS.get("regular") and _FONT_PATHS.get("bold")):
            raise RuntimeError(f"Korean font is not ready: {_FONT_ERROR or 'font not found'}")
        return dict(_FONT_PATHS)


def _font(size: int, bold: bool = False):
    paths = _prepare_korean_fonts()
    return ImageFont.truetype(paths["bold" if bold else "regular"], size)


def _font_status() -> dict[str, Any]:
    try:
        paths = _prepare_korean_fonts()
        return {
            "korean_font_ready": True,
            "regular_font": Path(paths["regular"]).name,
            "bold_font": Path(paths["bold"]).name,
            "font_error": None,
        }
    except Exception as exc:
        return {
            "korean_font_ready": False,
            "regular_font": None,
            "bold_font": None,
            "font_error": str(exc),
        }


def _duration(minutes: float | int | None) -> str:
    if minutes is None:
        return "-"
    value = max(0, int(minutes))
    days, rem = divmod(value, 1440)
    hours, mins = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}일")
    if hours:
        parts.append(f"{hours}시간")
    if mins or not parts:
        parts.append(f"{mins}분")
    return " ".join(parts)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _format_kst(value: Any, multiline: bool = False) -> str:
    dt = _parse_datetime(value)
    if dt is None:
        return "-"
    local = dt.astimezone(KST)
    return local.strftime("%y.%m.%d\\n%H:%M KST" if multiline else "%y.%m.%d %H:%M KST")


def _price(value: Any) -> str:
    """가격을 불필요하게 길게 표시하지 않고 약 4자리 유효숫자로 정리한다."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if number == 0:
        return "0"
    import math
    magnitude = math.floor(math.log10(abs(number)))
    decimals = max(0, min(10, 3 - magnitude))  # 약 4자리 유효숫자
    return f"{number:,.{decimals}f}".rstrip("0").rstrip(".")


def _send_photo(chat_id: str, png: bytes, caption: str) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN/TELEGRAM_BOT_TOKEN is not configured")
    response = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
        data={"chat_id": chat_id, "caption": caption[:1024]},
        files={"photo": ("performance.png", png, "image/png")},
        timeout=30,
    )
    result = response.json()
    if not response.ok or not result.get("ok"):
        raise RuntimeError(f"Telegram sendPhoto failed: {result}")


def _base_canvas(height: int = 1350):
    image = Image.new("RGB", (1080, height), "#0c0d0f")
    return image, ImageDraw.Draw(image)


def _rounded(draw, box, fill="#191a1e", outline="#34363d", radius=28, width=2):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def _png_bytes(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, "PNG", optimize=True)
    return output.getvalue()



def _chart_interval(entry_group: str) -> int:
    return 1 if entry_group in {"SCALP", "SWING"} else 5


def _telegram_send_allowed(market: str, exit_group: str) -> bool:
    return exit_group in {"SWING", "LONG", "LIFE"}

def _draw_signal_flow_fallback(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    entry_points: list[dict[str, Any]],
    exit_price: float,
) -> None:
    """캔들이 없을 때 실제 매수·종료 신호 가격 흐름을 표시한다."""
    left, top, right, bottom = box
    values = []
    for item in entry_points or []:
        try:
            values.append(float(item.get("price")))
        except (TypeError, ValueError):
            pass
    values.append(float(exit_price))
    low, high = min(values), max(values)
    span = max(high - low, abs(high) * 0.001, 1e-9)
    count = max(2, len(values))
    coords = []
    for index, value in enumerate(values):
        x = left + 40 + (right - left - 80) * index / (count - 1)
        y = bottom - 50 - (bottom - top - 100) * (value - low) / span
        coords.append((x, y))
    if len(coords) > 1:
        draw.line(coords, fill="#54e39a", width=5)
    for index, (x, y) in enumerate(coords):
        is_exit = index == len(coords) - 1
        color = "#54e39a" if is_exit else "#ffc857"
        r = 9 if is_exit else 7
        draw.ellipse((x-r, y-r, x+r, y+r), fill=color)
        draw.text((x-25, y+15), "종료" if is_exit else f"매수 {index+1}", font=_font(17, True), fill=color)
    draw.text((left+18, top+12), "과거 캔들 없음 · 실제 매수/종료 신호 가격 흐름", font=_font(20, True), fill="#a5a6ad")


def render_exit_image(
    market: str,
    symbol: str,
    position: dict[str, Any],
    result: dict[str, Any],
) -> bytes:
    """텔레그램용 최소 정보 수익률 결과 카드."""
    interval = _chart_interval(position["entry_group"])
    candles = load_candles(symbol, position["entry_first_time"], result["exit_time"], interval)
    image, draw = _base_canvas(1510)

    white = "#F7F8FA"
    blue = "#66C7FF"
    green = "#42E39B"
    red = "#FF6673"
    muted = "#A4A9B3"
    gold = "#FFC857"
    panel = "#17191E"
    panel_dark = "#111318"
    line = "#343841"

    cycle_no = position.get("position_sequence") or position.get("cycle_no") or "-"
    return_pct = float(result.get("return_pct") or 0)
    result_color = green if return_pct >= 0 else red
    result_icon = "▲" if return_pct >= 0 else "▼"
    holding_text = result.get("holding_text") or _duration(result.get("holding_minutes"))

    candle_low = min((c["low"] for c in candles), default=None)
    adverse_pct = (
        (candle_low - float(position["entry_price"])) / float(position["entry_price"]) * 100
        if candle_low is not None else float(result.get("signal_adverse_pct") or 0)
    )

    market_name = MARKET_LABEL.get(market, market)
    group_name = GROUP_LABEL.get(position.get("entry_group"), position.get("entry_group", "-"))
    title = f"{market_name} · {group_name} · {symbol} (사이클 {cycle_no})"

    # 제목: 요청한 정보만 한 줄로 표시
    draw.text((55, 45), title, font=_font(38, True), fill=white)

    # 최종 결과
    _rounded(draw, (45, 120, 1035, 350), fill=panel_dark, outline=result_color, radius=30, width=4)
    draw.text((78, 150), "수익률", font=_font(27, True), fill=muted)
    draw.text((78, 198), f"{result_icon} {return_pct:+.2f}%", font=_font(82, True), fill=result_color)
    draw.line((665, 155, 665, 315), fill=line, width=2)
    draw.text((710, 165), "보유기간", font=_font(26, True), fill=muted)
    duration_size = 36 if len(str(holding_text)) <= 10 else 30
    draw.text((710, 218), str(holding_text), font=_font(duration_size, True), fill=white)

    # 매수 / 매도 최소 정보 카드
    entry_x1, entry_x2 = 45, 525
    exit_x1, exit_x2 = 555, 1035
    card_top, card_bottom = 385, 685

    _rounded(draw, (entry_x1, card_top, entry_x2, card_bottom), fill=panel, outline=gold, radius=28, width=3)
    draw.text((75, 418), "① 매수", font=_font(34, True), fill=gold)
    draw.text((75, 490), str(position.get("entry_timeframe") or "-"), font=_font(54, True), fill=white)
    draw.text((205, 500), _price(position.get("entry_price")), font=_font(46, True), fill=white)
    draw.text((410, 505), f"{position.get('entry_count', 0)}회", font=_font(35, True), fill=gold)
    draw.text((75, 565), "시간봉", font=_font(20, True), fill=muted)
    draw.text((205, 565), "진입가", font=_font(20, True), fill=muted)
    draw.text((410, 565), "분할", font=_font(20, True), fill=muted)
    draw.text((75, 625), _format_kst(position.get("entry_first_time")), font=_font(22, True), fill=muted)

    _rounded(draw, (exit_x1, card_top, exit_x2, card_bottom), fill=panel, outline=red, radius=28, width=3)
    draw.text((585, 418), "② 매도", font=_font(34, True), fill=red)
    draw.text((585, 490), str(result.get("exit_timeframe") or "-"), font=_font(54, True), fill=white)
    draw.text((715, 500), _price(result.get("exit_price")), font=_font(46, True), fill=white)
    draw.text((585, 565), "시간봉", font=_font(20, True), fill=muted)
    draw.text((715, 565), "매도가", font=_font(20, True), fill=muted)
    draw.text((585, 625), _format_kst(result.get("exit_time")), font=_font(22, True), fill=muted)

    # 쉬운 표현만 사용
    _rounded(draw, (45, 720, 1035, 835), fill="#191316", outline="#6E3038", radius=24, width=2)
    draw.text((75, 748), "최대 하락폭", font=_font(25, True), fill=red)
    draw.text((75, 785), f"{adverse_pct:+.2f}%", font=_font(39, True), fill=red)

    # 차트
    _rounded(draw, (45, 870, 1035, 1415), fill=panel_dark, outline=line, radius=28, width=2)
    draw.text((75, 900), "③ 매수 → 매도 가격 흐름", font=_font(31, True), fill=white)
    draw.text((75, 944), "노랑 = 매수 · 초록 = 매도", font=_font(21, True), fill=muted)
    if candles:
        _draw_candle_chart(
            draw, (80, 990, 1000, 1360), candles,
            float(position["entry_price"]), position.get("entry_points") or [], float(result["exit_price"]),
        )
    else:
        _draw_signal_flow_fallback(
            draw, (80, 990, 1000, 1360),
            position.get("entry_points") or [],
            float(result["exit_price"]),
        )

    draw.text(
        (55, 1450),
        "※ 신호 가격 기준 · 수수료·슬리피지·세금 미반영",
        font=_font(19), fill=muted,
    )
    return _png_bytes(image)

def render_cycle_summary_image(
    market: str,
    symbol: str,
    position: dict[str, Any],
) -> bytes:
    results = sorted(
        position.get("exit_results") or [],
        key=lambda row: row.get("exit_timeframe_minutes", 0),
    )
    completion_time = max((row["exit_time"] for row in results), default=position["entry_first_time"])
    interval = _chart_interval(position["entry_group"])
    candles = load_candles(symbol, position["entry_first_time"], completion_time, interval)
    height = max(2050, 1510 + len(results) * 125)
    image, draw = _base_canvas(height)
    white, blue, green, red, muted, gold = (
        "#f4f4f5", "#73cfff", "#54e39a", "#ff7f87", "#a5a6ad", "#ffc857"
    )

    draw.text((60, 42), "완료 사이클 종합", font=_font(56, True), fill=gold)
    draw.text((60, 118), f"{MARKET_LABEL.get(market, market)} · {GROUP_LABEL.get(position['entry_group'], position['entry_group'])}", font=_font(32, True), fill=blue)
    draw.text((60, 178), symbol, font=_font(50, True), fill=white)

    # 매수 정보는 별도 카드로 분리하여 종료 정보와 혼동되지 않게 한다.
    _rounded(draw, (45, 265, 1035, 555), fill="#15161a", outline="#ffc857")
    draw.text((70, 286), "① 매수(진입) 정보", font=_font(33, True), fill=gold)
    buy_rows = [
        ("매수 시간봉", position.get("entry_timeframe") or "-"),
        ("평균 진입가", _price(position.get("entry_price"))),
        ("진입 횟수", f"{position.get('entry_count', 0)}회"),
        ("최초 진입 시각", _format_kst(position.get("entry_first_time"))),
    ]
    for idx, (label, value) in enumerate(buy_rows):
        x = 70 + (idx % 2) * 480
        y = 350 + (idx // 2) * 92
        draw.text((x, y), label, font=_font(29, True), fill=blue)
        draw.text((x + 170, y - 3), str(value), font=_font(30, True), fill=white)

    _rounded(draw, (45, 600, 1035, 1125), fill="#111216")
    draw.text((70, 625), f"② 매수 → 매도 가격 흐름 ({interval}분봉 압축)", font=_font(32, True), fill=white)
    final_exit = float(results[-1]["exit_price"]) if results else float(position["entry_price"])
    if candles:
        low = _draw_candle_chart(draw, (80, 700, 1000, 1080), candles, float(position["entry_price"]), position.get("entry_points") or [], final_exit)
    else:
        _draw_signal_flow_fallback(draw, (80, 700, 1000, 1080), position.get("entry_points") or [], final_exit)
        low = None
    adverse = (((low - float(position["entry_price"])) / float(position["entry_price"]) * 100) if low is not None else float(position.get("signal_adverse_pct") or 0))

    draw.text((60, 1170), "③ 매도(종료) 시간봉별 결과", font=_font(36, True), fill=gold)
    draw.text((75, 1225), "매도 TF", font=_font(29, True), fill=blue)
    draw.text((230, 1225), "매도가", font=_font(29, True), fill=blue)
    draw.text((455, 1225), "수익률", font=_font(29, True), fill=blue)
    draw.text((650, 1225), "매도 시각", font=_font(29, True), fill=blue)
    y = 1270
    returns = []
    for result in results:
        value = float(result.get("return_pct") or 0)
        returns.append(value)
        _rounded(draw, (55, y, 1025, y + 102), fill="#15161a")
        draw.text((80, y + 30), f"매도 {result.get('exit_timeframe') or '-'}", font=_font(29, True), fill=blue)
        draw.text((230, y + 30), _price(result.get("exit_price")), font=_font(30, True), fill=white)
        draw.text((455, y + 24), f"{value:+.3f}%", font=_font(32, True), fill=green if value >= 0 else red)
        draw.text((650, y + 21), _format_kst(result.get("exit_time"), multiline=True), font=_font(24, True), fill=white)
        y += 118

    if returns:
        avg = sum(returns) / len(returns)
        _rounded(draw, (55, y + 15, 1025, y + 205), fill="#101216", outline="#3b3e45")
        draw.text((80, y + 40), "종료 평균", font=_font(23, True), fill=blue)
        draw.text((260, y + 34), f"{avg:+.3f}%", font=_font(34, True), fill=green if avg >= 0 else red)
        draw.text((555, y + 40), "최고 수익", font=_font(23, True), fill=blue)
        draw.text((745, y + 34), f"{max(returns):+.3f}%", font=_font(34, True), fill=green if max(returns) >= 0 else red)
        draw.text((80, y + 105), "최저 수익", font=_font(23, True), fill=blue)
        draw.text((260, y + 99), f"{min(returns):+.3f}%", font=_font(34, True), fill=green if min(returns) >= 0 else red)
        draw.text((555, y + 105), "최대 손절폭", font=_font(23, True), fill=blue)
        draw.text((745, y + 99), f"{adverse:+.3f}%", font=_font(34, True), fill=red)

    draw.text((60, height - 75), "※ TradingView 확정 OHLC/신호 가격 기준이며 수수료·슬리피지는 포함하지 않습니다.", font=_font(21), fill=muted)
    return _png_bytes(image)

def _expected_exit_timeframes(market: str, entry_group: str) -> list[str]:
    output: list[str] = []
    for group in EXIT_GROUPS.get(entry_group, []):
        output.extend(MARKET_GROUPS.get(market, {}).get(group, []))
    return output


def _position_key(market: str, symbol: str, position: dict[str, Any]) -> str:
    ids = position.get("entry_signal_ids") or []
    base = ids[0] if ids else f"{position.get('entry_first_time')}:{position.get('position_sequence')}"
    return f"{market}:{symbol}:{base}"


def _exit_destination(market: str, exit_group: str) -> tuple[str, str]:
    env_name = EXIT_CHAT_ENV.get((market, exit_group), "")
    return env_name, os.getenv(env_name, "").strip() if env_name else ""


def process_new_cycle_deliveries(after_high_signal_id: int) -> int:
    """새 HIGH만 처리하여 종료 그룹별 매도방으로 발송한다."""
    observed_max = after_high_signal_id

    for market in ("KOREA", "US", "COIN"):
        market_data = group_analysis_market_data(market)

        for symbol, symbol_data in market_data.get("symbol_data", {}).items():
            for position in symbol_data.get("positions", []):
                if position.get("entry_group") not in {"SWING", "LONG", "LIFE"}:
                    continue

                position_key = _position_key(market, symbol, position)
                all_results = [
                    row for row in (position.get("exit_results") or [])
                    if row.get("exit_group") in {"SWING", "LONG", "LIFE"}
                ]

                # 새 개별 종료 결과는 종료 그룹에 해당하는 매도방으로 발송한다.
                for result in all_results:
                    try:
                        exit_id = int(result.get("exit_signal_id") or 0)
                    except (TypeError, ValueError):
                        continue

                    observed_max = max(observed_max, exit_id)
                    if exit_id <= after_high_signal_id:
                        continue

                    exit_group = str(result.get("exit_group") or "")
                    env_name, chat_id = _exit_destination(market, exit_group)
                    if not _telegram_send_allowed(market, exit_group):
                        continue
                    if not env_name or not chat_id:
                        log.warning(
                            "sell destination missing market=%s group=%s env=%s",
                            market, exit_group, env_name,
                        )
                        continue

                    delivery_key = (
                        f"exit-v4:{position_key}:{exit_id}:"
                        f"{exit_group}:{result.get('exit_timeframe')}"
                    )
                    if not _claim(
                        delivery_key, "EXIT_IMAGE", market, symbol, env_name
                    ):
                        continue

                    try:
                        png = render_exit_image(market, symbol, position, result)
                        caption = (
                            f"📈 {symbol} · 매도 "
                            f"{GROUP_LABEL.get(exit_group, exit_group)} 결과\n"
                            f"매수 {position.get('entry_timeframe','-')} · "
                            f"{_format_kst(position.get('entry_first_time'))}\n"
                            f"매도 {result.get('exit_timeframe','-')} · "
                            f"{_format_kst(result.get('exit_time'))}\n"
                            f"수익률 {float(result['return_pct']):+.3f}% · "
                            f"보유 {result.get('holding_text') or _duration(result.get('holding_minutes'))}"
                        )
                        _send_photo(chat_id, png, caption)
                        log.info(
                            "sell result sent market=%s symbol=%s exit_id=%s "
                            "exit_group=%s exit_tf=%s env=%s",
                            market, symbol, exit_id, exit_group,
                            result.get("exit_timeframe"), env_name,
                        )
                    except Exception:
                        _release(delivery_key)
                        log.exception(
                            "sell result delivery failed key=%s", delivery_key
                        )

                # 종료 그룹별 필요한 시간봉이 모두 모이면 같은 매도방에 종합 1장.
                for exit_group in ("SWING", "LONG", "LIFE"):
                    expected = set(
                        MARKET_GROUPS.get(market, {}).get(exit_group, [])
                    )
                    group_results = [
                        row for row in all_results
                        if row.get("exit_group") == exit_group
                    ]
                    completed = {
                        row.get("exit_timeframe") for row in group_results
                    }
                    trigger_id = max(
                        (
                            int(row.get("exit_signal_id") or 0)
                            for row in group_results
                        ),
                        default=0,
                    )

                    if (
                        not expected
                        or not expected.issubset(completed)
                        or trigger_id <= after_high_signal_id
                    ):
                        continue

                    env_name, chat_id = _exit_destination(market, exit_group)
                    if not env_name or not chat_id:
                        continue

                    summary_key = (
                        f"exit-group-summary-v4:{position_key}:"
                        f"{exit_group}:{trigger_id}"
                    )
                    if not _claim(
                        summary_key, "EXIT_GROUP_SUMMARY",
                        market, symbol, env_name
                    ):
                        continue

                    try:
                        summary_position = dict(position)
                        summary_position["exit_results"] = group_results
                        png = render_cycle_summary_image(
                            market, symbol, summary_position
                        )
                        values = [
                            float(row["return_pct"]) for row in group_results
                        ]
                        entry_tf = position.get("entry_timeframe", "-")
                        entry_price = _price(position.get("entry_price"))
                        entry_count = position.get("entry_count", 0)
                        caption_lines = [
                            f"✅ {symbol} · 매도 {GROUP_LABEL.get(exit_group, exit_group)} 종합",
                            f"🟠 매수 {entry_tf}  |  평균 진입가 {entry_price}  |  {entry_count}회  |  {_format_kst(position.get('entry_first_time'))}",
                        ]
                        for row in sorted(group_results, key=lambda item: item.get("exit_timeframe_minutes", 0)):
                            caption_lines.append(
                                f"🟢 매도 {row.get('exit_timeframe','-')}  |  "
                                f"매도가 {_price(row.get('exit_price'))}  |  "
                                f"수익률 {float(row.get('return_pct') or 0):+.3f}%  |  "
                                f"{_format_kst(row.get('exit_time'))}"
                            )
                        caption_lines.append(
                            f"평균 {sum(values)/len(values):+.3f}% · "
                            f"최고 {max(values):+.3f}% · 최저 {min(values):+.3f}%"
                        )
                        caption = "\n".join(caption_lines)
                        _send_photo(chat_id, png, caption)
                        completion_time = max(
                            row["exit_time"] for row in group_results
                        )
                        archive_cycle_chart(
                            summary_key, market, symbol,
                            position["entry_first_time"],
                            completion_time, png,
                        )
                        log.info(
                            "sell group summary sent market=%s symbol=%s "
                            "group=%s trigger_id=%s env=%s",
                            market, symbol, exit_group, trigger_id, env_name,
                        )
                    except Exception:
                        _release(summary_key)
                        log.exception(
                            "sell group summary failed key=%s", summary_key
                        )

                # 모든 30m 이상 종료 그룹이 완료되면 원본 캔들을 정리한다.
                expected_all = {
                    tf
                    for group in ("SWING", "LONG", "LIFE")
                    for tf in MARKET_GROUPS.get(market, {}).get(group, [])
                }
                completed_all = {
                    row.get("exit_timeframe") for row in all_results
                }
                trigger_all = max(
                    (
                        int(row.get("exit_signal_id") or 0)
                        for row in all_results
                    ),
                    default=0,
                )

                if (
                    expected_all
                    and expected_all.issubset(completed_all)
                    and trigger_all > after_high_signal_id
                ):
                    completion_time = max(
                        row["exit_time"] for row in all_results
                    )
                    watch = candle_watch_status(symbol)
                    watch_started = watch.get("started_at") if watch else None
                    incomplete = False

                    for other in symbol_data.get("positions", []):
                        if other.get("entry_group") not in {
                            "SWING", "LONG", "LIFE"
                        }:
                            continue
                        try:
                            other_start = datetime.fromisoformat(
                                other["entry_first_time"]
                            )
                        except Exception:
                            continue
                        if watch_started and other_start < watch_started:
                            continue

                        other_expected = {
                            tf
                            for group in ("SWING", "LONG", "LIFE")
                            for tf in MARKET_GROUPS.get(
                                market, {}
                            ).get(group, [])
                        }
                        other_done = {
                            row.get("exit_timeframe")
                            for row in (other.get("exit_results") or [])
                            if row.get("exit_group") in {
                                "SWING", "LONG", "LIFE"
                            }
                        }
                        if (
                            other_expected
                            and not other_expected.issubset(other_done)
                        ):
                            incomplete = True
                            break

                    if not incomplete:
                        deleted = finish_candle_watch(
                            symbol, completion_time
                        )
                        log.info(
                            "candle watch finished symbol=%s deleted=%s",
                            symbol, deleted,
                        )

    return observed_max


def _report_target(
    report_market: str,
) -> tuple[str, set[str] | None, set[str] | None, ZoneInfo]:
    # 5m·15m 단타는 모든 성과 리포트에서 제외한다.
    if report_market == "COIN":
        return "COIN", {"SWING", "LONG", "LIFE"}, None, KST
    if report_market == "KOREA":
        return "KOREA", {"SWING", "LONG", "LIFE"}, None, KST
    if report_market == "US":
        return "US", {"SWING", "LONG", "LIFE"}, None, NY
    raise ValueError(f"unsupported report market: {report_market}")


def _period_bounds(
    kind: str,
    report_market: str,
    now_local: datetime,
    tz: ZoneInfo,
) -> tuple[datetime, datetime, str]:
    """리포트 시장별 집계 구간을 UTC 반개구간 [start, end)으로 반환한다.

    완료 사이클은 진입 시각과 무관하게 종료 시각으로 기간에 귀속한다.
    - 국장 주간: KST 월요일 09:00 ~ 금요일 15:30 포함
    - 미장 주간: ET 월요일 09:30 ~ 금요일 16:00 포함
    - 코인 주간: KST 이전 월요일 06:00 ~ 현재 월요일 06:00 미만
    - 국장/미장 월간: 해당 달 1일 00:00 ~ 다음 달 1일 00:00 미만
    - 코인 월간: 이전 달 1일 06:00 ~ 현재 달 1일 06:00 미만
    """
    local_now = now_local.astimezone(tz)

    if kind == "weekly":
        if report_market == "COIN":
            # 자동 발송 시점은 KST 월요일 06:00이다.
            end_local = local_now.replace(hour=6, minute=0, second=0, microsecond=0)
            end_local -= timedelta(days=end_local.weekday())
            if local_now < end_local:
                end_local -= timedelta(days=7)
            start_local = end_local - timedelta(days=7)
            label = f"{start_local:%Y-%m-%d %H:%M} ~ {end_local:%Y-%m-%d %H:%M} KST"
        else:
            monday = (local_now - timedelta(days=local_now.weekday())).date()
            if report_market == "KOREA":
                start_local = datetime.combine(monday, datetime.min.time(), tzinfo=tz).replace(hour=9)
                close_local = start_local + timedelta(days=4, hours=6, minutes=30)
                zone_label = "KST"
            elif report_market == "US":
                start_local = datetime.combine(monday, datetime.min.time(), tzinfo=tz).replace(hour=9, minute=30)
                close_local = start_local + timedelta(days=4, hours=6, minutes=30)
                zone_label = "ET"
            else:
                raise ValueError(f"unsupported report market: {report_market}")
            # 장 마감 정각의 신호까지 포함하기 위해 종료 경계는 1분 뒤로 둔다.
            end_local = close_local + timedelta(minutes=1)
            label = f"{start_local:%Y-%m-%d %H:%M} ~ {close_local:%Y-%m-%d %H:%M} {zone_label}"

    elif kind == "monthly":
        if report_market == "COIN":
            # 매월 1일 06:00 발송 시 직전 월의 1일 06:00부터 집계한다.
            end_local = local_now.replace(day=1, hour=6, minute=0, second=0, microsecond=0)
            if local_now < end_local:
                if end_local.month == 1:
                    end_local = end_local.replace(year=end_local.year - 1, month=12)
                else:
                    end_local = end_local.replace(month=end_local.month - 1)
            if end_local.month == 1:
                start_local = end_local.replace(year=end_local.year - 1, month=12)
            else:
                start_local = end_local.replace(month=end_local.month - 1)
            label = f"{start_local:%Y-%m-%d %H:%M} ~ {end_local:%Y-%m-%d %H:%M} KST"
        else:
            start_local = local_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if start_local.month == 12:
                end_local = start_local.replace(year=start_local.year + 1, month=1)
            else:
                end_local = start_local.replace(month=start_local.month + 1)
            zone_label = "KST" if report_market == "KOREA" else "ET"
            label = f"{start_local:%Y-%m-%d} ~ {(end_local - timedelta(days=1)):%Y-%m-%d} {zone_label}"
    else:
        raise ValueError(f"unsupported period kind: {kind}")

    return start_local.astimezone(UTC), end_local.astimezone(UTC), label


def _collect_period(kind: str, report_market: str, now_local: datetime):
    market, include_groups, exclude_groups, tz = _report_target(report_market)
    start_utc, end_utc, label = _period_bounds(kind, report_market, now_local, tz)
    data = group_analysis_market_data(market)
    rows: list[dict[str, Any]] = []
    symbols: set[str] = set()
    group_rows: dict[str, list[dict[str, Any]]] = {}
    for symbol, symbol_data in data.get("symbol_data", {}).items():
        for position in symbol_data.get("positions", []):
            entry_group = str(position.get("entry_group") or "")
            if include_groups and entry_group not in include_groups:
                continue
            if exclude_groups and entry_group in exclude_groups:
                continue
            for result in position.get("exit_results") or []:
                group = str(result.get("exit_group") or "")
                if group not in {"SWING", "LONG", "LIFE"}:
                    continue
                exit_time = _parse_datetime(result.get("exit_time"))
                if exit_time is None or not (start_utc <= exit_time.astimezone(UTC) < end_utc):
                    continue
                row = {
                    "market": market,
                    "symbol": symbol,
                    "entry_group": entry_group,
                    "exit_group": group,
                    "entry_timeframe": position.get("entry_timeframe"),
                    "entry_time": position.get("entry_first_time"),
                    **result,
                }
                rows.append(row)
                group_rows.setdefault(group, []).append(row)
                symbols.add(symbol)
    values = [float(row.get("return_pct") or 0) for row in rows]
    stats = {
        "rows": rows,
        "count": len(rows),
        "symbol_count": len(symbols),
        "average": sum(values) / len(values) if values else None,
        "best": max(values) if values else None,
        "worst": min(values) if values else None,
        "win_rate": (sum(1 for value in values if value > 0) / len(values) * 100) if values else None,
        "average_holding": (sum(float(row.get("holding_minutes") or 0) for row in rows) / len(rows)) if rows else None,
    }
    grouped = []
    for group in MARKET_GROUPS.get(market, {}):
        selected = group_rows.get(group, [])
        if not selected:
            continue
        selected_values = [float(row.get("return_pct") or 0) for row in selected]
        grouped.append({
            "group": group,
            "count": len(selected),
            "average": sum(selected_values) / len(selected_values),
            "best": max(selected_values),
            "win_rate": sum(1 for value in selected_values if value > 0) / len(selected_values) * 100,
            "average_holding": sum(float(row.get("holding_minutes") or 0) for row in selected) / len(selected),
        })
    rows.sort(key=lambda row: float(row.get("return_pct") or 0), reverse=True)
    return stats, grouped, rows, label



KRX_REPORT_NAMES = {
    "005930":"삼성전자","000660":"SK하이닉스","005380":"현대차","032830":"삼성생명",
    "373220":"LG에너지솔루션","207940":"삼성바이오로직스","000270":"기아","068270":"셀트리온",
    "105560":"KB금융","055550":"신한지주","035420":"NAVER","035720":"카카오",
    "012450":"한화에어로스페이스","034020":"두산에너빌리티","086520":"에코프로",
    "247540":"에코프로비엠","006400":"삼성SDI","051910":"LG화학","005490":"POSCO홀딩스",
    "028260":"삼성물산","012330":"현대모비스","066570":"LG전자","003670":"포스코퓨처엠",
    "009150":"삼성전기","042700":"한미반도체","000810":"삼성화재","329180":"HD현대중공업"
}

def _report_symbol_name(market: str, symbol: str) -> str:
    code = str(symbol or "").upper().split(":")[-1].replace(".KS","").replace(".KQ","")
    return f"{KRX_REPORT_NAMES.get(code, code)}({code})" if market == "KOREA" and code.isdigit() else code

def _distinct_best_rows(rows):
    best = {}
    for row in rows:
        symbol = row.get("symbol")
        if symbol not in best or float(row.get("return_pct") or 0) > float(best[symbol].get("return_pct") or 0):
            best[symbol] = row
    return sorted(best.values(), key=lambda r: float(r.get("return_pct") or 0), reverse=True)

def _report_display_label(report_market: str) -> str:
    return {"KOREA": "국장", "US": "미장", "COIN": "코인"}[report_market]


def render_period_report(kind: str, report_market: str, now_local: datetime) -> tuple[bytes, str]:
    stats, grouped, rows, label = _collect_period(kind, report_market, now_local)
    top_rows = _distinct_best_rows(rows)
    period_name = "주간" if kind == "weekly" else "월간"
    market_label = _report_display_label(report_market)
    title = f"{market_label} {period_name} 성과 리포트"
    height = max(1500, 710 + len(grouped) * 180 + min(5, len(top_rows)) * 150)
    image, draw = _base_canvas(height)
    white, blue, green, red, muted, gold = (
        "#f4f4f5", "#73cfff", "#54e39a", "#ff7f87", "#a5a6ad", "#ffc857"
    )
    draw.text((55, 45), title, font=_font(48, True), fill=white)
    draw.text((55, 112), label, font=_font(27, True), fill=blue)
    subtitle = "단타 제외 · 스윙/장기/인생타점" if report_market == "COIN" and _coin_scalp_report_mode() != "include" else "완료 결과 기준"
    draw.text((55, 160), subtitle, font=_font(22), fill=muted)

    _rounded(draw, (45, 225, 1035, 495))
    if stats["average"] is None:
        draw.text((75, 305), "기간 내 완료 결과 없음", font=_font(32, True), fill=muted)
    else:
        metrics = [
            ("평균 수익률", f"{stats['average']:+.2f}%", green if stats['average'] >= 0 else red),
            ("최고 수익률", f"{stats['best']:+.2f}%", green),
            ("최저 수익률", f"{stats['worst']:+.2f}%", red if stats['worst'] < 0 else green),
            ("승률", f"{stats['win_rate']:.1f}%", white),
            ("완료 결과", f"{stats['count']}건", white),
            ("종목", f"{stats['symbol_count']}개", white),
        ]
        for idx, (metric, value, color) in enumerate(metrics):
            x = 70 + (idx % 3) * 320
            y = 260 + (idx // 3) * 115
            draw.text((x, y), metric, font=_font(20, True), fill=blue)
            draw.text((x, y + 38), value, font=_font(31, True), fill=color)
        draw.text((705, 450), f"평균 보유 {_duration(stats['average_holding'])}", font=_font(19), fill=muted)

    y = 540
    if grouped:
        draw.text((55, y), "포지션별 성과", font=_font(31, True), fill=white)
        y += 55
        for item in grouped:
            _rounded(draw, (50, y, 1030, y + 145), fill="#15161a")
            draw.text((80, y + 20), GROUP_LABEL.get(item["group"], item["group"]), font=_font(27, True), fill=blue)
            draw.text((300, y + 20), f"평균 {item['average']:+.2f}%", font=_font(27, True), fill=green if item['average'] >= 0 else red)
            draw.text((600, y + 20), f"최고 {item['best']:+.2f}%", font=_font(25, True), fill=green)
            draw.text((80, y + 82), f"승률 {item['win_rate']:.1f}% · 완료 {item['count']}건", font=_font(21), fill=white)
            draw.text((650, y + 82), f"평균보유 {_duration(item['average_holding'])}", font=_font(19), fill=muted)
            y += 165

    draw.text((55, y + 5), "TOP 5", font=_font(33, True), fill=gold)
    y += 60
    for rank, row in enumerate(top_rows[:5], 1):
        _rounded(draw, (50, y, 1030, y + 135), fill="#15161a")
        draw.text((75, y + 22), str(rank), font=_font(28, True), fill=gold)
        draw.text((125, y + 18), _report_symbol_name("KOREA" if report_market == "KOREA" else report_market, row["symbol"]), font=_font(24, True), fill=white)
        draw.text((390, y + 18), f"매수 {row.get('entry_timeframe','-')} → 종료 {row.get('exit_timeframe','-')}", font=_font(19), fill=blue)
        draw.text((125, y + 67), f"매수 {_format_kst(row.get('entry_time'))}  |  종료 {_format_kst(row.get('exit_time'))}", font=_font(17), fill=muted)
        value = float(row.get("return_pct") or 0)
        draw.text((830, y + 17), f"{value:+.2f}%", font=_font(30, True), fill=green if value >= 0 else red)
        y += 147
    if not top_rows:
        draw.text((75, y + 10), "기간 내 완료된 종료 결과가 없습니다.", font=_font(25), fill=muted)
    draw.text((55, height - 65), "※ 알람 신호 가격 기준이며 수수료·슬리피지·세금은 포함하지 않습니다.", font=_font(18), fill=muted)
    caption = f"📊 {title} · {label}\\n{market_label} 완료 결과 현황"
    return _png_bytes(image), caption


def _is_last_weekday_of_month(now_local: datetime) -> bool:
    tomorrow = (now_local + timedelta(days=1)).date()
    cursor = tomorrow
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor.month != now_local.month


def _send_period_report(chat_id: str, kind: str, report_market: str, now_local: datetime) -> None:
    key = f"{kind}:{report_market}:{now_local:%Y-%m-%d}" if kind == "weekly" else f"{kind}:{report_market}:{now_local:%Y-%m}"
    if not _claim(key, f"{kind.upper()}_REPORT", report_market, None, MEMBER_NOTICE_ENV):
        return
    try:
        png, caption = render_period_report(kind, report_market, now_local)
        _send_photo(chat_id, png, caption)
        log.info("%s report sent market=%s key=%s", kind, report_market, key)
    except Exception:
        _release(key)
        log.exception("%s report failed market=%s key=%s", kind, report_market, key)


def process_scheduled_reports() -> None:
    chat_id = os.getenv(MEMBER_NOTICE_ENV, "").strip()
    if not chat_id:
        return
    now_kst = datetime.now(KST)
    now_ny = datetime.now(NY)

    # 국장: 한국시간 금요일 15:30 장 종료 후 1시간, 16:30부터 1회
    if now_kst.weekday() == 4 and now_kst.hour == 16 and now_kst.minute >= 30:
        _send_period_report(chat_id, "weekly", "KOREA", now_kst)
    # 미장: 뉴욕시간 금요일 정규장 종료 1시간 후 17시
    if now_ny.weekday() == 4 and now_ny.hour == 17:
        _send_period_report(chat_id, "weekly", "US", now_ny)
    # 코인: 한국시간 월요일 오전 06:00, 직전 월요일 06:00부터 집계
    if now_kst.weekday() == 0 and now_kst.hour == 6:
        _send_period_report(chat_id, "weekly", "COIN", now_kst)

    # 월간 국장: 마지막 평일 16:30 KST
    if _is_last_weekday_of_month(now_kst) and now_kst.hour == 16 and now_kst.minute >= 30:
        _send_period_report(chat_id, "monthly", "KOREA", now_kst)
    # 월간 미장: 마지막 평일 17:00 New York
    if _is_last_weekday_of_month(now_ny) and now_ny.hour == 17:
        _send_period_report(chat_id, "monthly", "US", now_ny)
    # 월간 코인: 한국시간 매월 1일 오전 06:00, 직전 월 1일 06:00부터 집계
    if now_kst.day == 1 and now_kst.hour == 6:
        _send_period_report(chat_id, "monthly", "COIN", now_kst)


def run_once() -> None:
    if not _automation_enabled():
        return
    if not DATABASE_URL or not BOT_TOKEN:
        log.warning(
            "performance automation skipped database=%s bot_token=%s",
            bool(DATABASE_URL), bool(BOT_TOKEN),
        )
        return

    watermark, bootstrapped = _bootstrap_or_get_high_watermark()
    if bootstrapped:
        return

    current_max = _current_max_high_signal_id()
    if current_max > watermark:
        process_new_cycle_deliveries(watermark)
        _set_state("last_processed_high_signal_id", current_max)
    process_scheduled_reports()


def _loop() -> None:
    time.sleep(15)
    while True:
        try:
            if _automation_enabled():
                run_once()
        except Exception:
            log.exception("performance automation loop failed")
        time.sleep(POLL_SECONDS)


_LOCK = threading.Lock()
_STARTED = False


def start_performance_automation() -> bool:
    global _STARTED
    if not _automation_enabled():
        log.warning("performance automation hard-disabled; background thread not started")
        return False
    with _LOCK:
        if _STARTED:
            return False
        _STARTED = True
        threading.Thread(
            target=_loop,
            daemon=True,
            name="performance-automation",
        ).start()
    log.info("performance automation thread started")
    return True



def automation_status() -> dict[str, Any]:
    """관리자 화면에서 민감값 없이 자동발송 준비 상태를 확인."""
    notice_id = os.getenv(MEMBER_NOTICE_ENV, "").strip()
    return {
        "ok": True,
        "enabled": _automation_enabled(),
        "database_configured": bool(DATABASE_URL),
        "bot_token_configured": bool(BOT_TOKEN),
        "member_notice_env": MEMBER_NOTICE_ENV,
        "member_notice_configured": bool(notice_id),
        "high_signal_watermark": _get_state("last_processed_high_signal_id") if DATABASE_URL else None,
        "no_backfill_mode": True,
        "minimum_performance_timeframe": "30m",
        "scalp_performance_excluded": True,
        **_font_status(),
        "poll_seconds": POLL_SECONDS,
        "thread_started": _STARTED,
        "result_destinations": {
            f"{market}_{group}": {
                "env": env_name,
                "configured": bool(os.getenv(env_name, "").strip()),
            }
            for (market, group), env_name in EXIT_CHAT_ENV.items()
        },
    }


def send_period_report_test(kind: str, report_market: str | None = None) -> dict[str, Any]:
    """회원 공지방으로 시장별 주간/월간 리포트를 즉시 테스트 발송."""
    kind = str(kind or "").strip().lower()
    if kind not in {"weekly", "monthly"}:
        raise ValueError("kind must be weekly or monthly")
    chat_id = os.getenv(MEMBER_NOTICE_ENV, "").strip()
    if not chat_id:
        raise RuntimeError(f"{MEMBER_NOTICE_ENV} is not configured")
    requested = str(report_market or "").strip().upper()
    targets = [requested] if requested else ["KOREA", "US", "COIN"]
    sent = []
    for target in targets:
        if target not in {"KOREA", "US", "COIN"}:
            raise ValueError("market must be KOREA, US, or COIN")
        now_local = datetime.now(NY if target == "US" else KST)
        png, caption = render_period_report(kind, target, now_local)
        _send_photo(chat_id, png, f"[관리자 테스트]\\n{caption}")
        sent.append(target)
    return {"ok": True, "kind": kind, "markets": sent, "destination_env": MEMBER_NOTICE_ENV}


def send_latest_cycle_test(
    market: str | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    """최근 종료 결과 1건을 해당 매도 그룹 채널로 즉시 테스트 발송."""
    requested_market = str(market or "").strip().upper()
    requested_symbol = str(symbol or "").strip().upper()
    candidates: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []

    market_order = (requested_market,) if requested_market else ("KOREA", "US", "COIN")
    for current_market in market_order:
        if current_market not in {"KOREA", "US", "COIN"}:
            continue
        data = group_analysis_market_data(current_market)
        for current_symbol, symbol_data in data.get("symbol_data", {}).items():
            if requested_symbol and current_symbol.upper() != requested_symbol:
                continue
            for position in symbol_data.get("positions", []):
                for result in position.get("exit_results") or []:
                    candidates.append((current_market, current_symbol, position, result))

    if not candidates:
        raise RuntimeError("조건에 맞는 완료 종료 결과가 없습니다")

    def sort_key(item):
        result = item[3]
        return str(result.get("exit_time") or "")

    current_market, current_symbol, position, result = max(candidates, key=sort_key)
    exit_group = str(result.get("exit_group") or "")
    env_name, chat_id = _exit_destination(current_market, exit_group)
    if not env_name:
        raise RuntimeError("해당 매도 그룹의 알람방 환경변수 매핑이 없습니다")
    if not chat_id:
        raise RuntimeError(f"{env_name} is not configured")

    png = render_exit_image(current_market, current_symbol, position, result)
    caption = (
        f"[관리자 테스트]\n📈 {current_symbol} · 매도 {GROUP_LABEL.get(exit_group)}\n"
        f"매수 {position.get('entry_timeframe','-')} · {_format_kst(position.get('entry_first_time'))}\n"
        f"매도 {result.get('exit_timeframe','-')} · {_format_kst(result.get('exit_time'))}\n"
        f"수익률 {float(result['return_pct']):+.3f}%"
    )
    _send_photo(chat_id, png, caption)
    return {
        "ok": True,
        "market": current_market,
        "symbol": current_symbol,
        "entry_group": position["entry_group"],
        "exit_group": exit_group,
        "exit_timeframe": result["exit_timeframe"],
        "destination_env": env_name,
    }
