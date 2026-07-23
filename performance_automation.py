
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
UTC = timezone.utc

POLL_SECONDS = max(30, int(os.getenv("PERFORMANCE_AUTOMATION_POLL_SECONDS", "60")))
AUTOMATION_ENABLED = os.getenv(
    "PERFORMANCE_AUTOMATION_ENABLED", "1"
).strip().lower() not in {"0", "false", "off", "no"}

MARKET_LABEL = {
    "KOREA": "국장",
    "US": "미장",
    "COIN": "코인",
}

# 기존 Render 환경변수 이름을 그대로 재사용한다.
ENTRY_CHAT_ENV = {
    ("COIN", "SCALP"): "BD_BUY_SHORT",
    ("COIN", "SWING"): "BD_BUY_SWING",
    ("COIN", "LONG"): "BD_BUY_LONG",
    ("COIN", "LIFE"): "BD_BUY_LIFE",
    ("KOREA", "SWING"): "BUY_SWING_1Q",
    ("KOREA", "LONG"): "BUY_LONG_1Q",
    ("KOREA", "LIFE"): "BUY_LIFE_1Q",
    ("US", "SWING"): "BUY_SWING_1Q",
    ("US", "LONG"): "BUY_LONG_1Q",
    ("US", "LIFE"): "BUY_LIFE_1Q",
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


def _font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
        if bold else
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc"
        if bold else
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold else
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size)
    return ImageFont.load_default()


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


def _price(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if abs(number) >= 1000:
        return f"{number:,.2f}".rstrip("0").rstrip(".")
    if abs(number) >= 1:
        return f"{number:.4f}".rstrip("0").rstrip(".")
    return f"{number:.8f}".rstrip("0").rstrip(".")


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


def render_exit_image(
    market: str,
    symbol: str,
    position: dict[str, Any],
    result: dict[str, Any],
) -> bytes:
    image, draw = _base_canvas(1430)
    white, blue, green, red, muted, gold = (
        "#f4f4f5", "#73cfff", "#54e39a", "#ff7f87", "#a5a6ad", "#ffc857"
    )
    draw.text((60, 50), "타점 수익률 결과", font=_font(48, True), fill=white)
    draw.text(
        (60, 120),
        f"{MARKET_LABEL.get(market, market)} · {GROUP_LABEL.get(position['entry_group'], position['entry_group'])}",
        font=_font(29, True),
        fill=blue,
    )
    draw.text((60, 175), symbol, font=_font(44, True), fill=white)

    _rounded(draw, (45, 255, 1035, 515))
    labels = [
        ("최초 진입", position["entry_timeframe"]),
        ("분할 진입", f"{position['entry_count']}회"),
        ("평균 진입가", _price(position["entry_price"])),
        ("청산 시간봉", result["exit_timeframe"]),
        ("청산가", _price(result["exit_price"])),
        ("보유기간", result.get("holding_text") or _duration(result.get("holding_minutes"))),
    ]
    for idx, (label, value) in enumerate(labels):
        x = 75 + (idx % 3) * 320
        y = 285 + (idx // 3) * 115
        draw.text((x, y), label, font=_font(21, True), fill=blue)
        draw.text((x, y + 38), str(value), font=_font(31, True), fill=white)

    return_pct = float(result.get("return_pct") or 0)
    adverse_pct = float(result.get("signal_adverse_pct") or 0)
    _rounded(draw, (45, 550, 1035, 765), outline=green if return_pct >= 0 else red)
    draw.text((75, 585), "실현 가능 수익률", font=_font(27, True), fill=blue)
    draw.text(
        (75, 635),
        f"{return_pct:+.3f}%",
        font=_font(70, True),
        fill=green if return_pct >= 0 else red,
    )
    draw.text((625, 590), "신호 기준 최대 역행", font=_font(23, True), fill=muted)
    draw.text((625, 635), f"{adverse_pct:+.3f}%", font=_font(40, True), fill=red)

    _rounded(draw, (45, 800, 1035, 1240))
    draw.text((75, 835), "실제 신호 가격 흐름", font=_font(30, True), fill=white)

    points = list(position.get("entry_points") or [])
    y = 915
    for idx, point in enumerate(points[:10], 1):
        draw.ellipse((85, y + 8, 105, y + 28), fill=gold)
        draw.text(
            (125, y),
            f"진입 {idx} · {point.get('timeframe')} · {_price(point.get('price'))}",
            font=_font(25, True),
            fill=white,
        )
        y += 60
    draw.text((95, y + 5), "⋯ 중간 캔들 생략 ⋯", font=_font(23), fill=muted)
    y += 70
    draw.ellipse((85, y + 8, 105, y + 28), fill=green)
    draw.text(
        (125, y),
        f"청산 · {result['exit_timeframe']} · {_price(result['exit_price'])}",
        font=_font(27, True),
        fill=green,
    )

    draw.text(
        (60, 1305),
        "※ TradingView 신호 가격 기반 통계이며 수수료·슬리피지는 포함하지 않습니다.",
        font=_font(20),
        fill=muted,
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
    height = max(1450, 780 + len(results) * 105)
    image, draw = _base_canvas(height)
    white, blue, green, red, muted, gold = (
        "#f4f4f5", "#73cfff", "#54e39a", "#ff7f87", "#a5a6ad", "#ffc857"
    )

    draw.text((60, 48), "완료 사이클 종합", font=_font(48, True), fill=gold)
    draw.text(
        (60, 115),
        f"{MARKET_LABEL.get(market, market)} · {GROUP_LABEL.get(position['entry_group'], position['entry_group'])}",
        font=_font(29, True),
        fill=blue,
    )
    draw.text((60, 170), symbol, font=_font(44, True), fill=white)

    _rounded(draw, (45, 250, 1035, 475))
    draw.text((75, 285), "최초 진입 시간봉", font=_font(22, True), fill=blue)
    draw.text((75, 330), position["entry_timeframe"], font=_font(35, True), fill=white)
    draw.text((380, 285), "분할 진입", font=_font(22, True), fill=blue)
    draw.text((380, 330), f"{position['entry_count']}회", font=_font(35, True), fill=white)
    draw.text((680, 285), "평균 진입가", font=_font(22, True), fill=blue)
    draw.text((680, 330), _price(position["entry_price"]), font=_font(35, True), fill=white)

    draw.text((60, 530), "시간봉별 청산 결과", font=_font(32, True), fill=white)
    y = 600
    returns = []
    for result in results:
        value = float(result.get("return_pct") or 0)
        returns.append(value)
        _rounded(draw, (55, y, 1025, y + 82), fill="#15161a")
        draw.text((85, y + 22), result["exit_timeframe"], font=_font(25, True), fill=blue)
        draw.text((230, y + 22), _price(result["exit_price"]), font=_font(24, True), fill=white)
        draw.text(
            (520, y + 18),
            f"{value:+.3f}%",
            font=_font(31, True),
            fill=green if value >= 0 else red,
        )
        draw.text(
            (745, y + 22),
            result.get("holding_text") or _duration(result.get("holding_minutes")),
            font=_font(22),
            fill=muted,
        )
        y += 98

    if returns:
        draw.text((65, y + 25), "청산 평균", font=_font(24, True), fill=blue)
        draw.text((250, y + 20), f"{sum(returns)/len(returns):+.3f}%", font=_font(34, True), fill=green)
        draw.text((550, y + 25), "최고 수익", font=_font(24, True), fill=blue)
        draw.text((735, y + 20), f"{max(returns):+.3f}%", font=_font(34, True), fill=green)

    draw.text(
        (60, height - 75),
        "※ 각 청산 시간봉의 첫 유효 고점 결과를 한 장으로 합산했습니다.",
        font=_font(20),
        fill=muted,
    )
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


def _entry_destination(market: str, entry_group: str) -> tuple[str, str]:
    env_name = ENTRY_CHAT_ENV.get((market, entry_group), "")
    return env_name, os.getenv(env_name, "").strip() if env_name else ""


def process_new_cycle_deliveries() -> None:
    for market in ("KOREA", "US", "COIN"):
        market_data = group_analysis_market_data(market)
        for symbol, symbol_data in market_data.get("symbol_data", {}).items():
            for position in symbol_data.get("positions", []):
                env_name, chat_id = _entry_destination(market, position["entry_group"])
                if not env_name or not chat_id:
                    continue

                position_key = _position_key(market, symbol, position)
                results = position.get("exit_results") or []

                # 청산 시간봉별 이미지
                for result in results:
                    delivery_key = (
                        f"exit:{position_key}:{result.get('exit_signal_id')}:{result['exit_timeframe']}"
                    )
                    if not _claim(delivery_key, "EXIT_IMAGE", market, symbol, env_name):
                        continue
                    try:
                        png = render_exit_image(market, symbol, position, result)
                        caption = (
                            f"📈 {symbol} {GROUP_LABEL.get(position['entry_group'])} "
                            f"{result['exit_timeframe']} 청산\n"
                            f"수익률 {float(result['return_pct']):+.3f}% · "
                            f"보유 {result.get('holding_text') or _duration(result.get('holding_minutes'))}"
                        )
                        _send_photo(chat_id, png, caption)
                        log.info(
                            "exit result sent market=%s symbol=%s group=%s exit_tf=%s env=%s",
                            market, symbol, position["entry_group"],
                            result["exit_timeframe"], env_name,
                        )
                    except Exception:
                        _release(delivery_key)
                        log.exception("exit result delivery failed key=%s", delivery_key)

                # 허용된 청산 시간봉이 전부 수집되면 종합 이미지
                expected = set(_expected_exit_timeframes(market, position["entry_group"]))
                completed = {row["exit_timeframe"] for row in results}
                if expected and expected.issubset(completed):
                    summary_key = f"cycle-summary:{position_key}"
                    if _claim(summary_key, "CYCLE_SUMMARY", market, symbol, env_name):
                        try:
                            png = render_cycle_summary_image(market, symbol, position)
                            values = [float(row["return_pct"]) for row in results]
                            caption = (
                                f"✅ {symbol} {GROUP_LABEL.get(position['entry_group'])} "
                                f"완료 사이클 종합\n"
                                f"청산 {len(results)}개 · 평균 {sum(values)/len(values):+.3f}% · "
                                f"최고 {max(values):+.3f}%"
                            )
                            _send_photo(chat_id, png, caption)
                            log.info(
                                "cycle summary sent market=%s symbol=%s group=%s env=%s",
                                market, symbol, position["entry_group"], env_name,
                            )
                        except Exception:
                            _release(summary_key)
                            log.exception("cycle summary delivery failed key=%s", summary_key)


def _period_bounds(kind: str, now_ny: datetime) -> tuple[datetime, datetime, str]:
    if kind == "weekly":
        end_ny = now_ny
        start_ny = end_ny - timedelta(days=7)
        label = f"{start_ny:%Y.%m.%d} ~ {end_ny:%Y.%m.%d}"
    else:
        start_ny = now_ny.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start_ny.month == 12:
            next_month = start_ny.replace(year=start_ny.year + 1, month=1)
        else:
            next_month = start_ny.replace(month=start_ny.month + 1)
        end_ny = min(now_ny, next_month)
        label = f"{start_ny:%Y년 %m월}"
    return start_ny.astimezone(UTC), end_ny.astimezone(UTC), label


def _collect_period(kind: str, now_ny: datetime):
    start_utc, end_utc, label = _period_bounds(kind, now_ny)
    markets = {}
    all_rows = []
    for market in ("KOREA", "US", "COIN"):
        data = group_analysis_market_data(market)
        rows = []
        symbols = set()
        for symbol, symbol_data in data.get("symbol_data", {}).items():
            for position in symbol_data.get("positions", []):
                for result in position.get("exit_results") or []:
                    try:
                        exit_time = datetime.fromisoformat(result["exit_time"])
                    except Exception:
                        continue
                    if start_utc <= exit_time <= end_utc:
                        row = {
                            "market": market,
                            "symbol": symbol,
                            "entry_group": position["entry_group"],
                            "entry_timeframe": position["entry_timeframe"],
                            **result,
                        }
                        rows.append(row)
                        all_rows.append(row)
                        symbols.add(symbol)
        values = [float(row["return_pct"]) for row in rows]
        markets[market] = {
            "rows": rows,
            "count": len(rows),
            "symbol_count": len(symbols),
            "average": sum(values) / len(values) if values else None,
            "best": max(values) if values else None,
            "win_rate": (
                sum(1 for value in values if value > 0) / len(values) * 100
                if values else None
            ),
            "average_holding": (
                sum(float(row.get("holding_minutes") or 0) for row in rows) / len(rows)
                if rows else None
            ),
        }
    all_rows.sort(key=lambda row: float(row["return_pct"]), reverse=True)
    return markets, all_rows, label


def render_period_report(kind: str, now_ny: datetime) -> tuple[bytes, str]:
    markets, all_rows, label = _collect_period(kind, now_ny)
    title = "주간 성과 리포트" if kind == "weekly" else "월간 성과 리포트"
    image, draw = _base_canvas(1770)
    white, blue, green, red, muted, gold = (
        "#f4f4f5", "#73cfff", "#54e39a", "#ff7f87", "#a5a6ad", "#ffc857"
    )
    draw.text((55, 45), title, font=_font(50, True), fill=white)
    draw.text((55, 115), label, font=_font(27, True), fill=blue)
    draw.text((55, 165), "국장 · 미장 · 코인 현황 집계", font=_font(25), fill=muted)

    y = 245
    for market in ("KOREA", "US", "COIN"):
        stat = markets[market]
        _rounded(draw, (45, y, 1035, y + 230))
        draw.text((75, y + 28), MARKET_LABEL[market], font=_font(34, True), fill=blue)
        if stat["average"] is None:
            draw.text((75, y + 95), "기간 내 완료 결과 없음", font=_font(28), fill=muted)
        else:
            draw.text(
                (75, y + 90),
                f"평균 {stat['average']:+.2f}%",
                font=_font(39, True),
                fill=green if stat["average"] >= 0 else red,
            )
            draw.text(
                (430, y + 90),
                f"최고 {stat['best']:+.2f}%",
                font=_font(32, True),
                fill=green,
            )
            draw.text(
                (75, y + 155),
                f"승률 {stat['win_rate']:.1f}% · 결과 {stat['count']}건 · 종목 {stat['symbol_count']}개",
                font=_font(24),
                fill=white,
            )
            draw.text(
                (670, y + 155),
                f"평균보유 {_duration(stat['average_holding'])}",
                font=_font(21),
                fill=muted,
            )
        y += 255

    draw.text((55, y + 10), "TOP 5", font=_font(35, True), fill=gold)
    y += 70
    for rank, row in enumerate(all_rows[:5], 1):
        _rounded(draw, (50, y, 1030, y + 100), fill="#15161a")
        draw.text((75, y + 25), f"{rank}", font=_font(30, True), fill=gold)
        draw.text((135, y + 22), row["symbol"], font=_font(28, True), fill=white)
        draw.text(
            (500, y + 24),
            f"{GROUP_LABEL.get(row['entry_group'])} {row['entry_timeframe']} → {row['exit_timeframe']}",
            font=_font(21),
            fill=blue,
        )
        value = float(row["return_pct"])
        draw.text(
            (845, y + 18),
            f"{value:+.2f}%",
            font=_font(31, True),
            fill=green if value >= 0 else red,
        )
        y += 112

    if not all_rows:
        draw.text((75, y + 20), "기간 내 완료된 청산 결과가 없습니다.", font=_font(27), fill=muted)

    draw.text(
        (55, 1690),
        "※ 신호 가격 기준이며 수수료·슬리피지·세금은 포함하지 않습니다.",
        font=_font(20),
        fill=muted,
    )
    caption = f"📊 {title} · {label}\n국장·미장·코인 완료 결과 현황"
    return _png_bytes(image), caption


def _is_last_weekday_of_month(now_ny: datetime) -> bool:
    """주말만 보정한 마지막 평일. 미국 휴장일 조기판정은 하지 않는다."""
    tomorrow = (now_ny + timedelta(days=1)).date()
    cursor = tomorrow
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor.month != now_ny.month


def process_scheduled_reports() -> None:
    chat_id = os.getenv(MEMBER_NOTICE_ENV, "").strip()
    if not chat_id:
        return

    now_ny = datetime.now(NY)

    # 금요일 미장 정규 종료 1시간 뒤: 17시부터 17시 59분 사이 한 번.
    if now_ny.weekday() == 4 and now_ny.hour == 17:
        key = f"weekly:{now_ny:%Y-%m-%d}"
        if _claim(key, "WEEKLY_REPORT", None, None, MEMBER_NOTICE_ENV):
            try:
                png, caption = render_period_report("weekly", now_ny)
                _send_photo(chat_id, png, caption)
                log.info("weekly report sent key=%s", key)
            except Exception:
                _release(key)
                log.exception("weekly report failed key=%s", key)

    # 달의 마지막 평일 미장 종료 1시간 뒤.
    if now_ny.hour == 17 and _is_last_weekday_of_month(now_ny):
        key = f"monthly:{now_ny:%Y-%m}"
        if _claim(key, "MONTHLY_REPORT", None, None, MEMBER_NOTICE_ENV):
            try:
                png, caption = render_period_report("monthly", now_ny)
                _send_photo(chat_id, png, caption)
                log.info("monthly report sent key=%s", key)
            except Exception:
                _release(key)
                log.exception("monthly report failed key=%s", key)


def run_once() -> None:
    if not AUTOMATION_ENABLED:
        return
    if not DATABASE_URL or not BOT_TOKEN:
        log.warning(
            "performance automation skipped database=%s bot_token=%s",
            bool(DATABASE_URL), bool(BOT_TOKEN),
        )
        return
    process_new_cycle_deliveries()
    process_scheduled_reports()


def _loop() -> None:
    time.sleep(15)
    while True:
        try:
            run_once()
        except Exception:
            log.exception("performance automation loop failed")
        time.sleep(POLL_SECONDS)


_LOCK = threading.Lock()
_STARTED = False


def start_performance_automation() -> bool:
    global _STARTED
    if not AUTOMATION_ENABLED:
        log.info("performance automation disabled")
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
