
"""회원 성과 분석용 시간봉 승계 포지션 엔진 v14.

핵심 규칙
=========
1. 각 LOW 시간봉은 자신의 최초진입 시간봉 포지션을 만든다.
2. 이미 열린 포지션은 최초진입 시간봉과 같거나 더 높은 LOW를
   추가 분할진입으로 받을 수 있다.
3. 각 포지션의 유효 진입 간격은 오직 5분이다.
   - 신호 원본 시간봉의 길이는 진입 쿨타임에 사용하지 않는다.
   - 다른 시간봉이라도 해당 포지션의 마지막 진입 후 5분 미만이면
     그 포지션에는 추가 진입하지 않는다.
4. 포지션별 최대 분할진입은 기본 3회다.
5. 시간봉 자체 길이 쿨타임은 발생 주기 통계에만 적용한다.
6. 24시간 제한은 사용하지 않는다.
7. 각 매수 최초시간봉에 허용된 매도 그룹의 모든 시간봉을 각각 비교하고,
   마지막 진입 뒤 해당 매도 시간봉의 첫 HIGH에서 전량청산한다.
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg

DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()

TF_MINUTES = {
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "12h": 720,
    "1d": 1440,
    "1w": 10080,
}

GROUP_RANK = {"SCALP": 0, "SWING": 1, "LONG": 2, "LIFE": 3}
GROUP_LABEL = {
    "SCALP": "단타",
    "SWING": "스윙",
    "LONG": "장기",
    "LIFE": "인생타점",
}

MARKET_GROUPS = {
    "COIN": {
        "SCALP": ["5m", "15m"],
        "SWING": ["30m", "1h"],
        "LONG": ["4h", "6h"],
        "LIFE": ["12h", "1d", "1w"],
    },
    "KOREA": {
        "SWING": ["30m", "1h"],
        "LONG": ["4h", "6h"],
        "LIFE": ["1d", "1w"],
    },
    "US": {
        "SWING": ["30m", "1h"],
        "LONG": ["4h", "6h"],
        "LIFE": ["1d", "1w"],
    },
}

EXIT_GROUPS = {
    "SCALP": ["SCALP", "SWING"],
    "SWING": ["SCALP", "SWING", "LONG"],
    "LONG": ["SWING", "LONG", "LIFE"],
    "LIFE": ["LONG", "LIFE"],
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS performance_analysis_settings (
    setting_key VARCHAR(80) PRIMARY KEY,
    setting_value VARCHAR(200) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO performance_analysis_settings(setting_key, setting_value)
VALUES ('recent_interval_count', '5')
ON CONFLICT (setting_key) DO NOTHING;

INSERT INTO performance_analysis_settings(setting_key, setting_value)
VALUES ('entry_split_limit', '3')
ON CONFLICT (setting_key) DO NOTHING;

INSERT INTO performance_analysis_settings(setting_key, setting_value)
VALUES ('entry_cooldown_minutes', '5')
ON CONFLICT (setting_key) DO NOTHING;
"""


def _connect():
    if not DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")
    return psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        connect_timeout=8,
        application_name="bbangdol-group-analysis-v14",
    )


def ensure_schema() -> None:
    with _connect() as conn:
        conn.execute(SCHEMA_SQL)


def get_settings() -> dict[str, int]:
    ensure_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT setting_key, setting_value FROM performance_analysis_settings"
        ).fetchall()
    raw = {row[0]: row[1] for row in rows}
    return {
        "recent_interval_count": max(
            1, min(int(raw.get("recent_interval_count", 5)), 100)
        ),
        "entry_split_limit": max(
            1, min(int(raw.get("entry_split_limit", 3)), 10)
        ),
        "entry_cooldown_minutes": max(
            1, min(int(raw.get("entry_cooldown_minutes", 5)), 1440)
        ),
    }


def update_settings(
    recent_interval_count: int | None = None,
    entry_split_limit: int | None = None,
) -> dict[str, int]:
    ensure_schema()
    updates = []
    if recent_interval_count is not None:
        updates.append((
            "recent_interval_count",
            str(max(1, min(int(recent_interval_count), 100))),
        ))
    if entry_split_limit is not None:
        updates.append((
            "entry_split_limit",
            str(max(1, min(int(entry_split_limit), 10))),
        ))

    if updates:
        with _connect() as conn:
            for setting_key, setting_value in updates:
                conn.execute(
                    """
                    INSERT INTO performance_analysis_settings(
                        setting_key, setting_value, updated_at
                    )
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (setting_key)
                    DO UPDATE SET
                        setting_value=EXCLUDED.setting_value,
                        updated_at=NOW()
                    """,
                    (setting_key, setting_value),
                )
    return get_settings()


def _market(strategy: str, exchange: str | None) -> str:
    text = f"{strategy or ''} {exchange or ''}".upper()
    if strategy == "STARFLOWER":
        return "COIN"
    if any(token in text for token in ("KRX", "KOSPI", "KOSDAQ", "KOREA")):
        return "KOREA"
    return "US"


def _group_for_tf(market: str, timeframe: str | None) -> str | None:
    if not timeframe:
        return None
    for group, timeframes in MARKET_GROUPS.get(market, {}).items():
        if timeframe in timeframes:
            return group
    return None


def _exit_timeframes(market: str, entry_group: str) -> list[str]:
    result: list[str] = []
    market_groups = MARKET_GROUPS.get(market, {})
    for exit_group in EXIT_GROUPS.get(entry_group, []):
        result.extend(market_groups.get(exit_group, []))
    return result


def _load_signals() -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                strategy,
                COALESCE(exchange, raw_exchange),
                symbol,
                signal_type,
                timeframe,
                timeframe_minutes,
                signal_price,
                received_at
            FROM performance_signals
            WHERE signal_price IS NOT NULL
              AND signal_type IN ('LOW', 'HIGH')
              AND timeframe IS NOT NULL
            ORDER BY received_at, id
            """
        ).fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        market = _market(row[1], row[2])
        timeframe = row[5]
        group = _group_for_tf(market, timeframe)
        minutes = row[6] or TF_MINUTES.get(timeframe)
        if not group or not minutes:
            continue
        output.append(
            {
                "id": row[0],
                "strategy": row[1],
                "exchange": row[2],
                "market": market,
                "symbol": row[3],
                "signal_type": row[4],
                "timeframe": timeframe,
                "minutes": int(minutes),
                "price": Decimal(row[7]),
                "time": row[8],
                "group": group,
            }
        )
    return output


def _format_duration(minutes: float | int | None) -> str:
    if minutes is None:
        return "-"
    total = max(0, int(round(float(minutes))))
    days, remainder = divmod(total, 1440)
    hours, mins = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}일")
    if hours:
        parts.append(f"{hours}시간")
    if mins or not parts:
        parts.append(f"{mins}분")
    return " ".join(parts)


def _average(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _occurrence_valid_lows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """발생 주기 통계 전용 필터.

    같은 종목·LOW·시간봉에서 직전 인정 시각으로부터
    해당 시간봉 길이 이상 지났을 때만 새 발생으로 인정한다.
    """
    last_by_timeframe: dict[tuple, datetime] = {}
    valid: list[dict[str, Any]] = []

    for row in sorted(rows, key=lambda item: (item["time"], item["id"])):
        if row["signal_type"] != "LOW":
            continue
        key = (row["market"], row["symbol"], row["timeframe"])
        previous = last_by_timeframe.get(key)
        timeframe_cooldown = timedelta(minutes=row["minutes"])
        if previous is not None and row["time"] - previous < timeframe_cooldown:
            continue
        valid.append(row)
        last_by_timeframe[key] = row["time"]

    return valid


def _occurrence_stats(
    rows: list[dict[str, Any]],
    recent_n: int,
    now: datetime,
) -> list[dict[str, Any]]:
    valid_lows = _occurrence_valid_lows(rows)
    grouped: dict[tuple, list[datetime]] = defaultdict(list)

    for row in valid_lows:
        grouped[(row["group"], row["timeframe"])].append(row["time"])

    result: list[dict[str, Any]] = []
    for (group, timeframe), times in grouped.items():
        times = sorted(times)
        intervals = [
            (times[index] - times[index - 1]).total_seconds() / 60
            for index in range(1, len(times))
        ]
        recent_intervals = intervals[-recent_n:]
        elapsed = (
            (now - times[-1]).total_seconds() / 60 if times else None
        )
        overall_average = _average(intervals)
        recent_average = _average(recent_intervals)

        result.append(
            {
                "group": group,
                "group_label": GROUP_LABEL[group],
                "timeframe": timeframe,
                "timeframe_minutes": TF_MINUTES.get(timeframe, 999999),
                "occurrence_count": len(times),
                "interval_count": len(intervals),
                "overall_average_minutes": overall_average,
                "recent_average_minutes": recent_average,
                "recent_n": recent_n,
                "minimum_minutes": min(intervals) if intervals else None,
                "maximum_minutes": max(intervals) if intervals else None,
                "last_at": times[-1].isoformat() if times else None,
                "elapsed_since_last_minutes": elapsed,
                "overall_average_text": _format_duration(overall_average),
                "recent_average_text": _format_duration(recent_average),
                "minimum_text": _format_duration(
                    min(intervals) if intervals else None
                ),
                "maximum_text": _format_duration(
                    max(intervals) if intervals else None
                ),
                "elapsed_text": _format_duration(elapsed),
            }
        )

    result.sort(
        key=lambda item: (
            GROUP_RANK[item["group"]],
            item["timeframe_minutes"],
        )
    )
    return result


def _new_position(signal: dict[str, Any], sequence: int) -> dict[str, Any]:
    return {
        "position_sequence": sequence,
        "market": signal["market"],
        "symbol": signal["symbol"],
        "entry_group": signal["group"],
        "entry_group_label": GROUP_LABEL[signal["group"]],
        "entry_timeframe": signal["timeframe"],
        "entry_timeframe_minutes": signal["minutes"],
        "entries": [],
        "entry_signal_ids": [],
        "entry_source_timeframes": [],
        "entry_first_time_raw": signal["time"],
        "entry_last_time_raw": None,
        "is_full": False,
    }


def _can_position_accept(
    position: dict[str, Any],
    signal: dict[str, Any],
    split_limit: int,
    cooldown: timedelta,
) -> bool:
    if position["is_full"]:
        return False
    if signal["minutes"] < position["entry_timeframe_minutes"]:
        return False
    if not position["entries"]:
        return True
    return signal["time"] - position["entry_last_time_raw"] >= cooldown


def _append_entry(
    position: dict[str, Any],
    signal: dict[str, Any],
    split_limit: int,
) -> None:
    position["entries"].append(signal)
    position["entry_signal_ids"].append(signal["id"])
    position["entry_source_timeframes"].append(signal["timeframe"])
    position["entry_last_time_raw"] = signal["time"]
    position["is_full"] = len(position["entries"]) >= split_limit


def _build_entry_positions(
    lows: list[dict[str, Any]],
    settings: dict[str, int],
) -> list[dict[str, Any]]:
    """최초시간봉 포지션을 만들고 상위시간봉 신호를 승계한다.

    처리 순서:
    - 한 LOW 신호는 먼저 기존 미완성 포지션들을 채운다.
    - 그 LOW의 자체 최초시간봉 포지션이 없거나 기존 자체 포지션이
      이미 3회 완료된 경우, 같은 신호로 새 자체 포지션도 시작한다.
    - 같은 신호가 기존 하위 포지션과 자신의 신규 포지션에 동시에
      쓰이는 것은 각 전략 경우의 수를 비교하는 가상 포지션이므로 허용한다.
    """
    split_limit = settings["entry_split_limit"]
    cooldown = timedelta(minutes=settings["entry_cooldown_minutes"])
    positions: list[dict[str, Any]] = []
    latest_open_by_base_tf: dict[str, dict[str, Any]] = {}
    sequence = 0

    for signal in sorted(lows, key=lambda item: (item["time"], item["id"])):
        # 같은 원본 시간봉의 기존 포지션이 이 신호 이전에 이미 완료됐는지 기록한다.
        # 이번 신호로 3회째를 채운 직후 같은 신호로 새 포지션까지 만드는 것은 금지한다.
        own_before = latest_open_by_base_tf.get(signal["timeframe"])
        own_was_full_before_signal = (
            own_before is not None and own_before["is_full"]
        )

        # 기존의 모든 미완성 포지션에 대해, 최초시간봉 이상이면 승계 가능.
        for position in positions:
            if _can_position_accept(
                position,
                signal,
                split_limit,
                cooldown,
            ):
                _append_entry(position, signal, split_limit)

        # 자체 시간봉 포지션이 전혀 없거나, 이번 신호가 오기 전부터 이미 완료된 경우에만
        # 이 신호로 새로운 자체 포지션을 시작한다.
        if own_before is None or own_was_full_before_signal:
            sequence += 1
            own = _new_position(signal, sequence)
            _append_entry(own, signal, split_limit)
            positions.append(own)
            latest_open_by_base_tf[signal["timeframe"]] = own
        # 기존 자체 포지션이 미완성이었다면 위 반복에서 이미 신호를 받았거나,
        # 5분 미충족으로 받지 못했으므로 중복 추가하지 않는다.

    return positions


def _attach_exit_results(
    positions: list[dict[str, Any]],
    highs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    highs_by_tf: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for high in sorted(highs, key=lambda item: (item["time"], item["id"])):
        highs_by_tf[high["timeframe"]].append(high)

    output: list[dict[str, Any]] = []
    for position in positions:
        entries = position["entries"]
        if not entries:
            continue

        average_price = (
            sum((entry["price"] for entry in entries), Decimal("0"))
            / Decimal(len(entries))
        )
        first_time = entries[0]["time"]
        last_time = entries[-1]["time"]
        exit_results: list[dict[str, Any]] = []

        for exit_timeframe in _exit_timeframes(
            position["market"],
            position["entry_group"],
        ):
            candidates = [
                high
                for high in highs_by_tf.get(exit_timeframe, [])
                if high["time"] > last_time
            ]
            if not candidates:
                continue

            exit_signal = candidates[0]
            holding_minutes = int(
                (exit_signal["time"] - last_time).total_seconds() / 60
            )
            return_pct = float(
                (exit_signal["price"] - average_price)
                / average_price
                * Decimal("100")
            )
            exit_group = exit_signal["group"]

            exit_results.append(
                {
                    "exit_group": exit_group,
                    "exit_group_label": GROUP_LABEL[exit_group],
                    "exit_timeframe": exit_timeframe,
                    "exit_timeframe_minutes": exit_signal["minutes"],
                    "exit_time": exit_signal["time"].isoformat(),
                    "exit_price": float(exit_signal["price"]),
                    "holding_minutes": holding_minutes,
                    "holding_text": _format_duration(holding_minutes),
                    "return_pct": return_pct,
                    "exit_signal_id": exit_signal["id"],
                }
            )

        source_counts: dict[str, int] = defaultdict(int)
        for timeframe in position["entry_source_timeframes"]:
            source_counts[timeframe] += 1

        output.append(
            {
                "position_sequence": position["position_sequence"],
                "entry_group": position["entry_group"],
                "entry_group_label": position["entry_group_label"],
                "entry_timeframe": position["entry_timeframe"],
                "entry_timeframe_minutes": position["entry_timeframe_minutes"],
                "entry_count": len(entries),
                "entry_split_limit": len(entries) if position["is_full"] else None,
                "entry_complete": position["is_full"],
                "entry_first_time": first_time.isoformat(),
                "entry_last_time": last_time.isoformat(),
                "entry_price": float(average_price),
                "entry_signal_ids": position["entry_signal_ids"],
                "entry_source_timeframes": position["entry_source_timeframes"],
                "entry_source_summary": " + ".join(
                    f"{timeframe} {count}회"
                    for timeframe, count in sorted(
                        source_counts.items(),
                        key=lambda item: TF_MINUTES.get(item[0], 999999),
                    )
                ),
                "exit_results": exit_results,
            }
        )

    return output


def _build_positions(
    lows: list[dict[str, Any]],
    highs: list[dict[str, Any]],
    settings: dict[str, int],
) -> list[dict[str, Any]]:
    entry_positions = _build_entry_positions(lows, settings)
    return _attach_exit_results(entry_positions, highs)


def _performance_summary(
    positions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """최초 매수 시간봉 × 실제 매도 시간봉별 누적 성과."""
    buckets: dict[tuple, list[dict[str, Any]]] = defaultdict(list)

    for position in positions:
        for result in position["exit_results"]:
            key = (
                position["entry_group"],
                position["entry_timeframe"],
                result["exit_group"],
                result["exit_timeframe"],
            )
            buckets[key].append(result)

    output: list[dict[str, Any]] = []
    for (
        entry_group,
        entry_timeframe,
        exit_group,
        exit_timeframe,
    ), rows in buckets.items():
        returns = [row["return_pct"] for row in rows]
        holdings = [row["holding_minutes"] for row in rows]

        output.append(
            {
                "entry_group": entry_group,
                "entry_group_label": GROUP_LABEL[entry_group],
                "entry_timeframe": entry_timeframe,
                "entry_timeframe_minutes": TF_MINUTES[entry_timeframe],
                "exit_group": exit_group,
                "exit_group_label": GROUP_LABEL[exit_group],
                "exit_timeframe": exit_timeframe,
                "exit_timeframe_minutes": TF_MINUTES[exit_timeframe],
                "trade_count": len(rows),
                "average_return_pct": _average(returns),
                "best_return_pct": max(returns),
                "worst_return_pct": min(returns),
                "win_rate_pct": (
                    len([value for value in returns if value > 0])
                    / len(returns)
                    * 100
                ),
                "average_holding_minutes": _average(holdings),
                "average_holding_text": _format_duration(
                    _average(holdings)
                ),
            }
        )

    output.sort(
        key=lambda item: (
            GROUP_RANK[item["entry_group"]],
            item["entry_timeframe_minutes"],
            GROUP_RANK[item["exit_group"]],
            item["exit_timeframe_minutes"],
        )
    )
    return output


def group_analysis_data(
    market: str | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    all_signals = _load_signals()
    now = datetime.now(timezone.utc)

    available_markets = ["KOREA", "US", "COIN"]
    selected_market = (
        market if market in available_markets else "KOREA"
    )
    market_rows = [
        row for row in all_signals if row["market"] == selected_market
    ]

    symbols = sorted({row["symbol"] for row in market_rows})
    selected_symbol = (
        symbol if symbol in symbols else (symbols[0] if symbols else None)
    )
    symbol_rows = [
        row for row in market_rows if row["symbol"] == selected_symbol
    ]

    lows = [
        row for row in symbol_rows if row["signal_type"] == "LOW"
    ]
    highs = [
        row for row in symbol_rows if row["signal_type"] == "HIGH"
    ]
    positions = _build_positions(lows, highs, settings)

    return {
        "settings": settings,
        "market": selected_market,
        "markets": available_markets,
        "symbols": symbols,
        "symbol": selected_symbol,
        "raw_signal_count": len(symbol_rows),
        "positions": positions,
        "performance_summary": _performance_summary(positions),
        "occurrence_stats": _occurrence_stats(
            symbol_rows,
            settings["recent_interval_count"],
            now,
        ),
        "group_labels": GROUP_LABEL,
    }
