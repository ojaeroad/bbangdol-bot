"""저점·고점 반복 알람 축소 B안 과거 데이터 시뮬레이터 v2.

핵심 계산 원칙
- 매수 LOW의 첫 신호는 '집중 알림'일 뿐 진입하지 않는다.
- 같은 LOW 상태의 두 번째 유효 신호부터 최대 3회 분할진입한다.
- ALL(현재): 첫 신호 이후 기존 공통 5분 쿨타임으로 최대 3회 진입.
- FULL(원 주기): 첫 신호 이후 원 시간봉의 다음 자연 경계부터 최대 3회 진입.
- HALF(운영 주기): 첫 신호 이후 운영 주기의 다음 자연 경계부터 최대 3회 진입.
- 매도 HIGH는 샘플링하지 않고 첫 유효 HIGH 신호에서 전량 종료한다.

이 계산으로 원 주기를 기다리는 동안 LOW 상태가 사라져 진입하지 못한 경우와,
운영 주기에서만 진입 기회를 확보한 경우를 구분할 수 있다.
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg

DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()
TF_MINUTES = {
    "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60,
    "2h": 120, "4h": 240, "6h": 360, "12h": 720,
    "1d": 1440, "1w": 10080,
}
HALF_MINUTES = {
    "3m": 3, "5m": 5, "15m": 5, "30m": 15, "1h": 30,
    "2h": 60, "4h": 120, "6h": 180, "12h": 360,
    "1d": 720, "1w": 5040,
}
GROUPS = {
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
GROUP_LABEL = {"SCALP": "단타", "SWING": "스윙", "LONG": "장기", "LIFE": "인생타점"}
EXIT_GROUPS = {
    "SCALP": {"SCALP", "SWING"},
    "SWING": {"SCALP", "SWING", "LONG"},
    "LONG": {"SWING", "LONG", "LIFE"},
    "LIFE": {"LONG", "LIFE"},
}
EPISODE_GAP_SECONDS = 125
CURRENT_ENTRY_COOLDOWN_SECONDS = 300
MAX_ENTRIES = 3


def _connect():
    if not DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")
    return psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        connect_timeout=8,
        application_name="cadence-simulator-v2",
    )


def _market(strategy: str, exchange: str | None) -> str:
    text = f"{strategy or ''} {exchange or ''}".upper()
    if strategy == "STARFLOWER":
        return "COIN"
    if any(x in text for x in ("KRX", "KOSPI", "KOSDAQ", "KOREA")):
        return "KOREA"
    return "US"


def _group(market: str, tf: str) -> str | None:
    for group, tfs in GROUPS.get(market, {}).items():
        if tf in tfs:
            return group
    return None


def _period_start(period_key: str) -> datetime | None:
    now = datetime.now(timezone.utc)
    if period_key == "today":
        return now - timedelta(days=1)
    if period_key == "7d":
        return now - timedelta(days=7)
    if period_key == "30d":
        return now - timedelta(days=30)
    return None


def _load(market_filter: str, period_key: str) -> list[dict[str, Any]]:
    start = _period_start(period_key)
    sql = """SELECT id,strategy,COALESCE(exchange,raw_exchange),symbol,signal_type,timeframe,
                    COALESCE(timeframe_minutes,0),signal_price,received_at
             FROM performance_signals
             WHERE signal_price IS NOT NULL
               AND signal_type IN ('LOW','HIGH')
               AND timeframe IS NOT NULL"""
    params: list[Any] = []
    if start:
        sql += " AND received_at >= %s"
        params.append(start)
    sql += " ORDER BY received_at,id"

    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        market = _market(row[1], row[2])
        tf = str(row[5]).lower()
        group = _group(market, tf)
        if market != market_filter or not group:
            continue
        minutes = int(row[6] or TF_MINUTES.get(tf, 0))
        if not minutes:
            continue
        result.append({
            "id": row[0], "market": market, "exchange": row[2],
            "symbol": row[3], "type": row[4], "tf": tf,
            "mins": minutes, "price": float(row[7]), "time": row[8],
            "group": group,
        })
    return result


def _slot(dt: datetime, minutes: int) -> int:
    return int(dt.timestamp() // (minutes * 60))


def _sample_alerts(signals: list[dict[str, Any]], mode: str) -> list[dict[str, Any]]:
    """실제 텔레그램에 표시될 알람 수를 계산한다.

    최초 LOW/HIGH는 즉시 포함하고, 같은 상태의 반복만 경계 주기로 줄인다.
    이 함수는 알람 수 계산용이며, 진입 계산은 _simulate_cycles에서 별도로 한다.
    """
    if mode == "ALL":
        return list(signals)

    sampled: list[dict[str, Any]] = []
    state: dict[tuple[str, str, str], dict[str, Any]] = {}
    for signal in signals:
        key = (signal["symbol"], signal["type"], signal["tf"])
        cadence = signal["mins"] if mode == "FULL" else HALF_MINUTES.get(
            signal["tf"], max(5, signal["mins"] // 2)
        )
        previous = state.get(key)
        new_episode = (
            previous is None
            or (signal["time"] - previous["last_time"]).total_seconds() > EPISODE_GAP_SECONDS
        )
        slot = _slot(signal["time"], cadence)
        if new_episode or slot != previous.get("sent_slot"):
            sampled.append(signal)
            sent_slot = slot
        else:
            sent_slot = previous.get("sent_slot")
        state[key] = {"last_time": signal["time"], "sent_slot": sent_slot}
    return sampled


def _entry_allowed(
    signal: dict[str, Any],
    episode: dict[str, Any],
    position: dict[str, Any] | None,
    mode: str,
) -> bool:
    """첫 집중 신호 이후 해당 방식에서 이 LOW를 실제 분할진입으로 쓸지 판정."""
    if episode["signal_count"] <= 1:
        return False

    if position and len(position["entries"]) >= MAX_ENTRIES:
        return False

    if mode == "ALL":
        last_entry_time = position["entries"][-1]["time"] if position else episode["focus_time"]
        return (signal["time"] - last_entry_time).total_seconds() >= CURRENT_ENTRY_COOLDOWN_SECONDS

    cadence = signal["mins"] if mode == "FULL" else HALF_MINUTES.get(
        signal["tf"], max(5, signal["mins"] // 2)
    )
    current_slot = _slot(signal["time"], cadence)
    # 최초 집중 신호가 속한 슬롯은 진입하지 않고, 다음 자연 경계부터 진입한다.
    if current_slot <= episode["focus_slot"]:
        return False
    if episode.get("last_entry_slot") == current_slot:
        return False
    episode["last_entry_slot"] = current_slot
    return True


def _simulate_cycles(signals: list[dict[str, Any]], mode: str) -> dict[str, Any]:
    """집중 알림과 실제 진입을 분리하여 완료사이클을 재계산한다."""
    episodes: dict[tuple[str, str, str], dict[str, Any]] = {}
    open_positions: dict[tuple[str, str, str], dict[str, Any]] = {}
    episode_records: dict[int, dict[str, Any]] = {}
    next_episode_id = 1
    cycles: list[dict[str, Any]] = []

    for signal in signals:
        if signal["type"] == "LOW":
            key = (signal["symbol"], signal["group"], signal["tf"])
            previous = episodes.get(key)
            new_episode = (
                previous is None
                or (signal["time"] - previous["last_time"]).total_seconds() > EPISODE_GAP_SECONDS
            )
            if new_episode:
                cadence = signal["mins"] if mode != "HALF" else HALF_MINUTES.get(
                    signal["tf"], max(5, signal["mins"] // 2)
                )
                episode = {
                    "id": next_episode_id,
                    "key": key,
                    "group": signal["group"],
                    "tf": signal["tf"],
                    "focus_time": signal["time"],
                    "focus_price": signal["price"],
                    "focus_slot": _slot(signal["time"], cadence),
                    "last_time": signal["time"],
                    "signal_count": 1,
                    "last_entry_slot": None,
                    "entered": False,
                }
                episodes[key] = episode
                episode_records[next_episode_id] = episode
                next_episode_id += 1
                continue

            episode = previous
            episode["last_time"] = signal["time"]
            episode["signal_count"] += 1
            position = open_positions.get(key)
            if not _entry_allowed(signal, episode, position, mode):
                continue

            if position is None:
                position = {
                    "symbol": signal["symbol"],
                    "group": signal["group"],
                    "entry_tf": signal["tf"],
                    "entries": [],
                    "episode_ids": set(),
                }
                open_positions[key] = position
            position["entries"].append(signal)
            position["episode_ids"].add(episode["id"])
            episode["entered"] = True
            continue

        # HIGH: 첫 유효 매도 신호에서 전량 종료. HIGH 자체는 원/운영 주기로 지연하지 않는다.
        if signal["type"] == "HIGH":
            for key, position in list(open_positions.items()):
                symbol, entry_group, _entry_tf = key
                if symbol != signal["symbol"]:
                    continue
                if signal["group"] not in EXIT_GROUPS.get(entry_group, set()):
                    continue
                if not position["entries"] or signal["time"] <= position["entries"][-1]["time"]:
                    continue

                avg_price = sum(item["price"] for item in position["entries"]) / len(position["entries"])
                return_pct = ((signal["price"] - avg_price) / avg_price * 100) if avg_price else 0.0
                cycles.append({
                    "symbol": symbol,
                    "group": entry_group,
                    "entry_tf": position["entry_tf"],
                    "exit_tf": signal["tf"],
                    "return_pct": return_pct,
                    "entries": len(position["entries"]),
                    "entry_price": avg_price,
                    "entry_time": position["entries"][0]["time"],
                    "exit_price": signal["price"],
                    "exit_time": signal["time"],
                })
                del open_positions[key]

    focus_count = len(episode_records)
    entered_focus_count = sum(1 for episode in episode_records.values() if episode["entered"])
    no_entry_focus_count = focus_count - entered_focus_count
    return {
        "cycles": cycles,
        "episodes": list(episode_records.values()),
        "focus_count": focus_count,
        "entered_focus_count": entered_focus_count,
        "no_entry_focus_count": no_entry_focus_count,
        "open_position_count": len(open_positions),
    }


def _stats(
    raw_count: int,
    sampled_count: int,
    simulation: dict[str, Any],
) -> dict[str, Any]:
    cycles = simulation["cycles"]
    values = [cycle["return_pct"] for cycle in cycles]
    entry_counts = [cycle["entries"] for cycle in cycles]
    focus_count = simulation["focus_count"]
    entered_focus_count = simulation["entered_focus_count"]
    return {
        "alert_count": sampled_count,
        "alert_reduction_pct": ((raw_count - sampled_count) / raw_count * 100) if raw_count else 0.0,
        "focus_count": focus_count,
        "entered_focus_count": entered_focus_count,
        "no_entry_focus_count": simulation["no_entry_focus_count"],
        "entry_capture_rate_pct": (entered_focus_count / focus_count * 100) if focus_count else None,
        "completed_cycles": len(values),
        "average_entries": (sum(entry_counts) / len(entry_counts)) if entry_counts else None,
        "one_entry_cycles": sum(1 for count in entry_counts if count == 1),
        "two_entry_cycles": sum(1 for count in entry_counts if count == 2),
        "three_entry_cycles": sum(1 for count in entry_counts if count >= 3),
        "average_return_pct": (sum(values) / len(values)) if values else None,
        "win_rate_pct": (sum(1 for value in values if value > 0) / len(values) * 100) if values else None,
        "best_return_pct": max(values) if values else None,
        "worst_return_pct": min(values) if values else None,
    }


def simulate_cadence(market: str, period_key: str = "all") -> dict[str, Any]:
    signals = _load(market, period_key)
    variants: list[dict[str, Any]] = []
    alert_samples: dict[str, list[dict[str, Any]]] = {}
    simulations: dict[str, dict[str, Any]] = {}

    labels = (
        ("ALL", "현재 방식"),
        ("FULL", "B안 · 원 시간봉 주기"),
        ("HALF", "B안 · 운영 주기"),
    )
    for code, label in labels:
        sampled = _sample_alerts(signals, code)
        alert_samples[code] = sampled
        simulation = _simulate_cycles(signals, code)
        simulations[code] = simulation
        variants.append({
            "code": code,
            "label": label,
            **_stats(len(signals), len(sampled), simulation),
        })

    timeframe_rows: list[dict[str, Any]] = []
    for tf in sorted({s["tf"] for s in signals}, key=lambda value: TF_MINUTES.get(value, 999999)):
        raw = [s for s in signals if s["tf"] == tf]
        full = _sample_alerts(raw, "FULL")
        half = _sample_alerts(raw, "HALF")
        timeframe_rows.append({
            "timeframe": tf,
            "raw_count": len(raw),
            "full_count": len(full),
            "half_count": len(half),
            "full_reduction_pct": ((len(raw) - len(full)) / len(raw) * 100) if raw else 0.0,
            "half_reduction_pct": ((len(raw) - len(half)) / len(raw) * 100) if raw else 0.0,
        })

    group_rows: list[dict[str, Any]] = []
    for group in GROUPS.get(market, {}):
        item = {"group": group, "group_label": GROUP_LABEL[group], "variants": []}
        raw_group_count = len([s for s in signals if s["group"] == group])
        for code, short_label in (("ALL", "현재"), ("FULL", "원 주기"), ("HALF", "운영 주기")):
            group_sample_count = len([s for s in alert_samples[code] if s["group"] == group])
            group_cycles = [c for c in simulations[code]["cycles"] if c["group"] == group]
            group_focus = [
                episode for episode in simulations[code]["episodes"]
                if episode["group"] == group
            ]
            # 그룹별 진입 통계는 같은 핵심 시뮬레이션의 완료 사이클을 사용한다.
            values = [c["return_pct"] for c in group_cycles]
            entry_counts = [c["entries"] for c in group_cycles]
            focus_count = len(group_focus)
            entered_focus_count = sum(1 for episode in group_focus if episode["entered"])
            item["variants"].append({
                "code": code,
                "label": short_label,
                "alert_count": group_sample_count,
                "alert_reduction_pct": ((raw_group_count - group_sample_count) / raw_group_count * 100) if raw_group_count else 0.0,
                "focus_count": focus_count,
                "entered_focus_count": entered_focus_count,
                "no_entry_focus_count": focus_count - entered_focus_count,
                "entry_capture_rate_pct": (entered_focus_count / focus_count * 100) if focus_count else None,
                "completed_cycles": len(values),
                "average_entries": (sum(entry_counts) / len(entry_counts)) if entry_counts else None,
                "one_entry_cycles": sum(1 for count in entry_counts if count == 1),
                "two_entry_cycles": sum(1 for count in entry_counts if count == 2),
                "three_entry_cycles": sum(1 for count in entry_counts if count >= 3),
                "average_return_pct": (sum(values) / len(values)) if values else None,
                "win_rate_pct": (sum(1 for value in values if value > 0) / len(values) * 100) if values else None,
                "best_return_pct": max(values) if values else None,
                "worst_return_pct": min(values) if values else None,
            })
        group_rows.append(item)

    return {
        "market": market,
        "period_key": period_key,
        "raw_signal_count": len(signals),
        "variants": variants,
        "timeframes": timeframe_rows,
        "groups": group_rows,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "note": (
            "매수 첫 LOW는 집중 알림으로만 사용하고, 두 번째 유효 LOW부터 최대 3회 분할진입했습니다. "
            "원 주기는 다음 원 시간봉 경계, 운영 주기는 다음 절반 시간봉 경계부터 진입하며, "
            "매도는 첫 유효 HIGH에서 전량 종료한 저장 신호 기반 가정 결과입니다."
        ),
    }

