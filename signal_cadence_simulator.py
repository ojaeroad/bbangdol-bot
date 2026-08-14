# V97_SYMBOL_ENTRYPLAN_AND_EMPTY_TF: 종목별 5가지 매수방식 + 미완료 시간봉 조합 빈칸 표시
# V96_ENTRY_PLAN_RESEARCH: 집중 포함/스킵 × 3/5분할 + 집중 단독 연구
# V95_RESEARCH_CADENCE: 3분 쿨타임 + 정시5분 연구 + MAE/MFE 성과분석
# V94_ADMIN_STAGE_RESEARCH: 운영 진입 3회 유지 + 관리자 유효1~5 단계 연구
# V93_FIVE_MIN_SIMULATOR: 5분 유효 쿨타임 + 시간봉별 집중 리셋 분석 포함
"""저점·고점 반복 알람 축소 B안 과거 데이터 시뮬레이터 v2.

핵심 계산 원칙
- 매수 LOW의 첫 신호는 '집중 알림'일 뿐 진입하지 않는다.
- 같은 LOW 상태의 두 번째 유효 신호부터 최대 3회 분할진입한다.
- ALL(1분 원본): 첫 신호 이후 기존 공통 5분 쿨타임으로 최대 3회 진입.
- FIVE(5분 운영): 유효 알람은 마지막 전송 후 5분, 집중 리셋은 5m/15m=15분, 30m=30분, 1h 이상=60분.
- FULL(자기 시간봉): 첫 신호 이후 원 시간봉의 다음 자연 경계부터 최대 3회 진입.
- HALF(절반 주기): 첫 신호 이후 기존 절반 주기의 다음 자연 경계부터 최대 3회 진입.
- 매도 HIGH는 샘플링하지 않고 첫 유효 HIGH 신호에서 전량 종료한다.

이 계산으로 원 주기를 기다리는 동안 LOW 상태가 사라져 진입하지 못한 경우와,
운영 주기에서만 진입 기회를 확보한 경우를 구분할 수 있다.
"""
from __future__ import annotations

import os
from collections import defaultdict
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg

DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()
TF_MINUTES = {
    "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60,
    "2h": 120, "4h": 240, "6h": 360, "12h": 720,
    "1d": 1440, "3d": 4320, "1w": 10080, "1M": 43200,
}
HALF_MINUTES = {
    "3m": 3, "5m": 5, "15m": 5, "30m": 15, "1h": 30,
    "2h": 60, "4h": 120, "6h": 180, "12h": 360,
    "1d": 720, "3d": 2160, "1w": 5040, "1M": 21600,
}
STOCK_OPERATING_MINUTES = {
    "30m": 15, "1h": 30, "4h": 60, "1d": 60,
    "3d": 60, "1w": 60, "1M": 1440,
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
        "LONG": ["4h", "1d"],
        "LIFE": ["3d", "1w", "1M"],
    },
    "US": {
        "SWING": ["30m", "1h"],
        "LONG": ["4h", "1d"],
        "LIFE": ["3d", "1w", "1M"],
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
MAX_ADMIN_VALID_STAGES = 5  # Telegram/실제 진입은 3 유지, 관리자 연구만 4~5까지 추적
FIVE_VALID_COOLDOWN_SECONDS = 300
THREE_VALID_COOLDOWN_SECONDS = 180
CLOCK5_MINUTES = 5
FIVE_RESET_MINUTES = {
    "3m": 15, "5m": 15, "15m": 15, "30m": 30,
    "1h": 60, "2h": 60, "4h": 60, "6h": 60, "12h": 60,
    "1d": 60, "3d": 60, "1w": 60, "1M": 60,
}

def _five_reset_minutes(tf: str) -> int:
    return FIVE_RESET_MINUTES.get(tf, 60)


def _operating_minutes(market: str, tf: str) -> int:
    if market in {"KOREA", "US"}:
        return STOCK_OPERATING_MINUTES.get(tf, HALF_MINUTES.get(tf, TF_MINUTES.get(tf, 0)))
    return HALF_MINUTES.get(tf, TF_MINUTES.get(tf, 0))


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
    """실제 텔레그램에 표시될 알람 수를 방식별로 계산한다.

    FIVE는 V93 실제 운영과 동일하게 '집중 기준 리셋 + 5분 유효 쿨타임'을 적용한다.
    """
    if mode == "ALL":
        return list(signals)

    sampled: list[dict[str, Any]] = []
    state: dict[tuple[str, str, str], dict[str, Any]] = {}
    for signal in signals:
        key = (signal["symbol"], signal["type"], signal["tf"])
        previous = state.get(key)

        if mode in {"FIVE", "THREE", "CLOCK5"}:
            reset_sec = _five_reset_minutes(signal["tf"]) * 60
            new_episode = previous is None or (signal["time"] - previous["focus_time"]).total_seconds() >= reset_sec
            if new_episode:
                sampled.append(signal)
                state[key] = {
                    "focus_time": signal["time"],
                    "last_sent": signal["time"],
                    "valid_count": 0,
                    "sent_slot": _slot(signal["time"], CLOCK5_MINUTES),
                }
                continue
            valid_count = int(previous.get("valid_count", 0))
            if valid_count >= MAX_ENTRIES:
                state[key] = previous
                continue
            should_send = False
            if mode == "CLOCK5":
                slot = _slot(signal["time"], CLOCK5_MINUTES)
                focus_slot = _slot(previous["focus_time"], CLOCK5_MINUTES)
                should_send = slot > focus_slot and slot != previous.get("sent_slot")
                if should_send:
                    previous["sent_slot"] = slot
            else:
                cooldown = FIVE_VALID_COOLDOWN_SECONDS if mode == "FIVE" else THREE_VALID_COOLDOWN_SECONDS
                should_send = (signal["time"] - previous["last_sent"]).total_seconds() >= cooldown
            if should_send:
                sampled.append(signal)
                previous["last_sent"] = signal["time"]
                previous["valid_count"] = valid_count + 1
            state[key] = previous
            continue

        cadence = signal["mins"] if mode == "FULL" else _operating_minutes(
            signal["market"], signal["tf"]
        )
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

    if mode in {"ALL", "FIVE", "THREE"}:
        last_entry_time = position["entries"][-1]["time"] if position else episode["focus_time"]
        cooldown = THREE_VALID_COOLDOWN_SECONDS if mode == "THREE" else CURRENT_ENTRY_COOLDOWN_SECONDS
        return (signal["time"] - last_entry_time).total_seconds() >= cooldown

    if mode == "CLOCK5":
        current_slot = _slot(signal["time"], CLOCK5_MINUTES)
        if current_slot <= episode["focus_slot"]:
            return False
        if episode.get("last_entry_slot") == current_slot:
            return False
        episode["last_entry_slot"] = current_slot
        return True

    cadence = signal["mins"] if mode == "FULL" else _operating_minutes(
        signal["market"], signal["tf"]
    )
    current_slot = _slot(signal["time"], cadence)
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
            if mode in {"FIVE", "THREE", "CLOCK5"}:
                new_episode = previous is None or (signal["time"] - previous["focus_time"]).total_seconds() >= _five_reset_minutes(signal["tf"]) * 60
            else:
                new_episode = (
                    previous is None
                    or (signal["time"] - previous["last_time"]).total_seconds() > EPISODE_GAP_SECONDS
                )
            if new_episode:
                if mode in {"FIVE", "CLOCK5"}:
                    cadence = 5
                elif mode == "THREE":
                    cadence = 3
                else:
                    cadence = signal["mins"] if mode != "HALF" else _operating_minutes(
                        signal["market"], signal["tf"]
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
                    "entry_points": [
                        {"price": item["price"], "time": item["time"]}
                        for item in position["entries"]
                    ],
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



def _cycle_stats(cycles: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(c["return_pct"]) for c in cycles]
    entries = [int(c.get("entries") or 0) for c in cycles]
    holds = [
        max(0.0, (c["exit_time"] - c["entry_time"]).total_seconds() / 60.0)
        for c in cycles if c.get("entry_time") and c.get("exit_time")
    ]
    mae_values = [float(c["mae_pct"]) for c in cycles if c.get("mae_pct") is not None]
    mfe_values = [float(c["mfe_pct"]) for c in cycles if c.get("mfe_pct") is not None]
    return {
        "completed_cycles": len(cycles),
        "win_rate_pct": (sum(v > 0 for v in values) / len(values) * 100.0) if values else None,
        "average_return_pct": (sum(values) / len(values)) if values else None,
        "best_return_pct": max(values) if values else None,
        "worst_return_pct": min(values) if values else None,
        "average_entries": (sum(entries) / len(entries)) if entries else None,
        "average_holding_minutes": (sum(holds) / len(holds)) if holds else None,
        "average_mae_pct": (sum(mae_values) / len(mae_values)) if mae_values else None,
        "average_mfe_pct": (sum(mfe_values) / len(mfe_values)) if mfe_values else None,
        "mae_mfe_samples": min(len(mae_values), len(mfe_values)),
    }


def _serialise_cycle(cycle: dict[str, Any], code: str) -> dict[str, Any]:
    return {
        "code": code,
        "symbol": cycle.get("symbol"),
        "group": cycle.get("group"),
        "group_label": GROUP_LABEL.get(cycle.get("group"), cycle.get("group")),
        "entry_tf": cycle.get("entry_tf"),
        "exit_tf": cycle.get("exit_tf"),
        "entries": int(cycle.get("entries") or 0),
        "entry_price": float(cycle.get("entry_price") or 0),
        "entry_time": cycle.get("entry_time"),
        "entry_points": cycle.get("entry_points") or [],
        "exit_price": float(cycle.get("exit_price") or 0),
        "exit_time": cycle.get("exit_time"),
        "return_pct": float(cycle.get("return_pct") or 0),
        "mae_pct": float(cycle["mae_pct"]) if cycle.get("mae_pct") is not None else None,
        "mfe_pct": float(cycle["mfe_pct"]) if cycle.get("mfe_pct") is not None else None,
        "mae_mfe_candle_count": int(cycle.get("mae_mfe_candle_count") or 0),
    }



def _latest_cycle_detail(cycles: list[dict[str, Any]], code: str) -> dict[str, Any] | None:
    if not cycles:
        return None
    latest = max(cycles, key=lambda c: c.get("exit_time") or datetime.min.replace(tzinfo=timezone.utc))
    return _serialise_cycle(latest, code)


def _allowed_exit_timeframes(market: str, entry_group: str) -> list[str]:
    """해당 진입 포지션에서 분석 가능한 매도 시간봉 후보를 운영 규칙 순서대로 반환한다."""
    allowed_groups = EXIT_GROUPS.get(entry_group, set())
    candidates: list[str] = []
    for group_name, tfs in GROUPS.get(market, {}).items():
        if group_name not in allowed_groups:
            continue
        for tf in tfs:
            if tf not in candidates:
                candidates.append(tf)
    return sorted(candidates, key=lambda tf: TF_MINUTES.get(tf, 999999))


def _symbol_recent_comparison(market: str, signals: list[dict[str, Any]], simulations: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """종목별 최근 실제 진입 타점 비교.

    V97:
    - 완료 사이클이 없어도 해당 종목에서 LOW 신호가 존재한 진입 시간봉은 분석 행을 만든다.
    - 허용 가능한 매도 시간봉 조합을 미리 생성하고 결과가 없으면 빈칸(-)으로 남긴다.
    - 따라서 4h 같은 장기 시간봉도 '완료 0회'라는 이유만으로 화면에서 사라지지 않는다.
    """
    symbols = sorted({str(s.get("symbol") or "") for s in signals if s.get("symbol")})
    by_code: dict[str, dict[tuple[str, str, str, str], list[dict[str, Any]]]] = {}
    all_keys: set[tuple[str, str, str, str]] = set()
    for code in ("ALL", "THREE", "FIVE", "CLOCK5", "FULL", "HALF"):
        bucket: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        for cycle in simulations.get(code, {}).get("cycles", []):
            key = (
                str(cycle.get("symbol") or ""),
                str(cycle.get("group") or ""),
                str(cycle.get("entry_tf") or ""),
                str(cycle.get("exit_tf") or ""),
            )
            bucket.setdefault(key, []).append(cycle)
            all_keys.add(key)
        by_code[code] = bucket

    for sig in signals:
        if sig.get("type") != "LOW":
            continue
        symbol = str(sig.get("symbol") or "")
        group = str(sig.get("group") or "")
        tf = str(sig.get("tf") or "")
        if symbol and group and tf:
            for exit_tf in _allowed_exit_timeframes(market, group):
                all_keys.add((symbol, group, tf, exit_tf))

    output: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_keys = [k for k in all_keys if k[0] == symbol]
        if not symbol_keys:
            continue
        groups_out = []
        market_group_order = list(GROUPS.get(market, {}))
        group_names = sorted(
            {k[1] for k in symbol_keys},
            key=lambda g: market_group_order.index(g) if g in market_group_order else 99,
        )
        for group in group_names:
            rows = []
            group_keys = [k for k in symbol_keys if k[1] == group]
            group_keys.sort(key=lambda k: (TF_MINUTES.get(k[2], 999999), TF_MINUTES.get(k[3], 999999)))
            for key in group_keys:
                _, _, entry_tf, exit_tf = key
                variants = []
                for code, label in (
                    ("ALL", "1분 원본"),
                    ("THREE", "3분 쿨타임 · 연구"),
                    ("FIVE", "5분 쿨타임 · 현재운영"),
                    ("CLOCK5", "정시 5분봉 · 연구"),
                    ("FULL", "자기 시간봉"),
                    ("HALF", "절반 주기"),
                ):
                    detail = _latest_cycle_detail(by_code[code].get(key, []), code)
                    variants.append({"code": code, "label": label, "detail": detail})
                rows.append({"entry_tf": entry_tf, "exit_tf": exit_tf, "variants": variants})
            groups_out.append({"group": group, "group_label": GROUP_LABEL.get(group, group), "rows": rows})
        output.append({"symbol": symbol, "groups": groups_out})
    return output


def _coin_scalp_combinations(simulations: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """V93 현재 운영(FIVE) 기준 코인 단타 5m/15m 매수×매도 4개 조합 성과."""
    cycles = simulations.get("FIVE", {}).get("cycles", [])
    rows: list[dict[str, Any]] = []
    for entry_tf in ("5m", "15m"):
        for exit_tf in ("5m", "15m"):
            matched = [c for c in cycles if c.get("group") == "SCALP" and c.get("entry_tf") == entry_tf and c.get("exit_tf") == exit_tf]
            stats = _cycle_stats(matched)
            values = [float(c.get("return_pct") or 0.0) for c in matched]
            rows.append({
                "entry_timeframe": entry_tf,
                "exit_timeframe": exit_tf,
                "has_results": bool(matched),
                "result_count": len(matched),
                "symbol_count": len({c.get("symbol") for c in matched if c.get("symbol")}),
                "average_return_pct": stats.get("average_return_pct"),
                "best_return_pct": stats.get("best_return_pct"),
                "worst_return_pct": stats.get("worst_return_pct"),
                "win_rate_pct": stats.get("win_rate_pct"),
                "win_count": sum(1 for v in values if v > 0),
                "loss_count": sum(1 for v in values if v <= 0),
                "average_holding_minutes": stats.get("average_holding_minutes"),
            })
    return rows



def _coin_scalp_details(simulations: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """V93 운영(FIVE) 기준 단타 4개 조합의 종목별/사이클별 상세 데이터."""
    cycles = simulations.get("FIVE", {}).get("cycles", [])
    output: list[dict[str, Any]] = []
    for entry_tf in ("5m", "15m"):
        for exit_tf in ("5m", "15m"):
            matched = [c for c in cycles if c.get("group") == "SCALP" and c.get("entry_tf") == entry_tf and c.get("exit_tf") == exit_tf]
            by_symbol: dict[str, list[dict[str, Any]]] = {}
            for cycle in matched:
                symbol = str(cycle.get("symbol") or "")
                if symbol:
                    by_symbol.setdefault(symbol, []).append(cycle)
            symbols_out = []
            for symbol in sorted(by_symbol):
                items = sorted(by_symbol[symbol], key=lambda c: c.get("exit_time") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
                stats = _cycle_stats(items)
                symbols_out.append({
                    "symbol": symbol,
                    "result_count": len(items),
                    "average_return_pct": stats.get("average_return_pct"),
                    "win_rate_pct": stats.get("win_rate_pct"),
                    "best_return_pct": stats.get("best_return_pct"),
                    "worst_return_pct": stats.get("worst_return_pct"),
                    "average_holding_minutes": stats.get("average_holding_minutes"),
                    "cycles": [_serialise_cycle(c, "HALF") for c in items],
                })
            output.append({
                "entry_timeframe": entry_tf,
                "exit_timeframe": exit_tf,
                "symbols": symbols_out,
                "result_count": len(matched),
            })
    return output


def _attach_mae_mfe(simulations: dict[str, dict[str, Any]]) -> None:
    """Attach 5m-candle based MAE/MFE to every completed simulated cycle.

    MAE is clamped to <=0% and MFE to >=0%. Missing candle coverage stays None,
    so historical periods without stored candles are never treated as zero excursion.
    One range query is used for all modes to avoid per-cycle DB load.
    """
    all_cycles = [c for sim in simulations.values() for c in sim.get("cycles", [])]
    usable = [c for c in all_cycles if c.get("symbol") and c.get("entry_time") and c.get("exit_time") and float(c.get("entry_price") or 0) > 0]
    if not usable:
        return
    ranges: dict[str, tuple[datetime, datetime]] = {}
    for c in usable:
        sym = str(c["symbol"])
        start, end = c["entry_time"], c["exit_time"]
        if sym not in ranges:
            ranges[sym] = (start, end)
        else:
            a, b = ranges[sym]
            ranges[sym] = (min(a, start), max(b, end))
    if not ranges:
        return
    values_sql = ",".join(["(%s,%s,%s)"] * len(ranges))
    params: list[Any] = []
    for sym, (start, end) in ranges.items():
        params.extend([sym, start, end])
    sql = f"""
        WITH ranges(symbol,start_at,end_at) AS (VALUES {values_sql})
        SELECT c.symbol,c.bar_time,c.high,c.low
        FROM performance_candles_5m c
        JOIN ranges r ON r.symbol=c.symbol
                     AND c.bar_time BETWEEN r.start_at AND r.end_at
        ORDER BY c.symbol,c.bar_time
    """
    try:
        with _connect() as conn:
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        return
    candles: dict[str, list[tuple[datetime, float, float]]] = defaultdict(list)
    for sym, bar_time, high, low in rows:
        candles[str(sym)].append((bar_time, float(high), float(low)))
    indexed: dict[str, tuple[list[datetime], list[tuple[datetime, float, float]]]] = {
        sym: ([r[0] for r in items], items) for sym, items in candles.items()
    }
    for c in usable:
        sym = str(c["symbol"])
        if sym not in indexed:
            continue
        times, items = indexed[sym]
        left = bisect_left(times, c["entry_time"])
        right = bisect_right(times, c["exit_time"])
        window = items[left:right]
        if not window:
            continue
        entry = float(c["entry_price"])
        low = min(x[2] for x in window)
        high = max(x[1] for x in window)
        c["mae_pct"] = min(0.0, (low - entry) / entry * 100.0)
        c["mfe_pct"] = max(0.0, (high - entry) / entry * 100.0)
        c["mae_mfe_candle_count"] = len(window)



def _simulate_entry_plan(
    signals: list[dict[str, Any]],
    *,
    include_focus: bool,
    max_entries: int,
    focus_only: bool = False,
) -> dict[str, Any]:
    """V96 관리자 연구용 5분 단계 진입 시나리오.

    실제 Telegram/실제 운영 진입에는 영향을 주지 않는다.
    - 집중 단독: 집중에서 1회만 진입
    - 집중 포함 N분할: 집중 + 유효1... 순서로 최대 N회
    - 집중 스킵 N분할: 유효1... 순서로 최대 N회

    단계 간격은 현재 운영과 동일한 5분이며 집중 리셋도 현재 운영 규칙을 따른다.
    HIGH 청산 규칙은 기존 cadence 시뮬레이터와 동일하게 첫 유효 HIGH에서 전량 종료한다.
    """
    episodes: dict[tuple[str, str, str], dict[str, Any]] = {}
    open_positions: dict[tuple[str, str, str], dict[str, Any]] = {}
    episode_records: list[dict[str, Any]] = []
    cycles: list[dict[str, Any]] = []

    def add_entry(key: tuple[str, str, str], signal: dict[str, Any], episode: dict[str, Any], stage: int) -> None:
        position = open_positions.get(key)
        if position is None:
            position = {
                "symbol": signal["symbol"],
                "group": signal["group"],
                "entry_tf": signal["tf"],
                "entries": [],
            }
            open_positions[key] = position
        if len(position["entries"]) >= max_entries:
            return
        position["entries"].append({**signal, "research_stage": stage})
        episode["entered"] = True

    for signal in signals:
        if signal["type"] == "LOW":
            key = (signal["symbol"], signal["group"], signal["tf"])
            previous = episodes.get(key)
            reset_sec = _five_reset_minutes(signal["tf"]) * 60
            new_episode = previous is None or (signal["time"] - previous["focus_time"]).total_seconds() >= reset_sec

            if new_episode:
                episode = {
                    "key": key,
                    "group": signal["group"],
                    "tf": signal["tf"],
                    "focus_time": signal["time"],
                    "focus_price": signal["price"],
                    "last_stage_time": signal["time"],
                    "stage": 0,
                    "entered": False,
                }
                episodes[key] = episode
                episode_records.append(episode)
                if include_focus:
                    add_entry(key, signal, episode, 0)
                continue

            episode = previous
            if focus_only:
                continue
            if int(episode.get("stage", 0)) >= MAX_ADMIN_VALID_STAGES:
                continue
            if (signal["time"] - episode["last_stage_time"]).total_seconds() < FIVE_VALID_COOLDOWN_SECONDS:
                continue

            stage = int(episode.get("stage", 0)) + 1
            episode["stage"] = stage
            episode["last_stage_time"] = signal["time"]

            position = open_positions.get(key)
            current_entries = len(position["entries"]) if position else 0
            if current_entries >= max_entries:
                continue

            # 집중 포함: 집중이 1차이므로 유효1부터 2차.
            # 집중 스킵: 유효1이 1차.
            add_entry(key, signal, episode, stage)
            continue

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
                    "entry_points": [
                        {
                            "price": item["price"],
                            "time": item["time"],
                            "stage": int(item.get("research_stage", 0)),
                        }
                        for item in position["entries"]
                    ],
                    "exit_price": signal["price"],
                    "exit_time": signal["time"],
                })
                del open_positions[key]

    return {
        "cycles": cycles,
        "episodes": episode_records,
        "focus_count": len(episode_records),
        "entered_focus_count": sum(1 for e in episode_records if e.get("entered")),
        "no_entry_focus_count": sum(1 for e in episode_records if not e.get("entered")),
        "open_position_count": len(open_positions),
    }


def _entry_plan_research(signals: list[dict[str, Any]]) -> dict[str, Any]:
    """V96: 같은 원본 신호를 다섯 가지 매수 방식으로 재계산한다."""
    plans = [
        ("FOCUS_ONLY", "집중 단독 1회", True, 1, True, "집중 100% 1회 진입"),
        ("FOCUS_3", "집중 포함 3분할", True, 3, False, "집중 + 유효1 + 유효2"),
        ("FOCUS_5", "집중 포함 5분할", True, 5, False, "집중 + 유효1 + 유효2 + 유효3 + 유효4"),
        ("SKIP_3", "집중 스킵 3분할 · 현재운영", False, 3, False, "유효1 + 유효2 + 유효3"),
        ("SKIP_5", "집중 스킵 5분할 · 연구", False, 5, False, "유효1 + 유효2 + 유효3 + 유효4 + 유효5"),
    ]
    sims: dict[str, dict[str, Any]] = {}
    for code, _label, include_focus, max_entries, focus_only, _desc in plans:
        sims[code] = _simulate_entry_plan(
            signals,
            include_focus=include_focus,
            max_entries=max_entries,
            focus_only=focus_only,
        )

    # 기존 5분봉 저장 데이터로 다섯 연구 시나리오의 MAE/MFE도 한 번에 계산.
    _attach_mae_mfe(sims)

    rows: list[dict[str, Any]] = []
    for code, label, _include_focus, max_entries, _focus_only, description in plans:
        stat = _cycle_stats(sims[code]["cycles"])
        rows.append({
            "code": code,
            "label": label,
            "description": description,
            "max_entries": max_entries,
            **stat,
        })

    by_timeframe: list[dict[str, Any]] = []
    timeframes = sorted({s["tf"] for s in signals if s["type"] == "LOW"}, key=lambda x: TF_MINUTES.get(x, 999999))
    for tf in timeframes:
        item = {"timeframe": tf, "group": _group(signals[0]["market"], tf) if signals else None, "variants": []}
        item["group_label"] = GROUP_LABEL.get(item["group"], "")
        for code, label, _a, _b, _c, description in plans:
            tf_cycles = [c for c in sims[code]["cycles"] if c.get("entry_tf") == tf]
            item["variants"].append({"code": code, "label": label, "description": description, **_cycle_stats(tf_cycles)})
        by_timeframe.append(item)

    by_symbol: list[dict[str, Any]] = []
    symbols = sorted({str(s.get("symbol") or "") for s in signals if s.get("symbol")})
    market = signals[0]["market"] if signals else ""
    for symbol in symbols:
        symbol_item = {"symbol": symbol, "plans": [], "by_timeframe": []}
        symbol_low_tfs = sorted(
            {str(s.get("tf") or "") for s in signals if s.get("type") == "LOW" and str(s.get("symbol") or "") == symbol},
            key=lambda x: TF_MINUTES.get(x, 999999),
        )
        for code, label, _include_focus, max_entries, _focus_only, description in plans:
            symbol_cycles = [c for c in sims[code]["cycles"] if str(c.get("symbol") or "") == symbol]
            symbol_item["plans"].append({
                "code": code,
                "label": label,
                "description": description,
                "max_entries": max_entries,
                **_cycle_stats(symbol_cycles),
            })
        for tf in symbol_low_tfs:
            tf_item = {
                "timeframe": tf,
                "group": _group(market, tf),
                "group_label": GROUP_LABEL.get(_group(market, tf), ""),
                "variants": [],
            }
            for code, label, _a, _b, _c, description in plans:
                tf_cycles = [
                    c for c in sims[code]["cycles"]
                    if str(c.get("symbol") or "") == symbol and c.get("entry_tf") == tf
                ]
                tf_item["variants"].append({
                    "code": code,
                    "label": label,
                    "description": description,
                    **_cycle_stats(tf_cycles),
                })
            symbol_item["by_timeframe"].append(tf_item)
        by_symbol.append(symbol_item)

    return {"plans": rows, "by_timeframe": by_timeframe, "by_symbol": by_symbol}


def _admin_stage_research(signals: list[dict[str, Any]]) -> dict[str, Any]:
    """Reconstruct focus + VALID1..VALID5 from raw signals without changing trade entries.

    This mirrors V94 live collection: 5-minute stage spacing and the same focus reset.
    Stages 4/5 are research-only and never count as entries or Telegram messages.
    """
    state: dict[tuple[str, str, str], dict[str, Any]] = {}
    counts = {i: 0 for i in range(6)}
    by_tf: dict[str, dict[int, int]] = {}
    for sig in signals:
        key=(sig["symbol"],sig["type"],sig["tf"])
        prev=state.get(key)
        reset_sec=_five_reset_minutes(sig["tf"])*60
        if prev is None or (sig["time"]-prev["focus_time"]).total_seconds() >= reset_sec:
            stage=0
            state[key]={"focus_time":sig["time"],"last_stage":sig["time"],"stage":0}
        else:
            stage=int(prev.get("stage",0))
            if stage>=MAX_ADMIN_VALID_STAGES:
                continue
            if (sig["time"]-prev["last_stage"]).total_seconds() < FIVE_VALID_COOLDOWN_SECONDS:
                continue
            stage += 1
            prev["stage"]=stage; prev["last_stage"]=sig["time"]
            state[key]=prev
        counts[stage]+=1
        by_tf.setdefault(sig["tf"],{i:0 for i in range(6)})[stage]+=1
    return {"counts":counts,"by_timeframe":by_tf,"max_stage":MAX_ADMIN_VALID_STAGES}


def simulate_cadence(market: str, period_key: str = "all") -> dict[str, Any]:
    signals = _load(market, period_key)
    variants: list[dict[str, Any]] = []
    alert_samples: dict[str, list[dict[str, Any]]] = {}
    simulations: dict[str, dict[str, Any]] = {}

    labels = (
        ("ALL", "1분 원본"),
        ("THREE", "3분 쿨타임 · 연구"),
        ("FIVE", "5분 쿨타임 · 현재운영"),
        ("CLOCK5", "정시 5분봉 · 연구"),
        ("FULL", "자기 시간봉 주기"),
        ("HALF", "절반 주기"),
    )
    for code, _label in labels:
        alert_samples[code] = _sample_alerts(signals, code)
        simulations[code] = _simulate_cycles(signals, code)

    # One bulk candle query enriches every mode with MAE/MFE; missing history remains blank.
    _attach_mae_mfe(simulations)

    for code, label in labels:
        simulation = simulations[code]
        stats = _stats(len(signals), len(alert_samples[code]), simulation)
        stats.update({k: v for k, v in _cycle_stats(simulation["cycles"]).items() if k in {"average_mae_pct", "average_mfe_pct", "mae_mfe_samples"}})
        variants.append({
            "code": code,
            "label": label,
            **stats,
        })

    timeframe_rows: list[dict[str, Any]] = []
    for tf in sorted({s["tf"] for s in signals}, key=lambda value: TF_MINUTES.get(value, 999999)):
        raw = [s for s in signals if s["tf"] == tf]
        three = _sample_alerts(raw, "THREE")
        five = _sample_alerts(raw, "FIVE")
        clock5 = _sample_alerts(raw, "CLOCK5")
        full = _sample_alerts(raw, "FULL")
        half = _sample_alerts(raw, "HALF")
        timeframe_rows.append({
            "timeframe": tf,
            "raw_count": len(raw),
            "three_count": len(three),
            "five_count": len(five),
            "clock5_count": len(clock5),
            "full_count": len(full),
            "half_count": len(half),
            "three_reduction_pct": ((len(raw) - len(three)) / len(raw) * 100) if raw else 0.0,
            "five_reduction_pct": ((len(raw) - len(five)) / len(raw) * 100) if raw else 0.0,
            "clock5_reduction_pct": ((len(raw) - len(clock5)) / len(raw) * 100) if raw else 0.0,
            "full_reduction_pct": ((len(raw) - len(full)) / len(raw) * 100) if raw else 0.0,
            "half_reduction_pct": ((len(raw) - len(half)) / len(raw) * 100) if raw else 0.0,
        })

    group_rows: list[dict[str, Any]] = []
    for group in GROUPS.get(market, {}):
        item = {"group": group, "group_label": GROUP_LABEL[group], "variants": []}
        raw_group_count = len([s for s in signals if s["group"] == group])
        for code, short_label in (("ALL", "1분 원본"), ("THREE", "3분 쿨타임"), ("FIVE", "5분 현재운영"), ("CLOCK5", "정시 5분봉"), ("FULL", "자기 시간봉"), ("HALF", "절반 주기")):
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

    # 같은 시간봉에서 1분 원본/자기 시간봉/절반 운영 주기를 적용했을 때의
    # 실제 진입가격·완료 수익률·승률을 직접 비교한다.
    timeframe_performance: list[dict[str, Any]] = []
    for tf in sorted({s["tf"] for s in signals}, key=lambda value: TF_MINUTES.get(value, 999999)):
        row = {"timeframe": tf, "group": _group(market, tf), "group_label": GROUP_LABEL.get(_group(market, tf), ""), "variants": []}
        for code, label in (("ALL", "1분 원본"), ("THREE", "3분 쿨타임"), ("FIVE", "5분 현재운영"), ("CLOCK5", "정시 5분봉"), ("FULL", "자기 시간봉"), ("HALF", "절반 주기")):
            tf_cycles = [c for c in simulations[code]["cycles"] if c["entry_tf"] == tf]
            row["variants"].append({"code": code, "label": label, **_cycle_stats(tf_cycles)})
        timeframe_performance.append(row)

    recent_cycles: list[dict[str, Any]] = []
    for code in ("ALL", "THREE", "FIVE", "CLOCK5", "FULL", "HALF"):
        for cycle in simulations[code]["cycles"]:
            recent_cycles.append(_serialise_cycle(cycle, code))
    recent_cycles.sort(key=lambda c: c.get("exit_time") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    recent_cycles = recent_cycles[:36]
    symbol_recent_comparison = _symbol_recent_comparison(market, signals, simulations)
    scalp_combinations = _coin_scalp_combinations(simulations) if market == "COIN" else []
    scalp_details = _coin_scalp_details(simulations) if market == "COIN" else []

    admin_stage_research = _admin_stage_research(signals)
    entry_plan_research = _entry_plan_research(signals)
    return {
        "admin_stage_research": admin_stage_research,
        "entry_plan_research": entry_plan_research,
        "market": market,
        "period_key": period_key,
        "raw_signal_count": len(signals),
        "variants": variants,
        "timeframes": timeframe_rows,
        "groups": group_rows,
        "timeframe_performance": timeframe_performance,
        "recent_cycles": recent_cycles,
        "symbol_recent_comparison": symbol_recent_comparison,
        "scalp_combinations": scalp_combinations,
        "scalp_details": scalp_details,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "note": (
            "매수 첫 LOW는 집중 알림으로만 사용하고, 두 번째 유효 LOW부터 최대 3회 분할진입했습니다. "
            "5분 쿨타임은 현재 실제 운영 규칙(5m/15m=15분·30m=30분·1h+=60분 집중 리셋)이며, "
            "3분 쿨타임과 정시 5분봉은 Telegram 전송 없는 관리자 연구용입니다. 자기 시간봉·절반 주기도 비교 연구용입니다. "
            "MAE/MFE는 저장된 5분봉이 존재하는 완료사이클만 계산하므로 과거 미수집 구간은 빈칸으로 남습니다. "
            "V97 관리자 연구에서는 집중 단독, 집중 포함 3/5분할, 집중 스킵 3/5분할을 종목별·시간봉별로도 별도 비교하며, 미완료 시간봉 조합은 빈칸으로 유지합니다."
        ),
    }

