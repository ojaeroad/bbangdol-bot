"""회원 운영용 성과 분석 엔진 2차.

진입 구간 정의:
- 같은 전략·거래소·종목에서 LOW 신호가 연속되는 구간을 하나의 진입 구간으로 본다.
- 그 뒤 HIGH 신호가 연속되는 구간을 해당 진입 구간의 청산 후보로 본다.
- HIGH 이후 다시 LOW가 나오면 새로운 진입 구간으로 시작한다.

진입 방식:
1) INDIVIDUAL
   각 저점 알람을 각각 개별 진입으로 계산
2) MAX_TIMEFRAME
   진입 구간 안에서 가장 높은 시간봉의 첫 저점 알람만 진입
3) ALL_SPLIT
   진입 구간 안의 모든 저점 알람을 동일 금액 분할진입한 평균가
4) TIMEFRAME_SPLIT
   같은 시간봉의 저점 알람끼리 동일 금액 분할진입한 평균가
   예: 1h 여러 번 평균, 4h 여러 번 평균을 각각 별도 결과로 계산

청산:
- 해당 구간 뒤에 발생한 모든 고점 알람을 후보로 계산
- 하위·동일·상위 시간봉을 모두 포함
- 현재 테스트 단계에서는 수익인 결과만 저장
"""

from __future__ import annotations

import os
from collections import defaultdict
from decimal import Decimal
from typing import Any

import psycopg

PERFORMANCE_DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS performance_trade_pairs (
    id BIGSERIAL PRIMARY KEY,

    cycle_key VARCHAR(120),
    strategy VARCHAR(30) NOT NULL,
    exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,

    entry_mode VARCHAR(30) NOT NULL,
    entry_group_label VARCHAR(50),

    entry_signal_id BIGINT,
    exit_signal_id BIGINT NOT NULL
        REFERENCES performance_signals(id) ON DELETE CASCADE,

    entry_signal_no VARCHAR(24),
    exit_signal_no VARCHAR(24),

    entry_count INTEGER NOT NULL DEFAULT 1,

    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ NOT NULL,

    entry_timeframe VARCHAR(10),
    exit_timeframe VARCHAR(10),
    entry_timeframe_minutes INTEGER,
    exit_timeframe_minutes INTEGER,
    exit_tf_relation VARCHAR(10) NOT NULL,

    entry_price NUMERIC(30, 10) NOT NULL,
    exit_price NUMERIC(30, 10) NOT NULL,
    return_pct NUMERIC(20, 8) NOT NULL,
    holding_minutes BIGINT NOT NULL,

    entry_signal_ids JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE performance_trade_pairs
    ADD COLUMN IF NOT EXISTS cycle_key VARCHAR(120);
ALTER TABLE performance_trade_pairs
    ADD COLUMN IF NOT EXISTS entry_group_label VARCHAR(50);
ALTER TABLE performance_trade_pairs
    ADD COLUMN IF NOT EXISTS entry_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE performance_trade_pairs
    ADD COLUMN IF NOT EXISTS entry_signal_ids JSONB;

CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_pair_scenario
    ON performance_trade_pairs(
        cycle_key,
        entry_mode,
        COALESCE(entry_group_label, ''),
        exit_signal_id
    );

CREATE INDEX IF NOT EXISTS idx_trade_pairs_symbol_entry
    ON performance_trade_pairs(strategy, symbol, entry_time);

CREATE INDEX IF NOT EXISTS idx_trade_pairs_mode
    ON performance_trade_pairs(entry_mode);

CREATE INDEX IF NOT EXISTS idx_trade_pairs_relation
    ON performance_trade_pairs(exit_tf_relation, exit_timeframe_minutes);
"""


def _connect() -> psycopg.Connection:
    if not PERFORMANCE_DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")
    return psycopg.connect(
        PERFORMANCE_DATABASE_URL,
        autocommit=True,
        connect_timeout=8,
        application_name="bbangdol-performance-analyzer",
    )


def ensure_analysis_schema() -> None:
    with _connect() as conn:
        conn.execute(CREATE_SQL)


def _relation(entry_minutes: int | None, exit_minutes: int | None) -> str:
    if entry_minutes is None or exit_minutes is None:
        return "UNKNOWN"
    if exit_minutes < entry_minutes:
        return "LOWER"
    if exit_minutes == entry_minutes:
        return "SAME"
    return "HIGHER"


def _weighted_average(prices: list[Decimal]) -> Decimal:
    return sum(prices, Decimal("0")) / Decimal(len(prices))


def _load_signals(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            id, signal_no, strategy,
            COALESCE(exchange, raw_exchange),
            symbol, signal_type,
            timeframe, timeframe_minutes,
            signal_price, received_at
        FROM performance_signals
        WHERE signal_price IS NOT NULL
          AND signal_type IN ('LOW', 'HIGH')
        ORDER BY strategy,
                 COALESCE(exchange, raw_exchange, ''),
                 symbol,
                 received_at,
                 id
        """
    ).fetchall()

    return [
        {
            "id": r[0],
            "signal_no": r[1],
            "strategy": r[2],
            "exchange": r[3],
            "symbol": r[4],
            "signal_type": r[5],
            "timeframe": r[6],
            "timeframe_minutes": r[7],
            "price": Decimal(r[8]),
            "time": r[9],
        }
        for r in rows
    ]


def _build_cycles(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for signal in signals:
        key = (
            signal["strategy"],
            signal["exchange"] or "",
            signal["symbol"],
        )
        grouped[key].append(signal)

    cycles: list[dict[str, Any]] = []

    for (strategy, exchange, symbol), rows in grouped.items():
        entries: list[dict[str, Any]] = []
        exits: list[dict[str, Any]] = []
        cycle_index = 0

        def finalize() -> None:
            nonlocal entries, exits, cycle_index
            if entries and exits:
                cycle_index += 1
                cycles.append(
                    {
                        "cycle_key": (
                            f"{strategy}|{exchange}|{symbol}|"
                            f"{entries[0]['id']}|{cycle_index}"
                        ),
                        "strategy": strategy,
                        "exchange": exchange or None,
                        "symbol": symbol,
                        "entries": entries[:],
                        "exits": exits[:],
                    }
                )
            entries = []
            exits = []

        for row in rows:
            if row["signal_type"] == "LOW":
                if exits:
                    finalize()
                entries.append(row)
            else:
                if entries:
                    exits.append(row)

        finalize()

    return cycles


def _scenario_entries(cycle: dict[str, Any]) -> list[dict[str, Any]]:
    entries = cycle["entries"]
    scenarios: list[dict[str, Any]] = []

    # 1. 각 알람 개별 진입
    for entry in entries:
        scenarios.append(
            {
                "mode": "INDIVIDUAL",
                "label": entry["signal_no"],
                "entry_signal_id": entry["id"],
                "entry_signal_no": entry["signal_no"],
                "signal_ids": [entry["id"]],
                "count": 1,
                "price": entry["price"],
                "time": entry["time"],
                "timeframe": entry["timeframe"],
                "timeframe_minutes": entry["timeframe_minutes"],
            }
        )

    # 2. 최대시간봉 진입: 가장 높은 시간봉의 첫 신호
    valid_tf = [e for e in entries if e["timeframe_minutes"] is not None]
    if valid_tf:
        max_minutes = max(e["timeframe_minutes"] for e in valid_tf)
        max_entries = [e for e in valid_tf if e["timeframe_minutes"] == max_minutes]
        chosen = max_entries[0]

        scenarios.append(
            {
                "mode": "MAX_TIMEFRAME",
                "label": f"MAX_{chosen['timeframe']}",
                "entry_signal_id": chosen["id"],
                "entry_signal_no": chosen["signal_no"],
                "signal_ids": [chosen["id"]],
                "count": 1,
                "price": chosen["price"],
                "time": chosen["time"],
                "timeframe": chosen["timeframe"],
                "timeframe_minutes": chosen["timeframe_minutes"],
            }
        )

    # 3. 전체 동일금액 분할진입
    scenarios.append(
        {
            "mode": "ALL_SPLIT",
            "label": "ALL_LOW_SPLIT",
            "entry_signal_id": entries[-1]["id"],
            "entry_signal_no": entries[-1]["signal_no"],
            "signal_ids": [e["id"] for e in entries],
            "count": len(entries),
            "price": _weighted_average([e["price"] for e in entries]),
            "time": entries[-1]["time"],
            "timeframe": max(
                entries,
                key=lambda e: e["timeframe_minutes"] or -1,
            )["timeframe"],
            "timeframe_minutes": max(
                (e["timeframe_minutes"] or -1) for e in entries
            ),
        }
    )

    # 4. 시간봉별 분할진입
    by_tf: dict[tuple[str, int | None], list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        by_tf[(entry["timeframe"] or "unknown", entry["timeframe_minutes"])].append(entry)

    for (timeframe, timeframe_minutes), tf_entries in by_tf.items():
        scenarios.append(
            {
                "mode": "TIMEFRAME_SPLIT",
                "label": f"{timeframe}_SPLIT",
                "entry_signal_id": tf_entries[-1]["id"],
                "entry_signal_no": tf_entries[-1]["signal_no"],
                "signal_ids": [e["id"] for e in tf_entries],
                "count": len(tf_entries),
                "price": _weighted_average([e["price"] for e in tf_entries]),
                "time": tf_entries[-1]["time"],
                "timeframe": timeframe,
                "timeframe_minutes": timeframe_minutes,
            }
        )

    return scenarios


def rebuild_individual_pairs() -> dict[str, Any]:
    """기존 API 이름을 유지하면서 전체 4가지 진입 방식을 함께 계산한다."""
    ensure_analysis_schema()

    insert_sql = """
        INSERT INTO performance_trade_pairs (
            cycle_key,
            strategy, exchange, symbol,
            entry_mode, entry_group_label,
            entry_signal_id, exit_signal_id,
            entry_signal_no, exit_signal_no,
            entry_count,
            entry_time, exit_time,
            entry_timeframe, exit_timeframe,
            entry_timeframe_minutes, exit_timeframe_minutes,
            exit_tf_relation,
            entry_price, exit_price,
            return_pct, holding_minutes,
            entry_signal_ids
        ) VALUES (
            %(cycle_key)s,
            %(strategy)s, %(exchange)s, %(symbol)s,
            %(entry_mode)s, %(entry_group_label)s,
            %(entry_signal_id)s, %(exit_signal_id)s,
            %(entry_signal_no)s, %(exit_signal_no)s,
            %(entry_count)s,
            %(entry_time)s, %(exit_time)s,
            %(entry_timeframe)s, %(exit_timeframe)s,
            %(entry_timeframe_minutes)s, %(exit_timeframe_minutes)s,
            %(exit_tf_relation)s,
            %(entry_price)s, %(exit_price)s,
            %(return_pct)s, %(holding_minutes)s,
            %(entry_signal_ids)s::jsonb
        )
        ON CONFLICT DO NOTHING
        RETURNING id
    """

    examined = 0
    inserted = 0
    skipped_nonprofit = 0
    cycle_count = 0
    mode_counts: dict[str, int] = defaultdict(int)

    with _connect() as conn:
        signals = _load_signals(conn)
        cycles = _build_cycles(signals)
        cycle_count = len(cycles)

        for cycle in cycles:
            scenarios = _scenario_entries(cycle)

            for scenario in scenarios:
                for exit_signal in cycle["exits"]:
                    # 해당 시나리오 진입 시각보다 뒤의 고점만 청산 후보
                    if exit_signal["time"] <= scenario["time"]:
                        continue

                    examined += 1
                    entry_price = scenario["price"]
                    exit_price = exit_signal["price"]

                    if exit_price <= entry_price:
                        skipped_nonprofit += 1
                        continue

                    return_pct = (
                        (exit_price - entry_price)
                        / entry_price
                        * Decimal("100")
                    )
                    holding_minutes = int(
                        (exit_signal["time"] - scenario["time"]).total_seconds()
                        // 60
                    )

                    params = {
                        "cycle_key": cycle["cycle_key"],
                        "strategy": cycle["strategy"],
                        "exchange": cycle["exchange"],
                        "symbol": cycle["symbol"],
                        "entry_mode": scenario["mode"],
                        "entry_group_label": scenario["label"],
                        "entry_signal_id": scenario["entry_signal_id"],
                        "exit_signal_id": exit_signal["id"],
                        "entry_signal_no": scenario["entry_signal_no"],
                        "exit_signal_no": exit_signal["signal_no"],
                        "entry_count": scenario["count"],
                        "entry_time": scenario["time"],
                        "exit_time": exit_signal["time"],
                        "entry_timeframe": scenario["timeframe"],
                        "exit_timeframe": exit_signal["timeframe"],
                        "entry_timeframe_minutes": scenario["timeframe_minutes"],
                        "exit_timeframe_minutes": exit_signal["timeframe_minutes"],
                        "exit_tf_relation": _relation(
                            scenario["timeframe_minutes"],
                            exit_signal["timeframe_minutes"],
                        ),
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "return_pct": return_pct,
                        "holding_minutes": holding_minutes,
                        "entry_signal_ids": str(scenario["signal_ids"]).replace("'", '"'),
                    }

                    result = conn.execute(insert_sql, params).fetchone()
                    if result:
                        inserted += 1
                        mode_counts[scenario["mode"]] += 1

        totals = conn.execute(
            """
            SELECT entry_mode, COUNT(*)
            FROM performance_trade_pairs
            GROUP BY entry_mode
            ORDER BY entry_mode
            """
        ).fetchall()

    return {
        "ok": True,
        "analysis": "ALL_ENTRY_MODES_TO_ALL_LATER_HIGHS",
        "cycle_count": cycle_count,
        "examined_pairs": examined,
        "new_pairs": inserted,
        "skipped_nonprofit_pairs": skipped_nonprofit,
        "new_pairs_by_mode": dict(mode_counts),
        "total_pairs_by_mode": {row[0]: int(row[1]) for row in totals},
        "total_pairs": sum(int(row[1]) for row in totals),
    }


def analysis_summary() -> dict[str, Any]:
    ensure_analysis_schema()

    with _connect() as conn:
        mode_rows = conn.execute(
            """
            SELECT
                entry_mode,
                COUNT(*),
                AVG(return_pct),
                MAX(return_pct),
                AVG(holding_minutes)
            FROM performance_trade_pairs
            GROUP BY entry_mode
            ORDER BY entry_mode
            """
        ).fetchall()

        relation_rows = conn.execute(
            """
            SELECT exit_tf_relation, COUNT(*)
            FROM performance_trade_pairs
            GROUP BY exit_tf_relation
            ORDER BY exit_tf_relation
            """
        ).fetchall()

    return {
        "ok": True,
        "analysis": "ALL_ENTRY_MODES",
        "by_entry_mode": [
            {
                "entry_mode": r[0],
                "pair_count": int(r[1]),
                "average_return_pct": float(r[2]) if r[2] is not None else None,
                "maximum_return_pct": float(r[3]) if r[3] is not None else None,
                "average_holding_minutes": float(r[4]) if r[4] is not None else None,
            }
            for r in mode_rows
        ],
        "by_exit_timeframe_relation": {
            r[0]: int(r[1]) for r in relation_rows
        },
        "total_pairs": sum(int(r[1]) for r in mode_rows),
    }


def latest_analysis_pairs(limit: int = 50) -> list[dict[str, Any]]:
    ensure_analysis_schema()
    safe_limit = max(1, min(int(limit), 200))

    sql = """
        SELECT
            id, cycle_key,
            strategy, exchange, symbol,
            entry_mode, entry_group_label, entry_count,
            entry_signal_no, exit_signal_no,
            entry_timeframe, exit_timeframe, exit_tf_relation,
            entry_price, exit_price, return_pct, holding_minutes,
            entry_time, exit_time
        FROM performance_trade_pairs
        ORDER BY id DESC
        LIMIT %s
    """

    with _connect() as conn:
        rows = conn.execute(sql, (safe_limit,)).fetchall()

    return [
        {
            "pair_id": r[0],
            "cycle_key": r[1],
            "strategy": r[2],
            "exchange": r[3],
            "symbol": r[4],
            "entry_mode": r[5],
            "entry_group_label": r[6],
            "entry_count": int(r[7]),
            "entry_signal_no": r[8],
            "exit_signal_no": r[9],
            "entry_timeframe": r[10],
            "exit_timeframe": r[11],
            "exit_tf_relation": r[12],
            "entry_price": str(r[13]),
            "exit_price": str(r[14]),
            "return_pct": float(r[15]),
            "holding_minutes": int(r[16]),
            "entry_time": r[17].isoformat(),
            "exit_time": r[18].isoformat(),
        }
        for r in rows
    ]
