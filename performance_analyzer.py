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
import re
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
        max_entries = [
            e for e in valid_tf
            if e["timeframe_minutes"] == max_minutes
        ]
        chosen = max_entries[-1]

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


def _market_category(
    strategy: str,
    exchange: str | None,
    symbol: str,
) -> tuple[str, str]:
    """성과 화면용 시장 카테고리 분류.

    STARFLOWER는 코인으로 분류한다.
    1Q는 거래소명과 종목코드를 함께 사용해 국장/미장을 구분한다.
    """
    strategy_upper = (strategy or "").upper()
    exchange_upper = (exchange or "").upper()
    symbol_upper = (symbol or "").upper()

    if strategy_upper == "STARFLOWER":
        return "COIN", "코인 · 별꽃타점"

    if strategy_upper == "1Q":
        korea_tokens = {
            "KRX", "KOSPI", "KOSDAQ", "KONEX",
            "KOREA", "KR", "KSC", "KOE",
        }
        us_tokens = {
            "NASDAQ", "NASDAQGS", "NASDAQGM", "NASDAQCM",
            "NYSE", "NYSEARCA", "AMEX", "ARCA", "BATS",
            "CBOE", "OTC", "USA", "US",
        }

        if any(token in exchange_upper for token in korea_tokens):
            return "KOREA_1Q", "국장 · 1Q 대형주"

        if any(token in exchange_upper for token in us_tokens):
            return "US_1Q", "미장 · 1Q 대형주"

        # 국내 종목은 TradingView 심볼이 6자리 숫자인 경우가 많다.
        compact_symbol = re.sub(r"[^0-9]", "", symbol_upper)
        if len(compact_symbol) == 6 and compact_symbol == symbol_upper:
            return "KOREA_1Q", "국장 · 1Q 대형주"

        return "US_1Q", "미장 · 1Q 대형주"

    return "OTHER", "기타"


def visual_cycle_data(limit_symbols: int = 30) -> dict[str, Any]:
    """회원용 대시보드 데이터.

    완료 사이클:
      개별 / 최대시간봉 / 전체분할 / 시간봉별분할 결과를
      모든 청산 고점 후보별로 계산한다.

    미완료 사이클:
      아직 고점이 없어도 현재 최대시간봉 후보 진입가,
      전체분할 평균가, 시간봉별 평균가를 미리 표시한다.
    """
    ensure_analysis_schema()
    safe_limit = max(1, min(int(limit_symbols), 100))

    with _connect() as conn:
        signals = _load_signals(conn)

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        grouped[
            (
                signal["strategy"],
                signal["exchange"] or "",
                signal["symbol"],
            )
        ].append(signal)

    def slim(signal: dict[str, Any]) -> dict[str, Any]:
        return {
            "signal_no": signal["signal_no"],
            "type": signal["signal_type"],
            "timeframe": signal["timeframe"],
            "timeframe_minutes": signal["timeframe_minutes"],
            "price": str(signal["price"]),
            "time": signal["time"].isoformat(),
        }

    def entry_preview(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not entries:
            return None

        valid_tf = [e for e in entries if e["timeframe_minutes"] is not None]
        if valid_tf:
            max_minutes = max(e["timeframe_minutes"] for e in valid_tf)
            max_entry = [
                e for e in valid_tf
                if e["timeframe_minutes"] == max_minutes
            ][-1]
        else:
            max_entry = entries[-1]
        all_split_price = _weighted_average([e["price"] for e in entries])

        by_tf: dict[tuple[str, int | None], list[dict[str, Any]]] = defaultdict(list)
        for entry in entries:
            by_tf[
                (
                    entry["timeframe"] or "unknown",
                    entry["timeframe_minutes"],
                )
            ].append(entry)

        timeframe_splits = []
        for (timeframe, minutes), tf_entries in sorted(
            by_tf.items(),
            key=lambda item: item[0][1] if item[0][1] is not None else -1,
        ):
            timeframe_splits.append(
                {
                    "timeframe": timeframe,
                    "timeframe_minutes": minutes,
                    "entry_count": len(tf_entries),
                    "average_entry_price": str(
                        _weighted_average([e["price"] for e in tf_entries])
                    ),
                    "last_entry_time": tf_entries[-1]["time"].isoformat(),
                    "signal_nos": [e["signal_no"] for e in tf_entries],
                }
            )

        return {
            "entry_count": len(entries),
            "max_timeframe_entry": slim(max_entry),
            "all_split_average_price": str(all_split_price),
            "all_split_last_entry_time": entries[-1]["time"].isoformat(),
            "timeframe_splits": timeframe_splits,
            "individual_entries": [slim(e) for e in entries],
        }

    symbols = []

    for (strategy, exchange, symbol), rows in grouped.items():
        lows = [r for r in rows if r["signal_type"] == "LOW"]
        highs = [r for r in rows if r["signal_type"] == "HIGH"]

        completed_raw: list[dict[str, Any]] = []
        open_entries: list[dict[str, Any]] = []
        high_only: list[dict[str, Any]] = []

        entries: list[dict[str, Any]] = []
        exits: list[dict[str, Any]] = []

        def finalize_segment() -> None:
            nonlocal entries, exits, open_entries
            if entries and exits:
                completed_raw.append(
                    {
                        "entries": entries[:],
                        "exits": exits[:],
                    }
                )
            elif entries:
                open_entries = entries[:]
            entries = []
            exits = []

        for row in rows:
            if row["signal_type"] == "LOW":
                if exits:
                    finalize_segment()
                entries.append(row)
            else:
                if entries:
                    exits.append(row)
                else:
                    high_only.append(row)

        finalize_segment()

        completed_cycles = []

        for cycle_no, cycle in enumerate(completed_raw, start=1):
            cycle_entries = cycle["entries"]
            cycle_exits = cycle["exits"]
            preview = entry_preview(cycle_entries)
            assert preview is not None

            valid_cycle_entries = [
                e for e in cycle_entries
                if e["timeframe_minutes"] is not None
            ]
            if valid_cycle_entries:
                max_minutes = max(
                    e["timeframe_minutes"]
                    for e in valid_cycle_entries
                )
                max_entry = [
                    e for e in valid_cycle_entries
                    if e["timeframe_minutes"] == max_minutes
                ][-1]
            else:
                max_entry = cycle_entries[-1]
            all_split_price = _weighted_average(
                [e["price"] for e in cycle_entries]
            )

            tf_groups: dict[tuple[str, int | None], list[dict[str, Any]]] = defaultdict(list)
            for entry in cycle_entries:
                tf_groups[
                    (
                        entry["timeframe"] or "unknown",
                        entry["timeframe_minutes"],
                    )
                ].append(entry)

            exit_results = []

            for exit_signal in cycle_exits:
                individual_results = []
                for entry in cycle_entries:
                    individual_results.append(
                        {
                            "entry": slim(entry),
                            "return_pct": float(
                                (exit_signal["price"] - entry["price"])
                                / entry["price"]
                                * Decimal("100")
                            ),
                            "holding_minutes": int(
                                (
                                    exit_signal["time"] - entry["time"]
                                ).total_seconds()
                                // 60
                            ),
                        }
                    )

                individual_return_values = [
                    item["return_pct"]
                    for item in individual_results
                ]
                individual_holding_values = [
                    item["holding_minutes"]
                    for item in individual_results
                ]

                timeframe_split_results = []
                for (timeframe, minutes), tf_entries in sorted(
                    tf_groups.items(),
                    key=lambda item: item[0][1] if item[0][1] is not None else -1,
                ):
                    tf_average = _weighted_average(
                        [e["price"] for e in tf_entries]
                    )
                    timeframe_split_results.append(
                        {
                            "timeframe": timeframe,
                            "entry_count": len(tf_entries),
                            "average_entry_price": str(tf_average),
                            "return_pct": float(
                                (exit_signal["price"] - tf_average)
                                / tf_average
                                * Decimal("100")
                            ),
                            "holding_minutes": int(
                                (
                                    exit_signal["time"] - tf_entries[-1]["time"]
                                ).total_seconds()
                                // 60
                            ),
                        }
                    )

                exit_results.append(
                    {
                        "exit": slim(exit_signal),
                        "relation_to_max_entry": _relation(
                            max_entry["timeframe_minutes"],
                            exit_signal["timeframe_minutes"],
                        ),
                        "max_timeframe_entry": slim(max_entry),
                        "max_timeframe_return_pct": float(
                            (exit_signal["price"] - max_entry["price"])
                            / max_entry["price"]
                            * Decimal("100")
                        ),
                        "max_timeframe_holding_minutes": int(
                            (
                                exit_signal["time"] - max_entry["time"]
                            ).total_seconds()
                            // 60
                        ),
                        "all_split_entry_price": str(all_split_price),
                        "all_split_return_pct": float(
                            (exit_signal["price"] - all_split_price)
                            / all_split_price
                            * Decimal("100")
                        ),
                        "all_split_holding_minutes": int(
                            (
                                exit_signal["time"] - cycle_entries[-1]["time"]
                            ).total_seconds()
                            // 60
                        ),
                        "timeframe_split_results": timeframe_split_results,
                        "individual_results": individual_results,
                        "individual_summary": {
                            "entry_count": len(individual_results),
                            "average_return_pct": (
                                sum(individual_return_values)
                                / len(individual_return_values)
                                if individual_return_values else None
                            ),
                            "maximum_return_pct": (
                                max(individual_return_values)
                                if individual_return_values else None
                            ),
                            "minimum_return_pct": (
                                min(individual_return_values)
                                if individual_return_values else None
                            ),
                            "average_holding_minutes": (
                                sum(individual_holding_values)
                                / len(individual_holding_values)
                                if individual_holding_values else None
                            ),
                        },
                    }
                )

            completed_cycles.append(
                {
                    "cycle_no": cycle_no,
                    "entry_count": len(cycle_entries),
                    "exit_count": len(cycle_exits),
                    "entry_preview": preview,
                    "entries": [slim(e) for e in cycle_entries],
                    "exits": [slim(x) for x in cycle_exits],
                    "exit_results": exit_results,
                }
            )

        category_key, category_label = _market_category(
            strategy,
            exchange,
            symbol,
        )

        # 종목 성과 요약
        max_tf_returns = []
        split_returns = []
        all_returns = []
        holding_minutes = []
        best_exit = None

        for completed_cycle in completed_cycles:
            for exit_result in completed_cycle["exit_results"]:
                max_tf_value = exit_result["max_timeframe_return_pct"]
                split_value = exit_result["all_split_return_pct"]

                max_tf_returns.append(max_tf_value)
                split_returns.append(split_value)
                all_returns.extend([max_tf_value, split_value])
                holding_minutes.append(
                    exit_result["max_timeframe_holding_minutes"]
                )

                for tf_result in exit_result["timeframe_split_results"]:
                    all_returns.append(tf_result["return_pct"])

                for individual_result in exit_result["individual_results"]:
                    all_returns.append(individual_result["return_pct"])

                candidate = {
                    "return_pct": max(max_tf_value, split_value),
                    "exit_timeframe": exit_result["exit"]["timeframe"],
                    "exit_price": exit_result["exit"]["price"],
                }
                if (
                    best_exit is None
                    or candidate["return_pct"] > best_exit["return_pct"]
                ):
                    best_exit = candidate

        wins = [value for value in all_returns if value > 0]
        losses = [value for value in all_returns if value <= 0]

        # 청산 시간봉별 통계
        exit_timeframe_stats_map: dict[str, dict[str, Any]] = {}
        for completed_cycle in completed_cycles:
            for exit_result in completed_cycle["exit_results"]:
                exit_tf = exit_result["exit"]["timeframe"] or "unknown"
                bucket = exit_timeframe_stats_map.setdefault(
                    exit_tf,
                    {
                        "returns": [],
                        "holding_minutes": [],
                        "timeframe_minutes": (
                            exit_result["exit"]["timeframe_minutes"]
                            if exit_result["exit"]["timeframe_minutes"] is not None
                            else -1
                        ),
                    },
                )
                bucket["returns"].extend(
                    [
                        exit_result["max_timeframe_return_pct"],
                        exit_result["all_split_return_pct"],
                    ]
                )
                bucket["holding_minutes"].append(
                    exit_result["max_timeframe_holding_minutes"]
                )

                for tf_result in exit_result["timeframe_split_results"]:
                    bucket["returns"].append(tf_result["return_pct"])

                for individual_result in exit_result["individual_results"]:
                    bucket["returns"].append(individual_result["return_pct"])

        exit_timeframe_stats = []
        for timeframe, bucket in sorted(
            exit_timeframe_stats_map.items(),
            key=lambda item: item[1]["timeframe_minutes"],
        ):
            values = bucket["returns"]
            tf_wins = [value for value in values if value > 0]
            tf_holding = bucket["holding_minutes"]

            exit_timeframe_stats.append(
                {
                    "timeframe": timeframe,
                    "result_count": len(values),
                    "win_count": len(tf_wins),
                    "win_rate_pct": (
                        len(tf_wins) / len(values) * 100
                        if values else None
                    ),
                    "average_return_pct": (
                        sum(values) / len(values)
                        if values else None
                    ),
                    "best_return_pct": max(values) if values else None,
                    "worst_return_pct": min(values) if values else None,
                    "average_holding_minutes": (
                        sum(tf_holding) / len(tf_holding)
                        if tf_holding else None
                    ),
                }
            )

        entry_mode_stats = []
        mode_series = [
            ("MAX_TIMEFRAME", "최대시간봉", max_tf_returns),
            ("ALL_SPLIT", "전체 분할", split_returns),
        ]
        for mode_key, mode_label, values in mode_series:
            mode_wins = [value for value in values if value > 0]
            entry_mode_stats.append(
                {
                    "mode": mode_key,
                    "label": mode_label,
                    "result_count": len(values),
                    "win_rate_pct": (
                        len(mode_wins) / len(values) * 100
                        if values else None
                    ),
                    "average_return_pct": (
                        sum(values) / len(values)
                        if values else None
                    ),
                    "best_return_pct": max(values) if values else None,
                    "worst_return_pct": min(values) if values else None,
                }
            )

        performance_summary = {
            "has_results": bool(all_returns),
            "best_return_pct": max(all_returns) if all_returns else None,
            "worst_return_pct": min(all_returns) if all_returns else None,
            "average_return_pct": (
                sum(all_returns) / len(all_returns)
                if all_returns else None
            ),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate_pct": (
                len(wins) / len(all_returns) * 100
                if all_returns else None
            ),
            "max_tf_average_return_pct": (
                sum(max_tf_returns) / len(max_tf_returns)
                if max_tf_returns else None
            ),
            "all_split_average_return_pct": (
                sum(split_returns) / len(split_returns)
                if split_returns else None
            ),
            "average_holding_minutes": (
                sum(holding_minutes) / len(holding_minutes)
                if holding_minutes else None
            ),
            "result_count": len(all_returns),
            "best_exit": best_exit,
            "exit_timeframe_stats": exit_timeframe_stats,
            "entry_mode_stats": entry_mode_stats,
        }

        symbols.append(
            {
                "category_key": category_key,
                "category_label": category_label,
                "strategy": strategy,
                "exchange": exchange,
                "symbol": symbol,
                "low_count": len(lows),
                "high_count": len(highs),
                "completed_cycle_count": len(completed_cycles),
                "open_low_count": len(open_entries),
                "high_only_count": len(high_only),
                "completed_cycles": completed_cycles,
                "performance_summary": performance_summary,
                "open_cycle_preview": entry_preview(open_entries),
                "open_lows": [slim(r) for r in open_entries[-100:]],
                "high_only": [slim(r) for r in high_only[-100:]],
            }
        )

    # 종목명 오름차순 정렬
    symbols.sort(
        key=lambda item: (
            item["category_key"],
            item["symbol"].upper(),
        )
    )

    category_order = ["COIN", "KOREA_1Q", "US_1Q", "OTHER"]
    category_labels = {
        "COIN": "코인 · 별꽃타점",
        "KOREA_1Q": "국장 · 1Q 대형주",
        "US_1Q": "미장 · 1Q 대형주",
        "OTHER": "기타",
    }

    limited_symbols = symbols[:safe_limit]
    categories = []

    for category_key in category_order:
        category_symbols = [
            item for item in limited_symbols
            if item["category_key"] == category_key
        ]

        # 코인·국장·미장은 데이터가 0건이어도 항상 표시한다.
        if category_key == "OTHER" and not category_symbols:
            continue

        category_result_symbols = [
            item for item in category_symbols
            if item["performance_summary"]["has_results"]
        ]
        category_best_values = [
            item["performance_summary"]["best_return_pct"]
            for item in category_result_symbols
        ]
        category_average_values = [
            item["performance_summary"]["average_return_pct"]
            for item in category_result_symbols
        ]
        category_holding_values = [
            item["performance_summary"]["average_holding_minutes"]
            for item in category_result_symbols
            if item["performance_summary"]["average_holding_minutes"] is not None
        ]

        category_total_results = sum(
            item["performance_summary"]["result_count"]
            for item in category_result_symbols
        )
        category_total_wins = sum(
            item["performance_summary"]["win_count"]
            for item in category_result_symbols
        )

        category_performance = {
            "has_results": bool(category_result_symbols),
            "best_return_pct": (
                max(category_best_values)
                if category_best_values else None
            ),
            "average_return_pct": (
                sum(category_average_values) / len(category_average_values)
                if category_average_values else None
            ),
            "win_rate_pct": (
                category_total_wins / category_total_results * 100
                if category_total_results else None
            ),
            "average_holding_minutes": (
                sum(category_holding_values) / len(category_holding_values)
                if category_holding_values else None
            ),
            "result_symbol_count": len(category_result_symbols),
            "result_count": category_total_results,
            "win_count": category_total_wins,
        }

        categories.append(
            {
                "category_key": category_key,
                "category_label": category_labels[category_key],
                "anchor": category_key.lower(),
                "symbol_count": len(category_symbols),
                "completed_cycle_count": sum(
                    item["completed_cycle_count"]
                    for item in category_symbols
                ),
                "open_low_count": sum(
                    item["open_low_count"]
                    for item in category_symbols
                ),
                "signal_count": sum(
                    item["low_count"] + item["high_count"]
                    for item in category_symbols
                ),
                "performance_summary": category_performance,
                "symbols": category_symbols,
            }
        )

    return {
        "ok": True,
        "symbol_count": len(symbols),
        "categories": categories,
        "symbols": limited_symbols,
    }
