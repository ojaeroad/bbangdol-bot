"""회원 운영용 성과 분석 엔진 1차.

- 같은 전략·거래소·종목의 각 저점 신호를 개별 진입으로 본다.
- 해당 진입 이후 발생한 모든 고점 신호를 청산 후보로 연결한다.
- 하위·동일·상위 시간봉 청산을 모두 저장한다.
- 현재 1차 테스트 기준에 따라 수익인 조합만 결과 테이블에 저장한다.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import psycopg

PERFORMANCE_DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS performance_trade_pairs (
    id BIGSERIAL PRIMARY KEY,
    strategy VARCHAR(30) NOT NULL,
    exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,

    entry_signal_id BIGINT NOT NULL
        REFERENCES performance_signals(id) ON DELETE CASCADE,
    exit_signal_id BIGINT NOT NULL
        REFERENCES performance_signals(id) ON DELETE CASCADE,

    entry_signal_no VARCHAR(24),
    exit_signal_no VARCHAR(24),

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

    entry_mode VARCHAR(30) NOT NULL DEFAULT 'INDIVIDUAL',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(entry_signal_id, exit_signal_id, entry_mode)
);

CREATE INDEX IF NOT EXISTS idx_trade_pairs_symbol_entry
    ON performance_trade_pairs(strategy, symbol, entry_time);

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


def rebuild_individual_pairs() -> dict[str, Any]:
    ensure_analysis_schema()

    select_sql = """
        SELECT
            e.id, x.id,
            e.signal_no, x.signal_no,
            e.strategy,
            COALESCE(e.exchange, e.raw_exchange),
            e.symbol,
            e.received_at, x.received_at,
            e.timeframe, x.timeframe,
            e.timeframe_minutes, x.timeframe_minutes,
            e.signal_price, x.signal_price
        FROM performance_signals e
        JOIN performance_signals x
          ON x.strategy = e.strategy
         AND x.symbol = e.symbol
         AND COALESCE(x.exchange, x.raw_exchange, '')
             = COALESCE(e.exchange, e.raw_exchange, '')
         AND x.received_at > e.received_at
        WHERE e.signal_type = 'LOW'
          AND x.signal_type = 'HIGH'
          AND e.signal_price IS NOT NULL
          AND x.signal_price IS NOT NULL
        ORDER BY e.id, x.id
    """

    insert_sql = """
        INSERT INTO performance_trade_pairs (
            strategy, exchange, symbol,
            entry_signal_id, exit_signal_id,
            entry_signal_no, exit_signal_no,
            entry_time, exit_time,
            entry_timeframe, exit_timeframe,
            entry_timeframe_minutes, exit_timeframe_minutes,
            exit_tf_relation,
            entry_price, exit_price, return_pct, holding_minutes,
            entry_mode
        ) VALUES (
            %(strategy)s, %(exchange)s, %(symbol)s,
            %(entry_signal_id)s, %(exit_signal_id)s,
            %(entry_signal_no)s, %(exit_signal_no)s,
            %(entry_time)s, %(exit_time)s,
            %(entry_timeframe)s, %(exit_timeframe)s,
            %(entry_timeframe_minutes)s, %(exit_timeframe_minutes)s,
            %(exit_tf_relation)s,
            %(entry_price)s, %(exit_price)s, %(return_pct)s, %(holding_minutes)s,
            'INDIVIDUAL'
        )
        ON CONFLICT (entry_signal_id, exit_signal_id, entry_mode)
        DO NOTHING
        RETURNING id
    """

    examined = 0
    inserted = 0
    skipped_nonprofit = 0

    with _connect() as conn:
        rows = conn.execute(select_sql).fetchall()

        for row in rows:
            examined += 1
            entry_price = Decimal(row[13])
            exit_price = Decimal(row[14])

            if exit_price <= entry_price:
                skipped_nonprofit += 1
                continue

            return_pct = ((exit_price - entry_price) / entry_price) * Decimal("100")
            holding_minutes = int((row[8] - row[7]).total_seconds() // 60)

            params = {
                "entry_signal_id": row[0],
                "exit_signal_id": row[1],
                "entry_signal_no": row[2],
                "exit_signal_no": row[3],
                "strategy": row[4],
                "exchange": row[5],
                "symbol": row[6],
                "entry_time": row[7],
                "exit_time": row[8],
                "entry_timeframe": row[9],
                "exit_timeframe": row[10],
                "entry_timeframe_minutes": row[11],
                "exit_timeframe_minutes": row[12],
                "exit_tf_relation": _relation(row[11], row[12]),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "return_pct": return_pct,
                "holding_minutes": holding_minutes,
            }

            if conn.execute(insert_sql, params).fetchone():
                inserted += 1

        total = conn.execute(
            "SELECT COUNT(*) FROM performance_trade_pairs WHERE entry_mode='INDIVIDUAL'"
        ).fetchone()[0]

    return {
        "ok": True,
        "analysis": "INDIVIDUAL_LOW_TO_ALL_LATER_HIGHS",
        "examined_pairs": examined,
        "new_pairs": inserted,
        "skipped_nonprofit_pairs": skipped_nonprofit,
        "total_pairs": int(total),
    }


def analysis_summary() -> dict[str, Any]:
    ensure_analysis_schema()
    sql = """
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE exit_tf_relation='LOWER'),
            COUNT(*) FILTER (WHERE exit_tf_relation='SAME'),
            COUNT(*) FILTER (WHERE exit_tf_relation='HIGHER'),
            AVG(return_pct),
            MAX(return_pct),
            AVG(holding_minutes)
        FROM performance_trade_pairs
        WHERE entry_mode='INDIVIDUAL'
    """
    with _connect() as conn:
        row = conn.execute(sql).fetchone()

    return {
        "ok": True,
        "analysis": "INDIVIDUAL",
        "total_pairs": int(row[0] or 0),
        "lower_exit_pairs": int(row[1] or 0),
        "same_exit_pairs": int(row[2] or 0),
        "higher_exit_pairs": int(row[3] or 0),
        "average_return_pct": float(row[4]) if row[4] is not None else None,
        "maximum_return_pct": float(row[5]) if row[5] is not None else None,
        "average_holding_minutes": float(row[6]) if row[6] is not None else None,
    }


def latest_analysis_pairs(limit: int = 50) -> list[dict[str, Any]]:
    ensure_analysis_schema()
    safe_limit = max(1, min(int(limit), 200))

    sql = """
        SELECT
            id, strategy, exchange, symbol,
            entry_signal_no, exit_signal_no,
            entry_timeframe, exit_timeframe, exit_tf_relation,
            entry_price, exit_price, return_pct, holding_minutes,
            entry_time, exit_time
        FROM performance_trade_pairs
        WHERE entry_mode='INDIVIDUAL'
        ORDER BY id DESC
        LIMIT %s
    """

    with _connect() as conn:
        rows = conn.execute(sql, (safe_limit,)).fetchall()

    return [
        {
            "pair_id": r[0],
            "strategy": r[1],
            "exchange": r[2],
            "symbol": r[3],
            "entry_signal_no": r[4],
            "exit_signal_no": r[5],
            "entry_timeframe": r[6],
            "exit_timeframe": r[7],
            "exit_tf_relation": r[8],
            "entry_price": str(r[9]),
            "exit_price": str(r[10]),
            "return_pct": float(r[11]),
            "holding_minutes": int(r[12]),
            "entry_time": r[13].isoformat(),
            "exit_time": r[14].isoformat(),
        }
        for r in rows
    ]
