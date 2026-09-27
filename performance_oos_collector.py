from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


log = logging.getLogger(__name__)

DB_URL = (
    os.environ.get("PERFORMANCE_DATABASE_URL")
    or os.environ.get("DATABASE_URL")
)

MASTER = "tajeomON_master_v59.3"

V15B_SHA256 = (
    "d0d8043fa25e78f4ae05de562368c928"
    "45800ac71e54277740c44084d7682de1"
)

V15C_SHA256 = (
    "de2507010b8c72f3f5255fedc5d670e4"
    "38a3d2d80d860e7dc6166e41c86ab53e"
)

OOS_START = datetime(
    2026, 10, 1, 0, 0, 0,
    tzinfo=timezone.utc,
)

OOS_END_EXCLUSIVE = datetime(
    2026, 11, 1, 0, 0, 0,
    tzinfo=timezone.utc,
)

# Install/preflight period. PREP rows are never part of October OOS scoring.
PREP_START = datetime(
    2026, 9, 27, 0, 0, 0,
    tzinfo=timezone.utc,
)

OOS_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "SUIUSDT",
    "LINKUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "ONDOUSDT",
}

_SCHEMA_READY = False


def _connect():
    if not DB_URL:
        raise RuntimeError(
            "PERFORMANCE_DATABASE_URL / DATABASE_URL is missing"
        )

    return psycopg.connect(
        DB_URL,
        connect_timeout=5,
    )


def ensure_oos_schema() -> None:
    global _SCHEMA_READY

    if _SCHEMA_READY:
        return

    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_oos_freeze_registry (
                freeze_key VARCHAR(80) PRIMARY KEY,
                master_version VARCHAR(80) NOT NULL,
                v15b_sha256 VARCHAR(64) NOT NULL,
                v15c_sha256 VARCHAR(64) NOT NULL,
                oos_start TIMESTAMPTZ NOT NULL,
                oos_end_exclusive TIMESTAMPTZ NOT NULL,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_oos_feed_health (
                id BIGSERIAL PRIMARY KEY,
                phase VARCHAR(16) NOT NULL,
                source VARCHAR(32) NOT NULL DEFAULT 'TRADINGVIEW',
                symbol VARCHAR(32) NOT NULL,
                interval_minutes INTEGER NOT NULL,
                bar_time TIMESTAMPTZ NOT NULL,
                bar_close_time TIMESTAMPTZ,
                first_received_at TIMESTAMPTZ NOT NULL,
                last_received_at TIMESTAMPTZ NOT NULL,
                receive_count INTEGER NOT NULL DEFAULT 1,
                first_delay_seconds NUMERIC,
                last_delay_seconds NUMERIC,
                exchange VARCHAR(32),
                raw_exchange VARCHAR(32),
                event_type VARCHAR(64),
                v15b_sha256 VARCHAR(64) NOT NULL,
                v15c_sha256 VARCHAR(64) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (
                    source,
                    symbol,
                    interval_minutes,
                    bar_time
                )
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_oos_feed_health_phase_time
            ON performance_oos_feed_health(phase, bar_time)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_oos_feed_health_symbol_time
            ON performance_oos_feed_health(symbol, bar_time)
            """
        )

        details = {
            "purpose": (
                "TAJEOMON October 2026 OOS "
                "TradingView inbound 1m feed audit"
            ),
            "symbols": sorted(OOS_SYMBOLS),
            "prediction_rules": "V15B frozen",
            "outcome_contract": "V15C frozen",
            "raw_candle_retention_changed": False,
            "production_signal_logic_changed": False,
        }

        conn.execute(
            """
            INSERT INTO performance_oos_freeze_registry(
                freeze_key,
                master_version,
                v15b_sha256,
                v15c_sha256,
                oos_start,
                oos_end_exclusive,
                details
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(freeze_key) DO NOTHING
            """,
            (
                "TAJEOMON_V15_OOS_202610",
                MASTER,
                V15B_SHA256,
                V15C_SHA256,
                OOS_START,
                OOS_END_EXCLUSIVE,
                Jsonb(details),
            ),
        )

        frozen = conn.execute(
            """
            SELECT
                master_version,
                v15b_sha256,
                v15c_sha256,
                oos_start,
                oos_end_exclusive
            FROM performance_oos_freeze_registry
            WHERE freeze_key=%s
            """,
            ("TAJEOMON_V15_OOS_202610",),
        ).fetchone()

        expected = (
            MASTER,
            V15B_SHA256,
            V15C_SHA256,
            OOS_START,
            OOS_END_EXCLUSIVE,
        )

        if frozen != expected:
            raise RuntimeError(
                "OOS FREEZE REGISTRY MISMATCH: "
                f"stored={frozen!r} expected={expected!r}"
            )

    _SCHEMA_READY = True


def _phase_for(bar_time: datetime) -> str | None:
    if bar_time < PREP_START:
        return None

    if bar_time < OOS_START:
        return "PREP"

    if bar_time < OOS_END_EXCLUSIVE:
        return "OOS"

    return None


def record_oos_feed_event(
    *,
    payload: dict[str, Any],
    symbol: str,
    interval_minutes: int,
    bar_time: datetime,
    bar_close_time: datetime | None,
) -> bool:
    symbol = str(symbol or "").strip().upper()

    # OOS feed health uses only the nine frozen crypto symbols and 1m feed.
    if symbol not in OOS_SYMBOLS:
        return False

    if int(interval_minutes) != 1:
        return False

    if bar_time.tzinfo is None:
        bar_time = bar_time.replace(tzinfo=timezone.utc)

    phase = _phase_for(bar_time)
    if phase is None:
        return False

    received_at = datetime.now(timezone.utc)

    expected_close = (
        bar_close_time
        if bar_close_time is not None
        else bar_time + timedelta(minutes=1)
    )

    if expected_close.tzinfo is None:
        expected_close = expected_close.replace(tzinfo=timezone.utc)

    delay_seconds = (received_at - expected_close).total_seconds()

    ensure_oos_schema()

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO performance_oos_feed_health(
                phase,
                source,
                symbol,
                interval_minutes,
                bar_time,
                bar_close_time,
                first_received_at,
                last_received_at,
                receive_count,
                first_delay_seconds,
                last_delay_seconds,
                exchange,
                raw_exchange,
                event_type,
                v15b_sha256,
                v15c_sha256
            )
            VALUES(
                %s,
                'TRADINGVIEW',
                %s,%s,%s,%s,
                %s,%s,
                1,
                %s,%s,
                %s,%s,%s,
                %s,%s
            )
            ON CONFLICT(
                source,
                symbol,
                interval_minutes,
                bar_time
            )
            DO UPDATE SET
                last_received_at=GREATEST(
                    performance_oos_feed_health.last_received_at,
                    EXCLUDED.last_received_at
                ),
                receive_count=performance_oos_feed_health.receive_count + 1,
                last_delay_seconds=EXCLUDED.last_delay_seconds,
                bar_close_time=COALESCE(
                    EXCLUDED.bar_close_time,
                    performance_oos_feed_health.bar_close_time
                ),
                exchange=COALESCE(
                    EXCLUDED.exchange,
                    performance_oos_feed_health.exchange
                ),
                raw_exchange=COALESCE(
                    EXCLUDED.raw_exchange,
                    performance_oos_feed_health.raw_exchange
                ),
                event_type=COALESCE(
                    EXCLUDED.event_type,
                    performance_oos_feed_health.event_type
                )
            """,
            (
                phase,
                symbol,
                int(interval_minutes),
                bar_time,
                bar_close_time,
                received_at,
                received_at,
                delay_seconds,
                delay_seconds,
                str(payload.get("exchange", "")).strip() or None,
                str(payload.get("raw_exchange", "")).strip() or None,
                str(payload.get("event_type", "")).strip() or None,
                V15B_SHA256,
                V15C_SHA256,
            ),
        )

    return True


def record_oos_feed_event_safely(**kwargs: Any) -> bool:
    try:
        return record_oos_feed_event(**kwargs)
    except Exception:
        # Never propagate research collector failures into existing signal/candle flow.
        log.exception("TAJEOMON OOS feed heartbeat save failed")
        return False


def print_status() -> None:
    ensure_oos_schema()

    print("=" * 104)
    print("TAJEOMON V15D-C OOS FEED STATUS")
    print("=" * 104)

    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
                freeze_key,
                master_version,
                v15b_sha256,
                v15c_sha256,
                oos_start,
                oos_end_exclusive,
                created_at
            FROM performance_oos_freeze_registry
            WHERE freeze_key=%s
            """,
            ("TAJEOMON_V15_OOS_202610",),
        ).fetchone()

        print()
        print("[FREEZE REGISTRY]")
        print(row)

        rows = conn.execute(
            """
            SELECT
                phase,
                symbol,
                COUNT(*) AS unique_1m_bars,
                MIN(bar_time),
                MAX(bar_time),
                MAX(last_received_at),
                SUM(receive_count)
            FROM performance_oos_feed_health
            GROUP BY phase, symbol
            ORDER BY phase, symbol
            """
        ).fetchall()

        print()
        print("[FEED HEALTH]")
        if not rows:
            print("NO HEARTBEAT ROWS YET (schema/freeze registry ready)")
        else:
            for row in rows:
                print(row)

        mismatch = conn.execute(
            """
            SELECT COUNT(*)
            FROM performance_oos_feed_health
            WHERE v15b_sha256<>%s
               OR v15c_sha256<>%s
            """,
            (V15B_SHA256, V15C_SHA256),
        ).fetchone()[0]

        print()
        print("HASH MISMATCH ROWS =", mismatch)

    print()
    print("MASTER =", MASTER)
    print("V15B =", V15B_SHA256)
    print("V15C =", V15C_SHA256)
    print("=" * 104)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    ensure_oos_schema()

    if args.status:
        print_status()
    else:
        print("V15D-C schema/freeze registry ready")


if __name__ == "__main__":
    main()
