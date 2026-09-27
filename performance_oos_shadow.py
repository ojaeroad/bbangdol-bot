from __future__ import annotations

"""TAJEOMON October-2026 OOS Shadow evaluator.

Research-only module. It never changes Production signal decisions, Telegram delivery,
or automatic trading. It records frozen V15B/V15C Shadow states and exposes pending
research events for the app-only FCM dispatcher in app.py.
"""

import hashlib
import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from performance_oos_collector import (
    MASTER,
    OOS_END_EXCLUSIVE,
    OOS_START,
    OOS_SYMBOLS,
    PREP_START,
    V15B_SHA256,
    V15C_SHA256,
)
from server_signal_engine import (
    TF_MINUTES,
    _fetch_klines,
    _latest_metric,
    _rows_at_evaluation,
)

log = logging.getLogger(__name__)

DB_URL = os.environ.get("PERFORMANCE_DATABASE_URL") or os.environ.get("DATABASE_URL")

FREEZE_KEY = "TAJEOMON_V15_OOS_202610"
SHADOW_SCHEMA_VERSION = 1

# V15B frozen prediction rules. LOW 1h->4h is observation-only.
FROZEN_PAIRS: dict[tuple[str, str, str], dict[str, Any]] = {
    ("LOW", "30m", "1h"): {
        "landmark_minutes": 10,
        "at_least": ("1h", "4h", "6h", "12h", "1d", "1w"),
        "rule": "PERSISTENCE_ON + TARGET_1H_CLUSTER_WIDEN",
    },
    ("HIGH", "30m", "1h"): {
        "landmark_minutes": 10,
        "at_least": ("1h", "4h", "6h", "12h", "1d", "1w"),
        "rule": "PERSISTENCE_ON + TARGET_1H_CLUSTER_TIGHTEN",
    },
    ("LOW", "1h", "4h"): {
        "landmark_minutes": 20,
        "at_least": ("4h", "6h", "12h", "1d", "1w"),
        "rule": "OBSERVE_ONLY_UNRESOLVED",
    },
    ("HIGH", "1h", "4h"): {
        "landmark_minutes": 20,
        "at_least": ("4h", "6h", "12h", "1d", "1w"),
        "rule": (
            "PERSISTENCE_ON + TARGET_4H_TIGHTEN + GATE_2H_TIGHTEN + "
            "GATE_ALIGNMENT_FAVORABLE_OR_MIXED; GATE_TIGHTEN_OPPOSITE=BLOCK"
        ),
    },
}

VISIBLE_STATES = {
    "SOURCE_CONFIRMED",
    "CONTEXT_BLOCK",
    "IMMINENT_CANDIDATE",
    "TARGET_CONFIRMED",
    "CENSORED",
    "NO_TARGET_OBSERVED",
}

_SCHEMA_READY = False


def _connect() -> psycopg.Connection:
    if not DB_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL / DATABASE_URL is missing")
    return psycopg.connect(DB_URL, autocommit=True, connect_timeout=5)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _phase_for(value: datetime) -> str | None:
    value = _utc(value)
    if value < PREP_START:
        return None
    if value < OOS_START:
        return "PREP"
    if value < OOS_END_EXCLUSIVE:
        return "OOS"
    return None


def _tf_delta(tf: str) -> timedelta:
    minutes = int(TF_MINUTES.get(tf, 0) or 0)
    if minutes <= 0:
        raise ValueError(f"unsupported timeframe {tf}")
    return timedelta(minutes=minutes)


def _floor_source_candle(value: datetime, tf: str) -> datetime:
    value = _utc(value)
    minutes = int(TF_MINUTES[tf])
    epoch_min = int(value.timestamp()) // 60
    bucket = (epoch_min // minutes) * minutes
    return datetime.fromtimestamp(bucket * 60, tz=timezone.utc)


def _snapshot_time(payload: dict[str, Any]) -> datetime:
    raw = payload.get("signal_time")
    if raw is not None:
        try:
            return datetime.fromtimestamp(int(raw) / 1000.0, tz=timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)


def _occurrence_key(symbol: str, direction: str, source_tf: str, target_tf: str, candle_at: datetime) -> str:
    source = f"{symbol}|{direction}|{source_tf}|{target_tf}|{_utc(candle_at).isoformat()}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _event_key(occurrence_key: str, state: str, event_at: datetime) -> str:
    source = f"{occurrence_key}|{state}|{_utc(event_at).isoformat()}|{V15B_SHA256}|{V15C_SHA256}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def ensure_shadow_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_oos_shadow_occurrences (
                occurrence_key VARCHAR(64) PRIMARY KEY,
                phase VARCHAR(16) NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                direction VARCHAR(10) NOT NULL,
                source_tf VARCHAR(10) NOT NULL,
                target_tf VARCHAR(10) NOT NULL,
                source_candle_at TIMESTAMPTZ NOT NULL,
                first_snapshot_at TIMESTAMPTZ NOT NULL,
                landmark_at TIMESTAMPTZ NOT NULL,
                left_truncated BOOLEAN NOT NULL DEFAULT FALSE,
                pre_landmark_target BOOLEAN NOT NULL DEFAULT FALSE,
                landmark_done BOOLEAN NOT NULL DEFAULT FALSE,
                persistence_on BOOLEAN,
                target_cluster_transition VARCHAR(16),
                gate_cluster_transition VARCHAR(16),
                gate_alignment VARCHAR(16),
                context_class VARCHAR(24),
                imminent BOOLEAN,
                natural_end_at TIMESTAMPTZ,
                first_at_least_target_at TIMESTAMPTZ,
                final_state VARCHAR(32),
                source_all_timeframes JSONB,
                source_payload JSONB NOT NULL,
                landmark_metrics JSONB,
                v15b_sha256 VARCHAR(64) NOT NULL,
                v15c_sha256 VARCHAR(64) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_oos_shadow_occ_due
            ON performance_oos_shadow_occurrences(landmark_done, landmark_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_oos_shadow_occ_symbol_source
            ON performance_oos_shadow_occurrences(symbol, direction, source_tf, source_candle_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_oos_shadow_events (
                id BIGSERIAL PRIMARY KEY,
                event_key VARCHAR(64) NOT NULL UNIQUE,
                occurrence_key VARCHAR(64),
                phase VARCHAR(16) NOT NULL,
                state VARCHAR(32) NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                direction VARCHAR(10),
                source_tf VARCHAR(10),
                target_tf VARCHAR(10),
                event_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                delivery_status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
                claimed_at TIMESTAMPTZ,
                delivered_at TIMESTAMPTZ,
                delivery_detail JSONB,
                v15b_sha256 VARCHAR(64) NOT NULL,
                v15c_sha256 VARCHAR(64) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_oos_shadow_events_delivery
            ON performance_oos_shadow_events(delivery_status, event_at)
            """
        )

    _SCHEMA_READY = True


def _queue_event(
    conn: psycopg.Connection,
    *,
    occurrence_key: str | None,
    phase: str,
    state: str,
    symbol: str,
    direction: str,
    source_tf: str,
    target_tf: str,
    event_at: datetime,
    payload: dict[str, Any],
) -> None:
    event_at = _utc(event_at)
    event_key = _event_key(occurrence_key or symbol, state, event_at)
    body = {
        **payload,
        "alert_kind": "SHADOW_OOS",
        "event_type": "SHADOW_OOS",
        "research_state": state,
        "phase": phase,
        "symbol": symbol,
        "direction": direction,
        "side": "BUY" if direction == "LOW" else "SELL",
        "source_tf": source_tf,
        "target_tf": target_tf,
        "timeframe": source_tf,
        "occurred_at": event_at.isoformat(),
        "master_version": MASTER,
        "v15b_sha256": V15B_SHA256,
        "v15c_sha256": V15C_SHA256,
        "shadow_schema_version": SHADOW_SCHEMA_VERSION,
    }
    conn.execute(
        """
        INSERT INTO performance_oos_shadow_events(
            event_key, occurrence_key, phase, state, symbol, direction,
            source_tf, target_tf, event_at, payload,
            v15b_sha256, v15c_sha256
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(event_key) DO NOTHING
        """,
        (
            event_key,
            occurrence_key,
            phase,
            state,
            symbol,
            direction,
            source_tf,
            target_tf,
            event_at,
            Jsonb(body),
            V15B_SHA256,
            V15C_SHA256,
        ),
    )


def _source_metrics_from_payload(payload: dict[str, Any], tf: str) -> dict[str, Any]:
    all_tf = payload.get("all_timeframes")
    if isinstance(all_tf, dict) and isinstance(all_tf.get(tf), dict):
        return dict(all_tf[tf])
    if tf == str(payload.get("source_timeframe", "")) and isinstance(payload.get("source_metrics"), dict):
        return dict(payload["source_metrics"])
    if tf == str(payload.get("target_timeframe", "")) and isinstance(payload.get("target_metrics"), dict):
        return dict(payload["target_metrics"])
    return {}


def _metric_float(metrics: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = metrics.get(key)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            return float(value)
    return None


def _cluster_pct(metrics: dict[str, Any]) -> float | None:
    direct = _metric_float(metrics, "sma20_60_120_cluster_pct")
    if direct is not None:
        return direct
    ma20 = _metric_float(metrics, "sma20")
    ma60 = _metric_float(metrics, "sma60")
    ma120 = _metric_float(metrics, "sma120")
    price = _metric_float(metrics, "price", "close")
    if None in (ma20, ma60, ma120, price) or abs(float(price)) <= 1e-15:
        return None
    return (max(ma20, ma60, ma120) - min(ma20, ma60, ma120)) / abs(price) * 100.0


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return sum(values[-length:]) / float(length)


def _research_metrics(rows: list[dict[str, float | int]], evaluation_ms: int) -> dict[str, Any]:
    basic = _latest_metric(rows, evaluation_time_ms=evaluation_ms)
    closes = [float(row["close"]) for row in rows]
    ma20 = _sma(closes, 20)
    ma60 = _sma(closes, 60)
    ma120 = _sma(closes, 120)
    if None in (ma20, ma60, ma120):
        raise RuntimeError("MA warm-up incomplete")
    price = float(closes[-1])
    cluster = (max(ma20, ma60, ma120) - min(ma20, ma60, ma120)) / abs(price) * 100.0
    return {
        **basic,
        "price": price,
        "sma20": ma20,
        "sma60": ma60,
        "sma120": ma120,
        "sma20_60_120_cluster_pct": cluster,
    }


def _evaluate_landmark(symbol: str, evaluation_at: datetime, tfs: set[str]) -> dict[str, dict[str, Any]]:
    evaluation_at = _utc(evaluation_at)
    evaluation_ms = int(evaluation_at.timestamp() * 1000)
    one_minute_rows = _fetch_klines(symbol, "1m", limit=70, end_time_ms=evaluation_ms - 1)
    one_hour_rows = _fetch_klines(symbol, "1h", limit=300, end_time_ms=evaluation_ms - 1)

    out: dict[str, dict[str, Any]] = {}
    for tf in sorted(tfs, key=lambda value: int(TF_MINUTES[value])):
        rows = _rows_at_evaluation(
            symbol,
            tf,
            evaluation_time_ms=evaluation_ms,
            one_minute_rows=one_minute_rows,
            one_hour_rows=one_hour_rows,
        )
        out[tf] = _research_metrics(rows, evaluation_ms)
    return out


def _transition(source_value: float | None, landmark_value: float | None) -> str:
    if source_value is None or landmark_value is None:
        return "NA"
    eps = 1e-12
    if landmark_value < source_value - eps:
        return "TIGHTEN"
    if landmark_value > source_value + eps:
        return "WIDEN"
    return "FLAT"


def _alignment(metrics: dict[str, Any], direction: str) -> str:
    ma20 = _metric_float(metrics, "sma20")
    ma60 = _metric_float(metrics, "sma60")
    ma120 = _metric_float(metrics, "sma120")
    if None in (ma20, ma60, ma120):
        return "NA"
    bullish = ma20 >= ma60 >= ma120
    bearish = ma20 <= ma60 <= ma120
    if direction == "HIGH":
        if bearish:
            return "FAVORABLE"
        if bullish:
            return "OPPOSITE"
    else:
        if bullish:
            return "FAVORABLE"
        if bearish:
            return "OPPOSITE"
    return "MIXED"


def _persistence_at_landmark(
    conn: psycopg.Connection,
    *,
    symbol: str,
    direction: str,
    source_tf: str,
    landmark_at: datetime,
) -> bool:
    """Frozen V13.1/V15B persistence: repeated source alert exists in landmark minute.

    This is intentionally NOT a replayed RSI/Stoch classifier. Historical research
    defined persistence by exact minute-match of actual source signal times.
    """
    minute_start = _utc(landmark_at).replace(second=0, microsecond=0)
    minute_end = minute_start + timedelta(minutes=1)
    row = conn.execute(
        """
        SELECT 1
        FROM performance_signals
        WHERE symbol=%s
          AND signal_type=%s
          AND timeframe=%s
          AND received_at >= %s
          AND received_at < %s
        LIMIT 1
        """,
        (symbol, direction, source_tf, minute_start, minute_end),
    ).fetchone()
    return bool(row)


def _has_signal_between(
    conn: psycopg.Connection,
    *,
    symbol: str,
    direction: str,
    timeframes: tuple[str, ...],
    start_at: datetime,
    end_at: datetime,
) -> datetime | None:
    row = conn.execute(
        """
        SELECT MIN(received_at)
        FROM performance_signals
        WHERE symbol=%s
          AND signal_type=%s
          AND timeframe = ANY(%s)
          AND received_at >= %s
          AND received_at < %s
        """,
        (symbol, direction, list(timeframes), _utc(start_at), _utc(end_at)),
    ).fetchone()
    return row[0] if row and row[0] else None


def register_prediction_snapshot(payload: dict[str, Any]) -> bool:
    event_type = str(payload.get("event_type", "")).strip().upper()
    if event_type != "PREDICTION_SNAPSHOT_1Q":
        return False

    symbol = str(payload.get("symbol", "")).strip().upper()
    symbol = {"SOLUSDT.P": "SOLUSDT", "SUIUSDT.P": "SUIUSDT"}.get(symbol, symbol)
    direction = str(payload.get("direction", "LOW") or "LOW").strip().upper()
    source_tf = str(payload.get("source_timeframe", "")).strip()
    target_tf = str(payload.get("target_timeframe", "")).strip()
    pair = (direction, source_tf, target_tf)
    if symbol not in OOS_SYMBOLS or pair not in FROZEN_PAIRS:
        return False

    first_at = _snapshot_time(payload)
    phase = _phase_for(first_at)
    if phase is None:
        return False

    source_candle_at = _floor_source_candle(first_at, source_tf)
    landmark_at = first_at + timedelta(minutes=int(FROZEN_PAIRS[pair]["landmark_minutes"]))
    occurrence_key = _occurrence_key(symbol, direction, source_tf, target_tf, source_candle_at)

    ensure_shadow_schema()
    with _connect() as conn:
        existing = conn.execute(
            "SELECT 1 FROM performance_oos_shadow_occurrences WHERE occurrence_key=%s",
            (occurrence_key,),
        ).fetchone()
        if existing:
            return False

        left_truncated = False
        if phase == "OOS":
            prev_candle = source_candle_at - _tf_delta(source_tf)
            prev = conn.execute(
                """
                SELECT 1
                FROM performance_prediction_snapshots
                WHERE symbol=%s
                  AND COALESCE(direction,'LOW')=%s
                  AND source_timeframe=%s
                  AND target_timeframe=%s
                  AND snapshot_at >= %s
                  AND snapshot_at < %s
                LIMIT 1
                """,
                (symbol, direction, source_tf, target_tf, prev_candle, source_candle_at),
            ).fetchone()
            left_truncated = bool(prev)

        all_tf = payload.get("all_timeframes") if isinstance(payload.get("all_timeframes"), dict) else {}
        conn.execute(
            """
            INSERT INTO performance_oos_shadow_occurrences(
                occurrence_key, phase, symbol, direction, source_tf, target_tf,
                source_candle_at, first_snapshot_at, landmark_at, left_truncated,
                source_all_timeframes, source_payload, v15b_sha256, v15c_sha256
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                occurrence_key,
                phase,
                symbol,
                direction,
                source_tf,
                target_tf,
                source_candle_at,
                first_at,
                landmark_at,
                left_truncated,
                Jsonb(all_tf),
                Jsonb(payload),
                V15B_SHA256,
                V15C_SHA256,
            ),
        )

        _queue_event(
            conn,
            occurrence_key=occurrence_key,
            phase=phase,
            state="SOURCE_CONFIRMED",
            symbol=symbol,
            direction=direction,
            source_tf=source_tf,
            target_tf=target_tf,
            event_at=first_at,
            payload={
                "left_truncated": left_truncated,
                "rule": FROZEN_PAIRS[pair]["rule"],
            },
        )

    return True


def register_prediction_snapshot_safely(payload: dict[str, Any]) -> bool:
    try:
        return register_prediction_snapshot(dict(payload or {}))
    except Exception:
        log.exception("TAJEOMON Shadow source registration failed")
        return False


def _landmark_context(conn: psycopg.Connection, row: dict[str, Any]) -> dict[str, Any]:
    symbol = row["symbol"]
    direction = row["direction"]
    source_tf = row["source_tf"]
    target_tf = row["target_tf"]
    pair = (direction, source_tf, target_tf)
    evaluation_at = row["landmark_at"]
    gate_tf = "2h" if pair == ("HIGH", "1h", "4h") else None

    tfs = {target_tf}
    if gate_tf:
        tfs.add(gate_tf)
    landmark_metrics = _evaluate_landmark(symbol, evaluation_at, tfs)

    source_payload = row["source_payload"] if isinstance(row["source_payload"], dict) else {}
    source_target_metrics = _source_metrics_from_payload(source_payload, target_tf)
    target_source_cluster = _cluster_pct(source_target_metrics)
    target_landmark_cluster = _cluster_pct(landmark_metrics[target_tf])
    target_transition = _transition(target_source_cluster, target_landmark_cluster)

    persistence_on = _persistence_at_landmark(
        conn,
        symbol=symbol,
        direction=direction,
        source_tf=source_tf,
        landmark_at=evaluation_at,
    )
    gate_transition = None
    gate_alignment = None
    gate_source_cluster = None
    gate_landmark_cluster = None
    if gate_tf:
        source_gate_metrics = _source_metrics_from_payload(source_payload, gate_tf)
        gate_source_cluster = _cluster_pct(source_gate_metrics)
        gate_landmark_cluster = _cluster_pct(landmark_metrics[gate_tf])
        gate_transition = _transition(gate_source_cluster, gate_landmark_cluster)
        gate_alignment = _alignment(landmark_metrics[gate_tf], direction)

    context_class = "OTHER"
    imminent = False
    if pair == ("LOW", "30m", "1h"):
        if persistence_on and target_transition == "WIDEN":
            context_class = "SUPPORT"
            imminent = True
    elif pair == ("HIGH", "30m", "1h"):
        if persistence_on and target_transition == "TIGHTEN":
            context_class = "SUPPORT"
            imminent = True
    elif pair == ("LOW", "1h", "4h"):
        context_class = "OBSERVE_ONLY"
    elif pair == ("HIGH", "1h", "4h"):
        if gate_transition == "TIGHTEN" and gate_alignment == "OPPOSITE":
            context_class = "BLOCK"
        elif (
            persistence_on
            and target_transition == "TIGHTEN"
            and gate_transition == "TIGHTEN"
            and gate_alignment in {"FAVORABLE", "MIXED"}
        ):
            context_class = "SUPPORT"
            imminent = True

    return {
        "persistence_on": persistence_on,
        "target_cluster_transition": target_transition,
        "target_cluster_source": target_source_cluster,
        "target_cluster_landmark": target_landmark_cluster,
        "gate_cluster_transition": gate_transition,
        "gate_cluster_source": gate_source_cluster,
        "gate_cluster_landmark": gate_landmark_cluster,
        "gate_alignment": gate_alignment,
        "context_class": context_class,
        "imminent": imminent,
        "landmark_metrics": landmark_metrics,
    }


def _dict_row(columns: list[str], values: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(columns, values))


def _natural_end(conn: psycopg.Connection, row: dict[str, Any], now: datetime) -> datetime | None:
    source_candle_at = row["source_candle_at"]
    next_candle = source_candle_at + _tf_delta(row["source_tf"])
    next_row = conn.execute(
        """
        SELECT first_snapshot_at
        FROM performance_oos_shadow_occurrences
        WHERE symbol=%s
          AND direction=%s
          AND source_tf=%s
          AND target_tf=%s
          AND source_candle_at=%s
        ORDER BY first_snapshot_at
        LIMIT 1
        """,
        (row["symbol"], row["direction"], row["source_tf"], row["target_tf"], next_candle),
    ).fetchone()
    if next_row and next_row[0]:
        return next_row[0]
    if _utc(now) >= next_candle:
        return next_candle
    return None


def _feed_complete(conn: psycopg.Connection, symbol: str, start_at: datetime, end_at: datetime) -> tuple[bool, int, int]:
    start_at = _utc(start_at).replace(second=0, microsecond=0)
    end_at = _utc(end_at).replace(second=0, microsecond=0)
    expected = max(0, int((end_at - start_at).total_seconds() // 60))
    if expected <= 0:
        return True, 0, 0
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT bar_time)
        FROM performance_oos_feed_health
        WHERE symbol=%s
          AND interval_minutes=1
          AND bar_time >= %s
          AND bar_time < %s
          AND v15b_sha256=%s
          AND v15c_sha256=%s
        """,
        (symbol, start_at, end_at, V15B_SHA256, V15C_SHA256),
    ).fetchone()
    actual = int(row[0] or 0) if row else 0
    return actual >= expected, actual, expected


def scan_shadow(now: datetime | None = None, limit: int = 40) -> dict[str, int]:
    now = _utc(now or datetime.now(timezone.utc))
    ensure_shadow_schema()
    counts = {"landmarks": 0, "targets": 0, "finalized": 0, "errors": 0}

    columns = [
        "occurrence_key", "phase", "symbol", "direction", "source_tf", "target_tf",
        "source_candle_at", "first_snapshot_at", "landmark_at", "left_truncated",
        "pre_landmark_target", "landmark_done", "persistence_on",
        "target_cluster_transition", "gate_cluster_transition", "gate_alignment",
        "context_class", "imminent", "natural_end_at", "first_at_least_target_at",
        "final_state", "source_all_timeframes", "source_payload", "landmark_metrics",
    ]

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM performance_oos_shadow_occurrences
            WHERE landmark_done=FALSE
              AND landmark_at <= %s
              AND phase IN ('PREP','OOS')
            ORDER BY landmark_at
            LIMIT %s
            """,
            (now, max(1, min(int(limit), 200))),
        ).fetchall()

        for values in rows:
            row = _dict_row(columns, values)
            pair = (row["direction"], row["source_tf"], row["target_tf"])
            try:
                pre_target = _has_signal_between(
                    conn,
                    symbol=row["symbol"],
                    direction=row["direction"],
                    timeframes=FROZEN_PAIRS[pair]["at_least"],
                    start_at=row["first_snapshot_at"],
                    end_at=row["landmark_at"],
                )
                if pre_target:
                    conn.execute(
                        """
                        UPDATE performance_oos_shadow_occurrences
                        SET pre_landmark_target=TRUE, landmark_done=TRUE,
                            context_class='PRE_LANDMARK_TARGET', updated_at=NOW()
                        WHERE occurrence_key=%s
                        """,
                        (row["occurrence_key"],),
                    )
                    counts["landmarks"] += 1
                    continue

                context = _landmark_context(conn, row)
                conn.execute(
                    """
                    UPDATE performance_oos_shadow_occurrences
                    SET landmark_done=TRUE,
                        persistence_on=%s,
                        target_cluster_transition=%s,
                        gate_cluster_transition=%s,
                        gate_alignment=%s,
                        context_class=%s,
                        imminent=%s,
                        landmark_metrics=%s,
                        updated_at=NOW()
                    WHERE occurrence_key=%s
                    """,
                    (
                        context["persistence_on"],
                        context["target_cluster_transition"],
                        context["gate_cluster_transition"],
                        context["gate_alignment"],
                        context["context_class"],
                        context["imminent"],
                        Jsonb(context),
                        row["occurrence_key"],
                    ),
                )

                common_payload = {
                    "persistence_on": context["persistence_on"],
                    "target_cluster_transition": context["target_cluster_transition"],
                    "gate_cluster_transition": context["gate_cluster_transition"],
                    "gate_alignment": context["gate_alignment"],
                    "context_class": context["context_class"],
                    "left_truncated": bool(row["left_truncated"]),
                    "rule": FROZEN_PAIRS[pair]["rule"],
                }
                if context["context_class"] == "BLOCK":
                    _queue_event(
                        conn,
                        occurrence_key=row["occurrence_key"],
                        phase=row["phase"],
                        state="CONTEXT_BLOCK",
                        symbol=row["symbol"],
                        direction=row["direction"],
                        source_tf=row["source_tf"],
                        target_tf=row["target_tf"],
                        event_at=row["landmark_at"],
                        payload=common_payload,
                    )
                elif context["imminent"]:
                    _queue_event(
                        conn,
                        occurrence_key=row["occurrence_key"],
                        phase=row["phase"],
                        state="IMMINENT_CANDIDATE",
                        symbol=row["symbol"],
                        direction=row["direction"],
                        source_tf=row["source_tf"],
                        target_tf=row["target_tf"],
                        event_at=row["landmark_at"],
                        payload=common_payload,
                    )
                counts["landmarks"] += 1
            except Exception:
                counts["errors"] += 1
                log.exception(
                    "TAJEOMON Shadow landmark evaluation failed occurrence=%s",
                    row["occurrence_key"],
                )

        open_rows = conn.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM performance_oos_shadow_occurrences
            WHERE landmark_done=TRUE
              AND pre_landmark_target=FALSE
              AND final_state IS NULL
              AND phase IN ('PREP','OOS')
            ORDER BY first_snapshot_at
            LIMIT %s
            """,
            (max(1, min(int(limit) * 3, 400)),),
        ).fetchall()

        for values in open_rows:
            row = _dict_row(columns, values)
            pair = (row["direction"], row["source_tf"], row["target_tf"])
            natural_end = _natural_end(conn, row, now)
            search_end = min(natural_end or now, now)
            if row["phase"] == "OOS":
                search_end = min(search_end, OOS_END_EXCLUSIVE)
            if search_end > row["landmark_at"]:
                target_at = _has_signal_between(
                    conn,
                    symbol=row["symbol"],
                    direction=row["direction"],
                    timeframes=FROZEN_PAIRS[pair]["at_least"],
                    start_at=row["landmark_at"],
                    end_at=search_end + timedelta(microseconds=1),
                )
            else:
                target_at = None

            if target_at:
                conn.execute(
                    """
                    UPDATE performance_oos_shadow_occurrences
                    SET first_at_least_target_at=%s,
                        final_state='TARGET_CONFIRMED',
                        natural_end_at=COALESCE(natural_end_at,%s),
                        updated_at=NOW()
                    WHERE occurrence_key=%s
                    """,
                    (target_at, natural_end, row["occurrence_key"]),
                )
                _queue_event(
                    conn,
                    occurrence_key=row["occurrence_key"],
                    phase=row["phase"],
                    state="TARGET_CONFIRMED",
                    symbol=row["symbol"],
                    direction=row["direction"],
                    source_tf=row["source_tf"],
                    target_tf=row["target_tf"],
                    event_at=target_at,
                    payload={
                        "persistence_on": row["persistence_on"],
                        "target_cluster_transition": row["target_cluster_transition"],
                        "gate_cluster_transition": row["gate_cluster_transition"],
                        "gate_alignment": row["gate_alignment"],
                        "context_class": row["context_class"],
                    },
                )
                counts["targets"] += 1
                continue

            if row["phase"] == "OOS" and now >= OOS_END_EXCLUSIVE and (
                natural_end is None or natural_end > OOS_END_EXCLUSIVE
            ):
                conn.execute(
                    """
                    UPDATE performance_oos_shadow_occurrences
                    SET final_state='CENSORED', natural_end_at=%s, updated_at=NOW()
                    WHERE occurrence_key=%s
                    """,
                    (OOS_END_EXCLUSIVE, row["occurrence_key"]),
                )
                _queue_event(
                    conn,
                    occurrence_key=row["occurrence_key"],
                    phase=row["phase"],
                    state="CENSORED",
                    symbol=row["symbol"],
                    direction=row["direction"],
                    source_tf=row["source_tf"],
                    target_tf=row["target_tf"],
                    event_at=OOS_END_EXCLUSIVE,
                    payload={
                        "censor_reason": "OOS_END",
                        "persistence_on": row["persistence_on"],
                        "target_cluster_transition": row["target_cluster_transition"],
                        "gate_cluster_transition": row["gate_cluster_transition"],
                        "gate_alignment": row["gate_alignment"],
                        "context_class": row["context_class"],
                    },
                )
                counts["finalized"] += 1
                continue

            if natural_end is None or natural_end > now:
                continue

            complete, actual_bars, expected_bars = _feed_complete(
                conn,
                row["symbol"],
                row["landmark_at"],
                natural_end,
            )
            state = "NO_TARGET_OBSERVED" if complete else "CENSORED"
            conn.execute(
                """
                UPDATE performance_oos_shadow_occurrences
                SET final_state=%s, natural_end_at=%s, updated_at=NOW()
                WHERE occurrence_key=%s
                """,
                (state, natural_end, row["occurrence_key"]),
            )
            _queue_event(
                conn,
                occurrence_key=row["occurrence_key"],
                phase=row["phase"],
                state=state,
                symbol=row["symbol"],
                direction=row["direction"],
                source_tf=row["source_tf"],
                target_tf=row["target_tf"],
                event_at=natural_end,
                payload={
                    "feed_actual_bars": actual_bars,
                    "feed_expected_bars": expected_bars,
                    "persistence_on": row["persistence_on"],
                    "target_cluster_transition": row["target_cluster_transition"],
                    "gate_cluster_transition": row["gate_cluster_transition"],
                    "gate_alignment": row["gate_alignment"],
                    "context_class": row["context_class"],
                },
            )
            counts["finalized"] += 1

    return counts


def scan_shadow_safely() -> dict[str, int]:
    try:
        return scan_shadow()
    except Exception:
        log.exception("TAJEOMON Shadow scan failed")
        return {"landmarks": 0, "targets": 0, "finalized": 0, "errors": 1}


def claim_pending_events(limit: int = 20) -> list[dict[str, Any]]:
    ensure_shadow_schema()
    safe_limit = max(1, min(int(limit), 100))
    with _connect() as conn:
        rows = conn.execute(
            """
            WITH candidates AS (
                SELECT id
                FROM performance_oos_shadow_events
                WHERE (
                    delivery_status='PENDING'
                    OR (delivery_status='CLAIMED' AND claimed_at < NOW() - INTERVAL '2 minutes')
                )
                ORDER BY event_at, id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE performance_oos_shadow_events e
            SET delivery_status='CLAIMED', claimed_at=NOW()
            FROM candidates c
            WHERE e.id=c.id
            RETURNING e.id, e.state, e.symbol, e.phase, e.payload, e.event_at
            """,
            (safe_limit,),
        ).fetchall()
    return [
        {
            "id": row[0],
            "state": row[1],
            "symbol": row[2],
            "phase": row[3],
            "payload": dict(row[4]) if isinstance(row[4], dict) else {},
            "event_at": row[5],
        }
        for row in rows
    ]


def mark_event_delivery(event_id: int, status: str, detail: dict[str, Any] | None = None) -> None:
    ensure_shadow_schema()
    clean_status = str(status or "ERROR").strip().upper()[:24]
    delivered_at = datetime.now(timezone.utc) if clean_status == "DELIVERED" else None
    with _connect() as conn:
        conn.execute(
            """
            UPDATE performance_oos_shadow_events
            SET delivery_status=%s,
                delivered_at=COALESCE(%s, delivered_at),
                delivery_detail=%s
            WHERE id=%s
            """,
            (clean_status, delivered_at, Jsonb(detail or {}), int(event_id)),
        )


def build_manual_test_event(symbol: str = "BTCUSDT") -> dict[str, Any]:
    symbol = str(symbol or "BTCUSDT").strip().upper()
    if symbol not in OOS_SYMBOLS:
        symbol = "BTCUSDT"
    now = datetime.now(timezone.utc)
    return {
        "alert_kind": "SHADOW_OOS",
        "event_type": "SHADOW_OOS",
        "research_state": "IMMINENT_CANDIDATE",
        "phase": "PREP",
        "manual_test": True,
        "symbol": symbol,
        "direction": "LOW",
        "side": "BUY",
        "source_tf": "30m",
        "target_tf": "1h",
        "timeframe": "30m",
        "occurred_at": now.isoformat(),
        "persistence_on": True,
        "target_cluster_transition": "WIDEN",
        "gate_cluster_transition": "",
        "gate_alignment": "",
        "context_class": "SUPPORT",
        "rule": FROZEN_PAIRS[("LOW", "30m", "1h")]["rule"],
        "master_version": MASTER,
        "v15b_sha256": V15B_SHA256,
        "v15c_sha256": V15C_SHA256,
        "shadow_schema_version": SHADOW_SCHEMA_VERSION,
    }


def status_summary() -> dict[str, Any]:
    ensure_shadow_schema()
    with _connect() as conn:
        counts = conn.execute(
            """
            SELECT phase, COALESCE(final_state,'OPEN') AS state, COUNT(*)
            FROM performance_oos_shadow_occurrences
            GROUP BY phase, COALESCE(final_state,'OPEN')
            ORDER BY phase, state
            """
        ).fetchall()
        events = conn.execute(
            """
            SELECT delivery_status, COUNT(*)
            FROM performance_oos_shadow_events
            GROUP BY delivery_status
            ORDER BY delivery_status
            """
        ).fetchall()
        latest = conn.execute(
            """
            SELECT MAX(first_snapshot_at), MAX(landmark_at)
            FROM performance_oos_shadow_occurrences
            """
        ).fetchone()
    return {
        "ok": True,
        "master": MASTER,
        "v15b_sha256": V15B_SHA256,
        "v15c_sha256": V15C_SHA256,
        "oos_start": OOS_START.isoformat(),
        "oos_end_exclusive": OOS_END_EXCLUSIVE.isoformat(),
        "occurrences": [{"phase": r[0], "state": r[1], "count": int(r[2])} for r in counts],
        "events": [{"status": r[0], "count": int(r[1])} for r in events],
        "latest_source_at": latest[0].isoformat() if latest and latest[0] else None,
        "latest_landmark_at": latest[1].isoformat() if latest and latest[1] else None,
    }
