"""Performance signal storage for the member analytics project.

This module is intentionally isolated from Telegram delivery and automated trading.
A database failure is logged but must never stop the existing alert flow.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg
from psycopg.types.json import Jsonb

log = logging.getLogger("bbangdol-performance")

PERFORMANCE_DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()

PERFORMANCE_ROUTES = {
    # 별꽃 타점
    "BD_BUY_SHORT", "BD_BUY_SWING", "BD_BUY_LONG", "BD_BUY_LIFE",
    "BD_SELL_SHORT", "BD_SELL_SWING", "BD_SELL_LONG", "BD_SELL_LIFE",
    # 1Q 대형주
    "BUY_SWING_1Q", "BUY_LONG_1Q", "BUY_LIFE_1Q",
    "SELL_SWING_1Q", "SELL_LONG_1Q", "SELL_LIFE_1Q",
}

_TIMEFRAME_MINUTES = {
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

_TF_RE = re.compile(r"\b(1w|1d|12h|6h|4h|2h|1h|30m|15m|5m|3m)\b", re.IGNORECASE)
_PRICE_RE = re.compile(r":\s*([0-9][0-9,]*(?:\.[0-9]+)?)")
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS performance_signals (
    id BIGSERIAL PRIMARY KEY,
    strategy VARCHAR(30) NOT NULL,
    route VARCHAR(50) NOT NULL,
    exchange VARCHAR(30),
    raw_exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,
    side VARCHAR(10) NOT NULL,
    signal_type VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10),
    timeframe_minutes INTEGER,
    signal_price NUMERIC(30, 10),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    signal_hash VARCHAR(64) NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_performance_signals_symbol_time
    ON performance_signals(symbol, received_at);
CREATE INDEX IF NOT EXISTS idx_performance_signals_strategy_route
    ON performance_signals(strategy, route);
CREATE INDEX IF NOT EXISTS idx_performance_signals_side_tf
    ON performance_signals(side, timeframe_minutes);
"""

_MIGRATE_SIGNAL_NO_SQL = """
ALTER TABLE performance_signals
    ADD COLUMN IF NOT EXISTS signal_no VARCHAR(24);

UPDATE performance_signals
SET signal_no = 'PF-' || LPAD(id::text, 10, '0')
WHERE signal_no IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_performance_signals_signal_no
    ON performance_signals(signal_no);
"""


def _connect() -> psycopg.Connection:
    if not PERFORMANCE_DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")
    return psycopg.connect(
        PERFORMANCE_DATABASE_URL,
        autocommit=True,
        connect_timeout=5,
        application_name="bbangdol-performance",
    )


def ensure_schema() -> None:
    """Create the table and indexes once per process."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        with _connect() as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_MIGRATE_SIGNAL_NO_SQL)
        _SCHEMA_READY = True
        log.info("Performance database schema is ready")


def is_performance_route(route: str) -> bool:
    return str(route or "").strip().upper() in PERFORMANCE_ROUTES


def _parse_strategy(route: str) -> str:
    return "1Q" if route.endswith("_1Q") else "STARFLOWER"


def _parse_side(route: str) -> str:
    return "SHORT" if "SELL" in route else "LONG"


def _parse_signal_type(route: str) -> str:
    return "HIGH" if "SELL" in route else "LOW"


def _parse_timeframe(message: str) -> tuple[Optional[str], Optional[int]]:
    match = _TF_RE.search(message or "")
    if not match:
        return None, None
    timeframe = match.group(1).lower()
    return timeframe, _TIMEFRAME_MINUTES.get(timeframe)


def _parse_price(message: str) -> Optional[Decimal]:
    """Read the displayed alert price from the first ': price' occurrence."""
    match = _PRICE_RE.search(message or "")
    if not match:
        return None
    try:
        return Decimal(match.group(1).replace(",", ""))
    except InvalidOperation:
        return None


def _make_signal_hash(route: str, symbol: str, message: str, received_at: datetime) -> str:
    """Deduplicate only near-simultaneous webhook retries.

    A 10-second bucket allows legitimate repeated minute alerts to remain separate.
    """
    ten_second_bucket = int(received_at.timestamp()) // 10
    source = f"{route}|{symbol}|{message}|{ten_second_bucket}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def save_signal(payload: dict[str, Any]) -> bool:
    """Save one eligible TradingView signal. Returns True if inserted."""
    route = str(payload.get("route", payload.get("type", ""))).strip().upper()
    if not is_performance_route(route):
        return False

    message = str(payload.get("msg", payload.get("message", ""))).strip()
    symbol = str(payload.get("symbol", "")).strip()
    if not message or not symbol:
        raise ValueError("performance signal is missing symbol or msg")

    received_at = datetime.now(timezone.utc)
    timeframe, timeframe_minutes = _parse_timeframe(message)
    price = _parse_price(message)
    signal_hash = _make_signal_hash(route, symbol, message, received_at)

    row = {
        "strategy": _parse_strategy(route),
        "route": route,
        "exchange": str(payload.get("exchange", "")).strip() or None,
        "raw_exchange": str(payload.get("raw_exchange", "")).strip() or None,
        "symbol": symbol,
        "side": _parse_side(route),
        "signal_type": _parse_signal_type(route),
        "timeframe": timeframe,
        "timeframe_minutes": timeframe_minutes,
        "signal_price": price,
        "received_at": received_at,
        "raw_message": message,
        "raw_payload": payload,
        "signal_hash": signal_hash,
    }

    ensure_schema()
    sql = """
        INSERT INTO performance_signals (
            strategy, route, exchange, raw_exchange, symbol,
            side, signal_type, timeframe, timeframe_minutes,
            signal_price, received_at, raw_message, raw_payload, signal_hash
        ) VALUES (
            %(strategy)s, %(route)s, %(exchange)s, %(raw_exchange)s, %(symbol)s,
            %(side)s, %(signal_type)s, %(timeframe)s, %(timeframe_minutes)s,
            %(signal_price)s, %(received_at)s, %(raw_message)s, %(raw_payload)s, %(signal_hash)s
        )
        ON CONFLICT (signal_hash) DO NOTHING
        RETURNING id
    """
    params = dict(row)
    params["raw_payload"] = Jsonb(payload)
    with _connect() as conn:
        inserted = conn.execute(sql, params).fetchone()
    if inserted:
        inserted_id = int(inserted[0])
        signal_no = f"PF-{inserted_id:010d}"
        with _connect() as conn:
            conn.execute(
                "UPDATE performance_signals SET signal_no = %s WHERE id = %s",
                (signal_no, inserted_id),
            )
        log.info(
            "Performance signal saved id=%s signal_no=%s strategy=%s route=%s symbol=%s tf=%s price=%s",
            inserted_id, signal_no, row["strategy"], route, symbol, timeframe, price,
        )
        return True
    log.info("Performance duplicate ignored route=%s symbol=%s", route, symbol)
    return False


def save_signal_safely(payload: dict[str, Any]) -> None:
    """Never raise into the existing Telegram webhook flow."""
    try:
        save_signal(payload)
    except Exception:
        log.exception("Performance DB save failed")


def queue_signal_save(payload: dict[str, Any]) -> None:
    """Store independently in a daemon thread so Telegram delivery is not delayed."""
    if not is_performance_route(str(payload.get("route", payload.get("type", "")))):
        return
    snapshot = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    threading.Thread(
        target=save_signal_safely,
        args=(snapshot,),
        daemon=True,
        name="performance-signal-save",
    ).start()


def health_summary() -> dict[str, Any]:
    """Return a non-sensitive connectivity and row-count summary."""
    if not PERFORMANCE_DATABASE_URL:
        return {"ok": False, "database": "not_configured", "signal_count": 0}
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*), MAX(received_at) FROM performance_signals"
        ).fetchone()
    return {
        "ok": True,
        "database": "connected",
        "signal_count": int(row[0]),
        "latest_signal_at": row[1].isoformat() if row[1] else None,
    }


def latest_signals(limit: int = 20) -> list[dict[str, Any]]:
    """Return recent stored signals without raw payload or secrets."""
    ensure_schema()
    safe_limit = max(1, min(int(limit), 100))
    sql = """
        SELECT signal_no, strategy, route, exchange, symbol, side, signal_type,
               timeframe, timeframe_minutes, signal_price, received_at, raw_message
        FROM performance_signals
        ORDER BY id DESC
        LIMIT %s
    """
    with _connect() as conn:
        rows = conn.execute(sql, (safe_limit,)).fetchall()
    result: list[dict[str, Any]] = []
    for r in rows:
        result.append({
            "signal_no": r[0],
            "strategy": r[1],
            "route": r[2],
            "exchange": r[3],
            "symbol": r[4],
            "side": r[5],
            "signal_type": r[6],
            "timeframe": r[7],
            "timeframe_minutes": r[8],
            "signal_price": str(r[9]) if r[9] is not None else None,
            "received_at": r[10].isoformat() if r[10] else None,
            "raw_message": r[11],
        })
    return result
