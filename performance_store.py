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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import psycopg
from psycopg.types.json import Jsonb

log = logging.getLogger("bbangdol-performance")

PERFORMANCE_DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()

# V63_MEMORY_FIX: webhook 1건마다 새 Thread를 만들지 않는다.
# Render 512MB 인스턴스에서 반복 알람이 몰릴 때 수백 개의 스레드/DB 연결이
# 동시에 생겨 OOM이 발생하는 것을 막기 위해 저장 작업을 고정 worker pool로 제한한다.
_DB_SAVE_WORKERS = max(1, min(int(os.getenv("PERFORMANCE_DB_SAVE_WORKERS", "3") or 3), 6))
_DB_SAVE_EXECUTOR = ThreadPoolExecutor(
    max_workers=_DB_SAVE_WORKERS,
    thread_name_prefix="performance-db-save",
)

def _submit_db_save(fn, *args) -> None:
    try:
        _DB_SAVE_EXECUTOR.submit(fn, *args)
    except RuntimeError:
        log.exception("Performance DB executor submit failed")


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
    "3d": 4320,
    "1w": 10080,
    "1M": 43200,
}

_TF_RE = re.compile(r"\b(1M|3d|1w|1d|12h|6h|4h|2h|1h|30m|15m|5m|3m)\b")
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

CREATE TABLE IF NOT EXISTS performance_candle_watch (
    symbol VARCHAR(100) PRIMARY KEY,
    exchange VARCHAR(30),
    raw_exchange VARCHAR(30),
    started_at TIMESTAMPTZ NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    need_1m BOOLEAN NOT NULL DEFAULT FALSE,
    need_5m BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE performance_candle_watch ADD COLUMN IF NOT EXISTS need_1m BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE performance_candle_watch ADD COLUMN IF NOT EXISTS need_5m BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS performance_candles_1m (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(30),
    raw_exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    bar_close_time TIMESTAMPTZ,
    open NUMERIC(30,10) NOT NULL,
    high NUMERIC(30,10) NOT NULL,
    low NUMERIC(30,10) NOT NULL,
    close NUMERIC(30,10) NOT NULL,
    volume NUMERIC(40,10),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_performance_candles_1m_symbol_time
    ON performance_candles_1m(symbol, bar_time);

CREATE TABLE IF NOT EXISTS performance_candles_5m (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(30),
    raw_exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,
    bar_time TIMESTAMPTZ NOT NULL,
    bar_close_time TIMESTAMPTZ,
    open NUMERIC(30,10) NOT NULL,
    high NUMERIC(30,10) NOT NULL,
    low NUMERIC(30,10) NOT NULL,
    close NUMERIC(30,10) NOT NULL,
    volume NUMERIC(40,10),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_performance_candles_5m_symbol_time
    ON performance_candles_5m(symbol, bar_time);


CREATE TABLE IF NOT EXISTS performance_prediction_snapshots (
    id BIGSERIAL PRIMARY KEY,
    strategy VARCHAR(30) NOT NULL DEFAULT '1Q',
    exchange VARCHAR(30),
    raw_exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,
    source_timeframe VARCHAR(10) NOT NULL,
    target_timeframe VARCHAR(10) NOT NULL,
    signal_price NUMERIC(30,10),
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_metrics JSONB NOT NULL,
    target_metrics JSONB NOT NULL,
    raw_payload JSONB NOT NULL,
    snapshot_hash VARCHAR(64) NOT NULL UNIQUE,
    first_target_signal_id BIGINT,
    first_target_at TIMESTAMPTZ,
    lead_minutes INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prediction_snapshot_symbol_pair_time
    ON performance_prediction_snapshots(symbol, source_timeframe, target_timeframe, snapshot_at);
CREATE INDEX IF NOT EXISTS idx_prediction_snapshot_unmatched
    ON performance_prediction_snapshots(symbol, target_timeframe, first_target_at);

CREATE TABLE IF NOT EXISTS performance_page_visits (
    id BIGSERIAL PRIMARY KEY,
    page_path VARCHAR(200) NOT NULL,
    visitor_hash VARCHAR(64) NOT NULL,
    visited_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_performance_page_visits_time
    ON performance_page_visits(visited_at);
CREATE INDEX IF NOT EXISTS idx_performance_page_visits_hash_time
    ON performance_page_visits(visitor_hash, visited_at);

CREATE TABLE IF NOT EXISTS performance_cycle_chart_archive (
    archive_key VARCHAR(300) PRIMARY KEY,
    market VARCHAR(20),
    symbol VARCHAR(100) NOT NULL,
    entry_first_time TIMESTAMPTZ,
    completion_time TIMESTAMPTZ,
    image_png BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
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
    timeframe = "1M" if match.group(1) == "1M" else match.group(1).lower()
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


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "off", "no"}


def _collection_requirements(route: str) -> tuple[bool, bool]:
    """Return (need_1m, need_5m) for a newly observed LOW route."""
    route = str(route or "").upper()
    if route == "BD_BUY_SHORT":
        # 5m·15m 단타는 성과 계산 대상에서 제외한다.
        return False, False
    if "SWING" in route:
        return True, False
    if "LONG" in route or "LIFE" in route:
        return False, True
    return False, False



def _prediction_hash(payload: dict[str, Any]) -> str:
    symbol = str(payload.get("symbol", "")).strip()
    source_tf = str(payload.get("source_timeframe", "")).strip()
    target_tf = str(payload.get("target_timeframe", "")).strip()
    signal_time = str(payload.get("signal_time", "")).strip()
    return hashlib.sha256(f"{symbol}|{source_tf}|{target_tf}|{signal_time}".encode("utf-8")).hexdigest()


def save_prediction_snapshot(payload: dict[str, Any]) -> bool:
    if str(payload.get("event_type", "")).strip().upper() != "PREDICTION_SNAPSHOT_1Q":
        return False

    symbol = str(payload.get("symbol", "")).strip()
    source_tf = str(payload.get("source_timeframe", "")).strip()
    target_tf = str(payload.get("target_timeframe", "")).strip()
    if not symbol or not source_tf or not target_tf:
        raise ValueError("prediction snapshot missing symbol/source_timeframe/target_timeframe")

    try:
        signal_price = Decimal(str(payload.get("signal_price"))) if payload.get("signal_price") is not None else None
    except (InvalidOperation, ValueError):
        signal_price = None

    snapshot_at = datetime.now(timezone.utc)
    try:
        if payload.get("signal_time") is not None:
            snapshot_at = datetime.fromtimestamp(int(payload["signal_time"]) / 1000.0, tz=timezone.utc)
    except Exception:
        pass

    ensure_schema()
    params = {
        "exchange": str(payload.get("exchange", "")).strip() or None,
        "raw_exchange": str(payload.get("raw_exchange", "")).strip() or None,
        "symbol": symbol,
        "source_tf": source_tf,
        "target_tf": target_tf,
        "signal_price": signal_price,
        "snapshot_at": snapshot_at,
        "source_metrics": Jsonb(payload.get("source_metrics") or {}),
        "target_metrics": Jsonb(payload.get("target_metrics") or {}),
        "raw_payload": Jsonb(payload),
        "snapshot_hash": _prediction_hash(payload),
    }
    with _connect() as conn:
        row = conn.execute(
            """
            INSERT INTO performance_prediction_snapshots(
                strategy, exchange, raw_exchange, symbol,
                source_timeframe, target_timeframe, signal_price, snapshot_at,
                source_metrics, target_metrics, raw_payload, snapshot_hash
            ) VALUES (
                '1Q', %(exchange)s, %(raw_exchange)s, %(symbol)s,
                %(source_tf)s, %(target_tf)s, %(signal_price)s, %(snapshot_at)s,
                %(source_metrics)s, %(target_metrics)s, %(raw_payload)s, %(snapshot_hash)s
            )
            ON CONFLICT (snapshot_hash) DO NOTHING
            RETURNING id
            """,
            params,
        ).fetchone()
    if row:
        log.info("Prediction snapshot saved id=%s symbol=%s %s->%s", row[0], symbol, source_tf, target_tf)
        return True
    return False


def save_prediction_snapshot_safely(payload: dict[str, Any]) -> None:
    try:
        save_prediction_snapshot(payload)
    except Exception:
        log.exception("Prediction snapshot DB save failed")


def queue_prediction_snapshot_save(payload: dict[str, Any]) -> None:
    if str(payload.get("event_type", "")).strip().upper() != "PREDICTION_SNAPSHOT_1Q":
        return
    snapshot = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    _submit_db_save(save_prediction_snapshot_safely, snapshot)


def _link_prediction_target_signal(symbol: str, timeframe: Optional[str], signal_id: int, signal_at: datetime) -> None:
    if not symbol or not timeframe:
        return
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """
            UPDATE performance_prediction_snapshots
            SET first_target_signal_id=%s,
                first_target_at=%s,
                lead_minutes=GREATEST(
                    0,
                    FLOOR(EXTRACT(EPOCH FROM (%s - snapshot_at)) / 60.0)::INTEGER
                )
            WHERE symbol=%s
              AND target_timeframe=%s
              AND first_target_at IS NULL
              AND snapshot_at <= %s
            """,
            (signal_id, signal_at, signal_at, symbol, timeframe, signal_at),
        )


def prediction_research_summary(limit: int = 100) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 1000))
    if not PERFORMANCE_DATABASE_URL:
        return {"ok": False, "database": "not_configured", "count": 0, "pairs": [], "recent": []}
    ensure_schema()
    with _connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM performance_prediction_snapshots").fetchone()[0]
        pairs_raw = conn.execute(
            """
            SELECT source_timeframe, target_timeframe, COUNT(*), COUNT(first_target_at),
                   AVG(lead_minutes) FILTER (WHERE first_target_at IS NOT NULL)
            FROM performance_prediction_snapshots
            GROUP BY source_timeframe, target_timeframe
            ORDER BY
              CASE source_timeframe
                WHEN '30m' THEN 1 WHEN '1h' THEN 2 WHEN '4h' THEN 3
                WHEN '1d' THEN 4 WHEN '3d' THEN 5 WHEN '1w' THEN 6 ELSE 99 END
            """
        ).fetchall()
        recent_raw = conn.execute(
            """
            SELECT id, exchange, symbol, source_timeframe, target_timeframe,
                   signal_price, snapshot_at, source_metrics, target_metrics,
                   first_target_at, lead_minutes
            FROM performance_prediction_snapshots
            ORDER BY snapshot_at DESC, id DESC
            LIMIT %s
            """,
            (safe_limit,),
        ).fetchall()
    return {
        "ok": True,
        "count": int(total),
        "pairs": [
            {
                "source_timeframe": r[0], "target_timeframe": r[1],
                "snapshots": int(r[2]), "matched": int(r[3]),
                "average_lead_minutes": float(r[4]) if r[4] is not None else None,
            } for r in pairs_raw
        ],
        "recent": [
            {
                "id": r[0], "exchange": r[1], "symbol": r[2],
                "source_timeframe": r[3], "target_timeframe": r[4],
                "signal_price": float(r[5]) if r[5] is not None else None,
                "snapshot_at": r[6].isoformat() if r[6] else None,
                "source_metrics": r[7] or {}, "target_metrics": r[8] or {},
                "first_target_at": r[9].isoformat() if r[9] else None,
                "lead_minutes": r[10],
            } for r in recent_raw
        ],
    }


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
        if row["signal_type"] == "LOW":
            _link_prediction_target_signal(symbol, timeframe, int(inserted[0]), received_at)
            need_1m, need_5m = _collection_requirements(route)
            if need_1m or need_5m:
                with _connect() as conn:
                    conn.execute(
                        """
                        INSERT INTO performance_candle_watch(
                            symbol, exchange, raw_exchange, started_at, active,
                            need_1m, need_5m, updated_at
                        ) VALUES (%s, %s, %s, %s, TRUE, %s, %s, NOW())
                        ON CONFLICT (symbol) DO UPDATE SET
                            exchange=EXCLUDED.exchange,
                            raw_exchange=EXCLUDED.raw_exchange,
                            started_at=CASE
                                WHEN performance_candle_watch.active
                                THEN LEAST(performance_candle_watch.started_at, EXCLUDED.started_at)
                                ELSE EXCLUDED.started_at
                            END,
                            active=TRUE,
                            need_1m=CASE
                                WHEN performance_candle_watch.active
                                THEN performance_candle_watch.need_1m OR EXCLUDED.need_1m
                                ELSE EXCLUDED.need_1m
                            END,
                            need_5m=CASE
                                WHEN performance_candle_watch.active
                                THEN performance_candle_watch.need_5m OR EXCLUDED.need_5m
                                ELSE EXCLUDED.need_5m
                            END,
                            updated_at=NOW()
                        """,
                        (symbol, row["exchange"], row["raw_exchange"], received_at, need_1m, need_5m),
                    )
        log.info(
            "Performance signal saved id=%s strategy=%s route=%s symbol=%s tf=%s price=%s",
            inserted[0], row["strategy"], route, symbol, timeframe, price,
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
    _submit_db_save(save_signal_safely, snapshot)


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
    """Return recent saved signals as JSON-serializable dictionaries."""
    safe_limit = max(1, min(int(limit), 500))
    if not PERFORMANCE_DATABASE_URL:
        return []
    ensure_schema()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, strategy, route, exchange, raw_exchange, symbol,
                   side, signal_type, timeframe, timeframe_minutes,
                   signal_price, received_at, raw_message
            FROM performance_signals
            ORDER BY received_at DESC, id DESC
            LIMIT %s
            """,
            (safe_limit,),
        ).fetchall()
    return [
        {
            "id": row[0],
            "strategy": row[1],
            "route": row[2],
            "exchange": row[3],
            "raw_exchange": row[4],
            "symbol": row[5],
            "side": row[6],
            "signal_type": row[7],
            "timeframe": row[8],
            "timeframe_minutes": row[9],
            "signal_price": float(row[10]) if row[10] is not None else None,
            "received_at": row[11].isoformat() if row[11] else None,
            "raw_message": row[12],
        }
        for row in rows
    ]


def _ms_to_datetime(value: Any) -> datetime:
    number = int(float(value))
    return datetime.fromtimestamp(number / 1000.0, tz=timezone.utc)


def _candle_interval(payload: dict[str, Any]) -> int:
    event = str(payload.get("event_type", "")).upper()
    if event == "PERFORMANCE_CANDLE_1M":
        return 1
    if event == "PERFORMANCE_CANDLE_5M":
        return 5
    try:
        value = int(payload.get("interval_minutes", 0))
    except (TypeError, ValueError):
        value = 0
    if value not in (1, 5):
        raise ValueError(f"unsupported candle interval: {value}")
    return value


def save_candle(payload: dict[str, Any]) -> bool:
    """Store confirmed TradingView 1m/5m OHLC only while its resolution is required."""
    symbol = str(payload.get("symbol", "")).strip()
    if not symbol:
        raise ValueError("candle payload missing symbol")
    interval = _candle_interval(payload)
    bar_time = _ms_to_datetime(payload.get("bar_time"))
    bar_close_time = _ms_to_datetime(payload.get("bar_close_time")) if payload.get("bar_close_time") else None
    values = {name: Decimal(str(payload.get(name))) for name in ("open", "high", "low", "close")}
    volume = Decimal(str(payload.get("volume", 0)))
    ensure_schema()
    with _connect() as conn:
        watch = conn.execute(
            "SELECT active, started_at, need_1m, need_5m FROM performance_candle_watch WHERE symbol=%s",
            (symbol,),
        ).fetchone()
        required = bool(watch and watch[0] and ((interval == 1 and watch[2]) or (interval == 5 and watch[3])))
        if not required or bar_time < watch[1] - timedelta(minutes=interval):
            return False
        table = "performance_candles_1m" if interval == 1 else "performance_candles_5m"
        sql = f"""
            INSERT INTO {table}(
                exchange, raw_exchange, symbol, bar_time, bar_close_time,
                open, high, low, close, volume
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(symbol, bar_time) DO NOTHING
            RETURNING id
        """
        row = conn.execute(
            sql,
            (
                str(payload.get("exchange", "")) or None,
                str(payload.get("raw_exchange", "")) or None,
                symbol, bar_time, bar_close_time,
                values["open"], values["high"], values["low"], values["close"], volume,
            ),
        ).fetchone()
    return bool(row)


def queue_candle_save(payload: dict[str, Any]) -> None:
    snapshot = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    def worker():
        try:
            save_candle(snapshot)
        except Exception:
            log.exception("Performance candle save failed event=%s", snapshot.get("event_type"))
    _submit_db_save(worker)


def load_candles(
    symbol: str,
    start_time: str | datetime,
    end_time: str | datetime,
    interval_minutes: int,
) -> list[dict[str, Any]]:
    if interval_minutes not in (1, 5):
        raise ValueError("interval_minutes must be 1 or 5")
    ensure_schema()
    start = datetime.fromisoformat(start_time) if isinstance(start_time, str) else start_time
    end = datetime.fromisoformat(end_time) if isinstance(end_time, str) else end_time
    table = "performance_candles_1m" if interval_minutes == 1 else "performance_candles_5m"
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT bar_time, open, high, low, close, volume
            FROM {table}
            WHERE symbol=%s AND bar_time BETWEEN %s AND %s
            ORDER BY bar_time
            """, (symbol, start, end)
        ).fetchall()
    return [
        {
            "time": r[0], "open": float(r[1]), "high": float(r[2]),
            "low": float(r[3]), "close": float(r[4]), "volume": float(r[5] or 0),
            "interval_minutes": interval_minutes,
        }
        for r in rows
    ]


def candle_watch_status(symbol: str) -> dict[str, Any] | None:
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT started_at, active, need_1m, need_5m FROM performance_candle_watch WHERE symbol=%s",
            (symbol,),
        ).fetchone()
    if not row:
        return None
    return {"started_at": row[0], "active": bool(row[1]), "need_1m": bool(row[2]), "need_5m": bool(row[3])}


# v19 compatibility aliases
def save_candle_5m(payload: dict[str, Any]) -> bool:
    payload = dict(payload)
    payload.setdefault("event_type", "PERFORMANCE_CANDLE_5M")
    return save_candle(payload)


def load_candles_5m(symbol: str, start_time: str | datetime, end_time: str | datetime) -> list[dict[str, Any]]:
    return load_candles(symbol, start_time, end_time, 5)



def archive_cycle_chart(archive_key: str, market: str, symbol: str, entry_first_time: str, completion_time: str, png: bytes) -> None:
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """INSERT INTO performance_cycle_chart_archive
            (archive_key, market, symbol, entry_first_time, completion_time, image_png)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT(archive_key) DO UPDATE SET image_png=EXCLUDED.image_png, created_at=NOW()""",
            (archive_key, market, symbol, entry_first_time, completion_time, png),
        )


def finish_candle_watch(symbol: str, through_time: str | datetime) -> int:
    """Deactivate collection and delete raw 1m/5m candles after final chart archive."""
    end = datetime.fromisoformat(through_time) if isinstance(through_time, str) else through_time
    ensure_schema()
    with _connect() as conn:
        conn.execute(
            """UPDATE performance_candle_watch
               SET active=FALSE, need_1m=FALSE, need_5m=FALSE, updated_at=NOW()
               WHERE symbol=%s""",
            (symbol,),
        )
        deleted_1m = conn.execute(
            "DELETE FROM performance_candles_1m WHERE symbol=%s AND bar_time<=%s RETURNING id",
            (symbol, end),
        ).fetchall()
        deleted_5m = conn.execute(
            "DELETE FROM performance_candles_5m WHERE symbol=%s AND bar_time<=%s RETURNING id",
            (symbol, end),
        ).fetchall()
    return len(deleted_1m) + len(deleted_5m)


def record_page_visit(page_path: str, visitor_hash: str) -> None:
    """회원 페이지 방문을 익명 해시로 기록한다.

    같은 방문자가 짧은 시간 새로고침해도 조회수가 과도하게 증가하지 않도록
    30분 내 동일 page_path 방문은 1회로 계산한다.
    """
    try:
        ensure_schema()
        with _connect() as conn:
            exists = conn.execute(
                """
                SELECT 1
                FROM performance_page_visits
                WHERE page_path=%s AND visitor_hash=%s
                  AND visited_at >= NOW() - INTERVAL '30 minutes'
                LIMIT 1
                """,
                (str(page_path or '/performance/member')[:200], str(visitor_hash)[:64]),
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO performance_page_visits(page_path, visitor_hash) VALUES (%s, %s)",
                    (str(page_path or '/performance/member')[:200], str(visitor_hash)[:64]),
                )
    except Exception:
        log.exception("Performance page visit save failed")


def page_visit_summary() -> dict[str, int]:
    """회원페이지 누적·오늘 방문자 수를 반환한다."""
    try:
        ensure_schema()
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_views,
                    COUNT(*) FILTER (
                        WHERE visited_at >= date_trunc('day', NOW() AT TIME ZONE 'Asia/Seoul')
                            AT TIME ZONE 'Asia/Seoul'
                    ) AS today_views,
                    COUNT(DISTINCT visitor_hash) AS total_visitors,
                    COUNT(DISTINCT visitor_hash) FILTER (
                        WHERE visited_at >= date_trunc('day', NOW() AT TIME ZONE 'Asia/Seoul')
                            AT TIME ZONE 'Asia/Seoul'
                    ) AS today_visitors
                FROM performance_page_visits
                """
            ).fetchone()
        return {
            "total_views": int(row[0] or 0),
            "today_views": int(row[1] or 0),
            "total_visitors": int(row[2] or 0),
            "today_visitors": int(row[3] or 0),
        }
    except Exception:
        log.exception("Performance page visit summary failed")
        return {"total_views": 0, "today_views": 0, "total_visitors": 0, "today_visitors": 0}
