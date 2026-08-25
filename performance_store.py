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

# V99: SOL/SUI 현물 전환. 과거 선물 심볼을 현물 심볼로 1회 치환하고,
# 이후 동일 두 심볼이 .P로 들어와도 성과 DB에는 현물명으로 저장한다.
_SYMBOL_CANONICAL_MAP = {
    "SOLUSDT.P": "SOLUSDT",
    "SUIUSDT.P": "SUIUSDT",
}

def canonical_performance_symbol(symbol: Any) -> str:
    text = str(symbol or "").strip().upper()
    return _SYMBOL_CANONICAL_MAP.get(text, text)

def _canonicalize_payload_symbol(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload or {})
    old = str(out.get("symbol", "") or "").strip().upper()
    new = canonical_performance_symbol(old)
    if old and new != old:
        out["symbol"] = new
        for key in ("msg", "message"):
            if out.get(key) is not None:
                out[key] = str(out[key]).replace(old, new)
    return out

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


CREATE TABLE IF NOT EXISTS performance_cadence_stage_events (
    id BIGSERIAL PRIMARY KEY,
    route_family VARCHAR(40) NOT NULL,
    route VARCHAR(60),
    exchange VARCHAR(30),
    raw_exchange VARCHAR(30),
    symbol VARCHAR(100) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    stage INTEGER NOT NULL,
    stage_label VARCHAR(30) NOT NULL,
    telegram_visible BOOLEAN NOT NULL DEFAULT FALSE,
    signal_price NUMERIC(30,10),
    episode_started_at TIMESTAMPTZ NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    raw_message TEXT,
    event_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cadence_stage_symbol_time
    ON performance_cadence_stage_events(symbol, occurred_at);
CREATE INDEX IF NOT EXISTS idx_cadence_stage_stage_tf
    ON performance_cadence_stage_events(stage, timeframe, direction);

CREATE TABLE IF NOT EXISTS performance_prediction_watch (
    symbol VARCHAR(100) PRIMARY KEY,
    active_until TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS performance_data_migrations (
    migration_key VARCHAR(100) PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    detail JSONB
);

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

-- V116: Tajum On push device registration.
-- user_uid is intentionally nullable until Firebase phone login is connected.
CREATE TABLE IF NOT EXISTS tajum_app_devices (
    device_id VARCHAR(128) PRIMARY KEY,
    user_uid VARCHAR(128),
    fcm_token TEXT NOT NULL,
    platform VARCHAR(20) NOT NULL DEFAULT 'android',
    enabled_symbols TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE tajum_app_devices
    ADD COLUMN IF NOT EXISTS sound_profile VARCHAR(24) NOT NULL DEFAULT 'clear';
ALTER TABLE tajum_app_devices
    ADD COLUMN IF NOT EXISTS vibration_enabled BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE tajum_app_devices
    ADD COLUMN IF NOT EXISTS enabled_signal_groups JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS uq_tajum_app_devices_fcm_token
    ON tajum_app_devices(fcm_token);
CREATE INDEX IF NOT EXISTS idx_tajum_app_devices_updated
    ON tajum_app_devices(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_tajum_app_devices_enabled_symbols
    ON tajum_app_devices USING GIN(enabled_symbols);

-- V118: per-device Tajum On push inbox.
-- Only successfully delivered FCM cadence notifications are stored here.
CREATE TABLE IF NOT EXISTS tajum_app_push_history (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(128) NOT NULL,
    delivery_key VARCHAR(64) NOT NULL,
    symbol VARCHAR(100) NOT NULL,
    display VARCHAR(220),
    market VARCHAR(20),
    exchange VARCHAR(30),
    direction VARCHAR(10),
    side VARCHAR(10),
    timeframe VARCHAR(10),
    stage SMALLINT NOT NULL DEFAULT 0,
    alert_label VARCHAR(120),
    signal_price NUMERIC(30, 10),
    route VARCHAR(50),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tajum_push_delivery UNIQUE(device_id, delivery_key)
);
CREATE INDEX IF NOT EXISTS idx_tajum_push_history_device_time
    ON tajum_app_push_history(device_id, occurred_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_tajum_push_history_device_symbol
    ON tajum_app_push_history(device_id, symbol, occurred_at DESC);
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


def _apply_v99_spot_symbol_migration(conn: psycopg.Connection) -> None:
    """One-time merge: SOLUSDT.P/SUIUSDT.P -> spot names without losing history."""
    migration_key = "v99_sol_sui_futures_to_spot"
    exists = conn.execute(
        "SELECT 1 FROM performance_data_migrations WHERE migration_key=%s",
        (migration_key,),
    ).fetchone()
    if exists:
        return

    merged = {}
    for old_symbol, new_symbol in _SYMBOL_CANONICAL_MAP.items():
        counts = {}

        # Time-series tables have UNIQUE(symbol, bar_time). Remove only true overlaps first.
        for table in ("performance_candles_1m", "performance_candles_5m"):
            deleted = conn.execute(
                f"""DELETE FROM {table} old_row
                    USING {table} new_row
                    WHERE old_row.symbol=%s AND new_row.symbol=%s
                      AND old_row.bar_time=new_row.bar_time""",
                (old_symbol, new_symbol),
            ).rowcount
            updated = conn.execute(
                f"UPDATE {table} SET symbol=%s WHERE symbol=%s",
                (new_symbol, old_symbol),
            ).rowcount
            counts[table] = {"dedup_deleted": int(deleted or 0), "renamed": int(updated or 0)}

        # Merge active candle-watch state if both old/new keys exist.
        conn.execute(
            """INSERT INTO performance_candle_watch(
                       symbol,exchange,raw_exchange,started_at,active,need_1m,need_5m,updated_at
                   )
                   SELECT %s,exchange,raw_exchange,started_at,active,need_1m,need_5m,updated_at
                   FROM performance_candle_watch WHERE symbol=%s
                   ON CONFLICT(symbol) DO UPDATE SET
                     exchange=COALESCE(EXCLUDED.exchange, performance_candle_watch.exchange),
                     raw_exchange=COALESCE(EXCLUDED.raw_exchange, performance_candle_watch.raw_exchange),
                     started_at=LEAST(EXCLUDED.started_at, performance_candle_watch.started_at),
                     active=(EXCLUDED.active OR performance_candle_watch.active),
                     need_1m=(EXCLUDED.need_1m OR performance_candle_watch.need_1m),
                     need_5m=(EXCLUDED.need_5m OR performance_candle_watch.need_5m),
                     updated_at=GREATEST(EXCLUDED.updated_at, performance_candle_watch.updated_at)""",
            (new_symbol, old_symbol),
        )
        conn.execute("DELETE FROM performance_candle_watch WHERE symbol=%s", (old_symbol,))

        # Merge prediction watch using the later expiry.
        conn.execute(
            """INSERT INTO performance_prediction_watch(symbol,active_until,updated_at)
                   SELECT %s,active_until,updated_at FROM performance_prediction_watch WHERE symbol=%s
                   ON CONFLICT(symbol) DO UPDATE SET
                     active_until=GREATEST(EXCLUDED.active_until, performance_prediction_watch.active_until),
                     updated_at=GREATEST(EXCLUDED.updated_at, performance_prediction_watch.updated_at)""",
            (new_symbol, old_symbol),
        )
        conn.execute("DELETE FROM performance_prediction_watch WHERE symbol=%s", (old_symbol,))

        # Event/history tables can be renamed in place; their hashes remain historical IDs.
        counts["performance_signals"] = int(conn.execute(
            """UPDATE performance_signals
                   SET symbol=%s,
                       raw_message=REPLACE(raw_message,%s,%s),
                       raw_payload=jsonb_set(raw_payload,'{symbol}',to_jsonb(%s::text),true)
                   WHERE symbol=%s""",
            (new_symbol, old_symbol, new_symbol, new_symbol, old_symbol),
        ).rowcount or 0)
        counts["performance_prediction_snapshots"] = int(conn.execute(
            """UPDATE performance_prediction_snapshots
                   SET symbol=%s,
                       raw_payload=jsonb_set(raw_payload,'{symbol}',to_jsonb(%s::text),true)
                   WHERE symbol=%s""",
            (new_symbol, new_symbol, old_symbol),
        ).rowcount or 0)
        counts["performance_cadence_stage_events"] = int(conn.execute(
            """UPDATE performance_cadence_stage_events
                   SET symbol=%s, raw_message=REPLACE(COALESCE(raw_message,''),%s,%s)
                   WHERE symbol=%s""",
            (new_symbol, old_symbol, new_symbol, old_symbol),
        ).rowcount or 0)
        counts["performance_cycle_chart_archive"] = int(conn.execute(
            "UPDATE performance_cycle_chart_archive SET symbol=%s WHERE symbol=%s",
            (new_symbol, old_symbol),
        ).rowcount or 0)
        merged[old_symbol] = counts

    conn.execute(
        """INSERT INTO performance_data_migrations(migration_key,detail)
               VALUES (%s,%s) ON CONFLICT(migration_key) DO NOTHING""",
        (migration_key, Jsonb(merged)),
    )
    log.warning("V99 spot-symbol migration applied: %s", merged)


def ensure_schema() -> None:
    """Create schema and apply safe one-time data migrations once per process."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        with _connect() as conn:
            conn.execute(_CREATE_TABLE_SQL)
            _apply_v99_spot_symbol_migration(conn)
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




# =========================================================
# v87: 상위시간봉 예측 연구용 확장 지표 정규화
# - 기존 payload/DB 스키마를 깨지 않고 source_metrics / target_metrics JSONB 안에 저장
# - Pine이 새 수치들을 보내기 시작하는 즉시 과거/신규 코드와 호환하여 누적
# - 기존 방향/상태 필드(rsi_dir, stoch_*_dir, sma_state, ema_state)는 그대로 유지
# =========================================================
def _metric_first(m: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in m and m.get(key) is not None:
            return m.get(key)
    return None


def _metric_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 8)
    except (TypeError, ValueError):
        return None


def _metric_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "y", "above", "up"}:
        return True
    if text in {"0", "false", "no", "off", "n", "below", "down"}:
        return False
    return None


def _rsi_zone(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 20: return "0-20"
    if value < 30: return "20-30"
    if value < 40: return "30-40"
    if value < 50: return "40-50"
    if value < 60: return "50-60"
    if value < 70: return "60-70"
    if value < 80: return "70-80"
    return "80-100"


def _stoch_zone(k: float | None) -> str | None:
    if k is None:
        return None
    if k <= 20: return "LOW_0_20"
    if k >= 80: return "HIGH_80_100"
    return "MID_20_80"


def _cross_from_values(k: float | None, d: float | None, k_prev: float | None, d_prev: float | None) -> str | None:
    if None in (k, d, k_prev, d_prev):
        return None
    if k > d and k_prev <= d_prev:
        return "GOLDEN"
    if k < d and k_prev >= d_prev:
        return "DEAD"
    return "NONE"


def _normalize_cross(value: Any) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).strip().upper()
    if text in {"GC", "GOLDEN", "GOLDEN_CROSS", "GOLDENCROSS", "UP_CROSS"}:
        return "GOLDEN"
    if text in {"DC", "DEAD", "DEAD_CROSS", "DEADCROSS", "DOWN_CROSS"}:
        return "DEAD"
    if text in {"NONE", "NO", "FLAT", "0"}:
        return "NONE"
    return text[:24]


def _price_position(price: float | None, ma: float | None) -> str | None:
    if price is None or ma is None:
        return None
    if price > ma: return "ABOVE"
    if price < ma: return "BELOW"
    return "ON"


def _pct_gap(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    return round((a - b) / abs(b) * 100.0, 6)


def normalize_prediction_metrics(metrics: Any) -> dict[str, Any]:
    """Normalize old/new Pine metric payloads into one research schema.

    This function is deliberately tolerant: unknown keys are preserved, while
    common aliases are copied to canonical v88 names. This lets us deploy the
    backend first and start collecting richer Pine fields without another DB
    migration.
    """
    src = dict(metrics or {}) if isinstance(metrics, dict) else {}
    out: dict[str, Any] = dict(src)

    # RSI: actual level + 1/3/5-bar changes + 50-line / zones.
    rsi = _metric_float(_metric_first(src, "rsi_value", "rsi", "rsi14", "rsi_14"))
    if rsi is not None:
        out["rsi_value"] = rsi
        out["rsi_zone"] = _rsi_zone(rsi)
        out["rsi_above_50"] = rsi >= 50.0
        out["rsi_below_30"] = rsi <= 30.0
        out["rsi_above_70"] = rsi >= 70.0
    for n in (1, 3, 5):
        dv = _metric_float(_metric_first(src, f"rsi_delta_{n}", f"rsi_change_{n}", f"rsi_d{n}"))
        if dv is not None:
            out[f"rsi_delta_{n}"] = dv
    if "rsi_above_50" not in out:
        v = _metric_bool(_metric_first(src, "rsi_above_50", "rsi_gt_50"))
        if v is not None: out["rsi_above_50"] = v

    # Stochastic 5,3,3 and 20,12,12: K/D levels, spread, cross, zone, slope deltas.
    for prefix, aliases in (
        ("stoch_5_3", ("stoch_5_3", "stoch5", "k5")),
        ("stoch_20_12", ("stoch_20_12", "stoch20", "k20")),
    ):
        k_keys = [f"{a}_k" for a in aliases] + [prefix + "_value"]
        d_keys = [f"{a}_d" for a in aliases]
        kp_keys = [f"{a}_k_prev" for a in aliases]
        dp_keys = [f"{a}_d_prev" for a in aliases]
        k = _metric_float(_metric_first(src, *k_keys))
        d = _metric_float(_metric_first(src, *d_keys))
        kp = _metric_float(_metric_first(src, *kp_keys))
        dp = _metric_float(_metric_first(src, *dp_keys))
        if k is not None:
            out[prefix + "_k"] = k
            out[prefix + "_zone"] = _stoch_zone(k)
        if d is not None: out[prefix + "_d"] = d
        if kp is not None: out[prefix + "_k_prev"] = kp
        if dp is not None: out[prefix + "_d_prev"] = dp
        if k is not None and d is not None:
            out[prefix + "_spread"] = round(k - d, 8)
        explicit_cross = _normalize_cross(_metric_first(src, prefix + "_cross", *(f"{a}_cross" for a in aliases)))
        derived_cross = _cross_from_values(k, d, kp, dp)
        if explicit_cross is not None:
            out[prefix + "_cross"] = explicit_cross
        elif derived_cross is not None:
            out[prefix + "_cross"] = derived_cross
        for n in (1, 3, 5):
            kd = _metric_float(_metric_first(src, prefix + f"_k_delta_{n}", *(f"{a}_k_delta_{n}" for a in aliases)))
            dd = _metric_float(_metric_first(src, prefix + f"_d_delta_{n}", *(f"{a}_d_delta_{n}" for a in aliases)))
            if kd is not None: out[prefix + f"_k_delta_{n}"] = kd
            if dd is not None: out[prefix + f"_d_delta_{n}"] = dd

    # Moving average research: V94 uses SMA only (20/60/200).
    # Old EMA keys already stored in JSONB are preserved for backward compatibility,
    # but new grouping/research intentionally ignores EMA to avoid duplicated features.
    price = _metric_float(_metric_first(src, "price", "close", "close_price"))
    if price is not None: out["price"] = price
    for family in ("sma",):
        ma20 = _metric_float(_metric_first(src, family + "20", family + "_20"))
        ma60 = _metric_float(_metric_first(src, family + "60", family + "_60"))
        if ma20 is not None: out[family + "20"] = ma20
        if ma60 is not None: out[family + "60"] = ma60
        if ma20 is not None and ma60 is not None:
            out[family + "_gap_pct"] = _pct_gap(ma20, ma60)
            out.setdefault(family + "_state", "GOLDEN" if ma20 >= ma60 else "DEAD")
        cross = _normalize_cross(_metric_first(src, family + "_cross", family + "_cross_now"))
        if cross is not None: out[family + "_cross"] = cross
        bars = _metric_float(_metric_first(src, family + "_bars_since_cross", family + "_cross_bars"))
        if bars is not None: out[family + "_bars_since_cross"] = int(max(0, bars))
        for n in (1, 3):
            s20 = _metric_float(_metric_first(src, family + f"20_delta_{n}", family + f"_20_delta_{n}"))
            s60 = _metric_float(_metric_first(src, family + f"60_delta_{n}", family + f"_60_delta_{n}"))
            if s20 is not None: out[family + f"20_delta_{n}"] = s20
            if s60 is not None: out[family + f"60_delta_{n}"] = s60
        if price is not None:
            if ma20 is not None:
                out["price_vs_" + family + "20"] = _price_position(price, ma20)
                out["price_gap_" + family + "20_pct"] = _pct_gap(price, ma20)
            if ma60 is not None:
                out["price_vs_" + family + "60"] = _price_position(price, ma60)
                out["price_gap_" + family + "60_pct"] = _pct_gap(price, ma60)

    sma200 = _metric_float(_metric_first(src, "sma200", "sma_200"))
    if sma200 is not None:
        out["sma200"] = sma200
        if price is not None:
            out["price_vs_sma200"] = _price_position(price, sma200)
            out["price_gap_sma200_pct"] = _pct_gap(price, sma200)
    for n in (1, 3):
        dv = _metric_float(_metric_first(src, f"sma200_delta_{n}", f"sma_200_delta_{n}"))
        if dv is not None:
            out[f"sma200_delta_{n}"] = dv
    s20 = _metric_float(out.get("sma20")); s60 = _metric_float(out.get("sma60")); s200 = _metric_float(out.get("sma200"))
    if s20 is not None and s60 is not None and s200 is not None:
        if s20 > s60 > s200:
            out["sma_alignment"] = "BULL"
        elif s20 < s60 < s200:
            out["sma_alignment"] = "BEAR"
        else:
            out["sma_alignment"] = "MIXED"

    # Bollinger Band (20,2): compact price-location + volatility context.
    for key in ("bb_basis", "bb_upper", "bb_lower", "bb_percent_b", "bb_width_pct", "recent20_position_pct"):
        v = _metric_float(_metric_first(src, key))
        if v is not None:
            out[key] = v
    pb = _metric_float(out.get("bb_percent_b"))
    if pb is not None:
        if pb < 0: out["bb_zone"] = "BELOW_LOWER"
        elif pb <= 0.2: out["bb_zone"] = "LOWER_20"
        elif pb < 0.8: out["bb_zone"] = "MIDDLE"
        elif pb <= 1.0: out["bb_zone"] = "UPPER_20"
        else: out["bb_zone"] = "ABOVE_UPPER"

    # Candle / volatility / volume context. These are optional, but store now if Pine sends them.
    for key in (
        "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "volume_ratio",
        "atr", "atr_pct", "range_pct",
    ):
        v = _metric_float(_metric_first(src, key))
        if v is not None: out[key] = v

    out["metrics_schema_version"] = 94
    return out


def _prediction_detail_signature(metrics: dict[str, Any]) -> str:
    """Human-readable rich signature for later ranking; not used as the main grouping yet."""
    m = metrics or {}
    bits = []
    rv = m.get("rsi_value")
    bits.append(f"RSI={rv:.1f}" if isinstance(rv, (int, float)) else f"RSI={m.get('rsi_dir','NA')}")
    bits.append(f"R50={'UP' if m.get('rsi_above_50') else 'DOWN'}" if m.get("rsi_above_50") is not None else "R50=NA")
    for label, pfx in (("K5", "stoch_5_3"), ("K20", "stoch_20_12")):
        k, d = m.get(pfx + "_k"), m.get(pfx + "_d")
        if isinstance(k, (int, float)) and isinstance(d, (int, float)):
            bits.append(f"{label}={k:.1f}/{d:.1f}:{m.get(pfx+'_cross','NONE')}")
        else:
            bits.append(f"{label}={m.get(pfx+'_dir','NA')}")
    bits.append(f"SMA={m.get('sma_alignment',m.get('sma_state','NA'))}/{m.get('sma_cross','NONE')}")
    bits.append(f"BB={m.get('bb_zone','NA')}")
    return " | ".join(bits)


def _prediction_hash(payload: dict[str, Any]) -> str:
    symbol = canonical_performance_symbol(payload.get("symbol", ""))
    source_tf = str(payload.get("source_timeframe", "")).strip()
    target_tf = str(payload.get("target_timeframe", "")).strip()
    signal_time = str(payload.get("signal_time", "")).strip()
    return hashlib.sha256(f"{symbol}|{source_tf}|{target_tf}|{signal_time}".encode("utf-8")).hexdigest()


def save_prediction_snapshot(payload: dict[str, Any]) -> bool:
    if str(payload.get("event_type", "")).strip().upper() != "PREDICTION_SNAPSHOT_1Q":
        return False
    payload = _canonicalize_payload_symbol(payload)

    symbol = canonical_performance_symbol(payload.get("symbol", ""))
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
        "source_metrics": Jsonb(normalize_prediction_metrics(payload.get("source_metrics") or {})),
        "target_metrics": Jsonb(normalize_prediction_metrics(payload.get("target_metrics") or {})),
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
        # V94: keep confirmed 5m candles long enough to calculate MAE/MFE later.
        # This does not increase Telegram alerts; Pine already sends 5m candle payloads.
        try:
            target_m = _tf_minutes_for_prediction(target_tf)
            if target_m > 0:
                watch_until = snapshot_at + timedelta(minutes=max(15, target_m * 2))
                with _connect() as watch_conn:
                    watch_conn.execute(
                        """INSERT INTO performance_prediction_watch(symbol, active_until, updated_at)
                           VALUES (%s,%s,NOW())
                           ON CONFLICT(symbol) DO UPDATE SET
                             active_until=GREATEST(performance_prediction_watch.active_until, EXCLUDED.active_until),
                             updated_at=NOW()""",
                        (symbol, watch_until),
                    )
        except Exception:
            log.exception("Prediction candle watch activation failed symbol=%s", symbol)
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


def _tf_minutes_for_prediction(tf: str) -> int:
    mapping = {
        "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120,
        "4h": 240, "6h": 360, "12h": 720, "1d": 1440,
        "3d": 4320, "1w": 10080, "1m": 43200, "1M": 43200,
    }
    return mapping.get(str(tf or ""), 0)


def _prediction_signature(metrics: dict[str, Any]) -> str:
    m = metrics or {}
    parts = [
        f"RSI:{m.get('rsi_zone','NA')}/{m.get('rsi_dir','NA')}",
        f"K5:{m.get('stoch_5_3_zone','NA')}/{m.get('stoch_5_3_cross','NONE')}",
        f"K20:{m.get('stoch_20_12_zone','NA')}/{m.get('stoch_20_12_cross','NONE')}",
        f"SMA:{m.get('sma_alignment',m.get('sma_state','NA'))}",
        f"BB:{m.get('bb_zone','NA')}",
    ]
    return " | ".join(parts)


def _prediction_market_where(category_key: str | None) -> str:
    """Prediction research market filter.

    The snapshot table predates an explicit market column, so classify with the
    same stable signals used by the performance UI: exchange first, then symbol.
    Returned SQL is static (no user input interpolation).
    """
    category = str(category_key or "").strip().upper()
    exch = "UPPER(COALESCE(exchange,''))"
    raw = "UPPER(COALESCE(raw_exchange,''))"
    sym = "UPPER(COALESCE(symbol,''))"
    # IMPORTANT: Avoid literal '%' wildcards here. Psycopg parses '%' in a query
    # that also has bound parameters (the recent LIMIT query below), which can raise
    # ProgrammingError before PostgreSQL sees the SQL. POSITION/RIGHT are equivalent
    # for this market classification and don't conflict with parameter binding.
    coin = f"(({exch} ~ '(BINANCE|BYBIT|OKX|BITGET|UPBIT|BITHUMB|COINBASE|KRAKEN)') OR ({raw} ~ '(BINANCE|BYBIT|OKX|BITGET|UPBIT|BITHUMB|COINBASE|KRAKEN)') OR (POSITION('USDT' IN {sym}) > 0 OR POSITION('USDC' IN {sym}) > 0 OR RIGHT({sym}, 2) = '.P'))"
    korea = f"(({exch} ~ '(KRX|KOSPI|KOSDAQ|KONEX|KOREA)') OR ({raw} ~ '(KRX|KOSPI|KOSDAQ|KONEX|KOREA)') OR ({sym} ~ '^[0-9]{{6}}$'))"
    us = f"(({exch} ~ '(NASDAQ|NYSE|AMEX|ARCA|BATS|CBOE|OTC|USA|US)') OR ({raw} ~ '(NASDAQ|NYSE|AMEX|ARCA|BATS|CBOE|OTC|USA|US)') OR (NOT {coin} AND NOT {korea} AND {sym} ~ '^[A-Z][A-Z0-9.\\-]{{0,14}}$'))"
    if category == "COIN":
        return coin
    if category == "KOREA_1Q":
        return korea
    if category == "US_1Q":
        return us
    return "TRUE"


def prediction_research_summary(limit: int = 100, category_key: str | None = None) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 1000))
    category = str(category_key or "").strip().upper()
    if category not in {"COIN", "KOREA_1Q", "US_1Q"}:
        category = ""
    if not PERFORMANCE_DATABASE_URL:
        return {"ok": False, "database": "not_configured", "count": 0, "pairs": [], "recent": [], "patterns": [], "category_key": category}

    where_sql = _prediction_market_where(category)
    target_minutes_sql = """CASE target_timeframe
        WHEN '5m' THEN 5 WHEN '15m' THEN 15 WHEN '30m' THEN 30
        WHEN '1h' THEN 60 WHEN '2h' THEN 120 WHEN '4h' THEN 240
        WHEN '6h' THEN 360 WHEN '12h' THEN 720 WHEN '1d' THEN 1440
        WHEN '3d' THEN 4320 WHEN '1w' THEN 10080 WHEN '1m' THEN 43200 WHEN '1M' THEN 43200
        ELSE NULL END"""

    ensure_schema()
    with _connect() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM performance_prediction_snapshots WHERE {where_sql}").fetchone()[0]
        pairs_raw = conn.execute(
            f"""
            SELECT source_timeframe, target_timeframe, COUNT(*), COUNT(first_target_at),
                   AVG(lead_minutes) FILTER (WHERE first_target_at IS NOT NULL),
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lead_minutes)
                     FILTER (WHERE first_target_at IS NOT NULL),
                   COUNT(*) FILTER (WHERE first_target_at IS NOT NULL AND lead_minutes <= ({target_minutes_sql})),
                   COUNT(*) FILTER (WHERE first_target_at IS NOT NULL AND lead_minutes <= 2 * ({target_minutes_sql}))
            FROM performance_prediction_snapshots
            WHERE {where_sql}
            GROUP BY source_timeframe, target_timeframe
            ORDER BY
              CASE source_timeframe
                WHEN '5m' THEN 1 WHEN '15m' THEN 2 WHEN '30m' THEN 3 WHEN '1h' THEN 4
                WHEN '4h' THEN 5 WHEN '6h' THEN 6 WHEN '12h' THEN 7 WHEN '1d' THEN 8
                WHEN '3d' THEN 9 WHEN '1w' THEN 10 ELSE 99 END
            """
        ).fetchall()
        recent_raw = conn.execute(
            f"""
            SELECT id, exchange, symbol, source_timeframe, target_timeframe,
                   signal_price, snapshot_at, source_metrics, target_metrics,
                   first_target_at, lead_minutes
            FROM performance_prediction_snapshots
            WHERE {where_sql}
            ORDER BY snapshot_at DESC, id DESC
            LIMIT %s
            """,
            (safe_limit,),
        ).fetchall()

    pairs = []
    for r in pairs_raw:
        target_minutes = _tf_minutes_for_prediction(r[1])
        snapshots = int(r[2])
        matched = int(r[3])
        strict = int(r[6] or 0)
        extended = int(r[7] or 0)
        pairs.append({
            "source_timeframe": r[0], "target_timeframe": r[1],
            "snapshots": snapshots, "matched": matched,
            "conversion_rate_pct": (matched / snapshots * 100.0) if snapshots else None,
            "average_lead_minutes": float(r[4]) if r[4] is not None else None,
            "median_lead_minutes": float(r[5]) if r[5] is not None else None,
            "strict_window_minutes": target_minutes or None,
            "strict_match_rate_pct": (strict / snapshots * 100.0) if snapshots and target_minutes else None,
            "extended_match_rate_pct": (extended / snapshots * 100.0) if snapshots and target_minutes else None,
            "sampled_for_window": snapshots,
        })

    recent = []
    pattern_bucket: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in recent_raw:
        source_metrics = r[7] or {}
        target_metrics = r[8] or {}
        target_minutes = _tf_minutes_for_prediction(r[4])
        strict_success = bool(r[10] is not None and target_minutes and int(r[10]) <= target_minutes)
        sig = _prediction_signature(target_metrics)
        key = (r[3], r[4], sig)
        b = pattern_bucket.setdefault(key, {"source_timeframe": r[3], "target_timeframe": r[4], "signature": sig, "samples": 0, "strict_successes": 0, "matched": 0, "lead_values": []})
        b["samples"] += 1
        if strict_success:
            b["strict_successes"] += 1
        if r[9] is not None:
            b["matched"] += 1
        if r[10] is not None:
            b["lead_values"].append(float(r[10]))
        recent.append({
            "id": r[0], "exchange": r[1], "symbol": r[2],
            "source_timeframe": r[3], "target_timeframe": r[4],
            "signal_price": float(r[5]) if r[5] is not None else None,
            "snapshot_at": r[6].isoformat() if r[6] else None,
            "source_metrics": source_metrics, "target_metrics": target_metrics,
            "source_signature": _prediction_signature(source_metrics),
            "source_detail_signature": _prediction_detail_signature(source_metrics),
            "target_detail_signature": _prediction_detail_signature(target_metrics),
            "target_signature": sig,
            "first_target_at": r[9].isoformat() if r[9] else None,
            "lead_minutes": r[10],
            "strict_window_minutes": target_minutes or None,
            "strict_success": strict_success,
        })

    patterns = []
    for b in pattern_bucket.values():
        leads = b.pop("lead_values")
        b["strict_success_rate_pct"] = (b["strict_successes"] / b["samples"] * 100.0) if b["samples"] else None
        b["matched_rate_pct"] = (b["matched"] / b["samples"] * 100.0) if b["samples"] else None
        b["average_lead_minutes"] = (sum(leads) / len(leads)) if leads else None
        patterns.append(b)
    patterns.sort(key=lambda x: (-x["samples"], -(x["strict_success_rate_pct"] or 0)))

    return {
        "ok": True,
        "category_key": category,
        "count": int(total),
        "pairs": pairs,
        "patterns": patterns[:30],
        "recent": recent,
        "window_note": "엄격 성공은 상위 목표 시간봉 길이 안에 실제 목표 저점이 처음 발생한 경우입니다. 확장 성공은 그 2배 시간 안에 발생한 경우입니다.",
    }



def save_signal(payload: dict[str, Any]) -> bool:
    """Save one eligible TradingView signal. Returns True if inserted."""
    payload = _canonicalize_payload_symbol(payload)
    route = str(payload.get("route", payload.get("type", ""))).strip().upper()
    if not is_performance_route(route):
        return False

    message = str(payload.get("msg", payload.get("message", ""))).strip()
    symbol = canonical_performance_symbol(payload.get("symbol", ""))
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


def save_cadence_stage_event(payload: dict[str, Any]) -> bool:
    """Persist visible stages 0~3 and admin-only research stages 4~5."""
    payload = _canonicalize_payload_symbol(payload)
    symbol = canonical_performance_symbol(payload.get("symbol", ""))
    timeframe = str(payload.get("timeframe", "")).strip()
    direction = str(payload.get("direction", "")).strip().upper()
    if not symbol or not timeframe or direction not in {"LOW", "HIGH"}:
        return False
    stage = int(payload.get("stage", 0) or 0)
    if stage < 0 or stage > 5:
        return False
    occurred_at = datetime.fromtimestamp(float(payload.get("occurred_at_ts", 0) or 0), tz=timezone.utc)
    episode_at = datetime.fromtimestamp(float(payload.get("episode_started_ts", 0) or 0), tz=timezone.utc)
    if occurred_at.year < 2020 or episode_at.year < 2020:
        return False
    route_family = str(payload.get("route_family", "")).strip() or "UNKNOWN"
    route = str(payload.get("route", "")).strip() or None
    try:
        price = Decimal(str(payload.get("signal_price"))) if payload.get("signal_price") is not None else None
    except Exception:
        price = None
    event_key = f"{symbol}|{route_family}|{direction}|{timeframe}|{episode_at.isoformat()}|{stage}"
    event_hash = hashlib.sha256(event_key.encode("utf-8")).hexdigest()
    label = "FOCUS" if stage == 0 else f"VALID_{stage}"
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            """INSERT INTO performance_cadence_stage_events(
                   route_family,route,exchange,raw_exchange,symbol,direction,timeframe,
                   stage,stage_label,telegram_visible,signal_price,episode_started_at,occurred_at,raw_message,event_hash
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT(event_hash) DO NOTHING RETURNING id""",
            (route_family, route, payload.get("exchange"), payload.get("raw_exchange"), symbol,
             direction, timeframe, stage, label, bool(payload.get("telegram_visible")), price,
             episode_at, occurred_at, str(payload.get("raw_message", "")) or None, event_hash),
        ).fetchone()
    return bool(row)


def queue_cadence_stage_event_save(payload: dict[str, Any]) -> None:
    snapshot = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    _submit_db_save(save_cadence_stage_event, snapshot)


def health_summary() -> dict[str, Any]:
    """Return a non-sensitive connectivity and row-count summary."""
    if not PERFORMANCE_DATABASE_URL:
        return {"ok": False, "database": "not_configured", "signal_count": 0}
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*), MAX(received_at) FROM performance_signals"
        ).fetchone()
        mig = conn.execute(
            "SELECT applied_at, detail FROM performance_data_migrations WHERE migration_key=%s",
            ("v99_sol_sui_futures_to_spot",),
        ).fetchone()
    return {
        "ok": True,
        "database": "connected",
        "signal_count": int(row[0]),
        "latest_signal_at": row[1].isoformat() if row[1] else None,
        "v99_spot_symbol_migration": {
            "applied": bool(mig),
            "applied_at": mig[0].isoformat() if mig and mig[0] else None,
            "detail": mig[1] if mig else None,
        },
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


def signals_for_symbol(symbol: str, limit: int = 10) -> list[dict[str, Any]]:
    """Return recent saved signals for one canonical symbol.

    Mobile-app read API helper only. It does not alter signal storage, Telegram delivery,
    cadence handling, or performance calculations.
    """
    safe_limit = max(1, min(int(limit), 50))
    canonical = canonical_performance_symbol(symbol)
    if not canonical or not PERFORMANCE_DATABASE_URL:
        return []
    ensure_schema()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, strategy, route, exchange, raw_exchange, symbol,
                   side, signal_type, timeframe, timeframe_minutes,
                   signal_price, received_at, raw_message
            FROM performance_signals
            WHERE symbol=%s
            ORDER BY received_at DESC, id DESC
            LIMIT %s
            """,
            (canonical, safe_limit),
        ).fetchall()
    return [
        {
            "id": row[0],
            "strategy": row[1],
            "route": row[2],
            "exchange": row[3],
            "raw_exchange": row[4],
            "symbol": canonical_performance_symbol(row[5]),
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



def recent_cadence_alerts(symbols: list[str], limit: int = 50) -> list[dict[str, Any]]:
    """Return Telegram-visible cadence stages for the requested symbols.

    This is a read-only mobile-app helper. It exposes only the same stages that are
    eligible for Telegram delivery: focus(stage 0) and valid stages 1~3. Hidden
    research stages 4~5 and suppressed raw signals are intentionally excluded.
    """
    safe_limit = max(1, min(int(limit), 100))
    canonical_symbols: list[str] = []
    seen: set[str] = set()
    for raw in symbols or []:
        symbol = canonical_performance_symbol(raw)
        if symbol and symbol not in seen:
            seen.add(symbol)
            canonical_symbols.append(symbol)

    if not canonical_symbols or not PERFORMANCE_DATABASE_URL:
        return []

    ensure_schema()
    placeholders = ",".join(["%s"] * len(canonical_symbols))
    params: list[Any] = [*canonical_symbols, safe_limit]
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT id, route_family, route, symbol, direction, timeframe,
                   stage, stage_label, telegram_visible, signal_price,
                   episode_started_at, occurred_at, raw_message
            FROM performance_cadence_stage_events
            WHERE telegram_visible=TRUE
              AND stage BETWEEN 0 AND 3
              AND symbol IN ({placeholders})
            ORDER BY occurred_at DESC, id DESC
            LIMIT %s
            """,
            params,
        ).fetchall()

    return [
        {
            "id": row[0],
            "route_family": row[1],
            "route": row[2],
            "symbol": canonical_performance_symbol(row[3]),
            "direction": row[4],
            "timeframe": row[5],
            "stage": int(row[6]),
            "stage_label": row[7],
            "telegram_visible": bool(row[8]),
            "signal_price": float(row[9]) if row[9] is not None else None,
            "episode_started_at": row[10].isoformat() if row[10] else None,
            "occurred_at": row[11].isoformat() if row[11] else None,
            "raw_message": row[12],
        }
        for row in rows
    ]


def _prune_app_push_histories(conn: psycopg.Connection, device_ids: list[str]) -> None:
    """Batch retention cleanup: max 300 rows and max 30 days per device."""
    clean_ids = list({str(value or "").strip()[:128] for value in device_ids if str(value or "").strip()})
    if not clean_ids:
        return
    conn.execute(
        """
        DELETE FROM tajum_app_push_history
        WHERE device_id = ANY(%s)
          AND occurred_at < NOW() - INTERVAL '30 days'
        """,
        (clean_ids,),
    )
    conn.execute(
        """
        DELETE FROM tajum_app_push_history
        WHERE id IN (
            SELECT id
            FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY device_id
                           ORDER BY occurred_at DESC, id DESC
                       ) AS rn
                FROM tajum_app_push_history
                WHERE device_id = ANY(%s)
            ) ranked
            WHERE rn > 300
        )
        """,
        (clean_ids,),
    )


def _prune_app_push_history(conn: psycopg.Connection, device_id: str) -> None:
    _prune_app_push_histories(conn, [device_id])


def app_devices_for_symbol(symbol: Optional[str] = None, limit: int = 500) -> list[dict[str, Any]]:
    """Return enabled device ids with their FCM tokens for accurate delivery logging."""
    if not PERFORMANCE_DATABASE_URL:
        return []
    safe_limit = max(1, min(int(limit), 500))
    canonical = canonical_performance_symbol(symbol) if symbol else ""
    ensure_schema()
    with _connect() as conn:
        if canonical:
            rows = conn.execute(
                """
                SELECT device_id, fcm_token, sound_profile, vibration_enabled, enabled_signal_groups
                FROM tajum_app_devices
                WHERE notifications_enabled=TRUE
                  AND %s = ANY(enabled_symbols)
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (canonical, safe_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT device_id, fcm_token, sound_profile, vibration_enabled, enabled_signal_groups
                FROM tajum_app_devices
                WHERE notifications_enabled=TRUE
                  AND cardinality(enabled_symbols) > 0
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (safe_limit,),
            ).fetchall()
    return [
        {
            "device_id": str(row[0] or "").strip(),
            "fcm_token": str(row[1] or "").strip(),
            "sound_profile": str(row[2] or "clear").strip().lower() or "clear",
            "vibration_enabled": bool(row[3]),
            "enabled_signal_groups": row[4] if isinstance(row[4], dict) else {},
        }
        for row in rows
        if row and str(row[0] or "").strip() and str(row[1] or "").strip()
    ]


def filter_app_devices_by_push_cooldown(
    devices: list[dict[str, Any]],
    symbol: str,
    side: str,
    timeframes: list[str],
    cooldown_minutes: int = 5,
) -> tuple[list[dict[str, Any]], int]:
    """Block duplicate app pushes for the same device/symbol/side/timeframe-group.

    Telegram cadence remains untouched. This is an app-delivery guard so that,
    for example, 5m and 15m signals in the same SCALP group do not produce two
    phone pushes one minute apart. The database is the source of truth, which
    keeps the guard shared across Render workers.
    """
    if not devices or not PERFORMANCE_DATABASE_URL:
        return list(devices or []), 0

    device_ids = []
    seen_ids: set[str] = set()
    for device in devices:
        device_id = str((device or {}).get("device_id", "") or "").strip()[:128]
        if device_id and device_id not in seen_ids:
            seen_ids.add(device_id)
            device_ids.append(device_id)
    canonical = canonical_performance_symbol(symbol)[:100]
    clean_side = str(side or "").strip().upper()[:10]
    clean_timeframes = []
    seen_tf: set[str] = set()
    for raw in timeframes or []:
        tf = str(raw or "").strip()[:10]
        if tf and tf not in seen_tf:
            seen_tf.add(tf)
            clean_timeframes.append(tf)
    try:
        safe_minutes = max(1, min(int(cooldown_minutes), 120))
    except (TypeError, ValueError):
        safe_minutes = 5

    if not device_ids or not canonical or not clean_side or not clean_timeframes:
        return list(devices), 0

    ensure_schema()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT device_id
            FROM tajum_app_push_history
            WHERE device_id = ANY(%s)
              AND symbol=%s
              AND COALESCE(side, '')=%s
              AND timeframe = ANY(%s)
              AND occurred_at > NOW() - (%s * INTERVAL '1 minute')
            """,
            (device_ids, canonical, clean_side, clean_timeframes, safe_minutes),
        ).fetchall()
    blocked = {str(row[0] or "").strip() for row in rows if row and row[0]}
    if not blocked:
        return list(devices), 0
    allowed = [
        device for device in devices
        if str((device or {}).get("device_id", "") or "").strip() not in blocked
    ]
    return allowed, len(blocked)


def save_app_push_history(deliveries: list[dict[str, Any]]) -> int:
    """Persist only FCM deliveries that Firebase reported as successful."""
    if not PERFORMANCE_DATABASE_URL or not deliveries:
        return 0
    ensure_schema()
    saved = 0
    touched_devices: set[str] = set()
    with _connect() as conn:
        for raw in deliveries:
            item = dict(raw or {})
            device_id = str(item.get("device_id", "") or "").strip()[:128]
            delivery_key = str(item.get("delivery_key", "") or "").strip()[:64]
            symbol = canonical_performance_symbol(item.get("symbol", ""))[:100]
            if not device_id or not delivery_key or not symbol:
                continue
            try:
                stage = max(0, min(int(item.get("stage", 0) or 0), 3))
            except (TypeError, ValueError):
                stage = 0
            price = item.get("signal_price")
            try:
                if isinstance(price, str):
                    price = price.replace(",", "").strip() or None
                price = Decimal(str(price)) if price is not None else None
            except (InvalidOperation, ValueError, TypeError):
                price = None
            occurred_at = item.get("occurred_at")
            if isinstance(occurred_at, str):
                try:
                    occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
                except ValueError:
                    occurred_at = None
            if not isinstance(occurred_at, datetime):
                occurred_at = datetime.now(timezone.utc)
            cur = conn.execute(
                """
                INSERT INTO tajum_app_push_history(
                    device_id, delivery_key, symbol, display, market, exchange,
                    direction, side, timeframe, stage, alert_label, signal_price,
                    route, occurred_at, created_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (device_id, delivery_key) DO NOTHING
                """,
                (
                    device_id, delivery_key, symbol,
                    str(item.get("display", "") or "")[:220] or symbol,
                    str(item.get("market", "") or "")[:20] or None,
                    str(item.get("exchange", "") or "")[:30] or None,
                    str(item.get("direction", "") or "")[:10] or None,
                    str(item.get("side", "") or "")[:10] or None,
                    str(item.get("timeframe", "") or "")[:10] or None,
                    stage,
                    str(item.get("alert_label", "") or "")[:120] or None,
                    price,
                    str(item.get("route", "") or "")[:50] or None,
                    occurred_at,
                ),
            )
            saved += int(cur.rowcount or 0)
            touched_devices.add(device_id)
        _prune_app_push_histories(conn, list(touched_devices))
    return saved


def recent_app_push_history(
    device_id: str,
    symbols: Optional[list[str]] = None,
    limit: int = 300,
    max_days: int = 30,
) -> list[dict[str, Any]]:
    """Return this device's real delivered push inbox (max 300 / 30 days)."""
    clean_device_id = str(device_id or "").strip()[:128]
    if not clean_device_id or not PERFORMANCE_DATABASE_URL:
        return []
    safe_limit = max(1, min(int(limit), 300))
    safe_days = max(1, min(int(max_days), 30))
    canonical_symbols: list[str] = []
    seen: set[str] = set()
    for raw in symbols or []:
        symbol = canonical_performance_symbol(raw)
        if symbol and symbol not in seen:
            seen.add(symbol)
            canonical_symbols.append(symbol)

    ensure_schema()
    with _connect() as conn:
        _prune_app_push_history(conn, clean_device_id)
        if canonical_symbols:
            rows = conn.execute(
                """
                SELECT id, symbol, display, market, exchange, direction, side,
                       timeframe, stage, alert_label, signal_price, route, occurred_at
                FROM tajum_app_push_history
                WHERE device_id=%s
                  AND symbol = ANY(%s)
                  AND occurred_at >= NOW() - (%s * INTERVAL '1 day')
                ORDER BY occurred_at DESC, id DESC
                LIMIT %s
                """,
                (clean_device_id, canonical_symbols, safe_days, safe_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, symbol, display, market, exchange, direction, side,
                       timeframe, stage, alert_label, signal_price, route, occurred_at
                FROM tajum_app_push_history
                WHERE device_id=%s
                  AND occurred_at >= NOW() - (%s * INTERVAL '1 day')
                ORDER BY occurred_at DESC, id DESC
                LIMIT %s
                """,
                (clean_device_id, safe_days, safe_limit),
            ).fetchall()
    return [
        {
            "id": row[0], "symbol": canonical_performance_symbol(row[1]),
            "display": row[2], "market": row[3], "exchange": row[4],
            "direction": row[5], "side": row[6], "timeframe": row[7],
            "stage": int(row[8] or 0), "alert_label": row[9],
            "signal_price": float(row[10]) if row[10] is not None else None,
            "route": row[11],
            "occurred_at": row[12].isoformat() if row[12] else None,
            "received_at": row[12].isoformat() if row[12] else None,
        }
        for row in rows
    ]


def upsert_app_device(
    device_id: str,
    fcm_token: str,
    enabled_symbols: list[str],
    notifications_enabled: bool = True,
    platform: str = "android",
    user_uid: Optional[str] = None,
    sound_profile: str = "clear",
    vibration_enabled: bool = True,
    enabled_signal_groups: Optional[dict[str, list[str]]] = None,
) -> dict[str, Any]:
    """Register or refresh one Tajum On installation for push delivery.

    The device is anonymous for now. When Firebase phone authentication is added,
    the same row can be bound to user_uid without changing the push routing model.
    """
    if not PERFORMANCE_DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")

    clean_device_id = str(device_id or "").strip()[:128]
    clean_token = str(fcm_token or "").strip()
    clean_platform = str(platform or "android").strip().lower()[:20] or "android"
    clean_uid = str(user_uid or "").strip()[:128] or None
    allowed_sound_profiles = {
        "system", "spark", "cash", "siren", "clear", "soft", "bright", "deep", "pulse", "silent"
    }
    clean_sound_profile = str(sound_profile or "clear").strip().lower()[:24]
    is_custom_sound = bool(re.fullmatch(r"custom_[0-9a-f]{12}", clean_sound_profile))
    if clean_sound_profile not in allowed_sound_profiles and not is_custom_sound:
        clean_sound_profile = "clear"
    clean_vibration_enabled = bool(vibration_enabled)
    if not clean_device_id:
        raise ValueError("device_id is required")
    if not clean_token or len(clean_token) < 20:
        raise ValueError("valid fcm_token is required")
    if len(clean_token) > 4096:
        raise ValueError("fcm_token is too long")

    canonical_symbols: list[str] = []
    seen: set[str] = set()
    for raw in enabled_symbols or []:
        symbol = canonical_performance_symbol(raw)
        if symbol and symbol not in seen:
            seen.add(symbol)
            canonical_symbols.append(symbol)
        if len(canonical_symbols) >= 100:
            break

    ensure_schema()
    with _connect() as conn:
        previous_row = conn.execute(
            "SELECT enabled_symbols, enabled_signal_groups FROM tajum_app_devices WHERE device_id=%s",
            (clean_device_id,),
        ).fetchone()
        previous_symbols = set(previous_row[0] or []) if previous_row else set()
        previous_group_prefs = (
            dict(previous_row[1])
            if previous_row and isinstance(previous_row[1], dict)
            else {}
        )

        allowed_groups = {"SCALP", "SWING", "LONG", "LIFE"}
        if enabled_signal_groups is None:
            clean_signal_groups: dict[str, list[str]] = previous_group_prefs
        else:
            clean_signal_groups = {}
            for raw_symbol, raw_groups in enabled_signal_groups.items():
                symbol = canonical_performance_symbol(raw_symbol)
                if symbol not in canonical_symbols:
                    continue
                if not isinstance(raw_groups, list):
                    continue
                groups: list[str] = []
                for raw_group in raw_groups:
                    group = str(raw_group or "").strip().upper()
                    if group in allowed_groups and group not in groups:
                        groups.append(group)
                clean_signal_groups[symbol] = groups

        # FCM tokens can rotate/rebind. Keep one authoritative installation row per token.
        conn.execute(
            "DELETE FROM tajum_app_devices WHERE fcm_token=%s AND device_id<>%s",
            (clean_token, clean_device_id),
        )
        conn.execute(
            """
            INSERT INTO tajum_app_devices(
                device_id,user_uid,fcm_token,platform,enabled_symbols,
                notifications_enabled,sound_profile,vibration_enabled,
                enabled_signal_groups,created_at,updated_at,last_seen_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW(),NOW())
            ON CONFLICT (device_id) DO UPDATE SET
                user_uid=COALESCE(EXCLUDED.user_uid,tajum_app_devices.user_uid),
                fcm_token=EXCLUDED.fcm_token,
                platform=EXCLUDED.platform,
                enabled_symbols=EXCLUDED.enabled_symbols,
                notifications_enabled=EXCLUDED.notifications_enabled,
                sound_profile=EXCLUDED.sound_profile,
                vibration_enabled=EXCLUDED.vibration_enabled,
                enabled_signal_groups=EXCLUDED.enabled_signal_groups,
                updated_at=NOW(),
                last_seen_at=NOW()
            """,
            (
                clean_device_id,
                clean_uid,
                clean_token,
                clean_platform,
                canonical_symbols,
                bool(notifications_enabled),
                clean_sound_profile,
                clean_vibration_enabled,
                Jsonb(clean_signal_groups),
            ),
        )

        # Disabled/removed symbols must disappear from this device inbox permanently.
        removed_symbols = previous_symbols.difference(canonical_symbols)
        if removed_symbols:
            conn.execute(
                "DELETE FROM tajum_app_push_history WHERE device_id=%s AND symbol = ANY(%s)",
                (clean_device_id, list(removed_symbols)),
            )
        if not canonical_symbols:
            conn.execute(
                "DELETE FROM tajum_app_push_history WHERE device_id=%s",
                (clean_device_id,),
            )
        _prune_app_push_history(conn, clean_device_id)
    return {
        "device_id": clean_device_id,
        "platform": clean_platform,
        "enabled_symbols": canonical_symbols,
        "notifications_enabled": bool(notifications_enabled),
        "sound_profile": clean_sound_profile,
        "vibration_enabled": clean_vibration_enabled,
        "enabled_signal_groups": clean_signal_groups,
    }


def app_device_tokens_for_symbol(symbol: Optional[str] = None, limit: int = 500) -> list[str]:
    """Return currently enabled FCM registration tokens, optionally filtered by symbol."""
    if not PERFORMANCE_DATABASE_URL:
        return []
    safe_limit = max(1, min(int(limit), 500))
    canonical = canonical_performance_symbol(symbol) if symbol else ""
    ensure_schema()
    with _connect() as conn:
        if canonical:
            rows = conn.execute(
                """
                SELECT fcm_token
                FROM tajum_app_devices
                WHERE notifications_enabled=TRUE
                  AND %s = ANY(enabled_symbols)
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (canonical, safe_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT fcm_token
                FROM tajum_app_devices
                WHERE notifications_enabled=TRUE
                  AND cardinality(enabled_symbols) > 0
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (safe_limit,),
            ).fetchall()
    return [str(row[0]).strip() for row in rows if row and str(row[0] or "").strip()]


def remove_app_device_token(fcm_token: str) -> int:
    """Delete an invalid/expired FCM token after Firebase reports it unusable."""
    token = str(fcm_token or "").strip()
    if not token or not PERFORMANCE_DATABASE_URL:
        return 0
    ensure_schema()
    with _connect() as conn:
        cur = conn.execute("DELETE FROM tajum_app_devices WHERE fcm_token=%s", (token,))
        return int(cur.rowcount or 0)


def app_device_summary() -> dict[str, Any]:
    if not PERFORMANCE_DATABASE_URL:
        return {"database": "not_configured", "device_count": 0, "push_enabled_count": 0}
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE notifications_enabled=TRUE AND cardinality(enabled_symbols)>0),
                   MAX(updated_at)
            FROM tajum_app_devices
            """
        ).fetchone()
    return {
        "database": "connected",
        "device_count": int(row[0] or 0),
        "push_enabled_count": int(row[1] or 0),
        "latest_device_update_at": row[2].isoformat() if row[2] else None,
    }

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
    payload = _canonicalize_payload_symbol(payload)
    symbol = canonical_performance_symbol(payload.get("symbol", ""))
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
        position_required = bool(watch and watch[0] and ((interval == 1 and watch[2]) or (interval == 5 and watch[3])))
        prediction_required = False
        if interval == 5:
            prow = conn.execute(
                "SELECT active_until FROM performance_prediction_watch WHERE symbol=%s AND active_until >= %s",
                (symbol, bar_time),
            ).fetchone()
            prediction_required = bool(prow)
        required = position_required or prediction_required
        if not required:
            return False
        if position_required and watch and bar_time < watch[1] - timedelta(minutes=interval) and not prediction_required:
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



def known_symbols(limit: int = 1500) -> list[dict[str, Any]]:
    """Return symbols that the server has actually received in performance signals.

    This is intentionally a lightweight DISTINCT-style lookup for the mobile app.
    It does not calculate performance statistics and does not affect Telegram delivery.
    """
    safe_limit = max(1, min(int(limit), 2000))
    if not PERFORMANCE_DATABASE_URL:
        return []
    ensure_schema()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT symbol,
                   MAX(NULLIF(exchange, '')) AS exchange,
                   MAX(NULLIF(raw_exchange, '')) AS raw_exchange,
                   MAX(received_at) AS latest_at
            FROM performance_signals
            WHERE COALESCE(symbol, '') <> ''
            GROUP BY symbol
            ORDER BY MAX(received_at) DESC, symbol ASC
            LIMIT %s
            """,
            (safe_limit,),
        ).fetchall()

    output = []
    seen = set()
    for row in rows:
        symbol = canonical_performance_symbol(row[0])
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        output.append(
            {
                "symbol": symbol,
                "exchange": row[1] or "",
                "raw_exchange": row[2] or "",
                "latest_at": row[3].isoformat() if row[3] else None,
            }
        )
    return output


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
