"""Tajum On core signal engine - V147.

Production signal math only. Legacy TradingView comparison/diagnostic code has been removed. This module is shared by the direct market workers.

Continuous coin chain:
1w -> 1d -> 12h -> 6h -> 4h -> 2h -> 1h -> 30m -> 15m -> 5m
2h is an internal chain gate. 6h is a normal chain member.
"""
from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

BINANCE_SPOT_BASE_URL = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
REQUEST_TIMEOUT_SEC = max(3, min(int(os.getenv("SERVER_ENGINE_HTTP_TIMEOUT_SEC", "10") or 10), 30))
KLINE_LIMIT = max(100, min(int(os.getenv("SERVER_ENGINE_KLINE_LIMIT", "300") or 300), 1000))

# Kept as a dynamic compatibility map because auto_exchange_engine adds member symbols at runtime.
SUPPORTED_SYMBOLS: dict[str, str] = {}
TF_ORDER = ("1w", "1d", "12h", "6h", "4h", "2h", "1h", "30m", "15m", "5m")
MAX_CANDIDATES = frozenset(("1w", "1d", "12h", "6h", "4h", "1h", "30m", "15m", "5m"))
INTERNAL_ONLY_TFS = frozenset(("2h",))
TF_MINUTES = {
    "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120,
    "4h": 240, "6h": 360, "12h": 720, "1d": 1440, "1w": 10080,
}
TF_ICON = {
    "1w": "💎", "1d": "✨", "12h": "⭐", "6h": "💚", "4h": "🧡",
    "2h": "❤️", "1h": "🟢", "30m": "🟠", "15m": "🔺", "5m": "△",
}
RSI_LENGTH = 14
RSI_OS = 30.0
RSI_OB = 70.0
K_OS = 20.0
K_OB = 80.0

def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").upper().replace("BINANCE:", "").strip()
    if not symbol or len(symbol) > 30 or not all(ch.isalnum() or ch in ".-_" for ch in symbol):
        raise ValueError(f"invalid symbol {symbol!r}")
    return symbol

def _safe_float(value: Any) -> float:
    out = float(value)
    if not math.isfinite(out):
        raise ValueError(f"non-finite numeric value: {value!r}")
    return out

def _fetch_klines(
    symbol: str,
    interval: str,
    *,
    limit: int = KLINE_LIMIT,
    end_time_ms: int | None = None,
) -> list[dict[str, float | int]]:
    url = f"{BINANCE_SPOT_BASE_URL}/api/v3/klines"
    params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time_ms is not None:
        params["endTime"] = int(end_time_ms)

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"empty/invalid Binance kline response for {symbol} {interval}")

    rows: list[dict[str, float | int]] = []
    for item in payload:
        if not isinstance(item, list) or len(item) < 7:
            continue
        rows.append({
            "open_time": int(item[0]),
            "open": _safe_float(item[1]),
            "high": _safe_float(item[2]),
            "low": _safe_float(item[3]),
            "close": _safe_float(item[4]),
            "volume": _safe_float(item[5]),
            "close_time": int(item[6]),
        })
    if len(rows) < 50 and interval not in ("1m", "1h"):
        raise RuntimeError(f"not enough Binance candles for {symbol} {interval}: {len(rows)}")
    return rows

def _pine_rsi_series(closes: Iterable[float], length: int = RSI_LENGTH) -> list[float | None]:
    """TradingView ta.rsi-compatible Wilder RSI using RMA with SMA seed."""
    values = [float(v) for v in closes]
    n = len(values)
    result: list[float | None] = [None] * n
    if n <= length:
        return result

    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        change = values[i] - values[i - 1]
        gains[i] = max(change, 0.0)
        losses[i] = max(-change, 0.0)

    avg_gain = sum(gains[1:length + 1]) / length
    avg_loss = sum(losses[1:length + 1]) / length

    def rsi_from_avgs(gain: float, loss: float) -> float:
        if loss == 0.0:
            return 100.0 if gain > 0.0 else 50.0
        if gain == 0.0:
            return 0.0
        rs = gain / loss
        return 100.0 - (100.0 / (1.0 + rs))

    result[length] = rsi_from_avgs(avg_gain, avg_loss)
    alpha = 1.0 / length
    for i in range(length + 1, n):
        avg_gain = alpha * gains[i] + (1.0 - alpha) * avg_gain
        avg_loss = alpha * losses[i] + (1.0 - alpha) * avg_loss
        result[i] = rsi_from_avgs(avg_gain, avg_loss)
    return result

def _pine_slowk_series(
    highs: Iterable[float],
    lows: Iterable[float],
    closes: Iterable[float],
    len_k: int,
    smooth: int,
) -> list[float | None]:
    """Replicate Pine f_slowk_expr(): raw %K over len_k then SMA(rawK, smooth)."""
    h = [float(v) for v in highs]
    l = [float(v) for v in lows]
    c = [float(v) for v in closes]
    n = len(c)
    fk: list[float | None] = [None] * n
    out: list[float | None] = [None] * n

    for i in range(len_k - 1, n):
        lo = min(l[i - len_k + 1:i + 1])
        hi = max(h[i - len_k + 1:i + 1])
        fk[i] = 0.0 if hi == lo else (c[i] - lo) / (hi - lo) * 100.0

    first_slow = (len_k - 1) + smooth - 1
    for i in range(first_slow, n):
        window = fk[i - smooth + 1:i + 1]
        if any(v is None for v in window):
            continue
        out[i] = sum(float(v) for v in window) / smooth
    return out

def _latest_metric(rows: list[dict[str, float | int]], *, evaluation_time_ms: int) -> dict[str, Any]:
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]

    rsi = _pine_rsi_series(closes, RSI_LENGTH)
    k5 = _pine_slowk_series(highs, lows, closes, 5, 3)
    k20 = _pine_slowk_series(highs, lows, closes, 20, 12)

    r = rsi[-1]
    k5v = k5[-1]
    k20v = k20[-1]
    if r is None or k5v is None or k20v is None:
        raise RuntimeError("indicator warm-up incomplete")

    os_basic = r <= RSI_OS and k5v <= K_OS
    ob_basic = r >= RSI_OB and k5v >= K_OB
    os_all = os_basic and k20v <= K_OS
    ob_all = ob_basic and k20v >= K_OB

    last = rows[-1]
    return {
        "rsi14": r,
        "stoch_5_3_k": k5v,
        "stoch_20_12_k": k20v,
        "os_basic": os_basic,
        "ob_basic": ob_basic,
        "os_all": os_all,
        "ob_all": ob_all,
        "bar_open_time_ms": int(last["open_time"]),
        "bar_close_time_ms": int(last["close_time"]),
        "bar_closed_at_evaluation": int(last["close_time"]) < int(evaluation_time_ms),
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "close": float(last["close"]),
        "candle_count": len(rows),
    }

def _bucket_open_ms(evaluation_time_ms: int, timeframe_minutes: int) -> int:
    """Return Binance candle open containing the millisecond before evaluation.

    All intervals except 1w align naturally to Unix-epoch multiples in UTC.
    Binance 1w candles align to Monday 00:00 UTC, so weekly needs a Monday anchor.
    """
    point_ms = int(evaluation_time_ms) - 1
    tf_ms = int(timeframe_minutes) * 60_000
    if timeframe_minutes == 10080:
        monday_anchor_ms = 4 * 24 * 60 * 60 * 1000  # 1970-01-05 00:00 UTC
        return monday_anchor_ms + ((point_ms - monday_anchor_ms) // tf_ms) * tf_ms
    return (point_ms // tf_ms) * tf_ms

def _partial_from_rows(
    rows: list[dict[str, float | int]],
    *,
    timeframe_minutes: int,
    evaluation_time_ms: int,
) -> dict[str, float | int]:
    bucket_open = _bucket_open_ms(evaluation_time_ms, timeframe_minutes)
    selected = [
        row for row in rows
        if int(row["open_time"]) >= bucket_open and int(row["close_time"]) < int(evaluation_time_ms)
    ]
    if not selected:
        raise RuntimeError(
            f"no finalized base candles for {timeframe_minutes}m bucket at evaluation_time_ms={evaluation_time_ms}"
        )
    return {
        "open_time": bucket_open,
        "open": float(selected[0]["open"]),
        "high": max(float(row["high"]) for row in selected),
        "low": min(float(row["low"]) for row in selected),
        "close": float(selected[-1]["close"]),
        "volume": sum(float(row["volume"]) for row in selected),
        "close_time": int(evaluation_time_ms) - 1,
    }

def _current_hour_partial(
    one_minute_rows: list[dict[str, float | int]],
    *,
    evaluation_time_ms: int,
) -> dict[str, float | int]:
    return _partial_from_rows(
        one_minute_rows,
        timeframe_minutes=60,
        evaluation_time_ms=evaluation_time_ms,
    )

def _target_partial(
    timeframe: str,
    *,
    evaluation_time_ms: int,
    one_minute_rows: list[dict[str, float | int]],
    one_hour_rows: list[dict[str, float | int]],
) -> dict[str, float | int]:
    tf_min = TF_MINUTES[timeframe]
    if tf_min < 60:
        return _partial_from_rows(
            one_minute_rows,
            timeframe_minutes=tf_min,
            evaluation_time_ms=evaluation_time_ms,
        )

    hour_open = _bucket_open_ms(evaluation_time_ms, 60)
    closed_prior_hours = [row for row in one_hour_rows if int(row["open_time"]) < hour_open]
    hour_partial = _current_hour_partial(one_minute_rows, evaluation_time_ms=evaluation_time_ms)
    base_hours = closed_prior_hours + [hour_partial]
    return _partial_from_rows(
        base_hours,
        timeframe_minutes=tf_min,
        evaluation_time_ms=evaluation_time_ms,
    )

def _rows_at_evaluation(
    symbol: str,
    timeframe: str,
    *,
    evaluation_time_ms: int,
    one_minute_rows: list[dict[str, float | int]],
    one_hour_rows: list[dict[str, float | int]],
) -> list[dict[str, float | int]]:
    tf_min = TF_MINUTES[timeframe]
    bucket_open = _bucket_open_ms(evaluation_time_ms, tf_min)

    # 1h base was already fetched for partial reconstruction, so reuse it and
    # avoid one duplicate Binance request per symbol/minute.
    if timeframe == "1h":
        rows = list(one_hour_rows)
    else:
        rows = _fetch_klines(
            symbol,
            timeframe,
            limit=KLINE_LIMIT,
            end_time_ms=int(evaluation_time_ms) - 1,
        )

    rows = [row for row in rows if int(row["open_time"]) < bucket_open]
    partial = _target_partial(
        timeframe,
        evaluation_time_ms=evaluation_time_ms,
        one_minute_rows=one_minute_rows,
        one_hour_rows=one_hour_rows,
    )
    rows.append(partial)
    if len(rows) < 50:
        raise RuntimeError(f"not enough reconstructed candles for {symbol} {timeframe}: {len(rows)}")
    return rows[-KLINE_LIMIT:]

def _pine_price_fmt(price: float) -> str:
    decimals = 5 if price < 10 else 3
    text = f"{price:,.{decimals}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text

def _token(timeframe: str, metrics: dict[str, Any], is_ob: bool) -> str:
    is_all = bool(metrics["ob_all"] if is_ob else metrics["os_all"])
    word = "고점" if is_ob else "저점"
    prefix = "ALL " if is_all else ""
    return f"{TF_ICON[timeframe]}{timeframe} {prefix}{word}"

def _route_for_tf(timeframe: str, *, is_ob: bool) -> str:
    if timeframe in ("5m", "15m"):
        return "BD_SELL_SHORT" if is_ob else "BD_BUY_SHORT"
    if timeframe in ("30m", "1h"):
        return "BD_SELL_SWING" if is_ob else "BD_BUY_SWING"
    if timeframe in ("4h", "6h"):
        return "BD_SELL_LONG" if is_ob else "BD_BUY_LONG"
    if timeframe in ("12h", "1d", "1w"):
        return "BD_SELL_LIFE" if is_ob else "BD_BUY_LIFE"
    return ""

def _chain_signal(
    symbol: str,
    metrics_by_tf: dict[str, dict[str, Any]],
    *,
    is_ob: bool,
    price: float,
) -> dict[str, Any]:
    key = "ob_basic" if is_ob else "os_basic"
    max_tf: str | None = None

    for i, tf in enumerate(TF_ORDER):
        if tf not in MAX_CANDIDATES:
            continue
        if all(bool(metrics_by_tf[lower_tf][key]) for lower_tf in TF_ORDER[i:]):
            max_tf = tf
            break

    if max_tf is None:
        return {"chain_ok": False, "max_timeframe": None, "route": "", "message_preview": None}

    route = _route_for_tf(max_tf, is_ob=is_ob)
    first_line = f"🪙 [BINANCE] {symbol} : {_pine_price_fmt(price)}"
    msg = first_line + "\n\n" + _token(max_tf, metrics_by_tf[max_tf], is_ob)
    return {"chain_ok": True, "max_timeframe": max_tf, "route": route, "message_preview": msg}

def _latest_closed_minute_boundary_ms() -> int:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return (now_ms // 60_000) * 60_000

def _evaluate_symbol_at(symbol: str, evaluation_time_ms: int) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    if evaluation_time_ms <= 0:
        raise ValueError("evaluation_time_ms must be positive")

    # 70 finalized 1m bars rebuild the developing current hour exactly.
    one_minute_rows = _fetch_klines(
        symbol,
        "1m",
        limit=70,
        end_time_ms=int(evaluation_time_ms) - 1,
    )
    # 300 1h candles give ample history for building developing HTF partials.
    one_hour_rows = _fetch_klines(
        symbol,
        "1h",
        limit=KLINE_LIMIT,
        end_time_ms=int(evaluation_time_ms) - 1,
    )

    metrics_by_tf: dict[str, dict[str, Any]] = {}
    for tf in reversed(TF_ORDER):  # 5m -> ... -> 1w
        rows = _rows_at_evaluation(
            symbol,
            tf,
            evaluation_time_ms=evaluation_time_ms,
            one_minute_rows=one_minute_rows,
            one_hour_rows=one_hour_rows,
        )
        metrics_by_tf[tf] = _latest_metric(rows, evaluation_time_ms=evaluation_time_ms)

    price_1m = float(one_minute_rows[-1]["close"])
    return {
        "evaluation_time_ms": int(evaluation_time_ms),
        "evaluation_time_utc": datetime.fromtimestamp(evaluation_time_ms / 1000, tz=timezone.utc).isoformat(),
        "price_1m": price_1m,
        "timeframes": metrics_by_tf,
        "buy": _chain_signal(symbol, metrics_by_tf, is_ob=False, price=price_1m),
        "sell": _chain_signal(symbol, metrics_by_tf, is_ob=True, price=price_1m),
    }
