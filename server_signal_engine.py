"""Tajum On server signal engine - V130 COIN9 all-timeframe TV/Binance comparator.

Safety / scope:
- comparison-only: no Telegram, no FCM, no performance DB writes
- supported Binance spot symbols: BTC/ETH/SOL/SUI/LINK/XRP/DOGE/ADA/ONDO USDT
- TradingView Pine parity test for 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w
- 2h is an internal chain gate only; it is NOT a user-facing maximum alert timeframe
- comparisons align to Pine's exact 1-minute evaluation boundary
- V130 stores aggregate counters + latest full record per symbol + recent state-change/mismatch events.
  It does NOT retain every full 1-minute JSON record, keeping 9-symbol memory use bounded.

Pine reference: PINE_CODE_별꽃_v26_V98_상위추세태그
Signal math:
- RSI(14)
- slow stochastic K(5,3)
- ALL tag additionally uses slow stochastic K(20,12)
- oversold basic: RSI <= 30 and K(5,3) <= 20
- overbought basic: RSI >= 70 and K(5,3) >= 80
- maximum candidate requires an unbroken chain from that TF down through every lower TF to 5m
- max candidates: 1w, 1d, 12h, 6h, 4h, 1h, 30m, 15m, 5m
- internal-only chain TF: 2h
"""

from __future__ import annotations

import math
import os
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

BINANCE_SPOT_BASE_URL = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
REQUEST_TIMEOUT_SEC = max(3, min(int(os.getenv("SERVER_ENGINE_HTTP_TIMEOUT_SEC", "10") or 10), 30))
KLINE_LIMIT = max(100, min(int(os.getenv("SERVER_ENGINE_KLINE_LIMIT", "300") or 300), 1000))
COMPARE_KEY = os.getenv("SERVER_ENGINE_COMPARE_KEY", "").strip()
COMPARE_WORKERS = max(1, min(int(os.getenv("SERVER_ENGINE_COMPARE_WORKERS", "3") or 3), 6))
EVENT_HISTORY_LIMIT = max(100, min(int(os.getenv("SERVER_ENGINE_EVENT_HISTORY_LIMIT", "1000") or 1000), 5000))
BORDERLINE_EPSILON = max(0.01, min(float(os.getenv("SERVER_ENGINE_BORDERLINE_EPSILON", "0.25") or 0.25), 2.0))

PHASE_NAME = "COIN9_ALL_TF_TV_AUTO_COMPARE_V130"
EVENT_TYPE = "SERVER_ENGINE_TV_COMPARE_V130"

SUPPORTED_SYMBOLS: dict[str, str] = {
    "BTCUSDT": "Bitcoin",
    "ETHUSDT": "Ethereum",
    "SOLUSDT": "SOL",
    "SUIUSDT": "SUI",
    "LINKUSDT": "ChainLink",
    "XRPUSDT": "XRP",
    "DOGEUSDT": "Dogecoin",
    "ADAUSDT": "Cardano",
    "ONDOUSDT": "ONDO",
}

# Same descending order as the operating 별꽃 Pine.
TF_ORDER = ("1w", "1d", "12h", "6h", "4h", "2h", "1h", "30m", "15m", "5m")
MAX_CANDIDATES = frozenset(("1w", "1d", "12h", "6h", "4h", "1h", "30m", "15m", "5m"))
INTERNAL_ONLY_TFS = frozenset(("2h",))
TF_MINUTES = {
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
TF_ICON = {
    "1w": "💎",
    "1d": "✨",
    "12h": "⭐",
    "6h": "💚",
    "4h": "🧡",
    "2h": "❤️",
    "1h": "🟢",
    "30m": "🟠",
    "15m": "🔺",
    "5m": "△",
}

RSI_LENGTH = 14
RSI_OS = 30.0
RSI_OB = 70.0
K_OS = 20.0
K_OB = 80.0

_COMPARE_LOCK = threading.Lock()
_COMPARE_EXECUTOR = ThreadPoolExecutor(max_workers=COMPARE_WORKERS, thread_name_prefix="coin9-tv-compare")
_LATEST_BY_SYMBOL: dict[str, dict[str, Any]] = {}
_LATEST_OVERALL: dict[str, Any] | None = None
_RECENT_EVENTS: deque[dict[str, Any]] = deque(maxlen=EVENT_HISTORY_LIMIT)
_LAST_CHAIN_STATE: dict[str, tuple[Any, ...]] = {}
_PENDING_OR_SEEN: set[tuple[str, int]] = set()
_SEEN_ORDER: deque[tuple[str, int]] = deque()
_SEEN_LIMIT = 9 * 180  # ~3 hours of duplicate protection at 1/min for 9 symbols.


def _new_tf_stats() -> dict[str, Any]:
    return {
        "samples": 0,
        "condition_match_count": 0,
        "indicator_abs_sum": 0.0,
        "indicator_abs_count": 0,
        "borderline_hits": 0,
        "borderline_mismatch_hits": 0,
    }


def _new_symbol_stats() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "error_count": 0,
        "signal_match_count": 0,
        "condition_match_count": 0,
        "buy_chain_match_count": 0,
        "sell_chain_match_count": 0,
        "indicator_abs_sum": 0.0,
        "indicator_abs_count": 0,
        "price_bps_abs_sum": 0.0,
        "price_bps_count": 0,
        "buy_signal_sample_count": 0,
        "sell_signal_sample_count": 0,
        "signal_transition_count": 0,
        "mismatch_count": 0,
        "first_received_at_utc": None,
        "last_received_at_utc": None,
        "latest_error": None,
        "per_timeframe": {tf: _new_tf_stats() for tf in reversed(TF_ORDER)},
    }


_STATS_BY_SYMBOL: dict[str, dict[str, Any]] = {symbol: _new_symbol_stats() for symbol in SUPPORTED_SYMBOLS}


def compare_key_configured() -> bool:
    return bool(COMPARE_KEY)


def compare_key_matches(value: str | None) -> bool:
    if not COMPARE_KEY:
        return True
    return (value or "").strip() == COMPARE_KEY


def comparison_retention_limit() -> int:
    # Kept for app.py backward compatibility. V130 retains events, not every minute's full record.
    return EVENT_HISTORY_LIMIT


def comparison_storage_mode() -> str:
    return "aggregate_stats + latest_full_per_symbol + recent_signal/mismatch_events"


def supported_symbols() -> dict[str, str]:
    return dict(SUPPORTED_SYMBOLS)


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").upper().replace("BINANCE:", "").strip()
    if symbol not in SUPPORTED_SYMBOLS:
        raise ValueError(f"unsupported symbol {symbol!r}; allowed={','.join(SUPPORTED_SYMBOLS)}")
    return symbol


def _safe_float(value: Any) -> float:
    out = float(value)
    if not math.isfinite(out):
        raise ValueError(f"non-finite numeric value: {value!r}")
    return out


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return _safe_float(value)
    except (TypeError, ValueError):
        return None


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
        "comparison_price_1m": price_1m,
        "timeframes": metrics_by_tf,
        "buy": _chain_signal(symbol, metrics_by_tf, is_ob=False, price=price_1m),
        "sell": _chain_signal(symbol, metrics_by_tf, is_ob=True, price=price_1m),
    }


def evaluate_symbol(symbol: str) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    evaluation_time_ms = _latest_closed_minute_boundary_ms()
    core = _evaluate_symbol_at(symbol, evaluation_time_ms)
    return {
        "ok": True,
        "phase": PHASE_NAME,
        "delivery_enabled": False,
        "telegram_enabled": False,
        "fcm_enabled": False,
        "database_write_enabled": False,
        "symbol": symbol,
        "display_name": SUPPORTED_SYMBOLS[symbol],
        "market_source": "BINANCE_SPOT_REST_RECONSTRUCTED_AT_TV_1M_BOUNDARY",
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        **core,
        "thresholds": {
            "rsi_length": RSI_LENGTH,
            "rsi_oversold": RSI_OS,
            "rsi_overbought": RSI_OB,
            "stoch_oversold": K_OS,
            "stoch_overbought": K_OB,
            "stoch_fast": "5,3",
            "stoch_slow": "20,12",
        },
        "pine_contract": {
            "evaluation_basis": "exact Pine minute_close; developing HTF candle reconstructed from finalized Binance lower-TF bars",
            "signal_timeframes": list(TF_ORDER),
            "maximum_alert_candidates": [tf for tf in TF_ORDER if tf in MAX_CANDIDATES],
            "internal_only_chain_timeframes": list(INTERNAL_ONLY_TFS),
            "chain_rule": "candidate TF through every lower TF to 5m must all satisfy basic condition",
        },
    }


def evaluate_phase1_btc() -> dict[str, Any]:
    """Backward-compatible endpoint helper retained from V129."""
    return evaluate_symbol("BTCUSDT")


def _normalize_tv_tf(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("timeframe payload must be an object")
    return {
        "rsi14": _optional_float(raw.get("rsi14")),
        "stoch_5_3_k": _optional_float(raw.get("stoch_5_3_k")),
        "stoch_20_12_k": _optional_float(raw.get("stoch_20_12_k")),
        "os_basic": bool(raw.get("os_basic")),
        "ob_basic": bool(raw.get("ob_basic")),
        "os_all": bool(raw.get("os_all")),
        "ob_all": bool(raw.get("ob_all")),
    }


def _numeric_diff(tv: float | None, server: float | None) -> float | None:
    if tv is None or server is None:
        return None
    return float(server) - float(tv)


def _is_borderline(tv_tf: dict[str, Any]) -> bool:
    values_and_thresholds = (
        (tv_tf.get("rsi14"), RSI_OS),
        (tv_tf.get("rsi14"), RSI_OB),
        (tv_tf.get("stoch_5_3_k"), K_OS),
        (tv_tf.get("stoch_5_3_k"), K_OB),
    )
    for value, threshold in values_and_thresholds:
        if value is not None and abs(float(value) - threshold) <= BORDERLINE_EPSILON:
            return True
    return False


def _compare_candidate(symbol: str, tv: dict[str, Any], *, evaluation_time_ms: int) -> dict[str, Any]:
    server = _evaluate_symbol_at(symbol, evaluation_time_ms)
    tf_result: dict[str, Any] = {}
    error_score = 0.0
    numeric_count = 0
    condition_match = True

    for tf in reversed(TF_ORDER):  # 5m -> ... -> 1w
        tv_tf = tv["timeframes"][tf]
        sv_tf = server["timeframes"][tf]
        diffs = {
            "rsi14": _numeric_diff(tv_tf["rsi14"], sv_tf["rsi14"]),
            "stoch_5_3_k": _numeric_diff(tv_tf["stoch_5_3_k"], sv_tf["stoch_5_3_k"]),
            "stoch_20_12_k": _numeric_diff(tv_tf["stoch_20_12_k"], sv_tf["stoch_20_12_k"]),
        }
        for diff in diffs.values():
            if diff is not None:
                error_score += abs(diff)
                numeric_count += 1

        condition_fields = ("os_basic", "ob_basic", "os_all", "ob_all")
        condition_matches = {field: bool(tv_tf[field]) == bool(sv_tf[field]) for field in condition_fields}
        if not all(condition_matches.values()):
            condition_match = False

        tf_result[tf] = {
            "tradingview": tv_tf,
            "server": {
                "rsi14": sv_tf["rsi14"],
                "stoch_5_3_k": sv_tf["stoch_5_3_k"],
                "stoch_20_12_k": sv_tf["stoch_20_12_k"],
                "os_basic": sv_tf["os_basic"],
                "ob_basic": sv_tf["ob_basic"],
                "os_all": sv_tf["os_all"],
                "ob_all": sv_tf["ob_all"],
                "open": sv_tf["open"],
                "high": sv_tf["high"],
                "low": sv_tf["low"],
                "close": sv_tf["close"],
            },
            "server_minus_tv": diffs,
            "condition_matches": condition_matches,
            "borderline": _is_borderline(tv_tf),
        }

    buy_match = (
        bool(tv["buy"]["chain_ok"]) == bool(server["buy"]["chain_ok"])
        and tv["buy"].get("max_timeframe") == server["buy"].get("max_timeframe")
    )
    sell_match = (
        bool(tv["sell"]["chain_ok"]) == bool(server["sell"]["chain_ok"])
        and tv["sell"].get("max_timeframe") == server["sell"].get("max_timeframe")
    )

    return {
        "evaluation_time_ms": int(evaluation_time_ms),
        "evaluation_time_utc": server["evaluation_time_utc"],
        "mean_abs_indicator_diff": (error_score / numeric_count) if numeric_count else None,
        "condition_match": condition_match,
        "buy_chain_match": buy_match,
        "sell_chain_match": sell_match,
        "signal_match": bool(condition_match and buy_match and sell_match),
        "timeframes": tf_result,
        "server_buy": server["buy"],
        "server_sell": server["sell"],
        "server_price_1m": server["comparison_price_1m"],
    }


def _normalize_signal(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    max_tf = raw.get("max_timeframe")
    allowed = set(MAX_CANDIDATES) | {None, ""}
    if max_tf not in allowed or max_tf == "":
        max_tf = None
    return {"chain_ok": bool(raw.get("chain_ok")), "max_timeframe": max_tf}


def _remember_seen(key: tuple[str, int]) -> None:
    _PENDING_OR_SEEN.add(key)
    _SEEN_ORDER.append(key)
    while len(_SEEN_ORDER) > _SEEN_LIMIT:
        old = _SEEN_ORDER.popleft()
        _PENDING_OR_SEEN.discard(old)


def _record_event(event: dict[str, Any]) -> None:
    _RECENT_EVENTS.append(event)


def _update_stats_with_record(record: dict[str, Any], tv: dict[str, Any]) -> None:
    global _LATEST_OVERALL
    symbol = record["symbol"]
    stats = _STATS_BY_SYMBOL[symbol]
    received = record["received_at_utc"]

    stats["sample_count"] += 1
    stats["signal_match_count"] += int(bool(record.get("signal_match")))
    stats["condition_match_count"] += int(bool(record.get("condition_match")))
    stats["buy_chain_match_count"] += int(bool(record.get("buy_chain_match")))
    stats["sell_chain_match_count"] += int(bool(record.get("sell_chain_match")))
    stats["mismatch_count"] += int(not bool(record.get("signal_match")))
    stats["first_received_at_utc"] = stats["first_received_at_utc"] or received
    stats["last_received_at_utc"] = received
    stats["latest_error"] = None

    mean_diff = record.get("mean_abs_indicator_diff")
    if mean_diff is not None:
        # Keep the true underlying indicator sum/count via per-timeframe diffs below.
        pass
    price_bps = record.get("server_minus_tv_price_bps")
    if price_bps is not None:
        stats["price_bps_abs_sum"] += abs(float(price_bps))
        stats["price_bps_count"] += 1

    tv_buy = tv["buy"]
    tv_sell = tv["sell"]
    stats["buy_signal_sample_count"] += int(bool(tv_buy["chain_ok"]))
    stats["sell_signal_sample_count"] += int(bool(tv_sell["chain_ok"]))

    best = record["best"]
    for tf in reversed(TF_ORDER):
        tf_result = best["timeframes"][tf]
        tf_stats = stats["per_timeframe"][tf]
        tf_stats["samples"] += 1
        conds = tf_result["condition_matches"]
        tf_match = bool(conds) and all(bool(v) for v in conds.values())
        tf_stats["condition_match_count"] += int(tf_match)
        if tf_result.get("borderline"):
            tf_stats["borderline_hits"] += 1
            if not tf_match:
                tf_stats["borderline_mismatch_hits"] += 1
        for diff in tf_result["server_minus_tv"].values():
            if diff is not None:
                val = abs(float(diff))
                tf_stats["indicator_abs_sum"] += val
                tf_stats["indicator_abs_count"] += 1
                stats["indicator_abs_sum"] += val
                stats["indicator_abs_count"] += 1

    _LATEST_BY_SYMBOL[symbol] = record
    _LATEST_OVERALL = record

    state = (
        bool(tv_buy["chain_ok"]), tv_buy.get("max_timeframe"),
        bool(tv_sell["chain_ok"]), tv_sell.get("max_timeframe"),
    )
    previous = _LAST_CHAIN_STATE.get(symbol)
    if previous is not None and state != previous:
        stats["signal_transition_count"] += 1
        _record_event({
            "event": "SIGNAL_STATE_CHANGE",
            "received_at_utc": received,
            "pine_minute_close_utc": record["pine_minute_close_utc"],
            "symbol": symbol,
            "display_name": SUPPORTED_SYMBOLS[symbol],
            "tv_buy": tv_buy,
            "server_buy": best["server_buy"],
            "tv_sell": tv_sell,
            "server_sell": best["server_sell"],
            "signal_match": record["signal_match"],
        })
    _LAST_CHAIN_STATE[symbol] = state

    if not record.get("signal_match"):
        _record_event({
            "event": "MISMATCH",
            "received_at_utc": received,
            "pine_minute_close_utc": record["pine_minute_close_utc"],
            "symbol": symbol,
            "display_name": SUPPORTED_SYMBOLS[symbol],
            "tv_buy": tv_buy,
            "server_buy": best["server_buy"],
            "tv_sell": tv_sell,
            "server_sell": best["server_sell"],
            "mean_abs_indicator_diff": record.get("mean_abs_indicator_diff"),
        })


def compare_tradingview_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    if str(payload.get("event_type") or "") != EVENT_TYPE:
        raise ValueError("unexpected event_type")

    symbol = _normalize_symbol(payload.get("symbol"))
    minute_close = int(payload.get("minute_close") or 0)
    if minute_close <= 0:
        raise ValueError("minute_close is required")

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # Queued work on a small free instance can arrive later when 9 alerts fire together.
    if abs(now_ms - minute_close) > 12 * 60 * 60 * 1000:
        raise ValueError("minute_close must be within 12 hours of server time")

    raw_tfs = payload.get("timeframes")
    if not isinstance(raw_tfs, dict):
        raise ValueError("timeframes object required")
    missing = [tf for tf in TF_ORDER if tf not in raw_tfs]
    if missing:
        raise ValueError(f"missing timeframe payloads: {', '.join(missing)}")

    tv = {
        "tv_price": _optional_float(payload.get("tv_price")),
        "timeframes": {tf: _normalize_tv_tf(raw_tfs[tf]) for tf in TF_ORDER},
        "buy": _normalize_signal(payload.get("buy")),
        "sell": _normalize_signal(payload.get("sell")),
    }

    best = _compare_candidate(symbol, tv, evaluation_time_ms=minute_close)
    tv_price = tv["tv_price"]
    server_price = best["server_price_1m"]
    price_diff = _numeric_diff(tv_price, server_price)
    price_diff_bps = None
    if price_diff is not None and tv_price not in (None, 0.0):
        price_diff_bps = price_diff / float(tv_price) * 10_000.0

    record = {
        "ok": True,
        "phase": PHASE_NAME,
        "received_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "display_name": SUPPORTED_SYMBOLS[symbol],
        "pine_minute_close_ms": minute_close,
        "pine_minute_close_utc": datetime.fromtimestamp(minute_close / 1000, tz=timezone.utc).isoformat(),
        "pine_chart_time_ms": int(payload.get("chart_time") or 0) or None,
        "pine_chart_time_close_ms": int(payload.get("chart_time_close") or 0) or None,
        "pine_timenow_ms": int(payload.get("pine_timenow") or 0) or None,
        "tradingview_price": tv_price,
        "best_alignment_offset_ms": 0,
        "best_alignment_evaluation_time_ms": best["evaluation_time_ms"],
        "best_alignment_evaluation_time_utc": best["evaluation_time_utc"],
        "server_price_1m": server_price,
        "server_minus_tv_price": price_diff,
        "server_minus_tv_price_bps": price_diff_bps,
        "signal_match": best["signal_match"],
        "condition_match": best["condition_match"],
        "buy_chain_match": best["buy_chain_match"],
        "sell_chain_match": best["sell_chain_match"],
        "mean_abs_indicator_diff": best["mean_abs_indicator_diff"],
        "best": best,
        "delivery_enabled": False,
        "database_write_enabled": False,
    }

    with _COMPARE_LOCK:
        _update_stats_with_record(record, tv)
    return record


def _record_error(symbol: str, minute_close: int, exc: Exception) -> None:
    global _LATEST_OVERALL
    received = datetime.now(timezone.utc).isoformat()
    error_record = {
        "ok": False,
        "phase": PHASE_NAME,
        "received_at_utc": received,
        "symbol": symbol,
        "display_name": SUPPORTED_SYMBOLS.get(symbol, symbol),
        "pine_minute_close_ms": minute_close,
        "pine_minute_close_utc": datetime.fromtimestamp(minute_close / 1000, tz=timezone.utc).isoformat() if minute_close else None,
        "error": f"{type(exc).__name__}: {exc}",
        "delivery_enabled": False,
        "database_write_enabled": False,
    }
    with _COMPARE_LOCK:
        if symbol in _STATS_BY_SYMBOL:
            stats = _STATS_BY_SYMBOL[symbol]
            stats["error_count"] += 1
            stats["last_received_at_utc"] = received
            stats["first_received_at_utc"] = stats["first_received_at_utc"] or received
            stats["latest_error"] = error_record["error"]
            _LATEST_BY_SYMBOL[symbol] = error_record
        _LATEST_OVERALL = error_record
        _record_event({
            "event": "ERROR",
            "received_at_utc": received,
            "symbol": symbol,
            "display_name": SUPPORTED_SYMBOLS.get(symbol, symbol),
            "pine_minute_close_utc": error_record["pine_minute_close_utc"],
            "error": error_record["error"],
        })


def enqueue_tradingview_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    if str(payload.get("event_type") or "") != EVENT_TYPE:
        raise ValueError("unexpected event_type")
    symbol = _normalize_symbol(payload.get("symbol"))
    minute_close = int(payload.get("minute_close") or 0)
    if minute_close <= 0:
        raise ValueError("minute_close is required")

    key = (symbol, minute_close)
    with _COMPARE_LOCK:
        if key in _PENDING_OR_SEEN:
            return {
                "ok": True,
                "accepted": False,
                "duplicate": True,
                "phase": PHASE_NAME,
                "symbol": symbol,
                "pine_minute_close_ms": minute_close,
                "processing": "already_pending_or_processed",
            }
        _remember_seen(key)

    snapshot = dict(payload)

    def worker() -> None:
        try:
            compare_tradingview_snapshot(snapshot)
        except Exception as exc:
            _record_error(symbol, minute_close, exc)

    _COMPARE_EXECUTOR.submit(worker)
    return {
        "ok": True,
        "accepted": True,
        "phase": PHASE_NAME,
        "symbol": symbol,
        "display_name": SUPPORTED_SYMBOLS[symbol],
        "pine_minute_close_ms": minute_close,
        "processing": f"queued_background_pool_{COMPARE_WORKERS}_workers",
        "timeframes": list(TF_ORDER),
        "delivery_enabled": False,
        "database_write_enabled": False,
    }


def _pct(count: int, total: int) -> float | None:
    return (count / total * 100.0) if total else None


def _avg(total: float, count: int) -> float | None:
    return (total / count) if count else None


def _symbol_summary(symbol: str, stats: dict[str, Any]) -> dict[str, Any]:
    samples = int(stats["sample_count"])
    per_tf: dict[str, Any] = {}
    for tf in reversed(TF_ORDER):
        tfs = stats["per_timeframe"][tf]
        tf_samples = int(tfs["samples"])
        per_tf[tf] = {
            "samples": tf_samples,
            "condition_match_rate_pct": _pct(int(tfs["condition_match_count"]), tf_samples),
            "mean_abs_indicator_diff_avg": _avg(float(tfs["indicator_abs_sum"]), int(tfs["indicator_abs_count"])),
            "borderline_hits": int(tfs["borderline_hits"]),
            "borderline_mismatch_hits": int(tfs["borderline_mismatch_hits"]),
            "internal_chain_only": tf in INTERNAL_ONLY_TFS,
        }
    latest = _LATEST_BY_SYMBOL.get(symbol) or {}
    latest_best = latest.get("best") or {}
    return {
        "symbol": symbol,
        "display_name": SUPPORTED_SYMBOLS[symbol],
        "sample_count": samples,
        "error_count": int(stats["error_count"]),
        "signal_match_rate_pct": _pct(int(stats["signal_match_count"]), samples),
        "condition_match_rate_pct": _pct(int(stats["condition_match_count"]), samples),
        "buy_chain_match_rate_pct": _pct(int(stats["buy_chain_match_count"]), samples),
        "sell_chain_match_rate_pct": _pct(int(stats["sell_chain_match_count"]), samples),
        "mean_abs_indicator_diff_avg": _avg(float(stats["indicator_abs_sum"]), int(stats["indicator_abs_count"])),
        "abs_price_diff_bps_avg": _avg(float(stats["price_bps_abs_sum"]), int(stats["price_bps_count"])),
        "buy_signal_sample_count": int(stats["buy_signal_sample_count"]),
        "sell_signal_sample_count": int(stats["sell_signal_sample_count"]),
        "signal_transition_count": int(stats["signal_transition_count"]),
        "mismatch_count": int(stats["mismatch_count"]),
        "first_received_at_utc": stats["first_received_at_utc"],
        "last_received_at_utc": stats["last_received_at_utc"],
        "latest_error": stats["latest_error"],
        "latest_signal_match": latest.get("signal_match"),
        "latest_buy": latest_best.get("server_buy"),
        "latest_sell": latest_best.get("server_sell"),
        "latest_price": latest.get("server_price_1m"),
        "per_timeframe": per_tf,
    }


def comparison_latest(symbol: str | None = None) -> dict[str, Any]:
    with _COMPARE_LOCK:
        if symbol:
            normalized = _normalize_symbol(symbol)
            row = _LATEST_BY_SYMBOL.get(normalized)
            if row is None:
                return {
                    "ok": True,
                    "phase": PHASE_NAME,
                    "symbol": normalized,
                    "sample_count": 0,
                    "message": "No V130 comparison received for this symbol since this Render process started.",
                }
            return dict(row)
        if _LATEST_OVERALL is None:
            return {
                "ok": True,
                "phase": PHASE_NAME,
                "sample_count": 0,
                "message": "No V130 TradingView comparison webhook received since this Render process started.",
            }
        return dict(_LATEST_OVERALL)


def comparison_events(symbol: str | None = None, limit: int = 100) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol) if symbol else None
    limit = max(1, min(int(limit), 500))
    with _COMPARE_LOCK:
        events = list(_RECENT_EVENTS)
    if normalized:
        events = [event for event in events if event.get("symbol") == normalized]
    events = events[-limit:]
    return {
        "ok": True,
        "phase": PHASE_NAME,
        "symbol": normalized,
        "count": len(events),
        "events": list(reversed(events)),
        "event_history_limit": EVENT_HISTORY_LIMIT,
    }


def comparison_summary() -> dict[str, Any]:
    with _COMPARE_LOCK:
        symbol_stats = {symbol: _symbol_summary(symbol, stats) for symbol, stats in _STATS_BY_SYMBOL.items()}
        events = list(_RECENT_EVENTS)

    active = [row for row in symbol_stats.values() if row["sample_count"] > 0 or row["error_count"] > 0]
    total_samples = sum(int(row["sample_count"]) for row in symbol_stats.values())
    total_errors = sum(int(row["error_count"]) for row in symbol_stats.values())
    total_signal_match = sum(int(_STATS_BY_SYMBOL[s]["signal_match_count"]) for s in SUPPORTED_SYMBOLS)
    total_condition_match = sum(int(_STATS_BY_SYMBOL[s]["condition_match_count"]) for s in SUPPORTED_SYMBOLS)
    total_buy_match = sum(int(_STATS_BY_SYMBOL[s]["buy_chain_match_count"]) for s in SUPPORTED_SYMBOLS)
    total_sell_match = sum(int(_STATS_BY_SYMBOL[s]["sell_chain_match_count"]) for s in SUPPORTED_SYMBOLS)
    indicator_sum = sum(float(_STATS_BY_SYMBOL[s]["indicator_abs_sum"]) for s in SUPPORTED_SYMBOLS)
    indicator_count = sum(int(_STATS_BY_SYMBOL[s]["indicator_abs_count"]) for s in SUPPORTED_SYMBOLS)
    price_sum = sum(float(_STATS_BY_SYMBOL[s]["price_bps_abs_sum"]) for s in SUPPORTED_SYMBOLS)
    price_count = sum(int(_STATS_BY_SYMBOL[s]["price_bps_count"]) for s in SUPPORTED_SYMBOLS)

    aggregate_tf: dict[str, Any] = {}
    for tf in reversed(TF_ORDER):
        samples = sum(int(_STATS_BY_SYMBOL[s]["per_timeframe"][tf]["samples"]) for s in SUPPORTED_SYMBOLS)
        matches = sum(int(_STATS_BY_SYMBOL[s]["per_timeframe"][tf]["condition_match_count"]) for s in SUPPORTED_SYMBOLS)
        diff_sum = sum(float(_STATS_BY_SYMBOL[s]["per_timeframe"][tf]["indicator_abs_sum"]) for s in SUPPORTED_SYMBOLS)
        diff_count = sum(int(_STATS_BY_SYMBOL[s]["per_timeframe"][tf]["indicator_abs_count"]) for s in SUPPORTED_SYMBOLS)
        borderline = sum(int(_STATS_BY_SYMBOL[s]["per_timeframe"][tf]["borderline_hits"]) for s in SUPPORTED_SYMBOLS)
        borderline_mismatch = sum(int(_STATS_BY_SYMBOL[s]["per_timeframe"][tf]["borderline_mismatch_hits"]) for s in SUPPORTED_SYMBOLS)
        aggregate_tf[tf] = {
            "samples": samples,
            "condition_match_rate_pct": _pct(matches, samples),
            "mean_abs_indicator_diff_avg": _avg(diff_sum, diff_count),
            "borderline_hits": borderline,
            "borderline_mismatch_hits": borderline_mismatch,
            "internal_chain_only": tf in INTERNAL_ONLY_TFS,
        }

    first_times = [row["first_received_at_utc"] for row in active if row.get("first_received_at_utc")]
    last_times = [row["last_received_at_utc"] for row in active if row.get("last_received_at_utc")]
    return {
        "ok": True,
        "phase": PHASE_NAME,
        "configured_symbol_count": len(SUPPORTED_SYMBOLS),
        "active_symbol_count": len(active),
        "configured_symbols": SUPPORTED_SYMBOLS,
        "sample_count": total_samples,
        "error_count": total_errors,
        "signal_match_count": total_signal_match,
        "signal_match_rate_pct": _pct(total_signal_match, total_samples),
        "condition_match_rate_pct": _pct(total_condition_match, total_samples),
        "buy_chain_match_rate_pct": _pct(total_buy_match, total_samples),
        "sell_chain_match_rate_pct": _pct(total_sell_match, total_samples),
        "alignment_offset_counts": {"0": total_samples},
        "mean_abs_indicator_diff_avg": _avg(indicator_sum, indicator_count),
        "abs_price_diff_bps_avg": _avg(price_sum, price_count),
        "per_symbol": symbol_stats,
        "per_timeframe": aggregate_tf,
        "first_received_at_utc": min(first_times) if first_times else None,
        "last_received_at_utc": max(last_times) if last_times else None,
        "recent_event_count": len(events),
        "signal_transition_count": sum(int(_STATS_BY_SYMBOL[s]["signal_transition_count"]) for s in SUPPORTED_SYMBOLS),
        "mismatch_count": sum(int(_STATS_BY_SYMBOL[s]["mismatch_count"]) for s in SUPPORTED_SYMBOLS),
        "database_write_enabled": False,
        "delivery_enabled": False,
        "compare_workers": COMPARE_WORKERS,
        "borderline_epsilon": BORDERLINE_EPSILON,
        "storage_mode": comparison_storage_mode(),
        "retention_note": "aggregate counters persist until Render restart/redeploy; only latest full record per symbol and recent events are retained in memory",
    }
