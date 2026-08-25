"""Tajum On server signal engine - V129 BTC all-timeframe TV/Binance comparator.

Safety / scope:
- BTCUSDT only
- comparison-only: no Telegram, no FCM, no performance DB writes
- TradingView Pine parity test for 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w
- 2h is an internal chain gate only; it is NOT a user-facing maximum alert timeframe
- comparisons are aligned to Pine's exact 1-minute evaluation boundary

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
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

BINANCE_SPOT_BASE_URL = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
REQUEST_TIMEOUT_SEC = max(3, min(int(os.getenv("SERVER_ENGINE_HTTP_TIMEOUT_SEC", "10") or 10), 30))
KLINE_LIMIT = max(100, min(int(os.getenv("SERVER_ENGINE_KLINE_LIMIT", "300") or 300), 1000))
# 1 alert/minute -> 1440/day. 2000 keeps roughly 33 hours in memory.
COMPARE_HISTORY_LIMIT = max(100, min(int(os.getenv("SERVER_ENGINE_COMPARE_HISTORY_LIMIT", "2000") or 2000), 5000))
COMPARE_KEY = os.getenv("SERVER_ENGINE_COMPARE_KEY", "").strip()

PHASE_SYMBOL = "BTCUSDT"
PHASE_NAME = "BTC_ALL_TF_TV_AUTO_COMPARE_V129"

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
_COMPARE_RESULTS: deque[dict[str, Any]] = deque(maxlen=COMPARE_HISTORY_LIMIT)


def compare_key_configured() -> bool:
    return bool(COMPARE_KEY)


def compare_key_matches(value: str | None) -> bool:
    if not COMPARE_KEY:
        return True
    return (value or "").strip() == COMPARE_KEY


def comparison_retention_limit() -> int:
    return COMPARE_HISTORY_LIMIT


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

    # Build 1h exactly at the Pine boundary from finalized 1m bars, then use
    # finalized prior 1h candles to aggregate 2h/4h/6h/12h/1d/1w cheaply.
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
    rows = _fetch_klines(
        symbol,
        timeframe,
        limit=KLINE_LIMIT,
        end_time_ms=int(evaluation_time_ms) - 1,
    )

    # Remove Binance's now-known version of the target bucket and replace it
    # with the exact developing candle state that Pine saw at minute_close.
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


def _chain_signal(metrics_by_tf: dict[str, dict[str, Any]], *, is_ob: bool, price: float) -> dict[str, Any]:
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
    first_line = f"🪙 [BINANCE] {PHASE_SYMBOL} : {_pine_price_fmt(price)}"
    msg = first_line + "\n\n" + _token(max_tf, metrics_by_tf[max_tf], is_ob)
    return {"chain_ok": True, "max_timeframe": max_tf, "route": route, "message_preview": msg}


def _latest_closed_minute_boundary_ms() -> int:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return (now_ms // 60_000) * 60_000


def _evaluate_btc_at(evaluation_time_ms: int) -> dict[str, Any]:
    if evaluation_time_ms <= 0:
        raise ValueError("evaluation_time_ms must be positive")

    # 70 finalized 1m bars are enough to rebuild the developing current hour.
    one_minute_rows = _fetch_klines(
        PHASE_SYMBOL,
        "1m",
        limit=70,
        end_time_ms=int(evaluation_time_ms) - 1,
    )
    # Up to one week needs only 168 hourly bars.  200 gives margin.
    one_hour_rows = _fetch_klines(
        PHASE_SYMBOL,
        "1h",
        limit=200,
        end_time_ms=int(evaluation_time_ms) - 1,
    )

    metrics_by_tf: dict[str, dict[str, Any]] = {}
    # calculate every Pine chain TF; order here is only for readable output
    for tf in reversed(TF_ORDER):  # 5m -> ... -> 1w
        rows = _rows_at_evaluation(
            PHASE_SYMBOL,
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
        "buy": _chain_signal(metrics_by_tf, is_ob=False, price=price_1m),
        "sell": _chain_signal(metrics_by_tf, is_ob=True, price=price_1m),
    }


def evaluate_phase1_btc() -> dict[str, Any]:
    evaluation_time_ms = _latest_closed_minute_boundary_ms()
    core = _evaluate_btc_at(evaluation_time_ms)
    return {
        "ok": True,
        "phase": PHASE_NAME,
        "delivery_enabled": False,
        "telegram_enabled": False,
        "fcm_enabled": False,
        "database_write_enabled": False,
        "symbol": PHASE_SYMBOL,
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


def _compare_candidate(tv: dict[str, Any], *, evaluation_time_ms: int) -> dict[str, Any]:
    server = _evaluate_btc_at(evaluation_time_ms)
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


def compare_tradingview_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Compare one V129 TradingView snapshot at the exact Pine minute_close.

    V128 proved 0ms alignment on every initial sample, so V129 removes the
    diagnostic -60s duplicate calculation. This keeps all 10 TF tests practical.
    """
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    if str(payload.get("event_type") or "") != "SERVER_ENGINE_TV_COMPARE_V129":
        raise ValueError("unexpected event_type")

    symbol = str(payload.get("symbol") or "").upper().replace("BINANCE:", "")
    if symbol != PHASE_SYMBOL:
        raise ValueError(f"V129 accepts only {PHASE_SYMBOL}, got {symbol!r}")

    minute_close = int(payload.get("minute_close") or 0)
    if minute_close <= 0:
        raise ValueError("minute_close is required")

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if abs(now_ms - minute_close) > 6 * 60 * 60 * 1000:
        raise ValueError("minute_close must be within 6 hours of server time")

    raw_tfs = payload.get("timeframes")
    if not isinstance(raw_tfs, dict):
        raise ValueError("timeframes object required")
    missing = [tf for tf in TF_ORDER if tf not in raw_tfs]
    if missing:
        raise ValueError(f"missing timeframe payloads: {', '.join(missing)}")

    def norm_signal(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        max_tf = raw.get("max_timeframe")
        allowed = set(MAX_CANDIDATES) | {None, ""}
        if max_tf not in allowed:
            max_tf = None
        if max_tf == "":
            max_tf = None
        return {"chain_ok": bool(raw.get("chain_ok")), "max_timeframe": max_tf}

    tv = {
        "tv_price": _optional_float(payload.get("tv_price")),
        "timeframes": {tf: _normalize_tv_tf(raw_tfs[tf]) for tf in TF_ORDER},
        "buy": norm_signal(payload.get("buy")),
        "sell": norm_signal(payload.get("sell")),
    }

    best = _compare_candidate(tv, evaluation_time_ms=minute_close)
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
        "symbol": PHASE_SYMBOL,
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
        _COMPARE_RESULTS.append(record)
    return record


def enqueue_tradingview_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    if str(payload.get("event_type") or "") != "SERVER_ENGINE_TV_COMPARE_V129":
        raise ValueError("unexpected event_type")
    symbol = str(payload.get("symbol") or "").upper().replace("BINANCE:", "")
    if symbol != PHASE_SYMBOL:
        raise ValueError(f"V129 accepts only {PHASE_SYMBOL}, got {symbol!r}")
    minute_close = int(payload.get("minute_close") or 0)
    if minute_close <= 0:
        raise ValueError("minute_close is required")

    snapshot = dict(payload)

    def worker() -> None:
        try:
            compare_tradingview_snapshot(snapshot)
        except Exception as exc:
            error_record = {
                "ok": False,
                "phase": PHASE_NAME,
                "received_at_utc": datetime.now(timezone.utc).isoformat(),
                "symbol": PHASE_SYMBOL,
                "pine_minute_close_ms": minute_close,
                "error": f"{type(exc).__name__}: {exc}",
                "delivery_enabled": False,
                "database_write_enabled": False,
            }
            with _COMPARE_LOCK:
                _COMPARE_RESULTS.append(error_record)

    threading.Thread(
        target=worker,
        name="tv-server-engine-compare-v129",
        daemon=True,
    ).start()
    return {
        "ok": True,
        "accepted": True,
        "phase": PHASE_NAME,
        "symbol": PHASE_SYMBOL,
        "pine_minute_close_ms": minute_close,
        "processing": "background",
        "timeframes": list(TF_ORDER),
        "delivery_enabled": False,
        "database_write_enabled": False,
    }


def comparison_latest() -> dict[str, Any]:
    with _COMPARE_LOCK:
        if not _COMPARE_RESULTS:
            return {
                "ok": True,
                "phase": PHASE_NAME,
                "sample_count": 0,
                "message": "No V129 TradingView comparison webhook received since this Render process started.",
            }
        return dict(_COMPARE_RESULTS[-1])


def comparison_summary() -> dict[str, Any]:
    with _COMPARE_LOCK:
        rows = list(_COMPARE_RESULTS)

    total_records = len(rows)
    valid_rows = [row for row in rows if "signal_match" in row]
    error_rows = [row for row in rows if not row.get("ok", False)]
    count = len(valid_rows)

    if not rows:
        return {
            "ok": True,
            "phase": PHASE_NAME,
            "sample_count": 0,
            "error_count": 0,
            "signal_match_rate_pct": None,
            "per_timeframe": {},
            "database_write_enabled": False,
            "retention_limit": COMPARE_HISTORY_LIMIT,
        }

    signal_matches = sum(1 for row in valid_rows if row.get("signal_match"))
    condition_matches = sum(1 for row in valid_rows if row.get("condition_match"))
    buy_matches = sum(1 for row in valid_rows if row.get("buy_chain_match"))
    sell_matches = sum(1 for row in valid_rows if row.get("sell_chain_match"))
    diffs: list[float] = []
    price_bps: list[float] = []

    per_tf: dict[str, dict[str, Any]] = {}
    for tf in reversed(TF_ORDER):
        tf_samples = 0
        tf_condition_match = 0
        tf_numeric_diffs: list[float] = []
        for row in valid_rows:
            tf_row = (((row.get("best") or {}).get("timeframes") or {}).get(tf))
            if not isinstance(tf_row, dict):
                continue
            tf_samples += 1
            conds = tf_row.get("condition_matches") or {}
            if conds and all(bool(v) for v in conds.values()):
                tf_condition_match += 1
            for value in (tf_row.get("server_minus_tv") or {}).values():
                if value is not None:
                    tf_numeric_diffs.append(abs(float(value)))
        per_tf[tf] = {
            "samples": tf_samples,
            "condition_match_rate_pct": (tf_condition_match / tf_samples * 100.0) if tf_samples else None,
            "mean_abs_indicator_diff_avg": (sum(tf_numeric_diffs) / len(tf_numeric_diffs)) if tf_numeric_diffs else None,
            "internal_chain_only": tf in INTERNAL_ONLY_TFS,
        }

    for row in valid_rows:
        if row.get("mean_abs_indicator_diff") is not None:
            diffs.append(float(row["mean_abs_indicator_diff"]))
        if row.get("server_minus_tv_price_bps") is not None:
            price_bps.append(abs(float(row["server_minus_tv_price_bps"])))

    first_received = rows[0].get("received_at_utc") if rows else None
    last_received = rows[-1].get("received_at_utc") if rows else None
    return {
        "ok": True,
        "phase": PHASE_NAME,
        "total_record_count": total_records,
        "sample_count": count,
        "error_count": len(error_rows),
        "signal_match_count": signal_matches,
        "signal_match_rate_pct": (signal_matches / count * 100.0) if count else None,
        "condition_match_rate_pct": (condition_matches / count * 100.0) if count else None,
        "buy_chain_match_rate_pct": (buy_matches / count * 100.0) if count else None,
        "sell_chain_match_rate_pct": (sell_matches / count * 100.0) if count else None,
        "alignment_offset_counts": {"0": count},
        "mean_abs_indicator_diff_avg": (sum(diffs) / len(diffs)) if diffs else None,
        "abs_price_diff_bps_avg": (sum(price_bps) / len(price_bps)) if price_bps else None,
        "per_timeframe": per_tf,
        "first_received_at_utc": first_received,
        "last_received_at_utc": last_received,
        "latest_signal_match": valid_rows[-1].get("signal_match") if valid_rows else None,
        "latest_error": error_rows[-1].get("error") if error_rows else None,
        "database_write_enabled": False,
        "retention_limit": COMPARE_HISTORY_LIMIT,
        "retention_note": f"in-memory only, max {COMPARE_HISTORY_LIMIT}; about 33h at 1/min; resets on Render restart/redeploy",
    }
