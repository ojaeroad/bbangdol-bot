"""Tajum On server signal engine - V128 TradingView/Binance parity comparator.

This module is intentionally side-effect free with respect to the member service:
- no Telegram sends
- no FCM sends
- no performance DB writes
- no cadence mutations

V128 scope:
1) BTCUSDT only
2) 5m / 15m only
3) reconstruct the exact higher-timeframe *developing* candle at a TradingView
   1-minute evaluation boundary using finalized Binance 1m candles
4) compare TradingView RSI/Stoch/chain results with Python results automatically

Pine reference: PINE_CODE_별꽃_v26_V98_상위추세태그
- RSI(14)
- slow stochastic K(5,3)
- ALL label additionally uses slow stochastic K(20,12)
- oversold basic: RSI <= 30 and K(5,3) <= 20
- overbought basic: RSI >= 70 and K(5,3) >= 80
- 15m candidate requires an unbroken 15m -> 5m chain
- if 15m fails, 5m may still be the maximum candidate
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
COMPARE_HISTORY_LIMIT = max(20, min(int(os.getenv("SERVER_ENGINE_COMPARE_HISTORY_LIMIT", "500") or 500), 2000))
COMPARE_KEY = os.getenv("SERVER_ENGINE_COMPARE_KEY", "").strip()

PHASE1_SYMBOL = "BTCUSDT"
PHASE1_SIGNAL_TFS = ("15m", "5m")
TF_MINUTES = {"5m": 5, "15m": 15}

RSI_LENGTH = 14
RSI_OS = 30.0
RSI_OB = 70.0
K_OS = 20.0
K_OB = 80.0

TF_ICON = {"15m": "🔺", "5m": "△"}

_COMPARE_LOCK = threading.Lock()
_COMPARE_RESULTS: deque[dict[str, Any]] = deque(maxlen=COMPARE_HISTORY_LIMIT)


def compare_key_configured() -> bool:
    return bool(COMPARE_KEY)


def compare_key_matches(value: str | None) -> bool:
    if not COMPARE_KEY:
        return True
    return (value or "").strip() == COMPARE_KEY


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
    """Fetch Binance Spot klines.

    For historical comparison ``end_time_ms`` is used.  A later REST request can
    therefore reconstruct an older TradingView evaluation without relying on the
    market price at the moment Render happens to wake up.
    """
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
    if len(rows) < 50 and interval != "1m":
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


def _latest_metric(rows: list[dict[str, float | int]], *, evaluation_time_ms: int | None = None) -> dict[str, Any]:
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
    ref_ms = int(evaluation_time_ms or datetime.now(timezone.utc).timestamp() * 1000)
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
        "bar_closed_at_evaluation": int(last["close_time"]) < ref_ms,
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "close": float(last["close"]),
        "candle_count": len(rows),
    }


def _bucket_open_ms(evaluation_time_ms: int, timeframe_minutes: int) -> int:
    # evaluation_time is an exact minute boundary.  Use the candle containing
    # the millisecond immediately before it, i.e. the candle state just evaluated.
    tf_ms = timeframe_minutes * 60_000
    return ((int(evaluation_time_ms) - 1) // tf_ms) * tf_ms


def _partial_from_1m(
    one_minute_rows: list[dict[str, float | int]],
    *,
    timeframe_minutes: int,
    evaluation_time_ms: int,
) -> dict[str, float | int]:
    bucket_open = _bucket_open_ms(evaluation_time_ms, timeframe_minutes)
    selected = [
        row for row in one_minute_rows
        if int(row["open_time"]) >= bucket_open and int(row["close_time"]) < int(evaluation_time_ms)
    ]
    if not selected:
        raise RuntimeError(
            f"no finalized 1m candles available for {timeframe_minutes}m bucket "
            f"at evaluation_time_ms={evaluation_time_ms}"
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


def _rows_at_evaluation(
    symbol: str,
    timeframe: str,
    *,
    evaluation_time_ms: int,
    one_minute_rows: list[dict[str, float | int]],
) -> list[dict[str, float | int]]:
    """Build HTF history with the last candle clipped to the TV 1m boundary."""
    tf_min = TF_MINUTES[timeframe]
    bucket_open = _bucket_open_ms(evaluation_time_ms, tf_min)

    # Fetch enough finalized HTF history for TradingView-style RMA convergence.
    rows = _fetch_klines(
        symbol,
        timeframe,
        limit=KLINE_LIMIT,
        end_time_ms=int(evaluation_time_ms) - 1,
    )

    # Remove a fully finalized version of the target candle (if Binance returns
    # it now) and any accidental later rows, then append our reconstructed
    # developing candle made only from 1m bars finalized by evaluation_time.
    rows = [row for row in rows if int(row["open_time"]) < bucket_open]
    partial = _partial_from_1m(
        one_minute_rows,
        timeframe_minutes=tf_min,
        evaluation_time_ms=evaluation_time_ms,
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


def _chain_signal(metrics_by_tf: dict[str, dict[str, Any]], *, is_ob: bool, price: float) -> dict[str, Any]:
    key = "ob_basic" if is_ob else "os_basic"
    max_tf: str | None = None

    if bool(metrics_by_tf["15m"][key]) and bool(metrics_by_tf["5m"][key]):
        max_tf = "15m"
    elif bool(metrics_by_tf["5m"][key]):
        max_tf = "5m"

    if max_tf is None:
        return {"chain_ok": False, "max_timeframe": None, "route": "", "message_preview": None}

    route = "BD_SELL_SHORT" if is_ob else "BD_BUY_SHORT"
    first_line = f"🪙 [BINANCE] {PHASE1_SYMBOL} : {_pine_price_fmt(price)}"
    msg = first_line + "\n\n" + _token(max_tf, metrics_by_tf[max_tf], is_ob)
    return {"chain_ok": True, "max_timeframe": max_tf, "route": route, "message_preview": msg}


def _latest_closed_minute_boundary_ms() -> int:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return (now_ms // 60_000) * 60_000


def _evaluate_btc_at(evaluation_time_ms: int) -> dict[str, Any]:
    if evaluation_time_ms <= 0:
        raise ValueError("evaluation_time_ms must be positive")

    # We only need the current 15m bucket of finalized 1m bars.  25 bars gives
    # margin around boundaries and is cheap enough for compare-only use.
    one_minute_rows = _fetch_klines(
        PHASE1_SYMBOL,
        "1m",
        limit=25,
        end_time_ms=int(evaluation_time_ms) - 1,
    )
    if not one_minute_rows:
        raise RuntimeError("no 1m data for evaluation")

    metrics_by_tf: dict[str, dict[str, Any]] = {}
    for tf in PHASE1_SIGNAL_TFS:
        rows = _rows_at_evaluation(
            PHASE1_SYMBOL,
            tf,
            evaluation_time_ms=evaluation_time_ms,
            one_minute_rows=one_minute_rows,
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
    """Side-effect-free browser snapshot aligned to the latest closed 1m boundary."""
    evaluation_time_ms = _latest_closed_minute_boundary_ms()
    core = _evaluate_btc_at(evaluation_time_ms)
    return {
        "ok": True,
        "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
        "delivery_enabled": False,
        "telegram_enabled": False,
        "fcm_enabled": False,
        "database_write_enabled": False,
        "symbol": PHASE1_SYMBOL,
        "market_source": "BINANCE_SPOT_REST_RECONSTRUCTED_FROM_1M",
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
            "evaluation_basis": "latest finalized 1m boundary; reconstruct developing 5m/15m candle from finalized Binance 1m bars",
            "delay_resistance": "comparison does not depend on the live REST price at webhook arrival time",
            "phase1_chain": "15m -> 5m; if 15m fails, 5m can still be maximum candidate",
            "full_engine_internal_tf_note": "2h must later be calculated as an internal chain gate for 4h+ Pine parity, but it is not a user alert TF.",
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

    for tf in ("5m", "15m"):
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
    """Compare one TradingView validation webhook with reconstructed Binance data.

    V128 deliberately evaluates two timestamp candidates:
    - offset 0s: Pine's transmitted minute_close
    - offset -60s: diagnostic for Pine realtime/alert close semantics

    We record both and report the numerically closer alignment.  After enough
    samples establish a stable timestamp convention, the next version can lock
    that convention instead of keeping the diagnostic branch.
    """
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    if str(payload.get("event_type") or "") != "SERVER_ENGINE_TV_COMPARE_V128":
        raise ValueError("unexpected event_type")

    symbol = str(payload.get("symbol") or "").upper().replace("BINANCE:", "")
    if symbol != PHASE1_SYMBOL:
        raise ValueError(f"phase1 accepts only {PHASE1_SYMBOL}, got {symbol!r}")

    minute_close = int(payload.get("minute_close") or 0)
    if minute_close <= 0:
        raise ValueError("minute_close is required")

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # Protect the public compare-only endpoint from arbitrary old/future heavy queries.
    if abs(now_ms - minute_close) > 6 * 60 * 60 * 1000:
        raise ValueError("minute_close must be within 6 hours of server time")

    raw_tfs = payload.get("timeframes")
    if not isinstance(raw_tfs, dict) or "5m" not in raw_tfs or "15m" not in raw_tfs:
        raise ValueError("timeframes.5m and timeframes.15m are required")

    def norm_signal(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        max_tf = raw.get("max_timeframe")
        if max_tf not in (None, "", "5m", "15m"):
            max_tf = None
        if max_tf == "":
            max_tf = None
        return {"chain_ok": bool(raw.get("chain_ok")), "max_timeframe": max_tf}

    tv = {
        "tv_price": _optional_float(payload.get("tv_price")),
        "timeframes": {
            "5m": _normalize_tv_tf(raw_tfs["5m"]),
            "15m": _normalize_tv_tf(raw_tfs["15m"]),
        },
        "buy": norm_signal(payload.get("buy")),
        "sell": norm_signal(payload.get("sell")),
    }

    candidates: list[dict[str, Any]] = []
    for offset_ms in (0, -60_000):
        candidate = _compare_candidate(tv, evaluation_time_ms=minute_close + offset_ms)
        candidate["offset_ms_from_pine_minute_close"] = offset_ms
        candidates.append(candidate)

    def candidate_score(item: dict[str, Any]) -> tuple[int, float]:
        # Prefer exact final signal parity first, then the smaller indicator difference.
        mismatch = 0 if item["signal_match"] else 1
        mean_diff = item["mean_abs_indicator_diff"]
        return mismatch, float(mean_diff if mean_diff is not None else 1e9)

    best = min(candidates, key=candidate_score)
    tv_price = tv["tv_price"]
    server_price = best["server_price_1m"]
    price_diff = _numeric_diff(tv_price, server_price)
    price_diff_bps = None
    if price_diff is not None and tv_price not in (None, 0.0):
        price_diff_bps = price_diff / float(tv_price) * 10_000.0

    record = {
        "ok": True,
        "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
        "received_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": PHASE1_SYMBOL,
        "pine_minute_close_ms": minute_close,
        "pine_minute_close_utc": datetime.fromtimestamp(minute_close / 1000, tz=timezone.utc).isoformat(),
        "pine_chart_time_ms": int(payload.get("chart_time") or 0) or None,
        "pine_chart_time_close_ms": int(payload.get("chart_time_close") or 0) or None,
        "pine_timenow_ms": int(payload.get("pine_timenow") or 0) or None,
        "tradingview_price": tv_price,
        "best_alignment_offset_ms": best["offset_ms_from_pine_minute_close"],
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
        "alignment_candidates": [
            {
                "offset_ms_from_pine_minute_close": item["offset_ms_from_pine_minute_close"],
                "evaluation_time_utc": item["evaluation_time_utc"],
                "signal_match": item["signal_match"],
                "condition_match": item["condition_match"],
                "buy_chain_match": item["buy_chain_match"],
                "sell_chain_match": item["sell_chain_match"],
                "mean_abs_indicator_diff": item["mean_abs_indicator_diff"],
            }
            for item in candidates
        ],
        "delivery_enabled": False,
        "database_write_enabled": False,
    }

    with _COMPARE_LOCK:
        _COMPARE_RESULTS.append(record)
    return record


def enqueue_tradingview_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept a TradingView webhook quickly and compare in a daemon thread.

    TradingView cancels slow webhooks, so the Flask route should not wait for
    Binance REST calls.  The actual comparison result appears shortly afterward
    in /server-engine/compare/latest and /summary.
    """
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    if str(payload.get("event_type") or "") != "SERVER_ENGINE_TV_COMPARE_V128":
        raise ValueError("unexpected event_type")
    symbol = str(payload.get("symbol") or "").upper().replace("BINANCE:", "")
    if symbol != PHASE1_SYMBOL:
        raise ValueError(f"phase1 accepts only {PHASE1_SYMBOL}, got {symbol!r}")
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
                "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
                "received_at_utc": datetime.now(timezone.utc).isoformat(),
                "symbol": PHASE1_SYMBOL,
                "pine_minute_close_ms": minute_close,
                "error": f"{type(exc).__name__}: {exc}",
                "delivery_enabled": False,
                "database_write_enabled": False,
            }
            with _COMPARE_LOCK:
                _COMPARE_RESULTS.append(error_record)

    threading.Thread(
        target=worker,
        name="tv-server-engine-compare",
        daemon=True,
    ).start()
    return {
        "ok": True,
        "accepted": True,
        "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
        "symbol": PHASE1_SYMBOL,
        "pine_minute_close_ms": minute_close,
        "processing": "background",
        "delivery_enabled": False,
        "database_write_enabled": False,
    }


def comparison_latest() -> dict[str, Any]:
    with _COMPARE_LOCK:
        if not _COMPARE_RESULTS:
            return {
                "ok": True,
                "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
                "sample_count": 0,
                "message": "No TradingView comparison webhook received since this Render process started.",
            }
        return dict(_COMPARE_RESULTS[-1])


def comparison_summary() -> dict[str, Any]:
    with _COMPARE_LOCK:
        rows = list(_COMPARE_RESULTS)

    count = len(rows)
    if not rows:
        return {
            "ok": True,
            "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
            "sample_count": 0,
            "signal_match_rate_pct": None,
            "alignment_offset_counts": {},
            "database_write_enabled": False,
        }

    signal_matches = sum(1 for row in rows if row.get("signal_match"))
    condition_matches = sum(1 for row in rows if row.get("condition_match"))
    buy_matches = sum(1 for row in rows if row.get("buy_chain_match"))
    sell_matches = sum(1 for row in rows if row.get("sell_chain_match"))
    offset_counts: dict[str, int] = {}
    diffs: list[float] = []
    price_bps: list[float] = []

    for row in rows:
        key = str(int(row.get("best_alignment_offset_ms") or 0))
        offset_counts[key] = offset_counts.get(key, 0) + 1
        if row.get("mean_abs_indicator_diff") is not None:
            diffs.append(float(row["mean_abs_indicator_diff"]))
        if row.get("server_minus_tv_price_bps") is not None:
            price_bps.append(abs(float(row["server_minus_tv_price_bps"])))

    return {
        "ok": True,
        "phase": "BTC_PHASE1_TV_AUTO_COMPARE",
        "sample_count": count,
        "signal_match_count": signal_matches,
        "signal_match_rate_pct": signal_matches / count * 100.0,
        "condition_match_rate_pct": condition_matches / count * 100.0,
        "buy_chain_match_rate_pct": buy_matches / count * 100.0,
        "sell_chain_match_rate_pct": sell_matches / count * 100.0,
        "alignment_offset_counts": offset_counts,
        "mean_abs_indicator_diff_avg": (sum(diffs) / len(diffs)) if diffs else None,
        "abs_price_diff_bps_avg": (sum(price_bps) / len(price_bps)) if price_bps else None,
        "first_received_at_utc": rows[0].get("received_at_utc"),
        "last_received_at_utc": rows[-1].get("received_at_utc"),
        "latest_signal_match": rows[-1].get("signal_match"),
        "database_write_enabled": False,
        "retention_note": f"in-memory only, max {COMPARE_HISTORY_LIMIT}; resets on Render restart/sleep/redeploy",
    }
