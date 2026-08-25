"""Phase-1 server signal engine for Tajum On.

Goal: reproduce the current TradingView Pine STARFLOWER logic without sending
Telegram/FCM alerts.  This first phase is intentionally restricted to BTCUSDT
and the 5m/15m chain so it can be compared safely before expanding.

Pine reference (V22B):
- RSI(14)
- slow stochastic K(5,3)
- ALL label additionally uses slow stochastic K(20,12)
- Oversold basic: RSI <= 30 and K(5,3) <= 20
- Overbought basic: RSI >= 70 and K(5,3) >= 80
- Highest satisfied candidate must form an unbroken chain down to 5m
- 5m/15m route to BD_BUY_SHORT / BD_SELL_SHORT

No database writes, Telegram sends, FCM sends or cadence state changes occur here.
"""

from __future__ import annotations

import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable

import requests

BINANCE_SPOT_BASE_URL = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
KLINE_LIMIT = max(100, min(int(os.getenv("SERVER_ENGINE_KLINE_LIMIT", "1000") or 1000), 1000))
REQUEST_TIMEOUT_SEC = max(3, min(int(os.getenv("SERVER_ENGINE_HTTP_TIMEOUT_SEC", "10") or 10), 30))

PHASE1_SYMBOL = "BTCUSDT"
PHASE1_SIGNAL_TFS = ("15m", "5m")  # descending, matching the Pine chain order for this phase
PHASE1_FETCH_TFS = ("1m", "5m", "15m")

RSI_LENGTH = 14
RSI_OS = 30.0
RSI_OB = 70.0
K_OS = 20.0
K_OB = 80.0

TF_ICON = {
    "15m": "🔺",
    "5m": "△",
}


def _safe_float(value: Any) -> float:
    out = float(value)
    if not math.isfinite(out):
        raise ValueError(f"non-finite numeric value: {value!r}")
    return out


def _fetch_klines(symbol: str, interval: str, limit: int = KLINE_LIMIT) -> list[dict[str, float | int]]:
    """Fetch Binance Spot klines, including the currently forming candle."""
    url = f"{BINANCE_SPOT_BASE_URL}/api/v3/klines"
    response = requests.get(
        url,
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=REQUEST_TIMEOUT_SEC,
    )
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
    if len(rows) < 50:
        raise RuntimeError(f"not enough Binance candles for {symbol} {interval}: {len(rows)}")
    return rows


def _pine_rsi_series(closes: Iterable[float], length: int = RSI_LENGTH) -> list[float | None]:
    """TradingView ta.rsi-compatible Wilder RSI using RMA(SMA seed)."""
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
    """Replicate f_slowk_expr(): raw %K over len_k then SMA(rawK, smooth)."""
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

    first_fk = len_k - 1
    first_slow = first_fk + smooth - 1
    for i in range(first_slow, n):
        window = fk[i - smooth + 1:i + 1]
        if any(v is None for v in window):
            continue
        out[i] = sum(float(v) for v in window) / smooth
    return out


def _latest_metric(rows: list[dict[str, float | int]]) -> dict[str, Any]:
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
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
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
        "bar_closed": int(last["close_time"]) < now_ms,
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "close": float(last["close"]),
        "candle_count": len(rows),
    }


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

    # Pine candidate order for phase 1: 15m first, then 5m.
    if bool(metrics_by_tf["15m"][key]) and bool(metrics_by_tf["5m"][key]):
        max_tf = "15m"
    elif bool(metrics_by_tf["5m"][key]):
        max_tf = "5m"

    if max_tf is None:
        return {
            "chain_ok": False,
            "max_timeframe": None,
            "route": "",
            "message_preview": None,
        }

    route = "BD_SELL_SHORT" if is_ob else "BD_BUY_SHORT"
    first_line = f"🪙 [BINANCE] {PHASE1_SYMBOL} : {_pine_price_fmt(price)}"
    msg = first_line + "\n\n" + _token(max_tf, metrics_by_tf[max_tf], is_ob)
    return {
        "chain_ok": True,
        "max_timeframe": max_tf,
        "route": route,
        "message_preview": msg,
    }


def evaluate_phase1_btc() -> dict[str, Any]:
    """Run a side-effect-free BTCUSDT 5m/15m Pine comparison snapshot."""
    fetched: dict[str, list[dict[str, float | int]]] = {}
    errors: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=len(PHASE1_FETCH_TFS), thread_name_prefix="server-engine-binance") as pool:
        future_map = {pool.submit(_fetch_klines, PHASE1_SYMBOL, tf): tf for tf in PHASE1_FETCH_TFS}
        for future in as_completed(future_map):
            tf = future_map[future]
            try:
                fetched[tf] = future.result()
            except Exception as exc:  # isolated test endpoint; surface a precise error
                errors[tf] = f"{type(exc).__name__}: {exc}"

    if errors:
        return {
            "ok": False,
            "phase": "BTC_PHASE1_COMPARE_ONLY",
            "delivery_enabled": False,
            "symbol": PHASE1_SYMBOL,
            "errors": errors,
        }

    metrics_by_tf = {tf: _latest_metric(fetched[tf]) for tf in PHASE1_SIGNAL_TFS}
    price_1m = float(fetched["1m"][-1]["close"])

    return {
        "ok": True,
        "phase": "BTC_PHASE1_COMPARE_ONLY",
        "delivery_enabled": False,
        "telegram_enabled": False,
        "fcm_enabled": False,
        "database_write_enabled": False,
        "symbol": PHASE1_SYMBOL,
        "market_source": "BINANCE_SPOT_REST",
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "comparison_price_1m": price_1m,
        "thresholds": {
            "rsi_length": RSI_LENGTH,
            "rsi_oversold": RSI_OS,
            "rsi_overbought": RSI_OB,
            "stoch_oversold": K_OS,
            "stoch_overbought": K_OB,
            "stoch_fast": "5,3",
            "stoch_slow": "20,12",
        },
        "timeframes": metrics_by_tf,
        "buy": _chain_signal(metrics_by_tf, is_ob=False, price=price_1m),
        "sell": _chain_signal(metrics_by_tf, is_ob=True, price=price_1m),
        "pine_contract": {
            "evaluation_basis": "current forming Binance candles; compare near each 1m Pine evaluation",
            "phase1_chain": "15m -> 5m; if 15m fails, 5m can still be the maximum candidate",
            "full_engine_internal_tf_note": "2h must later be calculated as an internal chain gate for 4h+ Pine parity, but it is not a user alert TF.",
        },
    }
