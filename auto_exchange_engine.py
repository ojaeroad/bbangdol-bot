"""Tajum On V136 automatic exchange signal engine.

Final operating path:
  member watchlist -> unique active exchange symbols -> one calculation per symbol
  -> existing V103 cadence state machine -> FCM fan-out to subscribed devices.

TradingView is not required for this worker. The legacy webhook/compare routes remain
available in app.py as validation/fallback paths.
"""
from __future__ import annotations

import os
import time
import math
import threading
import logging
from datetime import datetime, timezone
from typing import Any, Callable

import requests
import server_signal_engine as v133

log = logging.getLogger("bbangdol-bot.auto-engine")

AUTO_INTERVAL_SEC = max(30, min(int(os.getenv("TAJUM_AUTO_ENGINE_INTERVAL_SEC", "60") or 60), 300))
HTTP_TIMEOUT = max(3, min(int(os.getenv("TAJUM_AUTO_ENGINE_HTTP_TIMEOUT_SEC", "8") or 8), 30))
MAX_SYMBOLS_PER_CYCLE = max(1, min(int(os.getenv("TAJUM_AUTO_ENGINE_MAX_SYMBOLS", "300") or 300), 1000))
UPBIT_BASE = os.getenv("UPBIT_API_BASE", "https://api.upbit.com").rstrip("/")

_started = False
_start_lock = threading.Lock()
_status_lock = threading.Lock()
_status: dict[str, Any] = {
    "running": False,
    "cycles": 0,
    "last_cycle_started_at": None,
    "last_cycle_finished_at": None,
    "last_symbol_count": 0,
    "last_success_count": 0,
    "last_error_count": 0,
    "last_errors": [],
}


def status() -> dict[str, Any]:
    with _status_lock:
        return dict(_status)


def _set_status(**kwargs: Any) -> None:
    with _status_lock:
        _status.update(kwargs)


def _float(value: Any) -> float:
    out = float(value)
    if not math.isfinite(out):
        raise ValueError("non-finite number")
    return out


def _upbit_rows(market: str, timeframe: str, count: int = 300) -> list[dict[str, Any]]:
    """Fetch Upbit candles, oldest -> newest, then aggregate 2h/6h/12h locally."""
    if timeframe in {"5m", "15m", "30m", "1h", "4h"}:
        unit = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240}[timeframe]
        url = f"{UPBIT_BASE}/v1/candles/minutes/{unit}"
        response = requests.get(url, params={"market": market, "count": min(count, 200)}, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        raw = response.json()
        rows = [_upbit_item_to_row(x) for x in reversed(raw)]
        return rows
    if timeframe in {"1d", "1w"}:
        kind = "days" if timeframe == "1d" else "weeks"
        url = f"{UPBIT_BASE}/v1/candles/{kind}"
        response = requests.get(url, params={"market": market, "count": min(count, 200)}, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        raw = response.json()
        return [_upbit_item_to_row(x) for x in reversed(raw)]
    if timeframe in {"2h", "6h", "12h"}:
        hours = {"2h": 2, "6h": 6, "12h": 12}[timeframe]
        # Upbit minute-candle count is capped at 200. 200 x 1h gives only ~16
        # 12h candles, which is not enough for RSI/Stoch seed. For 12h use 4h
        # candles and aggregate 3 at a time; 2h/6h continue to use 1h candles.
        base_unit = 240 if timeframe == "12h" else 60
        url = f"{UPBIT_BASE}/v1/candles/minutes/{base_unit}"
        response = requests.get(url, params={"market": market, "count": 200}, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        base = [_upbit_item_to_row(x) for x in reversed(response.json())]
        return _aggregate_rows(base, hours * 60)
    raise ValueError(f"unsupported Upbit timeframe: {timeframe}")


def _upbit_item_to_row(item: dict[str, Any]) -> dict[str, Any]:
    dt = datetime.fromisoformat(str(item["candle_date_time_utc"]).replace("Z", "+00:00"))
    open_ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return {
        "open_time": open_ms,
        "open": _float(item["opening_price"]),
        "high": _float(item["high_price"]),
        "low": _float(item["low_price"]),
        "close": _float(item["trade_price"]),
        "volume": _float(item.get("candle_acc_trade_volume", 0.0)),
        "close_time": open_ms,
    }


def _aggregate_rows(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    bucket_ms = minutes * 60_000
    out: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    current_bucket: int | None = None
    for row in rows:
        bucket = int(row["open_time"]) // bucket_ms * bucket_ms
        if current is None or bucket != current_bucket:
            if current is not None:
                out.append(current)
            current_bucket = bucket
            current = {
                "open_time": bucket,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
                "close_time": int(row.get("close_time", row["open_time"])),
            }
        else:
            current["high"] = max(float(current["high"]), float(row["high"]))
            current["low"] = min(float(current["low"]), float(row["low"]))
            current["close"] = float(row["close"])
            current["volume"] = float(current["volume"]) + float(row.get("volume", 0.0))
            current["close_time"] = int(row.get("close_time", row["open_time"]))
    if current is not None:
        out.append(current)
    return out


def _evaluate_upbit(market: str) -> dict[str, Any]:
    evaluation_time_ms = int(datetime.now(timezone.utc).timestamp() // 60 * 60 * 1000)
    metrics: dict[str, dict[str, Any]] = {}
    # V133 chain order. Fetch only once per needed TF; 2h stays internal chain-only.
    for tf in reversed(v133.TF_ORDER):
        rows = _upbit_rows(market, tf)
        if len(rows) < 20:
            raise RuntimeError(f"not enough Upbit candles {market} {tf}: {len(rows)}")
        metrics[tf] = v133._latest_metric(rows, evaluation_time_ms=evaluation_time_ms)
    # Current price from the smallest timeframe.
    price = float(_upbit_rows(market, "5m", 50)[-1]["close"])
    # _chain_signal only needs a label and metrics, not a Binance-only fetch.
    buy = v133._chain_signal(market, metrics, is_ob=False, price=price)
    sell = v133._chain_signal(market, metrics, is_ob=True, price=price)
    return {
        "exchange": "UPBIT",
        "symbol": market,
        "price": price,
        "evaluation_time_ms": evaluation_time_ms,
        "timeframes": metrics,
        "buy": buy,
        "sell": sell,
    }


def _evaluate_binance(symbol: str) -> dict[str, Any]:
    # V133 restricted comparison to COIN9. For production calculations the same
    # verified calculation functions are reused after registering the active symbol.
    symbol = symbol.upper().replace(".P", "")
    v133.SUPPORTED_SYMBOLS.setdefault(symbol, symbol)
    core = v133._evaluate_symbol_at(symbol, v133._latest_closed_minute_boundary_ms())
    return {
        "exchange": "BINANCE",
        "symbol": symbol,
        "price": float(core["comparison_price_1m"]),
        **core,
    }


def _event_from_chain(result: dict[str, Any], side_key: str) -> dict[str, Any] | None:
    chain = result.get(side_key) or {}
    if not chain.get("chain_ok"):
        return None
    tf = str(chain.get("max_timeframe") or "")
    route = str(chain.get("route") or "")
    if not tf or not route:
        return None
    direction = "LOW" if side_key == "buy" else "HIGH"
    exchange = str(result.get("exchange") or "")
    symbol = str(result.get("symbol") or "")
    price = float(result.get("price") or 0.0)
    word = "저점" if direction == "LOW" else "고점"
    # Existing cadence parser only needs route/symbol/timeframe/message shape.
    prefix = "UPBIT" if exchange == "UPBIT" else "BINANCE"
    message = f"🪙 [{prefix}] {symbol} : {price}\n\n{tf} {word}"
    return {
        "exchange": exchange,
        "symbol": symbol,
        "direction": direction,
        "timeframe": tf,
        "route": route,
        "price": price,
        "message": message,
    }


def _run_loop(
    subscription_provider: Callable[[], list[str]],
    signal_callback: Callable[[dict[str, Any]], None],
) -> None:
    _set_status(running=True)
    while True:
        started = datetime.now(timezone.utc)
        errors: list[str] = []
        success = 0
        try:
            raw_symbols = subscription_provider() or []
            # Unique symbol only: one calculation no matter how many members subscribe.
            symbols = list(dict.fromkeys(str(x or "").strip().upper() for x in raw_symbols if str(x or "").strip()))[:MAX_SYMBOLS_PER_CYCLE]
            for symbol in symbols:
                try:
                    if symbol.startswith("KRW-"):
                        result = _evaluate_upbit(symbol)
                    elif symbol.endswith("USDT"):
                        result = _evaluate_binance(symbol)
                    else:
                        # V136 production auto engine is deliberately coin-only.
                        continue
                    success += 1
                    for side in ("buy", "sell"):
                        event = _event_from_chain(result, side)
                        if event:
                            signal_callback(event)
                    # Avoid hammering exchange REST endpoints in one burst.
                    time.sleep(0.05)
                except Exception as exc:
                    errors.append(f"{symbol}: {type(exc).__name__}: {exc}")
                    log.exception("Auto engine symbol failed %s", symbol)
        except Exception as exc:
            errors.append(f"cycle: {type(exc).__name__}: {exc}")
            log.exception("Auto engine cycle failed")
        finished = datetime.now(timezone.utc)
        with _status_lock:
            _status["cycles"] = int(_status.get("cycles", 0)) + 1
            _status["last_cycle_started_at"] = started.isoformat()
            _status["last_cycle_finished_at"] = finished.isoformat()
            _status["last_symbol_count"] = len(locals().get("symbols", []))
            _status["last_success_count"] = success
            _status["last_error_count"] = len(errors)
            _status["last_errors"] = errors[-20:]
        elapsed = (finished - started).total_seconds()
        time.sleep(max(1.0, AUTO_INTERVAL_SEC - elapsed))


def start(subscription_provider: Callable[[], list[str]], signal_callback: Callable[[dict[str, Any]], None]) -> bool:
    global _started
    with _start_lock:
        if _started:
            return False
        _started = True
        thread = threading.Thread(
            target=_run_loop,
            args=(subscription_provider, signal_callback),
            name="tajum-auto-exchange-engine",
            daemon=True,
        )
        thread.start()
        return True
