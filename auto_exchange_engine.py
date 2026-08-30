"""Tajum On V145 automatic market signal engine (Binance/Upbit + KIS Korea/US).

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Callable

import requests
import server_signal_engine as v133
import kis_market_provider as kis

log = logging.getLogger("bbangdol-bot.auto-engine")

STOCK_TF_ORDER = ("1w", "3d", "1d", "4h", "1h", "30m")
STOCK_MIN_BARS = 31

AUTO_INTERVAL_SEC = max(30, min(int(os.getenv("TAJUM_AUTO_ENGINE_INTERVAL_SEC", "60") or 60), 300))
HTTP_TIMEOUT = max(3, min(int(os.getenv("TAJUM_AUTO_ENGINE_HTTP_TIMEOUT_SEC", "8") or 8), 30))
MAX_SYMBOLS_PER_CYCLE = max(1, min(int(os.getenv("TAJUM_AUTO_ENGINE_MAX_SYMBOLS", "300") or 300), 1000))
AUTO_WORKERS = max(1, min(int(os.getenv("TAJUM_AUTO_ENGINE_WORKERS", "4") or 4), 8))
UPBIT_BASE = os.getenv("UPBIT_API_BASE", "https://api.upbit.com").rstrip("/")
# Upbit candle REST limit is IP-based. Keep a conservative global rate across
# all worker threads and reuse responses inside a cycle to avoid 429 bursts.
UPBIT_MIN_INTERVAL_SEC = max(0.13, min(float(os.getenv("UPBIT_HTTP_MIN_INTERVAL_SEC", "0.13") or 0.13), 1.0))
UPBIT_CACHE_TTL_SEC = max(5, min(int(os.getenv("UPBIT_CACHE_TTL_SEC", "50") or 50), 300))
_upbit_http_lock = threading.Lock()
_upbit_last_http_at = 0.0
_upbit_cache_lock = threading.Lock()
_upbit_cache: dict[tuple[str, int], tuple[float, list[dict[str, Any]]]] = {}

_started = False
_started_pid = 0
_worker_thread: threading.Thread | None = None
_start_lock = threading.Lock()
_status_lock = threading.Lock()
_status: dict[str, Any] = {
    "running": False,
    "cycles": 0,
    "cycles_started": 0,
    "cycle_in_progress": False,
    "current_cycle_total": 0,
    "current_cycle_completed": 0,
    "current_cycle_success": 0,
    "current_cycle_error": 0,
    "current_symbols_in_flight": [],
    "last_processed_symbol": None,
    "last_result_at": None,
    "last_cycle_started_at": None,
    "last_cycle_finished_at": None,
    "last_symbol_count": 0,
    "last_success_count": 0,
    "last_error_count": 0,
    "last_errors": [],
    "workers": AUTO_WORKERS,
    "worker_pid": None,
    "worker_thread_alive": False,
    "worker_started_at": None,
    "worker_last_heartbeat": None,
    "worker_last_exception": None,
    "last_warnings": [],
    "last_skipped_timeframes": {},
    "market_success": {"BINANCE": 0, "UPBIT": 0, "KIS_KR": 0, "KIS_US": 0},
    "market_error": {"BINANCE": 0, "UPBIT": 0, "KIS_KR": 0, "KIS_US": 0},
    "kis": kis.status(),
}


def status() -> dict[str, Any]:
    with _status_lock:
        out = dict(_status)
        out["market_success"] = dict(_status.get("market_success") or {})
        out["market_error"] = dict(_status.get("market_error") or {})
    thread = _worker_thread
    out["worker_thread_alive"] = bool(thread and thread.is_alive())
    out["worker_pid"] = _started_pid or None
    out["kis"] = kis.status()
    return out


def _set_status(**kwargs: Any) -> None:
    with _status_lock:
        _status.update(kwargs)


def _float(value: Any) -> float:
    out = float(value)
    if not math.isfinite(out):
        raise ValueError("non-finite number")
    return out


def _upbit_rate_guard() -> None:
    global _upbit_last_http_at
    with _upbit_http_lock:
        now = time.monotonic()
        wait = UPBIT_MIN_INTERVAL_SEC - (now - _upbit_last_http_at)
        if wait > 0:
            time.sleep(wait)
        _upbit_last_http_at = time.monotonic()


def _upbit_fetch(path: str, market: str, *, request_count: int = 200) -> list[dict[str, Any]]:
    """Fetch one Upbit candle resource with global throttling/backoff/cache.

    The cache key intentionally ignores the caller's smaller requested count: each
    network call asks for up to 200 rows and callers slice locally. This lets 1h
    share data with 2h/6h, 4h share with 12h, and 5m current-price reuse 5m metrics.
    """
    key = (f"{path}|{market}", 200)
    now = time.monotonic()
    with _upbit_cache_lock:
        hit = _upbit_cache.get(key)
        if hit and now - hit[0] <= UPBIT_CACHE_TTL_SEC:
            return list(hit[1])

    url = f"{UPBIT_BASE}{path}"
    last_exc: Exception | None = None
    for attempt in range(5):
        _upbit_rate_guard()
        try:
            response = requests.get(url, params={"market": market, "count": 200}, timeout=HTTP_TIMEOUT)
            if response.status_code in {429, 418}:
                # 429 = rate limit, 418 = temporary block. Back off globally before retry.
                delay = min(8.0, 0.75 * (2 ** attempt))
                time.sleep(delay)
                last_exc = requests.HTTPError(f"Upbit {response.status_code} rate limit for {market} {path}")
                continue
            response.raise_for_status()
            raw = response.json()
            if not isinstance(raw, list):
                raise RuntimeError(f"unexpected Upbit candle response: {type(raw).__name__}")
            with _upbit_cache_lock:
                _upbit_cache[key] = (time.monotonic(), list(raw))
                # bounded cache: active symbols x a handful of candle resources only
                if len(_upbit_cache) > 2000:
                    cutoff = time.monotonic() - UPBIT_CACHE_TTL_SEC
                    for cache_key, (ts, _) in list(_upbit_cache.items()):
                        if ts < cutoff:
                            _upbit_cache.pop(cache_key, None)
            return raw
        except (requests.RequestException, RuntimeError) as exc:
            last_exc = exc
            if attempt >= 4:
                break
            time.sleep(min(4.0, 0.35 * (2 ** attempt)))
    assert last_exc is not None
    raise last_exc


def _upbit_rows(market: str, timeframe: str, count: int = 300) -> list[dict[str, Any]]:
    """Fetch Upbit candles, oldest -> newest, then aggregate 2h/6h/12h locally."""
    if timeframe in {"5m", "15m", "30m", "1h", "4h"}:
        unit = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240}[timeframe]
        raw = _upbit_fetch(f"/v1/candles/minutes/{unit}", market)
        rows = [_upbit_item_to_row(x) for x in reversed(raw)]
        return rows[-min(count, 200):]
    if timeframe in {"1d", "1w"}:
        kind = "days" if timeframe == "1d" else "weeks"
        raw = _upbit_fetch(f"/v1/candles/{kind}", market)
        rows = [_upbit_item_to_row(x) for x in reversed(raw)]
        return rows[-min(count, 200):]
    if timeframe in {"2h", "6h", "12h"}:
        hours = {"2h": 2, "6h": 6, "12h": 12}[timeframe]
        # 12h uses cached 4h candles, 2h/6h reuse the cached 1h resource.
        base_unit = 240 if timeframe == "12h" else 60
        raw = _upbit_fetch(f"/v1/candles/minutes/{base_unit}", market)
        base = [_upbit_item_to_row(x) for x in reversed(raw)]
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


def _chain_signal_available(
    symbol: str,
    metrics_by_tf: dict[str, dict[str, Any]],
    *,
    is_ob: bool,
    price: float,
) -> dict[str, Any]:
    """V141 chain calculation that tolerates missing *higher* history timeframes.

    A candidate is considered only when every timeframe required from that candidate
    down to 5m exists. Example: if a newly listed coin has only 13 weekly candles,
    1w is skipped but 1d/12h/... signals can still be evaluated normally.
    """
    key = "ob_basic" if is_ob else "os_basic"
    for i, tf in enumerate(v133.TF_ORDER):
        if tf not in v133.MAX_CANDIDATES:
            continue
        required = list(v133.TF_ORDER[i:])
        if any(req not in metrics_by_tf for req in required):
            continue
        if all(bool(metrics_by_tf[req][key]) for req in required):
            route = v133._route_for_tf(tf, is_ob=is_ob)
            return {
                "chain_ok": True,
                "max_timeframe": tf,
                "route": route,
                "message_preview": None,
            }
    return {"chain_ok": False, "max_timeframe": None, "route": "", "message_preview": None}


def _evaluate_upbit(market: str) -> dict[str, Any]:
    evaluation_time_ms = int(datetime.now(timezone.utc).timestamp() // 60 * 60 * 1000)
    metrics: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    skipped_timeframes: list[str] = []

    # V141: insufficient history in one timeframe (common for newly listed coins)
    # must not fail the whole symbol. Skip only that TF; lower complete chains remain valid.
    for tf in reversed(v133.TF_ORDER):
        rows = _upbit_rows(market, tf)
        if len(rows) < 31:  # RSI14 + Stoch20,12 warm-up needs more than 20 candles.
            skipped_timeframes.append(tf)
            warnings.append(f"{market} {tf}: not enough candles ({len(rows)})")
            continue
        try:
            metrics[tf] = v133._latest_metric(rows, evaluation_time_ms=evaluation_time_ms)
        except RuntimeError as exc:
            if "warm-up incomplete" in str(exc):
                skipped_timeframes.append(tf)
                warnings.append(f"{market} {tf}: {exc}")
                continue
            raise

    # Current price is independent of the long-TF history availability.
    price_rows = _upbit_rows(market, "5m", 50)
    if not price_rows:
        raise RuntimeError(f"no Upbit price candles {market} 5m")
    price = float(price_rows[-1]["close"])

    buy = _chain_signal_available(market, metrics, is_ob=False, price=price)
    sell = _chain_signal_available(market, metrics, is_ob=True, price=price)
    return {
        "exchange": "UPBIT",
        "symbol": market,
        "price": price,
        "evaluation_time_ms": evaluation_time_ms,
        "timeframes": metrics,
        "buy": buy,
        "sell": sell,
        "warnings": warnings,
        "skipped_timeframes": skipped_timeframes,
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



def _stock_route_for_tf(timeframe: str, *, is_ob: bool) -> str:
    if timeframe in ("30m", "1h"):
        return "SELL_SWING_1Q" if is_ob else "BUY_SWING_1Q"
    if timeframe in ("4h", "1d"):
        return "SELL_LONG_1Q" if is_ob else "BUY_LONG_1Q"
    if timeframe in ("3d", "1w"):
        return "SELL_LIFE_1Q" if is_ob else "BUY_LIFE_1Q"
    return ""


def _stock_chain_signal(metrics_by_tf: dict[str, dict[str, Any]], *, is_ob: bool) -> dict[str, Any]:
    key = "ob_basic" if is_ob else "os_basic"
    # Descending order. A candidate requires every lower stock timeframe down to 30m.
    for i, tf in enumerate(STOCK_TF_ORDER):
        required = STOCK_TF_ORDER[i:]
        if any(req not in metrics_by_tf for req in required):
            continue
        if all(bool(metrics_by_tf[req][key]) for req in required):
            return {
                "chain_ok": True,
                "max_timeframe": tf,
                "route": _stock_route_for_tf(tf, is_ob=is_ob),
                "message_preview": None,
            }
    return {"chain_ok": False, "max_timeframe": None, "route": "", "message_preview": None}


def _evaluate_kis_stock(symbol: str) -> dict[str, Any]:
    if not kis.configured():
        raise RuntimeError("KIS_NOT_CONFIGURED: set KIS_APP_KEY/KIS_APP_SECRET")

    evaluation_time_ms = int(datetime.now(timezone.utc).timestamp() // 60 * 60 * 1000)
    metrics: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    skipped: list[str] = []
    market = "KOREA" if symbol.isdigit() and len(symbol) == 6 else "US"

    def add_metric(tf: str, rows: list[dict[str, Any]]) -> None:
        if len(rows) < STOCK_MIN_BARS:
            skipped.append(tf)
            warnings.append(f"{symbol} {tf}: not enough KIS candles ({len(rows)})")
            return
        try:
            metrics[tf] = v133._latest_metric(rows, evaluation_time_ms=evaluation_time_ms)
        except RuntimeError as exc:
            if "warm-up incomplete" in str(exc) or "not enough" in str(exc).lower():
                skipped.append(tf)
                warnings.append(f"{symbol} {tf}: {exc}")
                return
            raise

    if market == "KOREA":
        # V145: fetch the expensive KRX 1-minute warm-up ONCE per symbol/cycle,
        # then derive 30m/1h/4h locally. V144 called domestic_minutes() once per
        # timeframe; during market hours that refreshed the current session tail
        # three separate times and multiplied KIS traffic.
        minute_rows = kis.domestic_minutes(symbol)
        add_metric("30m", kis.aggregate(minute_rows, 30))
        add_metric("1h", kis.aggregate(minute_rows, 60))
        add_metric("4h", kis.aggregate(minute_rows, 240))

        # Daily history is also fetched once and reused for 1d/3d/1w.
        daily = kis.domestic_daily(symbol)
        add_metric("1d", daily)
        add_metric("3d", kis._day_aggregate(daily, 3))
        add_metric("1w", kis._day_aggregate(daily, 5))
    else:
        # Keep KIS-native 30/60/240-minute bars for US stocks so candle anchoring
        # remains identical to the exchange/KIS definition. Provider-level caching
        # and the global KIS pacer control request volume.
        for tf in ("30m", "1h", "4h"):
            try:
                _, rows = kis.rows(symbol, tf)
                add_metric(tf, rows)
            except RuntimeError as exc:
                if "warm-up incomplete" in str(exc) or "not enough" in str(exc).lower():
                    skipped.append(tf)
                    warnings.append(f"{symbol} {tf}: {exc}")
                    continue
                raise

        # One daily fetch, reused locally for all long stock timeframes.
        daily = kis.overseas_daily(symbol)
        add_metric("1d", daily)
        add_metric("3d", kis._day_aggregate(daily, 3))
        add_metric("1w", kis._day_aggregate(daily, 5))

    _, price = kis.current_price(symbol)
    buy = _stock_chain_signal(metrics, is_ob=False)
    sell = _stock_chain_signal(metrics, is_ob=True)
    return {
        "exchange": "KIS_KR" if market == "KOREA" else "KIS_US",
        "market": market,
        "symbol": symbol,
        "price": float(price),
        "evaluation_time_ms": evaluation_time_ms,
        "timeframes": metrics,
        "buy": buy,
        "sell": sell,
        "warnings": warnings,
        "skipped_timeframes": skipped,
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
    prefix = {
        "UPBIT": "UPBIT",
        "BINANCE": "BINANCE",
        "KIS_KR": "KIS-KR",
        "KIS_US": "KIS-US",
    }.get(exchange, exchange or "AUTO")
    icon = "🪙" if exchange in {"UPBIT", "BINANCE"} else "📈"
    message = f"{icon} [{prefix}] {symbol} : {price}\n\n{tf} {word}"
    return {
        "exchange": exchange,
        "symbol": symbol,
        "direction": direction,
        "timeframe": tf,
        "route": route,
        "price": price,
        "message": message,
    }


def _market_name_for_symbol(symbol: str) -> str:
    symbol = str(symbol or "").strip().upper()
    if symbol.startswith("KRW-"):
        return "UPBIT"
    if symbol.endswith("USDT"):
        return "BINANCE"
    if symbol.isdigit() and len(symbol) == 6:
        return "KIS_KR"
    return "KIS_US"


def _evaluate_one_symbol(symbol: str) -> tuple[str, dict[str, Any]]:
    if symbol.startswith("KRW-"):
        return symbol, _evaluate_upbit(symbol)
    if symbol.endswith("USDT"):
        return symbol, _evaluate_binance(symbol)
    if symbol.isdigit() and len(symbol) == 6:
        return symbol, _evaluate_kis_stock(symbol)
    if symbol and symbol.replace(".", "").replace("-", "").isalnum():
        return symbol, _evaluate_kis_stock(symbol)
    raise ValueError(f"unsupported automatic market symbol: {symbol}")


def _run_loop(
    subscription_provider: Callable[[], list[str]],
    signal_callback: Callable[[dict[str, Any]], None],
) -> None:
    _set_status(running=True, worker_pid=os.getpid(), worker_thread_alive=True, worker_started_at=datetime.now(timezone.utc).isoformat(), worker_last_heartbeat=datetime.now(timezone.utc).isoformat(), worker_last_exception=None)
    executor = ThreadPoolExecutor(max_workers=AUTO_WORKERS, thread_name_prefix="tajum-market-calc")
    while True:
        started = datetime.now(timezone.utc)
        _set_status(worker_last_heartbeat=started.isoformat(), worker_thread_alive=True)
        errors: list[str] = []
        warnings: list[str] = []
        skipped_by_symbol: dict[str, list[str]] = {}
        success = 0
        symbols: list[str] = []
        try:
            raw_symbols = subscription_provider() or []
            symbols = list(dict.fromkeys(
                str(x or "").strip().upper()
                for x in raw_symbols
                if str(x or "").strip()
            ))[:MAX_SYMBOLS_PER_CYCLE]

            with _status_lock:
                _status["cycles_started"] = int(_status.get("cycles_started", 0)) + 1
                _status["cycle_in_progress"] = True
                _status["current_cycle_total"] = len(symbols)
                _status["current_cycle_completed"] = 0
                _status["current_cycle_success"] = 0
                _status["current_cycle_error"] = 0
                _status["current_symbols_in_flight"] = list(symbols[:AUTO_WORKERS])
                _status["last_cycle_started_at"] = started.isoformat()

            future_map = {executor.submit(_evaluate_one_symbol, symbol): symbol for symbol in symbols}
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    _, result = future.result()
                    success += 1
                    exchange = str(result.get("exchange") or _market_name_for_symbol(symbol))
                    with _status_lock:
                        bucket = dict(_status.get("market_success") or {})
                        bucket[exchange] = int(bucket.get(exchange, 0) or 0) + 1
                        _status["market_success"] = bucket
                    result_warnings = [str(x) for x in (result.get("warnings") or []) if str(x)]
                    if result_warnings:
                        warnings.extend(result_warnings)
                    skipped = [str(x) for x in (result.get("skipped_timeframes") or []) if str(x)]
                    if skipped:
                        skipped_by_symbol[symbol] = skipped
                    for side in ("buy", "sell"):
                        event = _event_from_chain(result, side)
                        if event:
                            signal_callback(event)
                except Exception as exc:
                    msg = f"{symbol}: {type(exc).__name__}: {exc}"
                    errors.append(msg)
                    exchange = _market_name_for_symbol(symbol)
                    with _status_lock:
                        bucket = dict(_status.get("market_error") or {})
                        bucket[exchange] = int(bucket.get(exchange, 0) or 0) + 1
                        _status["market_error"] = bucket
                    log.exception("Auto engine symbol failed %s", symbol)
                finally:
                    now = datetime.now(timezone.utc).isoformat()
                    with _status_lock:
                        completed = int(_status.get("current_cycle_completed", 0)) + 1
                        _status["current_cycle_completed"] = completed
                        _status["current_cycle_success"] = success
                        _status["current_cycle_error"] = len(errors)
                        _status["last_processed_symbol"] = symbol
                        _status["last_result_at"] = now
                        # Lightweight visibility only; exact futures state is not needed.
                        remaining = [s for f, s in future_map.items() if not f.done()]
                        _status["current_symbols_in_flight"] = remaining[:AUTO_WORKERS]
        except Exception as exc:
            errors.append(f"cycle: {type(exc).__name__}: {exc}")
            _set_status(worker_last_exception=f"{type(exc).__name__}: {exc}")
            log.exception("Auto engine cycle failed")
        finished = datetime.now(timezone.utc)
        with _status_lock:
            _status["cycles"] = int(_status.get("cycles", 0)) + 1
            _status["cycle_in_progress"] = False
            _status["current_symbols_in_flight"] = []
            _status["last_cycle_finished_at"] = finished.isoformat()
            _status["last_symbol_count"] = len(symbols)
            _status["last_success_count"] = success
            _status["last_error_count"] = len(errors)
            _status["last_errors"] = errors[-20:]
            _status["last_warnings"] = warnings[-50:]
            _status["last_skipped_timeframes"] = dict(skipped_by_symbol)
        elapsed = (finished - started).total_seconds()
        time.sleep(max(1.0, AUTO_INTERVAL_SEC - elapsed))

def start(subscription_provider: Callable[[], list[str]], signal_callback: Callable[[dict[str, Any]], None]) -> bool:
    # Gunicorn imports app.py before/around worker fork. A daemon thread created in
    # the parent process does not survive the fork, while module globals may still
    # say "started" in the child. Track PID + actual thread liveness so each live
    # worker can recover the engine safely.
    global _started, _started_pid, _worker_thread
    pid = os.getpid()
    with _start_lock:
        if _started_pid != pid:
            _started = False
            _worker_thread = None
            _started_pid = pid
        if _started and _worker_thread is not None and _worker_thread.is_alive():
            return False
        _started = True
        _worker_thread = threading.Thread(
            target=_run_loop,
            args=(subscription_provider, signal_callback),
            name="tajum-auto-market-engine",
            daemon=True,
        )
        _worker_thread.start()
        _set_status(worker_pid=pid, worker_thread_alive=True, worker_started_at=datetime.now(timezone.utc).isoformat())
        return True
