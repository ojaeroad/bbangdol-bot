"""Tajum On V150 automatic market signal engine (Binance/Upbit + Korea + US provider router).

Final operating path:
  member watchlist -> unique active exchange symbols -> one calculation per symbol
  -> existing V103 cadence state machine -> FCM fan-out to subscribed devices.

TradingView is not required for this worker. The legacy webhook/compare routes remain
available in app.py as validation/fallback paths.
"""
from __future__ import annotations

import os
import atexit
import time
import math
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Callable

import requests
import server_signal_engine as v133
import kis_market_provider as kis
import alpaca_market_provider as alpaca

log = logging.getLogger("bbangdol-bot.auto-engine")

STOCK_TF_ORDER = ("1w", "3d", "1d", "6h", "4h", "2h", "1h", "30m", "15m", "5m")
STOCK_MAX_CANDIDATES = frozenset(("1w","3d","1d","4h","1h","30m"))
STOCK_INTERNAL_CHAIN_TFS = frozenset(("6h","2h","15m","5m"))
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
    out["alpaca_us"] = alpaca.status()
    out["us_stock_provider"] = "ALPACA" if alpaca.configured() else "KIS_FALLBACK"
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
        "price": float(core["price_1m"]),
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
    # Descending order. A candidate requires every lower stock timeframe down to 5m.
    for i, tf in enumerate(STOCK_TF_ORDER):
        if tf not in STOCK_MAX_CANDIDATES:
            continue
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
            skipped.append(tf); warnings.append(f"{symbol} {tf}: not enough KIS candles ({len(rows)})"); return
        try:
            metrics[tf] = v133._latest_metric(rows, evaluation_time_ms=evaluation_time_ms)
        except RuntimeError as exc:
            if "warm-up incomplete" in str(exc) or "not enough" in str(exc).lower():
                skipped.append(tf); warnings.append(f"{symbol} {tf}: {exc}"); return
            raise

    if market == "KOREA":
        minute_rows = kis.domestic_minutes(symbol)
        for tf, mins in (
            ("5m",5),("15m",15),("30m",30),("1h",60),
            ("2h",120),("4h",240),("6h",360)
        ):
            add_metric(tf, kis.aggregate_stock_session(minute_rows, mins, "KOREA"))
        daily = kis.domestic_daily(symbol)
    else:
        # US cold-start uses only three source families while preserving exchange
        # session alignment: 5m -> 5/15/30m, 60m -> 1h/2h/6h, native 240m -> 4h.
        base_5m = kis.overseas_minutes(symbol, 5)
        add_metric("5m", base_5m)
        add_metric("15m", kis.aggregate_stock_session(base_5m, 15, "US"))
        add_metric("30m", kis.aggregate_stock_session(base_5m, 30, "US"))

        hourly = kis.overseas_minutes(symbol, 60)
        add_metric("1h", hourly)
        add_metric("2h", kis.aggregate_stock_session(hourly, 120, "US"))
        add_metric("4h", kis.overseas_minutes(symbol, 240))
        add_metric("6h", kis.aggregate_stock_session(hourly, 360, "US"))
        daily = kis.overseas_daily(symbol)

    add_metric("1d", daily)
    add_metric("3d", kis._day_aggregate(daily, 3))
    add_metric("1w", kis._day_aggregate(daily, 5))
    _, price = kis.current_price(symbol)
    return {
        "exchange": "KIS_KR" if market == "KOREA" else "KIS_US",
        "market": market, "symbol": symbol, "price": float(price),
        "evaluation_time_ms": evaluation_time_ms, "timeframes": metrics,
        "buy": _stock_chain_signal(metrics, is_ob=False),
        "sell": _stock_chain_signal(metrics, is_ob=True),
        "warnings": warnings, "skipped_timeframes": skipped,
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



from alert_queue import AlertQueue
from market_stream_engine import MarketStreamHub, classify as _classify_market
from prediction_engine import PredictionEngine

# V150 uses four independent market calculation workers. REST evaluators remain
# available as warm-up / gap-fill / fallback. WebSocket-owned candle series take priority.
_V150_STARTED = False
_V150_PID = 0
_V150_LOCK = threading.Lock()
_V150_STOP_EVENT = threading.Event()
_V150_ATEXIT_REGISTERED = False
_STREAM_HUB: MarketStreamHub | None = None
_ALERT_QUEUE: AlertQueue | None = None
_PREDICTION = PredictionEngine()
_MARKET_THREADS: dict[str, threading.Thread] = {}
_MARKET_STATUS_LOCK = threading.Lock()
_MARKET_STATUS: dict[str, dict[str, Any]] = {
    m: {"cycles":0,"running":False,"symbols":0,"success":0,"error":0,
        "in_progress":False,"current_total":0,"current_completed":0,
        "current_success":0,"current_error":0,
        "last_started_at":None,"last_finished_at":None,"last_error":None,
        "last_duration_sec":None}
    for m in ("BINANCE","UPBIT","KIS_KR","KIS_US")
}
_SEEDED: set[tuple[str,str]] = set()
_SEED_LOCK = threading.Lock()
_STOCK_REFRESH_LOCK = threading.Lock()
_STOCK_LAST_REFRESH: dict[tuple[str, str], float] = {}

V150_INTERVAL = max(
    15,
    min(int(os.getenv("TAJUM_V150_EVAL_INTERVAL_SEC", "60") or 60), 300),
)
STOCK_REST_REFRESH_SEC = max(
    45,
    min(int(os.getenv("TAJUM_STOCK_REST_FALLBACK_REFRESH_SEC", "75") or 75), 300),
)
SHARD_COUNT = max(1, int(os.getenv("TAJUM_WORKER_SHARD_COUNT","1") or 1))
SHARD_INDEX = max(0, int(os.getenv("TAJUM_WORKER_SHARD_INDEX","0") or 0)) % SHARD_COUNT

KST = ZoneInfo("Asia/Seoul")
NY = ZoneInfo("America/New_York")


def _owned(symbol: str) -> bool:
    if SHARD_COUNT <= 1:
        return True
    import hashlib
    n = int(hashlib.sha1(symbol.encode()).hexdigest()[:8], 16)
    return (n % SHARD_COUNT) == SHARD_INDEX


def _stock_market_is_open(market: str, now_utc: datetime | None = None) -> bool:
    """Regular-session gate used only for REST refresh policy.

    Holidays are intentionally not guessed here. On an exchange holiday, REST refresh
    may run a few harmless times; the WebSocket/REST fallback remains safe.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    if market == "KIS_KR":
        local = now_utc.astimezone(KST)
        if local.weekday() >= 5:
            return False
        hm = local.hour * 60 + local.minute
        return 9 * 60 <= hm <= 15 * 60 + 30
    if market == "KIS_US":
        local = now_utc.astimezone(NY)
        if local.weekday() >= 5:
            return False
        hm = local.hour * 60 + local.minute
        return 9 * 60 + 30 <= hm <= 16 * 60
    return True


def _stock_seed_rows(symbol: str, market: str) -> dict[str, list[dict[str, Any]]]:
    """Fetch each KIS source family only once and derive every internal TF locally."""
    if market == "KIS_KR":
        minute_rows = kis.domestic_minutes(symbol)
        daily = kis.domestic_daily(symbol)
        return {
            "1w": kis._day_aggregate(daily, 5),
            "3d": kis._day_aggregate(daily, 3),
            "1d": daily,
            "6h": kis.aggregate_stock_session(minute_rows, 360, "KOREA"),
            "4h": kis.aggregate_stock_session(minute_rows, 240, "KOREA"),
            "2h": kis.aggregate_stock_session(minute_rows, 120, "KOREA"),
            "1h": kis.aggregate_stock_session(minute_rows, 60, "KOREA"),
            "30m": kis.aggregate_stock_session(minute_rows, 30, "KOREA"),
            "15m": kis.aggregate_stock_session(minute_rows, 15, "KOREA"),
            "5m": kis.aggregate_stock_session(minute_rows, 5, "KOREA"),
        }

    # US primary = Alpaca when configured. KIS remains a compatibility fallback.
    if alpaca.configured():
        src = alpaca.warmup_sources(symbol)
        base_5m = src["5m"]
        base_30m = src["30m"]
        daily = src["1d"]
        return {
            "1w": kis._day_aggregate(daily, 5),
            "3d": kis._day_aggregate(daily, 3),
            "1d": daily,
            "6h": kis.aggregate_stock_session(base_30m, 360, "US"),
            "4h": kis.aggregate_stock_session(base_30m, 240, "US"),
            "2h": kis.aggregate_stock_session(base_30m, 120, "US"),
            "1h": kis.aggregate_stock_session(base_30m, 60, "US"),
            "30m": base_30m,
            "15m": kis.aggregate_stock_session(base_5m, 15, "US"),
            "5m": base_5m,
        }

    # KIS US fallback when Alpaca credentials are not configured yet.
    excd = kis.resolve_us_exchange(symbol)
    base_5m = kis.overseas_minutes(symbol, 5, exchange=excd)
    base_60m = kis.overseas_minutes(symbol, 60, exchange=excd)
    base_240m = kis.overseas_minutes(symbol, 240, exchange=excd)
    daily = kis.overseas_daily(symbol, exchange=excd)
    return {
        "1w": kis._day_aggregate(daily, 5),
        "3d": kis._day_aggregate(daily, 3),
        "1d": daily,
        "6h": kis.aggregate_stock_session(base_60m, 360, "US"),
        "4h": base_240m,
        "2h": kis.aggregate_stock_session(base_60m, 120, "US"),
        "1h": base_60m,
        "30m": kis.aggregate_stock_session(base_5m, 30, "US"),
        "15m": kis.aggregate_stock_session(base_5m, 15, "US"),
        "5m": base_5m,
    }


def _write_stock_book(symbol: str, market: str, rows_by_tf: dict[str, list[dict[str, Any]]]) -> None:
    if _STREAM_HUB is None:
        return
    # Seed high -> low so the freshest 5m close becomes CandleBook.last_price.
    for tf in STOCK_TF_ORDER:
        rows = rows_by_tf.get(tf) or []
        if rows:
            _STREAM_HUB.seed(symbol, market, tf, rows)


def _seed_symbol(symbol: str, market: str) -> None:
    """Initial warm-up.

    V147 seeded stock history only when WebSocket was already healthy, which meant a
    failed/quiet KIS stream kept calling REST forever and never populated CandleBook.
    V150 always seeds stock history exactly once first.
    """
    if _STREAM_HUB is None:
        return
    key = (market, symbol)
    with _SEED_LOCK:
        if key in _SEEDED:
            return

    try:
        if market == "BINANCE":
            v133.SUPPORTED_SYMBOLS.setdefault(symbol, symbol)
            for tf in v133.TF_ORDER:
                rows = v133._fetch_klines(symbol, tf, limit=min(v133.KLINE_LIMIT, 300))
                _STREAM_HUB.seed(symbol, market, tf, rows)

        elif market == "UPBIT":
            for tf in v133.TF_ORDER:
                _STREAM_HUB.seed(symbol, market, tf, _upbit_rows(symbol, tf))

        else:
            _write_stock_book(symbol, market, _stock_seed_rows(symbol, market))
            with _STOCK_REFRESH_LOCK:
                _STOCK_LAST_REFRESH[key] = time.monotonic()

        with _SEED_LOCK:
            _SEEDED.add(key)

    except Exception:
        # REST evaluator below is still available if initial warm-up fails.
        log.exception("V150 warm-up seed failed market=%s symbol=%s", market, symbol)


def _refresh_stock_book_if_due(symbol: str, market: str, *, force: bool = False) -> bool:
    """Refresh KIS REST-backed CandleBook only while live WebSocket is unavailable.

    Closed markets reuse their already-seeded book and generate *zero* repeated REST
    refresh traffic. Open markets refresh at a bounded cadence (default 75 sec).
    """
    if _STREAM_HUB is None:
        return False
    key = (market, symbol)
    now = time.monotonic()
    with _STOCK_REFRESH_LOCK:
        last = float(_STOCK_LAST_REFRESH.get(key, 0.0) or 0.0)
        if not force and now - last < STOCK_REST_REFRESH_SEC:
            return False

    rows_by_tf = _stock_seed_rows(symbol, market)
    _write_stock_book(symbol, market, rows_by_tf)
    with _STOCK_REFRESH_LOCK:
        _STOCK_LAST_REFRESH[key] = time.monotonic()
    return True


def _metric_result_from_book(symbol: str, market: str) -> dict[str, Any] | None:
    if _STREAM_HUB is None:
        return None
    _seed_symbol(symbol, market)
    eval_ms = int(datetime.now(timezone.utc).timestamp() // 60 * 60 * 1000)
    metrics: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    skipped: list[str] = []
    tf_order = v133.TF_ORDER if market in {"BINANCE","UPBIT"} else STOCK_TF_ORDER

    for tf in tf_order:
        rows = _STREAM_HUB.rows(symbol, tf, 320)
        if len(rows) < 31:
            skipped.append(tf)
            warnings.append(f"{symbol} {tf}: candle warm-up {len(rows)}")
            continue
        try:
            metrics[tf] = v133._latest_metric(rows, evaluation_time_ms=eval_ms)
        except RuntimeError as exc:
            skipped.append(tf)
            warnings.append(f"{symbol} {tf}: {exc}")

    price_age = 172800 if market in {"KIS_KR","KIS_US"} else 180
    price = _STREAM_HUB.last_price(symbol, max_age_sec=price_age)
    if price is None:
        return None

    if market in {"BINANCE","UPBIT"}:
        buy = _chain_signal_available(symbol, metrics, is_ob=False, price=price)
        sell = _chain_signal_available(symbol, metrics, is_ob=True, price=price)
    else:
        buy = _stock_chain_signal(metrics, is_ob=False)
        sell = _stock_chain_signal(metrics, is_ob=True)

    if market == "KIS_US" and alpaca.configured():
        source = (
            "ALPACA_WEBSOCKET"
            if _STREAM_HUB.healthy(market)
            else "ALPACA_REST_SEEDED_CANDLEBOOK"
        )
    elif market == "KIS_KR":
        source = (
            "KIS_WEBSOCKET"
            if _STREAM_HUB.healthy(market)
            else "KIS_REST_SEEDED_CANDLEBOOK"
        )
    else:
        source = "WEBSOCKET" if _STREAM_HUB.healthy(market) else "REST_SEEDED_CANDLEBOOK"
    return {
        "exchange": market,
        "market": "KOREA" if market == "KIS_KR" else ("US" if market == "KIS_US" else market),
        "symbol": symbol,
        "price": float(price),
        "evaluation_time_ms": eval_ms,
        "timeframes": metrics,
        "buy": buy,
        "sell": sell,
        "warnings": warnings,
        "skipped_timeframes": skipped,
        "data_source": source,
    }


def _evaluate_market_symbol(symbol: str, market: str) -> dict[str, Any]:
    # Crypto behavior remains unchanged.
    if market == "BINANCE":
        if _STREAM_HUB is not None and _STREAM_HUB.healthy(market):
            result = _metric_result_from_book(symbol, market)
            if result is not None:
                return result
        return _evaluate_binance(symbol)

    if market == "UPBIT":
        if _STREAM_HUB is not None and _STREAM_HUB.healthy(market):
            result = _metric_result_from_book(symbol, market)
            if result is not None:
                return result
        return _evaluate_upbit(symbol)

    # Stocks: seed once regardless of WS state.
    _seed_symbol(symbol, market)

    if _STREAM_HUB is not None:
        ws_fresh = _STREAM_HUB.healthy(market)

        # During the live regular session, REST refreshes only when WS is unavailable.
        # Outside session, the seeded book is reused without recurring KIS calls.
        if not ws_fresh and _stock_market_is_open(market):
            try:
                _refresh_stock_book_if_due(symbol, market)
            except Exception:
                log.exception("V150 stock REST fallback refresh failed market=%s symbol=%s", market, symbol)

        result = _metric_result_from_book(symbol, market)
        if result is not None:
            return result

    # Last-resort compatibility fallback.
    # Even after Alpaca becomes primary, KIS US can keep the service alive if Alpaca
    # is temporarily unavailable during the migration period.
    return _evaluate_kis_stock(symbol)


def _market_loop(market: str, subscription_provider: Callable[[], list[str]]) -> None:
    while not _V150_STOP_EVENT.is_set():
        started = datetime.now(timezone.utc)
        symbols = [s.upper() for s in (subscription_provider() or []) if _classify_market(s) == market and _owned(s.upper())]

        # KIS REST warm-up shares one global rate guard. At process start, give the
        # currently-open stock market first priority so live alerts recover quickly.
        if market == "KIS_KR" and not _stock_market_is_open("KIS_KR") and _stock_market_is_open("KIS_US"):
            with _MARKET_STATUS_LOCK:
                us_cycles = int(_MARKET_STATUS["KIS_US"].get("cycles", 0) or 0)
            if us_cycles == 0:
                _V150_STOP_EVENT.wait(2.0)
                continue
        if market == "KIS_US" and not _stock_market_is_open("KIS_US") and _stock_market_is_open("KIS_KR"):
            with _MARKET_STATUS_LOCK:
                kr_cycles = int(_MARKET_STATUS["KIS_KR"].get("cycles", 0) or 0)
            if kr_cycles == 0:
                _V150_STOP_EVENT.wait(2.0)
                continue
        symbols = list(dict.fromkeys(symbols))
        with _MARKET_STATUS_LOCK:
            _MARKET_STATUS[market].update(
                running=True, symbols=len(symbols), in_progress=True,
                current_total=len(symbols), current_completed=0,
                current_success=0, current_error=0,
                last_started_at=started.isoformat()
            )
        success = error = 0
        last_error = None
        for symbol in symbols:
            try:
                result = _evaluate_market_symbol(symbol, market)
                success += 1
                with _MARKET_STATUS_LOCK:
                    _MARKET_STATUS[market]["current_success"] = success
                for side in ("buy","sell"):
                    event = _event_from_chain(result, side)
                    if event and _ALERT_QUEUE is not None:
                        event["evaluation_time_ms"] = result.get("evaluation_time_ms")
                        pred = _PREDICTION.predict({"result":result,"event":event})
                        if pred is not None: event["prediction"] = pred
                        _ALERT_QUEUE.enqueue(event)
            except Exception as exc:
                error += 1
                with _MARKET_STATUS_LOCK:
                    _MARKET_STATUS[market]["current_error"] = error
                last_error = f"{symbol}: {type(exc).__name__}: {exc}"
                log.exception("V150 market worker failed market=%s symbol=%s", market, symbol)
            finally:
                with _MARKET_STATUS_LOCK:
                    _MARKET_STATUS[market]["current_completed"] = int(_MARKET_STATUS[market].get("current_completed", 0)) + 1
        finished = datetime.now(timezone.utc)
        with _MARKET_STATUS_LOCK:
            st = _MARKET_STATUS[market]
            st["cycles"] += 1
            st["success"] += success
            st["error"] += error
            st["in_progress"] = False
            st["current_total"] = len(symbols)
            st["current_completed"] = len(symbols)
            st["current_success"] = success
            st["current_error"] = error
            st["last_error"] = last_error
            st["last_finished_at"] = finished.isoformat()
            st["last_duration_sec"] = round((finished-started).total_seconds(),3)
        elapsed = (finished-started).total_seconds()
        _V150_STOP_EVENT.wait(max(1.0, V150_INTERVAL-elapsed))

def stop() -> None:
    """Best-effort graceful shutdown for Render/Gunicorn worker replacement."""
    global _V150_STARTED
    _V150_STOP_EVENT.set()
    try:
        if _STREAM_HUB is not None:
            _STREAM_HUB.stop()
    except Exception:
        log.exception("V150 stream shutdown failed")
    try:
        if _ALERT_QUEUE is not None:
            _ALERT_QUEUE.stop(drain_timeout=1.5)
    except Exception:
        log.exception("V150 alert queue shutdown failed")
    for thread in list(_MARKET_THREADS.values()):
        try:
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=0.4)
        except Exception:
            pass
    _V150_STARTED = False


def start(subscription_provider: Callable[[], list[str]], signal_callback: Callable[[dict[str, Any]], None]) -> bool:
    global _V150_STARTED, _V150_PID, _STREAM_HUB, _ALERT_QUEUE, _V150_ATEXIT_REGISTERED
    pid = os.getpid()
    with _V150_LOCK:
        if _V150_STARTED and _V150_PID == pid and any(t.is_alive() for t in _MARKET_THREADS.values()):
            return False
        _V150_STOP_EVENT.clear()
        _V150_STARTED = True
        _V150_PID = pid
        _STREAM_HUB = MarketStreamHub(subscription_provider)
        _STREAM_HUB.start()
        _ALERT_QUEUE = AlertQueue(signal_callback)
        _ALERT_QUEUE.start()
        for market in ("BINANCE","UPBIT","KIS_KR","KIS_US"):
            thread = threading.Thread(
                target=_market_loop,
                args=(market,subscription_provider),
                name=f"tajum-worker-{market.lower()}",
                daemon=True,
            )
            _MARKET_THREADS[market] = thread
            thread.start()
        if not _V150_ATEXIT_REGISTERED:
            atexit.register(stop)
            _V150_ATEXIT_REGISTERED = True
        return True

def status() -> dict[str, Any]:
    with _MARKET_STATUS_LOCK:
        markets = {k:dict(v) for k,v in _MARKET_STATUS.items()}
    total_symbols = sum(int(x.get("symbols",0)) for x in markets.values())
    total_cycles = sum(int(x.get("cycles",0)) for x in markets.values())
    current_total = sum(int(x.get("current_total",0)) for x in markets.values())
    current_completed = sum(int(x.get("current_completed",0)) for x in markets.values())
    current_success = sum(int(x.get("current_success",0)) for x in markets.values())
    current_error = sum(int(x.get("current_error",0)) for x in markets.values())
    lifetime_success = sum(int(x.get("success",0)) for x in markets.values())
    lifetime_error = sum(int(x.get("error",0)) for x in markets.values())
    streams = _STREAM_HUB.status() if _STREAM_HUB else {}
    queue_status = _ALERT_QUEUE.status() if _ALERT_QUEUE else {}
    # Backward-compatible keys remain so existing screenshots/status checks still work.
    return {
        "running": _V150_STARTED,
        "shutdown_requested": _V150_STOP_EVENT.is_set(),
        "worker_pid": _V150_PID or None,
        "worker_thread_alive": all(t.is_alive() for t in _MARKET_THREADS.values()) if _MARKET_THREADS else False,
        "workers": 4,
        "worker_model": "4_market_workers + crypto_ws + korea_kis_ws + us_alpaca_ws_or_kis_fallback + alert_queue",
        "shard_count": SHARD_COUNT, "shard_index": SHARD_INDEX,
        "cycles": total_cycles, "cycles_started": total_cycles + sum(1 for x in markets.values() if x.get("in_progress")),
        "cycle_in_progress": any(bool(x.get("in_progress")) for x in markets.values()),
        "current_cycle_total": current_total,
        "current_cycle_completed": current_completed,
        "current_cycle_success": current_success,
        "current_cycle_error": current_error,
        "last_symbol_count": total_symbols,
        "last_success_count": lifetime_success,
        "last_error_count": lifetime_error,
        "last_errors": [x["last_error"] for x in markets.values() if x.get("last_error")][-20:],
        "market_workers": markets,
        "streams": streams,
        "alert_queue": queue_status,
        "prediction_engine": _PREDICTION.status(),
        "stock_providers": {
            "korea_primary": "KIS",
            "us_primary": "ALPACA" if alpaca.configured() else "KIS_FALLBACK",
            "us_alpaca": alpaca.status(),
            "migration_note": "US worker is provider-routed; signal/candle/FCM logic is unchanged.",
        },
        "chain": {
            "coin_order": list(v133.TF_ORDER),
            "coin_internal_only": sorted(v133.INTERNAL_ONLY_TFS),
            "stock_order": list(STOCK_TF_ORDER),
            "stock_internal_only": sorted(STOCK_INTERNAL_CHAIN_TFS),
            "note": "V150: stock/coin mandatory chains preserved; KIS KR+US share one transport socket but calculate in separate workers",
        },
        "kis": kis.status(),
    }
