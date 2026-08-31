"""Tajum On V146 candle builder.

One market data stream is converted into all required timeframes locally.
Important internal chain gates:
- 2h and 6h are ALWAYS built and can participate in chain continuity.
- They do not have to be user-facing maximum alert timeframes.

The store accepts either trade ticks or exchange 1-minute candle snapshots.
Historical REST rows can seed any timeframe at process start / reconnect.
"""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any

UTC = timezone.utc
KST = ZoneInfo("Asia/Seoul")
NY = ZoneInfo("America/New_York")

TF_MINUTES = {
    "1m": 1,
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
}

COIN_TFS = ("1w", "1d", "12h", "6h", "4h", "2h", "1h", "30m", "15m", "5m")
STOCK_TFS = ("1w", "3d", "1d", "6h", "4h", "2h", "1h", "30m", "15m", "5m")


def _market_tz(market: str):
    if market == "KIS_KR":
        return KST
    if market == "KIS_US":
        return NY
    return UTC


def _session_open(dt_local: datetime, market: str) -> datetime:
    if market == "KIS_KR":
        return dt_local.replace(hour=9, minute=0, second=0, microsecond=0)
    if market == "KIS_US":
        return dt_local.replace(hour=9, minute=30, second=0, microsecond=0)
    return dt_local.replace(hour=0, minute=0, second=0, microsecond=0)


def _bucket_start_ms(ts_ms: int, timeframe: str, market: str) -> int:
    minutes = TF_MINUTES[timeframe]
    if market in {"BINANCE", "UPBIT"}:
        bucket_ms = minutes * 60_000
        return (int(ts_ms) // bucket_ms) * bucket_ms

    tz = _market_tz(market)
    local = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).astimezone(tz)

    # Intraday stock candles are session-open aligned, not UTC-midnight aligned.
    if minutes < 1440:
        start = _session_open(local, market)
        elapsed_min = int((local - start).total_seconds() // 60)
        if elapsed_min < 0:
            elapsed_min = 0
        bucket = start + __import__("datetime").timedelta(minutes=(elapsed_min // minutes) * minutes)
        return int(bucket.astimezone(UTC).timestamp() * 1000)

    # Daily and above are only used as an overlay on REST-seeded history.
    # Calendar-aligned keys are sufficient for new periods after a long-running process.
    if timeframe == "1d":
        start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    elif timeframe == "1w":
        start = (local - __import__("datetime").timedelta(days=local.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:  # 3d
        ordinal = local.date().toordinal()
        start_ord = ordinal - (ordinal % 3)
        start_date = datetime.fromordinal(start_ord).date()
        start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=tz)
    return int(start.astimezone(UTC).timestamp() * 1000)


class CandleBook:
    def __init__(self, max_bars: int = 420):
        self.max_bars = max(80, int(max_bars))
        self._lock = threading.RLock()
        self._series: dict[tuple[str, str], deque[dict[str, Any]]] = {}
        self._market: dict[str, str] = {}
        self._last_price: dict[str, tuple[int, float]] = {}
        self._minute_snapshot_volume: dict[tuple[str, int], float] = {}

    def seed(self, symbol: str, market: str, timeframe: str, rows: list[dict[str, Any]]) -> None:
        symbol = symbol.upper()
        cleaned = []
        for row in rows[-self.max_bars:]:
            try:
                cleaned.append({
                    "open_time": int(row["open_time"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0.0)),
                    "close_time": int(row.get("close_time", row["open_time"])),
                })
            except Exception:
                continue
        with self._lock:
            self._market[symbol] = market
            self._series[(symbol, timeframe)] = deque(cleaned, maxlen=self.max_bars)
            if cleaned:
                self._last_price[symbol] = (int(cleaned[-1]["close_time"]), float(cleaned[-1]["close"]))

    def rows(self, symbol: str, timeframe: str, limit: int = 320) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(x) for x in list(self._series.get((symbol.upper(), timeframe), ()))][-limit:]

    def last_price(self, symbol: str, max_age_sec: int = 180) -> float | None:
        with self._lock:
            item = self._last_price.get(symbol.upper())
        if not item:
            return None
        ts_ms, price = item
        if int(time.time() * 1000) - ts_ms > max_age_sec * 1000:
            return None
        return price

    def _update_tf(self, symbol: str, market: str, timeframe: str, ts_ms: int,
                   op: float, hi: float, lo: float, cl: float, vol_delta: float) -> None:
        key = (symbol, timeframe)
        bucket = _bucket_start_ms(ts_ms, timeframe, market)
        series = self._series.setdefault(key, deque(maxlen=self.max_bars))

        # For REST-seeded daily/3d/weekly stock bars, keep updating the newest bar
        # during the same live session instead of creating a differently anchored duplicate.
        if series and TF_MINUTES[timeframe] >= 1440 and market.startswith("KIS_"):
            last = series[-1]
            tz = _market_tz(market)
            last_date = datetime.fromtimestamp(int(last["open_time"]) / 1000, tz=UTC).astimezone(tz).date()
            now_date = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).astimezone(tz).date()
            max_days = {"1d": 1, "3d": 3, "1w": 7}[timeframe]
            if 0 <= (now_date - last_date).days < max_days:
                last["high"] = max(float(last["high"]), hi)
                last["low"] = min(float(last["low"]), lo)
                last["close"] = cl
                last["volume"] = float(last.get("volume", 0.0)) + max(0.0, vol_delta)
                last["close_time"] = ts_ms
                return

        if series and int(series[-1]["open_time"]) == bucket:
            last = series[-1]
            last["high"] = max(float(last["high"]), hi)
            last["low"] = min(float(last["low"]), lo)
            last["close"] = cl
            last["volume"] = float(last.get("volume", 0.0)) + max(0.0, vol_delta)
            last["close_time"] = ts_ms
        else:
            series.append({
                "open_time": bucket,
                "open": op,
                "high": hi,
                "low": lo,
                "close": cl,
                "volume": max(0.0, vol_delta),
                "close_time": ts_ms,
            })

    def update_tick(self, symbol: str, market: str, ts_ms: int, price: float, qty: float = 0.0) -> None:
        symbol = symbol.upper()
        price = float(price)
        qty = max(0.0, float(qty or 0.0))
        tfs = COIN_TFS if market in {"BINANCE", "UPBIT"} else STOCK_TFS
        with self._lock:
            self._market[symbol] = market
            self._last_price[symbol] = (int(ts_ms), price)
            for tf in tfs:
                self._update_tf(symbol, market, tf, int(ts_ms), price, price, price, price, qty)

    def update_minute_snapshot(self, symbol: str, market: str, open_time_ms: int,
                               op: float, hi: float, lo: float, cl: float, volume: float,
                               event_time_ms: int | None = None) -> None:
        """Replace/update a live 1m snapshot without double-counting cumulative minute volume."""
        symbol = symbol.upper()
        event_time_ms = int(event_time_ms or open_time_ms + 59_999)
        snap_key = (symbol, int(open_time_ms))
        with self._lock:
            prev = self._minute_snapshot_volume.get(snap_key, 0.0)
            volume_delta = max(0.0, float(volume or 0.0) - prev)
            self._minute_snapshot_volume[snap_key] = max(prev, float(volume or 0.0))
            self._last_price[symbol] = (event_time_ms, float(cl))
            tfs = COIN_TFS if market in {"BINANCE", "UPBIT"} else STOCK_TFS
            for tf in tfs:
                self._update_tf(
                    symbol, market, tf, event_time_ms,
                    float(op), float(hi), float(lo), float(cl), volume_delta
                )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "symbols": len(self._market),
                "series": len(self._series),
                "last_prices": len(self._last_price),
            }


# `time` is imported at bottom to keep the top constants easy to inspect.
import time
