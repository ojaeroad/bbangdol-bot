"""Tajum On V146 durable-ish alert queue with optional Redis.

The queue protects the signal->FCM handoff from short CPU/network stalls and performs
deduplication before calling the existing app.py FCM callback.

If REDIS_URL + redis package are available, dedup keys survive process restarts.
Without Redis it safely falls back to in-process memory.
"""
from __future__ import annotations
import hashlib, json, os, queue, threading, time
from datetime import datetime, timezone
from typing import Any, Callable

try:
    import redis as _redis_mod
except Exception:
    _redis_mod = None

class AlertQueue:
    def __init__(self, callback: Callable[[dict[str, Any]], Any]):
        self.callback = callback
        self.q: queue.Queue[tuple[dict[str, Any], int]] = queue.Queue(maxsize=max(1000, int(os.getenv("TAJUM_ALERT_QUEUE_MAX", "20000"))))
        self.max_retry = max(1, min(int(os.getenv("TAJUM_ALERT_QUEUE_RETRY", "4")), 10))
        self.dedup_ttl = max(30, min(int(os.getenv("TAJUM_ALERT_DEDUP_TTL_SEC", "180")), 3600))
        self._seen: dict[str, float] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._success = 0
        self._failure = 0
        self._retry = 0
        self._duplicate = 0
        self._last_error = None
        self._redis = None
        url = os.getenv("REDIS_URL", "").strip()
        if url and _redis_mod is not None:
            try:
                self._redis = _redis_mod.from_url(url, decode_responses=True, socket_timeout=1.0)
                self._redis.ping()
            except Exception:
                self._redis = None

    @staticmethod
    def event_id(event: dict[str, Any]) -> str:
        parts = [
            str(event.get("exchange", "")), str(event.get("symbol", "")),
            str(event.get("direction", "")), str(event.get("timeframe", "")),
            str(event.get("route", "")), str(event.get("evaluation_time_ms", "")),
        ]
        return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()

    def _claim(self, key: str) -> bool:
        if self._redis is not None:
            try:
                return bool(self._redis.set(f"tajum:alert:{key}", "1", nx=True, ex=self.dedup_ttl))
            except Exception:
                pass
        now = time.monotonic()
        with self._lock:
            for k, exp in list(self._seen.items()):
                if exp <= now:
                    self._seen.pop(k, None)
            if key in self._seen:
                return False
            self._seen[key] = now + self.dedup_ttl
            return True

    def enqueue(self, event: dict[str, Any]) -> bool:
        key = self.event_id(event)
        if not self._claim(key):
            self._duplicate += 1
            return False
        event = dict(event)
        event["signal_id"] = key
        try:
            self.q.put_nowait((event, 0))
            return True
        except queue.Full:
            self._failure += 1
            self._last_error = "queue full"
            return False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, name="tajum-alert-queue", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while self._running:
            try:
                event, retry = self.q.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                self.callback(event)
                self._success += 1
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                if retry < self.max_retry:
                    self._retry += 1
                    time.sleep(min(8.0, 0.5 * (2 ** retry)))
                    try:
                        self.q.put_nowait((event, retry + 1))
                    except queue.Full:
                        self._failure += 1
                else:
                    self._failure += 1
            finally:
                self.q.task_done()

    def status(self) -> dict[str, Any]:
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "pending": self.q.qsize(),
            "success": self._success,
            "failure": self._failure,
            "retry": self._retry,
            "duplicate_blocked": self._duplicate,
            "redis_enabled": self._redis is not None,
            "last_error": self._last_error,
        }
