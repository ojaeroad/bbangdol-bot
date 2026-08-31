"""Tajum On V148 real-time market stream hub.

Primary market-data paths
-------------------------
- Binance USDT : public 1m kline WebSocket
- Upbit KRW    : public 1m candle WebSocket
- KIS KR / US  : ONE shared KIS WebSocket session, separate downstream market workers

Why one KIS socket?
-------------------
KIS official samples subscribe domestic + overseas real-time data on one session.
V147 opened KR and US KIS sockets concurrently with the same approval key; on the
live server KIS_US repeatedly ended with BrokenPipe. V148 follows the official
single-session pattern and keeps the *calculation workers* separated.

REST is still the warm-up / gap-fill / fallback source. WebSocket loss must never
stop the signal engine.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import requests

from candle_builder import CandleBook

try:
    import websocket
except Exception:
    websocket = None

UTC = timezone.utc
KST = ZoneInfo("Asia/Seoul")
NY = ZoneInfo("America/New_York")

BINANCE_WS = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
UPBIT_WS = os.getenv("UPBIT_WS_URL", "wss://api.upbit.com/websocket/v1")
KIS_WS = os.getenv("KIS_WS_URL", "ws://ops.koreainvestment.com:21000")
KIS_BASE = os.getenv("KIS_API_BASE", "https://openapi.koreainvestment.com:9443").rstrip("/")
KIS_KEY = os.getenv("KIS_APP_KEY", "").strip()
KIS_SECRET = os.getenv("KIS_APP_SECRET", "").strip()

# KIS official current websocket helper uses 0.1 sec pacing between subscriptions.
# Use a slightly safer default on the public live endpoint.
KIS_WS_SUBSCRIBE_INTERVAL = max(
    0.10,
    min(float(os.getenv("KIS_WS_SUBSCRIBE_INTERVAL_SEC", "0.15") or 0.15), 1.0),
)
KIS_WS_RECV_TIMEOUT = max(
    10.0,
    min(float(os.getenv("KIS_WS_RECV_TIMEOUT_SEC", "30") or 30), 120.0),
)
KIS_WS_MAX_SUBSCRIPTIONS = 40


def classify(symbol: str) -> str:
    s = symbol.upper()
    if s.startswith("KRW-"):
        return "UPBIT"
    if s.endswith("USDT"):
        return "BINANCE"
    if s.isdigit() and len(s) == 6:
        return "KIS_KR"
    return "KIS_US"


def _stock_market_open(market: str, now_utc: datetime | None = None) -> bool:
    now_utc = now_utc or datetime.now(UTC)
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


class MarketStreamHub:
    def __init__(self, subscription_provider: Callable[[], list[str]]):
        self.subscription_provider = subscription_provider
        self.book = CandleBook(max_bars=int(os.getenv("TAJUM_CANDLE_MAX_BARS", "420")))
        self._threads: dict[str, threading.Thread] = {}
        self._stop = threading.Event()
        self._status_lock = threading.Lock()
        self._status = {
            m: {
                "websocket_available": websocket is not None,
                "connected": False,
                "symbols": 0,
                "messages": 0,
                "reconnects": 0,
                "last_message_at": None,
                "last_error": None,
                "mode": "REST_FALLBACK",
                "subscribed_count": 0,
                "subscription_errors": 0,
                "overflow_count": 0,
            }
            for m in ("BINANCE", "UPBIT", "KIS_KR", "KIS_US")
        }
        self._kis_approval = None
        self._kis_approval_exp = 0.0
        self._kis_subscribed: dict[str, set[str]] = {"KIS_KR": set(), "KIS_US": set()}

    def _symbols(self, market: str) -> list[str]:
        return sorted(
            {
                s.upper()
                for s in (self.subscription_provider() or [])
                if classify(s) == market
            }
        )

    def _set(self, market: str, **kw: Any) -> None:
        with self._status_lock:
            self._status[market].update(kw)

    def _inc(self, market: str, key: str, amount: int = 1) -> None:
        with self._status_lock:
            self._status[market][key] = int(self._status[market].get(key, 0) or 0) + amount

    def _msg(self, market: str) -> None:
        with self._status_lock:
            st = self._status[market]
            st["messages"] += 1
            st["last_message_at"] = datetime.now(UTC).isoformat()
            st["connected"] = True
            st["mode"] = "WEBSOCKET_PRIMARY"
            st["last_error"] = None

    def start(self) -> None:
        # Two crypto sockets + one shared KIS socket.
        for name, target in [
            ("BINANCE", self._binance_loop),
            ("UPBIT", self._upbit_loop),
            ("KIS_SHARED", self._kis_loop_combined),
        ]:
            if name in self._threads and self._threads[name].is_alive():
                continue
            t = threading.Thread(target=target, name=f"tajum-ws-{name.lower()}", daemon=True)
            self._threads[name] = t
            if name == "KIS_SHARED":
                # Both market status entries point to the same transport thread.
                self._threads["KIS_KR"] = t
                self._threads["KIS_US"] = t
            t.start()

    def seed(self, symbol: str, market: str, timeframe: str, rows: list[dict[str, Any]]) -> None:
        self.book.seed(symbol, market, timeframe, rows)

    def rows(self, symbol: str, timeframe: str, limit: int = 320) -> list[dict[str, Any]]:
        return self.book.rows(symbol, timeframe, limit)

    def last_price(self, symbol: str, max_age_sec: int = 180) -> float | None:
        return self.book.last_price(symbol, max_age_sec)

    def healthy(self, market: str, stale_sec: int = 180) -> bool:
        """Fresh real-time data received recently for this market."""
        with self._status_lock:
            st = dict(self._status[market])
        if not st.get("connected") or not st.get("last_message_at"):
            return False
        try:
            dt = datetime.fromisoformat(str(st["last_message_at"]))
            return (datetime.now(UTC) - dt).total_seconds() <= stale_sec
        except Exception:
            return False

    def subscribed(self, market: str) -> bool:
        with self._status_lock:
            return int(self._status[market].get("subscribed_count", 0) or 0) > 0

    def status(self) -> dict[str, Any]:
        with self._status_lock:
            data = {k: dict(v) for k, v in self._status.items()}
        for market in data:
            thread = self._threads.get(market)
            if market in {"KIS_KR", "KIS_US"}:
                thread = self._threads.get("KIS_SHARED") or thread
                data[market]["transport"] = "KIS_SHARED_SOCKET"
                data[market]["subscription_limit_per_socket"] = KIS_WS_MAX_SUBSCRIPTIONS
            data[market]["thread_alive"] = bool(thread and thread.is_alive())
            data[market]["symbols"] = len(self._symbols(market))
        data["candle_book"] = self.book.snapshot()
        data["kis_transport"] = {
            "model": "single_shared_socket",
            "max_subscriptions": KIS_WS_MAX_SUBSCRIPTIONS,
            "subscribe_interval_sec": KIS_WS_SUBSCRIBE_INTERVAL,
        }
        return data

    # -----------------------------------------------------------------
    # Binance
    # -----------------------------------------------------------------
    def _binance_loop(self) -> None:
        if websocket is None:
            self._set("BINANCE", last_error="websocket-client not installed")
            return
        last_set = None
        while not self._stop.is_set():
            symbols = self._symbols("BINANCE")
            if not symbols:
                time.sleep(2)
                continue
            params = [f"{s.lower()}@kline_1m" for s in symbols]
            try:
                ws = websocket.create_connection(BINANCE_WS, timeout=20, enable_multithread=True)
                ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
                self._set(
                    "BINANCE",
                    connected=True,
                    mode="WEBSOCKET_PRIMARY",
                    last_error=None,
                    symbols=len(symbols),
                    subscribed_count=len(symbols),
                )
                last_set = tuple(symbols)
                while not self._stop.is_set():
                    if tuple(self._symbols("BINANCE")) != last_set:
                        break
                    raw = ws.recv()
                    if not raw:
                        continue
                    obj = json.loads(raw)
                    if obj.get("e") != "kline":
                        continue
                    k = obj.get("k") or {}
                    sym = str(obj.get("s") or "").upper()
                    self.book.update_minute_snapshot(
                        sym,
                        "BINANCE",
                        int(k["t"]),
                        float(k["o"]),
                        float(k["h"]),
                        float(k["l"]),
                        float(k["c"]),
                        float(k["v"]),
                        int(obj.get("E") or k["T"]),
                    )
                    self._msg("BINANCE")
                ws.close()
            except Exception as exc:
                self._set(
                    "BINANCE",
                    connected=False,
                    mode="REST_FALLBACK",
                    last_error=f"{type(exc).__name__}: {exc}",
                )
                self._inc("BINANCE", "reconnects")
                time.sleep(2)

    # -----------------------------------------------------------------
    # Upbit
    # -----------------------------------------------------------------
    def _upbit_loop(self) -> None:
        if websocket is None:
            self._set("UPBIT", last_error="websocket-client not installed")
            return
        last_set = None
        while not self._stop.is_set():
            symbols = self._symbols("UPBIT")
            if not symbols:
                time.sleep(2)
                continue
            try:
                ws = websocket.create_connection(UPBIT_WS, timeout=20, enable_multithread=True)
                payload = [
                    {"ticket": f"tajum-{uuid.uuid4()}"},
                    {"type": "candle.1m", "codes": symbols, "is_only_realtime": True},
                    {"format": "DEFAULT"},
                ]
                ws.send(json.dumps(payload))
                self._set(
                    "UPBIT",
                    connected=True,
                    mode="WEBSOCKET_PRIMARY",
                    last_error=None,
                    symbols=len(symbols),
                    subscribed_count=len(symbols),
                )
                last_set = tuple(symbols)
                while not self._stop.is_set():
                    if tuple(self._symbols("UPBIT")) != last_set:
                        break
                    raw = ws.recv()
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    obj = json.loads(raw)
                    if not str(obj.get("type", "")).startswith("candle."):
                        continue
                    sym = str(obj["code"]).upper()
                    dt = datetime.fromisoformat(
                        str(obj["candle_date_time_utc"]).replace("Z", "+00:00")
                    )
                    open_ms = int(dt.replace(tzinfo=UTC).timestamp() * 1000)
                    self.book.update_minute_snapshot(
                        sym,
                        "UPBIT",
                        open_ms,
                        float(obj["opening_price"]),
                        float(obj["high_price"]),
                        float(obj["low_price"]),
                        float(obj["trade_price"]),
                        float(obj.get("candle_acc_trade_volume") or 0.0),
                        int(obj.get("timestamp") or open_ms + 59_999),
                    )
                    self._msg("UPBIT")
                ws.close()
            except Exception as exc:
                self._set(
                    "UPBIT",
                    connected=False,
                    mode="REST_FALLBACK",
                    last_error=f"{type(exc).__name__}: {exc}",
                )
                self._inc("UPBIT", "reconnects")
                time.sleep(2)

    # -----------------------------------------------------------------
    # KIS shared transport
    # -----------------------------------------------------------------
    def _approval(self) -> str:
        now = time.time()
        if self._kis_approval and now < self._kis_approval_exp:
            return self._kis_approval
        if not KIS_KEY or not KIS_SECRET:
            raise RuntimeError("KIS_APP_KEY/KIS_APP_SECRET missing")
        r = requests.post(
            f"{KIS_BASE}/oauth2/Approval",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": KIS_KEY,
                "secretkey": KIS_SECRET,
            },
            timeout=10,
        )
        r.raise_for_status()
        key = str(r.json().get("approval_key") or "")
        if not key:
            raise RuntimeError(f"KIS approval_key missing: {r.text[:200]}")
        self._kis_approval = key
        # Official auth helper treats WS auth as long-lived; refresh conservatively.
        self._kis_approval_exp = now + 12 * 3600
        return key

    @staticmethod
    def _kis_us_key(symbol: str) -> str:
        symbol = symbol.upper()
        raw = os.getenv("KIS_US_REALTIME_EXCHANGE_MAP", "")
        mapping: dict[str, str] = {}
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    mapping = {str(k).upper(): str(v).upper() for k, v in parsed.items()}
            except Exception:
                mapping = {}

        excd = mapping.get(symbol)
        if not excd:
            # Reuse the REST provider's exchange resolver so future NYSE/AMEX symbols
            # are not silently forced to NASDAQ.
            try:
                import kis_market_provider as kis
                excd = str(kis.resolve_us_exchange(symbol)).upper()
            except Exception:
                excd = "NAS"
        if excd not in {"NAS", "NYS", "AMS"}:
            excd = "NAS"
        # Official KIS example: DNASAAPL
        return f"D{excd}{symbol}"

    def _kis_subscription_plan(self) -> tuple[list[tuple[str, str, str, str]], dict[str, int]]:
        """Return [(market,tr_id,tr_key,symbol), ...] capped at official 40/session."""
        kr = self._symbols("KIS_KR")
        us = self._symbols("KIS_US")

        # If capacity is ever exceeded, prioritize the market that is open now.
        market_order = ["KIS_US", "KIS_KR"] if _stock_market_open("KIS_US") else ["KIS_KR", "KIS_US"]
        by_market = {"KIS_KR": kr, "KIS_US": us}

        plan: list[tuple[str, str, str, str]] = []
        overflow = {"KIS_KR": 0, "KIS_US": 0}
        for market in market_order:
            tr_id = "H0STCNT0" if market == "KIS_KR" else "HDFSCNT0"
            for sym in by_market[market]:
                if len(plan) >= KIS_WS_MAX_SUBSCRIPTIONS:
                    overflow[market] += 1
                    continue
                tr_key = sym if market == "KIS_KR" else self._kis_us_key(sym)
                plan.append((market, tr_id, tr_key, sym))
        return plan, overflow

    @staticmethod
    def _market_from_kis_ack(tr_id: str, tr_key: str) -> str | None:
        if tr_id == "H0STCNT0":
            return "KIS_KR"
        if tr_id == "HDFSCNT0":
            return "KIS_US"
        # Some ACKs may omit/alter tr_id; use key form as fallback.
        if tr_key.startswith("D") and len(tr_key) > 5:
            return "KIS_US"
        if tr_key.isdigit() and len(tr_key) == 6:
            return "KIS_KR"
        return None

    def _handle_kis_json(self, ws: Any, raw: str) -> None:
        obj = json.loads(raw)
        header = obj.get("header") or {}
        body = obj.get("body") or {}
        tr_id = str(header.get("tr_id") or "")
        tr_key = str(header.get("tr_key") or "")

        if tr_id == "PINGPONG":
            # KIS protocol heartbeat: echo the exact payload.
            ws.send(raw)
            return

        market = self._market_from_kis_ack(tr_id, tr_key)
        if not market:
            return

        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1") or "")
        normalized = msg1.upper()

        if rt_cd in {"0", ""} or "ALREADY IN SUBSCRIBE" in normalized:
            if tr_key:
                self._kis_subscribed[market].add(tr_key)
            self._set(
                market,
                connected=True,
                mode="WEBSOCKET_SUBSCRIBED",
                subscribed_count=len(self._kis_subscribed[market]),
                last_error=None,
            )
            return

        self._inc(market, "subscription_errors")
        self._set(
            market,
            connected=True,
            mode="REST_FALLBACK",
            last_error=f"KIS subscribe error tr_key={tr_key}: {msg1 or rt_cd}",
        )

    def _handle_kis_realtime(self, raw: str) -> None:
        parts = raw.split("|", 3)
        if len(parts) < 4:
            return
        _, tr_id, cnt_s, payload = parts
        try:
            cnt = max(1, int(cnt_s or 1))
        except Exception:
            cnt = 1
        vals = payload.split("^")

        if tr_id == "H0STCNT0":
            width = 46
            for i in range(cnt):
                row = vals[i * width:(i + 1) * width]
                if len(row) < 34:
                    continue
                sym, hhmmss, px = row[0], row[1], row[2]
                date_s = row[33] if len(row[33]) == 8 else datetime.now(KST).strftime("%Y%m%d")
                try:
                    dt = datetime.strptime(
                        date_s + hhmmss[:6], "%Y%m%d%H%M%S"
                    ).replace(tzinfo=KST)
                    ts = int(dt.astimezone(UTC).timestamp() * 1000)
                    qty = float(row[12] or 0)
                    self.book.update_tick(sym, "KIS_KR", ts, float(px), qty)
                    self._msg("KIS_KR")
                except Exception:
                    continue
            return

        if tr_id == "HDFSCNT0":
            # KIS has legacy/current HDFSCNT0 samples in circulation:
            # - legacy raw layout: 26 fields, [realtime_code, symbol, zdiv, business_date,
            #   local_date, local_time, ..., last@11, evol@19, ...]
            # - current helper column layout: 25 fields, [SYMB, ZDIV, TYMD, XYMD, XHMS,
            #   ..., LAST@10, ..., EVOL@18, ...]
            # Accept both so a provider-side schema presentation change cannot kill FCM.
            if cnt <= 0:
                cnt = 1
            per_record = len(vals) // cnt if cnt else len(vals)
            width = 26 if per_record >= 26 else 25
            for i in range(cnt):
                row = vals[i * width:(i + 1) * width]
                if len(row) < 25:
                    continue

                if width >= 26:
                    realtime_sym = str(row[0]).upper()
                    plain_sym = str(row[1]).upper()
                    sym = plain_sym or realtime_sym
                    date_s = str(row[4])   # local date
                    hhmmss = str(row[5])   # local time
                    px = row[11]           # current price
                    qty = row[19]          # execution volume
                else:
                    realtime_sym = str(row[0]).upper()
                    sym = realtime_sym
                    date_s = str(row[3])   # XYMD
                    hhmmss = str(row[4])   # XHMS
                    px = row[10]           # LAST
                    qty = row[18]          # EVOL

                for prefix in ("DNAS", "DNYS", "DAMS"):
                    if sym.startswith(prefix):
                        sym = sym[len(prefix):]
                        break
                # Some legacy rows put the prefixed code only in field 0.
                if not sym or sym in {"NAS", "NYS", "AMS"}:
                    sym = realtime_sym
                    for prefix in ("DNAS", "DNYS", "DAMS"):
                        if sym.startswith(prefix):
                            sym = sym[len(prefix):]
                            break

                try:
                    dt = datetime.strptime(
                        date_s + hhmmss[:6], "%Y%m%d%H%M%S"
                    ).replace(tzinfo=NY)
                    ts = int(dt.astimezone(UTC).timestamp() * 1000)
                    self.book.update_tick(sym, "KIS_US", ts, float(px), float(qty or 0))
                    self._msg("KIS_US")
                except Exception:
                    continue

    def _kis_loop_combined(self) -> None:
        if websocket is None:
            self._set("KIS_KR", last_error="websocket-client not installed")
            self._set("KIS_US", last_error="websocket-client not installed")
            return

        reconnect_attempt = 0
        last_signature: tuple[tuple[str, str, str, str], ...] | None = None

        while not self._stop.is_set():
            plan, overflow = self._kis_subscription_plan()
            if not plan:
                time.sleep(2)
                continue

            signature = tuple(plan)
            try:
                approval = self._approval()
                ws = websocket.create_connection(
                    KIS_WS,
                    timeout=KIS_WS_RECV_TIMEOUT,
                    enable_multithread=True,
                )
                try:
                    ws.settimeout(KIS_WS_RECV_TIMEOUT)
                except Exception:
                    pass

                self._kis_subscribed = {"KIS_KR": set(), "KIS_US": set()}
                for market in ("KIS_KR", "KIS_US"):
                    count = sum(1 for item in plan if item[0] == market)
                    self._set(
                        market,
                        connected=count > 0,
                        mode="WEBSOCKET_SUBSCRIBING" if count > 0 else "REST_FALLBACK",
                        last_error=None,
                        overflow_count=int(overflow.get(market, 0)),
                        subscribed_count=0,
                    )

                # Official KIS helper paces subscriptions at >=0.1 sec.
                for market, tr_id, tr_key, _sym in plan:
                    msg = {
                        "header": {
                            "approval_key": approval,
                            "custtype": "P",
                            "tr_type": "1",
                            "content-type": "utf-8",
                        },
                        "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
                    }
                    ws.send(json.dumps(msg))
                    time.sleep(KIS_WS_SUBSCRIBE_INTERVAL)

                # Socket accepted the subscription batch; ACKs/realtime follow below.
                for market in ("KIS_KR", "KIS_US"):
                    count = sum(1 for item in plan if item[0] == market)
                    if count:
                        self._set(market, connected=True, mode="WEBSOCKET_SUBSCRIBED")
                last_signature = signature
                reconnect_attempt = 0

                while not self._stop.is_set():
                    new_plan, _ = self._kis_subscription_plan()
                    if tuple(new_plan) != last_signature:
                        break
                    try:
                        raw = ws.recv()
                    except Exception as exc:
                        timeout_cls = getattr(websocket, "WebSocketTimeoutException", ())
                        if timeout_cls and isinstance(exc, timeout_cls):
                            # Closed markets can legitimately be quiet. Timeout alone is
                            # not a reason to churn/reconnect the KIS socket.
                            continue
                        raise

                    if not raw:
                        continue
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    if raw.startswith("{"):
                        self._handle_kis_json(ws, raw)
                    elif raw[0] in {"0", "1"}:
                        self._handle_kis_realtime(raw)

                try:
                    ws.close()
                except Exception:
                    pass

            except Exception as exc:
                reconnect_attempt += 1
                delay = min(30.0, 1.5 * (2 ** min(reconnect_attempt - 1, 4)))
                for market in ("KIS_KR", "KIS_US"):
                    self._set(
                        market,
                        connected=False,
                        mode="REST_FALLBACK",
                        last_error=f"{type(exc).__name__}: {exc}",
                    )
                    self._inc(market, "reconnects")
                time.sleep(delay)
