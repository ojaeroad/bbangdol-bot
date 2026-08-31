"""Tajum On V146 real-time market stream hub.

Primary path:
- Binance: public 1m kline WebSocket
- Upbit: public 1m candle WebSocket
- KIS Korea: H0STCNT0 real-time trades
- KIS US: HDFSCNT0 real-time US trades

REST remains the warm-up / gap-fill / fallback source in auto_exchange_engine.py.

`websocket-client` is optional at import time. If unavailable or a stream disconnects,
the worker remains operational through REST and status clearly reports the fallback.
"""
from __future__ import annotations

import json, os, threading, time, uuid
from datetime import datetime, timezone
from typing import Any, Callable
import requests

from candle_builder import CandleBook

try:
    import websocket
except Exception:
    websocket = None

UTC = timezone.utc

BINANCE_WS = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
UPBIT_WS = os.getenv("UPBIT_WS_URL", "wss://api.upbit.com/websocket/v1")
KIS_WS = os.getenv("KIS_WS_URL", "ws://ops.koreainvestment.com:21000")
KIS_BASE = os.getenv("KIS_API_BASE", "https://openapi.koreainvestment.com:9443").rstrip("/")
KIS_KEY = os.getenv("KIS_APP_KEY", "").strip()
KIS_SECRET = os.getenv("KIS_APP_SECRET", "").strip()

def classify(symbol: str) -> str:
    s = symbol.upper()
    if s.startswith("KRW-"): return "UPBIT"
    if s.endswith("USDT"): return "BINANCE"
    if s.isdigit() and len(s) == 6: return "KIS_KR"
    return "KIS_US"

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
                "connected": False, "symbols": 0, "messages": 0, "reconnects": 0,
                "last_message_at": None, "last_error": None, "mode": "REST_FALLBACK"
            }
            for m in ("BINANCE","UPBIT","KIS_KR","KIS_US")
        }
        self._kis_approval = None
        self._kis_approval_exp = 0.0

    def _symbols(self, market: str) -> list[str]:
        return sorted({s.upper() for s in (self.subscription_provider() or []) if classify(s) == market})

    def _set(self, market: str, **kw):
        with self._status_lock:
            self._status[market].update(kw)

    def _msg(self, market: str):
        with self._status_lock:
            st = self._status[market]
            st["messages"] += 1
            st["last_message_at"] = datetime.now(UTC).isoformat()
            st["connected"] = True
            st["mode"] = "WEBSOCKET_PRIMARY"

    def start(self):
        for market, target in [
            ("BINANCE", self._binance_loop),
            ("UPBIT", self._upbit_loop),
            ("KIS_KR", lambda: self._kis_loop("KIS_KR")),
            ("KIS_US", lambda: self._kis_loop("KIS_US")),
        ]:
            if market in self._threads and self._threads[market].is_alive():
                continue
            t = threading.Thread(target=target, name=f"tajum-ws-{market.lower()}", daemon=True)
            self._threads[market] = t
            t.start()

    def seed(self, symbol: str, market: str, timeframe: str, rows: list[dict[str, Any]]):
        self.book.seed(symbol, market, timeframe, rows)

    def rows(self, symbol: str, timeframe: str, limit: int = 320):
        return self.book.rows(symbol, timeframe, limit)

    def last_price(self, symbol: str, max_age_sec: int = 180):
        return self.book.last_price(symbol, max_age_sec)

    def healthy(self, market: str, stale_sec: int = 180) -> bool:
        with self._status_lock:
            st = dict(self._status[market])
        if not st.get("connected") or not st.get("last_message_at"):
            return False
        try:
            dt = datetime.fromisoformat(st["last_message_at"])
            return (datetime.now(UTC) - dt).total_seconds() <= stale_sec
        except Exception:
            return False

    def status(self):
        with self._status_lock:
            data = {k: dict(v) for k,v in self._status.items()}
        for market in data:
            data[market]["thread_alive"] = bool(self._threads.get(market) and self._threads[market].is_alive())
            data[market]["symbols"] = len(self._symbols(market))
        data["candle_book"] = self.book.snapshot()
        return data

    def _binance_loop(self):
        if websocket is None:
            self._set("BINANCE", last_error="websocket-client not installed")
            return
        last_set = None
        while not self._stop.is_set():
            symbols = self._symbols("BINANCE")
            if not symbols:
                time.sleep(2); continue
            params = [f"{s.lower()}@kline_1m" for s in symbols]
            try:
                ws = websocket.create_connection(BINANCE_WS, timeout=20, enable_multithread=True)
                ws.send(json.dumps({"method":"SUBSCRIBE","params":params,"id":1}))
                self._set("BINANCE", connected=True, mode="WEBSOCKET_PRIMARY", last_error=None, symbols=len(symbols))
                last_set = tuple(symbols)
                while not self._stop.is_set():
                    if tuple(self._symbols("BINANCE")) != last_set:
                        break
                    raw = ws.recv()
                    if not raw: continue
                    obj = json.loads(raw)
                    if obj.get("e") != "kline": continue
                    k = obj.get("k") or {}
                    sym = str(obj.get("s") or "").upper()
                    self.book.update_minute_snapshot(
                        sym, "BINANCE", int(k["t"]), float(k["o"]), float(k["h"]),
                        float(k["l"]), float(k["c"]), float(k["v"]), int(obj.get("E") or k["T"])
                    )
                    self._msg("BINANCE")
                ws.close()
            except Exception as exc:
                self._set("BINANCE", connected=False, mode="REST_FALLBACK",
                          last_error=f"{type(exc).__name__}: {exc}",
                          reconnects=self._status["BINANCE"]["reconnects"] + 1)
                time.sleep(2)

    def _upbit_loop(self):
        if websocket is None:
            self._set("UPBIT", last_error="websocket-client not installed")
            return
        last_set = None
        while not self._stop.is_set():
            symbols = self._symbols("UPBIT")
            if not symbols:
                time.sleep(2); continue
            try:
                ws = websocket.create_connection(UPBIT_WS, timeout=20, enable_multithread=True)
                payload = [
                    {"ticket": f"tajum-{uuid.uuid4()}"},
                    {"type":"candle.1m","codes":symbols,"is_only_realtime":True},
                    {"format":"DEFAULT"},
                ]
                ws.send(json.dumps(payload))
                self._set("UPBIT", connected=True, mode="WEBSOCKET_PRIMARY", last_error=None, symbols=len(symbols))
                last_set = tuple(symbols)
                while not self._stop.is_set():
                    if tuple(self._symbols("UPBIT")) != last_set:
                        break
                    raw = ws.recv()
                    if isinstance(raw, bytes): raw = raw.decode("utf-8")
                    obj = json.loads(raw)
                    if not str(obj.get("type","")).startswith("candle."): continue
                    sym = str(obj["code"]).upper()
                    dt = datetime.fromisoformat(str(obj["candle_date_time_utc"]).replace("Z","+00:00"))
                    open_ms = int(dt.replace(tzinfo=UTC).timestamp()*1000)
                    self.book.update_minute_snapshot(
                        sym, "UPBIT", open_ms,
                        float(obj["opening_price"]), float(obj["high_price"]),
                        float(obj["low_price"]), float(obj["trade_price"]),
                        float(obj.get("candle_acc_trade_volume") or 0.0),
                        int(obj.get("timestamp") or open_ms+59_999)
                    )
                    self._msg("UPBIT")
                ws.close()
            except Exception as exc:
                self._set("UPBIT", connected=False, mode="REST_FALLBACK",
                          last_error=f"{type(exc).__name__}: {exc}",
                          reconnects=self._status["UPBIT"]["reconnects"] + 1)
                time.sleep(2)

    def _approval(self) -> str:
        now = time.time()
        if self._kis_approval and now < self._kis_approval_exp:
            return self._kis_approval
        if not KIS_KEY or not KIS_SECRET:
            raise RuntimeError("KIS_APP_KEY/KIS_APP_SECRET missing")
        r = requests.post(
            f"{KIS_BASE}/oauth2/Approval",
            headers={"content-type":"application/json"},
            json={"grant_type":"client_credentials","appkey":KIS_KEY,"secretkey":KIS_SECRET},
            timeout=10,
        )
        r.raise_for_status()
        key = str(r.json().get("approval_key") or "")
        if not key:
            raise RuntimeError(f"KIS approval_key missing: {r.text[:200]}")
        self._kis_approval = key
        self._kis_approval_exp = now + 12*3600
        return key

    @staticmethod
    def _kis_us_key(symbol: str) -> str:
        # Current app US list is NASDAQ-heavy. Allow explicit override for NYS/AMS.
        raw = os.getenv("KIS_US_REALTIME_EXCHANGE_MAP", "")
        mapping = {}
        if raw:
            try: mapping = json.loads(raw)
            except Exception: mapping = {}
        excd = str(mapping.get(symbol.upper(), "NAS")).upper()
        return f"D{excd}{symbol.upper()}"

    def _kis_loop(self, market: str):
        if websocket is None:
            self._set(market, last_error="websocket-client not installed")
            return
        last_set = None
        tr_id = "H0STCNT0" if market == "KIS_KR" else "HDFSCNT0"
        while not self._stop.is_set():
            symbols = self._symbols(market)
            if not symbols:
                time.sleep(2); continue
            try:
                approval = self._approval()
                ws = websocket.create_connection(KIS_WS, timeout=20, enable_multithread=True)
                for sym in symbols:
                    tr_key = sym if market == "KIS_KR" else self._kis_us_key(sym)
                    msg = {
                        "header":{"approval_key":approval,"custtype":"P","tr_type":"1","content-type":"utf-8"},
                        "body":{"input":{"tr_id":tr_id,"tr_key":tr_key}},
                    }
                    ws.send(json.dumps(msg))
                    time.sleep(0.08)
                self._set(market, connected=True, mode="WEBSOCKET_PRIMARY", last_error=None, symbols=len(symbols))
                last_set = tuple(symbols)
                while not self._stop.is_set():
                    if tuple(self._symbols(market)) != last_set:
                        break
                    raw = ws.recv()
                    if not raw: continue
                    if isinstance(raw, bytes): raw = raw.decode("utf-8")
                    if raw.startswith("{"):
                        # Subscribe ack / ping.
                        try:
                            obj = json.loads(raw)
                            if str((obj.get("header") or {}).get("tr_id")) == "PINGPONG":
                                ws.send(raw)
                        except Exception:
                            pass
                        continue
                    parts = raw.split("|", 3)
                    if len(parts) < 4: continue
                    _, received_tr_id, cnt_s, payload = parts
                    if received_tr_id != tr_id: continue
                    cnt = max(1, int(cnt_s or 1))
                    vals = payload.split("^")
                    if market == "KIS_KR":
                        width = 46
                        for i in range(cnt):
                            row = vals[i*width:(i+1)*width]
                            if len(row) < 13: continue
                            sym, hhmmss, px = row[0], row[1], row[2]
                            date_s = row[33] if len(row) > 33 and len(row[33]) == 8 else datetime.now().strftime("%Y%m%d")
                            try:
                                dt = datetime.strptime(date_s+hhmmss[:6], "%Y%m%d%H%M%S").replace(tzinfo=__import__("zoneinfo").ZoneInfo("Asia/Seoul"))
                                ts = int(dt.astimezone(UTC).timestamp()*1000)
                                qty = float(row[12] or 0)
                                self.book.update_tick(sym, market, ts, float(px), qty)
                                self._msg(market)
                            except Exception:
                                continue
                    else:
                        width = 26
                        for i in range(cnt):
                            row = vals[i*width:(i+1)*width]
                            if len(row) < 20: continue
                            # HDFSCNT0: [realtime_sym,symbol,zdiv,business_date,local_date,local_time,
                            # korean_date,korean_time,open,high,low,last,...,trade_qty,...]
                            sym = row[1].upper()
                            date_s, hhmmss, px = row[4], row[5], row[11]
                            try:
                                dt = datetime.strptime(date_s+hhmmss[:6], "%Y%m%d%H%M%S").replace(tzinfo=__import__("zoneinfo").ZoneInfo("America/New_York"))
                                ts = int(dt.astimezone(UTC).timestamp()*1000)
                                qty = float(row[19] or 0)
                                self.book.update_tick(sym, market, ts, float(px), qty)
                                self._msg(market)
                            except Exception:
                                continue
                ws.close()
            except Exception as exc:
                self._set(market, connected=False, mode="REST_FALLBACK",
                          last_error=f"{type(exc).__name__}: {exc}",
                          reconnects=self._status[market]["reconnects"] + 1)
                time.sleep(2)
