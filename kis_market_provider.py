"""Tajum On V142 KIS market-data provider.

Purpose
-------
- KRX/US member watchlist symbols can be evaluated without TradingView registration.
- KIS REST APIs are used for stock OHLC warm-up and current prices.
- No trading/order API is used.
- Missing KIS credentials disable only stock direct-calculation; coin engines remain live.

Environment
-----------
KIS_APP_KEY, KIS_APP_SECRET                         required for stock direct calculation
KIS_ENV=real|demo                                   default real
KIS_BASE_URL                                        optional override
KIS_HTTP_TIMEOUT_SEC                                default 8
KIS_HTTP_MIN_INTERVAL_SEC                           default 0.07 (global rate guard)
KIS_CACHE_DIR                                       default /tmp/tajum_kis_cache
KIS_CACHE_TTL_SEC                                   default 21600 (6h)
KIS_DOMESTIC_HISTORY_DAYS                           default 24
KIS_DOMESTIC_PAGES_PER_DAY                          default 8
KIS_OVERSEAS_PAGES                                  default 4

Notes
-----
Domestic KIS minute history is page-oriented 1-minute data. V142 intentionally
caches fetched history on disk so expensive warm-up is not repeated every cycle.
Attach a Render persistent disk and set KIS_CACHE_DIR to that mount for production.
"""
from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import requests

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "").strip()
KIS_ENV = os.getenv("KIS_ENV", "real").strip().lower() or "real"
if KIS_ENV not in {"real", "demo"}:
    KIS_ENV = "real"
KIS_BASE_URL = os.getenv(
    "KIS_BASE_URL",
    "https://openapi.koreainvestment.com:9443" if KIS_ENV == "real" else "https://openapivts.koreainvestment.com:29443",
).rstrip("/")
HTTP_TIMEOUT = max(3, min(int(os.getenv("KIS_HTTP_TIMEOUT_SEC", "8") or 8), 30))
MIN_INTERVAL = max(0.03, min(float(os.getenv("KIS_HTTP_MIN_INTERVAL_SEC", "0.07") or 0.07), 1.0))
CACHE_DIR = Path(os.getenv("KIS_CACHE_DIR", "/tmp/tajum_kis_cache") or "/tmp/tajum_kis_cache")
CACHE_TTL = max(300, min(int(os.getenv("KIS_CACHE_TTL_SEC", "21600") or 21600), 86400 * 7))
DOMESTIC_HISTORY_DAYS = max(5, min(int(os.getenv("KIS_DOMESTIC_HISTORY_DAYS", "24") or 24), 40))
DOMESTIC_PAGES_PER_DAY = max(1, min(int(os.getenv("KIS_DOMESTIC_PAGES_PER_DAY", "8") or 8), 20))
OVERSEAS_PAGES = max(1, min(int(os.getenv("KIS_OVERSEAS_PAGES", "4") or 4), 10))

_token_lock = threading.Lock()
_token_value = ""
_token_expiry = 0.0
_http_lock = threading.Lock()
_last_http_at = 0.0
_us_exchange_cache: dict[str, str] = {}
_status_lock = threading.Lock()
_status: dict[str, Any] = {
    "configured": bool(KIS_APP_KEY and KIS_APP_SECRET),
    "env": KIS_ENV,
    "base_url": KIS_BASE_URL,
    "cache_dir": str(CACHE_DIR),
    "last_request_at": None,
    "last_success_at": None,
    "last_error": None,
    "last_symbol": None,
    "request_count": 0,
    "success_count": 0,
    "error_count": 0,
}


def configured() -> bool:
    return bool(KIS_APP_KEY and KIS_APP_SECRET)


def status() -> dict[str, Any]:
    with _status_lock:
        return dict(_status)


def _status_update(**kwargs: Any) -> None:
    with _status_lock:
        _status.update(kwargs)


def _rate_guard() -> None:
    global _last_http_at
    with _http_lock:
        now = time.monotonic()
        wait = MIN_INTERVAL - (now - _last_http_at)
        if wait > 0:
            time.sleep(wait)
        _last_http_at = time.monotonic()


def _access_token() -> str:
    global _token_value, _token_expiry
    if not configured():
        raise RuntimeError("KIS credentials missing: set KIS_APP_KEY and KIS_APP_SECRET")
    now = time.time()
    with _token_lock:
        if _token_value and now < _token_expiry - 120:
            return _token_value
        _rate_guard()
        resp = requests.post(
            f"{KIS_BASE_URL}/oauth2/tokenP",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
            },
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()
        token = str(body.get("access_token") or "").strip()
        if not token:
            raise RuntimeError(f"KIS token missing: {body}")
        expires = int(body.get("expires_in") or 86400)
        _token_value = token
        _token_expiry = now + max(300, expires)
        return token


def _get(path: str, tr_id: str, params: dict[str, Any], *, tr_cont: str = "") -> tuple[dict[str, Any], dict[str, str]]:
    if not configured():
        raise RuntimeError("KIS_NOT_CONFIGURED")
    _rate_guard()
    with _status_lock:
        _status["request_count"] = int(_status.get("request_count", 0) or 0) + 1
        _status["last_request_at"] = datetime.now(timezone.utc).isoformat()
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {_access_token()}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
        "custtype": "P",
    }
    if tr_cont:
        headers["tr_cont"] = tr_cont
    try:
        resp = requests.get(f"{KIS_BASE_URL}{path}", headers=headers, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        if str(body.get("rt_cd", "0")) not in {"0", ""}:
            raise RuntimeError(f"KIS {body.get('msg_cd')}: {body.get('msg1')}")
        with _status_lock:
            _status["success_count"] = int(_status.get("success_count", 0) or 0) + 1
            _status["last_success_at"] = datetime.now(timezone.utc).isoformat()
            _status["last_error"] = None
        return body, {str(k).lower(): str(v) for k, v in resp.headers.items()}
    except Exception as exc:
        with _status_lock:
            _status["error_count"] = int(_status.get("error_count", 0) or 0) + 1
            _status["last_error"] = f"{type(exc).__name__}: {exc}"
        raise


def _num(value: Any) -> float:
    out = float(str(value or "0").replace(",", ""))
    if not math.isfinite(out):
        raise ValueError("non-finite KIS number")
    return out


def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _cache_path(kind: str, symbol: str) -> Path:
    safe = re.sub(r"[^A-Z0-9_-]", "_", symbol.upper())
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{kind}_{safe}.json"


def _cache_load(kind: str, symbol: str) -> list[dict[str, Any]] | None:
    path = _cache_path(kind, symbol)
    try:
        if not path.exists() or time.time() - path.stat().st_mtime > CACHE_TTL:
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else None
    except Exception:
        return None


def _cache_save(kind: str, symbol: str, rows: list[dict[str, Any]]) -> None:
    try:
        path = _cache_path(kind, symbol)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass


def aggregate(rows: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    bucket_ms = int(minutes) * 60_000
    out: list[dict[str, Any]] = []
    cur = None
    cur_bucket = None
    for row in sorted(rows, key=lambda x: int(x["open_time"])):
        bucket = int(row["open_time"]) // bucket_ms * bucket_ms
        if cur is None or bucket != cur_bucket:
            if cur is not None:
                out.append(cur)
            cur_bucket = bucket
            cur = {
                "open_time": bucket,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
                "close_time": int(row.get("close_time", row["open_time"])),
            }
        else:
            cur["high"] = max(float(cur["high"]), float(row["high"]))
            cur["low"] = min(float(cur["low"]), float(row["low"]))
            cur["close"] = float(row["close"])
            cur["volume"] = float(cur["volume"]) + float(row.get("volume", 0.0))
            cur["close_time"] = int(row.get("close_time", row["open_time"]))
    if cur is not None:
        out.append(cur)
    return out


def _day_aggregate(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 1:
        return rows
    out: list[dict[str, Any]] = []
    buf: list[dict[str, Any]] = []
    for row in rows:
        buf.append(row)
        if len(buf) == days:
            out.append({
                "open_time": int(buf[0]["open_time"]),
                "open": float(buf[0]["open"]),
                "high": max(float(x["high"]) for x in buf),
                "low": min(float(x["low"]) for x in buf),
                "close": float(buf[-1]["close"]),
                "volume": sum(float(x.get("volume", 0.0)) for x in buf),
                "close_time": int(buf[-1].get("close_time", buf[-1]["open_time"])),
            })
            buf = []
    return out


def domestic_daily(symbol: str, count_hint: int = 180) -> list[dict[str, Any]]:
    cached = _cache_load("kr_daily", symbol)
    if cached:
        return cached
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=max(240, count_hint * 2))).strftime("%Y%m%d")
    body, _ = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        "FHKST03010100",
        {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
    )
    raw = body.get("output2") or body.get("output") or []
    rows = []
    for item in raw if isinstance(raw, list) else []:
        ds = str(item.get("stck_bsop_date") or "")
        if len(ds) != 8:
            continue
        dt = datetime.strptime(ds, "%Y%m%d").replace(tzinfo=timezone.utc)
        try:
            rows.append({
                "open_time": _ms(dt),
                "open": _num(item.get("stck_oprc")),
                "high": _num(item.get("stck_hgpr")),
                "low": _num(item.get("stck_lwpr")),
                "close": _num(item.get("stck_clpr")),
                "volume": _num(item.get("acml_vol")),
                "close_time": _ms(dt),
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["open_time"])
    _cache_save("kr_daily", symbol, rows)
    return rows


def _parse_domestic_minute_item(item: dict[str, Any], fallback_date: str) -> dict[str, Any] | None:
    ds = str(item.get("stck_bsop_date") or fallback_date or "")
    hs = str(item.get("stck_cntg_hour") or item.get("cntg_hour") or "")[:6].zfill(6)
    if len(ds) != 8 or len(hs) != 6:
        return None
    try:
        # KST -> UTC timestamp
        local = datetime.strptime(ds + hs, "%Y%m%d%H%M%S").replace(tzinfo=timezone(timedelta(hours=9)))
        dt = local.astimezone(timezone.utc)
        close = _num(item.get("stck_prpr") or item.get("stck_clpr"))
        return {
            "open_time": _ms(dt),
            "open": _num(item.get("stck_oprc") or close),
            "high": _num(item.get("stck_hgpr") or close),
            "low": _num(item.get("stck_lwpr") or close),
            "close": close,
            "volume": _num(item.get("cntg_vol") or item.get("acml_vol") or 0),
            "close_time": _ms(dt),
        }
    except Exception:
        return None


def domestic_minutes(symbol: str) -> list[dict[str, Any]]:
    """Warm-up KRX 1m rows across recent business days, cache on disk.

    KIS domestic minute history is page based. The cache is intentionally reused
    for all 30m/1h/4h calculations and refreshed only every CACHE_TTL seconds.
    """
    cached = _cache_load("kr_minute", symbol)
    if cached:
        return cached
    all_rows: dict[int, dict[str, Any]] = {}
    today = datetime.now(timezone(timedelta(hours=9))).date()
    days_seen = 0
    cursor_day = today
    while days_seen < DOMESTIC_HISTORY_DAYS:
        if cursor_day.weekday() >= 5:
            cursor_day -= timedelta(days=1)
            continue
        ds = cursor_day.strftime("%Y%m%d")
        cursor_time = "200000"
        day_had_data = False
        last_oldest = ""
        for _ in range(DOMESTIC_PAGES_PER_DAY):
            body, _ = _get(
                "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
                "FHKST03010230",
                {
                    "FID_COND_MRKT_DIV_CODE": "J",
                    "FID_INPUT_ISCD": symbol,
                    "FID_INPUT_HOUR_1": cursor_time,
                    "FID_INPUT_DATE_1": ds,
                    "FID_PW_DATA_INCU_YN": "Y",
                    "FID_FAKE_TICK_INCU_YN": "",
                },
            )
            raw = body.get("output2") or []
            if not isinstance(raw, list) or not raw:
                break
            page_rows = []
            for item in raw:
                row = _parse_domestic_minute_item(item, ds)
                if row:
                    all_rows[row["open_time"]] = row
                    page_rows.append(row)
            if not page_rows:
                break
            day_had_data = True
            oldest = min(page_rows, key=lambda x: x["open_time"])
            local_oldest = datetime.fromtimestamp(oldest["open_time"] / 1000, timezone.utc).astimezone(timezone(timedelta(hours=9)))
            oldest_key = local_oldest.strftime("%H%M%S")
            if oldest_key == last_oldest or oldest_key <= "085900":
                break
            last_oldest = oldest_key
            cursor_time = (local_oldest - timedelta(minutes=1)).strftime("%H%M%S")
        if day_had_data:
            days_seen += 1
        cursor_day -= timedelta(days=1)
    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save("kr_minute", symbol, rows)
    return rows


def resolve_us_exchange(symbol: str) -> str:
    symbol = symbol.upper().strip()
    cached = _us_exchange_cache.get(symbol)
    if cached:
        return cached
    # Product basic info API resolves market without requiring a pre-built US master.
    for product_type, excd in (("512", "NAS"), ("513", "NYS"), ("529", "AMS")):
        try:
            body, _ = _get(
                "/uapi/overseas-price/v1/quotations/search-info",
                "CTPF1702R",
                {"PRDT_TYPE_CD": product_type, "PDNO": symbol},
            )
            output = body.get("output") or {}
            if isinstance(output, list):
                output = output[0] if output else {}
            # KIS may return rt_cd=0 with an empty record, so require a recognizable code/name.
            text = " ".join(str(v or "") for v in output.values()).upper() if isinstance(output, dict) else ""
            if symbol in text or any(str(output.get(k) or "").strip() for k in ("prdt_name", "prdt_name120", "ovrs_item_name", "hts_eng_isnm")):
                _us_exchange_cache[symbol] = excd
                return excd
        except Exception:
            continue
    # Common default for the user's current tech-heavy watchlist; price lookup will fail visibly if wrong.
    _us_exchange_cache[symbol] = "NAS"
    return "NAS"


def overseas_current_price(symbol: str, exchange: str | None = None) -> float:
    excd = exchange or resolve_us_exchange(symbol)
    body, _ = _get(
        "/uapi/overseas-price/v1/quotations/price",
        "HHDFS00000300",
        {"AUTH": "", "EXCD": excd, "SYMB": symbol.upper()},
    )
    output = body.get("output") or {}
    if isinstance(output, list):
        output = output[0] if output else {}
    for key in ("last", "clos", "stck_prpr"):
        if isinstance(output, dict) and str(output.get(key) or "").strip():
            return _num(output[key])
    raise RuntimeError(f"KIS overseas current price missing {symbol}/{excd}")


def domestic_current_price(symbol: str) -> float:
    body, _ = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "FHKST01010100",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
    )
    output = body.get("output") or {}
    if isinstance(output, list):
        output = output[0] if output else {}
    return _num(output.get("stck_prpr"))


def overseas_minutes(symbol: str, minutes: int, exchange: str | None = None) -> list[dict[str, Any]]:
    excd = exchange or resolve_us_exchange(symbol)
    kind = f"us_{excd}_{minutes}m"
    cached = _cache_load(kind, symbol)
    if cached:
        return cached
    all_rows: dict[int, dict[str, Any]] = {}
    keyb = ""
    next_flag = ""
    for page in range(OVERSEAS_PAGES):
        body, headers = _get(
            "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice",
            "HHDFS76950200",
            {
                "AUTH": "",
                "EXCD": excd,
                "SYMB": symbol.upper(),
                "NMIN": str(minutes),
                "PINC": "0" if page == 0 else "1",
                "NEXT": next_flag,
                "NREC": "120",
                "FILL": "",
                "KEYB": keyb,
            },
            tr_cont="" if page == 0 else "N",
        )
        raw = body.get("output2") or []
        if not isinstance(raw, list) or not raw:
            break
        parsed = []
        for item in raw:
            ds = str(item.get("tymd") or item.get("xymd") or item.get("date") or "")
            hs = str(item.get("xhms") or item.get("hour") or item.get("time") or "").replace(":", "")[:6].zfill(6)
            if len(ds) != 8:
                continue
            try:
                dt = datetime.strptime(ds + hs, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                close = _num(item.get("last") or item.get("clos"))
                row = {
                    "open_time": _ms(dt),
                    "open": _num(item.get("open") or close),
                    "high": _num(item.get("high") or close),
                    "low": _num(item.get("low") or close),
                    "close": close,
                    "volume": _num(item.get("evol") or item.get("tvol") or 0),
                    "close_time": _ms(dt),
                }
                all_rows[row["open_time"]] = row
                parsed.append(row)
            except Exception:
                continue
        if not parsed:
            break
        oldest = min(parsed, key=lambda x: x["open_time"])
        keyb = (datetime.fromtimestamp(oldest["open_time"] / 1000, timezone.utc) - timedelta(minutes=minutes)).strftime("%Y%m%d%H%M%S")
        next_flag = "1"
        if str(headers.get("tr_cont", "")).upper() not in {"M", "F"} and page > 0:
            break
    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save(kind, symbol, rows)
    return rows


def overseas_daily(symbol: str, exchange: str | None = None) -> list[dict[str, Any]]:
    excd = exchange or resolve_us_exchange(symbol)
    kind = f"us_{excd}_daily"
    cached = _cache_load(kind, symbol)
    if cached:
        return cached
    all_rows: dict[int, dict[str, Any]] = {}
    bymd = datetime.now().strftime("%Y%m%d")
    tr_cont = ""
    for page in range(4):
        body, headers = _get(
            "/uapi/overseas-price/v1/quotations/dailyprice",
            "HHDFS76240000",
            {"AUTH": "", "EXCD": excd, "SYMB": symbol.upper(), "GUBN": "0", "BYMD": bymd, "MODP": "1"},
            tr_cont=tr_cont,
        )
        raw = body.get("output2") or []
        if not isinstance(raw, list) or not raw:
            break
        last_date = ""
        for item in raw:
            ds = str(item.get("xymd") or "")
            if len(ds) != 8:
                continue
            try:
                dt = datetime.strptime(ds, "%Y%m%d").replace(tzinfo=timezone.utc)
                row = {
                    "open_time": _ms(dt),
                    "open": _num(item.get("open")),
                    "high": _num(item.get("high")),
                    "low": _num(item.get("low")),
                    "close": _num(item.get("clos")),
                    "volume": _num(item.get("tvol") or 0),
                    "close_time": _ms(dt),
                }
                all_rows[row["open_time"]] = row
                last_date = ds
            except Exception:
                continue
        if not last_date:
            break
        bymd = last_date
        if str(headers.get("tr_cont", "")).upper() not in {"M", "F"}:
            break
        tr_cont = "N"
    rows = [all_rows[k] for k in sorted(all_rows)]
    _cache_save(kind, symbol, rows)
    return rows


def rows(symbol: str, timeframe: str) -> tuple[str, list[dict[str, Any]]]:
    """Return (market, OHLC rows) for KRX six-digit or US ticker symbol."""
    symbol = symbol.upper().strip()
    _status_update(last_symbol=symbol)
    domestic = bool(re.fullmatch(r"\d{6}", symbol))
    market = "KOREA" if domestic else "US"
    if timeframe in {"30m", "1h", "4h"}:
        minutes = {"30m": 30, "1h": 60, "4h": 240}[timeframe]
        if domestic:
            return market, aggregate(domestic_minutes(symbol), minutes)
        return market, overseas_minutes(symbol, minutes)
    if timeframe in {"1d", "3d", "1w"}:
        daily = domestic_daily(symbol) if domestic else overseas_daily(symbol)
        if timeframe == "1d":
            return market, daily
        if timeframe == "3d":
            return market, _day_aggregate(daily, 3)
        return market, _day_aggregate(daily, 5)  # trading-week approximation from daily bars
    raise ValueError(f"unsupported KIS stock timeframe: {timeframe}")


def current_price(symbol: str) -> tuple[str, float]:
    symbol = symbol.upper().strip()
    if re.fullmatch(r"\d{6}", symbol):
        return "KOREA", domestic_current_price(symbol)
    return "US", overseas_current_price(symbol)


# Official KIS master-file source used by its own open-trading-api examples.
_KRX_MASTER_URLS = {
    "KOSPI": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
    "KOSDAQ": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
}
_master_cache: tuple[float, list[dict[str, str]]] = (0.0, [])
_master_lock = threading.Lock()


def domestic_master() -> list[dict[str, str]]:
    """All KOSPI/KOSDAQ symbols for app search; public KIS master downloads, no API key."""
    global _master_cache
    now = time.time()
    if _master_cache[1] and now - _master_cache[0] < 21600:
        return list(_master_cache[1])
    with _master_lock:
        if _master_cache[1] and now - _master_cache[0] < 21600:
            return list(_master_cache[1])
        out: dict[str, dict[str, str]] = {}
        for exchange, url in _KRX_MASTER_URLS.items():
            try:
                resp = requests.get(url, timeout=HTTP_TIMEOUT)
                resp.raise_for_status()
                with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                    names = zf.namelist()
                    if not names:
                        continue
                    content = zf.read(names[0])
                for line in content.split(b"\n"):
                    if len(line) < 61:
                        continue
                    code = line[0:9].decode("euc-kr", errors="ignore").strip()
                    name = line[21:61].decode("euc-kr", errors="ignore").strip()
                    if len(code) > 6:
                        code = code[-6:]
                    if re.fullmatch(r"\d{6}", code) and name:
                        out[code] = {"symbol": code, "name": name, "exchange": exchange}
            except Exception:
                continue
        values = sorted(out.values(), key=lambda x: x["symbol"])
        if values:
            _master_cache = (now, values)
        return list(_master_cache[1])


def domestic_name(symbol: str) -> str:
    """Best-effort Korean company name from the official KIS KOSPI/KOSDAQ master."""
    code = str(symbol or "").strip().upper()
    if not re.fullmatch(r"\d{6}", code):
        return ""
    for item in domestic_master():
        if str(item.get("symbol") or "").strip() == code:
            return str(item.get("name") or "").strip()
    return ""
