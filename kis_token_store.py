"""Persistent KIS OAuth access-token store for Tajum On V149.

Why this exists
---------------
KIS access tokens are normally valid for many hours. A Render deploy/restart destroys
Python process memory, so a memory-only token cache can request another token even while
the previous one is still valid. KIS warns that frequent re-issuance can be restricted.

V149 stores the *existing valid access token* in PostgreSQL and serializes issuance with
a PostgreSQL advisory transaction lock. All app processes/services that share the same
PERFORMANCE_DATABASE_URL + KIS app key can reuse one valid token after a restart.

Security
--------
- The token value is never returned by status(), logged, or written to documentation.
- PostgreSQL contains the token because reuse after restart requires the original value.
  Keep PERFORMANCE_DATABASE_URL private and restrict DB access.
- If PostgreSQL is unavailable, the caller can fall back to process-memory behavior.
"""
from __future__ import annotations

import hashlib
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

try:
    import psycopg
except Exception:
    psycopg = None

DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()
TOKEN_REUSE_SAFETY_SEC = max(
    300,
    min(int(os.getenv("KIS_TOKEN_REUSE_SAFETY_SEC", "900") or 900), 3600),
)

_process_lock = threading.Lock()
_status_lock = threading.Lock()
_schema_ready = False
_status: dict[str, Any] = {
    "persistence": "postgres" if DATABASE_URL and psycopg is not None else "memory_fallback",
    "last_source": None,
    "expires_at": None,
    "remaining_sec": None,
    "issue_count": 0,
    "reuse_count": 0,
    "invalidate_count": 0,
    "db_error_count": 0,
    "last_db_error": None,
    "safety_sec": TOKEN_REUSE_SAFETY_SEC,
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _token_key(app_key: str, env: str) -> str:
    # Non-secret identity only; never store/emit the raw app key.
    fp = hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:24]
    return f"{str(env or 'real').lower()}:{fp}"


def _advisory_key(token_key: str) -> int:
    raw = int.from_bytes(hashlib.sha256(("tajum-kis-token:" + token_key).encode()).digest()[:8], "big")
    # PostgreSQL advisory locks use signed bigint.
    return raw - (1 << 64) if raw >= (1 << 63) else raw


def _set_status(**kwargs: Any) -> None:
    with _status_lock:
        _status.update(kwargs)


def _count(name: str) -> None:
    with _status_lock:
        _status[name] = int(_status.get(name, 0) or 0) + 1


def status() -> dict[str, Any]:
    with _status_lock:
        out = dict(_status)
    exp = out.get("expires_at")
    if exp:
        try:
            dt = datetime.fromisoformat(str(exp))
            out["remaining_sec"] = max(0, int((dt - _utcnow()).total_seconds()))
        except Exception:
            out["remaining_sec"] = None
    # Deliberately no token value / app-key fingerprint.
    return out


def _connect():
    if psycopg is None or not DATABASE_URL:
        raise RuntimeError("persistent token database unavailable")
    return psycopg.connect(
        DATABASE_URL,
        autocommit=False,
        connect_timeout=5,
        application_name="bbangdol-kis-token",
    )


def _ensure_schema(conn) -> None:
    global _schema_ready
    if _schema_ready:
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tajum_kis_oauth_tokens(
            token_key TEXT PRIMARY KEY,
            access_token TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            issued_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    # Commit schema creation separately. Otherwise a later token-issuance failure
    # could roll back CREATE TABLE while the process-level `_schema_ready` flag
    # incorrectly remains True.
    conn.commit()
    _schema_ready = True


def _valid(expires_at: datetime, now: datetime) -> bool:
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return (expires_at - now).total_seconds() > TOKEN_REUSE_SAFETY_SEC


def get_or_issue(
    *,
    app_key: str,
    env: str,
    issuer: Callable[[], tuple[str, int]],
) -> tuple[str, float, str]:
    """Return token, expiry_epoch, source.

    source:
      memory_fallback_new  - PostgreSQL unavailable; issuer called by caller process.
      db_reused            - valid token loaded from PostgreSQL.
      db_new               - this process obtained the advisory lock and issued once.
    """
    key = _token_key(app_key, env)

    with _process_lock:
        if not DATABASE_URL or psycopg is None:
            token, expires_in = issuer()
            now = time.time()
            _count("issue_count")
            _set_status(
                persistence="memory_fallback",
                last_source="memory_fallback_new",
                expires_at=datetime.fromtimestamp(now + max(300, int(expires_in)), timezone.utc).isoformat(),
            )
            return token, now + max(300, int(expires_in)), "memory_fallback_new"

        try:
            with _connect() as conn:
                _ensure_schema(conn)
                # Cross-process/deploy serialization. Lock is automatically released
                # on commit/rollback/connection close.
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (_advisory_key(key),))
                now_dt = _utcnow()
                row = conn.execute(
                    """
                    SELECT access_token, expires_at
                    FROM tajum_kis_oauth_tokens
                    WHERE token_key=%s
                    """,
                    (key,),
                ).fetchone()

                if row:
                    token = str(row[0] or "")
                    expires_at = row[1]
                    if token and _valid(expires_at, now_dt):
                        if expires_at.tzinfo is None:
                            expires_at = expires_at.replace(tzinfo=timezone.utc)
                        conn.commit()
                        _count("reuse_count")
                        _set_status(
                            persistence="postgres",
                            last_source="db_reused",
                            expires_at=expires_at.isoformat(),
                            last_db_error=None,
                        )
                        return token, expires_at.timestamp(), "db_reused"

                # Still holding the advisory lock: only one process can reach issuance.
                token, expires_in = issuer()
                expires_in = max(300, int(expires_in))
                issued_at = _utcnow()
                expires_at = datetime.fromtimestamp(time.time() + expires_in, timezone.utc)
                conn.execute(
                    """
                    INSERT INTO tajum_kis_oauth_tokens(
                        token_key, access_token, expires_at, issued_at, updated_at
                    )
                    VALUES(%s,%s,%s,%s,NOW())
                    ON CONFLICT(token_key) DO UPDATE SET
                        access_token=EXCLUDED.access_token,
                        expires_at=EXCLUDED.expires_at,
                        issued_at=EXCLUDED.issued_at,
                        updated_at=NOW()
                    """,
                    (key, token, expires_at, issued_at),
                )
                conn.commit()
                _count("issue_count")
                _set_status(
                    persistence="postgres",
                    last_source="db_new",
                    expires_at=expires_at.isoformat(),
                    last_db_error=None,
                )
                return token, expires_at.timestamp(), "db_new"

        except Exception as exc:
            # Availability first: a temporary performance-DB issue must not stop KIS.
            _count("db_error_count")
            _set_status(
                persistence="memory_fallback",
                last_db_error=f"{type(exc).__name__}: {exc}",
            )
            token, expires_in = issuer()
            expires_in = max(300, int(expires_in))
            now = time.time()
            _count("issue_count")
            _set_status(
                last_source="memory_fallback_new",
                expires_at=datetime.fromtimestamp(now + expires_in, timezone.utc).isoformat(),
            )
            return token, now + expires_in, "memory_fallback_new"


def invalidate(*, app_key: str, env: str, token: str, reason: str = "") -> None:
    """Invalidate only the token the caller actually used.

    The conditional DELETE prevents an older process from deleting a newer token that
    another deployment has already issued.
    """
    _count("invalidate_count")
    if not token or not DATABASE_URL or psycopg is None:
        return
    key = _token_key(app_key, env)
    try:
        with _connect() as conn:
            _ensure_schema(conn)
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (_advisory_key(key),))
            conn.execute(
                """
                DELETE FROM tajum_kis_oauth_tokens
                WHERE token_key=%s AND access_token=%s
                """,
                (key, token),
            )
            conn.commit()
            _set_status(last_source=f"invalidated:{reason or 'auth_error'}", last_db_error=None)
    except Exception as exc:
        _count("db_error_count")
        _set_status(last_db_error=f"{type(exc).__name__}: {exc}")
