"""SQLite-backed persistence layer for RosterIQ stores (Round 11).

Why this matters: all module-level singletons — tenants, subscriptions,
onboarding, shift events, history warehouse, concierge KB — currently
live in memory. A worker restart on Railway throws all of it away. The
runbook flags this as the biggest production gap.

Design:
- One process-wide SQLite connection (thread-safe, WAL mode).
- A registry-based migration runner (`register_schema(name, ddl)` +
  `init_db()`).
- Generic helpers (`json_dumps`, `json_loads`, `now_iso`) so stores can
  serialize their own dataclasses without a heavy ORM.
- `is_persistence_enabled()` short-circuits to no-op when the env var
  `ROSTERIQ_DB_PATH` is unset — preserves demo/sandbox behaviour and
  test isolation.

Stores opt in by:
  1. Calling `register_schema("my_store", "CREATE TABLE IF NOT EXISTS ...")`
     at module load.
  2. Calling `connection()` inside their write paths to upsert rows.
  3. Reading rows back from SQLite when constructed.

Pure stdlib — sqlite3 ships with Python.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

logger = logging.getLogger("rosteriq.persistence")


# ---------------------------------------------------------------------------
# Environment / wiring
# ---------------------------------------------------------------------------

_DB_PATH_ENV = "ROSTERIQ_DB_PATH"
_DEFAULT_DB_PATH = ":memory:"


def db_path() -> str:
    """Return the configured DB path; ":memory:" if unset."""
    return os.environ.get(_DB_PATH_ENV) or _DEFAULT_DB_PATH


def is_persistence_enabled() -> bool:
    """True when the ROSTERIQ_DB_PATH env var is set to a real file path.

    In-memory mode (`:memory:`) is treated as DISABLED by stores so that
    they don't accidentally double-write into a throwaway connection
    during tests. Use `force_enable_for_tests()` to override.
    """
    if _force_enabled[0]:
        return True
    p = os.environ.get(_DB_PATH_ENV)
    return bool(p) and p != ":memory:"


_force_enabled = [False]


def force_enable_for_tests(value: bool = True) -> None:
    """Test helper — pretend persistence is enabled even in :memory: mode."""
    _force_enabled[0] = value


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

_conn: Optional[sqlite3.Connection] = None
_conn_lock = threading.Lock()
_schemas: List[str] = []  # ordered list of DDL strings
_schema_names: set = set()


def _open() -> sqlite3.Connection:
    path = db_path()
    # check_same_thread=False because RosterIQ uses a thread pool. We
    # serialise writes via _conn_lock above.
    c = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    c.row_factory = sqlite3.Row
    # WAL = better concurrency for readers + faster writes. Only valid
    # for file-backed DBs.
    if path != ":memory:":
        try:
            c.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError as e:  # pragma: no cover
            logger.warning("WAL pragma failed: %s", e)
    c.execute("PRAGMA foreign_keys=ON")
    return c


def connection() -> sqlite3.Connection:
    """Return the process-wide connection, opening + initializing on demand."""
    global _conn
    with _conn_lock:
        if _conn is None:
            _conn = _open()
            _apply_schemas(_conn)
    return _conn


def reset_for_tests() -> None:
    """Close + drop the global connection. Use between test cases."""
    global _conn
    with _conn_lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
        _conn = None


def register_schema(name: str, ddl: str) -> None:
    """Register a CREATE-TABLE-IF-NOT-EXISTS statement for a store.

    Safe to call multiple times for the same name (idempotent). DDL is
    applied next time `connection()` is called against a fresh connection.
    """
    if name in _schema_names:
        return
    _schema_names.add(name)
    _schemas.append(ddl)
    # If the connection is already open, apply immediately so callers
    # registering late still get their tables.
    if _conn is not None:
        try:
            _conn.executescript(ddl)
        except sqlite3.DatabaseError as e:
            logger.error("schema apply failed for %s: %s", name, e)


def _apply_schemas(c: sqlite3.Connection) -> None:
    for ddl in _schemas:
        try:
            c.executescript(ddl)
        except sqlite3.DatabaseError as e:
            logger.error("schema apply failed: %s\nDDL:\n%s", e, ddl)


# ---------------------------------------------------------------------------
# Locking — serialise writes across threads
# ---------------------------------------------------------------------------

_write_lock = threading.RLock()


@contextmanager
def write_txn() -> Iterator[sqlite3.Connection]:
    """Serialise writers — yields the connection inside an exclusive lock.

    Caller is responsible for catching its own exceptions. Errors here
    are logged but not swallowed.
    """
    c = connection()
    with _write_lock:
        try:
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.execute("COMMIT")
        except Exception:
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_dumps(obj: Any) -> str:
    """JSON encode with sane defaults for dataclasses + datetime."""
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        if hasattr(o, "to_dict"):
            return o.to_dict()
        if hasattr(o, "__dict__"):
            return o.__dict__
        return str(o)
    return json.dumps(obj, default=default, separators=(",", ":"))


def json_loads(s: Optional[str], default: Any = None) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except (TypeError, ValueError):
        return default


def upsert(table: str, row: Dict[str, Any], pk: str = "id") -> None:
    """Generic UPSERT helper. row[pk] must be present.

    No-op when persistence is disabled. Logs+swallows errors so callers
    never crash on a write — the in-memory copy stays the source of
    truth either way.
    """
    if not is_persistence_enabled():
        return
    keys = list(row.keys())
    placeholders = ",".join("?" for _ in keys)
    cols = ",".join(keys)
    updates = ",".join(f"{k}=excluded.{k}" for k in keys if k != pk)
    sql = (
        f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT({pk}) DO UPDATE SET {updates}"
    )
    try:
        with write_txn() as c:
            c.execute(sql, [row[k] for k in keys])
    except sqlite3.DatabaseError as e:
        logger.warning("upsert into %s failed: %s", table, e)


def delete(table: str, pk: str, value: Any) -> None:
    if not is_persistence_enabled():
        return
    try:
        with write_txn() as c:
            c.execute(f"DELETE FROM {table} WHERE {pk} = ?", [value])
    except sqlite3.DatabaseError as e:
        logger.warning("delete from %s failed: %s", table, e)


def fetchall(sql: str, params: Optional[List[Any]] = None) -> List[sqlite3.Row]:
    if not is_persistence_enabled():
        return []
    try:
        c = connection()
        cur = c.execute(sql, params or [])
        return list(cur.fetchall())
    except sqlite3.DatabaseError as e:
        logger.warning("fetchall failed for %r: %s", sql, e)
        return []


def fetchone(sql: str, params: Optional[List[Any]] = None) -> Optional[sqlite3.Row]:
    rows = fetchall(sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# Lifecycle hook for stores
# ---------------------------------------------------------------------------

_rehydrate_callbacks: List[Callable[[], None]] = []


def on_init(fn: Callable[[], None]) -> Callable[[], None]:
    """Register a callback to rehydrate state from SQLite at startup.

    The callback is invoked the first time `init_db()` is called, in
    registration order. Stores typically register a `_rehydrate()`
    method that walks their table and rebuilds the in-memory dict.
    """
    _rehydrate_callbacks.append(fn)
    return fn


_rehydrated = [False]


def init_db() -> None:
    """Open the connection, apply schemas, run rehydrate callbacks once.

    Safe to call multiple times — rehydration only runs the first time.
    Should be called from the FastAPI startup event (or test setUp).
    """
    connection()
    if _rehydrated[0]:
        return
    if not is_persistence_enabled():
        # Mark rehydrated so we don't keep re-checking; callbacks would
        # be no-ops anyway.
        _rehydrated[0] = True
        return
    _rehydrated[0] = True
    for cb in list(_rehydrate_callbacks):
        try:
            cb()
        except Exception as e:
            logger.error("rehydrate callback %s failed: %s", cb, e)


def reset_rehydrate_for_tests() -> None:
    _rehydrated[0] = False
