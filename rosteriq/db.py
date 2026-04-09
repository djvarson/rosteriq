"""
RosterIQ Database Module

Async database abstraction layer supporting SQLite (development) and PostgreSQL (production).
Uses aiosqlite for async operations with connection pooling and context managers.

Database connection is configured via DATABASE_URL environment variable:
- sqlite:///rosteriq.db (default, local development)
- postgresql://user:password@host/dbname (production)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, AsyncGenerator, Any

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

try:
    import asyncpg
except ImportError:
    asyncpg = None

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///rosteriq.db"
)

# Connection pool size for PostgreSQL (ignored for SQLite)
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))


# ============================================================================
# Database Client Classes
# ============================================================================

class SQLiteClient:
    """Async SQLite database client with connection management."""

    def __init__(self, database_path: str):
        """
        Initialize SQLite client.

        Args:
            database_path: Path to SQLite database file
        """
        if database_path.startswith("sqlite:///"):
            database_path = database_path.replace("sqlite:///", "")

        self.database_path = database_path
        self.db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Establish database connection."""
        if aiosqlite is None:
            raise ImportError("aiosqlite is required for SQLite support. Install with: pip install aiosqlite")

        logger.info(f"Connecting to SQLite database: {self.database_path}")
        self.db = await aiosqlite.connect(self.database_path)
        # Enable foreign key constraints
        await self.db.execute("PRAGMA foreign_keys = ON")
        await self.db.commit()

    async def disconnect(self) -> None:
        """Close database connection."""
        if self.db:
            await self.db.close()
            logger.info("SQLite connection closed")

    async def execute(
        self,
        query: str,
        params: tuple | list | None = None
    ) -> aiosqlite.Cursor:
        """
        Execute a query and return cursor.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Cursor with result
        """
        if not self.db:
            raise RuntimeError("Database not connected. Call connect() first.")

        cursor = await self.db.execute(query, params or ())
        return cursor

    async def execute_many(
        self,
        query: str,
        params_list: list[tuple | list]
    ) -> None:
        """
        Execute the same query with multiple parameter sets.

        Args:
            query: SQL query string
            params_list: List of parameter tuples/lists
        """
        if not self.db:
            raise RuntimeError("Database not connected. Call connect() first.")

        await self.db.executemany(query, params_list)

    async def commit(self) -> None:
        """Commit current transaction."""
        if self.db:
            await self.db.commit()

    async def rollback(self) -> None:
        """Rollback current transaction."""
        if self.db:
            await self.db.rollback()

    async def execute_script(self, script: str) -> None:
        """
        Execute SQL script (useful for schema initialization).

        Args:
            script: SQL script with multiple statements
        """
        if not self.db:
            raise RuntimeError("Database not connected. Call connect() first.")

        await self.db.executescript(script)


class PostgreSQLClient:
    """Async PostgreSQL database client with connection pooling."""

    def __init__(self, database_url: str):
        """
        Initialize PostgreSQL client.

        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self) -> None:
        """Establish connection pool."""
        if asyncpg is None:
            raise ImportError("asyncpg is required for PostgreSQL support. Install with: pip install asyncpg")

        logger.info(f"Connecting to PostgreSQL: {self.database_url}")
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=1,
            max_size=DB_POOL_SIZE,
            command_timeout=DB_POOL_TIMEOUT,
        )
        logger.info(f"PostgreSQL connection pool established (size: {DB_POOL_SIZE})")

    async def disconnect(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")

    async def execute(
        self,
        query: str,
        *args: Any
    ) -> list[Any]:
        """
        Execute a query and return results.

        Args:
            query: SQL query string
            args: Query parameters

        Returns:
            List of result rows
        """
        if not self.pool:
            raise RuntimeError("Database not connected. Call connect() first.")

        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def execute_many(
        self,
        query: str,
        args_list: list[tuple]
    ) -> None:
        """
        Execute the same query with multiple parameter sets.

        Args:
            query: SQL query string
            args_list: List of parameter tuples
        """
        if not self.pool:
            raise RuntimeError("Database not connected. Call connect() first.")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for args in args_list:
                    await conn.execute(query, *args)

    async def execute_script(self, script: str) -> None:
        """
        Execute SQL script.

        Args:
            script: SQL script with multiple statements
        """
        if not self.pool:
            raise RuntimeError("Database not connected. Call connect() first.")

        async with self.pool.acquire() as conn:
            await conn.execute(script)


# ============================================================================
# Global Database Instance
# ============================================================================

_db_client: SQLiteClient | PostgreSQLClient | None = None


def _parse_database_url(url: str) -> tuple[str, str]:
    """
    Parse DATABASE_URL to determine backend and extract path/url.

    Args:
        url: Connection URL

    Returns:
        Tuple of (backend_type, connection_string)
    """
    if url.startswith("postgresql://") or url.startswith("postgres://"):
        return "postgresql", url.replace("postgres://", "postgresql://")
    elif url.startswith("sqlite://"):
        return "sqlite", url
    else:
        raise ValueError(f"Unsupported database URL format: {url}")


async def init_db() -> None:
    """
    Initialize database connection and create all tables.

    Creates database client based on DATABASE_URL environment variable.
    If using SQLite, creates the database file if it doesn't exist.
    Loads and executes schema.sql.
    """
    global _db_client

    if _db_client is not None:
        logger.warning("Database already initialized")
        return

    backend, connection_string = _parse_database_url(DATABASE_URL)

    if backend == "sqlite":
        _db_client = SQLiteClient(connection_string)
    else:
        _db_client = PostgreSQLClient(connection_string)

    await _db_client.connect()

    # Load and execute schema
    schema_path = Path(__file__).parent / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    logger.info("Creating database tables from schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    await _db_client.execute_script(schema_sql)
    logger.info("Database initialization complete")


async def close_db() -> None:
    """Close database connection."""
    global _db_client

    if _db_client:
        await _db_client.disconnect()
        _db_client = None


def get_db_client() -> SQLiteClient | PostgreSQLClient:
    """
    Get the global database client instance.

    Returns:
        Database client

    Raises:
        RuntimeError: If database not initialized
    """
    if _db_client is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db_client


# ============================================================================
# Context Manager for Database Access
# ============================================================================

@asynccontextmanager
async def get_db() -> AsyncGenerator[SQLiteClient | PostgreSQLClient, None]:
    """
    Async context manager for database access.

    Usage:
        async with get_db() as db:
            cursor = await db.execute("SELECT * FROM venues WHERE id = ?", (venue_id,))
            venues = await cursor.fetchall()

    Yields:
        Database client instance
    """
    db_client = get_db_client()
    try:
        yield db_client
    finally:
        # For SQLite, commit any pending transactions
        if isinstance(db_client, SQLiteClient):
            await db_client.commit()


# ============================================================================
# Helper Functions for Common Operations
# ============================================================================

async def execute_query(
    query: str,
    params: tuple | list | None = None
) -> list[Any]:
    """
    Execute a SELECT query and return all rows.

    Args:
        query: SQL SELECT query
        params: Query parameters

    Returns:
        List of result rows
    """
    db_client = get_db_client()

    if isinstance(db_client, SQLiteClient):
        cursor = await db_client.execute(query, params)
        return await cursor.fetchall()
    else:  # PostgreSQL
        return await db_client.execute(query, *(params or ()))


async def execute_insert(
    query: str,
    params: tuple | list
) -> None:
    """
    Execute an INSERT query.

    Args:
        query: SQL INSERT query
        params: Query parameters
    """
    db_client = get_db_client()

    if isinstance(db_client, SQLiteClient):
        await db_client.execute(query, params)
        await db_client.commit()
    else:  # PostgreSQL
        await db_client.execute(query, *params)


async def execute_update(
    query: str,
    params: tuple | list
) -> int:
    """
    Execute an UPDATE query.

    Args:
        query: SQL UPDATE query
        params: Query parameters

    Returns:
        Number of rows affected
    """
    db_client = get_db_client()

    if isinstance(db_client, SQLiteClient):
        cursor = await db_client.execute(query, params)
        await db_client.commit()
        return cursor.rowcount
    else:  # PostgreSQL
        result = await db_client.execute(query, *params)
        # asyncpg returns the status string, parse rows affected
        if result and result.startswith("UPDATE"):
            return int(result.split()[-1])
        return 0


async def execute_delete(
    query: str,
    params: tuple | list
) -> int:
    """
    Execute a DELETE query.

    Args:
        query: SQL DELETE query
        params: Query parameters

    Returns:
        Number of rows deleted
    """
    db_client = get_db_client()

    if isinstance(db_client, SQLiteClient):
        cursor = await db_client.execute(query, params)
        await db_client.commit()
        return cursor.rowcount
    else:  # PostgreSQL
        result = await db_client.execute(query, *params)
        if result and result.startswith("DELETE"):
            return int(result.split()[-1])
        return 0


# ============================================================================
# Lifespan Management for FastAPI
# ============================================================================

async def lifespan_startup() -> None:
    """Initialize database on application startup."""
    await init_db()
    logger.info(f"Database initialized: {DATABASE_URL}")


async def lifespan_shutdown() -> None:
    """Close database on application shutdown."""
    await close_db()
    logger.info("Database closed")
