#!/usr/bin/env python3
"""
RosterIQ Database Migration Runner - Production Hardened

Runs all SQL migrations in order, tracking which ones have been applied
in the migration_history table. Migrations are applied atomically with
proper error handling, idempotency, and dry-run support.

Usage:
    python -m rosteriq.migrations.run_migrations --run     # Run pending migrations
    python -m rosteriq.migrations.run_migrations --status   # Show status
    python -m rosteriq.migrations.run_migrations --dry-run  # Preview changes

Environment:
    DATABASE_URL=postgres://user:pass@host/dbname
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import sql
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("rosteriq.migrations")

MIGRATIONS_DIR = Path(__file__).parent
DATABASE_URL = os.environ.get("DATABASE_URL")


@dataclass
class MigrationResult:
    """Result of a migration operation."""
    success: bool
    migrations_run: int
    errors: list[str]
    message: str


class MigrationRunner:
    """Production-grade database migration manager."""

    def __init__(self, database_url: str):
        """
        Initialize migration runner with database connection.

        Args:
            database_url: PostgreSQL connection string

        Raises:
            SystemExit: If connection fails
        """
        self.database_url = database_url
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.conn.autocommit = False
            logger.info("Connected to PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            sys.exit(1)

    def _cursor(self):
        """Get a cursor factory."""
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def ensure_migration_table(self) -> bool:
        """
        Create migration_history table if it doesn't exist.
        Uses idempotent CREATE TABLE IF NOT EXISTS.

        Returns:
            True if successful
        """
        try:
            with self._cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS migration_history (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        status TEXT NOT NULL DEFAULT 'success',
                        error_message TEXT
                    );
                """)
            self.conn.commit()
            logger.debug("Ensured migration_history table exists")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to create migration_history table: {e}")
            self.conn.rollback()
            return False

    def get_applied_migrations(self) -> list[str]:
        """
        Query migration_history for successfully applied migrations.

        Returns:
            List of migration names (e.g., ['001_initial_schema', '002_seed_data'])
        """
        try:
            with self._cursor() as cur:
                cur.execute("""
                    SELECT name FROM migration_history
                    WHERE status = 'success'
                    ORDER BY name ASC
                """)
                return [row["name"] for row in cur.fetchall()]
        except psycopg2.Error as e:
            logger.error(f"Failed to query applied migrations: {e}")
            return []

    def get_pending_migrations(self) -> list[str]:
        """
        Compare migration files on disk with applied migrations.
        Returns only migrations not yet applied.

        Returns:
            List of pending migration names in numeric order
        """
        all_migrations = self._get_migration_files()
        applied = set(self.get_applied_migrations())
        pending = [name for name, _ in all_migrations if name not in applied]
        return pending

    def _get_migration_files(self) -> list[tuple[str, Path]]:
        """
        Get all SQL migration files in numeric order.

        Expected naming: NNN_description.sql (e.g., 001_initial_schema.sql)

        Returns:
            List of (name, path) tuples sorted by numeric prefix
        """
        sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

        # Sort by numeric prefix (001, 002, 003, etc.)
        def get_sort_key(f: Path) -> int:
            try:
                prefix = f.stem.split('_')[0]
                return int(prefix) if prefix.isdigit() else 999
            except (ValueError, IndexError):
                return 999

        sql_files.sort(key=get_sort_key)
        return [(f.stem, f) for f in sql_files]

    def _load_migration_sql(self, filepath: Path) -> str:
        """
        Load SQL from migration file, strip comments.
        Preserves inline SQL, removes full-line comments.

        Args:
            filepath: Path to SQL migration file

        Returns:
            Cleaned SQL string
        """
        try:
            with open(filepath, 'r') as f:
                lines = f.readlines()
        except IOError as e:
            logger.error(f"Failed to read {filepath}: {e}")
            return ""

        sql_lines = []
        for line in lines:
            stripped = line.strip()
            # Skip full-line comments but preserve inline SQL
            if not stripped.startswith('--'):
                sql_lines.append(line)

        return ''.join(sql_lines).strip()

    def run_single(self, filename: str) -> bool:
        """
        Run a single migration file.

        Args:
            filename: Migration filename (e.g., '001_initial_schema.sql')

        Returns:
            True if successful, False if failed
        """
        filepath = MIGRATIONS_DIR / f"{filename}.sql"
        if not filepath.exists():
            logger.error(f"Migration file not found: {filepath}")
            return False

        logger.info(f"Running {filename}...")
        try:
            sql_content = self._load_migration_sql(filepath)
            if not sql_content:
                logger.warning(f"Migration {filename} is empty")
                return False

            with self._cursor() as cur:
                cur.execute(sql_content)
            self.conn.commit()

            # Record in migration history
            with self._cursor() as cur:
                cur.execute("""
                    INSERT INTO migration_history (name, status)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO NOTHING
                """, (filename, 'success'))
            self.conn.commit()

            logger.info(f"✓ {filename} completed")
            return True

        except psycopg2.Error as e:
            logger.error(f"✗ {filename} failed: {e}")
            self.conn.rollback()

            # Record failure
            try:
                with self._cursor() as cur:
                    cur.execute("""
                        INSERT INTO migration_history (name, status, error_message)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (name) DO UPDATE
                        SET status = EXCLUDED.status, error_message = EXCLUDED.error_message
                    """, (filename, 'failed', str(e)))
                self.conn.commit()
            except psycopg2.Error as record_err:
                logger.error(f"Failed to record migration error: {record_err}")

            return False

    def run_pending(self) -> MigrationResult:
        """
        Run all pending migrations in order.
        Stops at first error; does not run subsequent migrations.

        Returns:
            MigrationResult with success status and details
        """
        logger.info(f"Starting migration run from {MIGRATIONS_DIR}")

        # Ensure table exists
        if not self.ensure_migration_table():
            return MigrationResult(
                success=False,
                migrations_run=0,
                errors=["Failed to create migration_history table"],
                message="Aborted: cannot create migration_history"
            )

        pending = self.get_pending_migrations()
        if not pending:
            logger.info("No pending migrations")
            return MigrationResult(
                success=True,
                migrations_run=0,
                errors=[],
                message="All migrations already applied"
            )

        logger.info(f"Found {len(pending)} pending migration(s)")
        migrations_run = 0
        errors = []

        for migration_name in pending:
            if not self.run_single(migration_name):
                errors.append(f"Migration {migration_name} failed")
                return MigrationResult(
                    success=False,
                    migrations_run=migrations_run,
                    errors=errors,
                    message=f"Failed at {migration_name}; stopped"
                )
            migrations_run += 1

        return MigrationResult(
            success=True,
            migrations_run=migrations_run,
            errors=[],
            message=f"All {migrations_run} pending migration(s) completed"
        )

    def get_status(self) -> dict:
        """
        Return current migration status.

        Returns:
            Dict with 'applied', 'pending', 'failed', 'total' counts
        """
        self.ensure_migration_table()

        all_migrations = self._get_migration_files()
        applied = set(self.get_applied_migrations())
        pending = self.get_pending_migrations()

        # Get failed migrations
        failed = []
        try:
            with self._cursor() as cur:
                cur.execute("SELECT name FROM migration_history WHERE status = 'failed'")
                failed = [row["name"] for row in cur.fetchall()]
        except psycopg2.Error as e:
            logger.warning(f"Could not query failed migrations: {e}")

        return {
            'total': len(all_migrations),
            'applied': len(applied),
            'pending': len(pending),
            'failed': len(failed),
            'migrations': [
                {
                    'name': name,
                    'status': 'applied' if name in applied else (
                        'failed' if name in failed else 'pending'
                    )
                }
                for name, _ in all_migrations
            ]
        }

    def dry_run(self) -> dict:
        """
        Preview pending migrations without applying them.

        Returns:
            Dict with pending migrations and their SQL content
        """
        pending = self.get_pending_migrations()
        preview = {}

        for migration_name in pending:
            all_migrations = self._get_migration_files()
            for name, path in all_migrations:
                if name == migration_name:
                    sql_content = self._load_migration_sql(path)
                    preview[migration_name] = {
                        'file': path.name,
                        'sql': sql_content[:500] + ('...' if len(sql_content) > 500 else ''),
                        'size_bytes': len(sql_content)
                    }
                    break

        return preview

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.debug("Database connection closed")
            except psycopg2.Error as e:
                logger.error(f"Error closing connection: {e}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="RosterIQ Production Database Migration Runner"
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run all pending migrations (default)"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show migration status and exit"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview pending migrations without applying"
    )

    args = parser.parse_args()

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    runner = MigrationRunner(DATABASE_URL)
    try:
        if args.status:
            status = runner.get_status()
            print("\nMigration Status:")
            print("-" * 70)
            print(f"Total: {status['total']} | Applied: {status['applied']} | "
                  f"Pending: {status['pending']} | Failed: {status['failed']}")
            print("-" * 70)
            for mig in status['migrations']:
                status_icon = {
                    'applied': '✓',
                    'pending': '○',
                    'failed': '✗'
                }.get(mig['status'], '?')
                print(f"  {status_icon} {mig['name']:<40} {mig['status']}")
            print()

        elif args.dry_run:
            preview = runner.dry_run()
            if preview:
                print("\nPending Migrations Preview:")
                print("-" * 70)
                for name, details in preview.items():
                    print(f"\n{name} ({details['size_bytes']} bytes)")
                    print(f"  {details['sql']}")
            else:
                print("No pending migrations")
            print()

        else:  # --run (default)
            result = runner.run_pending()
            logger.info(result.message)
            if not result.success:
                for error in result.errors:
                    logger.error(error)
                sys.exit(1)

    finally:
        runner.close()


if __name__ == "__main__":
    main()
else:
    # Allow import as module
    pass
