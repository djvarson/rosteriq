"""
Static schema-guard regression test (no live Postgres required).

PostgresStore touches a number of feature tables that NO SQL migration
provisions (password reset / email verify tokens, audit_logs, shift swaps,
open shifts + bids, roster/payroll/analytics/approval/A-B/privacy tables,
themes, etc.). On a fresh Railway/Postgres deploy, the first request that hits
such a feature would raise psycopg2 UndefinedTable -> HTTP 500.

The fix is to create those tables lazily via ``CREATE TABLE IF NOT EXISTS``
(mirroring the existing revenue_actuals / direct_bookings pattern). This test
parses database.py's source and asserts that EVERY table referenced by
PostgresStore SQL (FROM / INTO / UPDATE / DELETE FROM / JOIN) has a
corresponding CREATE TABLE somewhere — either an inline guard in database.py or
a migration (001 / 003 / 004). If someone adds a query against a brand-new
table without creating it, this test fails before it can 500 in production.
"""

import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATABASE_PY = os.path.join(ROOT, "database.py")
MIGRATIONS = [
    os.path.join(ROOT, "migrations", "001_initial_schema.sql"),
    os.path.join(ROOT, "migrations", "003_new_features.sql"),
    os.path.join(ROOT, "migrations", "004_webhook_durability.sql"),
]

# SQL keywords that show up after FROM/INTO/UPDATE/JOIN but are NOT table
# names: CTE/table aliases, the trailing word of multi-word clauses, etc.
_NON_TABLE_TOKENS = {
    "a", "an", "e", "ip", "pe", "pb",            # single/double-letter aliases
    "select", "set", "data", "statements",        # clause fragments
    "rosteriq",                                    # appears in a docstring url
    "feed", "employee", "notification",           # alias words for *_config etc.
}

_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|INTO|UPDATE|DELETE\s+FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)
_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)


def _postgres_store_source() -> str:
    src = open(DATABASE_PY, encoding="utf-8").read()
    idx = src.find("class PostgresStore")
    assert idx != -1, "PostgresStore class not found in database.py"
    return src[idx:]


def _referenced_tables(pg_src: str) -> set:
    refs = {m.lower() for m in _TABLE_REF_RE.findall(pg_src)}
    return refs - _NON_TABLE_TOKENS


def _created_tables() -> set:
    created = set()
    # Inline CREATE TABLE IF NOT EXISTS guards inside database.py
    db_src = open(DATABASE_PY, encoding="utf-8").read()
    created |= {m.lower() for m in _CREATE_RE.findall(db_src)}
    # Migration-provisioned tables
    for path in MIGRATIONS:
        with open(path, encoding="utf-8") as fh:
            created |= {m.lower() for m in _CREATE_RE.findall(fh.read())}
    return created


def test_every_referenced_table_has_a_create():
    pg_src = _postgres_store_source()
    referenced = _referenced_tables(pg_src)
    created = _created_tables()

    missing = sorted(referenced - created)
    assert not missing, (
        "PostgresStore queries tables with no CREATE TABLE (migration or inline "
        f"guard) — these would 500 on a fresh deploy: {missing}"
    )


def test_known_runtime_guarded_tables_present():
    """Spot-check: tables that no migration creates must have an inline guard."""
    db_src = open(DATABASE_PY, encoding="utf-8").read()
    inline_created = {m.lower() for m in _CREATE_RE.findall(db_src)}
    expected_runtime_tables = {
        "roster_templates", "password_reset_tokens", "email_verification_tokens",
        "webhook_subscriptions", "shift_swaps", "privacy_consents",
        "privacy_audit_log", "anonymised_employees", "revenue_snapshots",
        "analytics_snapshots", "audit_logs", "themes", "api_key_records",
        "webhook_secrets", "preference_profiles", "ab_experiments",
        "ab_experiment_outcomes", "payroll_batches", "payroll_exports",
        "approval_requests", "roster_revisions", "open_shifts", "bids",
    }
    missing = sorted(expected_runtime_tables - inline_created)
    assert not missing, f"Missing inline CREATE TABLE guard for: {missing}"


def test_on_conflict_targets_have_constraints():
    """Every ON CONFLICT (col) target must be a PRIMARY KEY / UNIQUE column in
    the matching inline CREATE TABLE, otherwise the upsert errors at runtime."""
    pg_src = _postgres_store_source()
    # Map table -> its inline CREATE TABLE body
    bodies = {}
    for m in re.finditer(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\)\s*\"\"\"",
        pg_src,
        re.IGNORECASE | re.DOTALL,
    ):
        bodies.setdefault(m.group(1).lower(), m.group(2))

    # Find INSERT INTO <table> ... ON CONFLICT (<col>) pairs. The middle group
    # must not cross into another INSERT/CREATE statement, so an INSERT that has
    # no ON CONFLICT can't "borrow" the next statement's clause.
    for m in re.finditer(
        r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        r"((?:(?!INSERT\s+INTO|CREATE\s+TABLE).)*?)"
        r"ON\s+CONFLICT\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)",
        pg_src,
        re.IGNORECASE | re.DOTALL,
    ):
        table = m.group(1).lower()
        conflict_col = m.group(3).lower()
        body = bodies.get(table)
        if body is None:
            # Table created by migration, not inline — out of scope here.
            continue
        body_l = body.lower()
        col_line = next(
            (ln for ln in body_l.splitlines() if re.match(
                rf"\s*{re.escape(conflict_col)}\b", ln)),
            "",
        )
        has_constraint = (
            "primary key" in col_line
            or "unique" in col_line
            or re.search(rf"unique\s*\(\s*[^)]*\b{re.escape(conflict_col)}\b",
                         body_l) is not None
            or re.search(rf"primary\s+key\s*\(\s*[^)]*\b{re.escape(conflict_col)}\b",
                         body_l) is not None
        )
        assert has_constraint, (
            f"ON CONFLICT ({conflict_col}) on inline table '{table}' but that "
            f"column has no PRIMARY KEY/UNIQUE constraint in its CREATE TABLE"
        )
