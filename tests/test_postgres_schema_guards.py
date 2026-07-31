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
    "jsonb_array_elements",                        # set-returning function, not a table
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


_CREATE_BLOCK_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\n\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_INSERT_COLS_RE = re.compile(
    r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
_NON_COLUMN_LEADS = {"PRIMARY", "UNIQUE", "FOREIGN", "CONSTRAINT", "CHECK", "REFERENCES"}


def _table_columns() -> dict:
    """Map table name -> set of column names, parsed from every CREATE TABLE
    (migrations + inline self-heal DDL)."""
    cols: dict = {}
    sources = [open(DATABASE_PY, encoding="utf-8").read()]
    for path in MIGRATIONS:
        sources.append(open(path, encoding="utf-8").read())
    for src in sources:
        for name, body in _CREATE_BLOCK_RE.findall(src):
            colset = cols.setdefault(name.lower(), set())
            for line in body.splitlines():
                line = line.strip().strip(",")
                if not line or line.startswith("--"):
                    continue
                # leading identifier, tolerating a quoted reserved word ("group")
                lead = re.match(r'"?([a-zA-Z_][a-zA-Z0-9_]*)"?', line)
                if not lead or lead.group(1).upper() in _NON_COLUMN_LEADS:
                    continue
                colset.add(lead.group(1).lower())
    return cols


def test_insert_columns_exist_in_table_schema():
    """Every column a PostgresStore INSERT names must exist in that table's
    CREATE TABLE. Catches column-name drift that 500s on real Postgres but slips
    past the MemoryStore-backed suite, e.g.:
      - login_attempts.created_at vs the real .attempted_at column
      - notification_preferences / push_subscriptions, whose code uses a blob
        schema (user_id + JSON) that an earlier normalised migration didn't match
    Tables with no locally-defined DDL are skipped (provisioned elsewhere)."""
    pg_src = _postgres_store_source()
    cols = _table_columns()
    problems = []
    for table, collist in _INSERT_COLS_RE.findall(pg_src):
        known = cols.get(table.lower())
        if not known:
            continue  # table defined outside our DDL sources — out of scope
        for c in collist.split(","):
            c = c.strip().strip('"').lower()  # tolerate quoted identifiers
            if c and c not in known:
                problems.append(f"{table}.{c}")
    assert not problems, (
        "PostgresStore INSERT references columns absent from the table schema "
        f"(would 500 on real Postgres): {sorted(set(problems))}"
    )


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
