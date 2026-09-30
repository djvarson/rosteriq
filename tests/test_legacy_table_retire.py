"""
notification_preferences / push_subscriptions: an early migration 003 created
them in a normalised shape no code reads. CREATE TABLE IF NOT EXISTS leaves
such a table in place, so every preferences read and write 500s on Postgres
(prod did: GET /api/notifications/preferences). At connect, a table missing
the column the code uses is renamed aside and the real one created.
"""

from contextlib import contextmanager

from rosteriq.database import PostgresStore


class _Cur:
    def __init__(self, shapes, log):
        self.shapes, self.log, self._row = shapes, log, None

    def execute(self, sql, params=None):
        self.log.append(" ".join(sql.split()))
        if "information_schema" in sql:
            table = params[0]
            has_table, has_column = self.shapes[table]
            self._row = {"has_table": has_table, "has_column": has_column}

    def fetchone(self):
        return self._row


class _Stub:
    _LEGACY_SHAPED_TABLES = PostgresStore._LEGACY_SHAPED_TABLES
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ensure_table = PostgresStore._ensure_table

    def __init__(self, shapes):
        self.shapes, self.log = shapes, []

    @contextmanager
    def _cursor(self):
        yield _Cur(self.shapes, self.log)


def test_a_legacy_shaped_table_is_renamed_aside_and_recreated():
    stub = _Stub({"notification_preferences": (True, False), "push_subscriptions": (True, True)})
    PostgresStore._retire_legacy_blob_tables(stub)
    renames = [s for s in stub.log if s.startswith("ALTER TABLE")]
    assert renames == ["ALTER TABLE notification_preferences RENAME TO notification_preferences_legacy003"]
    assert any("CREATE TABLE IF NOT EXISTS notification_preferences" in s for s in stub.log)
    assert not any(s.startswith("DROP") for s in stub.log)


def test_right_shaped_or_missing_tables_are_left_alone():
    stub = _Stub({"notification_preferences": (False, False), "push_subscriptions": (True, True)})
    PostgresStore._retire_legacy_blob_tables(stub)
    assert not [s for s in stub.log if s.startswith("ALTER TABLE")]
