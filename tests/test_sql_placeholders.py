"""
Every PostgresStore INSERT binds as many placeholders as it names columns.

MemoryStore never runs this SQL, so a column added to an insert without its
%s (easy to do when two changes to the same statement are merged) only fails
on Postgres — on every save through that statement.
"""

import inspect
import re

from rosteriq import database


def _inserts():
    src = inspect.getsource(database)
    for m in re.finditer(r"INSERT INTO (\w+)\s*\(([^)]*)\)\s*VALUES\s*\(([^)]*)\)", src):
        cols = [c for c in m.group(2).split(",") if c.strip()]
        vals = [v for v in m.group(3).split(",") if v.strip()]
        yield m.group(1), src[:m.start()].count("\n") + 1, cols, vals


def test_every_insert_binds_one_value_per_column():
    checked = 0
    for table, line, cols, vals in _inserts():
        assert len(cols) == len(vals), f"INSERT INTO {table} (database.py:{line}): {len(cols)} columns, {len(vals)} values"
        checked += 1
    assert checked > 20
