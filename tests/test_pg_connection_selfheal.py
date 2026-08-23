"""
The shared PostgreSQL connection must survive being dropped by the server.

PostgresStore holds ONE psycopg2 connection for the life of the process, with
no pool and (before this) no reconnect. When Railway's Postgres restarted
overnight on 2026-08-23, that connection died and every subsequent query in
both workers failed forever — demo login, registration, ordinary reads, all
500 until a redeploy. /health kept returning 200 throughout, because it never
touches this connection.

These tests drive _cursor() with fake connections to pin the healing contract:
a closed connection is replaced before a cursor is handed out, a socket that
dies without marking itself closed gets one retry on a fresh connection, and a
healthy connection is left alone.
"""

import sys
import types

import psycopg2
import pytest

from rosteriq.database import PostgresStore


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn


class FakeConn:
    def __init__(self, closed=0, raise_on_cursor=None):
        self.closed = closed
        self.raise_on_cursor = raise_on_cursor
        self.close_calls = 0
        self.autocommit = False

    def cursor(self, cursor_factory=None):
        if self.raise_on_cursor is not None:
            raise self.raise_on_cursor
        return FakeCursor(self)

    def close(self):
        self.close_calls += 1
        self.closed = 1


@pytest.fixture
def store(monkeypatch):
    """A PostgresStore skeleton with a controllable psycopg2.connect."""
    st = PostgresStore.__new__(PostgresStore)      # skip __init__ (no real DB)
    st._dsn = "postgres://fake/db"
    made = []

    def fake_connect(dsn):
        assert dsn == st._dsn
        conn = FakeConn()
        made.append(conn)
        return conn

    monkeypatch.setattr(psycopg2, "connect", fake_connect)
    return st, made


def test_healthy_connection_is_left_alone(store):
    st, made = store
    healthy = FakeConn()
    st._conn = healthy
    cur = st._cursor()
    assert cur.conn is healthy
    assert made == []                     # no reconnect happened


def test_closed_connection_is_replaced_before_use(store):
    """The exact prod failure: server dropped the conn, psycopg2 marked it
    closed, and every later query used to fail forever."""
    st, made = store
    st._conn = FakeConn(closed=2)
    cur = st._cursor()
    assert len(made) == 1
    assert cur.conn is made[0]
    assert made[0].autocommit is True     # reconnect keeps the same semantics


def test_dead_socket_not_marked_closed_gets_one_retry(store):
    """closed == 0 but the socket is gone: cursor() raises InterfaceError.
    One retry on a fresh connection, not an infinite loop."""
    st, made = store
    st._conn = FakeConn(closed=0, raise_on_cursor=psycopg2.InterfaceError("connection already closed"))
    cur = st._cursor()
    assert len(made) == 1
    assert cur.conn is made[0]


def test_missing_connection_attribute_reconnects(store):
    st, made = store
    # a store that lost its connection entirely (e.g. failed boot path)
    if hasattr(st, "_conn"):
        del st._conn
    cur = st._cursor()
    assert len(made) == 1 and cur.conn is made[0]


def test_reconnect_failure_propagates(store, monkeypatch):
    """If Postgres is truly down, the caller sees the real error — no silent
    retry loop hiding an outage."""
    st, _ = store
    st._conn = FakeConn(closed=2)
    monkeypatch.setattr(psycopg2, "connect",
                        lambda dsn: (_ for _ in ()).throw(psycopg2.OperationalError("still down")))
    with pytest.raises(psycopg2.OperationalError):
        st._cursor()
