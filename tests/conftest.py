"""
Pytest configuration and shared fixtures for RosterIQ tests.

Provides MemoryStore instances and test utilities for auth, webhooks,
and notification services.

Key fixtures for the e2e suites:
- ``reset_global_db`` (autouse): resets the global in-memory DB before/after
  every test AND re-binds the module-level ``auth_service`` to the fresh store.
  The auth service is a singleton that captured ``get_db()`` at import time, so
  without re-binding it keeps writing to the previous (pre-reset) store while the
  route dependencies read from the fresh one — which made registration/login
  appear to fail (users "vanished" between calls).
- ``_disable_rate_limiter`` (autouse): neutralises the RateLimiterMiddleware so
  the e2e tests that hammer endpoints don't spuriously hit HTTP 429.
- ``client`` / ``auth_client``: TestClients. ``client`` is unauthenticated;
  ``auth_client`` registers + logs in a user (owner) and sets the
  ``Authorization: Bearer <token>`` header by default so protected endpoints
  (behind TenantMiddleware) are reachable.

Usage:
    pytest tests/ -v
"""

import sys
import os
from datetime import datetime

try:
    import pytest
except ImportError:
    # pytest not available - tests will be reported as import error
    pytest = None

# Ensure rosteriq imports work from the tests directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

if pytest:
    from rosteriq.database import MemoryStore, reset_db, get_db
    from rosteriq.services.auth import AuthService
    from rosteriq.services import auth as _auth_module

    @pytest.fixture
    def memory_store():
        """Create a fresh MemoryStore instance for each test."""
        store = MemoryStore()
        yield store
        # Cleanup is implicit (instance is garbage collected)

    @pytest.fixture
    def auth_service(memory_store):
        """Create an AuthService with MemoryStore backend."""
        service = AuthService(db=memory_store)
        yield service

    def _rebind_stores():
        """
        Re-point every module that captured the DB store at import time to the
        *current* global store.

        reset_db() nulls the global DB instance; the next get_db() builds a
        fresh one. But two modules grabbed the store at import time and would
        otherwise keep reading/writing the previous (pre-reset) store:

          * ``rosteriq.api._db`` — the module-level store behind ``_store`` /
            the ``_StoreProxy`` that every venue/employee/forecast/roster
            endpoint uses. If this isn't re-pointed, created entities survive
            ``reset_db()`` and leak across tests (off-by-N list counts).
          * ``auth_service.db`` — the auth singleton. If not re-pointed,
            registration writes to one store while route dependencies read
            another, so users appear to vanish between calls.

        Re-binding both to a single fresh ``get_db()`` keeps the whole app on
        one store per test.
        """
        fresh = get_db()
        _auth_module.auth_service.db = fresh
        try:
            from rosteriq import api as _api_module
            _api_module._db = fresh
        except Exception:
            pass

    def _clear_response_caches():
        """
        Clear the singleton CacheManager's in-memory TTL caches.

        The /venues, /employees, /forecasts and /rosters list endpoints cache
        their responses in a process-wide CacheManager singleton that reset_db()
        does NOT touch. Without clearing it, a list response cached in one test
        leaks into the next (e.g. stale venue/employee counts), causing
        off-by-N assertion failures.
        """
        try:
            from rosteriq.middleware.cache import get_cache_manager
        except Exception:
            return
        try:
            cm = get_cache_manager()
            for cache in getattr(cm, "caches", {}).values():
                cache.invalidate_all()
        except Exception:
            pass

    @pytest.fixture(autouse=True)
    def reset_global_db():
        """Reset the global database singleton before each test, re-bind the
        auth service so it shares the fresh store, and clear response caches."""
        reset_db()
        _rebind_stores()
        _clear_response_caches()
        yield
        reset_db()
        _rebind_stores()
        _clear_response_caches()

    # ------------------------------------------------------------------
    # Rate limiter handling
    # ------------------------------------------------------------------
    # The e2e tests fire many requests in quick succession from a single
    # TestClient (client.host == "testclient"), which trips the
    # RateLimiterMiddleware token bucket (60/min unauth, 200/min authed) and
    # yields spurious 429s. We can't edit app code, so we neutralise the
    # middleware instance here: monkeypatch its dispatch to a pure passthrough
    # and clear any accumulated buckets. This runs for every test.
    def _find_rate_limiter_middleware(app):
        """Return the live RateLimiterMiddleware instance, if mounted."""
        try:
            from rosteriq.middleware.rate_limiter import RateLimiterMiddleware
        except Exception:
            return None
        # Starlette builds the middleware stack lazily on first request; build
        # it so we can reach the live instance.
        stack = getattr(app, "middleware_stack", None)
        if stack is None:
            stack = app.build_middleware_stack()
            app.middleware_stack = stack
        node = stack
        while node is not None:
            if isinstance(node, RateLimiterMiddleware):
                return node
            node = getattr(node, "app", None)
        return None

    @pytest.fixture(autouse=True)
    def _disable_rate_limiter():
        """Disable the RateLimiterMiddleware for the duration of each test."""
        try:
            from rosteriq.api import app
            from rosteriq.middleware.rate_limiter import RateLimiterMiddleware
        except Exception:
            yield
            return

        async def _passthrough(self, request, call_next):
            return await call_next(request)

        original = RateLimiterMiddleware.dispatch
        # Patch at the class level so any (re)built middleware instance is inert.
        RateLimiterMiddleware.dispatch = _passthrough

        # Also clear buckets on any already-built instance for good measure.
        mw = _find_rate_limiter_middleware(app)
        if mw is not None and hasattr(mw, "buckets"):
            mw.buckets.clear()

        try:
            yield
        finally:
            RateLimiterMiddleware.dispatch = original

    # ------------------------------------------------------------------
    # HTTP clients
    # ------------------------------------------------------------------
    @pytest.fixture
    def client():
        """Unauthenticated TestClient for the FastAPI app."""
        from fastapi.testclient import TestClient
        from rosteriq.api import app
        return TestClient(app)

    def _register_and_login(test_client, email="e2e-owner@example.com",
                            password="SecurePassword123!", name="E2E Owner"):
        """Register a user (first user => owner) and return its access token."""
        reg = test_client.post(
            "/api/auth/register",
            json={"email": email, "password": password, "name": name},
        )
        # 201 on fresh register; 409 if a prior test in the same store already
        # created it (shouldn't happen with autouse reset, but be defensive).
        if reg.status_code == 201:
            return reg.json()["tokens"]["access_token"]

        login = test_client.post(
            "/api/auth/login",
            json={"email": email, "password": password},
        )
        login.raise_for_status()
        return login.json()["access_token"]

    @pytest.fixture
    def auth_client():
        """
        TestClient with a registered+logged-in owner user whose bearer token is
        sent by default on every request. Use for endpoints behind
        TenantMiddleware (venues, employees, forecasts, rosters, etc.).
        """
        from fastapi.testclient import TestClient
        from rosteriq.api import app

        c = TestClient(app)
        token = _register_and_login(c)
        c.headers.update({"Authorization": f"Bearer {token}"})
        return c


@pytest.fixture(autouse=True)
def _tests_speak_one_today(request, monkeypatch):
    """Make "today" mean ONE thing across the suite, at any hour of the day.

    The suite runs under TZ=UTC while every real venue clock is UTC+8..+11
    (services/clock.py, DEFAULT_TZ Australia/Perth). Between 16:00 UTC and
    midnight UTC the venue's date is TOMORROW relative to the tests'
    date.today() — eight hours of every day in which ~35 date-sensitive tests
    failed (a sale recorded venue-locally on the 26th, asserted against a
    window built for the 25th). Product code stays venue-local; the tests
    align the DEFAULT venue timezone with the harness clock instead.

    Venue-SPECIFIC timezone behaviour keeps its real coverage in
    test_venue_clock.py, which freezes the clock and sets explicit
    VenueConfig timezones — that module is exempt here.
    """
    if "test_venue_clock" in request.node.nodeid:
        yield
        return
    from rosteriq.services import clock
    # Patch the resolver itself, not just DEFAULT_TZ: VenueConfig.timezone
    # DEFAULTS to Australia/Melbourne, so every test venue carries an explicit
    # timezone and the DEFAULT_TZ fallback is never consulted.
    monkeypatch.setattr(clock, "venue_timezone", lambda venue_id, db=None: "UTC")
    clock._tz_cache.clear()          # a cached zone from import time must not leak
    yield
    clock._tz_cache.clear()
