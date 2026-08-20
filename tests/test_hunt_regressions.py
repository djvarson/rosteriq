"""
Regressions from the adversarial bug hunt. Each test names a concrete failure
a venue would have hit, and each failed before its fix.

Not covered here (verified by reading, no test harness for them):
* Postgres-only breakage — the JSONB/column bugs (save_approval_request,
  webhook_events.id, employees.anonymised_at, roster_templates.shift_patterns).
  MemoryStore cannot express them; they are guarded by the schema-guard test
  and verified live on prod after deploy.
"""

from datetime import date, datetime, time, timedelta

import pytest

from rosteriq.models import Shift, ShiftStatus


# ---------------------------------------------------------------------------
# Tanda: month-end overnight shift
# ---------------------------------------------------------------------------

def test_overnight_shift_on_the_last_day_of_a_month_still_builds():
    """31 Aug 18:00-02:00: replace(day=32) raised ValueError, so every
    month-end overnight shift silently failed to reach Tanda."""
    from rosteriq.services.tanda_roster_push import TandaRosterPush

    pusher = TandaRosterPush.__new__(TandaRosterPush)
    shift = Shift(
        id="s1", employee_id="e1", date=date(2026, 8, 31),
        start_time=time(18, 0), end_time=time(2, 0), break_minutes=0,
        status=ShiftStatus.scheduled, role="bar",
    )
    payload = pusher._build_tanda_shift_payload(
        shift, tanda_user_id="42", tanda_roster_id="7")
    assert payload, "no payload built"
    # The end must roll into the next month rather than raising day=32.
    assert "2026-09-01" in str(payload), payload


# ---------------------------------------------------------------------------
# Roster publish for Tanda-connected venues
# ---------------------------------------------------------------------------

def test_tanda_push_summary_reads_the_fields_pushresult_actually_has():
    """publish read .success/.message/.shifts_pushed, none of which exist on
    PushResult — so publishing ALWAYS failed for a Tanda-connected venue."""
    from rosteriq.services.roster_publisher import _tanda_push_summary
    from rosteriq.services.tanda_roster_push import PushResult

    ok = _tanda_push_summary(PushResult(success_count=7, failed_count=0))
    assert ok == {"success": True, "message": "Pushed to Tanda",
                  "shifts_pushed": 7, "shifts_failed": 0}

    bad = _tanda_push_summary(PushResult(success_count=2, failed_count=1,
                                         errors=["shift 9 rejected"]))
    assert bad["success"] is False
    assert bad["shifts_pushed"] == 2 and bad["shifts_failed"] == 1
    assert "shift 9 rejected" in bad["message"]


# ---------------------------------------------------------------------------
# Menu costing survives one unconvertible line
# ---------------------------------------------------------------------------

def test_one_bad_unit_does_not_422_the_whole_menu():
    """A venue switching milk from litres to cartons used to 422 the entire
    costing screen — every dish, permanently, until the data was hand-fixed."""
    from rosteriq.routes.menu_costing import _cost_recipe

    ingredients = {
        "milk": {"id": "milk", "name": "Milk", "unit": "carton", "cost_per_unit": 4.0},
        "beans": {"id": "beans", "name": "Beans", "unit": "kg", "cost_per_unit": 30.0},
    }
    recipe = {
        "id": "r1", "name": "Flat White", "yield_portions": 1,
        "sell_price_inc_gst": 5.5,
        "items": [
            {"ingredient_id": "milk", "qty": 150, "unit": "ml"},   # ml -> carton: no
            {"ingredient_id": "beans", "qty": 0.02, "unit": "kg"},  # fine
        ],
    }
    out = _cost_recipe(recipe, ingredients)          # must not raise
    assert out["cost_incomplete"] is True
    assert out["unit_errors"] and out["unit_errors"][0]["ingredient_id"] == "milk"
    # the convertible line still costs
    assert out["cost_per_portion"] == pytest.approx(0.6, abs=0.01)
    bad_line = [l for l in out["lines"] if l["ingredient_id"] == "milk"][0]
    assert bad_line["line_cost"] is None and bad_line["unit_error"]


# ---------------------------------------------------------------------------
# MYOB: never retry a write that may already have landed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_myob_does_not_retry_a_post_that_timed_out_mid_flight():
    """A POST that timed out while reading the response may have created the
    bill. Retrying it makes a second one, and the push ledger is only written
    after success so nothing downstream notices."""
    import httpx
    from rosteriq.myob_adapter import MYOBAdapter, MYOBAPIError

    adapter = MYOBAdapter.__new__(MYOBAdapter)
    calls = []

    class _Client:
        async def request(self, method, url, **kw):
            calls.append((method, url))
            raise httpx.ReadTimeout("timed out reading response")

    class _Limiter:
        async def acquire(self):
            return None

    adapter._client = _Client()
    adapter._rate_limiter = _Limiter()

    async def _noop():
        return None
    adapter._ensure_valid_token = _noop

    with pytest.raises(MYOBAPIError) as ei:
        await adapter._request_inner("POST", "/Purchase/Bill/Service", json={"x": 1})
    assert len(calls) == 1, f"POST was retried {len(calls)} times after a mid-flight timeout"
    assert "may have been applied" in str(ei.value.api_error.message)


@pytest.mark.asyncio
async def test_myob_still_retries_a_get_and_a_failed_connection():
    """Reads, and writes that never left the machine, are safe to retry."""
    import httpx
    from rosteriq.myob_adapter import MYOBAdapter, MYOBAPIError

    for method, exc in (("GET", httpx.ReadTimeout("slow")),
                        ("POST", httpx.ConnectError("refused"))):
        adapter = MYOBAdapter.__new__(MYOBAdapter)
        calls = []

        class _Client:
            async def request(self, m, url, **kw):
                calls.append(m)
                raise exc

        class _Limiter:
            async def acquire(self):
                return None

        adapter._client = _Client()
        adapter._rate_limiter = _Limiter()

        async def _noop():
            return None
        adapter._ensure_valid_token = _noop

        with pytest.raises(MYOBAPIError):
            await adapter._request_inner(method, "/Contact/Supplier")
        assert len(calls) > 1, f"{method} with {type(exc).__name__} should have retried"


# ---------------------------------------------------------------------------
# Webhook dead-letter must not be redelivered forever
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dead_lettered_webhook_is_marked_terminal_on_the_source_row():
    """The DLQ write alone leaves webhook_deliveries at status='pending' with
    next_retry_at in the past, so the poller hands it back on every pass — a
    decommissioned endpoint spins the queue worker forever."""
    from rosteriq.services.webhook_queue import WebhookRetryQueue

    saved = {}

    class _DB:
        def save_dead_letter(self, d):
            saved["dead_letter"] = dict(d)

        def save_webhook_delivery(self, d):
            saved["delivery"] = dict(d)

    q = WebhookRetryQueue.__new__(WebhookRetryQueue)
    q.db = _DB()
    await q._move_to_deadletter({"id": "wh-1", "status": "pending", "url": "https://gone"})

    assert saved["dead_letter"]["status"] == "dead_letter"
    assert "delivery" in saved, "source row was never updated — it stays pending forever"
    assert saved["delivery"]["status"] == "dead_letter"


@pytest.mark.asyncio
async def test_dead_letter_survives_a_failing_source_update():
    """The DLQ record is what matters; a failure persisting the source row must
    not lose it."""
    from rosteriq.services.webhook_queue import WebhookRetryQueue

    saved = {}

    class _DB:
        def save_dead_letter(self, d):
            saved["dead_letter"] = dict(d)

        def save_webhook_delivery(self, d):
            raise RuntimeError("db down")

    q = WebhookRetryQueue.__new__(WebhookRetryQueue)
    q.db = _DB()
    await q._move_to_deadletter({"id": "wh-2", "status": "pending"})   # must not raise
    assert saved["dead_letter"]["status"] == "dead_letter"
