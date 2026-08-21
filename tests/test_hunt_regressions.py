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


# ---------------------------------------------------------------------------
# Sales import must be all-or-nothing
# ---------------------------------------------------------------------------

def test_a_failing_line_records_nothing_at_all():
    """Each save_dish_sale is its own committed statement, so a mid-loop error
    used to leave the earlier rows persisted with no batch fingerprint. The
    obvious re-upload then counted them twice — real money, wrong."""
    from rosteriq.routes import dish_sales as ds

    saved = []

    class _DB:
        def list_ingredients(self, vid):
            return [{"id": "milk", "name": "Milk", "unit": "carton", "cost_per_unit": 4.0}]

        def save_dish_sale(self, row):
            saved.append(row)

        def increment_ingredient_stock(self, *a):
            pass

        def get_ingredient(self, i):
            return None

    good = {"id": "r-ok", "name": "Toast", "yield_portions": 1, "sell_price_inc_gst": 6.0,
            "items": []}
    # second line's unit cannot convert -> raises while pricing
    bad = {"id": "r-bad", "name": "Latte", "yield_portions": 1, "sell_price_inc_gst": 5.0,
           "items": [{"ingredient_id": "milk", "qty": 150, "unit": "ml"}]}

    with pytest.raises(Exception):
        ds._record_lines(_DB(), "v1", date(2026, 8, 20), [(good, 3), (bad, 2)], "pos_import")
    assert saved == [], f"{len(saved)} row(s) were persisted before the failure"


def test_import_claims_its_fingerprint_before_recording():
    """Claim-then-record, not record-then-claim: the claim is what makes a
    re-upload a clean 409 instead of a second helping of revenue."""
    from rosteriq.database import MemoryStore

    db = MemoryStore()
    batch = {"id": "ib-abc", "venue_id": "v1", "sale_date": date(2026, 8, 20),
             "row_count": 3, "revenue": 0.0, "imported_at": datetime(2026, 8, 20)}
    assert db.claim_import_batch(batch) is True     # first caller wins
    assert db.claim_import_batch(batch) is False    # second is refused
    db.delete_import_batch("ib-abc")
    assert db.claim_import_batch(batch) is True     # released after a failure


# ---------------------------------------------------------------------------
# Tanda: an unreadable roster is not "nothing to do"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_aborts_when_the_tanda_roster_cannot_be_read():
    """diff_roster swallowed the fetch error and returned an EMPTY diff, so a
    Tanda outage produced a push that wrote nothing and reported success."""
    from rosteriq.services.tanda_roster_push import TandaRosterPush, RosterDiff
    from rosteriq.models import Roster

    pusher = TandaRosterPush.__new__(TandaRosterPush)

    async def _diff(roster, venue_id):
        return RosterDiff(fetch_failed=True, fetch_error="503 from Tanda")

    async def _valid(emp_id):
        return True

    pusher.diff_roster = _diff
    pusher._validate_employee_mapping = _valid

    roster = Roster(id="r1", venue_id="v1", week_start=date(2026, 8, 17),
                    week_end=date(2026, 8, 23), shifts=[], created_at=datetime(2026, 8, 1))
    result = await pusher.push_roster(roster, venue_id="v1", dry_run=False)

    assert result.failed_count >= 1, "an unreadable roster looked like a clean push"
    assert any("Could not read" in e for e in result.errors), result.errors


# ---------------------------------------------------------------------------
# Webhook queue: two workers must not deliver the same row
# ---------------------------------------------------------------------------

def test_polling_claims_deliveries_so_a_second_worker_gets_nothing():
    """Production runs two uvicorn workers, each with its own queue loop. As a
    plain SELECT both got the same row and — because the POST happens before
    the status is written back — both delivered it."""
    from rosteriq.database import MemoryStore

    db = MemoryStore()
    now = datetime(2026, 8, 20, 12, 0, 0)
    db.save_webhook_delivery({
        "id": "dlv-1", "subscription_id": "s1", "venue_id": "v1",
        "event_type": "roster.published", "url": "https://example/hook",
        "status": "pending", "attempt": 1,
        "next_retry_at": (now - timedelta(minutes=1)).isoformat(),
    })

    first = db.list_pending_retries(now)
    second = db.list_pending_retries(now)          # the other worker's poll
    assert [d["id"] for d in first] == ["dlv-1"]
    assert second == [], "the same delivery was handed to a second poller"
