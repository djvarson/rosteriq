"""Tests for Tanda webhook receiver: signature verification, event parsing, storage, dispatch."""
import asyncio
import hashlib
import hmac
from datetime import datetime, timezone

import pytest

from rosteriq.tanda_webhooks import (
    TandaWebhookEvent,
    TandaWebhookStore,
    get_webhook_store,
    parse_tanda_event,
    register_handler,
    verify_tanda_signature,
    dispatch,
)


# ---------------------------------------------------------------------------
# Signature verification tests
# ---------------------------------------------------------------------------


def test_verify_tanda_signature_valid():
    """Valid HMAC-SHA256 signature passes."""
    secret = "test-secret-key"
    payload = b'{"event_type":"shift.published","org_id":"org123"}'

    expected_sig = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()

    assert verify_tanda_signature(expected_sig, payload, secret) is True


def test_verify_tanda_signature_invalid():
    """Tampered signature fails."""
    secret = "test-secret-key"
    payload = b'{"event_type":"shift.published","org_id":"org123"}'

    # Correct sig
    correct_sig = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()

    # Tampered sig (flip last char)
    tampered_sig = correct_sig[:-1] + ("0" if correct_sig[-1] != "0" else "1")

    assert verify_tanda_signature(tampered_sig, payload, secret) is False


def test_verify_tanda_signature_wrong_secret():
    """Wrong secret fails."""
    secret = "test-secret-key"
    wrong_secret = "wrong-secret"
    payload = b'{"event_type":"shift.published","org_id":"org123"}'

    expected_sig = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()

    assert verify_tanda_signature(expected_sig, payload, wrong_secret) is False


# ---------------------------------------------------------------------------
# Event parsing tests
# ---------------------------------------------------------------------------


def test_parse_tanda_event_wrapped_data():
    """Parses event with data nested under 'data' key."""
    payload = {
        "event_type": "shift.published",
        "org_id": "org123",
        "occurred_at": "2026-04-15T10:30:00+00:00",
        "data": {"shift_id": "shift456", "status": "published"},
    }

    event = parse_tanda_event(payload)

    assert event.event_type == "shift.published"
    assert event.org_id == "org123"
    assert event.data == {"shift_id": "shift456", "status": "published"}
    assert event.occurred_at.year == 2026


def test_parse_tanda_event_payload_key():
    """Parses event with data nested under 'payload' key."""
    payload = {
        "event_type": "employee.updated",
        "org_id": "org789",
        "payload": {"employee_id": "emp123", "name": "Alice"},
    }

    event = parse_tanda_event(payload)

    assert event.event_type == "employee.updated"
    assert event.org_id == "org789"
    assert event.data == {"employee_id": "emp123", "name": "Alice"}


def test_parse_tanda_event_flat():
    """Parses event with flat payload (no nested data key)."""
    payload = {
        "event_type": "shift.updated",
        "org_id": "org999",
        "shift_id": "shift111",
        "old_status": "draft",
        "new_status": "published",
    }

    event = parse_tanda_event(payload)

    assert event.event_type == "shift.updated"
    assert event.org_id == "org999"
    # Flat fields extracted as data
    assert "shift_id" in event.data
    assert event.data["shift_id"] == "shift111"


def test_parse_tanda_event_missing_event_type():
    """Missing event_type raises ValueError."""
    payload = {"org_id": "org123", "data": {}}

    with pytest.raises(ValueError, match="Missing required fields"):
        parse_tanda_event(payload)


def test_parse_tanda_event_missing_org_id():
    """Missing org_id raises ValueError."""
    payload = {"event_type": "shift.published", "data": {}}

    with pytest.raises(ValueError, match="Missing required fields"):
        parse_tanda_event(payload)


def test_parse_tanda_event_defaults_occurred_at():
    """Defaults occurred_at to now if missing."""
    before = datetime.now(tz=timezone.utc)
    payload = {
        "event_type": "shift.published",
        "org_id": "org123",
        "data": {},
    }

    event = parse_tanda_event(payload)
    after = datetime.now(tz=timezone.utc)

    assert before <= event.occurred_at <= after


def test_parse_tanda_event_org_id_alternate_key():
    """Accepts 'organisation_id' as alternative to 'org_id'."""
    payload = {
        "event_type": "shift.published",
        "organisation_id": "org456",
        "data": {},
    }

    event = parse_tanda_event(payload)

    assert event.org_id == "org456"


# ---------------------------------------------------------------------------
# Webhook store tests
# ---------------------------------------------------------------------------


def test_webhook_store_append_and_list():
    """Store appends event and lists for org."""
    store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)

    event1 = TandaWebhookEvent(
        event_id="evt1",
        event_type="shift.published",
        org_id="org123",
        occurred_at=datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc),
        data={"shift_id": "s1"},
        raw={},
    )
    event2 = TandaWebhookEvent(
        event_id="evt2",
        event_type="shift.updated",
        org_id="org123",
        occurred_at=datetime(2026, 4, 15, 11, 0, tzinfo=timezone.utc),
        data={"shift_id": "s2"},
        raw={},
    )

    store.append(event1)
    store.append(event2)

    events = store.list_for_org("org123")

    assert len(events) == 2
    # Newest first
    assert events[0].event_id == "evt2"
    assert events[1].event_id == "evt1"


def test_webhook_store_list_limit():
    """list_for_org respects limit."""
    store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)

    for i in range(10):
        event = TandaWebhookEvent(
            event_id=f"evt{i}",
            event_type="shift.published",
            org_id="org123",
            occurred_at=datetime(2026, 4, 15, 10 + i, tzinfo=timezone.utc),
            data={},
            raw={},
        )
        store.append(event)

    events = store.list_for_org("org123", limit=3)

    assert len(events) == 3
    # Newest first
    assert events[0].event_id == "evt9"


def test_webhook_store_separate_orgs():
    """Store keeps events separate by org."""
    store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)

    event1 = TandaWebhookEvent(
        event_id="evt1",
        event_type="shift.published",
        org_id="org_a",
        occurred_at=datetime.now(tz=timezone.utc),
        data={},
        raw={},
    )
    event2 = TandaWebhookEvent(
        event_id="evt2",
        event_type="shift.published",
        org_id="org_b",
        occurred_at=datetime.now(tz=timezone.utc),
        data={},
        raw={},
    )

    store.append(event1)
    store.append(event2)

    org_a_events = store.list_for_org("org_a")
    org_b_events = store.list_for_org("org_b")

    assert len(org_a_events) == 1
    assert len(org_b_events) == 1
    assert org_a_events[0].event_id == "evt1"
    assert org_b_events[0].event_id == "evt2"


def test_webhook_store_get_event():
    """get_event finds event by ID across orgs."""
    store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)

    event = TandaWebhookEvent(
        event_id="evt_unique",
        event_type="shift.published",
        org_id="org123",
        occurred_at=datetime.now(tz=timezone.utc),
        data={},
        raw={},
    )

    store.append(event)

    found = store.get_event("evt_unique")

    assert found is not None
    assert found.event_id == "evt_unique"


def test_webhook_store_get_event_not_found():
    """get_event returns None if not found."""
    store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)

    found = store.get_event("nonexistent")

    assert found is None


def test_webhook_store_clear():
    """clear removes all events."""
    store = TandaWebhookStore(max_events_per_org=100, ttl_hours=24)

    event = TandaWebhookEvent(
        event_id="evt1",
        event_type="shift.published",
        org_id="org123",
        occurred_at=datetime.now(tz=timezone.utc),
        data={},
        raw={},
    )

    store.append(event)
    assert len(store.list_for_org("org123")) == 1

    store.clear()
    assert len(store.list_for_org("org123")) == 0


# ---------------------------------------------------------------------------
# Event dispatcher tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_stores_event():
    """dispatch appends event to store."""
    store = get_webhook_store()
    store.clear()

    event = TandaWebhookEvent(
        event_id="evt_dispatch_1",
        event_type="shift.published",
        org_id="org_test",
        occurred_at=datetime.now(tz=timezone.utc),
        data={"test": "data"},
        raw={},
    )

    await dispatch(event)

    stored_events = store.list_for_org("org_test")
    assert len(stored_events) == 1
    assert stored_events[0].event_id == "evt_dispatch_1"


@pytest.mark.asyncio
async def test_dispatch_calls_registered_handler():
    """dispatch calls registered handlers."""
    from rosteriq.tanda_webhooks import (
        _handlers,
    )  # Access internal registry for test

    store = get_webhook_store()
    store.clear()
    _handlers.clear()

    handled_events = []

    async def test_handler(event):
        handled_events.append(event)

    register_handler("shift.published", test_handler)

    event = TandaWebhookEvent(
        event_id="evt_handler_1",
        event_type="shift.published",
        org_id="org_test",
        occurred_at=datetime.now(tz=timezone.utc),
        data={},
        raw={},
    )

    await dispatch(event)

    assert len(handled_events) == 1
    assert handled_events[0].event_id == "evt_handler_1"

    _handlers.clear()


@pytest.mark.asyncio
async def test_dispatch_handler_error_logged():
    """dispatch logs handler errors and continues."""
    from rosteriq.tanda_webhooks import (
        _handlers,
    )  # Access internal registry for test

    store = get_webhook_store()
    store.clear()
    _handlers.clear()

    async def failing_handler(event):
        raise RuntimeError("Test handler error")

    register_handler("shift.updated", failing_handler)

    event = TandaWebhookEvent(
        event_id="evt_error_1",
        event_type="shift.updated",
        org_id="org_test",
        occurred_at=datetime.now(tz=timezone.utc),
        data={},
        raw={},
    )

    # Should not raise
    await dispatch(event)

    # Event should still be stored
    stored = store.list_for_org("org_test")
    assert len(stored) == 1

    _handlers.clear()


# ---------------------------------------------------------------------------
# Singleton tests
# ---------------------------------------------------------------------------


def test_get_webhook_store_singleton():
    """get_webhook_store returns same instance on repeated calls."""
    store1 = get_webhook_store()
    store2 = get_webhook_store()

    assert store1 is store2
