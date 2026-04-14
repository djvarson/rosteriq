"""Tests for call-in SMS flow: format, parse, store, service, inbound handling."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from rosteriq.call_in import (
    CallInRequest,
    CallInStatus,
    CallInService,
    CallInStore,
    DemoSMSProvider,
    TwilioSMSProvider,
    format_call_in_message,
    get_service,
    get_store,
    parse_reply,
    reset_singletons,
)


# ---------------------------------------------------------------------------
# format_call_in_message tests
# ---------------------------------------------------------------------------


def test_format_call_in_message_evening_shift():
    """Evening shift produces lowercase am/pm, includes name, times, and is under 160 chars."""
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)  # 6pm
    end = datetime(2026, 4, 15, 23, 59, tzinfo=timezone.utc)  # close
    msg = format_call_in_message("Alex", start, end, role="bar", venue_name="The Marina")
    assert "Alex" in msg
    assert "6pm" in msg
    assert "close" in msg or "11pm" in msg
    assert "bar" in msg
    assert "Marina" in msg
    assert len(msg) <= 160
    assert "am" in msg or "pm" in msg  # lowercase
    assert "AM" not in msg and "PM" not in msg


def test_format_call_in_message_no_role_no_venue():
    """Message works without optional role and venue."""
    start = datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 17, 0, tzinfo=timezone.utc)
    msg = format_call_in_message("Jamie", start, end)
    assert "Jamie" in msg
    assert "9am" in msg
    assert "5pm" in msg
    assert len(msg) <= 160


def test_format_call_in_message_early_morning():
    """Early morning shift formats correctly."""
    start = datetime(2026, 4, 15, 6, 30, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 14, 30, tzinfo=timezone.utc)
    msg = format_call_in_message("Sam", start, end)
    assert "6am" in msg
    assert "2pm" in msg


# ---------------------------------------------------------------------------
# parse_reply tests
# ---------------------------------------------------------------------------


def test_parse_reply_accept_yes():
    """'yes' → ACCEPTED."""
    assert parse_reply("yes") == CallInStatus.ACCEPTED


def test_parse_reply_accept_y():
    """'y' → ACCEPTED."""
    assert parse_reply("y") == CallInStatus.ACCEPTED


def test_parse_reply_accept_yep():
    """'yep' → ACCEPTED."""
    assert parse_reply("yep") == CallInStatus.ACCEPTED


def test_parse_reply_accept_sure():
    """'sure' → ACCEPTED."""
    assert parse_reply("sure") == CallInStatus.ACCEPTED


def test_parse_reply_accept_in():
    """'in' → ACCEPTED."""
    assert parse_reply("in") == CallInStatus.ACCEPTED


def test_parse_reply_accept_emoji():
    """'👍' → ACCEPTED."""
    assert parse_reply("👍") == CallInStatus.ACCEPTED


def test_parse_reply_accept_case_insensitive():
    """'YES', 'Yes', 'YES!' → ACCEPTED."""
    assert parse_reply("YES") == CallInStatus.ACCEPTED
    assert parse_reply("Yes") == CallInStatus.ACCEPTED
    assert parse_reply("YES!") == CallInStatus.ACCEPTED


def test_parse_reply_decline_no():
    """'no' → DECLINED."""
    assert parse_reply("no") == CallInStatus.DECLINED


def test_parse_reply_decline_n():
    """'n' → DECLINED."""
    assert parse_reply("n") == CallInStatus.DECLINED


def test_parse_reply_decline_cant():
    """'can't', 'cant' → DECLINED."""
    assert parse_reply("can't") == CallInStatus.DECLINED
    assert parse_reply("cant") == CallInStatus.DECLINED


def test_parse_reply_decline_sorry():
    """'sorry' → DECLINED."""
    assert parse_reply("sorry") == CallInStatus.DECLINED


def test_parse_reply_decline_case_insensitive():
    """'NO', 'No', 'NO!' → DECLINED."""
    assert parse_reply("NO") == CallInStatus.DECLINED
    assert parse_reply("No") == CallInStatus.DECLINED
    assert parse_reply("NO!") == CallInStatus.DECLINED


def test_parse_reply_garbage():
    """Unrecognized reply → None."""
    assert parse_reply("maybe") is None
    assert parse_reply("later") is None
    assert parse_reply("xyz") is None


def test_parse_reply_empty_string():
    """Empty string → None."""
    assert parse_reply("") is None


def test_parse_reply_whitespace():
    """Whitespace-only → None after strip."""
    assert parse_reply("   ") is None


# ---------------------------------------------------------------------------
# DemoSMSProvider tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_demo_sms_provider_send():
    """DemoSMSProvider.send returns a receipt with queued status."""
    provider = DemoSMSProvider()
    receipt = await provider.send("+61412345678", "Hello")
    assert receipt["status"] == "queued"
    assert receipt["to"] == "+61412345678"
    assert receipt["body"] == "Hello"
    assert "id" in receipt


# ---------------------------------------------------------------------------
# TwilioSMSProvider tests
# ---------------------------------------------------------------------------


def test_twilio_sms_provider_no_credentials():
    """TwilioSMSProvider raises ValueError if env vars missing."""
    # Clear any existing env vars
    import os
    old_sid = os.environ.pop("TWILIO_ACCOUNT_SID", None)
    old_token = os.environ.pop("TWILIO_AUTH_TOKEN", None)
    old_from = os.environ.pop("TWILIO_FROM", None)

    try:
        with pytest.raises(ValueError):
            TwilioSMSProvider()
    finally:
        # Restore
        if old_sid:
            os.environ["TWILIO_ACCOUNT_SID"] = old_sid
        if old_token:
            os.environ["TWILIO_AUTH_TOKEN"] = old_token
        if old_from:
            os.environ["TWILIO_FROM"] = old_from


# ---------------------------------------------------------------------------
# CallInStore tests
# ---------------------------------------------------------------------------


def test_call_in_store_create():
    """Store.create() initializes a request in PENDING state."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
        role="bar",
    )

    assert req.status == CallInStatus.PENDING
    assert req.employee_name == "Alex"
    assert req.phone == "+61412345678"
    assert req.sent_at is None


def test_call_in_store_get():
    """Store.get() retrieves a request by ID."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req1 = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )

    req2 = store.get(req1.request_id)
    assert req2 is not None
    assert req2.request_id == req1.request_id
    assert req2.employee_name == "Alex"


def test_call_in_store_get_missing():
    """Store.get() returns None for missing ID."""
    store = CallInStore()
    assert store.get("nonexistent") is None


def test_call_in_store_list_for_venue():
    """Store.list_for_venue() returns requests sorted newest first."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req1 = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )
    # Slight delay to ensure different timestamps
    import time
    time.sleep(0.01)

    req2 = store.create(
        venue_id="venue_1",
        employee_id="emp_2",
        employee_name="Jamie",
        phone="+61487654321",
        shift_start=start,
        shift_end=end,
    )

    reqs = store.list_for_venue("venue_1")
    assert len(reqs) == 2
    assert reqs[0].request_id == req2.request_id  # newest first
    assert reqs[1].request_id == req1.request_id


def test_call_in_store_list_empty_venue():
    """Store.list_for_venue() returns empty list for unknown venue."""
    store = CallInStore()
    assert store.list_for_venue("unknown") == []


def test_call_in_store_update_status():
    """Store.update_status() changes status and updates timestamp."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )

    req_updated = store.update_status(
        req.request_id, CallInStatus.ACCEPTED, response_text="yes"
    )
    assert req_updated.status == CallInStatus.ACCEPTED
    assert req_updated.response_text == "yes"
    assert req_updated.responded_at is not None


def test_call_in_store_mark_sent():
    """Store.mark_sent() sets status to SENT and sent_at."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )

    req_marked = store.mark_sent(req.request_id, {"id": "sms_123"})
    assert req_marked.status == CallInStatus.SENT
    assert req_marked.sent_at is not None


def test_call_in_store_expire_stale():
    """Store.expire_stale() marks old PENDING/SENT requests as EXPIRED."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    # Create an old request (manually backdate it)
    req_old = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )
    req_old.created_at = datetime.now(timezone.utc) - timedelta(hours=2)

    # Create a recent request
    req_recent = store.create(
        venue_id="venue_1",
        employee_id="emp_2",
        employee_name="Jamie",
        phone="+61487654321",
        shift_start=start,
        shift_end=end,
    )

    # Expire stale (60 min default)
    count = store.expire_stale(max_age_minutes=60)

    assert count == 1
    assert store.get(req_old.request_id).status == CallInStatus.EXPIRED
    assert store.get(req_recent.request_id).status == CallInStatus.PENDING


def test_call_in_store_expire_stale_leaves_accepted():
    """Store.expire_stale() ignores ACCEPTED/DECLINED requests."""
    store = CallInStore()
    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = store.create(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )
    store.update_status(req.request_id, CallInStatus.ACCEPTED)
    req.created_at = datetime.now(timezone.utc) - timedelta(hours=2)

    count = store.expire_stale(max_age_minutes=60)

    assert count == 0
    assert store.get(req.request_id).status == CallInStatus.ACCEPTED


# ---------------------------------------------------------------------------
# CallInService tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_call_in_service_create_and_send():
    """Service.create_and_send() creates request, formats message, sends via provider."""
    store = CallInStore()
    provider = DemoSMSProvider()
    service = CallInService(store, provider)

    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = await service.create_and_send(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
        role="bar",
        venue_name="The Marina",
    )

    assert req.status == CallInStatus.SENT
    assert req.sent_at is not None
    assert req.message_body  # message was formatted
    assert "Alex" in req.message_body


@pytest.mark.asyncio
async def test_call_in_service_handle_inbound_accept():
    """Service.handle_inbound() matches latest PENDING request and updates on 'yes'."""
    store = CallInStore()
    provider = DemoSMSProvider()
    service = CallInService(store, provider)

    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    # Create and send a request
    req = await service.create_and_send(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )

    # Handle inbound reply
    updated = service.handle_inbound("+61412345678", "yes")

    assert updated is not None
    assert updated.status == CallInStatus.ACCEPTED
    assert updated.response_text == "yes"


@pytest.mark.asyncio
async def test_call_in_service_handle_inbound_decline():
    """Service.handle_inbound() updates to DECLINED on 'no'."""
    store = CallInStore()
    provider = DemoSMSProvider()
    service = CallInService(store, provider)

    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = await service.create_and_send(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )

    updated = service.handle_inbound("+61412345678", "no")

    assert updated is not None
    assert updated.status == CallInStatus.DECLINED


@pytest.mark.asyncio
async def test_call_in_service_handle_inbound_no_match():
    """Service.handle_inbound() returns None if no pending request for phone."""
    store = CallInStore()
    provider = DemoSMSProvider()
    service = CallInService(store, provider)

    result = service.handle_inbound("+61412345678", "yes")

    assert result is None


@pytest.mark.asyncio
async def test_call_in_service_handle_inbound_garbage():
    """Service.handle_inbound() returns None on unrecognized reply."""
    store = CallInStore()
    provider = DemoSMSProvider()
    service = CallInService(store, provider)

    start = datetime(2026, 4, 15, 18, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)

    req = await service.create_and_send(
        venue_id="venue_1",
        employee_id="emp_1",
        employee_name="Alex",
        phone="+61412345678",
        shift_start=start,
        shift_end=end,
    )

    updated = service.handle_inbound("+61412345678", "maybe later")

    assert updated is None


# ---------------------------------------------------------------------------
# Singleton tests
# ---------------------------------------------------------------------------


def test_get_store_singleton():
    """get_store() returns the same singleton across calls."""
    reset_singletons()
    store1 = get_store()
    store2 = get_store()
    assert store1 is store2


def test_get_service_singleton():
    """get_service() returns the same singleton across calls."""
    reset_singletons()
    service1 = get_service()
    service2 = get_service()
    assert service1 is service2
