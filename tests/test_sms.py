"""
The SMS layer: provider abstraction over plain HTTPS, inert without creds.

What these pin down:
* number normalisation (AU → E.164) and masking (numbers never whole in logs)
* no credentials → clean no-op with reason "not_configured", and a status()
  that names the exact variables to set
* Twilio and MessageMedia both work behind the same interface, selected by
  environment; the wire format of each is asserted against a mocked API
* rate limiting is per RECIPIENT keyed on the NORMALISED number, and reports
  "rate_limited" — not "failed" (which made managers re-send)
* provider errors and timeouts degrade to (False, "failed"), never an
  exception in the caller
* every send lands as an ``integration`` event with the number masked
* the announcements route splits sent / rate-limited / no-phone / failed, and
  /api/sms/status is manager-only and credential-free
"""

import uuid

import httpx
import pytest
import respx

from rosteriq.services import sms as sms_mod
from rosteriq.services.sms import (
    MAX_SMS_CHARS, SMSService, format_au_number, mask_number,
    get_sms_service, reset_sms_service,
)


TWILIO_ENV = {
    "TWILIO_ACCOUNT_SID": "ACtest123",
    "TWILIO_AUTH_TOKEN": "tok-secret",
    "TWILIO_FROM_NUMBER": "+61480000000",
}
MM_ENV = {
    "MESSAGEMEDIA_API_KEY": "mm-key",
    "MESSAGEMEDIA_API_SECRET": "mm-secret",
}
ALL_SMS_VARS = list(TWILIO_ENV) + list(MM_ENV) + ["SMS_PROVIDER", "SMS_RATE_LIMIT_SECONDS"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ALL_SMS_VARS:
        monkeypatch.delenv(k, raising=False)
    reset_sms_service()
    yield
    reset_sms_service()


def _configure(monkeypatch, env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    reset_sms_service()


# ---------------------------------------------------------------------------
# normalisation + masking
# ---------------------------------------------------------------------------

def test_au_numbers_normalise_to_e164():
    assert format_au_number("0412 345 678") == "+61412345678"
    assert format_au_number("+61412345678") == "+61412345678"
    assert format_au_number("61412345678") == "+61412345678"
    assert format_au_number("412345678") == "+61412345678"
    assert format_au_number("(04) 1234-5678") == "+61412345678"
    assert format_au_number("") == ""
    assert format_au_number("123") == ""            # hopeless → refused, not sent


def test_mask_never_shows_the_middle():
    assert mask_number("+61412345678") == "+61…678"
    assert "12345" not in mask_number("+61412345678")


# ---------------------------------------------------------------------------
# unconfigured: honest no-op
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_without_credentials_everything_is_a_clean_noop():
    svc = SMSService()
    assert svc.is_configured is False
    ok, reason = await svc.send_detailed("0412345678", "hello")
    assert (ok, reason) == (False, "not_configured")
    assert await svc.send_sms("0412345678", "hello") is False

    st = svc.status()
    assert st["configured"] is False
    assert "TWILIO_ACCOUNT_SID" in st["setup"]["twilio"]
    assert "MESSAGEMEDIA_API_KEY" in st["setup"]["messagemedia"]
    # a status must never leak a credential value
    assert "tok-secret" not in str(st)


# ---------------------------------------------------------------------------
# Twilio
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_twilio_send_hits_the_right_wire_format(monkeypatch):
    _configure(monkeypatch, TWILIO_ENV)
    route = respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(return_value=httpx.Response(201, json={"sid": "SM123"}))

    svc = get_sms_service()
    assert svc.is_configured and svc.status()["provider"] == "twilio"
    ok, reason = await svc.send_detailed("0412 345 678", "Shift tonight 6pm")
    assert (ok, reason) == (True, "sent")

    req = route.calls[0].request
    body = req.content.decode()
    assert "To=%2B61412345678" in body               # normalised E.164
    assert "From=%2B61480000000" in body
    assert req.headers["Authorization"].startswith("Basic ")


@pytest.mark.asyncio
@respx.mock
async def test_twilio_error_and_timeout_degrade_to_failed(monkeypatch):
    _configure(monkeypatch, TWILIO_ENV)
    respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(return_value=httpx.Response(401, json={"message": "Authentication Error"}))
    ok, reason = await get_sms_service().send_detailed("0412345678", "x")
    assert (ok, reason) == (False, "failed")

    reset_sms_service()
    respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(side_effect=httpx.ConnectTimeout("boom"))
    ok, reason = await get_sms_service().send_detailed("0413333333", "x")
    assert (ok, reason) == (False, "failed")         # never an exception


@pytest.mark.asyncio
@respx.mock
async def test_long_messages_are_capped_not_mangled_at_160(monkeypatch):
    _configure(monkeypatch, TWILIO_ENV)
    route = respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(return_value=httpx.Response(201, json={"sid": "SM1"}))
    long_msg = "a" * 1000
    ok, _ = await get_sms_service().send_detailed("0412345678", long_msg)
    assert ok
    sent_body = route.calls[0].request.content.decode()
    # capped at MAX_SMS_CHARS (3 segments), well above the old 160 mangle-point
    from urllib.parse import parse_qs
    text = parse_qs(sent_body)["Body"][0]
    assert len(text) == MAX_SMS_CHARS
    assert MAX_SMS_CHARS > 400


# ---------------------------------------------------------------------------
# MessageMedia
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_messagemedia_selected_by_env_and_wire_format(monkeypatch):
    _configure(monkeypatch, {**MM_ENV, "SMS_PROVIDER": "messagemedia"})
    route = respx.post("https://api.messagemedia.com/v1/messages").mock(
        return_value=httpx.Response(202, json={"messages": [{"message_id": "mm-1"}]}))

    svc = get_sms_service()
    assert svc.status()["provider"] == "messagemedia"
    ok, reason = await svc.send_detailed("0412345678", "hello")
    assert (ok, reason) == (True, "sent")

    import json as _json
    payload = _json.loads(route.calls[0].request.content)
    assert payload["messages"][0]["destination_number"] == "+61412345678"
    assert payload["messages"][0]["format"] == "SMS"


def test_provider_selection_rules(monkeypatch):
    # both configured, no override → Twilio wins
    _configure(monkeypatch, {**TWILIO_ENV, **MM_ENV})
    assert get_sms_service().status()["provider"] == "twilio"
    # explicit override wins
    _configure(monkeypatch, {**TWILIO_ENV, **MM_ENV, "SMS_PROVIDER": "messagemedia"})
    assert get_sms_service().status()["provider"] == "messagemedia"
    # forcing a provider whose creds are missing = unconfigured, loudly not wrongly
    for k in MM_ENV:                      # clear the MM creds set two cases up
        monkeypatch.delenv(k, raising=False)
    _configure(monkeypatch, {**TWILIO_ENV, "SMS_PROVIDER": "messagemedia"})
    assert get_sms_service().is_configured is False


# ---------------------------------------------------------------------------
# rate limiting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_rate_limit_is_per_recipient_and_reported_as_such(monkeypatch):
    _configure(monkeypatch, TWILIO_ENV)
    respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(return_value=httpx.Response(201, json={"sid": "SM1"}))
    svc = get_sms_service()

    assert (await svc.send_detailed("0412345678", "first"))[1] == "sent"
    # same person, different formatting — still one person, still limited
    ok, reason = await svc.send_detailed("+61 412 345 678", "second")
    assert (ok, reason) == (False, "rate_limited")
    # a different person is unaffected
    assert (await svc.send_detailed("0499999999", "other"))[1] == "sent"


# ---------------------------------------------------------------------------
# observability
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_sends_are_recorded_as_integration_events_with_masked_numbers(monkeypatch):
    from rosteriq.database import get_db
    from datetime import datetime, timedelta
    _configure(monkeypatch, TWILIO_ENV)
    respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(return_value=httpx.Response(201, json={"sid": "SM1"}))

    await get_sms_service().send_detailed("0412345678", "hello")
    rows = get_db().list_events(category="integration", limit=20)
    sms_rows = [r for r in rows if r["action"] == "twilio.send_sms"]
    assert sms_rows, [r["action"] for r in rows]
    details = str(sms_rows[0]["details"])
    assert "+61…678" in details
    assert "61412345678" not in details              # never the whole number


# ---------------------------------------------------------------------------
# the announcements route + status endpoint
# ---------------------------------------------------------------------------

PW = "Passw0rd!234"


def _world():
    from fastapi.testclient import TestClient
    from rosteriq.api import app
    from rosteriq.database import get_db
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    vid = f"sms_{tag}"
    email = f"owner_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "O"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "owner"
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    r = c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-06-20T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text
    return c, h, vid, tag


def test_announcement_reports_unconfigured_sms_helpfully():
    c, h, vid, _ = _world()
    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "Hi", "body": "Team meeting 3pm", "send_sms": True,
    }, headers=h)
    assert r.status_code == 200, r.text
    sr = r.json()["sms_result"]
    assert sr["attempted"] is False
    assert "TWILIO_ACCOUNT_SID" in sr["reason"]


@pytest.mark.asyncio
@respx.mock
async def test_announcement_splits_sent_ratelimited_nophone(monkeypatch):
    _configure(monkeypatch, TWILIO_ENV)
    respx.post(
        "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    ).mock(return_value=httpx.Response(201, json={"sid": "SM1"}))

    c, h, vid, tag = _world()
    for i, phone in enumerate(["0412000001", "0412000001", ""]):   # dup number + missing
        r = c.post("/employees", json={
            "id": f"e{i}_{tag}", "name": f"P{i}", "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "venue_id": vid,
            "hourly_base_rate": "30.00", "email": f"p{i}_{tag}@x.com", "phone": phone,
            "skills": ["bar"], "created_at": "2026-06-20T00:00:00",
            "updated_at": "2026-06-20T00:00:00"}, headers=h)
        assert r.status_code in (200, 201), r.text

    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "Roster", "body": "New roster is up", "send_sms": True,
    }, headers=h)
    assert r.status_code == 200, r.text
    sr = r.json()["sms_result"]
    assert sr["attempted"] is True
    assert sr["sent"] == 1
    assert sr["rate_limited"] == 1        # the duplicate number, NOT "failed"
    assert sr["no_phone"] == 1
    assert sr["failed"] == 0


def test_sms_status_is_manager_only_and_credential_free(monkeypatch):
    for k, v in TWILIO_ENV.items():
        monkeypatch.setenv(k, v)
    reset_sms_service()

    c, h, vid, tag = _world()
    r = c.get("/api/sms/status", headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["configured"] is True and body["provider"] == "twilio"
    assert "tok-secret" not in r.text and "ACtest123" not in r.text
    assert body.get("from") == "+61…000"

    # staff get the cosmetic flag only — no from-number, no setup detail
    from rosteriq.database import get_db
    email = f"st_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "staff"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    r = c.get("/api/sms/status", headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 200
    assert set(r.json().keys()) <= {"configured", "provider"}
    assert "from" not in r.json() and "setup" not in r.json()


def test_staff_see_announcements_without_delivery_detail(monkeypatch):
    """The stored sms_result names who has no phone / whose send failed — a
    per-person roster only managers should see."""
    from rosteriq.database import get_db
    c, h, vid, tag = _world()
    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "Hi", "body": "Meeting", "send_sms": True}, headers=h)
    assert r.status_code == 200, r.text

    email = f"viewer_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "V"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "staff"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]

    staff_view = c.get(f"/api/announcements?venue_id={vid}",
                       headers={"Authorization": f"Bearer {tok}"}).json()
    assert staff_view["announcements"][0]["sms_result"] is None
    mgr_view = c.get(f"/api/announcements?venue_id={vid}", headers=h).json()
    assert mgr_view["announcements"][0]["sms_result"] is not None


# ---------------------------------------------------------------------------
# review fixes: normalisation edges, scrubbing, rate-limit-on-success,
# role gates
# ---------------------------------------------------------------------------

def test_normalisation_handles_trunk_zero_and_foreign_codes():
    # "+61 (0)412…" — the trunk zero must be dropped, not kept
    assert format_au_number("+61 0412 345 678") == "+61412345678"
    assert format_au_number("+61 (0)412 345 678") == "+61412345678"
    # a foreign number with its own +country-code is respected, not mangled
    assert format_au_number("+44 7911 123456") == "+447911123456"
    # junk that is number-shaped but the wrong length is refused, not sent
    assert format_au_number("0412 345") == ""
    assert format_au_number("+61 1234") == ""


def test_provider_error_text_is_scrubbed_of_numbers():
    from rosteriq.services.sms import scrub_digits
    scrubbed = scrub_digits("The 'To' number +61412345678 is not a valid phone number.")
    assert "61412345678" not in scrubbed and "[number]" in scrubbed
    # short ids survive (error codes are useful)
    assert scrub_digits("Error 21211") == "Error 21211"


@pytest.mark.asyncio
@respx.mock
async def test_failed_send_does_not_burn_the_rate_limit_slot(monkeypatch):
    _configure(monkeypatch, TWILIO_ENV)
    url = "https://api.twilio.com/2010-04-01/Accounts/ACtest123/Messages.json"
    respx.post(url).mock(return_value=httpx.Response(500, text="upstream sad"))
    svc = get_sms_service()
    assert (await svc.send_detailed("0412345678", "x"))[1] == "failed"
    # provider recovers — the retry must SEND, not report rate_limited
    respx.post(url).mock(return_value=httpx.Response(201, json={"sid": "SM1"}))
    assert (await svc.send_detailed("0412345678", "x"))[1] == "sent"


def test_staff_cannot_publish_pin_or_blast(monkeypatch):
    from rosteriq.database import get_db
    c, h, vid, tag = _world()
    email = f"stf_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "staff"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    sh = {"Authorization": f"Bearer {tok}"}

    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "spam", "body": "spam", "send_sms": True}, headers=sh)
    assert r.status_code == 403, r.text
    # manager path still works
    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "ok", "body": "real one", "send_sms": False}, headers=h)
    assert r.status_code == 200, r.text
    ann_id = r.json()["announcement_id"]
    assert c.post(f"/api/announcements/{ann_id}/pin",
                  json={"venue_id": vid, "pinned": True}, headers=sh).status_code == 403
    assert c.post(f"/api/announcements/{ann_id}/pin",
                  json={"venue_id": vid, "pinned": True}, headers=h).status_code == 200


def test_staff_cannot_use_the_test_sms_endpoint(monkeypatch):
    from rosteriq.database import get_db
    c, h, vid, tag = _world()
    email = f"stf2_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "staff"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    r = c.post("/api/notifications/test-sms", json={"phone": "0412345678"},
               headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 403, r.text
