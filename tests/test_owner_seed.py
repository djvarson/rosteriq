"""
Env-driven platform-owner seed. Register-time bootstrap can only grant owner to
the very first user, so once the demo user (or anyone) exists, no signup can
become owner. ensure_owner_from_env() guarantees an owner login out-of-band.
"""

from datetime import datetime

from rosteriq.services.auth import ensure_owner_from_env, auth_service
from rosteriq.database import get_db


def test_owner_seed_creates_owner(monkeypatch):
    monkeypatch.setenv("ROSTERIQ_OWNER_EMAIL", "boss@venue.com")
    monkeypatch.setenv("ROSTERIQ_OWNER_PASSWORD", "S3curePass!234")
    ensure_owner_from_env()

    u = get_db().get_user_by_email("boss@venue.com")
    assert u and u["role"] == "owner" and u["is_active"]
    assert u.get("venue_ids") == []            # platform owner, not venue-scoped
    assert auth_service.verify_password("S3curePass!234", u["password_hash"])


def test_owner_seed_promotes_existing_staff(monkeypatch):
    db = get_db()
    db.save_user({
        "id": "u-existing", "email": "mgr@venue.com",
        "password_hash": auth_service.hash_password("whatever"),
        "name": "Mgr", "role": "staff", "is_active": True,
        "venue_ids": [], "api_key_hash": "",
        "created_at": datetime.utcnow(), "last_login": None,
    })
    monkeypatch.setenv("ROSTERIQ_OWNER_EMAIL", "mgr@venue.com")
    monkeypatch.delenv("ROSTERIQ_OWNER_PASSWORD", raising=False)
    ensure_owner_from_env()

    assert db.get_user_by_email("mgr@venue.com")["role"] == "owner"


def test_owner_seed_resets_password_of_existing_account(monkeypatch):
    """An account that pre-dates the seed (e.g. registered on the old app with a
    forgotten password) gets its password reset to the env value — the env vars
    are authoritative break-glass access, so the operator is never locked out."""
    db = get_db()
    db.save_user({
        "id": "u-old", "email": "old-owner@venue.com",
        "password_hash": auth_service.hash_password("forgotten-old-pass"),
        "name": "Old", "role": "owner", "is_active": True,
        "venue_ids": [], "api_key_hash": "",
        "created_at": datetime.utcnow(), "last_login": None,
    })
    monkeypatch.setenv("ROSTERIQ_OWNER_EMAIL", "old-owner@venue.com")
    monkeypatch.setenv("ROSTERIQ_OWNER_PASSWORD", "NewEnvPass!234")
    ensure_owner_from_env()

    u = db.get_user_by_email("old-owner@venue.com")
    assert auth_service.verify_password("NewEnvPass!234", u["password_hash"])
    assert not auth_service.verify_password("forgotten-old-pass", u["password_hash"])


def test_owner_seed_keeps_matching_password_untouched(monkeypatch):
    db = get_db()
    original_hash = auth_service.hash_password("SamePass!234")
    db.save_user({
        "id": "u-same", "email": "same@venue.com",
        "password_hash": original_hash,
        "name": "Same", "role": "owner", "is_active": True,
        "venue_ids": [], "api_key_hash": "",
        "created_at": datetime.utcnow(), "last_login": None,
    })
    monkeypatch.setenv("ROSTERIQ_OWNER_EMAIL", "same@venue.com")
    monkeypatch.setenv("ROSTERIQ_OWNER_PASSWORD", "SamePass!234")
    ensure_owner_from_env()
    # matching password -> hash left alone (no needless rehash/rotation)
    assert db.get_user_by_email("same@venue.com")["password_hash"] == original_hash


def test_owner_seed_noop_when_unset(monkeypatch):
    monkeypatch.delenv("ROSTERIQ_OWNER_EMAIL", raising=False)
    ensure_owner_from_env()  # must not raise


def test_owner_seed_idempotent(monkeypatch):
    monkeypatch.setenv("ROSTERIQ_OWNER_EMAIL", "boss2@venue.com")
    monkeypatch.setenv("ROSTERIQ_OWNER_PASSWORD", "S3curePass!234")
    ensure_owner_from_env()
    ensure_owner_from_env()  # second call is a no-op, no duplicate/crash
    db = get_db()
    owners = [u for u in db.list_users() if u["email"] == "boss2@venue.com"]
    assert len(owners) == 1 and owners[0]["role"] == "owner"
