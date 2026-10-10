"""
Time clock devices and kiosk PINs.

A venue's time clock runs on a DEVICE credential, never on a person's login:
a manager sets the tablet up once and the server mints a long-lived kiosk
token for that one venue. The token is a signed JWT (type "kiosk") whose hash
is also stored, so a device can be listed and revoked in Settings and a token
is only good while its row says so. TenantMiddleware lets a kiosk token reach
KIOSK_ROUTES and nothing else.

Staff PINs are 4-6 digits, stored as salted PBKDF2 hashes, and lock for
PIN_LOCK_MINUTES after MAX_PIN_FAILURES wrong tries (counted in the database,
so both workers see the same count).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import math
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from fastapi import HTTPException

from rosteriq.services.auth import JWT_ALGORITHM, JWT_SECRET

logger = logging.getLogger(__name__)

KIOSK_TOKEN_TYPE = "kiosk"

# (method, path) a kiosk token may call. Everything else is refused centrally.
KIOSK_ROUTES = frozenset({
    ("GET", "/api/clock/board"),
    ("GET", "/api/clock/kiosk"),
    ("POST", "/api/clock/kiosk/exit"),
    ("POST", "/api/clock/pin/check"),
    ("POST", "/api/clock/in"),
    ("POST", "/api/clock/out"),
    ("POST", "/api/clock/break/start"),
    ("POST", "/api/clock/break/end"),
})

MAX_PIN_FAILURES = 5
PIN_LOCK_MINUTES = 15
PIN_ITERATIONS = 20000
MAX_ACTIVE_DEVICES = 20
TOUCH_EVERY = timedelta(minutes=5)

# Operational thresholds for the time clock (not award values).
STALE_OPEN_HOURS = 14       # an open punch this old is a forgotten clock-out
LONG_SHIFT_HOURS = 12       # warn on any clock-out longer than this
CLEAN_VARIANCE_MINUTES = 15

DEFAULT_SETTINGS = {
    "allow_self_clock": False,
    # Deduct the rostered unpaid meal break when none was recorded on a shift
    # long enough to need one. ADVISOR TO CONFIRM for each venue.
    "auto_meal_break": True,
}

FLAG_LABELS = {
    "auto_break": "meal break auto-deducted (rostered)",
    "no_break": "no break recorded",
    "finish_entered_later": "finish time entered later",
    "auto_closed": "auto-closed at rostered end — check",
    "manager_closed": "finish time entered by a manager",
    "long_shift": f"over {LONG_SHIFT_HOURS}h",
    "short_punch": "under 5 min",
}


def _sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def naive_utc(dt):
    """Stored punches are naive UTC; Postgres hands back tz-aware values."""
    if dt is None:
        return None
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


# ---------------------------------------------------------------------------
# Device tokens
# ---------------------------------------------------------------------------

def bearer_token(request) -> Optional[str]:
    parts = (request.headers.get("Authorization") or "").split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def token_claims(token: Optional[str]) -> Optional[dict]:
    """The claims of a validly signed kiosk token, else None (no DB access)."""
    if not token:
        return None
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None
    if claims.get("type") != KIOSK_TOKEN_TYPE or not claims.get("sub") or not claims.get("vid"):
        return None
    return claims


def authenticate(db, token: str) -> Optional[dict]:
    """The live device row this token belongs to, or None (unknown, revoked,
    venue mismatch, or a token that was replaced)."""
    claims = token_claims(token)
    if claims is None:
        return None
    try:
        device = db.get_kiosk_device(claims["sub"])
    except Exception as e:  # noqa: BLE001 — fail closed
        logger.warning(f"kiosk device lookup failed: {e}")
        return None
    if not device or device.get("revoked_at") or device.get("venue_id") != claims["vid"]:
        return None
    if not hmac.compare_digest(str(device.get("token_hash") or ""), _sha(token)):
        return None
    return device


def route_allowed(method: str, path: str) -> bool:
    return (method.upper(), path.rstrip("/") or "/") in KIOSK_ROUTES


def touch(db, device: dict) -> None:
    """Record that the device is in use, at most every TOUCH_EVERY."""
    last = naive_utc(device.get("last_used_at"))
    if last and datetime.utcnow() - last < TOUCH_EVERY:
        return
    try:
        db.touch_kiosk_device(device["id"])
    except Exception:  # noqa: BLE001 — a missed stamp never blocks a punch
        pass


def create_device(db, venue_id: str, name: str, created_by: Optional[str]) -> tuple:
    """(device row, token). The token is returned once and stored only hashed."""
    live = [d for d in (db.list_kiosk_devices(venue_id) or []) if not d.get("revoked_at")]
    try:
        from rosteriq.services.demo import DEMO_VENUE_ID
    except Exception:  # noqa: BLE001
        DEMO_VENUE_ID = None
    if venue_id == DEMO_VENUE_ID:
        # Every Try Demo visitor shares this venue; the oldest demo clock makes
        # way rather than the cap locking the next visitor out.
        live.sort(key=lambda d: str(d.get("created_at")))
        while len(live) >= MAX_ACTIVE_DEVICES:
            revoke_device(db, live.pop(0), by="demo-recycle")
    if len(live) >= MAX_ACTIVE_DEVICES:
        raise HTTPException(status_code=409, detail=(
            f"This venue already has {len(live)} time clock devices. Revoke an old one in "
            "Settings → Time clock first."))
    device_id = f"kd-{uuid.uuid4().hex[:16]}"
    token = jwt.encode({"sub": device_id, "vid": venue_id, "type": KIOSK_TOKEN_TYPE,
                        "iat": int(time.time()), "jti": secrets.token_hex(8)},
                       JWT_SECRET, algorithm=JWT_ALGORITHM)
    device = {
        "id": device_id, "venue_id": venue_id, "name": (name or "Time clock").strip()[:80],
        "token_hash": _sha(token), "created_by": created_by,
        "created_at": datetime.utcnow(), "last_used_at": None,
        "revoked_at": None, "revoked_by": None,
    }
    db.save_kiosk_device(device)
    return device, token


def revoke_device(db, device: dict, by: str) -> dict:
    device = dict(device)
    device["revoked_at"] = datetime.utcnow()
    device["revoked_by"] = by
    db.save_kiosk_device(device)
    return device


def public_device(d: dict) -> dict:
    iso = lambda v: naive_utc(v).isoformat() if v else None  # noqa: E731
    return {
        "id": d.get("id"), "venue_id": d.get("venue_id"), "name": d.get("name"),
        "created_by": d.get("created_by"), "created_at": iso(d.get("created_at")),
        "last_used_at": iso(d.get("last_used_at")), "revoked_at": iso(d.get("revoked_at")),
        "active": not d.get("revoked_at"),
    }


# ---------------------------------------------------------------------------
# PINs
# ---------------------------------------------------------------------------

def valid_pin(pin) -> bool:
    return isinstance(pin, str) and pin.isdigit() and 4 <= len(pin) <= 6


def hash_pin(venue_id: str, employee_id: str, pin: str) -> str:
    salt = secrets.token_hex(8)
    dk = hashlib.pbkdf2_hmac("sha256", f"{venue_id}:{employee_id}:{pin}".encode(),
                             salt.encode(), PIN_ITERATIONS)
    return f"pbkdf2${PIN_ITERATIONS}${salt}${dk.hex()}"


def pin_matches(stored: str, venue_id: str, employee_id: str, pin: str) -> bool:
    if not stored or not pin:
        return False
    if stored.startswith("pbkdf2$"):
        try:
            _, iterations, salt, digest = stored.split("$")
            dk = hashlib.pbkdf2_hmac("sha256", f"{venue_id}:{employee_id}:{pin}".encode(),
                                     salt.encode(), int(iterations))
        except (ValueError, TypeError):
            return False
        return hmac.compare_digest(dk.hex(), digest)
    # PINs set through the API before they were salted
    legacy = hashlib.sha256(f"{venue_id}:{employee_id}:{pin}".encode()).hexdigest()
    return hmac.compare_digest(stored, legacy)


_WEAK = {"1234", "4321", "1212", "6969", "2580", "0852", "123456", "654321"}


def generate_pin(length: int = 4) -> str:
    while True:
        pin = "".join(str(secrets.randbelow(10)) for _ in range(length))
        if len(set(pin)) > 1 and pin not in _WEAK:
            return pin


def _minutes_left(until) -> int:
    return max(1, math.ceil((naive_utc(until) - datetime.utcnow()).total_seconds() / 60))


def _locked(until) -> bool:
    return bool(until) and naive_utc(until) > datetime.utcnow()


def check_pin(db, venue_id: str, employee_id: str, pin: str) -> bool:
    """True when the employee's PIN is right; False when they have no PIN.
    Raises 403 for a wrong PIN and 429 while the PIN is locked."""
    rec = db.get_timeclock_pin_record(venue_id, employee_id)
    if not rec or not rec.get("pin_hash"):
        return False
    if _locked(rec.get("locked_until")):
        raise HTTPException(status_code=429, detail=(
            f"Too many wrong PINs — this PIN is locked for {_minutes_left(rec['locked_until'])} "
            "more minute(s). A manager can reset it on the Staff page."))
    if not pin:
        raise HTTPException(status_code=403, detail="Enter your PIN")
    if pin_matches(rec["pin_hash"], venue_id, employee_id, pin):
        if rec.get("failed_attempts"):
            db.clear_timeclock_pin_failures(venue_id, employee_id)
        return True
    after = db.note_timeclock_pin_failure(venue_id, employee_id, MAX_PIN_FAILURES, PIN_LOCK_MINUTES) or {}
    try:
        from rosteriq.services.events import security
        security("timeclock.pin_wrong", venue_id=venue_id, employee_id=employee_id,
                 locked=_locked(after.get("locked_until")))
    except Exception:  # noqa: BLE001
        pass
    if _locked(after.get("locked_until")):
        raise HTTPException(status_code=429, detail=(
            f"Wrong PIN {MAX_PIN_FAILURES} times — this PIN is locked for {PIN_LOCK_MINUTES} "
            "minutes. A manager can reset it on the Staff page."))
    left = MAX_PIN_FAILURES - int(after.get("failed_attempts") or 0)
    raise HTTPException(status_code=403, detail=(
        f"Incorrect PIN — {left} {'try' if left == 1 else 'tries'} left before it locks "
        f"for {PIN_LOCK_MINUTES} minutes."))


def manager_employee_ids(db, venue_id: str, with_pins: set) -> list:
    """Employees at the venue (with a PIN) whose login is a full manager of it.
    A login is someone's when the staff record carries its email."""
    out = []
    for emp in db.get_employees(venue_id) or []:
        if emp.id not in with_pins or not getattr(emp, "active", True):
            continue
        email = (getattr(emp, "email", "") or "").strip().lower()
        if not email:
            continue
        try:
            user = db.get_user_by_email(email)
        except Exception:  # noqa: BLE001
            user = None
        if not user or not user.get("is_active", True):
            continue
        role = str(getattr(user.get("role"), "value", user.get("role")) or "")
        if role == "owner":
            out.append(emp.id)
        elif role == "manager" and venue_id in (user.get("venue_ids") or []) \
                and venue_id not in (user.get("section_grants") or {}):
            out.append(emp.id)
    return out


def check_exit_pin(db, device: dict, pin: str) -> str:
    """The manager employee id whose PIN this is. Raises 403/429."""
    if _locked(device.get("exit_locked_until")):
        raise HTTPException(status_code=429, detail=(
            f"Too many wrong manager PINs — try again in {_minutes_left(device['exit_locked_until'])} "
            "minute(s), or sign in as a manager."))
    venue_id = device["venue_id"]
    if valid_pin(pin):
        pins = db.list_timeclock_pin_employee_ids(venue_id) or set()
        for emp_id in manager_employee_ids(db, venue_id, pins):
            rec = db.get_timeclock_pin_record(venue_id, emp_id) or {}
            if _locked(rec.get("locked_until")):
                continue
            if pin_matches(rec.get("pin_hash") or "", venue_id, emp_id, pin):
                if device.get("exit_failures"):
                    db.clear_kiosk_exit_failures(device["id"])
                return emp_id
    after = db.note_kiosk_exit_failure(device["id"], MAX_PIN_FAILURES, PIN_LOCK_MINUTES) or {}
    try:
        from rosteriq.services.events import security
        security("timeclock.kiosk_exit_denied", venue_id=venue_id, device_id=device["id"])
    except Exception:  # noqa: BLE001
        pass
    if _locked(after.get("exit_locked_until")):
        raise HTTPException(status_code=429, detail=(
            f"Wrong manager PIN {MAX_PIN_FAILURES} times — Exit is locked for {PIN_LOCK_MINUTES} "
            "minutes, or sign in as a manager."))
    raise HTTPException(status_code=403, detail="That isn't a manager's kiosk PIN at this venue.")


# ---------------------------------------------------------------------------
# Venue time clock settings (kv_store, both backends)
# ---------------------------------------------------------------------------

def _settings_key(venue_id: str) -> str:
    return f"timeclock_settings:{venue_id}"


def load_settings(db, venue_id: str) -> dict:
    out = dict(DEFAULT_SETTINGS)
    try:
        from rosteriq.database import _jsonb
        stored = _jsonb(db.load_from_key(_settings_key(venue_id)), {}) or {}
    except Exception:  # noqa: BLE001
        stored = {}
    for k in DEFAULT_SETTINGS:
        if k in stored:
            out[k] = bool(stored[k])
    return out


def save_settings(db, venue_id: str, changes: dict) -> dict:
    current = load_settings(db, venue_id)
    for k, v in changes.items():
        if k in DEFAULT_SETTINGS and v is not None:
            current[k] = bool(v)
    db.save_to_key(_settings_key(venue_id), dict(current, venue_id=venue_id))
    return current


# ---------------------------------------------------------------------------
# Whose record is this login?
# ---------------------------------------------------------------------------

def own_employee_ids(db, user_rec: Optional[dict], venue_id: str) -> set:
    """Employee ids at ``venue_id`` that are this login's own records: the
    staff record carries the login's email. The demo identities only ever
    match inside the demo venue (services/linking.py rules)."""
    if not user_rec:
        return set()
    email = (user_rec.get("email") or "").strip().lower()
    if not email:
        return set()
    try:
        from rosteriq.services.demo import DEMO_VENUE_ID, is_demo_identity
        if is_demo_identity(user_rec.get("id"), email) and venue_id != DEMO_VENUE_ID:
            return set()
    except Exception:  # noqa: BLE001
        pass
    return {e.id for e in (db.get_employees(venue_id) or [])
            if (getattr(e, "email", "") or "").strip().lower() == email}
