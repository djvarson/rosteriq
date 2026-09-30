"""
Reset the public Try Demo venue to its seeded state.

Try Demo is ONE shared venue. Without a reset, whatever a visitor adds (a feed
post, an announcement, an open stocktake, a renamed ingredient, a kiosk PIN)
is what the next visitor sees — possibly a prospect mid-pitch. The seed
(services/demo.py) re-creates each showcase pillar only when that pillar is
EMPTY, so the reset wipes every demo pillar completely and the seed then
rebuilds it from scratch.

Safety rules — the reset can only ever be incomplete, never destructive:
* Every delete is scoped by the DEMO_VENUE_ID constant (or the two demo user
  ids) passed as a bound parameter. Nothing here accepts a venue argument.
* Only leaf rows are deleted. The venue row and the demo user rows are never
  deleted (FK cascades); users are reset in place.
* Demo staff are deleted only after every demo shift is gone, and never one
  that a shift in another venue's roster still references (the
  shifts.employee_id cascade must not cross tenants).
* Each statement runs on its own; a table that doesn't exist yet (many are
  created lazily) just skips that step.
* Abuse-control state is left alone: login attempts, the AI daily budget,
  join-code throttles, security/error events.

When a reset happens (see _should_reset):
* never within MIN_INTERVAL_SECONDS of the last one (across workers), so Try
  Demo can't be used to hammer the database;
* never under a client that is still using the demo — a salesperson who sets
  things up as the manager and then opens the staff phone keeps their work;
* not while ANOTHER client was active in the last few minutes, unless the
  venue hasn't been reset for an hour (so a busy demo still gets cleaned).
Activity is stamped by the tenant middleware (note_demo_activity). Clients
are told apart by a hash of the address the edge saw, never stored raw.

The seed runs only after a reset, or when the venue is missing its data: it
re-asserts the showcase, so running it on every Try Demo undid a pitch in
progress (a claimed cover re-opened, a comment on a showcase post vanished).
"""

import hashlib
import logging
import threading
import time as _time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

from rosteriq.services.demo import DEMO_STAFF_USER_ID, DEMO_USER_ID, DEMO_VENUE_ID

logger = logging.getLogger(__name__)

MIN_INTERVAL_SECONDS = 60
_LAST_RESET_KEY = "demo:last_reset"
_ADVISORY_LOCK_KEY = 7_734_001  # arbitrary, stable: "demo reset"
_lock = threading.Lock()

D = DEMO_VENUE_ID
DU = (DEMO_USER_ID, DEMO_STAFF_USER_ID)
_DEMO_ROSTERS = "SELECT id FROM rosters WHERE venue_id = %s"

# (label, sql, params) — run in order: children before parents. Every rule is
# scoped by the demo venue or the demo user ids; test_demo_reset pins that.
PG_RULES = [
    ("handover notes", "DELETE FROM kv_store WHERE key LIKE 'handover%%' AND value->>'venue_id' = %s", (D,)),
    ("bids", "DELETE FROM bids WHERE open_shift_id IN (SELECT id FROM open_shifts WHERE venue_id = %s)", (D,)),
    ("open shifts", "DELETE FROM open_shifts WHERE venue_id = %s", (D,)),
    ("shift bids", "DELETE FROM shift_bids WHERE venue_id = %s", (D,)),
    ("payroll exports", "DELETE FROM payroll_exports WHERE batch_id IN (SELECT batch_id FROM payroll_batches WHERE venue_id = %s)", (D,)),
    ("payroll batches", "DELETE FROM payroll_batches WHERE venue_id = %s", (D,)),
    ("roster revisions", f"DELETE FROM roster_revisions WHERE roster_id IN ({_DEMO_ROSTERS})", (D,)),
    ("approval requests", "DELETE FROM approval_requests WHERE venue_id = %s", (D,)),
    ("approval steps", "DELETE FROM approval_steps WHERE workflow_id IN (SELECT id FROM approval_workflows WHERE venue_id = %s)", (D,)),
    ("approval workflows", "DELETE FROM approval_workflows WHERE venue_id = %s", (D,)),
    ("roster states", f"DELETE FROM roster_states WHERE roster_id IN ({_DEMO_ROSTERS})", (D,)),
    ("roster state history", f"DELETE FROM roster_state_history WHERE roster_id IN ({_DEMO_ROSTERS})", (D,)),
    ("roster conflicts", f"DELETE FROM roster_conflicts WHERE roster_id IN ({_DEMO_ROSTERS})", (D,)),
    ("publication events", "DELETE FROM publication_events WHERE venue_id = %s", (D,)),
    ("shift covers", "DELETE FROM shift_covers WHERE venue_id = %s", (D,)),
    ("shift swaps", "DELETE FROM shift_swaps WHERE venue_id = %s", (D,)),
    ("shifts", f"DELETE FROM shifts WHERE roster_id IN ({_DEMO_ROSTERS})", (D,)),
    ("rosters", "DELETE FROM rosters WHERE venue_id = %s", (D,)),
    ("forecasts", "DELETE FROM forecasts WHERE venue_id = %s", (D,)),
    ("timesheets", "DELETE FROM timesheets WHERE venue_id = %s", (D,)),
    ("timeclock pins", "DELETE FROM timeclock_pins WHERE venue_id = %s", (D,)),
    ("leave requests", "DELETE FROM leave_requests WHERE venue_id = %s", (D,)),
    ("checklist runs", "DELETE FROM checklist_runs WHERE venue_id = %s", (D,)),
    ("checklist templates", "DELETE FROM checklist_templates WHERE venue_id = %s", (D,)),
    ("sop acknowledgements", "DELETE FROM sop_acknowledgements WHERE venue_id = %s", (D,)),
    ("sop documents", "DELETE FROM sop_documents WHERE venue_id = %s", (D,)),
    ("feed posts", "DELETE FROM feed_posts WHERE venue_id = %s", (D,)),
    ("announcements", "DELETE FROM announcements WHERE venue_id = %s", (D,)),
    ("waste log", "DELETE FROM waste_log WHERE venue_id = %s", (D,)),
    ("stocktakes", "DELETE FROM stocktakes WHERE venue_id = %s", (D,)),
    ("supplier orders", "DELETE FROM supplier_orders WHERE venue_id = %s", (D,)),
    ("supplier invoices", "DELETE FROM supplier_invoices WHERE venue_id = %s", (D,)),
    ("xero bill pushes", "DELETE FROM xero_bill_pushes WHERE venue_id = %s", (D,)),
    ("myob bill pushes", "DELETE FROM myob_bill_pushes WHERE venue_id = %s", (D,)),
    ("dish sales", "DELETE FROM dish_sales WHERE venue_id = %s", (D,)),
    ("pos item maps", "DELETE FROM pos_item_maps WHERE venue_id = %s", (D,)),
    ("sales import batches", "DELETE FROM sales_import_batches WHERE venue_id = %s", (D,)),
    ("recipes", "DELETE FROM recipes WHERE venue_id = %s", (D,)),
    ("ingredients", "DELETE FROM ingredients WHERE venue_id = %s", (D,)),
    ("roster templates", "DELETE FROM roster_templates WHERE venue_id = %s", (D,)),
    ("onboarding states", "DELETE FROM onboarding_states WHERE venue_id = %s", (D,)),
    ("xero credentials", "DELETE FROM xero_credentials WHERE venue_id = %s", (D,)),
    ("themes", "DELETE FROM themes WHERE venue_id = %s", (D,)),
    ("revenue snapshots", "DELETE FROM revenue_snapshots WHERE venue_id = %s", (D,)),
    ("analytics snapshots", "DELETE FROM analytics_snapshots WHERE venue_id = %s", (D,)),
    ("revenue models", "DELETE FROM revenue_models WHERE venue_id = %s", (D,)),
    ("revenue actuals", "DELETE FROM revenue_actuals WHERE venue_id = %s", (D,)),
    ("direct bookings", "DELETE FROM direct_bookings WHERE venue_id = %s", (D,)),
    ("experiment outcomes", "DELETE FROM ab_experiment_outcomes WHERE venue_id = %s", (D,)),
    ("webhook deliveries", "DELETE FROM webhook_deliveries WHERE venue_id = %s", (D,)),
    ("webhook dead letters", "DELETE FROM webhook_dead_letters WHERE venue_id = %s", (D,)),
    ("webhook subscriptions", "DELETE FROM webhook_subscriptions WHERE venue_id = %s", (D,)),
    ("webhook secrets", "DELETE FROM webhook_secrets WHERE venue_id = %s", (D,)),
    ("push subscriptions", "DELETE FROM push_subscriptions WHERE venue_id = %s OR user_id IN %s", (D, DU)),
    ("notification preferences",
     "DELETE FROM notification_preferences WHERE user_id IN (SELECT id FROM employees WHERE venue_id = %s) OR user_id IN %s",
     (D, DU)),
    ("preference profiles", "DELETE FROM preference_profiles WHERE employee_id IN (SELECT id FROM employees WHERE venue_id = %s)", (D,)),
    ("notification log", "DELETE FROM notification_log WHERE venue_id = %s", (D,)),
    ("api key records", "DELETE FROM api_key_records WHERE user_id IN %s", (DU,)),
    ("privacy consents", "DELETE FROM privacy_consents WHERE user_id IN %s", (DU,)),
    ("privacy audit log", "DELETE FROM privacy_audit_log WHERE user_id IN %s", (DU,)),
    ("password reset tokens", "DELETE FROM password_reset_tokens WHERE user_id IN %s", (DU,)),
    ("email verification tokens", "DELETE FROM email_verification_tokens WHERE user_id IN %s", (DU,)),
    ("audit log", "DELETE FROM audit_logs WHERE venue_id = %s AND COALESCE(details->>'category', 'audit') IN ('audit', 'ai')", (D,)),
    # Security/error rows are the abuse trail: keep them for the platform owner,
    # but take them out of the demo venue's Activity view — they carry earlier
    # visitors' IPs and visitor-typed text.
    ("audit log detach",
     "UPDATE audit_logs SET venue_id = NULL, "
     "details = COALESCE(details, '{}'::jsonb) || jsonb_build_object('demo_venue_id', venue_id) "
     "WHERE venue_id = %s",
     (D,)),
    # Staff last: every demo shift is gone by now, so the only shifts that could
    # still reference a demo employee belong to another venue — keep those rows.
    # NOT EXISTS, not NOT IN: one NULL employee_id anywhere would make NOT IN
    # match nothing and silently skip the whole staff pillar.
    ("employees",
     "DELETE FROM employees e WHERE e.venue_id = %s AND NOT EXISTS "
     "(SELECT 1 FROM shifts s JOIN rosters r ON r.id = s.roster_id "
     "WHERE s.employee_id = e.id AND r.venue_id <> %s)",
     (D, D)),
]


def _pg_reset(db) -> dict:
    report = {"deleted": {}, "skipped": {}}
    for label, sql, params in PG_RULES:
        try:
            with db._cursor() as cur:
                cur.execute(sql, params)
                report["deleted"][label] = cur.rowcount
        except Exception as e:  # missing lazy table, or a column this deploy lacks
            report["skipped"][label] = type(e).__name__
    return report


def _v(item, field):
    return item.get(field) if isinstance(item, dict) else getattr(item, field, None)


def _prune_dict(d, keep) -> int:
    doomed = [k for k, v in list(d.items()) if not keep(k, v)]
    for k in doomed:
        d.pop(k, None)
    return len(doomed)


def _prune_list(lst, keep) -> int:
    before = len(lst)
    lst[:] = [x for x in lst if keep(x)]
    return before - len(lst)


def _memory_reset(s) -> dict:
    """Same scope as PG_RULES, for the in-memory store. Mutates the singleton's
    collections IN PLACE — api._db and several services hold a reference."""
    n = {}
    g = lambda name: getattr(s, name, None)  # noqa: E731 — collections vary by build
    ros = {k for k, r in (g("_rosters") or {}).items() if _v(r, "venue_id") == D}
    rsh = {sh.id for k in ros for sh in (getattr(s._rosters[k], "shifts", None) or [])}
    open_shifts = {k for k, v in (g("_open_shifts") or {}).items() if _v(v, "venue_id") == D}
    batches = {k for k, v in (g("_payroll_batches") or {}).items() if _v(v, "venue_id") == D}
    emp = {e.id for e in (g("_employees") or {}).values() if _v(e, "venue_id") == D}

    by_venue_dicts = [
        "_announcements", "_feed_posts", "_sop_documents", "_sop_acks", "_checklist_templates",
        "_checklist_runs", "_ingredients", "_recipes", "_leave_requests", "_shift_covers",
        "_shift_swaps", "_stocktakes", "_supplier_orders", "_supplier_invoices", "_dish_sales",
        "_xero_bill_pushes", "_myob_bill_pushes", "_waste_log", "_pos_item_maps", "_import_batches",
        "_timesheets", "_approval_requests", "_roster_templates", "_webhook_subscriptions",
        "_experiment_outcomes", "_analytics_snapshots", "_payroll_batches", "_open_shifts",
    ]
    for name in by_venue_dicts:
        d = g(name)
        if isinstance(d, dict):
            n[name] = _prune_dict(d, lambda k, v: _v(v, "venue_id") != D)
    for name in ("_forecasts", "_publication_events", "_revenue_actuals", "_direct_bookings"):
        lst = g(name)
        if isinstance(lst, list):
            n[name] = _prune_list(lst, lambda x: _v(x, "venue_id") != D)
    for name in ("_onboarding_states", "_themes", "_revenue_models", "_webhook_secrets", "_xero_credentials"):
        d = g(name)
        if isinstance(d, dict) and D in d:
            d.pop(D, None)
            n[name] = 1
    for name in ("_revenue_snapshots", "_feed_configs", "_timeclock_pins"):
        d = g(name)
        if isinstance(d, dict):
            n[name] = _prune_dict(d, lambda k, v: str(k).rsplit(":", 1)[0] != D)
    for name in ("_roster_revisions", "_roster_states", "_roster_state_history"):
        d = g(name)
        if isinstance(d, dict):
            n[name] = _prune_dict(d, lambda k, v: k not in ros)
    if isinstance(g("_bids"), dict):
        n["_bids"] = _prune_dict(s._bids, lambda k, v: _v(v, "open_shift_id") not in open_shifts)
    if isinstance(g("_bids_by_shift"), dict):
        n["_bids_by_shift"] = _prune_dict(s._bids_by_shift, lambda k, v: k not in open_shifts)
    if isinstance(g("_payroll_exports"), list):
        n["_payroll_exports"] = _prune_list(s._payroll_exports, lambda x: _v(x, "batch_id") not in batches)
    if isinstance(g("_kv"), dict):
        n["_kv"] = _prune_dict(s._kv, lambda k, v: not (str(k).startswith("handover")
                                                        and isinstance(v, dict) and v.get("venue_id") == D))
    if isinstance(g("_shifts"), dict):
        n["_shifts"] = _prune_dict(s._shifts, lambda k, v: k not in rsh)
    if isinstance(g("_rosters"), dict):
        n["_rosters"] = _prune_dict(s._rosters, lambda k, v: k not in ros)
    for name in ("_notification_preferences", "_preference_profiles"):
        d = g(name)
        if isinstance(d, dict):
            n[name] = _prune_dict(d, lambda k, v: k not in emp and k not in DU)
    if isinstance(g("_push_subscriptions"), dict):
        n["_push_subscriptions"] = _prune_dict(s._push_subscriptions, lambda k, v: k not in DU)
    if isinstance(g("_api_key_records"), dict):
        n["_api_key_records"] = _prune_dict(s._api_key_records, lambda k, v: _v(v, "user_id") not in DU)
    for name in ("_password_reset_tokens", "_email_verification_tokens"):
        d = g(name)
        if isinstance(d, dict):
            n[name] = _prune_dict(d, lambda k, v: _v(v, "user_id") not in DU)
    if isinstance(g("_consents"), dict):
        n["_consents"] = _prune_dict(s._consents, lambda k, v: k not in DU)
    if isinstance(g("_privacy_logs"), list):
        n["_privacy_logs"] = _prune_list(s._privacy_logs, lambda x: _v(x, "user_id") not in DU)
    if isinstance(g("_audit_logs"), list):
        def keep_log(r):
            cat = ((r.get("details") or {}).get("category") if isinstance(r, dict) else None) or "audit"
            return not (_v(r, "venue_id") == D and cat in ("audit", "ai"))
        n["_audit_logs"] = _prune_list(s._audit_logs, keep_log)
        for r in s._audit_logs:                      # detach the kept abuse trail
            if isinstance(r, dict) and r.get("venue_id") == D:
                r["venue_id"] = None
                r["details"] = {**(r.get("details") or {}), "demo_venue_id": D}
    if isinstance(g("_employees"), dict):
        # As on Postgres: keep a demo employee another venue's roster still uses.
        foreign = {sh.employee_id for r in (g("_rosters") or {}).values() if _v(r, "venue_id") != D
                   for sh in (getattr(r, "shifts", None) or [])}
        n["_employees"] = _prune_dict(s._employees, lambda k, v: _v(v, "venue_id") != D or k in foreign)
    return {"deleted": n, "skipped": {}}


def _reset_demo_users(db) -> None:
    canon = {DEMO_USER_ID: "Demo User", DEMO_STAFF_USER_ID: "Emma Thompson"}
    for uid, name in canon.items():
        try:
            u = db.get_user_by_id(uid)
        except Exception:
            u = None
        if not u:
            continue
        changed = False
        for field, value in (("name", name), ("api_key_hash", ""), ("password_hash", ""),
                             ("venue_ids", [D]), ("is_active", True)):
            if u.get(field) != value and (field in u or field in ("name", "venue_ids")):
                u[field] = value
                changed = True
        if changed:
            db.save_user(u)


def _reset_process_state() -> None:
    """Per-worker memory later visitors would otherwise still see."""
    for mod, attr in (("rosteriq.routes.forecast_v2", "_forecasters"),):
        try:
            import importlib
            getattr(importlib.import_module(mod), attr).pop(D, None)
        except Exception:
            pass
    try:
        from rosteriq.routes import labour
        tracker = getattr(labour, "_labour_tracker", None)
        if tracker is not None:
            tracker.threshold_configs.pop(D, None)
    except Exception:
        pass
    try:
        from rosteriq.services.employee_onboarding import get_onboarding_service
        svc = get_onboarding_service()
        getattr(svc, "_venue_templates", {}).pop(D, None)
        checklists = getattr(svc, "_checklists", {})
        for k, c in list(checklists.items()):
            if getattr(c, "venue_id", None) == D:
                checklists.pop(k, None)
    except Exception:
        pass


_SESSION_KEY = "demo:last_session"
_ACTIVITY_KEY = "demo:activity"
SAME_CLIENT_WINDOW = timedelta(minutes=60)
OTHER_CLIENT_GRACE = timedelta(minutes=10)
MAX_PROTECTED = timedelta(minutes=60)
_ACTIVITY_EVERY_SECONDS = 60
_ACTIVITY_MAX_CLIENTS = 20
BUSY_WAIT_SECONDS = 8.0
_activity_written: dict = {}                     # client tag -> monotonic time (per worker)


def client_address(request) -> str:
    """The address Railway's edge saw: the LAST X-Forwarded-For hop (the first
    hop is whatever the client chose to send), else the socket peer."""
    fwd = request.headers.get("x-forwarded-for", "")
    peer = request.client.host if request.client else "unknown"
    return (fwd.split(",")[-1].strip() if fwd else "") or peer


def _tag(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    return hashlib.sha256(f"rosteriq-demo:{address}".encode()).hexdigest()[:16]


def _parse(at) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(at) if at else None
    except (TypeError, ValueError):
        return None


def _load_stamp(db, key):
    """(datetime | None, record). Raises on a store error so callers fail closed."""
    rec = db.load_from_key(key) or {}
    return _parse(rec.get("at")), rec


def _recent_clients(db, now: datetime) -> dict:
    """client tag -> last seen, for the last MAX_PROTECTED. Raises on a store error."""
    rec = db.load_from_key(_ACTIVITY_KEY) or {}
    out = {}
    for tag, at in (rec.get("clients") or {}).items():
        seen = _parse(at)
        if seen is not None and timedelta(0) <= now - seen < MAX_PROTECTED:
            out[tag] = seen
    return out


def note_demo_activity(db, address: Optional[str], now: Optional[datetime] = None) -> bool:
    """Record that this client is using the demo. At most once a minute per
    client per worker; best-effort (a lost write only makes a reset likelier)."""
    tag = _tag(address)
    if not tag:
        return False
    mono = _time.monotonic()
    if mono - _activity_written.get(tag, float("-inf")) < _ACTIVITY_EVERY_SECONDS:
        return False
    _activity_written[tag] = mono
    if len(_activity_written) > 2000:
        for k in [k for k, v in _activity_written.items() if mono - v > 3600]:
            _activity_written.pop(k, None)
    now = now or datetime.utcnow()
    try:
        clients = _recent_clients(db, now)
        clients[tag] = now
        newest = sorted(clients.items(), key=lambda kv: kv[1], reverse=True)[:_ACTIVITY_MAX_CLIENTS]
        db.save_to_key(_ACTIVITY_KEY, {"clients": {k: v.isoformat() for k, v in newest}})
        return True
    except Exception:
        return False


def activity_due(address: Optional[str]) -> bool:
    """Cheap in-memory pre-check so the middleware only does I/O when a write is due."""
    tag = _tag(address)
    return bool(tag) and _time.monotonic() - _activity_written.get(tag, float("-inf")) >= _ACTIVITY_EVERY_SECONDS


def _should_reset(db, now: datetime, client_ip: Optional[str]) -> bool:
    try:
        last_reset, _ = _load_stamp(db, _LAST_RESET_KEY)
        last_session, session = _load_stamp(db, _SESSION_KEY)
        clients = _recent_clients(db, now)
    except Exception:
        return False                                   # can't read the throttle: don't wipe
    if last_reset is not None and last_reset <= now + timedelta(seconds=5) \
            and now - last_reset < timedelta(seconds=MIN_INTERVAL_SECONDS):
        return False
    if session.get("client") and last_session is not None \
            and timedelta(0) <= now - last_session < MAX_PROTECTED:
        prev = clients.get(session["client"])
        clients[session["client"]] = max(prev, last_session) if prev else last_session
    me = _tag(client_ip)
    # The same client continuing its own session keeps its work.
    if me and me in clients and now - clients[me] < SAME_CLIENT_WINDOW:
        return False
    # Someone else is mid-demo: leave them be, unless the venue has gone an
    # hour without a clean-up (then the newcomer's clean demo wins).
    others_active = any(t != me and now - at < OTHER_CLIENT_GRACE for t, at in clients.items())
    if others_active and last_reset is not None and timedelta(0) <= now - last_reset < MAX_PROTECTED:
        return False
    return True


def _demo_seeded(db) -> bool:
    try:
        return bool(db.get_venue(D)) and bool(db.get_employees(D)) \
            and bool(db.get_user_by_id(DEMO_USER_ID)) and bool(db.get_user_by_id(DEMO_STAFF_USER_ID))
    except Exception:
        return True                                    # can't tell: don't re-seed over a pitch


def _demo_current(db) -> bool:
    """Is today's (venue-local) demo roster there? False after the venue's
    midnight until the seed rolls the day forward. (The seed pre-books one
    shift for tomorrow, so a seeded day has more than one.)"""
    try:
        from rosteriq.services.clock import venue_today
        today = venue_today(D, db)
        roster = db.get_roster(f"demo-roster-{(today - timedelta(days=today.weekday())).isoformat()}")
        return bool(roster) and sum(1 for s in roster.shifts if s.date == today) > 1
    except Exception:
        return True


def _try_pg_lock(db) -> bool:
    try:
        with db._cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s) AS got", (_ADVISORY_LOCK_KEY,))
            row = cur.fetchone()   # RealDictCursor: rows are dicts
            return bool(row["got"] if isinstance(row, dict) else row[0])
    except Exception:
        return False


@contextmanager
def _demo_lock(db, wait_seconds: float):
    """The one lock every demo wipe and seed runs under: this worker's thread
    lock, then (on Postgres) the cross-worker advisory lock. Yields whether it
    was acquired within wait_seconds."""
    deadline = _time.monotonic() + max(0.0, wait_seconds)
    if not (_lock.acquire(timeout=wait_seconds) if wait_seconds > 0 else _lock.acquire(blocking=False)):
        yield False
        return
    got_pg_lock = False
    try:
        if _is_pg(db):
            got_pg_lock = _try_pg_lock(db)
            while not got_pg_lock and _time.monotonic() < deadline:
                _time.sleep(0.25)
                got_pg_lock = _try_pg_lock(db)
            if not got_pg_lock:
                yield False
                return
        yield True
    finally:
        if got_pg_lock:
            try:
                with db._cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_KEY,))
            except Exception:
                pass
        _lock.release()


def _is_pg(db) -> bool:
    return hasattr(db, "_cursor") and db.__class__.__name__ == "PostgresStore"


def _seed_if_needed(db, full: bool) -> str:
    """Full seed after a wipe or on an empty demo; the new-day refresh (roster,
    forecast, sales — not the showcase re-assertion) when today is missing."""
    from rosteriq.services.demo import seed_demo_environment
    try:
        if full or not _demo_seeded(db):
            seed_demo_environment(db)
            return "seeded"
        if not _demo_current(db):
            seed_demo_environment(db, reassert_showcase=False)
            return "refreshed"
    except Exception as e:
        logger.warning("Demo seed issue (continuing): %s", e)
    return "kept"


def reset_and_seed(db, client_ip: Optional[str] = None, now: Optional[datetime] = None,
                   force: bool = False, wait_seconds: float = BUSY_WAIT_SECONDS) -> dict:
    """What POST /api/auth/demo runs: under ONE lock, wipe the demo venue when
    due and seed it when wiped (or empty, or a day behind). If another worker
    holds the lock it is mid-reset: wait for it (up to wait_seconds) rather
    than hand the visitor a half-wiped venue, then re-decide — its fresh stamp
    usually means there's nothing left to do."""
    with _demo_lock(db, wait_seconds) as held:
        if not held:
            return {"status": "busy"}
        now = now or datetime.utcnow()
        report = None
        if force or _should_reset(db, now, client_ip):
            try:
                db.save_to_key(_LAST_RESET_KEY, {"at": now.isoformat()})
                stamped = True
            except Exception:
                stamped = False                         # can't throttle: don't wipe
            if stamped:
                report = _pg_reset(db) if _is_pg(db) else _memory_reset(db)
                _reset_demo_users(db)
                _reset_process_state()
                logger.info("Demo venue reset: %s", {k: v for k, v in report["deleted"].items() if v})
                if report.get("skipped"):
                    logger.warning("Demo reset skipped steps: %s", report["skipped"])
        seed_status = _seed_if_needed(db, full=report is not None)
        try:
            db.save_to_key(_SESSION_KEY, {"at": now.isoformat(), "client": _tag(client_ip)})
        except Exception:
            pass
        return {"status": "reset" if report is not None else seed_status, "report": report}


def seed_on_boot(db, wait_seconds: float = 30.0) -> str:
    """Startup: both workers boot at once, so seed under the same lock as a
    reset (two unlocked seeds each insert the default checklists)."""
    with _demo_lock(db, wait_seconds) as held:
        if not held:
            return "busy"
        return _seed_if_needed(db, full=False)


def reset_demo_venue(db, now: Optional[datetime] = None, force: bool = False) -> Optional[dict]:
    """Wipe only (no seed). Returns the report, or None when skipped."""
    now = now or datetime.utcnow()
    if not force and not _should_reset(db, now, None):
        return None
    if not _lock.acquire(blocking=False):
        return None
    try:
        db.save_to_key(_LAST_RESET_KEY, {"at": now.isoformat()})
        report = _pg_reset(db) if _is_pg(db) else _memory_reset(db)
        _reset_demo_users(db)
        _reset_process_state()
        return report
    finally:
        _lock.release()
