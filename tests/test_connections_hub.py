"""
Tests for the Connections hub (catalog + per-venue status) and the generic
booking ingestion path (CSV/webhook) that lets any booking system feed demand.
"""

import asyncio
import base64

from rosteriq.database import MemoryStore
from rosteriq.services.connector_registry import get_catalog, get_connector, known_keys
from rosteriq.services.connection_status import venue_connections, connection_summary
from rosteriq.routes import direct_bookings as dbm


# --- catalog ---------------------------------------------------------------

def test_catalog_has_core_connectors():
    keys = known_keys()
    for expected in ("deputy", "swiftpos", "xero", "keypay", "direct_bookings",
                     "resdiary", "sevenrooms"):
        assert expected in keys


def test_every_connector_has_directions_and_endpoint():
    for conn in get_catalog():
        assert conn["methods"], f"{conn['key']} has no connect methods"
        for m in conn["methods"]:
            assert m.get("endpoint"), f"{conn['key']}/{m.get('id')} missing endpoint"
            # endpoint is 'VERB /path'
            verb, _, path = m["endpoint"].partition(" ")
            assert verb in {"GET", "POST"} and path.startswith("/")
            assert m.get("directions"), f"{conn['key']}/{m['id']} has no directions"


def test_paste_methods_have_fields():
    deputy = get_connector("deputy")
    token_method = [m for m in deputy["methods"] if m["id"] == "api_token"][0]
    names = [f["name"] for f in token_method["fields"]]
    assert "subdomain" in names and "access_token" in names
    # secret fields flagged
    assert any(f["secret"] for f in token_method["fields"])


# --- per-venue status ------------------------------------------------------

def test_status_reflects_installs():
    db = MemoryStore()
    db.save_plugin_install({
        "organisation_id": "deputy_v1", "venue_id": "v1", "provider": "deputy",
        "status": "active", "tokens": {"token_expires_at": "2026-07-01T00:00:00"},
    })
    conns = {c["key"]: c for c in venue_connections(db, "v1")}
    assert conns["deputy"]["status"] == "connected"
    assert conns["swiftpos"]["status"] == "disconnected"
    summary = connection_summary(db, "v1")
    assert summary["connected"] == 1 and "deputy" in summary["connected_keys"]


def test_oauth_method_flags_missing_server_env(monkeypatch):
    monkeypatch.delenv("XERO_CLIENT_ID", raising=False)
    db = MemoryStore()
    xero = {c["key"]: c for c in venue_connections(db, "v1")}["xero"]
    method = xero["methods"][0]
    assert method["server_ready"] is False
    assert "XERO_CLIENT_ID" in method["missing_env"]


def test_status_infers_provider_from_org_prefix_without_provider_field():
    db = MemoryStore()
    # No explicit 'provider' field — must be inferred from the org_key prefix.
    db.save_plugin_install({
        "organisation_id": "swiftpos_v1", "venue_id": "v1",
        "status": "active", "tokens": {},
    })
    conns = {c["key"]: c for c in venue_connections(db, "v1")}
    assert conns["swiftpos"]["status"] == "connected"


# --- generic booking ingestion --------------------------------------------

def test_direct_booking_ingest_and_signals():
    db = MemoryStore()
    req = dbm.IngestRequest(venue_id="v1", bookings=[
        dbm.DirectBooking(date="2026-06-22", party_size=120, time="19:00"),
        dbm.DirectBooking(date="2026-06-22", party_size=80, time="21:30"),
        dbm.DirectBooking(date="2026-06-23", party_size=60),
    ])
    r = asyncio.run(dbm.ingest(req, db))
    assert r["stored"] == 3 and r["total_bookings"] == 3

    sig = asyncio.run(dbm.signals("v1", "2026-06-22", "2026-06-23", db))
    by_date = {s["date"]: s for s in sig["signals"]}
    assert by_date["2026-06-22"]["covers"] == 200.0  # 120 + 80
    assert by_date["2026-06-22"]["bookings"] == 2
    # direct_bookings now shows connected in the hub
    conns = {c["key"]: c for c in venue_connections(db, "v1")}
    assert conns["direct_bookings"]["status"] == "connected"
    assert conns["direct_bookings"]["detail"]["bookings"] == 3


def test_direct_booking_reupload_is_idempotent():
    """Re-uploading the same bookings must NOT double-count (was a pure append)."""
    db = MemoryStore()
    rows = [{"date": "2026-06-22", "party_size": 90, "time": "19:00"},
            {"date": "2026-06-22", "party_size": 40, "time": "21:00"}]
    assert db.save_direct_bookings("v1", rows) == 2
    assert db.save_direct_bookings("v1", rows) == 0   # identical re-upload adds nothing
    assert db.count_direct_bookings("v1") == 2
    # a genuinely new booking still gets added
    assert db.save_direct_bookings("v1", [{"date": "2026-06-23", "party_size": 20}]) == 1
    assert db.count_direct_bookings("v1") == 3


def test_direct_booking_csv_upload():
    db = MemoryStore()
    csv = "date,covers,time\n2026-06-24,200,18:00\n2026-06-24,50,20:00\n"
    up = dbm.UploadRequest(venue_id="v2", csv_data=base64.b64encode(csv.encode()).decode())
    r = asyncio.run(dbm.upload(up, db))
    assert r["stored"] == 2
    assert db.count_direct_bookings("v2") == 2


def test_direct_booking_rejects_rows_without_date():
    db = MemoryStore()
    req = dbm.IngestRequest(venue_id="v1", bookings=[dbm.DirectBooking(date="", party_size=10)])
    try:
        asyncio.run(dbm.ingest(req, db))
        assert False, "expected HTTPException"
    except Exception as e:
        assert "valid bookings" in str(e) or "400" in str(e)
