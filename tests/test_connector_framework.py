"""
Tests for the extensible connector framework: the generic connect/disconnect/test
dispatcher, declarative capabilities, and the key future-proofing guarantee — a
brand-new connector needs only a registry entry + an optional class, no FastAPI
route and no api.py edit.
"""

import asyncio

import pytest

from rosteriq.database import MemoryStore
from rosteriq.routes import connections as C
from rosteriq.services.connection_status import venue_connections, connectors_with_capability
from rosteriq.services.connector_registry import SCHEMA_VERSION, get_catalog
from rosteriq.services.connectors.base import Connector, register_connector


def _status_map(db, venue):
    return {c["key"]: c["status"] for c in venue_connections(db, venue)}


# --- catalog metadata ------------------------------------------------------

def test_catalog_exposes_schema_version_and_capabilities():
    assert SCHEMA_VERSION >= 1
    cat = {c["key"]: c for c in get_catalog()}
    assert "sales_actuals" in cat["swiftpos"]["capabilities"]
    assert "payroll_export" in cat["keypay"]["capabilities"]
    # every method now has a handler (custom by default, generic for framework ones)
    for c in get_catalog():
        for m in c["methods"]:
            assert m["handler"] in ("custom", "generic")


# --- the live 'custom' catch-all connector (built on the framework) --------

def test_custom_connector_connect_test_disconnect():
    db = MemoryStore()
    r = asyncio.run(C.generic_connect(
        C.GenericConnectRequest(venue_id="v1", fields={"api_key": "k-123", "base_url": "https://api.example.com"}),
        key="custom", db=db))
    assert r["status"] == "success"
    assert _status_map(db, "v1")["custom"] == "connected"

    h = asyncio.run(C.generic_test(C.GenericVenueRequest(venue_id="v1"), key="custom", db=db))
    assert h["health"]["healthy"] is True

    d = asyncio.run(C.generic_disconnect(C.GenericVenueRequest(venue_id="v1"), key="custom", db=db))
    assert d["status"] == "success"
    assert _status_map(db, "v1")["custom"] == "disconnected"


def test_custom_connect_accepts_flat_fields():
    """The hub posts flat {venue_id, api_key, ...}; the dispatcher must accept it."""
    db = MemoryStore()
    r = asyncio.run(C.generic_connect(
        C.GenericConnectRequest(venue_id="v1", api_key="k-1", base_url="https://x.io"),
        key="custom", db=db))
    assert r["status"] == "success"
    assert _status_map(db, "v1")["custom"] == "connected"


def test_custom_connector_validates_inputs():
    db = MemoryStore()
    # missing required api_key
    with pytest.raises(Exception) as e1:
        asyncio.run(C.generic_connect(C.GenericConnectRequest(venue_id="v1", fields={}), key="custom", db=db))
    assert "Missing required" in str(e1.value)
    # bad URL
    with pytest.raises(Exception) as e2:
        asyncio.run(C.generic_connect(
            C.GenericConnectRequest(venue_id="v1", fields={"api_key": "k", "base_url": "not-a-url"}),
            key="custom", db=db))
    assert "valid http" in str(e2.value)


def test_non_generic_connector_rejects_generic_connect():
    """Deputy connects via its own endpoint, not the generic dispatcher."""
    db = MemoryStore()
    with pytest.raises(Exception) as e:
        asyncio.run(C.generic_connect(
            C.GenericConnectRequest(venue_id="v1", fields={"x": "y"}), key="deputy", db=db))
    assert "own endpoint" in str(e.value)


def test_unknown_connector_404():
    db = MemoryStore()
    with pytest.raises(Exception) as e:
        asyncio.run(C.generic_connect(C.GenericConnectRequest(venue_id="v1", fields={}), key="nope", db=db))
    assert "Unknown connector" in str(e.value)


# --- THE future-proofing guarantee: a new connector with zero plumbing -----

def test_new_connector_needs_only_registry_entry_and_class():
    """
    Register a brand-new connector with ONLY a Connector subclass + a catalog
    entry, then connect it through the SAME generic endpoint — no new FastAPI
    route, no api.py change. This is the extensibility contract.
    """
    from rosteriq.services import connector_registry as REG

    class WidgetConnector(Connector):
        key = "widget_test"

        async def validate(self, ctx):
            return (ctx.fields.get("token") == "good"), "token must be 'good'"

        async def on_connect(self, ctx):
            return {"widgets_imported": 7}

    register_connector(WidgetConnector())
    entry = REG._normalise({
        "key": "widget_test", "name": "Widget", "category": "workforce",
        "summary": "x", "methods": [{
            "id": "t", "label": "t", "endpoint": "POST /api/connections/widget_test/connect",
            "handler": "generic",
            "fields": [{"name": "token", "label": "Token", "secret": True, "required": True,
                        "placeholder": "", "help": ""}],
            "directions": ["paste token"],
        }],
    })
    REG.CONNECTORS.append(entry)
    REG._BY_KEY["widget_test"] = entry
    try:
        db = MemoryStore()
        # validation hook is enforced
        with pytest.raises(Exception):
            asyncio.run(C.generic_connect(
                C.GenericConnectRequest(venue_id="v1", fields={"token": "bad"}), key="widget_test", db=db))
        # happy path: connects + runs on_connect, with no route added for it
        r = asyncio.run(C.generic_connect(
            C.GenericConnectRequest(venue_id="v1", fields={"token": "good"}), key="widget_test", db=db))
        assert r["status"] == "success"
        assert r["detail"] == {"widgets_imported": 7}
        assert _status_map(db, "v1")["widget_test"] == "connected"
    finally:
        REG.CONNECTORS[:] = [c for c in REG.CONNECTORS if c["key"] != "widget_test"]
        REG._BY_KEY.pop("widget_test", None)
        from rosteriq.services.connectors.base import _PROVIDERS
        _PROVIDERS.pop("widget_test", None)


# --- declarative capability query -----------------------------------------

def test_capability_query_finds_connected_providers():
    db = MemoryStore()
    # connect a sales_actuals provider the normal (bespoke) way
    db.save_plugin_install({
        "organisation_id": "swiftpos_v1", "venue_id": "v1", "provider": "swiftpos",
        "status": "active", "tokens": {},
    })
    sales = connectors_with_capability(db, "v1", "sales_actuals")
    assert [c["key"] for c in sales] == ["swiftpos"]
    # a venue with nothing connected has no providers for the capability
    assert connectors_with_capability(db, "other-venue", "sales_actuals") == []
