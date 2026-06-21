"""
Connections hub API — one place to see every integration, its live status, and the
built-in directions to connect it.

  GET  /api/connections/catalog              — all connectors + connect directions
  GET  /api/connections/venue/{venue_id}     — per-venue live status + directions
  GET  /api/connections/venue/{venue_id}/summary — counts by status

The actual connect/disconnect actions stay on each provider's existing routes
(e.g. POST /api/deputy/install-token) — this hub tells the UI which endpoint and
fields to use, so the venue can self-serve without bespoke per-provider screens.
"""

import logging
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field

from rosteriq.database import BaseStore, get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.services.connector_registry import (
    get_catalog, get_connector, generic_method, CATEGORIES, SCHEMA_VERSION,
)
from rosteriq.services.connection_status import (
    venue_connections, connection_summary, connectors_with_capability,
)
# Importing the package self-registers the built-in connector providers.
from rosteriq.services import connectors as _connectors  # noqa: F401
from rosteriq.services.connectors.base import ConnectorContext, get_connector_provider

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/connections", tags=["connections"])


def _org_key(key: str, venue_id: str) -> str:
    """Uniform plugin_installs key for framework-managed (generic) connectors."""
    return f"conn_{key}_{venue_id}"


class GenericConnectRequest(BaseModel):
    # Accept either nested {venue_id, fields:{...}} or flat {venue_id, api_key, ...}
    # so the hub's existing flat connect form works unchanged for framework connectors.
    model_config = ConfigDict(extra="allow")
    venue_id: str = Field(..., description="RosterIQ venue ID")
    fields: Dict[str, Any] = Field(default_factory=dict, description="Credential fields the connector declares")

    def resolved_fields(self) -> Dict[str, Any]:
        if self.fields:
            return self.fields
        return dict(self.__pydantic_extra__ or {})


class GenericVenueRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")


@router.get("/catalog")
async def catalog():
    """All connectors with their connect directions and form specs (venue-agnostic)."""
    return {
        "schema_version": SCHEMA_VERSION,
        "categories": CATEGORIES,
        "connectors": get_catalog(),
    }


@router.get("/venue/{venue_id}")
async def venue_status(
    venue_id: str = Path(..., description="RosterIQ venue ID"),
    db: BaseStore = Depends(get_db),
):
    """Per-venue connection status merged with connect directions."""
    enforce_venue_access(venue_id)
    return {
        "venue_id": venue_id,
        "schema_version": SCHEMA_VERSION,
        "categories": CATEGORIES,
        "connectors": venue_connections(db, venue_id),
    }


@router.get("/venue/{venue_id}/summary")
async def venue_summary(
    venue_id: str = Path(..., description="RosterIQ venue ID"),
    db: BaseStore = Depends(get_db),
):
    """Roll-up counts (connected / total) for dashboards."""
    enforce_venue_access(venue_id)
    return connection_summary(db, venue_id)


@router.get("/venue/{venue_id}/capability/{capability}")
async def venue_capability(
    venue_id: str = Path(...),
    capability: str = Path(..., description="e.g. sales_actuals, bookings, payroll_export"),
    db: BaseStore = Depends(get_db),
):
    """Connected connectors for a venue that provide a capability (declarative)."""
    enforce_venue_access(venue_id)
    matches = connectors_with_capability(db, venue_id, capability)
    return {"venue_id": venue_id, "capability": capability,
            "connectors": [{"key": c["key"], "name": c["name"]} for c in matches]}


# ===========================================================================
# Generic connect dispatcher — the future-proof extension point.
# Any connector whose catalog entry declares a generic-handler method is fully
# driven from here: no per-provider route, no api.py edit. Adding a connector is
# a catalog entry (+ an optional Connector subclass for validation/import).
# ===========================================================================


def _require_generic(key: str) -> Dict[str, Any]:
    conn = get_connector(key)
    if not conn:
        raise HTTPException(status_code=404, detail=f"Unknown connector '{key}'")
    method = generic_method(conn)
    if not method:
        endpoints = [m.get("endpoint") for m in conn.get("methods", [])]
        raise HTTPException(
            status_code=400,
            detail=f"'{key}' connects via its own endpoint(s): {endpoints}",
        )
    return method


@router.post("/{key}/connect")
async def generic_connect(
    body: GenericConnectRequest,
    key: str = Path(..., description="Connector key"),
    db: BaseStore = Depends(get_db),
):
    """Connect a framework-managed connector: validate required fields, run the
    connector's validate/on_connect hooks, persist (encrypted), return status."""
    enforce_venue_access(body.venue_id)
    method = _require_generic(key)
    fields = body.resolved_fields()
    required = [f["name"] for f in method.get("fields", []) if f.get("required")]
    missing = [r for r in required if not fields.get(r)]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required field(s): {', '.join(missing)}")

    ctx = ConnectorContext(key=key, venue_id=body.venue_id, fields=fields, db=db)
    provider = get_connector_provider(key)
    if provider:
        ok, message = await provider.validate(ctx)
        if not ok:
            raise HTTPException(status_code=400, detail=message or "Validation failed")

    record = {
        "organisation_id": _org_key(key, body.venue_id),
        "venue_id": body.venue_id,
        "provider": key,
        "status": "active",
        "tokens": dict(fields),
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(record)

    detail: Dict[str, Any] = {}
    if provider:
        ctx.tokens = dict(fields)
        try:
            detail = await provider.on_connect(ctx) or {}
        except Exception as e:  # pragma: no cover - on_connect is best-effort
            logger.warning(f"{key} on_connect hook failed: {e}")
            detail = {"on_connect_error": str(e)}

    logger.info(f"Connector '{key}' connected for venue {body.venue_id}")
    return {"status": "success", "key": key, "venue_id": body.venue_id, "detail": detail}


@router.post("/{key}/disconnect")
async def generic_disconnect(
    body: GenericVenueRequest,
    key: str = Path(...),
    db: BaseStore = Depends(get_db),
):
    """Disconnect a framework-managed connector."""
    enforce_venue_access(body.venue_id)
    _require_generic(key)
    org_key = _org_key(key, body.venue_id)
    install = db.get_plugin_install(org_key)
    if not install:
        raise HTTPException(status_code=404, detail=f"No {key} connection for venue {body.venue_id}")
    provider = get_connector_provider(key)
    if provider:
        try:
            await provider.on_disconnect(ConnectorContext(
                key=key, venue_id=body.venue_id, tokens=install.get("tokens", {}), db=db))
        except Exception as e:  # pragma: no cover
            logger.warning(f"{key} on_disconnect hook failed: {e}")
    install["status"] = "uninstalled"
    install["updated_at"] = datetime.utcnow()
    install["tokens"] = {}
    db.save_plugin_install(install)
    return {"status": "success", "key": key, "venue_id": body.venue_id}


@router.post("/{key}/test")
async def generic_test(
    body: GenericVenueRequest,
    key: str = Path(...),
    db: BaseStore = Depends(get_db),
):
    """Run a connector's health check against its stored credentials."""
    enforce_venue_access(body.venue_id)
    _require_generic(key)
    install = db.get_plugin_install(_org_key(key, body.venue_id))
    if not install or install.get("status") != "active":
        raise HTTPException(status_code=404, detail=f"No active {key} connection for venue {body.venue_id}")
    provider = get_connector_provider(key)
    if not provider:
        return {"key": key, "venue_id": body.venue_id, "health": {"healthy": None}}
    health = await provider.health(ConnectorContext(
        key=key, venue_id=body.venue_id, tokens=install.get("tokens", {}), db=db))
    return {"key": key, "venue_id": body.venue_id, "health": health}
