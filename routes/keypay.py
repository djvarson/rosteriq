"""
KeyPay (Employment Hero Payroll) connect routes — give KeyPay the same self-serve
connect/status/disconnect lifecycle as the other connectors, so it shows up in the
Connections hub. KeyPay uses a static API key + business ID (no OAuth); RosterIQ
exports timesheets to it for pay processing.

  POST /api/keypay/install    {venue_id, api_key, business_id}
  GET  /api/keypay/status     ?venue_id
  POST /api/keypay/uninstall  {venue_id}
"""

import logging
from datetime import datetime

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services.keypay_export import KeyPayClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/keypay", tags=["keypay"])

KEYPAY_ORG_PREFIX = "keypay_"


def _org_key(venue_id: str) -> str:
    return f"{KEYPAY_ORG_PREFIX}{venue_id}"


class KeyPayInstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    api_key: str = Field(..., min_length=8, description="KeyPay API key")
    business_id: str = Field(..., min_length=1, description="KeyPay business/account ID")


class KeyPayVenueRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")


async def _validate(api_key: str, business_id: str) -> bool:
    """Best-effort credential check: reject only on a clear auth failure."""
    url = f"{KeyPayClient.API_BASE}/business/{business_id}/employees"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers={"X-Api-Key": api_key}, timeout=10.0)
    except Exception as e:  # network error — don't hard-fail the connect
        logger.warning(f"KeyPay validation request failed (storing anyway): {e}")
        return True
    if resp.status_code in (401, 403):
        return False
    return True


@router.post("/install")
async def install(body: KeyPayInstallRequest):
    """Connect KeyPay for a venue (validates the key, then stores it)."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    if not await _validate(body.api_key, body.business_id):
        raise HTTPException(
            status_code=400,
            detail="KeyPay rejected the API key / business ID. Check both and retry.",
        )

    install_record = {
        "organisation_id": _org_key(body.venue_id),
        "venue_id": body.venue_id,
        "provider": "keypay",
        "status": "active",
        "tokens": {"api_key": body.api_key, "business_id": body.business_id},
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(install_record)
    logger.info(f"KeyPay connected for venue {body.venue_id}")
    return {"status": "success", "provider": "keypay", "venue_id": body.venue_id}


@router.get("/status")
async def status(venue_id: str):
    """Connection status for KeyPay at a venue."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))
    if not install or install.get("status") != "active":
        return {"provider": "keypay", "venue_id": venue_id, "status": "not_installed"}
    tokens = install.get("tokens", {})
    return {
        "provider": "keypay",
        "venue_id": venue_id,
        "status": "active",
        "business_id": tokens.get("business_id"),
        "installed_at": str(install.get("installed_at")),
    }


@router.post("/uninstall")
async def uninstall(body: KeyPayVenueRequest):
    """Disconnect KeyPay for a venue."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    org_key = _org_key(body.venue_id)
    install = db.get_plugin_install(org_key)
    if not install:
        raise HTTPException(status_code=404, detail=f"No KeyPay connection for venue {body.venue_id}")
    install["status"] = "uninstalled"
    install["updated_at"] = datetime.utcnow()
    install["tokens"] = {}
    db.save_plugin_install(install)
    logger.info(f"KeyPay disconnected for venue {body.venue_id}")
    return {"status": "success", "provider": "keypay", "venue_id": body.venue_id}
