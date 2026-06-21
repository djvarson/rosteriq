"""
Per-venue connection status — merges the connector catalog (directions/forms) with
a venue's live install records so the Connections hub can show, for every
integration: connected / pending / disconnected, plus whether each OAuth method is
actually configured server-side (so we never show a one-click button that 500s).
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from rosteriq.services.connector_registry import get_catalog


# organisation_id prefix -> connector key, for installs that don't carry an
# explicit `provider` field (fallback only; most routes set `provider`).
_PREFIX_TO_KEY = {
    "deputy_": "deputy",
    "humanforce_": "humanforce",
    "myob_": "myob",
    "swiftpos_": "swiftpos",
    "lightspeed_": "lightspeed",
    "kounta_": "kounta",
    "resdiary_reservations_": "resdiary",
    "nowbookit_reservations_": "nowbookit",
    "opentable_reservations_": "opentable",
    "sevenrooms_reservations_": "sevenrooms",
    "bookitlive_reservations_": "bookitlive",
}


def _install_status(record: Dict[str, Any]) -> str:
    st = (record.get("status") or "").lower()
    if st in ("active", "installed", "connected"):
        return "connected"
    if st in ("pending",):
        return "pending"
    return "disconnected"


def _infer_provider(record: Dict[str, Any]) -> Optional[str]:
    prov = record.get("provider")
    if prov:
        return prov
    org = record.get("organisation_id") or ""
    for prefix, key in _PREFIX_TO_KEY.items():
        if org.startswith(prefix):
            return key
    return None


def _method_with_readiness(method: Dict[str, Any]) -> Dict[str, Any]:
    """Annotate a connect method with whether its required server env is present."""
    mm = dict(method)
    missing = [e for e in (method.get("requires_env") or []) if not os.environ.get(e)]
    mm["server_ready"] = not missing
    mm["missing_env"] = missing
    return mm


def venue_connections(db, venue_id: str) -> List[Dict[str, Any]]:
    """
    Return one entry per connector with the venue's live status and connect spec.
    """
    try:
        installs = [
            i for i in db.list_plugin_installs() if i.get("venue_id") == venue_id
        ]
    except Exception:  # pragma: no cover - store may not implement it
        installs = []

    # Pick the best (prefer a connected) install per provider key.
    by_key: Dict[str, Dict[str, Any]] = {}
    for rec in installs:
        key = _infer_provider(rec)
        if not key:
            continue
        cur = by_key.get(key)
        if cur is None or (_install_status(rec) == "connected" and _install_status(cur) != "connected"):
            by_key[key] = rec

    xero_rec = None
    if hasattr(db, "get_xero_credentials"):
        try:
            xero_rec = db.get_xero_credentials(venue_id)
        except Exception:
            xero_rec = None

    out: List[Dict[str, Any]] = []
    for conn in get_catalog():
        key = conn["key"]
        status = "disconnected"
        detail: Dict[str, Any] = {}

        if key == "xero":
            if xero_rec:
                status = "connected"
                detail = {"tenant_id": xero_rec.get("tenant_id")}
        elif key == "direct_bookings":
            count = 0
            if hasattr(db, "count_direct_bookings"):
                try:
                    count = db.count_direct_bookings(venue_id)
                except Exception:
                    count = 0
            status = "connected" if count else "disconnected"
            detail = {"bookings": count}
        else:
            rec = by_key.get(key)
            if rec:
                status = _install_status(rec)
                tokens = rec.get("tokens") or {}
                if tokens.get("token_expires_at"):
                    detail["token_expires_at"] = tokens["token_expires_at"]
                if rec.get("installed_at"):
                    detail["installed_at"] = str(rec["installed_at"])

        out.append({
            "key": key,
            "name": conn["name"],
            "category": conn["category"],
            "summary": conn["summary"],
            "imports": conn.get("imports"),
            "docs_url": conn.get("docs_url"),
            "capabilities": conn.get("capabilities", []),
            "status": status,
            "detail": detail,
            "methods": [_method_with_readiness(m) for m in conn["methods"]],
        })
    return out


def connectors_with_capability(db, venue_id: str, capability: str) -> List[Dict[str, Any]]:
    """
    CONNECTED connectors for a venue that provide a given capability (e.g.
    'sales_actuals', 'bookings'). Lets the app ask "what feeds me sales actuals?"
    declaratively, so new connectors that declare the capability light up with no
    code change elsewhere.
    """
    return [
        c for c in venue_connections(db, venue_id)
        if c["status"] == "connected" and capability in (c.get("capabilities") or [])
    ]


def connection_summary(db, venue_id: str) -> Dict[str, Any]:
    """A small roll-up for dashboards: counts by status."""
    conns = venue_connections(db, venue_id)
    connected = [c for c in conns if c["status"] == "connected"]
    return {
        "venue_id": venue_id,
        "total": len(conns),
        "connected": len(connected),
        "connected_keys": [c["key"] for c in connected],
    }
