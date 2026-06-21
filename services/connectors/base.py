"""
Extensible connector framework.

The goal is future-proofing: adding a NEW integration should be a drop-in, not a
new FastAPI route + api.py edit + copy-pasted OAuth/persistence code. A connector
that just stores an API key needs *no code at all* (a catalog entry is enough);
one that wants to validate credentials or import data on connect implements only
the hooks it cares about.

A `Connector` is registered once (by key). The generic dispatcher
(routes/connections.py: POST /api/connections/{key}/connect|disconnect|test) then
drives the whole lifecycle from the declarative catalog entry + these hooks, with
uniform, encrypted persistence into plugin_installs.

  capabilities — a connector declares what it provides (e.g. "sales_actuals",
  "bookings", "import_staff", "payroll_export"). The rest of the app can ask
  "which connected systems give me sales actuals for this venue?" declaratively
  instead of hard-coding provider lists — so new providers light up automatically.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


# Known capability tags (open set — connectors may declare others). Documented
# here so callers have a canonical vocabulary to query against.
CAPABILITIES = {
    "import_staff",      # pulls employees into RosterIQ
    "import_rosters",    # pulls rosters/shifts
    "publish_rosters",   # pushes rosters back to the provider
    "sales_actuals",     # streams real sales (covers/revenue/transactions)
    "bookings",          # provides bookings/covers as a demand signal
    "revenue",           # provides revenue (accounting)
    "payroll_export",    # accepts exported timesheets for pay
    "custom",            # a bespoke/other system
}


@dataclass
class ConnectorContext:
    """Everything a connector hook needs, without coupling to FastAPI."""
    key: str
    venue_id: str
    fields: Dict[str, Any] = field(default_factory=dict)   # creds supplied on connect
    tokens: Dict[str, Any] = field(default_factory=dict)    # stored creds (disconnect/health)
    db: Any = None


class Connector:
    """
    Base class for a pluggable connector. Override only the hooks you need; the
    defaults make a credential-store-only connector work with no code.
    """

    key: str = ""

    async def validate(self, ctx: ConnectorContext) -> Tuple[bool, str]:
        """Return (ok, message). Default: accept. Override to test credentials."""
        return True, ""

    async def on_connect(self, ctx: ConnectorContext) -> Dict[str, Any]:
        """Post-connect side effects (e.g. import staff). Return extra detail to surface."""
        return {}

    async def on_disconnect(self, ctx: ConnectorContext) -> None:
        """Clean-up on disconnect (e.g. revoke a token). Default: nothing."""
        return None

    async def health(self, ctx: ConnectorContext) -> Dict[str, Any]:
        """Lightweight liveness check. Default: unknown (healthy=None)."""
        return {"healthy": None}


# --- registry --------------------------------------------------------------

_PROVIDERS: Dict[str, Connector] = {}


def register_connector(provider: Connector) -> Connector:
    """Register a connector instance under its `key`."""
    if not provider.key:
        raise ValueError("Connector.key must be set")
    _PROVIDERS[provider.key] = provider
    logger.debug(f"Registered connector provider: {provider.key}")
    return provider


def connector(cls):
    """Class decorator: instantiate and register the connector."""
    register_connector(cls())
    return cls


def get_connector_provider(key: str) -> Optional[Connector]:
    """Return the registered Connector for a key, or None (code-free connector)."""
    return _PROVIDERS.get(key)


def registered_keys() -> list:
    return list(_PROVIDERS.keys())
