"""
Connector registry — the single source of truth for every external integration a
venue can connect, plus the in-app DIRECTIONS for connecting each one.

This powers the Connections hub: the catalog (what can be connected + how) and,
combined with the live install records, the per-venue connection status. The goal
is that a pilot venue can self-serve — every connector carries human directions
(where to get the credentials) and a machine-readable form spec (what to paste),
so the UI can render a working connect flow without bespoke per-provider code.

Auth styles:
  - "api_token" / "api_key": the venue pastes a key/token we store. No app
    registration needed — the fastest path for a pilot.
  - "oauth": one-click redirect; requires RosterIQ to be registered as an app with
    the provider first (env client id/secret + redirect URI). `requires_env` lists
    what must be configured server-side before the button works.
  - "marketplace": installed from the provider's marketplace (e.g. Tanda), not a
    paste flow.
  - "csv": file upload / direct ingestion (generic bookings).

Each connector's `plugin_installs` records carry a `provider` field and `venue_id`,
so live status is read uniformly by scanning installs for the venue (see
services/connection_status.py). Xero uses its own credentials table.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Bump when the catalog/response shape changes in a way clients must notice.
# Clients should tolerate unknown connectors/fields/capabilities (forward-compat).
SCHEMA_VERSION = 1

# Declarative capabilities per connector — what each integration provides. The app
# queries these (connectors_with_capability) instead of hard-coding provider lists,
# so a new connector that declares a capability is picked up automatically.
CONNECTOR_CAPABILITIES: Dict[str, List[str]] = {
    "deputy": ["import_staff", "import_rosters", "publish_rosters"],
    "tanda": ["import_staff", "import_rosters", "publish_rosters"],
    "humanforce": ["import_staff", "import_rosters", "publish_rosters"],
    "swiftpos": ["sales_actuals"],
    "lightspeed": ["sales_actuals"],
    "kounta": ["sales_actuals"],
    "resdiary": ["bookings"],
    "nowbookit": ["bookings"],
    "opentable": ["bookings"],
    "sevenrooms": ["bookings"],
    "bookitlive": ["bookings"],
    "direct_bookings": ["bookings"],
    "xero": ["revenue", "payroll_export"],
    "myob": ["import_staff", "payroll_export"],
    "keypay": ["payroll_export"],
    "custom": ["custom"],
}


# A field the venue fills in when connecting via a paste flow.
def _field(name: str, label: str, *, secret: bool = False,
           required: bool = True, placeholder: str = "", help: str = "") -> Dict[str, Any]:
    return {
        "name": name,
        "label": label,
        "secret": secret,
        "required": required,
        "placeholder": placeholder,
        "help": help,
    }


# ---------------------------------------------------------------------------
# Category metadata
# ---------------------------------------------------------------------------

CATEGORIES = {
    "workforce": "Workforce & Rostering",
    "pos": "Point of Sale",
    "reservations": "Bookings & Reservations",
    "accounting": "Accounting & Payroll",
}


# ---------------------------------------------------------------------------
# The connectors
# ---------------------------------------------------------------------------

CONNECTORS: List[Dict[str, Any]] = [
    # ---- Workforce / rostering ------------------------------------------
    {
        "key": "deputy",
        "name": "Deputy",
        "category": "workforce",
        "summary": "Imports your staff and rosters, and can publish rosters back.",
        "imports": "Staff + the next 14 days of rosters on connect.",
        "docs_url": "https://www.deputy.com/api-doc",
        "methods": [
            {
                "id": "api_token",
                "label": "Paste a permanent token (simplest)",
                "endpoint": "POST /api/deputy/install-token",
                "fields": [
                    _field("subdomain", "Deputy subdomain", placeholder="mycompany",
                           help="The part before '.deputy.com' in your Deputy web address."),
                    _field("access_token", "Permanent access token", secret=True,
                           help="In Deputy: Integrations → Install new → 'Deputy API' → "
                                "generate a Permanent Token, then paste it here."),
                ],
                "directions": [
                    "Log in to Deputy as an administrator.",
                    "Go to Integrations → Install new → search 'Deputy API'.",
                    "Generate a Permanent Token and copy it.",
                    "Enter your Deputy subdomain and paste the token, then Connect.",
                ],
            },
            {
                "id": "oauth",
                "label": "Connect with Deputy login (OAuth)",
                "endpoint": "POST /api/deputy/install",
                "requires_env": ["DEPUTY_CLIENT_ID", "DEPUTY_CLIENT_SECRET", "DEPUTY_REDIRECT_URI"],
                "directions": [
                    "Click Connect — you'll be redirected to Deputy to log in.",
                    "Authorise RosterIQ; you'll be returned here once connected.",
                ],
            },
        ],
    },
    {
        "key": "tanda",
        "name": "Tanda",
        "category": "workforce",
        "summary": "Imports staff and rosters; installed from the Tanda Marketplace.",
        "imports": "Staff + current rosters on install.",
        "docs_url": "https://my.tanda.co/api/v2/documentation",
        "methods": [
            {
                "id": "marketplace",
                "label": "Install from the Tanda Marketplace",
                "endpoint": "POST /api/tanda/plugin/install",
                "directions": [
                    "In Tanda, open the Marketplace and find RosterIQ.",
                    "Click Install — Tanda will connect it to this account automatically.",
                    "Return here; your staff and rosters import in the background.",
                ],
            },
        ],
    },
    {
        "key": "humanforce",
        "name": "Humanforce",
        "category": "workforce",
        "summary": "Imports staff and rosters, and can publish rosters back.",
        "imports": "Staff + rosters on connect.",
        "docs_url": "https://www.humanforce.com",
        "methods": [
            {
                "id": "oauth",
                "label": "Connect with Humanforce login (OAuth)",
                "endpoint": "POST /api/humanforce/install",
                "requires_env": ["HUMANFORCE_CLIENT_ID", "HUMANFORCE_CLIENT_SECRET",
                                 "HUMANFORCE_REDIRECT_URI"],
                "directions": [
                    "Click Connect — you'll be redirected to Humanforce to log in.",
                    "Authorise RosterIQ; you'll be returned here once connected.",
                ],
            },
        ],
    },

    # ---- Point of sale ---------------------------------------------------
    {
        "key": "swiftpos",
        "name": "SwiftPOS",
        "category": "pos",
        "summary": "Streams hourly sales so forecasts are graded against real takings.",
        "imports": "Hourly revenue, transactions and covers.",
        "docs_url": "https://www.swiftpos.com.au",
        "methods": [
            {
                "id": "api_token",
                "label": "Paste API credentials",
                "endpoint": "POST /api/pos/swiftpos/install",
                "fields": [
                    _field("client_id", "SwiftPOS Client ID",
                           help="From your SwiftPOS API application (ask your SwiftPOS rep to enable API access)."),
                    _field("client_secret", "SwiftPOS Client Secret", secret=True),
                ],
                "directions": [
                    "Ask SwiftPOS to enable API access for your venue and issue API credentials.",
                    "Paste the Client ID and Client Secret, then Connect.",
                    "We validate the credentials before saving.",
                ],
            },
        ],
    },
    {
        "key": "lightspeed",
        "name": "Lightspeed Restaurant",
        "category": "pos",
        "summary": "Streams hourly sales so forecasts are graded against real takings.",
        "imports": "Hourly revenue, transactions and covers.",
        "docs_url": "https://developers.lightspeedhq.com",
        "methods": [
            {
                "id": "api_token",
                "label": "Paste API credentials",
                "endpoint": "POST /api/pos/lightspeed/install",
                "fields": [
                    _field("client_id", "Client ID"),
                    _field("client_secret", "Client Secret", secret=True),
                    _field("refresh_token", "Refresh token", secret=True,
                           help="From the Lightspeed OAuth authorisation for your account."),
                ],
                "directions": [
                    "In the Lightspeed developer portal, create an API client for your account.",
                    "Authorise it and copy the Client ID, Secret and Refresh Token.",
                    "Paste them here, then Connect.",
                ],
            },
        ],
    },
    {
        "key": "kounta",
        "name": "Kounta (Lightspeed K-Series)",
        "category": "pos",
        "summary": "Streams hourly sales so forecasts are graded against real takings.",
        "imports": "Hourly revenue, transactions and covers.",
        "docs_url": "https://developers.kounta.com",
        "methods": [
            {
                "id": "api_token",
                "label": "Paste an API key",
                "endpoint": "POST /api/pos/kounta/install",
                "fields": [
                    _field("api_key", "Kounta API key", secret=True),
                ],
                "directions": [
                    "In Kounta, open Add-ons / API and generate an API key.",
                    "Paste the key here, then Connect.",
                ],
            },
        ],
    },

    # ---- Reservations / bookings ----------------------------------------
    *[
        {
            "key": key,
            "name": name,
            "category": "reservations",
            "summary": "Imports bookings/covers as a demand signal for forecasting.",
            "imports": "Bookings, covers and party sizes by day.",
            "docs_url": docs,
            "methods": [
                {
                    "id": "api_key",
                    "label": "Paste an API key",
                    "endpoint": f"POST /api/reservations/{key}/install",
                    "fields": [
                        _field("api_key", f"{name} API key / token", secret=True),
                        _field("provider_venue_id", f"{name} venue ID", required=False,
                               help="Optional — only if your ID in {0} differs from RosterIQ.".format(name)),
                    ],
                    "directions": [
                        f"In {name}, open the integrations/API settings and create an API key.",
                        "Paste the key here (and the provider venue ID if different), then Connect.",
                    ],
                },
            ],
        }
        for key, name, docs in [
            ("resdiary", "ResDiary", "https://www.resdiary.com"),
            ("nowbookit", "NowBookIt", "https://www.nowbookit.com"),
            ("opentable", "OpenTable", "https://www.opentable.com"),
            ("sevenrooms", "SevenRooms", "https://sevenrooms.com"),
            ("bookitlive", "bookitLive", "https://www.bookitlive.net"),
        ]
    ],
    {
        "key": "direct_bookings",
        "name": "Other booking system (file import)",
        "category": "reservations",
        "summary": "For any booking/ticketing system not listed — upload a CSV or "
                   "send bookings to a webhook.",
        "imports": "Bookings/covers by day (and time, if provided).",
        "docs_url": "",
        "methods": [
            {
                "id": "csv",
                "label": "Upload a bookings CSV",
                "endpoint": "POST /api/reservations/direct/upload",
                "fields": [
                    _field("csv_data", "Bookings CSV", required=True,
                           help="Columns: date (YYYY-MM-DD), party_size (or covers), "
                                "and optionally time (HH:MM). One row per booking."),
                ],
                "directions": [
                    "Export your bookings to CSV with columns: date, party_size, time (optional).",
                    "Upload the file here — bookings become a demand signal for those days.",
                    "Re-upload whenever you have new bookings (e.g. nightly).",
                ],
            },
            {
                "id": "webhook",
                "label": "Send bookings to a webhook (JSON)",
                "endpoint": "POST /api/reservations/direct/ingest",
                "directions": [
                    "POST JSON {venue_id, bookings:[{date, party_size, time?}]} to the ingest endpoint.",
                    "Useful if your booking system can fire webhooks on new bookings.",
                ],
            },
        ],
    },

    # ---- Accounting / payroll -------------------------------------------
    {
        "key": "xero",
        "name": "Xero",
        "category": "accounting",
        "summary": "Pulls revenue and pushes labour-cost journals; powers P&L / labour %.",
        "imports": "Daily revenue; exports wages + super journals.",
        "docs_url": "https://developer.xero.com",
        "methods": [
            {
                "id": "oauth",
                "label": "Connect with Xero login (OAuth)",
                "endpoint": "POST /api/xero/connect",
                "requires_env": ["XERO_CLIENT_ID", "XERO_CLIENT_SECRET", "XERO_REDIRECT_URI"],
                "directions": [
                    "Click Connect — you'll be redirected to Xero to log in.",
                    "Choose the organisation and authorise RosterIQ.",
                ],
            },
        ],
    },
    {
        "key": "myob",
        "name": "MYOB",
        "category": "accounting",
        "summary": "Imports staff and exports timesheets for payroll.",
        "imports": "Staff; exports timesheets to a company file.",
        "docs_url": "https://developer.myob.com",
        "methods": [
            {
                "id": "api_token",
                "label": "Paste API credentials",
                "endpoint": "POST /api/myob/install-token",
                "fields": [
                    _field("api_key", "MYOB API key"),
                    _field("api_secret", "MYOB API secret", secret=True),
                    _field("access_token", "Access token", secret=True),
                    _field("company_file_uri", "Company file URI",
                           help="From MYOB → the company file you want timesheets exported to."),
                ],
                "directions": [
                    "Register an app at developer.myob.com to get an API key/secret.",
                    "Authorise it and copy the access token and your company file URI.",
                    "Paste them here, then Connect.",
                ],
            },
            {
                "id": "oauth",
                "label": "Connect with MYOB login (OAuth)",
                "endpoint": "POST /api/myob/install",
                "requires_env": ["MYOB_CLIENT_ID", "MYOB_CLIENT_SECRET", "MYOB_REDIRECT_URI"],
                "directions": [
                    "Click Connect — you'll be redirected to MYOB to log in.",
                    "Authorise RosterIQ and choose your company file.",
                ],
            },
        ],
    },
    {
        "key": "keypay",
        "name": "KeyPay (Employment Hero Payroll)",
        "category": "accounting",
        "summary": "Exports timesheets to KeyPay for pay processing.",
        "imports": "Exports timesheets (push only).",
        "docs_url": "https://api.keypay.com.au",
        "methods": [
            {
                "id": "api_token",
                "label": "Paste an API key",
                "endpoint": "POST /api/keypay/install",
                "fields": [
                    _field("api_key", "KeyPay API key", secret=True,
                           help="In KeyPay: your profile → Manage → API Key."),
                    _field("business_id", "KeyPay Business ID",
                           help="The numeric business/account ID in KeyPay."),
                ],
                "directions": [
                    "In KeyPay, open your profile → Manage → generate an API Key.",
                    "Find your Business ID (in the business settings / URL).",
                    "Paste both here, then Connect.",
                ],
            },
        ],
    },

    # ---- Catch-all (framework reference; connects via the generic dispatcher) --
    {
        "key": "custom",
        "name": "Other system (custom)",
        "category": "workforce",
        "summary": "Connect any other system by API key (and optional webhook/base URL).",
        "imports": "Whatever that system sends; a catch-all for tools without a native connector.",
        "docs_url": "",
        "methods": [
            {
                "id": "api_token",
                "label": "Store an API key",
                "endpoint": "POST /api/connections/custom/connect",
                "handler": "generic",
                "fields": [
                    _field("api_key", "API key", secret=True),
                    _field("base_url", "Base URL", required=False, placeholder="https://api.example.com",
                           help="Optional — the system's API base URL."),
                    _field("webhook_url", "Webhook URL", required=False,
                           help="Optional — where that system posts events."),
                ],
                "directions": [
                    "Get an API key from the system you want to connect.",
                    "Paste it here (and a base/webhook URL if you have one), then Connect.",
                ],
            },
        ],
    },
]


def _normalise(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Attach declarative capabilities and default each method's handler.

    Methods default to handler="custom" (they connect via their own bespoke
    route); handler="generic" routes through the framework dispatcher.
    """
    conn.setdefault("capabilities", CONNECTOR_CAPABILITIES.get(conn["key"], []))
    for m in conn["methods"]:
        m.setdefault("handler", "custom")
    return conn


CONNECTORS = [_normalise(c) for c in CONNECTORS]
_BY_KEY: Dict[str, Dict[str, Any]] = {c["key"]: c for c in CONNECTORS}


def get_catalog() -> List[Dict[str, Any]]:
    """Return all connectors (connect directions, form specs, capabilities)."""
    return CONNECTORS


def get_connector(key: str) -> Optional[Dict[str, Any]]:
    """Return one connector's spec by key, or None."""
    return _BY_KEY.get(key)


def get_capabilities(key: str) -> List[str]:
    """Capabilities a connector provides (e.g. ['sales_actuals'])."""
    conn = _BY_KEY.get(key)
    return list(conn.get("capabilities", [])) if conn else []


def generic_method(conn: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the connector's generic-handler connect method, or None."""
    for m in conn.get("methods", []):
        if m.get("handler") == "generic":
            return m
    return None


def known_keys() -> List[str]:
    return list(_BY_KEY.keys())
