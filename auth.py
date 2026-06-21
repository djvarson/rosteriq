"""
Compatibility shim.

The canonical authentication helpers live in ``rosteriq.middleware.auth``.
Several route modules historically imported them from ``rosteriq.auth``; this
module re-exports the public names so those imports resolve correctly instead
of failing at startup (ModuleNotFoundError: No module named 'rosteriq.auth').

Prefer importing from ``rosteriq.middleware.auth`` directly in new code.
"""

from rosteriq.middleware.auth import (  # noqa: F401
    UserContext,
    get_current_user,
    get_current_user_optional,
    get_api_key_user,
    require_role,
    require_owner,
    require_venue_access,
    check_venue_access,
)

__all__ = [
    "UserContext",
    "get_current_user",
    "get_current_user_optional",
    "get_api_key_user",
    "require_role",
    "require_owner",
    "require_venue_access",
    "check_venue_access",
]
