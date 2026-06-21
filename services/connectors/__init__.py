"""Connector framework package — importing it self-registers built-in providers."""

from rosteriq.services.connectors.base import (  # noqa: F401
    Connector,
    ConnectorContext,
    CAPABILITIES,
    register_connector,
    get_connector_provider,
    registered_keys,
    connector,
)

# Import built-in connector providers so they self-register on package import.
from rosteriq.services.connectors import custom_connector  # noqa: F401,E402
