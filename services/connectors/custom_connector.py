"""
Reference connector built on the framework: a catch-all "Custom" integration for
any system without a dedicated adapter. The venue stores an API key (and optionally
a base/webhook URL); future data can flow in via that URL. Demonstrates the
zero-route extension path — this connector adds no FastAPI route of its own; the
generic dispatcher handles its whole lifecycle from the catalog entry + these hooks.
"""

from urllib.parse import urlparse

from rosteriq.services.connectors.base import Connector, ConnectorContext, register_connector


class CustomConnector(Connector):
    key = "custom"

    async def validate(self, ctx: ConnectorContext):
        if not ctx.fields.get("api_key"):
            return False, "An API key is required."
        for url_field in ("base_url", "webhook_url"):
            url = ctx.fields.get(url_field)
            if url:
                parsed = urlparse(url)
                if parsed.scheme not in ("http", "https") or not parsed.netloc:
                    return False, f"{url_field} must be a valid http(s) URL."
        return True, ""

    async def health(self, ctx: ConnectorContext):
        return {"healthy": bool(ctx.tokens.get("api_key"))}


register_connector(CustomConnector())
