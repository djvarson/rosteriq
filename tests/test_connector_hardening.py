"""
Connector hardening tests.

Covers four reliability/security fixes on the integration boundary:

1. Tanda token refresh fires the on_token_refresh persistence callback (so a
   refreshed token survives a process restart, like Deputy/MYOB/HumanForce).
2. POS realtime ingestion deduplicates by transaction id, so webhook
   re-delivery counts revenue exactly once.
3. Inbound webhooks with no configured secret are REJECTED in production
   (fail closed) while non-production keeps warn-and-accept.
"""

import os
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
import pytest
import respx

from rosteriq.models import TandaCredentials, State
from rosteriq.tanda_adapter import TandaAdapter
from rosteriq.services.tanda_plugin import TandaPluginService
from rosteriq.services.pos_realtime import (
    RevenueAccumulator,
    RealtimePOSFeed,
    StaffingTrigger,
)
from rosteriq.database import MemoryStore


# ---------------------------------------------------------------------------
# Fix 1 — Tanda token refresh fires the persistence callback
# ---------------------------------------------------------------------------


@respx.mock
async def test_tanda_refresh_fires_persistence_callback():
    """A successful refresh must invoke on_token_refresh with the new creds."""
    respx.post("https://my.tanda.co/api/oauth/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "tanda-new",
                "refresh_token": "tanda-refresh-2",
                "expires_in": 7200,
            },
        )
    )

    persisted = []
    creds = TandaCredentials(
        client_id="t-id",
        client_secret="t-secret",
        access_token="tanda-old",
        refresh_token="tanda-refresh-1",
        org_id="org-1",
    )
    adapter = TandaAdapter(creds, on_token_refresh=persisted.append)

    await adapter.refresh_token()

    # Callback fired exactly once, with the refreshed credentials object.
    assert len(persisted) == 1
    assert persisted[0] is creds
    assert creds.access_token == "tanda-new"
    assert creds.refresh_token == "tanda-refresh-2"


@respx.mock
async def test_tanda_plugin_build_adapter_persists_refreshed_token_to_store():
    """
    build_adapter() wires a callback that writes refreshed tokens back to the
    plugin install record, so they survive a restart.
    """
    respx.post("https://my.tanda.co/api/oauth/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "store-new",
                "refresh_token": "store-refresh-2",
                "expires_in": 7200,
            },
        )
    )

    db = MemoryStore()
    db.save_plugin_install(
        {
            "organisation_id": "org-99",
            "venue_id": "venue-99",
            "access_token": "store-old",
            "refresh_token": "store-refresh-1",
            "token_expires_at": datetime.utcnow() - timedelta(hours=1),
        }
    )

    service = TandaPluginService(
        db, tanda_client_id="cid", tanda_client_secret="csecret"
    )
    adapter = service.build_adapter("org-99", state_code="vic")

    await adapter.refresh_token()

    # The store now holds the refreshed tokens.
    record = db.get_plugin_install("org-99")
    assert record["access_token"] == "store-new"
    assert record["refresh_token"] == "store-refresh-2"


# ---------------------------------------------------------------------------
# Fix 3 — POS realtime sale dedup by transaction id
# ---------------------------------------------------------------------------


async def test_accumulator_dedups_same_transaction_id():
    """Same transaction id ingested twice counts revenue once."""
    acc = RevenueAccumulator()
    ts = datetime(2026, 6, 15, 12, 0, 0)

    first = await acc.add_transaction(
        "venue-1", Decimal("100.00"), timestamp=ts, transaction_id="txn-1"
    )
    second = await acc.add_transaction(
        "venue-1", Decimal("100.00"), timestamp=ts, transaction_id="txn-1"
    )

    assert first is True
    assert second is False  # duplicate rejected

    total = await acc.get_today_total("venue-1")
    assert total["total_revenue"] == 100.00
    assert total["transaction_count"] == 1


async def test_ingest_sale_dedups_redelivered_webhook():
    """ingest_sale via the feed dedups on the sale's transaction_id."""

    class _StubTrigger:
        async def check_and_trigger(self, *_a, **_k):
            return None

    acc = RevenueAccumulator()
    feed = RealtimePOSFeed(acc, _StubTrigger())

    sale = {
        "transaction_id": "pos-abc",
        "amount": Decimal("50.00"),
        "timestamp": "2026-06-15T13:00:00",
    }
    await feed.ingest_sale("venue-2", sale)
    await feed.ingest_sale("venue-2", dict(sale))  # webhook re-delivery

    total = await acc.get_today_total("venue-2")
    assert total["total_revenue"] == 50.00
    assert total["transaction_count"] == 1


async def test_reset_day_clears_seen_transaction_ids():
    """After reset_day the same transaction id may be counted again."""
    acc = RevenueAccumulator()
    ts = datetime(2026, 6, 15, 9, 0, 0)

    assert await acc.add_transaction(
        "venue-3", Decimal("10.00"), timestamp=ts, transaction_id="txn-x"
    )
    await acc.reset_day("venue-3")
    # Fresh day -> id no longer remembered, counts again.
    assert await acc.add_transaction(
        "venue-3", Decimal("10.00"), timestamp=ts, transaction_id="txn-x"
    )


# ---------------------------------------------------------------------------
# Fix 4 — inbound webhook secret mandatory in production
# ---------------------------------------------------------------------------


async def test_webhook_rejected_in_production_without_secret(monkeypatch):
    """In production, a missing secret must reject inbound webhooks (503)."""
    from fastapi import HTTPException
    from rosteriq.routes import webhook_routes

    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("TANDA_WEBHOOK_SECRET", raising=False)

    with pytest.raises(HTTPException) as exc:
        await webhook_routes.process_webhook_event(
            payload_dict={"event_type": "roster.published", "id": "wh-1"},
            raw_payload=b'{"event_type": "roster.published", "id": "wh-1"}',
            webhook_signature=None,
            webhook_secret=None,
        )

    assert exc.value.status_code == 503
    assert "verification not configured" in str(exc.value.detail)


async def test_webhook_warns_but_accepts_outside_production(monkeypatch):
    """Outside production, missing secret keeps warn-and-accept dev behaviour."""
    from rosteriq.routes import webhook_routes

    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("TANDA_WEBHOOK_SECRET", raising=False)

    # Should NOT raise 503 for the missing-secret case; it proceeds past the
    # verification block (and is handled as a normal — here, unknown — event).
    # We assert only that it does not fail closed with 503.
    from fastapi import HTTPException

    try:
        await webhook_routes.process_webhook_event(
            payload_dict={"event_type": "definitely.not.a.real.event", "id": "wh-2"},
            raw_payload=b'{"event_type": "definitely.not.a.real.event", "id": "wh-2"}',
            webhook_signature=None,
            webhook_secret=None,
        )
    except HTTPException as e:
        # A 400 (unknown event type) is fine — that's past the secret gate.
        assert e.status_code != 503, "must not fail closed outside production"
