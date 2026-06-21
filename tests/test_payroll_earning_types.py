"""
Tests for Xero EarningTypeID GUID resolution and Stripe tier price resolution.

Xero rejects timesheet lines whose EarningTypeID is not a real tenant GUID, and
Stripe rejects a fabricated price id — both used to silently ship placeholders
("Ordinary Hours" / "price_pro"). These pin the corrected behaviour: resolve to
real configured/fetched IDs, and fail loud rather than push a placeholder.
"""

import os
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
import respx

from rosteriq.services.xero_payroll import XeroPayrollClient, XeroEarningType
from rosteriq.xero_integration import XeroCredentials
from rosteriq.services.billing import BillingService, SubscriptionTier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _xero_creds() -> XeroCredentials:
    now = datetime.utcnow()
    return XeroCredentials(
        venue_id="venue-1", client_id="c", client_secret="s",
        tenant_id="tenant-uuid", access_token="tok", refresh_token="r",
        token_expires=now + timedelta(hours=1), created_at=now, updated_at=now,
    )


def _emp_payroll():
    return SimpleNamespace(
        name="Alice",
        ordinary_hours=Decimal("38"),
        ordinary_rate=Decimal("30.00"),
        penalty_entries=[
            SimpleNamespace(penalty_type="saturday", hours=Decimal("8"),
                            multiplier=Decimal("1.25")),
        ],
        overtime_hours=Decimal("0"),
        allowances=[],
    )


# ---------------------------------------------------------------------------
# Xero EarningTypeID resolution
# ---------------------------------------------------------------------------

def test_configured_map_resolves_names_to_guids():
    """A configured earnings_rate_ids map is used to emit GUIDs, not names."""
    rate_map = {
        XeroEarningType.ordinary_hours.value: "guid-ordinary",
        XeroEarningType.saturday_loading.value: "guid-saturday",
    }
    client = XeroPayrollClient(_xero_creds(), earnings_rate_ids=rate_map)

    lines = client._build_earnings_for_employee(_emp_payroll())
    ids = [ln["EarningTypeID"] for ln in lines if "EarningTypeID" in ln]

    assert ids == ["guid-ordinary", "guid-saturday"]
    # Never the human-readable name.
    assert XeroEarningType.ordinary_hours.value not in ids


def test_missing_mapping_fails_loud():
    """An unmapped earning type raises rather than pushing a placeholder name."""
    client = XeroPayrollClient(_xero_creds(), earnings_rate_ids={})  # empty
    with pytest.raises(ValueError, match="No Xero EarningsRateID"):
        client._build_earnings_for_employee(_emp_payroll())


@respx.mock
@pytest.mark.asyncio
async def test_fetch_earnings_rates_builds_guid_map():
    """PayItems are fetched and turned into a {Name: EarningsRateID} map."""
    respx.get(
        "https://api.xero.com/payroll.xro/2.0/PayItems"
    ).mock(return_value=httpx.Response(200, json={
        "PayItems": {
            "EarningsRates": [
                {"Name": "Ordinary Hours", "EarningsRateID": "guid-ord"},
                {"Name": "Saturday Loading 1.25", "EarningsRateID": "guid-sat"},
                {"Name": "Incomplete"},  # no ID -> skipped
            ]
        }
    }))

    client = XeroPayrollClient(_xero_creds())
    await client._ensure_earnings_rate_map()

    assert client._earnings_rate_map == {
        "Ordinary Hours": "guid-ord",
        "Saturday Loading 1.25": "guid-sat",
    }
    # And resolution now produces the fetched GUID.
    assert client._resolve_earning_type_id("Ordinary Hours") == "guid-ord"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_earnings_rates_empty_on_error():
    """A non-200 from PayItems leaves the map empty (caller fails loud)."""
    respx.get(
        "https://api.xero.com/payroll.xro/2.0/PayItems"
    ).mock(return_value=httpx.Response(403, text="forbidden"))

    client = XeroPayrollClient(_xero_creds())
    await client._ensure_earnings_rate_map()
    assert client._earnings_rate_map == {}


# ---------------------------------------------------------------------------
# Stripe tier price resolution
# ---------------------------------------------------------------------------

def test_price_prefers_configured_env(monkeypatch):
    """A configured STRIPE_PRICE_<TIER> env id is returned verbatim."""
    monkeypatch.setenv("STRIPE_PRICE_PRO", "price_real_pro_123")
    svc = BillingService()  # re-reads env in __init__
    assert svc._get_or_create_price(SubscriptionTier.pro, 9900) == "price_real_pro_123"


def test_price_never_returns_fabricated_placeholder(monkeypatch):
    """With no config and Stripe disabled, resolution fails loud (no 'price_pro')."""
    monkeypatch.delenv("STRIPE_PRICE_PRO", raising=False)
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    svc = BillingService()
    with pytest.raises(RuntimeError, match="Stripe is not configured"):
        svc._get_or_create_price(SubscriptionTier.pro, 9900)
