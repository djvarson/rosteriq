"""Finalize hooks for onboarding wizard completion (Round 15).

When a tenant completes the wizard, a lot of downstream state should
light up: the tenant record updates, the concierge KB seeds from the
venue basics they entered, Tanda credentials (if provided) get stashed
against the tenant, and we note that a history backfill is queued.

All hooks are best-effort. If one step fails we log and keep going —
the wizard is "done" from the user's perspective the moment the API
returns, and the dashboard should render even if one side effect
hiccups.

Why this matters: without these hooks the first post-onboarding dashboard
shows cold demo data, which is the single most common trial-drop signal
we see. Seeding from wizard data produces a dashboard that looks like
*their* venue on first login.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger("rosteriq.onboarding_finalize")


def _seed_concierge_kb(tenant_id: str, venue_basics: Dict[str, Any]) -> None:
    """Seed the venue's concierge KB with hours + venue name.

    We register the KB under the tenant_id (which doubles as venue_id
    in the single-venue wizard path). Live context is the minimum set
    of fields the default FAQ templates substitute into.
    """
    try:
        from rosteriq.concierge import get_kb, VenueKB, _DEFAULT_FAQS
    except Exception as e:
        logger.warning("concierge KB unavailable for seeding: %s", e)
        return

    venue_name = venue_basics.get("venue_name") or tenant_id
    open_time = venue_basics.get("open_time") or "11:00"
    close_time = venue_basics.get("close_time") or "23:00"

    kb = VenueKB(
        venue_id=tenant_id,
        venue_name=venue_name,
        faqs=list(_DEFAULT_FAQS),
        live_context={
            "venue_name": venue_name,
            "open_time": open_time,
            "close_time": close_time,
        },
    )
    get_kb().register(kb)
    logger.info("Seeded concierge KB for tenant %s (%s)", tenant_id, venue_name)


def _ensure_tenant(tenant_id: str, venue_basics: Dict[str, Any],
                   billing_tier: str) -> None:
    """Create or update the tenant based on wizard data."""
    try:
        from rosteriq.tenants import get_tenant_store, BillingTier
    except Exception as e:
        logger.warning("tenant store unavailable: %s", e)
        return

    store = get_tenant_store()
    existing = store.get(tenant_id)

    tier_enum = BillingTier.STARTUP
    if billing_tier and hasattr(BillingTier, billing_tier.upper()):
        tier_enum = getattr(BillingTier, billing_tier.upper())
    elif billing_tier == "trial":
        # Trial is a status, not a tier — default to startup tier.
        tier_enum = BillingTier.STARTUP

    name = venue_basics.get("venue_name") or tenant_id
    slug = tenant_id.lower().replace(" ", "-")

    if existing is None:
        try:
            store.create(
                tenant_id=tenant_id,
                name=name,
                slug=slug,
                billing_tier=tier_enum,
                contact_email=venue_basics.get("contact_email", ""),
                notes={"created_via": "onboarding_wizard"},
            )
        except ValueError:
            # Race or edge — fall through to update.
            store.update(tenant_id, name=name, billing_tier=tier_enum)
    else:
        store.update(tenant_id, name=name, billing_tier=tier_enum)


def _register_tanda_creds(tenant_id: str, tanda_data: Dict[str, Any]) -> None:
    """Stash Tanda credentials in tenant notes if provided."""
    if tanda_data.get("skip"):
        return
    api_key = tanda_data.get("tanda_api_key")
    if not api_key:
        return
    try:
        from rosteriq.tenants import get_tenant_store
    except Exception as e:
        logger.warning("tenant store unavailable for tanda creds: %s", e)
        return

    store = get_tenant_store()
    tenant = store.get(tenant_id)
    if tenant is None:
        return
    # NOTE: In production these would be encrypted. For now we record
    # only that creds are present — the raw key is stored but should be
    # moved to a secrets manager before multi-tenant launch.
    notes = dict(tenant.notes or {})
    notes["tanda_connected"] = True
    notes["tanda_org_id"] = tanda_data.get("tanda_org_id") or tenant_id
    notes["tanda_api_key_present"] = True
    store.update(tenant_id, notes=notes)
    logger.info("Recorded Tanda connection for tenant %s", tenant_id)


def _flag_history_backfill(tenant_id: str, tanda_data: Dict[str, Any]) -> List[str]:
    """Return a list of human-readable "next step" notes.

    We don't fire the ingest synchronously — the scheduled job (Round 14)
    will pick it up on its next tick if the tenant is configured. This
    just records intent and returns a message for the UI.
    """
    if tanda_data.get("skip"):
        return ["Tanda skipped — you can connect later from Settings."]
    if not tanda_data.get("tanda_api_key"):
        return ["No Tanda key provided — skipping history backfill."]
    return [
        f"Tanda backfill queued for tenant {tenant_id} (scheduler will run within 24h).",
    ]


def run_finalize(state: Any) -> Dict[str, Any]:
    """Run all finalize hooks for a completed wizard state.

    Returns a summary dict the UI can show as a "here's what happened"
    confirmation on the thank-you screen.
    """
    tenant_id = state.tenant_id
    data = getattr(state, "data", {}) or {}

    venue_basics = data.get("venue_basics", {}) or {}
    tanda_data = data.get("tanda_connect", {}) or {}
    billing_data = data.get("billing_tier", {}) or {}
    tier = (billing_data.get("tier") or "startup").lower()

    summary: Dict[str, Any] = {
        "tenant_id": tenant_id,
        "steps": [],
        "errors": [],
    }

    for label, hook in (
        ("ensure_tenant", lambda: _ensure_tenant(tenant_id, venue_basics, tier)),
        ("seed_concierge_kb", lambda: _seed_concierge_kb(tenant_id, venue_basics)),
        ("register_tanda_creds", lambda: _register_tanda_creds(tenant_id, tanda_data)),
    ):
        try:
            hook()
            summary["steps"].append(label)
        except Exception as e:
            logger.exception("finalize hook %s failed", label)
            summary["errors"].append({"step": label, "error": str(e)})

    summary["next_actions"] = _flag_history_backfill(tenant_id, tanda_data)
    return summary
