"""REST endpoints for the onboarding wizard (Round 8 Track D).

Endpoints:
  POST /api/v1/onboarding/start
    body: {tenant_id}
    Returns initial wizard state.

  GET  /api/v1/onboarding/{wizard_id}
    Returns current wizard state.

  POST /api/v1/onboarding/{wizard_id}/step
    body: {step: "<step_key>", data: {...}}
    Validates and stores the step, advances current_step.

  POST /api/v1/onboarding/{wizard_id}/complete
    Marks the wizard complete and runs finalize hooks.

All endpoints are L1+ (any logged-in user can drive their own onboarding),
except /complete which is L2+ because it triggers tenant configuration
changes.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from rosteriq.onboarding import (
    STEPS,
    get_onboarding_store,
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


logger = logging.getLogger("rosteriq.onboarding_router")

router = APIRouter(tags=["onboarding"])


@router.get("/api/v1/onboarding/spec")
async def get_spec() -> dict:
    """Return the wizard step specification (public, no auth)."""
    return {
        "steps": [
            {
                "key": s.key,
                "title": s.title,
                "description": s.description,
                "required_fields": s.required_fields,
                "optional_fields": s.optional_fields,
            }
            for s in STEPS
        ],
        "total_steps": len(STEPS),
    }


@router.post("/api/v1/onboarding/start")
async def start_wizard(request: Request) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")
    tenant_id = body.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id is required")
    state = get_onboarding_store().start(tenant_id)
    return state.to_dict()


@router.get("/api/v1/onboarding/{wizard_id}")
async def get_wizard(request: Request, wizard_id: str) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    state = get_onboarding_store().get(wizard_id)
    if state is None:
        raise HTTPException(status_code=404, detail="wizard not found")
    return state.to_dict()


@router.post("/api/v1/onboarding/{wizard_id}/step")
async def submit_step(request: Request, wizard_id: str) -> dict:
    await _gate(request, "L1_SUPERVISOR")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid JSON body")
    step = body.get("step")
    payload = body.get("data") or {}
    if not step:
        raise HTTPException(status_code=400, detail="step is required")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="data must be an object")
    try:
        state = get_onboarding_store().submit_step(wizard_id, step, payload)
    except KeyError:
        raise HTTPException(status_code=404, detail="wizard not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return state.to_dict()


@router.post("/api/v1/onboarding/{wizard_id}/complete")
async def complete_wizard(request: Request, wizard_id: str) -> dict:
    await _gate(request, "L2_ROSTER_MAKER")

    def _finalize(state) -> None:
        # Best-effort: if the operator chose a billing tier we record it
        # in the tenant store. Tanda/Stripe wiring is beyond this scope —
        # the wizard just captures intent here.
        try:
            from rosteriq.tenants import get_tenant_store, BillingTier  # type: ignore
        except Exception:
            return
        try:
            store = get_tenant_store()
            tenant = store.get(state.tenant_id)
            if tenant is None:
                return
            tier_str = (state.data.get("billing_tier", {}).get("tier") or "").lower()
            if tier_str and hasattr(BillingTier, tier_str.upper()):
                tenant.billing_tier = getattr(BillingTier, tier_str.upper())
        except Exception as e:  # pragma: no cover
            logger.warning("tenant finalize failed: %s", e)

    try:
        state = get_onboarding_store().complete(wizard_id, finalize=_finalize)
    except KeyError:
        raise HTTPException(status_code=404, detail="wizard not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return state.to_dict()
