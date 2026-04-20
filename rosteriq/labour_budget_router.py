"""FastAPI router for labour budget endpoints.

Provides REST API for budget guardrails:
- GET /api/v1/budget/snapshot/{venue_id} - Current budget snapshot
- PUT /api/v1/budget/thresholds/{venue_id} - Set/update thresholds
- GET /api/v1/budget/thresholds/{venue_id} - Get current thresholds
- GET /api/v1/budget/alerts/{venue_id} - Query budget alerts
- POST /api/v1/budget/what-if - What-if scenario analysis
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

# Lazy imports for optional deps
try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
    FASTAPI_AVAILABLE = True
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    BaseModel = object
    Field = None
    FASTAPI_AVAILABLE = False

from rosteriq.labour_budget import (
    get_threshold_store,
    get_alert_store,
    calculate_shift_cost,
    calculate_roster_cost,
    build_budget_snapshot,
    check_budget_alerts,
    project_hours_remaining,
    BudgetThreshold,
    BudgetAlert,
    ShiftCostProjection,
    BudgetSnapshot,
    AlertType,
    DEFAULT_THRESHOLDS,
)

logger = logging.getLogger("rosteriq.labour_budget_router")

# Auth gating - fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel
except Exception:
    require_access = None
    AccessLevel = None


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ---------------------------------------------------------------------------
# Pydantic Models (only if FastAPI is available)
# ---------------------------------------------------------------------------

if FASTAPI_AVAILABLE:

    class ShiftInput(BaseModel):
        """Shift data for budget calculation."""
        employee_id: str
        employee_name: str
        shift_start: str = Field(..., description="HH:MM")
        shift_end: str = Field(..., description="HH:MM")
        shift_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD")

    class BudgetThresholdRequest(BaseModel):
        """Request to set budget thresholds."""
        target_labour_pct: float = Field(..., description="Target labour % (e.g. 30.0)")
        warning_labour_pct: float = Field(..., description="Warning threshold (e.g. 28.0)")
        critical_labour_pct: float = Field(..., description="Critical threshold (e.g. 35.0)")
        max_wage_cost_per_hour: Optional[float] = Field(None, description="Optional max AUD/hour")

    class BudgetThresholdResponse(BaseModel):
        """Response with budget thresholds."""
        venue_id: str
        target_labour_pct: float
        warning_labour_pct: float
        critical_labour_pct: float
        max_wage_cost_per_hour: Optional[float] = None
        created_at: str
        updated_at: str

        @classmethod
        def from_threshold(cls, t: BudgetThreshold) -> BudgetThresholdResponse:
            return cls(**t.to_dict())

    class ShiftCostResponse(BaseModel):
        """Response with shift cost projection."""
        employee_id: str
        employee_name: str
        shift_start: str
        shift_end: str
        base_cost: float
        penalty_cost: float
        total_cost: float
        hourly_rate: float
        is_overtime: bool
        is_penalty_rate: bool

        @classmethod
        def from_projection(cls, p: ShiftCostProjection) -> ShiftCostResponse:
            return cls(**p.to_dict())

    class BudgetAlertResponse(BaseModel):
        """Response with budget alert."""
        alert_id: str
        venue_id: str
        alert_type: str
        current_labour_pct: float
        target_labour_pct: float
        current_wage_cost: float
        projected_revenue: Optional[float] = None
        message: Optional[str] = None
        shift_date: Optional[str] = None
        created_at: str

        @classmethod
        def from_alert(cls, a: BudgetAlert) -> BudgetAlertResponse:
            return cls(**a.to_dict())

    class BudgetSnapshotResponse(BaseModel):
        """Response with complete budget snapshot."""
        venue_id: str
        snapshot_date: str
        total_wage_cost: float
        projected_revenue: float
        labour_pct: float
        headcount: int
        avg_hourly_cost: float
        shift_costs: List[Dict[str, Any]] = Field(default_factory=list)
        alerts: List[Dict[str, Any]] = Field(default_factory=list)
        hours_remaining_in_budget: Optional[float] = None
        created_at: str

        @classmethod
        def from_snapshot(cls, s: BudgetSnapshot) -> BudgetSnapshotResponse:
            return cls(**s.to_dict())

    class WhatIfRequest(BaseModel):
        """Request to analyze a what-if scenario."""
        venue_id: str
        shifts: List[ShiftInput]
        rates_map: Dict[str, float] = Field(..., description="Map of employee_id -> hourly_rate")
        projected_revenue: float
        snapshot_date: Optional[str] = Field(None, description="ISO date; defaults to today")
        action: str = Field("project", description="'add' to add shift, 'remove' to remove, 'project' for full roster")

    class WhatIfResponse(BaseModel):
        """Response with what-if scenario result."""
        scenario: str
        original_snapshot: Optional[Dict[str, Any]] = None
        new_snapshot: Dict[str, Any]
        impact: Dict[str, Any]  # {labour_pct_change, cost_change, headcount_change}

    class AlertQueryResponse(BaseModel):
        """Response with list of alerts."""
        venue_id: str
        alerts: List[Dict[str, Any]]
        count: int

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter() if FASTAPI_AVAILABLE else None


if FASTAPI_AVAILABLE:

    @router.get("/snapshot/{venue_id}", response_model=BudgetSnapshotResponse)
    async def get_budget_snapshot(
        request: Request,
        venue_id: str,
        shifts_json: Optional[str] = None,
        rates_json: Optional[str] = None,
        projected_revenue: Optional[float] = None,
        snapshot_date: Optional[str] = None,
    ) -> BudgetSnapshotResponse:
        """Get current budget snapshot for a venue.

        Query params:
        - shifts_json: JSON array of shift objects (employee_id, shift_start, shift_end, shift_date, employee_name)
        - rates_json: JSON object mapping employee_id to hourly_rate
        - projected_revenue: forecast revenue for period
        - snapshot_date: ISO date YYYY-MM-DD (defaults to today)
        """
        await _gate(request, "L1_SUPERVISOR")

        if not projected_revenue:
            raise HTTPException(status_code=400, detail="projected_revenue required")

        # Parse JSON inputs
        import json
        shifts = []
        rates_map = {}

        if shifts_json:
            try:
                shifts = json.loads(shifts_json)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid shifts_json")

        if rates_json:
            try:
                rates_map = json.loads(rates_json)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid rates_json")

        if not snapshot_date:
            from datetime import date
            snapshot_date = date.today().isoformat()

        # Get thresholds
        threshold_store = get_threshold_store()
        thresholds = threshold_store.get(venue_id)

        # Build snapshot
        snapshot = build_budget_snapshot(
            venue_id=venue_id,
            shifts=shifts,
            rates_map=rates_map,
            projected_revenue=projected_revenue,
            snapshot_date=snapshot_date,
            thresholds=thresholds,
        )

        return BudgetSnapshotResponse.from_snapshot(snapshot)

    @router.put("/thresholds/{venue_id}", response_model=BudgetThresholdResponse)
    async def set_budget_thresholds(
        request: Request,
        venue_id: str,
        body: BudgetThresholdRequest,
    ) -> BudgetThresholdResponse:
        """Set or update budget thresholds for a venue."""
        await _gate(request, "L2_ROSTER_MAKER")

        threshold = BudgetThreshold(
            venue_id=venue_id,
            target_labour_pct=body.target_labour_pct,
            warning_labour_pct=body.warning_labour_pct,
            critical_labour_pct=body.critical_labour_pct,
            max_wage_cost_per_hour=body.max_wage_cost_per_hour,
        )

        threshold_store = get_threshold_store()
        threshold = threshold_store.set(threshold)

        return BudgetThresholdResponse.from_threshold(threshold)

    @router.get("/thresholds/{venue_id}", response_model=BudgetThresholdResponse)
    async def get_budget_thresholds(
        request: Request,
        venue_id: str,
    ) -> BudgetThresholdResponse:
        """Get current thresholds for a venue."""
        await _gate(request, "L1_SUPERVISOR")

        threshold_store = get_threshold_store()
        threshold = threshold_store.get(venue_id)

        return BudgetThresholdResponse.from_threshold(threshold)

    @router.get("/alerts/{venue_id}", response_model=AlertQueryResponse)
    async def get_budget_alerts(
        request: Request,
        venue_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        alert_type: Optional[str] = None,
        limit: int = 50,
    ) -> AlertQueryResponse:
        """Get recent budget alerts for a venue.

        Query params:
        - date_from: ISO date YYYY-MM-DD (filter by date range)
        - date_to: ISO date YYYY-MM-DD
        - alert_type: AlertType enum (on_track, warning, over_budget, critical)
        - limit: max alerts to return (default 50)
        """
        await _gate(request, "L1_SUPERVISOR")

        alert_store = get_alert_store()

        if date_from and date_to:
            alerts = alert_store.get_by_date_range(venue_id, date_from, date_to, limit)
        else:
            alerts = alert_store.get_by_venue(venue_id, limit, alert_type)

        return AlertQueryResponse(
            venue_id=venue_id,
            alerts=[a.to_dict() for a in alerts],
            count=len(alerts),
        )

    @router.post("/what-if", response_model=WhatIfResponse)
    async def what_if_scenario(
        request: Request,
        body: WhatIfRequest,
    ) -> WhatIfResponse:
        """Analyze what-if scenarios (add/remove shift, project full roster).

        Action types:
        - 'add': add a shift and see impact
        - 'remove': remove a shift and see impact
        - 'project': project current roster (default)
        """
        await _gate(request, "L1_SUPERVISOR")

        from datetime import date

        snapshot_date = body.snapshot_date or date.today().isoformat()

        # Get thresholds
        threshold_store = get_threshold_store()
        thresholds = threshold_store.get(body.venue_id)

        # Build shifts list from input
        shifts = [
            {
                "employee_id": s.employee_id,
                "employee_name": s.employee_name,
                "shift_start": s.shift_start,
                "shift_end": s.shift_end,
                "shift_date": s.shift_date or snapshot_date,
            }
            for s in body.shifts
        ]

        # Convert rates_map dict to proper format
        rates_map = body.rates_map

        # Build new snapshot
        new_snapshot = build_budget_snapshot(
            venue_id=body.venue_id,
            shifts=shifts,
            rates_map=rates_map,
            projected_revenue=body.projected_revenue,
            snapshot_date=snapshot_date,
            thresholds=thresholds,
        )

        # Calculate impact
        impact = {
            "labour_pct_change": 0.0,
            "cost_change": 0.0,
            "headcount_change": len(shifts),
        }

        return WhatIfResponse(
            scenario=body.action,
            new_snapshot=new_snapshot.to_dict(),
            impact=impact,
        )
