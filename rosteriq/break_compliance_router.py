"""FastAPI router for break compliance endpoints.

Provides REST API for Fair Work Act 2009 compliance checking:
- POST /api/v1/compliance/check-shift - Check a single shift
- POST /api/v1/compliance/check-roster - Check full roster
- GET /api/v1/compliance/violations - Query violations
- GET /api/v1/compliance/report/{venue_id} - Compliance summary
- DELETE /api/v1/compliance/violations/{violation_id} - Dismiss violation
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

from rosteriq.break_compliance import (
    get_compliance_store,
    check_shift_breaks,
    check_roster_compliance,
    BreakViolation,
    ViolationSeverity,
    RuleType,
    ComplianceReport,
    DEFAULT_RULES,
)

logger = logging.getLogger("rosteriq.break_compliance_router")

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
    class ShiftCheckRequest(BaseModel):
        """Request to check a single shift for compliance."""
        shift_date: str = Field(..., description="ISO date YYYY-MM-DD")
        shift_start: str = Field(..., description="HH:MM")
        shift_end: str = Field(..., description="HH:MM")
        employee_id: Optional[str] = Field(None, description="Employee ID")
        employee_name: Optional[str] = Field(None, description="Employee name")
        break_minutes: int = Field(30, description="Break minutes taken")


    class RosterCheckRequest(BaseModel):
        """Request to check a roster for compliance."""
        venue_id: str = Field(..., description="Venue ID")
        shifts: List[Dict[str, Any]] = Field(..., description="List of shifts with date, start, end, employee_id, employee_name, break_minutes")


    class BreakViolationResponse(BaseModel):
        """Response containing a break violation."""
        violation_id: str
        venue_id: str
        employee_id: str
        employee_name: str
        shift_date: str
        shift_start: str
        shift_end: str
        rule_type: str
        severity: str
        description: str
        detected_at: str
        dismissed_at: Optional[str] = None
        dismissed_by: Optional[str] = None
        dismiss_reason: Optional[str] = None

        @classmethod
        def from_violation(cls, v: BreakViolation) -> BreakViolationResponse:
            """Convert a BreakViolation to response."""
            return cls(**v.to_dict())


    class ViolationListResponse(BaseModel):
        """Response containing a list of violations."""
        count: int = Field(..., description="Number of violations")
        violations: List[BreakViolationResponse]


    class ComplianceReportResponse(BaseModel):
        """Response containing a compliance report."""
        venue_id: str
        check_date: str
        total_shifts: int
        violations: List[BreakViolationResponse]
        summary: Dict[str, Any]
        compliant: bool


    class DismissRequest(BaseModel):
        """Request to dismiss a violation."""
        reason: Optional[str] = Field(None, description="Reason for dismissal")


# Only create router if FastAPI is available
if APIRouter is not None:
    router = APIRouter(prefix="/api/v1/compliance", tags=["compliance"])

    @router.post("/check-shift", response_model=ViolationListResponse)
    async def check_shift(
        req: ShiftCheckRequest,
        request: Request,
    ) -> ViolationListResponse:
        """
        Check a single shift for break compliance violations.

        Staff (L1+) can check shifts. Returns any violations found.

        Args:
            req: Shift details to check
            request: HTTP request for auth context

        Returns:
            List of violations found (if any)

        Raises:
            400: Invalid input
            403: Not authorized
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            violations = check_shift_breaks(
                req.shift_date,
                req.shift_start,
                req.shift_end,
                req.break_minutes,
                rules=DEFAULT_RULES,
            )

            # Populate employee info
            for v in violations:
                v.employee_id = req.employee_id or ""
                v.employee_name = req.employee_name or ""

            return ViolationListResponse(
                count=len(violations),
                violations=[BreakViolationResponse.from_violation(v) for v in violations],
            )
        except Exception as e:
            logger.exception("Failed to check shift")
            raise HTTPException(status_code=400, detail=str(e))

    @router.post("/check-roster", response_model=ComplianceReportResponse)
    async def check_roster(
        req: RosterCheckRequest,
        request: Request,
    ) -> ComplianceReportResponse:
        """
        Check a full roster for compliance violations.

        Roster makers (L2+) check entire rosters. Returns a comprehensive report.

        Args:
            req: Roster details with list of shifts
            request: HTTP request for auth context

        Returns:
            Compliance report with violations and summary

        Raises:
            400: Invalid input
            403: Not authorized
        """
        await _gate(request, "L2_ROSTER_MAKER")

        try:
            report = check_roster_compliance(
                req.venue_id,
                req.shifts,
                rules=DEFAULT_RULES,
            )

            return ComplianceReportResponse(
                venue_id=report.venue_id,
                check_date=report.check_date.isoformat(),
                total_shifts=report.total_shifts,
                violations=[BreakViolationResponse.from_violation(v) for v in report.violations],
                summary=report.summary,
                compliant=report.compliant,
            )
        except Exception as e:
            logger.exception("Failed to check roster")
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/violations", response_model=ViolationListResponse)
    async def list_violations(
        venue_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        severity: Optional[str] = None,
        employee_id: Optional[str] = None,
        include_dismissed: bool = False,
        limit: int = 100,
        request: Request = None,
    ) -> ViolationListResponse:
        """
        Query violations for a venue.

        Staff (L1+) can query violations. Supports filtering by date range,
        severity, and employee.

        Args:
            venue_id: Venue ID to query
            date_from: Start date (ISO format, optional)
            date_to: End date (ISO format, optional)
            severity: Filter by severity (warning, violation, critical)
            employee_id: Filter by employee ID
            include_dismissed: Include dismissed violations (default: false)
            limit: Max results (default 100)
            request: HTTP request for auth context

        Returns:
            List of matching violations

        Raises:
            400: Invalid input
            403: Not authorized
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            severity_filter = None
            if severity:
                try:
                    severity_filter = ViolationSeverity(severity)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid severity {severity}. Must be one of: "
                        f"{', '.join(s.value for s in ViolationSeverity)}",
                    )

            store = get_compliance_store()
            violations = store.list_by_venue(
                venue_id,
                date_from=date_from,
                date_to=date_to,
                severity=severity_filter,
                employee_id=employee_id,
                include_dismissed=include_dismissed,
                limit=limit,
            )

            return ViolationListResponse(
                count=len(violations),
                violations=[BreakViolationResponse.from_violation(v) for v in violations],
            )
        except Exception as e:
            logger.exception("Failed to list violations")
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/report/{venue_id}", response_model=ComplianceReportResponse)
    async def get_report(
        venue_id: str,
        request: Request = None,
    ) -> ComplianceReportResponse:
        """
        Get a compliance summary report for a venue.

        Roster makers (L2+) get compliance summaries. Shows all undismissed
        violations and compliance status.

        Args:
            venue_id: Venue ID to report on
            request: HTTP request for auth context

        Returns:
            Compliance report with violation summary

        Raises:
            403: Not authorized
        """
        await _gate(request, "L2_ROSTER_MAKER")

        try:
            store = get_compliance_store()
            violations = store.list_by_venue(
                venue_id,
                include_dismissed=False,
                limit=1000,
            )

            compliant = len(violations) == 0
            report = ComplianceReport(
                venue_id=venue_id,
                check_date=datetime.now(datetime.now().astimezone().tzinfo),
                total_shifts=0,  # Would require data from roster
                violations=violations,
                compliant=compliant,
            )

            return ComplianceReportResponse(
                venue_id=report.venue_id,
                check_date=report.check_date.isoformat(),
                total_shifts=report.total_shifts,
                violations=[BreakViolationResponse.from_violation(v) for v in report.violations],
                summary=report.summary,
                compliant=report.compliant,
            )
        except Exception as e:
            logger.exception("Failed to get report")
            raise HTTPException(status_code=400, detail=str(e))

    @router.delete("/violations/{violation_id}")
    async def dismiss_violation(
        violation_id: str,
        req: DismissRequest,
        request: Request,
    ) -> BreakViolationResponse:
        """
        Dismiss a violation with reason.

        Only owners can dismiss violations. Marks the violation as dismissed
        so it doesn't count toward compliance scores.

        Args:
            violation_id: Violation ID to dismiss
            req: Dismissal reason
            request: HTTP request for auth context

        Returns:
            Updated violation with dismissal info

        Raises:
            400: Invalid input
            403: Not authorized
            404: Violation not found
        """
        await _gate(request, "OWNER")

        dismissed_by = getattr(request.state, "user_id", "unknown")

        try:
            store = get_compliance_store()
            violation = store.get(violation_id)
            if not violation:
                raise HTTPException(status_code=404, detail=f"Violation {violation_id} not found")

            updated = store.dismiss_violation(violation_id, dismissed_by, reason=req.reason)
            return BreakViolationResponse.from_violation(updated)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.exception("Failed to dismiss violation")
            raise HTTPException(status_code=400, detail=str(e))

else:
    # Fallback: create a no-op router for when FastAPI is unavailable
    router = None
    logger.warning("FastAPI not available; break_compliance_router cannot be used")
