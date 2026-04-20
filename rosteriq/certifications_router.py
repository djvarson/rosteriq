"""FastAPI router for staff certification tracking endpoints.

Provides REST API for managing and monitoring staff certifications:
- POST /api/v1/certs/ (L1+) — add a certification for an employee
- GET /api/v1/certs/{venue_id} (L1+) — list all certs for a venue
- GET /api/v1/certs/{venue_id}/compliance (L2+) — venue compliance status
- GET /api/v1/certs/{venue_id}/alerts (L1+) — expiry alerts for venue
- GET /api/v1/certs/{venue_id}/calendar (L1+) — upcoming expiries (90 days)
- PUT /api/v1/certs/{cert_id} (L1+) — update a certification
- DELETE /api/v1/certs/{cert_id} (L2+) — remove a certification record
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

# Lazy imports for optional deps
try:
    from fastapi import APIRouter, HTTPException, Request, Query
    from pydantic import BaseModel, Field
    FASTAPI_AVAILABLE = True
except ImportError:
    APIRouter = None
    HTTPException = None
    Request = None
    Query = None
    BaseModel = object
    Field = None
    FASTAPI_AVAILABLE = False

from rosteriq.certifications import (
    get_certification_store,
    check_venue_compliance,
    check_expiry_alerts,
    get_missing_certs,
    get_expiry_calendar,
    Certification,
    CertType,
    CertStatus,
    CertAlert,
    VenueComplianceStatus,
)

logger = logging.getLogger("rosteriq.certifications_router")

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
    class CertificationCreateRequest(BaseModel):
        """Request to add a new certification."""
        employee_id: str = Field(..., description="Employee ID")
        employee_name: str = Field(..., description="Employee name")
        venue_id: str = Field(..., description="Venue ID")
        cert_type: str = Field(..., description="Cert type: rsa, food_safety, first_aid, rsg, working_with_children, other")
        cert_number: str = Field(..., description="Certificate/license number")
        issued_date: str = Field(..., description="ISO date YYYY-MM-DD")
        expiry_date: Optional[str] = Field(None, description="ISO date YYYY-MM-DD (optional)")
        state: str = Field(..., description="Australian state: QLD, NSW, VIC, etc.")
        notes: Optional[str] = Field(None, description="Notes")

    class CertificationUpdateRequest(BaseModel):
        """Request to update a certification."""
        cert_number: Optional[str] = None
        issued_date: Optional[str] = None
        expiry_date: Optional[str] = None
        state: Optional[str] = None
        notes: Optional[str] = None

    class CertificationResponse(BaseModel):
        """Response containing a certification."""
        cert_id: str
        employee_id: str
        employee_name: str
        venue_id: str
        cert_type: str
        cert_number: str
        issued_date: str
        expiry_date: Optional[str]
        state: str
        notes: Optional[str]
        recorded_at: str
        status: str

        @classmethod
        def from_cert(cls, cert: Certification) -> CertificationResponse:
            """Convert a Certification to response."""
            d = cert.to_dict()
            return cls(**d)

    class CertificationListResponse(BaseModel):
        """Response containing a list of certifications."""
        count: int = Field(..., description="Number of certifications")
        certifications: List[CertificationResponse]

    class CertAlertResponse(BaseModel):
        """Response containing a certification alert."""
        alert_id: str
        employee_id: str
        employee_name: str
        cert_type: str
        expiry_date: Optional[str]
        days_until_expiry: int
        severity: str

        @classmethod
        def from_alert(cls, alert: CertAlert) -> CertAlertResponse:
            """Convert a CertAlert to response."""
            d = alert.to_dict()
            return cls(**d)

    class CertAlertListResponse(BaseModel):
        """Response containing a list of alerts."""
        count: int
        alerts: List[CertAlertResponse]

    class VenueComplianceResponse(BaseModel):
        """Response containing venue compliance status."""
        venue_id: str
        total_staff: int
        certs_valid: int
        certs_expiring: int
        certs_expired: int
        certs_missing: int
        food_safety_covered: bool
        compliance_pct: float
        alerts: List[CertAlertResponse]
        checked_at: str

    class ExpiryCalendarItemResponse(BaseModel):
        """Single item in expiry calendar."""
        employee_id: str
        employee_name: str
        cert_type: str
        expiry_date: str
        days_remaining: int

    class ExpiryCalendarResponse(BaseModel):
        """Response containing expiry calendar."""
        count: int
        items: List[ExpiryCalendarItemResponse]


# Only create router if FastAPI is available
if APIRouter is not None:
    router = APIRouter(prefix="/api/v1/certs", tags=["certifications"])

    @router.post("/", response_model=CertificationResponse)
    async def add_certification(
        req: CertificationCreateRequest,
        request: Request,
    ) -> CertificationResponse:
        """
        Add a new certification for an employee.

        Staff (L1+) can add certifications.

        Args:
            req: Certification details
            request: HTTP request for auth context

        Returns:
            Created certification record

        Raises:
            400: Invalid input
            403: Not authorized
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            # Validate cert type
            try:
                cert_type = CertType(req.cert_type)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid cert_type. Must be one of: {', '.join([t.value for t in CertType])}",
                )

            # Parse dates
            try:
                issued_date = date.fromisoformat(req.issued_date)
            except ValueError:
                raise HTTPException(status_code=400, detail="issued_date must be ISO format YYYY-MM-DD")

            expiry_date = None
            if req.expiry_date:
                try:
                    expiry_date = date.fromisoformat(req.expiry_date)
                except ValueError:
                    raise HTTPException(status_code=400, detail="expiry_date must be ISO format YYYY-MM-DD")

            # Create certification
            cert = Certification(
                cert_id=f"cert_{str(__import__('uuid').uuid4()).replace('-', '')[:12]}",
                employee_id=req.employee_id,
                employee_name=req.employee_name,
                venue_id=req.venue_id,
                cert_type=cert_type,
                cert_number=req.cert_number,
                issued_date=issued_date,
                expiry_date=expiry_date,
                state=req.state.upper(),
                notes=req.notes,
            )

            store = get_certification_store()
            cert = store.add(cert)

            return CertificationResponse.from_cert(cert)

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to add certification")
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/{venue_id}", response_model=CertificationListResponse)
    async def list_certifications(
        venue_id: str,
        request: Request,
        employee_id: Optional[str] = Query(None, description="Filter by employee ID"),
        cert_type: Optional[str] = Query(None, description="Filter by cert type"),
        status: Optional[str] = Query(None, description="Filter by status"),
    ) -> CertificationListResponse:
        """
        List all certifications for a venue.

        Staff (L1+) can view certifications.

        Args:
            venue_id: Venue identifier
            request: HTTP request for auth context
            employee_id: Optional filter by employee ID
            cert_type: Optional filter by cert type
            status: Optional filter by status

        Returns:
            List of certifications matching filters

        Raises:
            403: Not authorized
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            # Parse optional filters
            cert_type_enum = None
            if cert_type:
                try:
                    cert_type_enum = CertType(cert_type)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid cert_type: {cert_type}")

            status_enum = None
            if status:
                try:
                    status_enum = CertStatus(status)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

            store = get_certification_store()
            certs = store.list_by_venue(
                venue_id,
                employee_id=employee_id,
                cert_type=cert_type_enum,
                status=status_enum,
            )

            return CertificationListResponse(
                count=len(certs),
                certifications=[CertificationResponse.from_cert(c) for c in certs],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to list certifications")
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/{venue_id}/compliance", response_model=VenueComplianceResponse)
    async def venue_compliance_status(
        venue_id: str,
        request: Request,
        staff_ids: Optional[str] = Query(None, description="Comma-separated list of staff IDs"),
    ) -> VenueComplianceResponse:
        """
        Get venue-level certification compliance status.

        Roster makers (L2+) can view compliance summaries.

        Args:
            venue_id: Venue identifier
            request: HTTP request for auth context
            staff_ids: Optional comma-separated list of staff IDs to check (default: all at venue)

        Returns:
            Compliance status with metrics

        Raises:
            403: Not authorized
        """
        await _gate(request, "L2_ROSTER_MAKER")

        try:
            store = get_certification_store()
            certs = store.list_by_venue(venue_id)

            # Determine staff list
            if staff_ids:
                staff_list = [s.strip() for s in staff_ids.split(",")]
            else:
                # Get unique staff IDs from certs
                staff_list = list({c.employee_id for c in certs})

            compliance = check_venue_compliance(venue_id, certs, staff_list)

            return VenueComplianceResponse(
                **compliance.to_dict()
            )

        except Exception as e:
            logger.exception("Failed to check venue compliance")
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/{venue_id}/alerts", response_model=CertAlertListResponse)
    async def venue_expiry_alerts(
        venue_id: str,
        request: Request,
        severity: Optional[str] = Query(None, description="Filter by severity: WARNING, URGENT, EXPIRED"),
    ) -> CertAlertListResponse:
        """
        Get certification expiry alerts for a venue.

        Staff (L1+) can view alerts.

        Args:
            venue_id: Venue identifier
            request: HTTP request for auth context
            severity: Optional filter by severity (WARNING, URGENT, EXPIRED)

        Returns:
            List of alerts

        Raises:
            403: Not authorized
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            store = get_certification_store()
            certs = store.list_by_venue(venue_id)

            alerts = check_expiry_alerts(certs)

            # Filter by severity if specified
            if severity:
                alerts = [a for a in alerts if a.severity == severity]

            return CertAlertListResponse(
                count=len(alerts),
                alerts=[CertAlertResponse.from_alert(a) for a in alerts],
            )

        except Exception as e:
            logger.exception("Failed to get expiry alerts")
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/{venue_id}/calendar", response_model=ExpiryCalendarResponse)
    async def expiry_calendar(
        venue_id: str,
        request: Request,
        days_ahead: int = Query(90, description="Look ahead this many days (default 90)"),
    ) -> ExpiryCalendarResponse:
        """
        Get upcoming certification expirations calendar.

        Staff (L1+) can view the calendar.

        Args:
            venue_id: Venue identifier
            request: HTTP request for auth context
            days_ahead: Look ahead this many days (default 90)

        Returns:
            Calendar of upcoming expirations sorted by date

        Raises:
            403: Not authorized
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            store = get_certification_store()
            certs = store.list_by_venue(venue_id)

            calendar = get_expiry_calendar(certs, days_ahead=days_ahead)

            items = [
                ExpiryCalendarItemResponse(**item) for item in calendar
            ]

            return ExpiryCalendarResponse(
                count=len(items),
                items=items,
            )

        except Exception as e:
            logger.exception("Failed to get expiry calendar")
            raise HTTPException(status_code=400, detail=str(e))

    @router.put("/{cert_id}", response_model=CertificationResponse)
    async def update_certification(
        cert_id: str,
        req: CertificationUpdateRequest,
        request: Request,
    ) -> CertificationResponse:
        """
        Update a certification.

        Staff (L1+) can update certifications (e.g. renew with new expiry).

        Args:
            cert_id: Certification ID
            req: Fields to update
            request: HTTP request for auth context

        Returns:
            Updated certification

        Raises:
            400: Invalid input
            403: Not authorized
            404: Certification not found
        """
        await _gate(request, "L1_SUPERVISOR")

        try:
            store = get_certification_store()
            cert = store.get(cert_id)
            if not cert:
                raise HTTPException(status_code=404, detail=f"Certification {cert_id} not found")

            # Build update dict
            updates = {}
            if req.cert_number is not None:
                updates["cert_number"] = req.cert_number
            if req.issued_date is not None:
                try:
                    updates["issued_date"] = date.fromisoformat(req.issued_date)
                except ValueError:
                    raise HTTPException(status_code=400, detail="issued_date must be ISO format YYYY-MM-DD")
            if req.expiry_date is not None:
                try:
                    updates["expiry_date"] = date.fromisoformat(req.expiry_date)
                except ValueError:
                    raise HTTPException(status_code=400, detail="expiry_date must be ISO format YYYY-MM-DD")
            if req.state is not None:
                updates["state"] = req.state.upper()
            if req.notes is not None:
                updates["notes"] = req.notes

            if not updates:
                raise HTTPException(status_code=400, detail="No fields to update")

            cert = store.update(cert_id, **updates)
            return CertificationResponse.from_cert(cert)

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to update certification")
            raise HTTPException(status_code=400, detail=str(e))

    @router.delete("/{cert_id}")
    async def delete_certification(
        cert_id: str,
        request: Request,
    ) -> Dict[str, str]:
        """
        Delete a certification record.

        Roster makers (L2+) can delete certifications.

        Args:
            cert_id: Certification ID
            request: HTTP request for auth context

        Returns:
            Confirmation message

        Raises:
            403: Not authorized
            404: Certification not found
        """
        await _gate(request, "L2_ROSTER_MAKER")

        try:
            store = get_certification_store()
            cert = store.get(cert_id)
            if not cert:
                raise HTTPException(status_code=404, detail=f"Certification {cert_id} not found")

            store.delete(cert_id)
            return {"message": f"Certification {cert_id} deleted"}

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to delete certification")
            raise HTTPException(status_code=400, detail=str(e))

else:
    # Placeholder when FastAPI is not available
    router = None
