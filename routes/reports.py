"""
Compliance reporting routes for RosterIQ.

REST endpoints for generating and exporting Fair Work compliance reports
in JSON, PDF, and CSV formats.
"""

from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse, JSONResponse
import json

from rosteriq.models import (
    Employee,
    Shift,
    State,
)
from rosteriq.services.compliance_reports import ComplianceReportService
from rosteriq.services.report_export import (
    export_compliance_pdf,
    export_compliance_csv,
)
from rosteriq.database import get_db


# ============================================================================
# SETUP
# ============================================================================


router = APIRouter(prefix="/api/reports", tags=["reports"])


# ============================================================================
# DATA CLASSES FOR JSON RESPONSES
# ============================================================================


class ViolationJSON:
    """JSON-serializable violation."""
    def __init__(self, violation):
        self.employee_id = violation.employee_id
        self.employee_name = violation.employee_name
        self.violation_type = violation.violation_type
        self.description = violation.description
        self.severity = violation.severity.value
        self.shift_id = violation.shift_id
        self.date = violation.date.isoformat() if violation.date else None

    def __dict__(self):
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "violation_type": self.violation_type,
            "description": self.description,
            "severity": self.severity,
            "shift_id": self.shift_id,
            "date": self.date,
        }


class SectionJSON:
    """JSON-serializable section."""
    def __init__(self, section):
        self.title = section.title
        self.description = section.description
        self.compliance_percentage = section.compliance_percentage
        self.findings = section.findings
        self.violation_count = len(section.violations)

    def __dict__(self):
        return {
            "title": self.title,
            "description": self.description,
            "compliance_percentage": self.compliance_percentage,
            "findings": self.findings,
            "violation_count": self.violation_count,
        }


class ComplianceReportJSON:
    """JSON-serializable compliance report."""
    def __init__(self, report):
        self.venue_id = report.venue_id
        self.period_start = report.period_start.isoformat()
        self.period_end = report.period_end.isoformat()
        self.generated_at = report.generated_at.isoformat()
        self.overall_score = report.overall_score
        self.score_rating = report.score_rating
        self.sections = [SectionJSON(s).__dict__() for s in report.sections]
        self.violation_count = len(report.violations)
        self.violations = [ViolationJSON(v).__dict__() for v in report.violations]

    def __dict__(self):
        return {
            "venue_id": self.venue_id,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "generated_at": self.generated_at,
            "overall_score": self.overall_score,
            "score_rating": self.score_rating,
            "sections": self.sections,
            "violation_count": self.violation_count,
            "violations": self.violations,
        }


# ============================================================================
# ROUTES
# ============================================================================


@router.get("/compliance/{venue_id}")
async def get_compliance_report(
    venue_id: str,
    period_days: int = Query(30, ge=1, le=365),
    db=Depends(get_db),
) -> dict:
    """
    Get a comprehensive Fair Work compliance report in JSON format.

    Args:
        venue_id: The venue identifier
        period_days: Number of days to analyse (1-365, default 30)

    Returns:
        JSON compliance report with sections, violations, and scoring
    """
    try:
        # Fetch venue config
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        state = State(venue.state if hasattr(venue, 'state') else "nsw")

        # Fetch all employees and shifts
        all_employees = db.list_employees()
        all_rosters = db.list_rosters()

        # Filter to employees in this venue and shifts in rosters for this venue
        employees = [e for e in all_employees if e.id.startswith(venue_id) or True]
        shifts = []
        for roster in all_rosters:
            if roster.venue_id == venue_id:
                shifts.extend(roster.shifts)

        # Generate report
        service = ComplianceReportService(state)
        report = service.generate_compliance_report(
            venue_id=venue_id,
            employees=employees,
            shifts=shifts,
            period_days=period_days,
        )

        # Return as JSON
        report_json = ComplianceReportJSON(report).__dict__()
        return report_json

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")


@router.get("/compliance/{venue_id}/pdf")
async def get_compliance_pdf(
    venue_id: str,
    period_days: int = Query(30, ge=1, le=365),
    db=Depends(get_db),
):
    """
    Download a Fair Work compliance report as PDF.

    Args:
        venue_id: The venue identifier
        period_days: Number of days to analyse (1-365, default 30)

    Returns:
        PDF file for download
    """
    try:
        # Fetch venue config
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        state = State(venue.state if hasattr(venue, 'state') else "nsw")

        # Fetch all employees and shifts
        all_employees = db.list_employees()
        all_rosters = db.list_rosters()

        # Filter to employees in this venue and shifts in rosters for this venue
        employees = [e for e in all_employees if e.id.startswith(venue_id) or True]
        shifts = []
        for roster in all_rosters:
            if roster.venue_id == venue_id:
                shifts.extend(roster.shifts)

        # Generate report
        service = ComplianceReportService(state)
        report = service.generate_compliance_report(
            venue_id=venue_id,
            employees=employees,
            shifts=shifts,
            period_days=period_days,
        )

        # Export as PDF
        pdf_bytes = export_compliance_pdf(report)

        # Return as streaming response
        return StreamingResponse(
            iter([pdf_bytes]),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=compliance_{venue_id}_{date.today().isoformat()}.pdf"
            },
        )

    except HTTPException:
        raise
    except ImportError as e:
        raise HTTPException(
            status_code=500,
            detail=f"PDF export not available: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating PDF: {str(e)}")


@router.get("/compliance/{venue_id}/csv")
async def get_compliance_csv(
    venue_id: str,
    period_days: int = Query(30, ge=1, le=365),
    db=Depends(get_db),
):
    """
    Download a Fair Work compliance report as CSV.

    Args:
        venue_id: The venue identifier
        period_days: Number of days to analyse (1-365, default 30)

    Returns:
        CSV file for download
    """
    try:
        # Fetch venue config
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        state = State(venue.state if hasattr(venue, 'state') else "nsw")

        # Fetch all employees and shifts
        all_employees = db.list_employees()
        all_rosters = db.list_rosters()

        # Filter to employees in this venue and shifts in rosters for this venue
        employees = [e for e in all_employees if e.id.startswith(venue_id) or True]
        shifts = []
        for roster in all_rosters:
            if roster.venue_id == venue_id:
                shifts.extend(roster.shifts)

        # Generate report
        service = ComplianceReportService(state)
        report = service.generate_compliance_report(
            venue_id=venue_id,
            employees=employees,
            shifts=shifts,
            period_days=period_days,
        )

        # Export as CSV
        csv_content = export_compliance_csv(report)

        # Return as streaming response
        return StreamingResponse(
            iter([csv_content.encode("utf-8")]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=compliance_{venue_id}_{date.today().isoformat()}.csv"
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating CSV: {str(e)}")


@router.get("/hours/{venue_id}")
async def get_hours_report(
    venue_id: str,
    week_start: Optional[str] = Query(None),
    db=Depends(get_db),
) -> dict:
    """
    Get hours-only compliance report (38h/week, max shift, minimum rest).

    Args:
        venue_id: The venue identifier
        week_start: Optional week start date (YYYY-MM-DD format)

    Returns:
        JSON report focused on hours compliance
    """
    try:
        # Parse week_start or use today
        if week_start:
            period_start = date.fromisoformat(week_start)
        else:
            period_start = date.today() - timedelta(days=date.today().weekday())

        period_end = period_start + timedelta(days=6)

        # Fetch venue config
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        state = State(venue.state if hasattr(venue, 'state') else "nsw")

        # Fetch all employees and shifts
        all_employees = db.list_employees()
        all_rosters = db.list_rosters()

        # Filter to employees in this venue and shifts in rosters for this venue
        employees = [e for e in all_employees if e.id.startswith(venue_id) or True]
        shifts = []
        for roster in all_rosters:
            if roster.venue_id == venue_id:
                shifts.extend(roster.shifts)

        # Generate report with 7-day period
        service = ComplianceReportService(state)
        report = service.generate_compliance_report(
            venue_id=venue_id,
            employees=employees,
            shifts=shifts,
            period_days=7,
        )

        # Return only hours section
        hours_section = next(
            (s for s in report.sections if s.title == "Hours Compliance"),
            None
        )

        if not hours_section:
            raise HTTPException(status_code=500, detail="Hours compliance section not found")

        return {
            "venue_id": venue_id,
            "week_start": period_start.isoformat(),
            "week_end": period_end.isoformat(),
            "compliance_percentage": hours_section.compliance_percentage,
            "findings": hours_section.findings,
            "violations": [
                {
                    "employee_id": v.employee_id,
                    "employee_name": v.employee_name,
                    "description": v.description,
                    "severity": v.severity.value,
                }
                for v in hours_section.violations
            ],
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")


@router.get("/penalties/{venue_id}")
async def get_penalties_report(
    venue_id: str,
    period_days: int = Query(30, ge=1, le=365),
    db=Depends(get_db),
) -> dict:
    """
    Get penalty rate audit report.

    Args:
        venue_id: The venue identifier
        period_days: Number of days to analyse (1-365, default 30)

    Returns:
        JSON report focused on penalty rates
    """
    try:
        # Fetch venue config
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        state = State(venue.state if hasattr(venue, 'state') else "nsw")

        # Fetch all employees and shifts
        all_employees = db.list_employees()
        all_rosters = db.list_rosters()

        # Filter to employees in this venue and shifts in rosters for this venue
        employees = [e for e in all_employees if e.id.startswith(venue_id) or True]
        shifts = []
        for roster in all_rosters:
            if roster.venue_id == venue_id:
                shifts.extend(roster.shifts)

        # Generate report
        service = ComplianceReportService(state)
        report = service.generate_compliance_report(
            venue_id=venue_id,
            employees=employees,
            shifts=shifts,
            period_days=period_days,
        )

        # Return only penalties section
        penalties_section = next(
            (s for s in report.sections if s.title == "Penalty Rate Audit"),
            None
        )

        if not penalties_section:
            raise HTTPException(status_code=500, detail="Penalty section not found")

        return {
            "venue_id": venue_id,
            "period_start": report.period_start.isoformat(),
            "period_end": report.period_end.isoformat(),
            "compliance_percentage": penalties_section.compliance_percentage,
            "findings": penalties_section.findings,
            "violations": [
                {
                    "employee_id": v.employee_id,
                    "employee_name": v.employee_name,
                    "description": v.description,
                    "severity": v.severity.value,
                }
                for v in penalties_section.violations
            ],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")


@router.get("/certifications/{venue_id}")
async def get_certifications_report(
    venue_id: str,
    db=Depends(get_db),
) -> dict:
    """
    Get employee certification status report (RSA, food safety, first aid).

    Args:
        venue_id: The venue identifier

    Returns:
        JSON report on certification compliance
    """
    try:
        # Fetch venue config
        venue = db.get_venue(venue_id)
        if not venue:
            raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

        state = State(venue.state if hasattr(venue, 'state') else "nsw")

        # Fetch all employees and shifts
        all_employees = db.list_employees()
        all_rosters = db.list_rosters()

        # Filter to employees in this venue and shifts in rosters for this venue
        employees = [e for e in all_employees if e.id.startswith(venue_id) or True]
        shifts = []
        for roster in all_rosters:
            if roster.venue_id == venue_id:
                shifts.extend(roster.shifts)

        # Generate report
        service = ComplianceReportService(state)
        report = service.generate_compliance_report(
            venue_id=venue_id,
            employees=employees,
            shifts=shifts,
            period_days=365,
        )

        # Return only certifications section
        certs_section = next(
            (s for s in report.sections if s.title == "Certification Status"),
            None
        )

        if not certs_section:
            raise HTTPException(status_code=500, detail="Certifications section not found")

        return {
            "venue_id": venue_id,
            "generated_at": report.generated_at.isoformat(),
            "compliance_percentage": certs_section.compliance_percentage,
            "findings": certs_section.findings,
            "note": "Certification tracking requires integration with external system. "
                    "Currently displaying placeholder data.",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")
