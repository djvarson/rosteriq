"""
Privacy compliance API routes for RosterIQ.

Endpoints for:
- Data export (user data portability)
- Employee anonymisation (right to erasure)
- Consent management
- Retention policy statistics
- Data cleanup operations

All routes require appropriate authentication and authorization.
Implements Australian Privacy Act 1988 compliance requirements.

Usage:
    Include in api.py:
    from rosteriq.routes import privacy
    app.include_router(privacy.router, prefix="/api/privacy", tags=["privacy"])
"""

import logging
from datetime import datetime
from typing import Optional
from io import BytesIO

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from fastapi.responses import JSONResponse, StreamingResponse

from rosteriq.database import get_db
from rosteriq.services.privacy import PrivacyService
from rosteriq.services.data_retention import DataRetentionService
from rosteriq.services.demo import DEMO_USER_ID
from rosteriq.middleware.auth import get_current_user, require_role
from rosteriq.middleware.tenant import (
    enforce_venue_access, enforce_venue_manager, get_tenant_context_optional,
    load_employee_in_scope,
)
from rosteriq.services.events import audit
from rosteriq.services.staff_records import linked_logins, retire_logins
from rosteriq.services.venue_access import STAFF, access_map, is_platform_owner

logger = logging.getLogger(__name__)

router = APIRouter()


def _require_owner(current_user) -> None:
    """
    Enforce owner-only access.

    get_current_user returns a UserContext (not a dict), and require_role is a
    decorator factory — so the previous `_require_owner(current_user)`
    calls were silent no-ops (no enforcement). This raises 403 for non-owners.
    """
    if getattr(current_user, "role", None) != "owner":
        raise HTTPException(status_code=403, detail="Owner role required")


def _refuse_demo(current_user) -> None:
    """
    The public "Try Demo" identity must never be able to pull or destroy PII —
    even for the demo venue's own seeded staff. Refuse it outright.
    """
    if getattr(current_user, "user_id", None) == DEMO_USER_ID:
        raise HTTPException(
            status_code=403, detail="This action is not available in the demo"
        )


def _load_employee_in_scope(db, employee_id: str):
    """
    Load an employee by id and enforce that the CURRENT request's user may act
    on that employee's venue.

    Owners pass; managers/staff must have the employee's venue in their
    venue_ids. A cross-venue request gets the SAME 404 as a missing employee so
    employee ids are not an oracle for other tenants' data.
    """
    employee = db.get_employee(employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    try:
        enforce_venue_access(getattr(employee, "venue_id", None))
    except HTTPException as exc:
        if exc.status_code == 403:
            raise HTTPException(status_code=404, detail="Employee not found")
        raise
    return employee


# ============================================================================
# Data Export (Portability)
# ============================================================================

@router.post("/export/me")
async def export_my_data(
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Export my user data as JSON (right to data portability).

    Returns:
    - User profile
    - Venues
    - Employees
    - Preferences
    - Audit trail

    Requires: Authentication
    """
    try:
        service = PrivacyService(db)
        data = service.export_user_data(current_user.user_id)

        # Audit: a full personal-data export left the building (self-service).
        audit("privacy.export", None, "user", current_user.user_id,
              subject="user", subject_id=current_user.user_id,
              email=getattr(current_user, "email", None), self_service=True)

        # Return as downloadable JSON
        return JSONResponse(
            content=data,
            headers={
                "Content-Disposition": f"attachment; filename=user_data_{current_user.user_id}.json"
            }
        )

    except Exception as e:
        logger.error(f"Failed to export user data: {e}")
        raise HTTPException(status_code=500, detail="Failed to export user data")


@router.post("/export/employee/{employee_id}")
async def export_employee_data(
    employee_id: str,
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Export employee data as JSON (manager+ only).

    Returns:
    - Employee profile
    - Shift history
    - Availability
    - Skills
    - Employment details

    Requires: Manager or Owner role, scoped to a venue the user can access
    """
    try:
        # The demo identity can never export PII.
        _refuse_demo(current_user)

        # Authorization: the employee must belong to a venue this user can
        # access (owner passes; manager/staff limited to their venue_ids).
        # Cross-venue -> 404 (not an id oracle).
        employee = _load_employee_in_scope(db, employee_id)

        # Role gate: staff cannot export other people's data.
        if current_user.role == 'staff':
            raise HTTPException(status_code=403, detail="Insufficient permissions")

        service = PrivacyService(db)
        data = service.export_employee_data(employee_id)

        # Audit: someone pulled an employee's personal data (who, whose, where).
        audit("privacy.export", getattr(employee, "venue_id", None), "employee", employee_id,
              subject="employee", subject_id=employee_id,
              employee_name=getattr(employee, "name", None))

        return JSONResponse(
            content=data,
            headers={
                "Content-Disposition": f"attachment; filename=employee_data_{employee_id}.json"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export employee data: {e}")
        raise HTTPException(status_code=500, detail="Failed to export employee data")


# ============================================================================
# Anonymisation (Right to Erasure)
# ============================================================================

@router.post("/anonymise/employee/{employee_id}")
async def anonymise_employee(
    employee_id: str,
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Remove a leaver's personal details ("right to erasure").

    Preserves shift history, employment type, skills, award level and cost
    data. Replaces the name and email, clears phone and work-rights details,
    marks the record inactive, rewrites the name where the venue's feed posts,
    comments and procedure sign-offs copied it, and retires their linked
    login: switched off if it held only this venue, otherwise it just loses
    this venue.

    Requires a FULL manager of the employee's venue (the platform owner
    passes). Another venue's employee is a 404, never a 403.
    """
    try:
        _refuse_demo(current_user)

        # Tenant scope FIRST so a cross-venue caller gets 404 (no id oracle);
        # a record with no venue is the platform owner's alone.
        employee = load_employee_in_scope(db, employee_id)
        _venue_id = getattr(employee, "venue_id", None)
        enforce_venue_manager(_venue_id)

        logins = linked_logins(db, employee)
        if any(u.get("id") == current_user.user_id for u in logins):
            raise HTTPException(status_code=400,
                                detail="You can't remove your own details from here")
        # A login that manages THIS venue is changed through Access or by the
        # platform owner, never as a side effect here. One that is only staff
        # here (a manager elsewhere) loses just this venue.
        tenant = get_tenant_context_optional()
        if not getattr(tenant, "is_owner", False) and any(
                is_platform_owner(u)
                or (access_map(u).get(_venue_id) or {}).get("level", STAFF) != STAFF
                for u in logins):
            raise HTTPException(
                status_code=409,
                detail=f"{employee.name}'s login has manager access. Set their access to staff "
                       "first (Access on their row), then remove their details.")

        service = PrivacyService(db)
        result = service.anonymise_employee(employee_id)

        new_name = getattr(db.get_employee(employee_id), "name", None) or "Former staff member"
        scrubbed = {}
        if _venue_id:
            scrubbed = db.scrub_person_name(_venue_id, employee_id,
                                            [u.get("id") for u in logins], new_name)
        retired = retire_logins(db, _venue_id, logins) if _venue_id else {
            "deactivated": [], "venue_removed": []}

        # The audit row says who removed whose record — by id, never the
        # erased name.
        audit("privacy.anonymise", _venue_id, "employee", employee_id,
              anonymised_fields=list(result.get("anonymised_fields") or []),
              preserved=list(result.get("preserved") or []),
              names_rewritten=scrubbed,
              logins_deactivated=len(retired["deactivated"]),
              logins_venue_removed=len(retired["venue_removed"]))

        return {
            'success': True,
            'employee_id': employee_id,
            'name': new_name,
            'anonymised_at': result['anonymised_at'],
            'preserved': result['preserved'],
            'anonymised_fields': result['anonymised_fields'],
            'names_rewritten': scrubbed,
            'logins_deactivated': len(retired["deactivated"]),
            'logins_venue_removed': len(retired["venue_removed"]),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to anonymise employee: {e}")
        raise HTTPException(status_code=500, detail="Failed to anonymise employee")


# ============================================================================
# Consent Management
# ============================================================================

@router.get("/consent")
async def get_my_consent(
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get my privacy consent status.

    Returns consent for:
    - data_processing: General data processing and storage
    - marketing_emails: Marketing and promotional emails
    - analytics: Analytics and usage tracking
    - third_party_sharing: Sharing data with third parties

    Requires: Authentication
    """
    try:
        service = PrivacyService(db)
        consent_status = service.get_consent_status(current_user.user_id)

        return {
            'user_id': current_user.user_id,
            'consents': consent_status,
        }

    except Exception as e:
        logger.error(f"Failed to get consent status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get consent status")


@router.put("/consent")
async def update_my_consent(
    consent_updates: dict = Body(...),
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Update my privacy consent.

    Request body:
    {
        "data_processing": true,
        "marketing_emails": false,
        "analytics": true,
        "third_party_sharing": false
    }

    Requires: Authentication
    """
    try:
        service = PrivacyService(db)

        # Record each consent
        results = {}
        for consent_type, granted in consent_updates.items():
            result = service.record_consent(
                current_user.user_id,
                consent_type,
                granted
            )
            results[consent_type] = result

        return {
            'user_id': current_user.user_id,
            'consents_updated': results,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update consent: {e}")
        raise HTTPException(status_code=500, detail="Failed to update consent")


# ============================================================================
# Retention Policy Management
# ============================================================================

@router.get("/retention/stats")
async def get_retention_stats(
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get data retention policy statistics.

    Shows:
    - Retention period for each data category (days)
    - Expiry dates
    - Descriptions

    Requires: Admin role
    """
    try:
        _require_owner(current_user)

        service = DataRetentionService(db)
        stats = service.get_retention_stats()

        return {
            'policies': stats,
            'checked_at': datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get retention stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get retention stats")


@router.post("/retention/cleanup")
async def trigger_retention_cleanup(
    dry_run: bool = Query(True, description="If true, show what would be deleted without deleting"),
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Trigger data retention cleanup.

    Query parameters:
    - dry_run: If true, show what would be deleted without actually deleting (default: true)

    Returns:
    - Count of records purged per category
    - Total records purged

    Requires: Admin role

    Example:
        POST /api/privacy/retention/cleanup?dry_run=true  # Preview
        POST /api/privacy/retention/cleanup?dry_run=false # Execute
    """
    try:
        _require_owner(current_user)

        service = DataRetentionService(db)
        stats = service.run_cleanup(dry_run=dry_run)

        # Audit only real purges (a dry run deletes nothing — keep the log quiet).
        if not dry_run:
            audit("privacy.retention_cleanup", None, "retention", None,
                  dry_run=False, stats=stats)

        return {
            'success': True,
            'dry_run': dry_run,
            'stats': stats,
            'executed_at': datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger retention cleanup: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger retention cleanup")


# ============================================================================
# Privacy Audit Trail
# ============================================================================

@router.get("/audit-log")
async def get_my_audit_log(
    limit: int = Query(50, ge=1, le=500),
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get my privacy audit trail (data access, exports, deletions).

    Query parameters:
    - limit: Maximum number of log entries (default: 50, max: 500)

    Returns:
    - List of privacy-related actions on my account

    Requires: Authentication
    """
    try:
        service = PrivacyService(db)
        logs = service.get_privacy_logs(user_id=current_user.user_id, limit=limit)

        return {
            'user_id': current_user.user_id,
            'logs': logs,
            'count': len(logs),
        }

    except Exception as e:
        logger.error(f"Failed to get audit log: {e}")
        raise HTTPException(status_code=500, detail="Failed to get audit log")


@router.get("/audit-log/all")
async def get_all_audit_logs(
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get all privacy audit logs (admin only).

    Query parameters:
    - limit: Maximum number of log entries (default: 100, max: 1000)

    Returns:
    - List of all privacy-related actions system-wide

    Requires: Admin role
    """
    try:
        _require_owner(current_user)

        service = PrivacyService(db)
        logs = service.get_privacy_logs(limit=limit)

        return {
            'logs': logs,
            'count': len(logs),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get audit logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to get audit logs")


# ============================================================================
# Privacy Policy & Compliance Info
# ============================================================================

@router.get("/compliance/info")
async def get_compliance_info():
    """
    Get privacy compliance information.

    Returns:
    - Applicable regulations (Australian Privacy Act 1988)
    - Data retention periods
    - User rights
    - Contact for privacy inquiries

    Public endpoint (no authentication required)
    """
    return {
        'legislation': 'Australian Privacy Act 1988',
        'principles': 'Australian Privacy Principles (APPs)',
        'data_retention_days': {
            'webhook_events': 90,
            'login_attempts': 30,
            'revoked_tokens': 7,
            'billing_events': 2555,  # 7 years for tax compliance
            'notification_logs': 60,
        },
        'user_rights': [
            'Right to know what personal information is held',
            'Right to access your personal information',
            'Right to ask for correction of personal information',
            'Right to ask for deletion or anonymisation (right to erasure)',
            'Right to request your data (data portability)',
            'Right to complain about privacy handling',
        ],
        'contact': {
            'privacy_officer': 'privacy@rosteriq.example.com',
            'data_requests': 'data-request@rosteriq.example.com',
        },
    }
