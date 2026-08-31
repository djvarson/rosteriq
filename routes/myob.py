"""
MYOB integration API routes.

Endpoints for OAuth installation, employee sync, timesheet push,
and payroll data for venues using MYOB as their accounting platform.

Routes:
    POST /api/myob/install-token       -- Connect via API key (pilot flow)
    POST /api/myob/install             -- Start OAuth flow
    GET  /api/myob/callback            -- OAuth callback
    POST /api/myob/uninstall           -- Remove MYOB connection
    GET  /api/myob/status              -- Check connection status
    POST /api/myob/sync/employees      -- Pull employees from MYOB
    GET  /api/myob/company-files       -- List available company files
    POST /api/myob/sync/timesheets     -- Pull timesheets from MYOB
    POST /api/myob/push/timesheet      -- Push timesheet entries to MYOB
    GET  /api/myob/payroll/categories  -- Get payroll wage categories
    GET  /api/myob/payroll/runs        -- Get pay run history
    GET  /api/myob/accounts            -- Get revenue accounts
    POST /api/myob/push-bill           -- Push a supplier invoice to MYOB as a bill
    GET  /api/myob/bill-pushes         -- List invoices already pushed (the push ledger)
"""

import os
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access, enforce_venue_manager
from rosteriq.services.events import audit, record_event
from rosteriq.services.visa import preserve_recorded_work_rights
from rosteriq.myob_adapter import (
    MYOBAdapter,
    MYOBOAuth,
    MYOBCredentials,
    MYOBAPIError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/myob", tags=["myob"])

# Environment config
MYOB_API_KEY = os.environ.get("MYOB_API_KEY", "")
MYOB_API_SECRET = os.environ.get("MYOB_API_SECRET", "")
MYOB_REDIRECT_URI = os.environ.get(
    "MYOB_REDIRECT_URI", "https://api.rosteriq.com.au/api/myob/callback"
)

# Key prefix for plugin_installs table
MYOB_ORG_PREFIX = "myob_"


# ============================================================================
# Pydantic Request Models
# ============================================================================


class MYOBInstallTokenRequest(BaseModel):
    """Direct API key install — venue provides their MYOB credentials."""
    venue_id: str = Field(..., description="RosterIQ venue to connect")
    api_key: str = Field(..., description="MYOB API Key (from developer.myob.com)")
    api_secret: str = Field(..., description="MYOB API Secret")
    access_token: str = Field(default="", description="OAuth access token (if already have one)")
    company_file_id: str = Field(default="", description="MYOB company file ID")
    company_file_uri: str = Field(default="", description="MYOB company file URI")
    cf_username: str = Field(default="Administrator", description="Company file username")
    cf_password: str = Field(default="", description="Company file password")


class MYOBInstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to connect")
    scope: str = Field(default="CompanyFile", description="OAuth scope")


class MYOBUninstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue to disconnect")


class MYOBSyncEmployeesRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    active_only: bool = Field(default=True, description="Only fetch active employees")


class MYOBTimesheetSyncRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")
    employee_uid: Optional[str] = Field(default=None, description="Specific MYOB employee UID")


class MYOBTimesheetPushRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    employee_uid: str = Field(..., description="MYOB employee UID")
    start_date: date = Field(..., description="Week start date")
    entries: list = Field(..., description="Timesheet entries to push")


class PushBillRequest(BaseModel):
    """Push a supplier invoice to MYOB as a Service purchase bill."""
    venue_id: str = Field(..., description="RosterIQ venue ID")
    invoice_id: str = Field(..., description="RosterIQ supplier invoice ID")
    account_display_id: str = Field(
        default="5-1000", description="DisplayID of the MYOB purchases/COGS account"
    )
    tax_code: str = Field(default="GST", description="MYOB tax code for the bill lines")


# ============================================================================
# Helpers
# ============================================================================


def _org_key(venue_id: str) -> str:
    """Generate the organisation_id key used in plugin_installs for MYOB."""
    return f"{MYOB_ORG_PREFIX}{venue_id}"


def _build_credentials(install: dict) -> MYOBCredentials:
    """Reconstruct MYOBCredentials from a stored plugin_install record."""
    tokens = install.get("tokens", {})
    if not tokens:
        raise HTTPException(
            status_code=401,
            detail="MYOB credentials missing. Please re-install.",
        )
    return MYOBCredentials(
        api_key=tokens.get("api_key", MYOB_API_KEY),
        api_secret=tokens.get("api_secret", MYOB_API_SECRET),
        access_token=tokens.get("access_token", ""),
        refresh_token=tokens.get("refresh_token", ""),
        token_expires_at=(
            datetime.fromisoformat(tokens["token_expires_at"])
            if tokens.get("token_expires_at")
            else None
        ),
        company_file_id=tokens.get("company_file_id", ""),
        company_file_uri=tokens.get("company_file_uri", ""),
        cf_username=tokens.get("cf_username", ""),
        cf_password=tokens.get("cf_password", ""),
        product=tokens.get("product", "AccountRight"),
    )


def _persist_refreshed_tokens(install: dict):
    """
    Build an on_token_refresh callback that writes refreshed MYOB tokens back
    into the venue's plugin_install record, so the next request reuses the new
    access/refresh token instead of starting from the expired one.
    """
    def _save(creds: "MYOBCredentials") -> None:
        tokens = install.setdefault("tokens", {})
        tokens["access_token"] = creds.access_token
        tokens["refresh_token"] = creds.refresh_token
        tokens["token_expires_at"] = (
            creds.token_expires_at.isoformat() if creds.token_expires_at else None
        )
        try:
            get_db().save_plugin_install(install)
        except Exception as e:  # noqa: BLE001 — never let persistence failure break the call
            logger.warning(f"Failed to persist refreshed MYOB tokens: {e}")

    return _save


def _get_install_or_404(venue_id: str) -> dict:
    """Fetch the MYOB install record for a venue, or raise 404."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))
    if not install or install.get("status") == "uninstalled":
        raise HTTPException(
            status_code=404,
            detail=f"No active MYOB connection for venue {venue_id}",
        )
    return install


# ============================================================================
# Direct Token Install (pilot flow)
# ============================================================================


@router.post("/install-token")
async def install_token(body: MYOBInstallTokenRequest) -> dict:
    """
    Connect MYOB using API credentials directly.

    For pilot venues — provide API key + secret from developer.myob.com.
    If an access_token is provided (from a prior OAuth flow), it will be
    used directly. Otherwise, the venue can authenticate via OAuth later.
    """
    enforce_venue_manager(body.venue_id)
    db = get_db()
    org_key = _org_key(body.venue_id)

    credentials = MYOBCredentials(
        api_key=body.api_key,
        api_secret=body.api_secret,
        access_token=body.access_token,
        company_file_id=body.company_file_id,
        company_file_uri=body.company_file_uri,
        cf_username=body.cf_username,
        cf_password=body.cf_password,
    )

    # Verify connection if we have an access token
    connected = False
    company_name = "unknown"
    synced_employees = 0

    if body.access_token:
        try:
            async with MYOBAdapter(credentials) as adapter:
                info = await adapter.verify_connection()
                connected = info.get("connected", False)
                company_name = info.get("company_name", "unknown")

                # Auto-sync employees on connect
                if connected:
                    try:
                        employees = await adapter.get_employees(active_only=True)
                        synced_employees = len(employees)
                        for emp in employees:
                            try:
                                emp.venue_id = body.venue_id
                                db.save_employee(preserve_recorded_work_rights(db, emp))
                            except Exception:
                                pass
                        logger.info(
                            f"Auto-synced {synced_employees} employees from MYOB "
                            f"for venue {body.venue_id}"
                        )
                    except Exception as e:
                        logger.warning(f"MYOB employee auto-sync failed: {e}")
        except Exception as e:
            logger.warning(f"MYOB connection verification failed for venue {body.venue_id}: {e}")

    # Save the install record
    install_record = {
        "organisation_id": org_key,
        "venue_id": body.venue_id,
        "provider": "myob",
        "status": "active",
        "tokens": {
            "api_key": body.api_key,
            "api_secret": body.api_secret,
            "access_token": body.access_token,
            "refresh_token": "",
            "company_file_id": body.company_file_id,
            "company_file_uri": body.company_file_uri,
            "cf_username": body.cf_username,
            "cf_password": body.cf_password,
            "product": "AccountRight",
        },
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(install_record)

    logger.info(
        f"MYOB connected for venue {body.venue_id} "
        f"(verified: {connected}, company: {company_name})"
    )
    audit("myob.connect", body.venue_id, "myob_connection", org_key, method="install_token",
          connectivity_verified=connected, company_name=company_name,
          synced_employees=synced_employees)

    return {
        "status": "success",
        "venue_id": body.venue_id,
        "connectivity_verified": connected,
        "company_name": company_name,
        "synced_employees": synced_employees,
        "message": "MYOB connected successfully.",
    }


# ============================================================================
# OAuth Install Flow
# ============================================================================


@router.post("/install")
async def install(body: MYOBInstallRequest) -> dict:
    """Start the MYOB OAuth install flow."""
    enforce_venue_manager(body.venue_id)
    if not MYOB_API_KEY or not MYOB_API_SECRET:
        raise HTTPException(
            status_code=500,
            detail="MYOB OAuth not configured (MYOB_API_KEY / MYOB_API_SECRET missing)",
        )

    oauth = MYOBOAuth(MYOB_API_KEY, MYOB_API_SECRET, MYOB_REDIRECT_URI)
    authorize_url = oauth.get_authorize_url(scope=body.scope)

    db = get_db()
    db.save_plugin_install({
        "organisation_id": _org_key(body.venue_id),
        "venue_id": body.venue_id,
        "provider": "myob",
        "status": "pending",
        "tokens": {},
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    })

    return {
        "status": "pending",
        "authorize_url": authorize_url,
        "venue_id": body.venue_id,
        "message": "Redirect the venue owner to authorize_url to complete installation.",
    }


# ============================================================================
# OAuth Callback
# ============================================================================


@router.get("/callback")
async def oauth_callback(
    code: str = Query(..., description="Authorization code from MYOB"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """OAuth callback — exchange code for tokens."""
    if not MYOB_API_KEY or not MYOB_API_SECRET:
        raise HTTPException(status_code=500, detail="MYOB OAuth not configured")

    db = get_db()
    org_key = _org_key(venue_id)
    install = db.get_plugin_install(org_key)

    if not install:
        raise HTTPException(
            status_code=404,
            detail=f"No pending install for venue {venue_id}. Call /install first.",
        )

    oauth = MYOBOAuth(MYOB_API_KEY, MYOB_API_SECRET, MYOB_REDIRECT_URI)

    try:
        token_data = await oauth.exchange_code(code)
    except MYOBAPIError as e:
        logger.error(f"MYOB token exchange failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=401, detail="OAuth token exchange failed")

    install["status"] = "active"
    install["tokens"] = {
        "api_key": MYOB_API_KEY,
        "api_secret": MYOB_API_SECRET,
        "access_token": token_data.get("access_token", ""),
        "refresh_token": token_data.get("refresh_token", ""),
        "token_expires_at": (
            datetime.now() + timedelta(seconds=token_data.get("expires_in", 1200))
        ).isoformat(),
        "product": "AccountRight",
    }
    install["updated_at"] = datetime.utcnow()
    db.save_plugin_install(install)

    logger.info(f"MYOB OAuth complete for venue {venue_id}")
    audit("myob.connect", venue_id, "myob_connection", org_key, method="oauth")
    return {
        "status": "success",
        "venue_id": venue_id,
        "message": "MYOB connected successfully via OAuth.",
    }


# ============================================================================
# Uninstall
# ============================================================================


@router.post("/uninstall")
async def uninstall(body: MYOBUninstallRequest) -> dict:
    """Remove the MYOB connection for a venue."""
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    install["status"] = "uninstalled"
    install["tokens"] = {}
    install["updated_at"] = datetime.utcnow()
    db = get_db()
    db.save_plugin_install(install)
    logger.info(f"MYOB uninstalled for venue {body.venue_id}")
    audit("myob.disconnect", body.venue_id, "myob_connection", _org_key(body.venue_id))
    return {"status": "success", "venue_id": body.venue_id, "message": "MYOB connection removed."}


# ============================================================================
# Status
# ============================================================================


@router.get("/status")
async def get_status(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """Check whether a venue has an active MYOB connection."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))
    if not install:
        return {"connected": False, "venue_id": venue_id, "status": "not_installed"}
    return {
        "connected": install.get("status") == "active",
        "venue_id": venue_id,
        "status": install.get("status", "unknown"),
        "provider": "myob",
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }


# ============================================================================
# Company Files
# ============================================================================


@router.get("/company-files")
async def list_company_files(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """List available MYOB company files for the connected account."""
    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            files = await adapter.list_company_files()
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    return {
        "status": "success",
        "venue_id": venue_id,
        "count": len(files),
        "company_files": files,
    }


# ============================================================================
# Sync Employees
# ============================================================================


@router.post("/sync/employees")
async def sync_employees(body: MYOBSyncEmployeesRequest) -> dict:
    """Pull employees from MYOB and save to RosterIQ database."""
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            employees = await adapter.get_employees(active_only=body.active_only)
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    # Save to database
    db = get_db()
    saved = 0
    for emp in employees:
        try:
            emp.venue_id = body.venue_id
            db.save_employee(preserve_recorded_work_rights(db, emp))
            saved += 1
        except Exception as e:
            logger.warning(f"Failed to save MYOB employee {emp.name}: {e}")

    logger.info(f"Synced {saved} employees from MYOB for venue {body.venue_id}")
    return {
        "status": "success",
        "venue_id": body.venue_id,
        "count": len(employees),
        "saved": saved,
        "employees": [emp.dict() for emp in employees],
    }


# ============================================================================
# Timesheets
# ============================================================================


@router.post("/sync/timesheets")
async def sync_timesheets(body: MYOBTimesheetSyncRequest) -> dict:
    """Pull timesheets from MYOB for a date range."""
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            timesheets = await adapter.get_timesheets(
                start_date=body.start_date,
                end_date=body.end_date,
                employee_uid=body.employee_uid,
            )
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    return {
        "status": "success",
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "count": len(timesheets),
        "timesheets": timesheets,
    }


@router.post("/push/timesheet")
async def push_timesheet(body: MYOBTimesheetPushRequest) -> dict:
    """Push timesheet entries to MYOB for an employee."""
    enforce_venue_manager(body.venue_id)
    install = _get_install_or_404(body.venue_id)
    credentials = _build_credentials(install)

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            result = await adapter.push_timesheet(
                employee_uid=body.employee_uid,
                start_date=body.start_date,
                entries=body.entries,
            )
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    return {
        "status": "success",
        "venue_id": body.venue_id,
        "employee_uid": body.employee_uid,
        "entries_pushed": len(body.entries),
        "myob_response": result,
    }


# ============================================================================
# Payroll
# ============================================================================


@router.get("/payroll/categories")
async def get_payroll_categories(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """Get payroll wage categories from MYOB."""
    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            categories = await adapter.get_payroll_categories()
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    return {"status": "success", "venue_id": venue_id, **categories}


@router.get("/payroll/runs")
async def get_pay_runs(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
) -> dict:
    """Get pay run history from MYOB."""
    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            runs = await adapter.get_pay_runs(start_date=sd, end_date=ed)
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    return {"status": "success", "venue_id": venue_id, "count": len(runs), "pay_runs": runs}


# ============================================================================
# Accounts (revenue)
# ============================================================================


@router.get("/accounts")
async def get_accounts(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """Get revenue/income accounts from MYOB chart of accounts."""
    install = _get_install_or_404(venue_id)
    credentials = _build_credentials(install)

    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            accounts = await adapter.get_accounts()
    except MYOBAPIError as e:
        raise HTTPException(status_code=502, detail=f"MYOB API error: {e}")

    return {"status": "success", "venue_id": venue_id, "count": len(accounts), "accounts": accounts}


# ============================================================================
# Supplier Bill Push
# ============================================================================


@router.post("/push-bill")
async def push_bill(body: PushBillRequest) -> dict:
    """
    Push an entered supplier invoice to MYOB as a Service purchase bill.

    One push per invoice: a ledger row records the MYOB bill, and pushing
    the same invoice again returns the recorded result instead of creating
    a duplicate bill. Unlike Xero there is no draft state — MYOB AccountRight
    creates a real open bill, which is correct because a RosterIQ supplier
    invoice is already the verified actual delivery.
    """
    enforce_venue_manager(body.venue_id)

    db = get_db()
    install = db.get_plugin_install(_org_key(body.venue_id))
    if not install or install.get("status") != "active":
        raise HTTPException(
            status_code=400,
            detail=(
                "MYOB is not connected for this venue — connect it on the "
                "Connections page first."
            ),
        )

    invoice = None
    for row in db.list_supplier_invoices(body.venue_id) or []:
        if row.get("id") == body.invoice_id:
            invoice = row
            break
    if invoice is None:
        raise HTTPException(
            status_code=404,
            detail=f"Invoice {body.invoice_id} not found for venue {body.venue_id}",
        )

    # Already pushed? Return the ledger row — never create a second bill.
    existing = db.get_myob_bill_push(body.invoice_id)
    if existing and existing.get("venue_id") == body.venue_id:
        return {
            "status": "already_pushed",
            "invoice_id": body.invoice_id,
            "myob_bill_uid": existing.get("myob_bill_uid"),
            "myob_bill_number": existing.get("myob_bill_number"),
        }

    credentials = _build_credentials(install)
    try:
        async with MYOBAdapter(credentials, on_token_refresh=_persist_refreshed_tokens(install)) as adapter:
            result = await adapter.push_bill(
                invoice,
                account_display_id=body.account_display_id,
                tax_code=body.tax_code,
            )
    except Exception as e:
        logger.error(f"Bill push failed for invoice {body.invoice_id}: {e}")
        record_event("audit", "myob.bill_push", venue_id=body.venue_id,
                     resource_type="supplier_invoice", resource_id=body.invoice_id,
                     outcome="failed",
                     details={"reason": str(e)[:300], "supplier": invoice.get("supplier"),
                              "invoice_number": invoice.get("invoice_number"),
                              "total": invoice.get("total")})
        raise HTTPException(status_code=502, detail=f"MYOB rejected the bill: {str(e)}")

    db.save_myob_bill_push({
        "id": body.invoice_id,  # PK == invoice id -> one push per invoice
        "venue_id": body.venue_id,
        "invoice_id": body.invoice_id,
        "myob_bill_uid": result.get("myob_bill_uid"),
        "myob_bill_number": result.get("myob_bill_number"),
        "status": "pushed",
        "pushed_at": datetime.utcnow(),
    })

    logger.info(
        f"Invoice {body.invoice_id} pushed to MYOB as bill "
        f"{result.get('myob_bill_number') or result.get('myob_bill_uid')} "
        f"for {body.venue_id}"
    )
    audit("myob.bill_push", body.venue_id, "supplier_invoice", body.invoice_id,
          myob_bill_uid=result.get("myob_bill_uid"),
          myob_bill_number=result.get("myob_bill_number"),
          supplier=invoice.get("supplier"), invoice_number=invoice.get("invoice_number"),
          total=invoice.get("total"), account_display_id=body.account_display_id,
          tax_code=body.tax_code)

    return {
        "status": "pushed",
        "invoice_id": body.invoice_id,
        "myob_bill_uid": result.get("myob_bill_uid"),
        "myob_bill_number": result.get("myob_bill_number"),
        "myob_status": result.get("status"),
        "total": invoice.get("total"),
    }


@router.get("/bill-pushes")
async def list_bill_pushes(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    List the push ledger for a venue — every supplier invoice already
    pushed to MYOB, newest first.
    """
    enforce_venue_access(venue_id)

    db = get_db()
    rows = db.list_myob_bill_pushes(venue_id) or []
    return {
        "venue_id": venue_id,
        "count": len(rows),
        "pushes": [{**r, "pushed_at": str(r.get("pushed_at"))} for r in rows],
    }
