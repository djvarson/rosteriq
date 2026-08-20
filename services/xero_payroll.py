"""
Xero Payroll integration service for RosterIQ.

Handles:
- OAuth2 token refresh for Xero API
- Push timesheets to Xero Payroll
- Map RosterIQ categories to Xero earning types
- Pay run status tracking
- Reconciliation with Xero actuals

All monetary values in AUD.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any
from enum import Enum

import httpx

from rosteriq.services.payroll_export import PayrollBatch, PayrollExporter
from rosteriq.xero_integration import XeroCredentials

logger = logging.getLogger(__name__)


class XeroEarningType(str, Enum):
    """Xero Payroll earning types."""
    ordinary_hours = "Ordinary Hours"
    saturday_loading = "Saturday Loading 1.25"
    sunday_loading = "Sunday Loading 1.5"
    public_holiday = "Public Holiday 2.5"
    evening_loading = "Evening Loading 1.15"
    night_loading = "Night Loading 1.175"
    overtime_1_5 = "Overtime 1.5"
    overtime_2_0 = "Overtime 2.0"


class XeroPayrollResult(Dict[str, Any]):
    """Result of Xero Payroll export operation."""

    def __init__(self):
        super().__init__()
        self["success"] = False
        self["pay_run_id"] = None
        self["timesheet_id"] = None
        self["exported_at"] = None
        self["employee_count"] = 0
        self["total_gross"] = "0.00"
        self["error_message"] = None
        self["xero_reference"] = None


class XeroPayrollClient:
    """Client for Xero Payroll API integration."""

    XERO_API_BASE = "https://api.xero.com"
    PAYROLL_API_PATH = "/payroll.xro/2.0"

    def __init__(
        self,
        credentials: XeroCredentials,
        on_token_refresh=None,
        earnings_rate_ids: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize Xero Payroll client.

        Args:
            credentials: XeroCredentials with OAuth tokens and tenant ID
            on_token_refresh: optional callback(credentials) invoked after a
                token refresh so the caller can persist the new tokens.
            earnings_rate_ids: optional pre-configured map of earning-type NAME
                (e.g. "Ordinary Hours") -> Xero EarningsRateID GUID for this
                tenant. When provided, it overrides the live PayItems lookup.
                EarningsRateIDs are tenant-specific GUIDs and MUST be real — Xero
                rejects timesheet lines whose EarningTypeID is not a known GUID.
        """
        self.credentials = credentials
        self.tenant_id = credentials.tenant_id
        self._token_expires = credentials.token_expires
        self._on_token_refresh = on_token_refresh
        # NAME -> EarningsRateID(GUID) for this tenant. Seeded from config if
        # given, else resolved once from the tenant's PayItems at push time.
        self._earnings_rate_map: Dict[str, str] = dict(earnings_rate_ids or {})

    async def _ensure_valid_token(self):
        """Refresh the access token if it has expired (or is about to)."""
        # Refresh slightly early to avoid races right on the expiry boundary.
        # token_expires is naive UTC (set via datetime.utcnow), so compare in UTC.
        if datetime.utcnow() >= self._token_expires - timedelta(seconds=60):
            await self._refresh_token()

    async def _refresh_token(self):
        """
        Refresh the OAuth2 access token via the Xero identity endpoint,
        reusing the canonical refresh implementation in xero_integration.
        """
        # Imported here to avoid a circular import at module load.
        from rosteriq.xero_integration import XeroOAuth

        oauth = XeroOAuth(
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
            redirect_uri="",
        )
        # refresh_access_token mutates and returns the same credentials object,
        # updating access_token / refresh_token / token_expires.
        self.credentials = await oauth.refresh_access_token(self.credentials)
        self._token_expires = self.credentials.token_expires
        logger.info("Xero payroll access token refreshed")

        if self._on_token_refresh:
            try:
                self._on_token_refresh(self.credentials)
            except Exception as e:
                logger.warning(f"Failed to persist refreshed Xero token: {e}")

    async def push_timesheets(
        self,
        batch: PayrollBatch,
        pushed_employees: Optional[List[str]] = None,
        on_employee_pushed=None,
    ) -> XeroPayrollResult:
        """
        Push timesheet batch to Xero Payroll.

        Creates or updates timesheets for each employee in the pay period.

        This is idempotent/resumable: employees whose timesheets have already
        been pushed (their employee_id present in ``pushed_employees``) are
        SKIPPED, so a retry of a partially-failed batch never re-pushes an
        already-paid employee. After each successful per-employee push the
        ``on_employee_pushed`` callback is invoked immediately so the caller
        can checkpoint progress (survives a mid-batch failure AND a restart).

        Args:
            batch: PayrollBatch with prepared timesheet data
            pushed_employees: employee_ids already successfully pushed for this
                batch (from a prior attempt). These are skipped.
            on_employee_pushed: optional callback(employee_id) invoked right
                after each successful per-employee push so the caller can
                persist the updated pushed-set.

        Returns:
            XeroPayrollResult with success status and pay run details
        """
        result = XeroPayrollResult()
        already_pushed = set(pushed_employees or [])

        try:
            await self._ensure_valid_token()

            # Step 1: Get Xero payroll settings (employees, earning rates)
            employees_data = await self._get_employees_from_xero()
            if not employees_data:
                result["error_message"] = "Failed to fetch employees from Xero"
                return result

            # Resolve earning-type NAMES -> tenant EarningsRateID GUIDs. Xero
            # rejects timesheet lines whose EarningTypeID is not a real GUID, so
            # we must translate before pushing. Fail loud (push nothing) if the
            # tenant has no usable earnings rates rather than send bad pay data.
            await self._ensure_earnings_rate_map()
            if not self._earnings_rate_map:
                result["error_message"] = (
                    "No Xero EarningsRateIDs available: could not resolve any "
                    "earning types to tenant GUIDs. Configure earnings_rate_ids "
                    "or ensure the tenant's PayItems expose earnings rates."
                )
                return result

            # Step 2: Create/get pay run
            pay_run = await self._get_or_create_pay_run(
                batch.period_start, batch.period_end
            )
            if not pay_run:
                result["error_message"] = "Failed to create pay run in Xero"
                return result

            result["pay_run_id"] = pay_run.get("PayRunID")

            # Step 3: Push timesheet entries (skipping already-pushed employees)
            retryable_error = None
            for emp_payroll in batch.employees:
                # Idempotency: never re-push an employee already pushed.
                if emp_payroll.employee_id in already_pushed:
                    logger.info(
                        f"Skipping already-pushed Xero employee "
                        f"{emp_payroll.name} ({emp_payroll.employee_id})"
                    )
                    continue

                # Find matching Xero employee
                xero_emp = self._find_xero_employee(
                    emp_payroll, employees_data
                )
                if not xero_emp:
                    logger.warning(
                        f"No Xero employee found for {emp_payroll.name}"
                    )
                    continue

                # Push timesheet for this employee
                idempotency_ref = f"{batch.batch_id}:{emp_payroll.employee_id}"
                pushed, retryable = await self._push_employee_timesheet(
                    emp_payroll,
                    xero_emp,
                    pay_run.get("PayRunID"),
                    idempotency_ref,
                )
                if pushed:
                    # Checkpoint immediately so a later failure does not lose
                    # the record that this employee was already paid.
                    already_pushed.add(emp_payroll.employee_id)
                    if on_employee_pushed:
                        try:
                            on_employee_pushed(emp_payroll.employee_id)
                        except Exception as e:  # noqa: BLE001
                            logger.warning(
                                f"Failed to checkpoint pushed employee "
                                f"{emp_payroll.employee_id}: {e}"
                            )
                elif retryable:
                    # e.g. a 429 — do NOT treat as a permanent failure; record
                    # it so the batch can be retried later (employee not in the
                    # pushed-set, so it will be attempted again, not skipped).
                    retryable_error = (
                        f"Xero rate limited (429) for {emp_payroll.name}; "
                        f"retry the export"
                    )

            # Step 4: Update result. Success means every employee in the batch
            # has now been pushed (either this attempt or a prior one).
            all_done = all(
                e.employee_id in already_pushed for e in batch.employees
            )
            result["success"] = all_done
            result["employee_count"] = len(already_pushed)
            result["total_gross"] = str(batch.total_gross)
            result["exported_at"] = datetime.utcnow().isoformat()
            result["xero_reference"] = f"PR-{batch.batch_id[:8].upper()}"

            if all_done:
                logger.info(
                    f"Xero Payroll export successful: "
                    f"{len(already_pushed)} employees, "
                    f"pay run {pay_run.get('PayRunID')}"
                )
            else:
                if retryable_error and not result["error_message"]:
                    result["error_message"] = retryable_error
                logger.warning(
                    f"Xero Payroll export partial: "
                    f"{len(already_pushed)}/{len(batch.employees)}"
                )

        except Exception as e:
            logger.error(f"Xero Payroll export failed: {str(e)}")
            result["error_message"] = str(e)

        return result

    async def _push_employee_timesheet(
        self,
        emp_payroll,
        xero_emp: Dict[str, Any],
        pay_run_id: str,
        idempotency_ref: Optional[str] = None,
    ) -> tuple:
        """Push timesheet for a single employee.

        Returns a ``(pushed, retryable)`` tuple: ``pushed`` is True on success;
        ``retryable`` is True for transient failures (e.g. HTTP 429) that must
        NOT be treated as a permanent per-employee failure.
        """
        try:
            # Build earnings data
            earnings = self._build_earnings_for_employee(emp_payroll)

            # POST to Xero Payroll timesheets endpoint
            endpoint = (
                f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}"
                f"/Employees/{xero_emp['EmployeeID']}/Timesheets"
            )

            timesheet = {
                "EmployeeID": xero_emp["EmployeeID"],
                "PayRunID": pay_run_id,
                "TimesheetLines": earnings,
            }
            # Idempotency reference derived from f"{batch_id}:{employee_id}" so
            # the provider can de-dupe a re-submitted timesheet.
            if idempotency_ref:
                timesheet["Reference"] = idempotency_ref

            payload = {"Timesheet": timesheet}

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                    "Content-Type": "application/json",
                }
                if idempotency_ref:
                    # Xero honours an Idempotency-Key header to de-dupe writes.
                    headers["Idempotency-Key"] = idempotency_ref
                response = await client.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=10.0,
                )

                if response.status_code in [200, 201]:
                    logger.info(
                        f"Xero timesheet pushed for {emp_payroll.name}"
                    )
                    return True, False
                elif response.status_code == 429:
                    logger.warning(
                        f"Xero timesheet push rate limited (429) for "
                        f"{emp_payroll.name}; will retry"
                    )
                    return False, True
                else:
                    logger.error(
                        f"Xero timesheet push failed: {response.text}"
                    )
                    return False, False

        except Exception as e:
            logger.error(
                f"Error pushing timesheet for {emp_payroll.name}: {str(e)}"
            )
            return False, False

    async def _ensure_earnings_rate_map(self) -> None:
        """
        Populate self._earnings_rate_map (NAME -> EarningsRateID GUID) for the
        tenant, unless it was already supplied via config. Resolved once per
        client. Best-effort: leaves the map empty on failure so the caller can
        fail loud rather than push name-strings as EarningTypeIDs.
        """
        if self._earnings_rate_map:
            return
        self._earnings_rate_map = await self._get_earnings_rates_from_xero()

    async def _get_earnings_rates_from_xero(self) -> Dict[str, str]:
        """
        Fetch the tenant's earnings rates from Xero PayItems and return a
        ``{Name: EarningsRateID}`` map. EarningsRateIDs are the GUIDs Xero
        expects as ``EarningTypeID`` on timesheet lines.
        """
        try:
            endpoint = f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}/PayItems"
            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                }
                response = await client.get(endpoint, headers=headers, timeout=10.0)

            if response.status_code != 200:
                logger.error(f"Failed to fetch Xero PayItems: {response.text}")
                return {}

            data = response.json()
            # PayItems groups rates under EarningsRates (Name + EarningsRateID).
            pay_items = data.get("PayItems") or data
            earnings_rates = []
            if isinstance(pay_items, dict):
                earnings_rates = pay_items.get("EarningsRates", [])
            elif isinstance(pay_items, list):
                earnings_rates = pay_items

            rate_map: Dict[str, str] = {}
            for rate in earnings_rates:
                name = rate.get("Name")
                rate_id = rate.get("EarningsRateID")
                if name and rate_id:
                    rate_map[name] = rate_id
            if not rate_map:
                logger.error(
                    "Xero PayItems returned no usable EarningsRates "
                    "(Name + EarningsRateID)."
                )
            return rate_map
        except Exception as e:
            logger.error(f"Error fetching Xero earnings rates: {str(e)}")
            return {}

    def _resolve_earning_type_id(self, name: str) -> str:
        """
        Translate an earning-type NAME to the tenant's EarningsRateID GUID.

        Fails loud if the name is not present in the tenant's earnings rates —
        pushing the name string (or a placeholder) would create incorrect pay or
        be rejected by Xero.
        """
        rate_id = self._earnings_rate_map.get(name)
        if not rate_id:
            raise ValueError(
                f"No Xero EarningsRateID for earning type '{name}'. Create a "
                f"matching earnings rate in the Xero tenant or map it via "
                f"earnings_rate_ids. Available: {sorted(self._earnings_rate_map)}"
            )
        return rate_id

    def _build_earnings_for_employee(self, emp_payroll) -> List[Dict[str, Any]]:
        """Build Xero timesheet lines from employee payroll data.

        EarningTypeID is resolved to the tenant's real EarningsRateID GUID (see
        _resolve_earning_type_id); a missing mapping raises rather than pushing a
        name string Xero would reject.
        """
        lines = []

        # Ordinary hours
        if emp_payroll.ordinary_hours > 0:
            lines.append({
                "EarningTypeID": self._resolve_earning_type_id(
                    XeroEarningType.ordinary_hours.value
                ),
                "NumberOfUnits": float(emp_payroll.ordinary_hours),
                # Casuals' ordinary hours carry the 25% loading (penalty lines
                # below already price it into their own multipliers).
                "UnitAmount": float(
                    emp_payroll.ordinary_rate
                    * getattr(emp_payroll, "ordinary_multiplier", Decimal("1.00"))
                ),
            })

        # Penalty rates
        for penalty in emp_payroll.penalty_entries:
            earning_type = self._map_penalty_to_xero_type(penalty.penalty_type)
            lines.append({
                "EarningTypeID": self._resolve_earning_type_id(earning_type),
                "NumberOfUnits": float(penalty.hours),
                "UnitAmount": float(emp_payroll.ordinary_rate * penalty.multiplier),
            })

        # Overtime
        if emp_payroll.overtime_hours > 0:
            overtime_type = XeroEarningType.overtime_1_5.value
            lines.append({
                "EarningTypeID": self._resolve_earning_type_id(overtime_type),
                "NumberOfUnits": float(emp_payroll.overtime_hours),
                "UnitAmount": float(emp_payroll.ordinary_rate * Decimal("1.5")),
            })

        # Allowances
        for allowance in emp_payroll.allowances:
            lines.append({
                "Description": allowance.allowance_type,
                "Amount": float(allowance.amount),
            })

        return lines

    def _map_penalty_to_xero_type(self, penalty_type) -> str:
        """Map RosterIQ penalty type to Xero earning type NAME."""
        mapping = {
            "saturday": XeroEarningType.saturday_loading.value,
            "sunday": XeroEarningType.sunday_loading.value,
            "public_holiday": XeroEarningType.public_holiday.value,
            "evening": XeroEarningType.evening_loading.value,
            "night": XeroEarningType.night_loading.value,
            "overtime_1_5": XeroEarningType.overtime_1_5.value,
            "overtime_2_0": XeroEarningType.overtime_2_0.value,
        }
        # Key on .value, not str(): on Python 3.12 str(PenaltyType.saturday) is
        # "PenaltyType.saturday", so EVERY penalty line silently fell through to
        # Ordinary Hours — Saturday, Sunday, public holiday and the loadings all
        # pushed to Xero at ordinary rates.
        key = getattr(penalty_type, "value", None) or str(penalty_type)
        if key not in mapping:
            logger.warning(
                f"No Xero earning type for penalty '{key}' — pushing as Ordinary Hours"
            )
        return mapping.get(key, XeroEarningType.ordinary_hours.value)

    def _find_xero_employee(
        self,
        emp_payroll,
        employees_data: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Find matching Xero employee by email or name."""
        # Try email match first
        if emp_payroll.email:
            for xero_emp in employees_data:
                if xero_emp.get("Email", "").lower() == emp_payroll.email.lower():
                    return xero_emp

        # Try name match
        for xero_emp in employees_data:
            if xero_emp.get("FirstName", "") + " " + xero_emp.get("LastName", "") == emp_payroll.name:
                return xero_emp

        return None

    async def _get_employees_from_xero(self) -> List[Dict[str, Any]]:
        """Fetch list of employees from Xero Payroll."""
        try:
            endpoint = (
                f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}/Employees"
            )

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                }
                response = await client.get(
                    endpoint,
                    headers=headers,
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    return data.get("Employees", [])
                else:
                    logger.error(
                        f"Failed to fetch Xero employees: {response.text}"
                    )
                    return []

        except Exception as e:
            logger.error(f"Error fetching Xero employees: {str(e)}")
            return []

    async def _get_or_create_pay_run(
        self,
        period_start: date,
        period_end: date,
    ) -> Optional[Dict[str, Any]]:
        """Get or create pay run in Xero for the period."""
        try:
            # First, try to get existing pay run
            endpoint = f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}/PayRuns"

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                }

                # Filter by period dates
                params = {
                    "where": f"PayRunPeriodStartDate='{period_start.isoformat()}'"
                }

                response = await client.get(
                    endpoint,
                    headers=headers,
                    params=params,
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    pay_runs = data.get("PayRuns", [])

                    if pay_runs:
                        return pay_runs[0]

                    # Create new pay run if not found
                    return await self._create_pay_run(period_start, period_end)
                else:
                    logger.error(f"Failed to fetch pay runs: {response.text}")
                    return None

        except Exception as e:
            logger.error(f"Error getting pay run: {str(e)}")
            return None

    async def _create_pay_run(
        self,
        period_start: date,
        period_end: date,
    ) -> Optional[Dict[str, Any]]:
        """Create a new pay run in Xero."""
        try:
            endpoint = (
                f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}/PayRuns"
            )

            payload = {
                "PayRun": {
                    "PayRunPeriodStartDate": period_start.isoformat(),
                    "PayRunPeriodEndDate": period_end.isoformat(),
                    "PaymentDate": period_end.isoformat(),
                }
            }

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                    "Content-Type": "application/json",
                }

                response = await client.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=10.0,
                )

                if response.status_code in [200, 201]:
                    data = response.json()
                    return data.get("PayRuns", [{}])[0]
                else:
                    logger.error(f"Failed to create pay run: {response.text}")
                    return None

        except Exception as e:
            logger.error(f"Error creating pay run: {str(e)}")
            return None

    async def get_pay_run_status(self, pay_run_id: str) -> Optional[str]:
        """Get status of a pay run (e.g., 'DRAFT', 'PUBLISHED', 'PAID')."""
        try:
            endpoint = (
                f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}/PayRuns/{pay_run_id}"
            )

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                }

                response = await client.get(
                    endpoint,
                    headers=headers,
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    pay_run = data.get("PayRuns", [{}])[0]
                    return pay_run.get("Status")
                else:
                    logger.error(f"Failed to fetch pay run status: {response.text}")
                    return None

        except Exception as e:
            logger.error(f"Error fetching pay run status: {str(e)}")
            return None

    async def reconcile(
        self,
        venue_id: str,
        period_start: date,
        period_end: date,
        expected_gross: Decimal,
    ) -> Dict[str, Any]:
        """
        Reconcile RosterIQ payroll with Xero actuals.

        Compares exported timesheet data with what's recorded in Xero.

        Returns:
            Dict with reconciliation status and any discrepancies
        """
        result = {
            "venue_id": venue_id,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "reconciled": False,
            "expected_gross": str(expected_gross),
            "xero_gross": "0.00",
            "variance": "0.00",
            "variance_percentage": 0.0,
            "discrepancies": [],
        }

        try:
            # Fetch pay runs for the period
            endpoint = f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}/PayRuns"

            async with httpx.AsyncClient() as client:
                headers = {
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Xero-tenant-id": self.tenant_id,
                }

                params = {
                    "where": (
                        f"PayRunPeriodStartDate='{period_start.isoformat()}' "
                        f"AND PayRunPeriodEndDate='{period_end.isoformat()}'"
                    )
                }

                response = await client.get(
                    endpoint,
                    headers=headers,
                    params=params,
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    pay_runs = data.get("PayRuns", [])

                    if not pay_runs:
                        result["discrepancies"].append(
                            "No pay runs found in Xero for this period"
                        )
                        return result

                    pay_run = pay_runs[0]

                    # Calculate totals from pay run
                    xero_gross = Decimal("0.00")
                    for payslip in pay_run.get("Payslips", []):
                        xero_gross += Decimal(
                            str(payslip.get("GrossPayableAmount", 0))
                        )

                    result["xero_gross"] = str(xero_gross)
                    result["variance"] = str(abs(expected_gross - xero_gross))

                    if xero_gross > 0:
                        variance_pct = (
                            abs(expected_gross - xero_gross) / xero_gross * 100
                        )
                        result["variance_percentage"] = round(variance_pct, 2)

                    # Check if variance is acceptable (< 1%)
                    result["reconciled"] = result["variance_percentage"] < 1.0

                    return result

        except Exception as e:
            logger.error(f"Error reconciling with Xero: {str(e)}")
            result["discrepancies"].append(str(e))

        return result
