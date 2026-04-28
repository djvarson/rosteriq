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
from datetime import datetime, date
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

    def __init__(self, credentials: XeroCredentials):
        """
        Initialize Xero Payroll client.

        Args:
            credentials: XeroCredentials with OAuth tokens and tenant ID
        """
        self.credentials = credentials
        self.tenant_id = credentials.tenant_id
        self._token_expires = credentials.token_expires

    async def _ensure_valid_token(self):
        """Refresh token if expired."""
        if datetime.utcnow() >= self._token_expires:
            await self._refresh_token()

    async def _refresh_token(self):
        """Refresh OAuth2 token from Xero."""
        # In production, implement actual token refresh
        # This is a placeholder that would call Xero token endpoint
        logger.warning("Token refresh not yet implemented in payroll client")

    async def push_timesheets(
        self, batch: PayrollBatch
    ) -> XeroPayrollResult:
        """
        Push timesheet batch to Xero Payroll.

        Creates or updates timesheets for each employee in the pay period.

        Args:
            batch: PayrollBatch with prepared timesheet data

        Returns:
            XeroPayrollResult with success status and pay run details
        """
        result = XeroPayrollResult()

        try:
            await self._ensure_valid_token()

            # Step 1: Get Xero payroll settings (employees, earning rates)
            employees_data = await self._get_employees_from_xero()
            if not employees_data:
                result["error_message"] = "Failed to fetch employees from Xero"
                return result

            # Step 2: Create/get pay run
            pay_run = await self._get_or_create_pay_run(
                batch.period_start, batch.period_end
            )
            if not pay_run:
                result["error_message"] = "Failed to create pay run in Xero"
                return result

            result["pay_run_id"] = pay_run.get("PayRunID")

            # Step 3: Push timesheet entries
            success_count = 0
            for emp_payroll in batch.employees:
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
                pushed = await self._push_employee_timesheet(
                    emp_payroll,
                    xero_emp,
                    pay_run.get("PayRunID"),
                )
                if pushed:
                    success_count += 1

            # Step 4: Update result
            result["success"] = success_count == len(batch.employees)
            result["employee_count"] = success_count
            result["total_gross"] = str(batch.total_gross)
            result["exported_at"] = datetime.utcnow().isoformat()
            result["xero_reference"] = f"PR-{batch.batch_id[:8].upper()}"

            if result["success"]:
                logger.info(
                    f"Xero Payroll export successful: {success_count} employees, "
                    f"pay run {pay_run.get('PayRunID')}"
                )
            else:
                logger.warning(
                    f"Xero Payroll export partial: {success_count}/{len(batch.employees)}"
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
    ) -> bool:
        """Push timesheet for a single employee."""
        try:
            # Build earnings data
            earnings = self._build_earnings_for_employee(emp_payroll)

            # POST to Xero Payroll timesheets endpoint
            endpoint = (
                f"{self.XERO_API_BASE}{self.PAYROLL_API_PATH}"
                f"/Employees/{xero_emp['EmployeeID']}/Timesheets"
            )

            payload = {
                "Timesheet": {
                    "EmployeeID": xero_emp["EmployeeID"],
                    "PayRunID": pay_run_id,
                    "TimesheetLines": earnings,
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
                    logger.info(
                        f"Xero timesheet pushed for {emp_payroll.name}"
                    )
                    return True
                else:
                    logger.error(
                        f"Xero timesheet push failed: {response.text}"
                    )
                    return False

        except Exception as e:
            logger.error(
                f"Error pushing timesheet for {emp_payroll.name}: {str(e)}"
            )
            return False

    def _build_earnings_for_employee(self, emp_payroll) -> List[Dict[str, Any]]:
        """Build Xero timesheet lines from employee payroll data."""
        lines = []

        # Ordinary hours
        if emp_payroll.ordinary_hours > 0:
            lines.append({
                "EarningTypeID": XeroEarningType.ordinary_hours.value,
                "NumberOfUnits": float(emp_payroll.ordinary_hours),
                "UnitAmount": float(emp_payroll.ordinary_rate),
            })

        # Penalty rates
        for penalty in emp_payroll.penalty_entries:
            earning_type = self._map_penalty_to_xero_type(penalty.penalty_type)
            lines.append({
                "EarningTypeID": earning_type,
                "NumberOfUnits": float(penalty.hours),
                "UnitAmount": float(emp_payroll.ordinary_rate * penalty.multiplier),
            })

        # Overtime
        if emp_payroll.overtime_hours > 0:
            overtime_type = XeroEarningType.overtime_1_5.value
            lines.append({
                "EarningTypeID": overtime_type,
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
        """Map RosterIQ penalty type to Xero earning type."""
        mapping = {
            "saturday": XeroEarningType.saturday_loading.value,
            "sunday": XeroEarningType.sunday_loading.value,
            "public_holiday": XeroEarningType.public_holiday.value,
            "evening": XeroEarningType.evening_loading.value,
            "night": XeroEarningType.night_loading.value,
            "overtime_1_5": XeroEarningType.overtime_1_5.value,
            "overtime_2_0": XeroEarningType.overtime_2_0.value,
        }
        return mapping.get(str(penalty_type), XeroEarningType.ordinary_hours.value)

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
