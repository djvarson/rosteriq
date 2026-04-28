"""
KeyPay (Employment Hero) integration service for RosterIQ.

Handles:
- API key authentication
- Push timesheets to KeyPay
- Employee matching by email or external ID
- Pay category mapping
- Rate limiting (100 req/min)
- Reconciliation with KeyPay actuals

Base URL: https://api.yourpayroll.com.au/api/v2
All monetary values in AUD.
"""

import asyncio
import logging
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Dict, List, Any
from enum import Enum

import httpx

from rosteriq.services.payroll_export import PayrollBatch, PayrollExporter, PenaltyType

logger = logging.getLogger(__name__)


class KeyPayPayCategory(str, Enum):
    """KeyPay pay categories."""
    ordinary = "Ordinary Hours"
    saturday = "Saturday Loading"
    sunday = "Sunday Loading"
    public_holiday = "Public Holiday"
    evening = "Evening Loading"
    night = "Night Loading"
    overtime_1_5 = "Overtime 1.5"
    overtime_2_0 = "Overtime 2.0"
    meal_allowance = "Meal Allowance"
    split_shift = "Split Shift Loading"
    laundry = "Laundry"


class KeyPayResult(Dict[str, Any]):
    """Result of KeyPay export operation."""

    def __init__(self):
        super().__init__()
        self["success"] = False
        self["timesheet_ids"] = []
        self["exported_at"] = None
        self["employee_count"] = 0
        self["total_gross"] = "0.00"
        self["error_message"] = None
        self["keypay_reference"] = None


class KeyPayClient:
    """Client for KeyPay API integration."""

    API_BASE = "https://api.yourpayroll.com.au/api/v2"
    RATE_LIMIT = 100  # requests per minute
    REQUEST_DELAY = 1.0 / (RATE_LIMIT / 60)  # seconds between requests

    def __init__(self, api_key: str, business_id: str):
        """
        Initialize KeyPay client.

        Args:
            api_key: KeyPay API key
            business_id: KeyPay business/organisation ID
        """
        self.api_key = api_key
        self.business_id = business_id
        self._last_request_time = 0.0

    async def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.REQUEST_DELAY:
            await asyncio.sleep(self.REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for KeyPay API requests."""
        return {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    async def push_timesheets(
        self, batch: PayrollBatch
    ) -> KeyPayResult:
        """
        Push timesheet batch to KeyPay.

        Creates timesheet entries for each employee in the pay period.

        Args:
            batch: PayrollBatch with prepared timesheet data

        Returns:
            KeyPayResult with success status and timesheet IDs
        """
        result = KeyPayResult()

        try:
            # Step 1: Fetch KeyPay employees for matching
            employees_data = await self.get_employees()
            if not employees_data:
                result["error_message"] = "Failed to fetch employees from KeyPay"
                return result

            # Step 2: Push timesheet for each employee
            success_count = 0
            timesheet_ids = []

            for emp_payroll in batch.employees:
                # Find matching KeyPay employee
                keypay_emp = self._find_keypay_employee(
                    emp_payroll, employees_data
                )
                if not keypay_emp:
                    logger.warning(
                        f"No KeyPay employee found for {emp_payroll.name}"
                    )
                    continue

                # Push timesheet for this employee
                timesheet_id = await self._push_employee_timesheet(
                    emp_payroll,
                    keypay_emp,
                    batch.period_start,
                    batch.period_end,
                )
                if timesheet_id:
                    success_count += 1
                    timesheet_ids.append(timesheet_id)

            # Step 3: Update result
            result["success"] = success_count == len(batch.employees)
            result["timesheet_ids"] = timesheet_ids
            result["employee_count"] = success_count
            result["total_gross"] = str(batch.total_gross)
            result["exported_at"] = datetime.utcnow().isoformat()
            result["keypay_reference"] = f"KP-{batch.batch_id[:8].upper()}"

            if result["success"]:
                logger.info(
                    f"KeyPay export successful: {success_count} timesheets"
                )
            else:
                logger.warning(
                    f"KeyPay export partial: {success_count}/{len(batch.employees)}"
                )

        except Exception as e:
            logger.error(f"KeyPay export failed: {str(e)}")
            result["error_message"] = str(e)

        return result

    async def _push_employee_timesheet(
        self,
        emp_payroll,
        keypay_emp: Dict[str, Any],
        period_start: date,
        period_end: date,
    ) -> Optional[str]:
        """Push timesheet for a single employee to KeyPay."""
        try:
            await self._rate_limit()

            # Build pay categories for this employee
            pay_categories = self._build_pay_categories(emp_payroll)

            endpoint = (
                f"{self.API_BASE}/business/{self.business_id}/timesheet"
            )

            payload = {
                "employeeId": keypay_emp["id"],
                "timesheetDate": period_start.isoformat(),
                "endDate": period_end.isoformat(),
                "payCategories": pay_categories,
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    endpoint,
                    json=payload,
                    headers=self._get_headers(),
                    timeout=10.0,
                )

                if response.status_code in [200, 201]:
                    data = response.json()
                    timesheet_id = data.get("id") or data.get("timesheetId")
                    logger.info(
                        f"KeyPay timesheet pushed for {emp_payroll.name} "
                        f"(ID: {timesheet_id})"
                    )
                    return timesheet_id
                else:
                    logger.error(
                        f"KeyPay timesheet push failed: {response.text}"
                    )
                    return None

        except Exception as e:
            logger.error(
                f"Error pushing timesheet for {emp_payroll.name}: {str(e)}"
            )
            return None

    def _build_pay_categories(self, emp_payroll) -> List[Dict[str, Any]]:
        """Build KeyPay pay categories from employee payroll data."""
        categories = []

        # Ordinary hours
        if emp_payroll.ordinary_hours > 0:
            categories.append({
                "payCategory": KeyPayPayCategory.ordinary.value,
                "hours": float(emp_payroll.ordinary_hours),
                "costAmount": float(emp_payroll.ordinary_gross),
            })

        # Penalty rates
        for penalty in emp_payroll.penalty_entries:
            pay_category = self._map_penalty_to_keypay_category(
                penalty.penalty_type
            )
            categories.append({
                "payCategory": pay_category,
                "hours": float(penalty.hours),
                "costAmount": float(penalty.amount),
            })

        # Overtime
        if emp_payroll.overtime_hours > 0:
            categories.append({
                "payCategory": KeyPayPayCategory.overtime_1_5.value,
                "hours": float(emp_payroll.overtime_hours),
                "costAmount": float(emp_payroll.overtime_amount),
            })

        # Allowances
        for allowance in emp_payroll.allowances:
            category_name = self._map_allowance_to_keypay_category(
                allowance.allowance_type
            )
            categories.append({
                "payCategory": category_name,
                "costAmount": float(allowance.amount),
            })

        # Leave
        for leave in emp_payroll.leave_entries:
            categories.append({
                "payCategory": leave.leave_type.capitalize(),
                "hours": float(leave.hours),
                "costAmount": float(leave.amount),
            })

        return categories

    def _map_penalty_to_keypay_category(self, penalty_type) -> str:
        """Map RosterIQ penalty type to KeyPay pay category."""
        mapping = {
            PenaltyType.ordinary: KeyPayPayCategory.ordinary.value,
            PenaltyType.saturday: KeyPayPayCategory.saturday.value,
            PenaltyType.sunday: KeyPayPayCategory.sunday.value,
            PenaltyType.public_holiday: KeyPayPayCategory.public_holiday.value,
            PenaltyType.evening: KeyPayPayCategory.evening.value,
            PenaltyType.night: KeyPayPayCategory.night.value,
            PenaltyType.overtime_1_5: KeyPayPayCategory.overtime_1_5.value,
            PenaltyType.overtime_2_0: KeyPayPayCategory.overtime_2_0.value,
        }
        penalty_str = str(penalty_type).split(".")[-1]
        return mapping.get(penalty_str, KeyPayPayCategory.ordinary.value)

    def _map_allowance_to_keypay_category(self, allowance_type: str) -> str:
        """Map allowance type to KeyPay pay category."""
        mapping = {
            "meal": KeyPayPayCategory.meal_allowance.value,
            "split_shift": KeyPayPayCategory.split_shift.value,
            "laundry": KeyPayPayCategory.laundry.value,
        }
        return mapping.get(allowance_type, allowance_type)

    def _find_keypay_employee(
        self,
        emp_payroll,
        employees_data: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Find matching KeyPay employee by email or name."""
        # Try email match first
        if emp_payroll.email:
            for keypay_emp in employees_data:
                if keypay_emp.get("email", "").lower() == emp_payroll.email.lower():
                    return keypay_emp

        # Try name match
        for keypay_emp in employees_data:
            full_name = (
                keypay_emp.get("firstName", "") + " " +
                keypay_emp.get("lastName", "")
            ).strip()
            if full_name.lower() == emp_payroll.name.lower():
                return keypay_emp

        # Try by external ID (RosterIQ employee_id)
        for keypay_emp in employees_data:
            if keypay_emp.get("externalId") == emp_payroll.employee_id:
                return keypay_emp

        return None

    async def get_employees(self) -> List[Dict[str, Any]]:
        """Fetch list of employees from KeyPay."""
        try:
            await self._rate_limit()

            endpoint = f"{self.API_BASE}/business/{self.business_id}/employees"

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    endpoint,
                    headers=self._get_headers(),
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    return data.get("employees", [])
                else:
                    logger.error(
                        f"Failed to fetch KeyPay employees: {response.text}"
                    )
                    return []

        except Exception as e:
            logger.error(f"Error fetching KeyPay employees: {str(e)}")
            return []

    async def reconcile(
        self,
        venue_id: str,
        period_start: date,
        period_end: date,
        expected_gross: Decimal,
    ) -> Dict[str, Any]:
        """
        Reconcile RosterIQ payroll with KeyPay actuals.

        Compares exported timesheet data with what's recorded in KeyPay.

        Returns:
            Dict with reconciliation status and any discrepancies
        """
        result = {
            "venue_id": venue_id,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "reconciled": False,
            "expected_gross": str(expected_gross),
            "keypay_gross": "0.00",
            "variance": "0.00",
            "variance_percentage": 0.0,
            "discrepancies": [],
        }

        try:
            await self._rate_limit()

            # Fetch timesheets for the period from KeyPay
            endpoint = (
                f"{self.API_BASE}/business/{self.business_id}/timesheets"
            )

            params = {
                "startDate": period_start.isoformat(),
                "endDate": period_end.isoformat(),
            }

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    endpoint,
                    params=params,
                    headers=self._get_headers(),
                    timeout=10.0,
                )

                if response.status_code == 200:
                    data = response.json()
                    timesheets = data.get("timesheets", [])

                    if not timesheets:
                        result["discrepancies"].append(
                            "No timesheets found in KeyPay for this period"
                        )
                        return result

                    # Calculate totals from timesheets
                    keypay_gross = Decimal("0.00")
                    for timesheet in timesheets:
                        for category in timesheet.get("payCategories", []):
                            keypay_gross += Decimal(
                                str(category.get("costAmount", 0))
                            )

                    result["keypay_gross"] = str(keypay_gross)
                    result["variance"] = str(abs(expected_gross - keypay_gross))

                    if keypay_gross > 0:
                        variance_pct = (
                            abs(expected_gross - keypay_gross) / keypay_gross * 100
                        )
                        result["variance_percentage"] = round(variance_pct, 2)

                    # Check if variance is acceptable (< 1%)
                    result["reconciled"] = result["variance_percentage"] < 1.0

                    return result

        except Exception as e:
            logger.error(f"Error reconciling with KeyPay: {str(e)}")
            result["discrepancies"].append(str(e))

        return result
