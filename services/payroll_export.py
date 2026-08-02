"""
Payroll export service for RosterIQ.

Prepares timesheet data for export to payroll systems (Xero Payroll, KeyPay).
Handles:
- Timesheet aggregation (ordinary, penalty, overtime hours)
- Leave reconciliation
- Superannuation calculations (11.5%)
- Allowances (meal, split shift, laundry per MA000009)
- Pay period grouping (weekly/fortnightly)
- Data validation and reconciliation

All monetary values in AUD using Decimal for precision.
"""

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Any
from enum import Enum

from rosteriq.models import Employee, Shift, EmploymentType, DayType, State
from rosteriq.award_rules import (
    get_day_type,
    get_penalty_multiplier,
    split_weekday_hours_by_band,
)
from rosteriq.cost_calculator import SUPER_RATE


class PayrollStatus(str, Enum):
    """Status of a payroll batch."""
    draft = "draft"
    approved = "approved"
    exported = "exported"
    reconciled = "reconciled"


class PenaltyType(str, Enum):
    """Types of penalty rates."""
    ordinary = "ordinary"
    saturday = "saturday"
    sunday = "sunday"
    public_holiday = "public_holiday"
    evening = "evening"
    night = "night"
    overtime_1_5 = "overtime_1_5"
    overtime_2_0 = "overtime_2_0"


@dataclass
class PenaltyEntry:
    """Single penalty rate entry for an employee."""
    penalty_type: PenaltyType
    hours: Decimal
    multiplier: Decimal
    amount: Decimal


@dataclass
class Allowance:
    """Allowance entry for an employee."""
    allowance_type: str  # "meal", "split_shift", "laundry"
    amount: Decimal


@dataclass
class LeaveEntry:
    """Leave entry for an employee."""
    leave_type: str  # "annual", "personal", "long_service"
    hours: Decimal
    rate: Decimal
    amount: Decimal


@dataclass
class EmployeePayroll:
    """Payroll data for a single employee in a pay period."""
    employee_id: str
    name: str
    email: Optional[str]
    tax_file_number_masked: Optional[str]  # Last 3 digits only for security

    # Hours worked
    ordinary_hours: Decimal = Decimal("0.00")
    ordinary_rate: Decimal = Decimal("0.00")
    ordinary_gross: Decimal = Decimal("0.00")

    # Penalty rates
    penalty_entries: List[PenaltyEntry] = field(default_factory=list)
    penalty_gross: Decimal = Decimal("0.00")

    # Overtime
    overtime_hours: Decimal = Decimal("0.00")
    overtime_amount: Decimal = Decimal("0.00")

    # Leave
    leave_entries: List[LeaveEntry] = field(default_factory=list)
    leave_gross: Decimal = Decimal("0.00")

    # Allowances
    allowances: List[Allowance] = field(default_factory=list)
    allowances_total: Decimal = Decimal("0.00")

    # Superannuation
    super_amount: Decimal = Decimal("0.00")

    @property
    def total_ordinary_hours(self) -> Decimal:
        """Total ordinary hours worked."""
        return self.ordinary_hours

    @property
    def total_penalty_hours(self) -> Decimal:
        """Total hours at penalty rates."""
        return sum(pe.hours for pe in self.penalty_entries)

    @property
    def total_leave_hours(self) -> Decimal:
        """Total leave hours taken."""
        return sum(le.hours for le in self.leave_entries)

    @property
    def total_hours(self) -> Decimal:
        """Total hours (ordinary + penalty + overtime + leave)."""
        return (
            self.ordinary_hours
            + self.total_penalty_hours
            + self.overtime_hours
            + self.total_leave_hours
        )

    @property
    def total_gross(self) -> Decimal:
        """Total gross pay (before super, after super is itemized)."""
        return (
            self.ordinary_gross
            + self.penalty_gross
            + self.overtime_amount
            + self.leave_gross
            + self.allowances_total
        )

    @property
    def total_with_super(self) -> Decimal:
        """Total including superannuation."""
        return self.total_gross + self.super_amount


@dataclass
class PayrollBatch:
    """Payroll data for a pay period across all employees."""
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    venue_id: str = ""
    period_start: date = field(default_factory=date.today)
    period_end: date = field(default_factory=date.today)

    employees: List[EmployeePayroll] = field(default_factory=list)

    status: PayrollStatus = PayrollStatus.draft
    created_at: datetime = field(default_factory=datetime.utcnow)
    approved_at: Optional[datetime] = None
    exported_at: Optional[datetime] = None

    # Totals
    total_ordinary_hours: Decimal = Decimal("0.00")
    total_penalty_hours: Decimal = Decimal("0.00")
    total_overtime_hours: Decimal = Decimal("0.00")
    total_leave_hours: Decimal = Decimal("0.00")

    total_ordinary_gross: Decimal = Decimal("0.00")
    total_penalty_gross: Decimal = Decimal("0.00")
    total_overtime_gross: Decimal = Decimal("0.00")
    total_leave_gross: Decimal = Decimal("0.00")
    total_allowances: Decimal = Decimal("0.00")

    total_gross: Decimal = Decimal("0.00")
    total_super: Decimal = Decimal("0.00")

    # Validation
    validation_errors: List[str] = field(default_factory=list)

    def calculate_totals(self):
        """Recalculate all batch totals."""
        self.total_ordinary_hours = sum(e.ordinary_hours for e in self.employees)
        self.total_penalty_hours = sum(e.total_penalty_hours for e in self.employees)
        self.total_overtime_hours = sum(e.overtime_hours for e in self.employees)
        self.total_leave_hours = sum(e.total_leave_hours for e in self.employees)

        self.total_ordinary_gross = sum(e.ordinary_gross for e in self.employees)
        self.total_penalty_gross = sum(e.penalty_gross for e in self.employees)
        self.total_overtime_gross = sum(e.overtime_amount for e in self.employees)
        self.total_leave_gross = sum(e.leave_gross for e in self.employees)
        self.total_allowances = sum(e.allowances_total for e in self.employees)

        self.total_gross = sum(e.total_gross for e in self.employees)
        self.total_super = sum(e.super_amount for e in self.employees)

    def to_dict(self) -> Dict[str, Any]:
        """Convert batch to dictionary for storage."""
        return {
            "batch_id": self.batch_id,
            "venue_id": self.venue_id,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "exported_at": self.exported_at.isoformat() if self.exported_at else None,
            "total_ordinary_hours": str(self.total_ordinary_hours),
            "total_penalty_hours": str(self.total_penalty_hours),
            "total_overtime_hours": str(self.total_overtime_hours),
            "total_leave_hours": str(self.total_leave_hours),
            "total_ordinary_gross": str(self.total_ordinary_gross),
            "total_penalty_gross": str(self.total_penalty_gross),
            "total_overtime_gross": str(self.total_overtime_gross),
            "total_leave_gross": str(self.total_leave_gross),
            "total_allowances": str(self.total_allowances),
            "total_gross": str(self.total_gross),
            "total_super": str(self.total_super),
            "employees": [_employee_payroll_to_dict(e) for e in self.employees],
            "validation_errors": self.validation_errors,
        }


def _employee_payroll_to_dict(emp: EmployeePayroll) -> Dict[str, Any]:
    """Convert EmployeePayroll to dictionary."""
    return {
        "employee_id": emp.employee_id,
        "name": emp.name,
        "email": emp.email,
        "tax_file_number_masked": emp.tax_file_number_masked,
        "ordinary_hours": str(emp.ordinary_hours),
        "ordinary_rate": str(emp.ordinary_rate),
        "ordinary_gross": str(emp.ordinary_gross),
        "penalty_entries": [
            {
                "penalty_type": pe.penalty_type.value,
                "hours": str(pe.hours),
                "multiplier": str(pe.multiplier),
                "amount": str(pe.amount),
            }
            for pe in emp.penalty_entries
        ],
        "penalty_gross": str(emp.penalty_gross),
        "overtime_hours": str(emp.overtime_hours),
        "overtime_amount": str(emp.overtime_amount),
        "leave_entries": [
            {
                "leave_type": le.leave_type,
                "hours": str(le.hours),
                "rate": str(le.rate),
                "amount": str(le.amount),
            }
            for le in emp.leave_entries
        ],
        "leave_gross": str(emp.leave_gross),
        "allowances": [
            {
                "allowance_type": a.allowance_type,
                "amount": str(a.amount),
            }
            for a in emp.allowances
        ],
        "allowances_total": str(emp.allowances_total),
        "super_amount": str(emp.super_amount),
        "total_gross": str(emp.total_gross),
        "total_with_super": str(emp.total_with_super),
    }


def _employee_payroll_from_dict(d: Dict[str, Any]) -> EmployeePayroll:
    """Reconstruct an EmployeePayroll from its stored dict (reverse of
    _employee_payroll_to_dict). Computed properties (total_gross, etc.) are
    intentionally NOT set — they derive from the fields below."""
    return EmployeePayroll(
        employee_id=d["employee_id"],
        name=d["name"],
        email=d.get("email"),
        tax_file_number_masked=d.get("tax_file_number_masked"),
        ordinary_hours=Decimal(d.get("ordinary_hours", "0.00")),
        ordinary_rate=Decimal(d.get("ordinary_rate", "0.00")),
        ordinary_gross=Decimal(d.get("ordinary_gross", "0.00")),
        penalty_entries=[
            PenaltyEntry(
                penalty_type=PenaltyType(pe["penalty_type"]),
                hours=Decimal(pe["hours"]),
                multiplier=Decimal(pe["multiplier"]),
                amount=Decimal(pe["amount"]),
            )
            for pe in d.get("penalty_entries", [])
        ],
        penalty_gross=Decimal(d.get("penalty_gross", "0.00")),
        overtime_hours=Decimal(d.get("overtime_hours", "0.00")),
        overtime_amount=Decimal(d.get("overtime_amount", "0.00")),
        leave_entries=[
            LeaveEntry(
                leave_type=le["leave_type"],
                hours=Decimal(le["hours"]),
                rate=Decimal(le["rate"]),
                amount=Decimal(le["amount"]),
            )
            for le in d.get("leave_entries", [])
        ],
        leave_gross=Decimal(d.get("leave_gross", "0.00")),
        allowances=[
            Allowance(allowance_type=a["allowance_type"], amount=Decimal(a["amount"]))
            for a in d.get("allowances", [])
        ],
        allowances_total=Decimal(d.get("allowances_total", "0.00")),
        super_amount=Decimal(d.get("super_amount", "0.00")),
    )


def payroll_batch_from_dict(data: Dict[str, Any]) -> PayrollBatch:
    """
    Reconstruct a PayrollBatch from its stored dict (reverse of
    PayrollBatch.to_dict) so a persisted batch can be pushed to a payroll
    provider. Totals are recomputed from the employees for self-consistency.
    """
    batch = PayrollBatch(
        batch_id=data.get("batch_id", ""),
        venue_id=data.get("venue_id", ""),
        period_start=date.fromisoformat(data["period_start"]),
        period_end=date.fromisoformat(data["period_end"]),
        status=PayrollStatus(data.get("status", "draft")),
        created_at=(
            datetime.fromisoformat(data["created_at"])
            if data.get("created_at") else datetime.utcnow()
        ),
        approved_at=(
            datetime.fromisoformat(data["approved_at"]) if data.get("approved_at") else None
        ),
        exported_at=(
            datetime.fromisoformat(data["exported_at"]) if data.get("exported_at") else None
        ),
        employees=[_employee_payroll_from_dict(e) for e in data.get("employees", [])],
        validation_errors=data.get("validation_errors", []),
    )
    batch.calculate_totals()
    return batch


class PayrollExporter:
    """Base class for payroll export operations."""

    def __init__(self, db):
        """
        Initialize payroll exporter.

        Args:
            db: Database store instance (BaseStore)
        """
        self.db = db

    def prepare_timesheet_data(
        self,
        venue_id: str,
        period_start: date,
        period_end: date,
        state: State,
        shifts: List[Shift],
        employees: Dict[str, Employee],
    ) -> PayrollBatch:
        """
        Prepare timesheet data for a pay period.

        Aggregates shifts by employee and calculates:
        - Ordinary hours and costs
        - Penalty rates (Saturday, Sunday, Public Holiday, evening, night)
        - Overtime hours and costs
        - Leave hours (if provided)
        - Superannuation
        - Allowances

        Args:
            venue_id: Venue ID
            period_start: Pay period start date (inclusive)
            period_end: Pay period end date (inclusive)
            state: State for public holiday calculations
            shifts: List of Shift objects for the period
            employees: Dict of {employee_id: Employee}

        Returns:
            PayrollBatch with prepared timesheet data
        """
        batch = PayrollBatch(
            venue_id=venue_id,
            period_start=period_start,
            period_end=period_end,
            status=PayrollStatus.draft,
        )

        # Group shifts by employee
        shifts_by_emp: Dict[str, List[Shift]] = {}
        for shift in shifts:
            if shift.date < period_start or shift.date > period_end:
                continue
            if shift.employee_id not in shifts_by_emp:
                shifts_by_emp[shift.employee_id] = []
            shifts_by_emp[shift.employee_id].append(shift)

        # Process each employee
        for emp_id, emp_shifts in shifts_by_emp.items():
            if emp_id not in employees:
                continue

            emp = employees[emp_id]
            emp_payroll = self._process_employee_shifts(
                emp, emp_shifts, state, period_start, period_end
            )
            if emp_payroll:
                batch.employees.append(emp_payroll)

        # Calculate batch totals
        batch.calculate_totals()

        return batch

    def _process_employee_shifts(
        self,
        employee: Employee,
        shifts: List[Shift],
        state: State,
        period_start: date,
        period_end: date,
    ) -> Optional[EmployeePayroll]:
        """
        Process shifts for a single employee.

        Calculates ordinary hours, penalty entries, overtime, allowances, and super.
        """
        emp_payroll = EmployeePayroll(
            employee_id=employee.id,
            name=employee.name,
            email=employee.email,
            tax_file_number_masked=self._mask_tfn(employee.id),
            ordinary_rate=employee.hourly_base_rate,
        )

        # Aggregate hours by penalty type. Shifts are processed CHRONOLOGICALLY
        # and accumulated so hours beyond 38/week become overtime (only the
        # ordinary portion is bucketed by penalty type). Previously every hour
        # was bucketed as ordinary/penalty and overtime was never computed.
        ordinary_total = Decimal("0.00")
        penalties_by_type: Dict[PenaltyType, Decimal] = {}
        overtime_total = Decimal("0.00")
        overtime_amount = Decimal("0.00")
        base_rate = employee.hourly_base_rate
        is_casual = employee.employment_type == EmploymentType.casual
        cumulative = Decimal("0")
        ot_used = Decimal("0")  # OT hours costed so far this week (for 1.5x->2x tier)

        for shift in sorted(shifts, key=lambda s: (s.date, s.start_time or time(0, 0))):
            day_type = get_day_type(shift.date, state)
            net_hours = Decimal(str(shift.net_hours))
            if net_hours <= 0:
                continue

            # Split into ordinary (<=38h cumulative) vs overtime (>38h). Casuals
            # do not accrue overtime.
            if is_casual:
                ord_hrs, ot_hrs = net_hours, Decimal("0.00")
            else:
                remaining_ordinary = max(Decimal("0.00"), Decimal("38") - cumulative)
                ord_hrs = min(net_hours, remaining_ordinary)
                ot_hrs = net_hours - ord_hrs
            cumulative += net_hours

            # Bucket the ORDINARY portion by penalty type.
            if ord_hrs > 0:
                if day_type == DayType.public_holiday:
                    penalties_by_type[PenaltyType.public_holiday] = (
                        penalties_by_type.get(PenaltyType.public_holiday, Decimal("0.00")) + ord_hrs
                    )
                elif day_type == DayType.sunday:
                    penalties_by_type[PenaltyType.sunday] = (
                        penalties_by_type.get(PenaltyType.sunday, Decimal("0.00")) + ord_hrs
                    )
                elif day_type == DayType.saturday:
                    penalties_by_type[PenaltyType.saturday] = (
                        penalties_by_type.get(PenaltyType.saturday, Decimal("0.00")) + ord_hrs
                    )
                else:
                    # Weekday: evening/night loading applies to the hours ACTUALLY
                    # WORKED in each window (19:00-24:00 evening, 00:00-07:00 night),
                    # not to the whole shift gated on its start hour. Distribute the
                    # ordinary portion across bands in the shift's own proportions.
                    if shift.start_time and shift.end_time:
                        bands = split_weekday_hours_by_band(
                            shift.start_time, shift.end_time, net_hours
                        )
                        factor = (ord_hrs / net_hours) if net_hours > 0 else Decimal("0")
                        ordinary_total += bands["ordinary"] * factor
                        if bands["evening"] > 0:
                            penalties_by_type[PenaltyType.evening] = (
                                penalties_by_type.get(PenaltyType.evening, Decimal("0.00"))
                                + bands["evening"] * factor
                            )
                        if bands["night"] > 0:
                            penalties_by_type[PenaltyType.night] = (
                                penalties_by_type.get(PenaltyType.night, Decimal("0.00"))
                                + bands["night"] * factor
                            )
                    else:
                        ordinary_total += ord_hrs

            # Overtime portion: first 2 OT hours of the week at 1.5x, then 2x;
            # Sunday/public-holiday OT at their flat rates.
            if ot_hrs > 0:
                overtime_total += ot_hrs
                if day_type == DayType.public_holiday:
                    overtime_amount += ot_hrs * base_rate * Decimal("2.5")
                elif day_type == DayType.sunday:
                    overtime_amount += ot_hrs * base_rate * Decimal("2.0")
                else:
                    first_tier = max(Decimal("0.00"), Decimal("2") - ot_used)
                    at_15 = min(ot_hrs, first_tier)
                    at_20 = ot_hrs - at_15
                    overtime_amount += at_15 * base_rate * Decimal("1.5") + at_20 * base_rate * Decimal("2.0")
                ot_used += ot_hrs

        # Calculate ordinary gross (band-split can yield long decimals; round hours).
        ordinary_total = ordinary_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        emp_payroll.ordinary_hours = ordinary_total
        emp_payroll.ordinary_gross = (
            ordinary_total * employee.hourly_base_rate
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        emp_payroll.overtime_hours = overtime_total
        emp_payroll.overtime_amount = overtime_amount.quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # Calculate penalty entries
        for penalty_type, hours in penalties_by_type.items():
            hours = hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            multiplier = self._get_penalty_multiplier_for_type(
                employee.employment_type, penalty_type
            )
            amount = (
                hours * employee.hourly_base_rate * multiplier
            ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            emp_payroll.penalty_entries.append(
                PenaltyEntry(
                    penalty_type=penalty_type,
                    hours=hours,
                    multiplier=multiplier,
                    amount=amount,
                )
            )
            emp_payroll.penalty_gross += amount

        # Add allowances (based on MA000009)
        # Meal allowance: $15.09 per week (if applicable)
        # Split shift loading: $3.26 per shift (if applicable)
        # Laundry: part of uniform provision
        emp_payroll.allowances.append(
            Allowance(allowance_type="meal", amount=Decimal("15.09"))
        )
        emp_payroll.allowances_total = sum(a.amount for a in emp_payroll.allowances)

        # Superannuation is on Ordinary Time Earnings — OVERTIME is excluded
        # (overtime is not OTE under SG law), so overtime_amount is NOT added.
        gross_for_super = (
            emp_payroll.ordinary_gross
            + emp_payroll.penalty_gross
            + emp_payroll.leave_gross
        )
        emp_payroll.super_amount = (
            gross_for_super * SUPER_RATE
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        return emp_payroll if emp_payroll.total_hours > 0 else None

    def _get_penalty_multiplier_for_type(
        self,
        employment_type: EmploymentType,
        penalty_type: PenaltyType,
    ) -> Decimal:
        """Get penalty multiplier for a specific type."""
        multipliers = {
            EmploymentType.full_time: {
                PenaltyType.ordinary: Decimal("1.0"),
                PenaltyType.saturday: Decimal("1.25"),
                PenaltyType.sunday: Decimal("1.5"),
                PenaltyType.public_holiday: Decimal("2.25"),  # MA000009 FT/PT PH = 225%
                PenaltyType.evening: Decimal("1.15"),
                PenaltyType.night: Decimal("1.175"),
                PenaltyType.overtime_1_5: Decimal("1.5"),
                PenaltyType.overtime_2_0: Decimal("2.0"),
            },
            EmploymentType.part_time: {
                PenaltyType.ordinary: Decimal("1.0"),
                PenaltyType.saturday: Decimal("1.25"),
                PenaltyType.sunday: Decimal("1.5"),
                PenaltyType.public_holiday: Decimal("2.25"),  # MA000009 FT/PT PH = 225%
                PenaltyType.evening: Decimal("1.15"),
                PenaltyType.night: Decimal("1.175"),
                PenaltyType.overtime_1_5: Decimal("1.5"),
                PenaltyType.overtime_2_0: Decimal("2.0"),
            },
            EmploymentType.casual: {
                PenaltyType.ordinary: Decimal("1.25"),  # includes 25% loading
                PenaltyType.saturday: Decimal("1.5"),
                PenaltyType.sunday: Decimal("1.75"),
                PenaltyType.public_holiday: Decimal("2.5"),
                PenaltyType.evening: Decimal("1.4"),  # 1.25 base + 0.15
                PenaltyType.night: Decimal("1.425"),  # 1.25 base + 0.175
                PenaltyType.overtime_1_5: Decimal("1.5"),
                PenaltyType.overtime_2_0: Decimal("2.0"),
            },
        }
        return multipliers.get(employment_type, {}).get(
            penalty_type, Decimal("1.0")
        )

    def _mask_tfn(self, employee_id: str) -> Optional[str]:
        """Mask TFN for security — return last 3 digits only."""
        # In real implementation, would look up actual TFN
        # For now, return masked version based on ID
        if len(employee_id) >= 3:
            return f"***{employee_id[-3:]}"
        return "***000"

    def approve_batch(self, batch: PayrollBatch) -> PayrollBatch:
        """Approve a batch for export."""
        if batch.status != PayrollStatus.draft:
            raise ValueError(f"Cannot approve batch with status {batch.status}")

        batch.status = PayrollStatus.approved
        batch.approved_at = datetime.utcnow()
        return batch

    def validate_batch(self, batch: PayrollBatch) -> List[str]:
        """
        Validate batch for export.

        Returns list of validation errors (empty if valid).
        """
        errors = []

        if not batch.employees:
            errors.append("Batch has no employees")

        # A single-day period (start == end) is a legitimate pay run
        if batch.period_start > batch.period_end:
            errors.append("Period start must be before period end")

        for emp in batch.employees:
            if emp.total_hours <= 0:
                errors.append(f"Employee {emp.name} has no hours")

            if emp.total_gross < 0:
                errors.append(f"Employee {emp.name} has negative gross pay")

        batch.validation_errors = errors
        return errors
