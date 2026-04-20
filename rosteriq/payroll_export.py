"""Payroll Export Engine for Australian Hospitality Awards.

Generates payroll-ready export data from timesheet/shift data with AU award
calculations. Outputs in formats compatible with Xero, MYOB, and KeyPay.

Handles:
- Hour categorization: ordinary, Saturday, Sunday, public holiday, evening, overtime
- Penalty rate calculations per AU hospitality awards
- Superannuation at 11.5% of ordinary time earnings (2025-26 guarantee)
- Allowances, deductions, and gross pay computation
- Export to Xero CSV, MYOB CSV, and KeyPay JSON
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, time, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.payroll_export")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class PeriodType(str, Enum):
    """Payroll period types."""
    WEEKLY = "weekly"
    FORTNIGHTLY = "fortnightly"
    MONTHLY = "monthly"


class PayrollStatus(str, Enum):
    """Status of a payroll record."""
    DRAFT = "draft"
    APPROVED = "approved"
    EXPORTED = "exported"


@dataclass
class Allowance:
    """An allowance (shift allowance, uniform, etc)."""
    name: str
    amount: float

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "amount": self.amount}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> Allowance:
        return Allowance(name=d["name"], amount=d["amount"])


@dataclass
class Deduction:
    """A deduction (tax, superannuation contribution, etc)."""
    name: str
    amount: float

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "amount": self.amount}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> Deduction:
        return Deduction(name=d["name"], amount=d["amount"])


@dataclass
class PayrollRecord:
    """Complete payroll record for an employee in a period."""
    id: str
    venue_id: str
    employee_id: str
    employee_name: str
    period_start: str  # ISO date
    period_end: str    # ISO date
    period_type: PeriodType
    ordinary_hours: float
    saturday_hours: float  # at 1.25x
    sunday_hours: float    # at 1.5x
    public_holiday_hours: float  # at 2.5x
    evening_hours: float  # at 1.15x (after 7pm)
    overtime_hours: float  # hours over 38/week
    overtime_rate: float = 1.5  # default multiplier
    base_rate: float = 0.0  # hourly rate in AUD
    gross_pay: float = 0.0  # calculated
    leave_hours: float = 0.0
    leave_type: Optional[str] = None
    allowances: List[Allowance] = field(default_factory=list)
    deductions: List[Deduction] = field(default_factory=list)
    super_amount: float = 0.0  # 11.5% of ordinary earnings
    notes: Optional[str] = None
    status: PayrollStatus = PayrollStatus.DRAFT
    exported_at: Optional[str] = None  # ISO datetime
    exported_format: Optional[str] = None  # xero, myob, keypay

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "period_type": self.period_type.value,
            "ordinary_hours": self.ordinary_hours,
            "saturday_hours": self.saturday_hours,
            "sunday_hours": self.sunday_hours,
            "public_holiday_hours": self.public_holiday_hours,
            "evening_hours": self.evening_hours,
            "overtime_hours": self.overtime_hours,
            "overtime_rate": self.overtime_rate,
            "base_rate": self.base_rate,
            "gross_pay": round(self.gross_pay, 2),
            "leave_hours": self.leave_hours,
            "leave_type": self.leave_type,
            "allowances": [a.to_dict() for a in self.allowances],
            "deductions": [d.to_dict() for d in self.deductions],
            "super_amount": round(self.super_amount, 2),
            "notes": self.notes,
            "status": self.status.value,
            "exported_at": self.exported_at,
            "exported_format": self.exported_format,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> PayrollRecord:
        """Reconstruct PayrollRecord from dict."""
        return PayrollRecord(
            id=d["id"],
            venue_id=d["venue_id"],
            employee_id=d["employee_id"],
            employee_name=d["employee_name"],
            period_start=d["period_start"],
            period_end=d["period_end"],
            period_type=PeriodType(d["period_type"]),
            ordinary_hours=d["ordinary_hours"],
            saturday_hours=d["saturday_hours"],
            sunday_hours=d["sunday_hours"],
            public_holiday_hours=d["public_holiday_hours"],
            evening_hours=d["evening_hours"],
            overtime_hours=d["overtime_hours"],
            overtime_rate=d.get("overtime_rate", 1.5),
            base_rate=d.get("base_rate", 0.0),
            gross_pay=d.get("gross_pay", 0.0),
            leave_hours=d.get("leave_hours", 0.0),
            leave_type=d.get("leave_type"),
            allowances=[Allowance.from_dict(a) for a in d.get("allowances", [])],
            deductions=[Deduction.from_dict(d) for d in d.get("deductions", [])],
            super_amount=d.get("super_amount", 0.0),
            notes=d.get("notes"),
            status=PayrollStatus(d.get("status", "draft")),
            exported_at=d.get("exported_at"),
            exported_format=d.get("exported_format"),
        )


# ---------------------------------------------------------------------------
# Core payroll logic
# ---------------------------------------------------------------------------


def _calculate_hours_breakdown(shifts_for_employee: List[Dict[str, Any]]) -> Dict[str, float]:
    """Categorize hours from shift data.

    Args:
        shifts_for_employee: List of shift dicts with:
            - date (str, ISO date)
            - start_time (str, HH:MM format)
            - end_time (str, HH:MM format)
            - is_public_holiday (bool)

    Returns:
        Dict with keys: ordinary_hours, saturday_hours, sunday_hours,
        public_holiday_hours, evening_hours, overtime_hours
    """
    ordinary = 0.0
    saturday = 0.0
    sunday = 0.0
    public_holiday = 0.0
    evening = 0.0
    overtime = 0.0

    # Track hours per week for overtime calculation
    hours_by_week: Dict[str, float] = {}

    for shift in shifts_for_employee:
        shift_date_str = shift.get("date")
        start_time_str = shift.get("start_time")
        end_time_str = shift.get("end_time")
        is_public_holiday = shift.get("is_public_holiday", False)

        try:
            shift_date = datetime.fromisoformat(shift_date_str).date()
            start_dt = datetime.fromisoformat(f"{shift_date_str}T{start_time_str}:00")
            end_dt = datetime.fromisoformat(f"{shift_date_str}T{end_time_str}:00")

            # Handle overnight shifts
            if end_dt < start_dt:
                end_dt += timedelta(days=1)

            # Skip zero-duration shifts
            shift_hours = (end_dt - start_dt).total_seconds() / 3600.0
            if shift_hours <= 0:
                continue

            # Get week number for overtime calc
            week_key = shift_date.isocalendar()[1]  # ISO week number

            # Categorize hours
            if is_public_holiday:
                # PH hours at 2.5x multiplier
                public_holiday += shift_hours
            else:
                # Determine day of week (0=Monday, 6=Sunday)
                day_of_week = shift_date.weekday()

                if day_of_week == 5:  # Saturday
                    saturday += shift_hours
                elif day_of_week == 6:  # Sunday
                    sunday += shift_hours
                else:
                    # Weekday: ordinary + evening component
                    ordinary += shift_hours

                    # Evening hours (after 7pm): 1.15x multiplier
                    # For overnight shifts, only count evening hours from the start date
                    evening_start = datetime.fromisoformat(f"{shift_date_str}T19:00:00")
                    if start_dt < evening_start < end_dt:
                        # Shift spans 7pm on the same day
                        evening += (end_dt - evening_start).total_seconds() / 3600.0
                    elif start_dt >= evening_start:
                        # Entire shift is after 7pm on the start date
                        # But for overnight, only count until midnight
                        midnight = datetime.fromisoformat(f"{shift_date_str}T23:59:59.999999")
                        hours_until_midnight = min(
                            (midnight - start_dt).total_seconds() / 3600.0,
                            shift_hours
                        )
                        evening += hours_until_midnight

            # Track for weekly overtime
            if week_key not in hours_by_week:
                hours_by_week[week_key] = 0
            hours_by_week[week_key] += shift_hours

        except (ValueError, KeyError, TypeError):
            logger.warning("Invalid shift data: %s", shift)
            continue

    # Calculate overtime (hours over 38 per week)
    for week_hours in hours_by_week.values():
        if week_hours > 38:
            overtime += week_hours - 38

    return {
        "ordinary_hours": ordinary,
        "saturday_hours": saturday,
        "sunday_hours": sunday,
        "public_holiday_hours": public_holiday,
        "evening_hours": evening,
        "overtime_hours": overtime,
    }


def _calculate_gross_pay(
    ordinary_hours: float,
    saturday_hours: float,
    sunday_hours: float,
    public_holiday_hours: float,
    evening_hours: float,
    overtime_hours: float,
    base_rate: float,
    overtime_rate: float = 1.5,
) -> float:
    """Calculate gross pay with AU award penalty rates.

    Args:
        ordinary_hours: Regular weekday hours
        saturday_hours: Saturday hours (1.25x multiplier)
        sunday_hours: Sunday hours (1.5x multiplier)
        public_holiday_hours: PH hours (2.5x multiplier)
        evening_hours: After 7pm hours (1.15x multiplier)
        overtime_hours: Hours over 38/week (overtime_rate multiplier)
        base_rate: Hourly rate in AUD
        overtime_rate: Multiplier for overtime (default 1.5)

    Returns:
        Gross pay in AUD
    """
    gross = 0.0

    # Ordinary rate (no multiplier)
    gross += ordinary_hours * base_rate

    # Saturday at 1.25x
    gross += saturday_hours * base_rate * 1.25

    # Sunday at 1.5x
    gross += sunday_hours * base_rate * 1.5

    # Public holiday at 2.5x
    gross += public_holiday_hours * base_rate * 2.5

    # Evening (after 7pm) at 1.15x
    gross += evening_hours * base_rate * 1.15

    # Overtime at specified rate (usually 1.5x)
    gross += overtime_hours * base_rate * overtime_rate

    return gross


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------


def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


_PAYROLL_SCHEMA = """
CREATE TABLE IF NOT EXISTS payroll_records (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    employee_name TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    period_type TEXT NOT NULL,
    ordinary_hours REAL NOT NULL,
    saturday_hours REAL NOT NULL,
    sunday_hours REAL NOT NULL,
    public_holiday_hours REAL NOT NULL,
    evening_hours REAL NOT NULL,
    overtime_hours REAL NOT NULL,
    overtime_rate REAL NOT NULL,
    base_rate REAL NOT NULL,
    gross_pay REAL NOT NULL,
    leave_hours REAL NOT NULL,
    leave_type TEXT,
    allowances TEXT,
    deductions TEXT,
    super_amount REAL NOT NULL,
    notes TEXT,
    status TEXT NOT NULL,
    exported_at TEXT,
    exported_format TEXT
);
CREATE INDEX IF NOT EXISTS ix_payroll_venue ON payroll_records(venue_id);
CREATE INDEX IF NOT EXISTS ix_payroll_employee ON payroll_records(employee_id);
CREATE INDEX IF NOT EXISTS ix_payroll_period ON payroll_records(period_start, period_end);
CREATE INDEX IF NOT EXISTS ix_payroll_status ON payroll_records(status);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("payroll_records", _PAYROLL_SCHEMA)

            def _rehydrate_on_init():
                store = get_payroll_export_store()
                store._rehydrate()

            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Payroll Export Store
# ---------------------------------------------------------------------------


class PayrollExportStore:
    """Thread-safe in-memory store for payroll records with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._records: Dict[str, PayrollRecord] = {}
        self._lock = threading.Lock()

    def _persist(self, record: PayrollRecord) -> None:
        """Persist a payroll record to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        row = {
            "id": record.id,
            "venue_id": record.venue_id,
            "employee_id": record.employee_id,
            "employee_name": record.employee_name,
            "period_start": record.period_start,
            "period_end": record.period_end,
            "period_type": record.period_type.value,
            "ordinary_hours": record.ordinary_hours,
            "saturday_hours": record.saturday_hours,
            "sunday_hours": record.sunday_hours,
            "public_holiday_hours": record.public_holiday_hours,
            "evening_hours": record.evening_hours,
            "overtime_hours": record.overtime_hours,
            "overtime_rate": record.overtime_rate,
            "base_rate": record.base_rate,
            "gross_pay": record.gross_pay,
            "leave_hours": record.leave_hours,
            "leave_type": record.leave_type,
            "allowances": json.dumps([a.to_dict() for a in record.allowances]),
            "deductions": json.dumps([d.to_dict() for d in record.deductions]),
            "super_amount": record.super_amount,
            "notes": record.notes,
            "status": record.status.value,
            "exported_at": record.exported_at,
            "exported_format": record.exported_format,
        }
        try:
            _p.upsert("payroll_records", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist payroll record %s: %s", record.id, e)

    def _rehydrate(self) -> None:
        """Load all payroll records from SQLite. Called on startup."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            rows = _p.fetchall("SELECT * FROM payroll_records")
            for row in rows:
                record = self._row_to_record(dict(row))
                self._records[record.id] = record
            logger.info("Rehydrated %d payroll records from persistence", len(self._records))
        except Exception as e:
            logger.warning("Failed to rehydrate payroll records: %s", e)

    @staticmethod
    def _row_to_record(row: Dict[str, Any]) -> PayrollRecord:
        """Reconstruct a PayrollRecord from a DB row."""
        allowances = []
        if row.get("allowances"):
            try:
                allowances = [Allowance.from_dict(a) for a in json.loads(row["allowances"])]
            except (json.JSONDecodeError, TypeError):
                pass

        deductions = []
        if row.get("deductions"):
            try:
                deductions = [Deduction.from_dict(d) for d in json.loads(row["deductions"])]
            except (json.JSONDecodeError, TypeError):
                pass

        return PayrollRecord(
            id=row["id"],
            venue_id=row["venue_id"],
            employee_id=row["employee_id"],
            employee_name=row["employee_name"],
            period_start=row["period_start"],
            period_end=row["period_end"],
            period_type=PeriodType(row.get("period_type", "weekly")),
            ordinary_hours=row.get("ordinary_hours", 0.0),
            saturday_hours=row.get("saturday_hours", 0.0),
            sunday_hours=row.get("sunday_hours", 0.0),
            public_holiday_hours=row.get("public_holiday_hours", 0.0),
            evening_hours=row.get("evening_hours", 0.0),
            overtime_hours=row.get("overtime_hours", 0.0),
            overtime_rate=row.get("overtime_rate", 1.5),
            base_rate=row.get("base_rate", 0.0),
            gross_pay=row.get("gross_pay", 0.0),
            leave_hours=row.get("leave_hours", 0.0),
            leave_type=row.get("leave_type"),
            allowances=allowances,
            deductions=deductions,
            super_amount=row.get("super_amount", 0.0),
            notes=row.get("notes"),
            status=PayrollStatus(row.get("status", "draft")),
            exported_at=row.get("exported_at"),
            exported_format=row.get("exported_format"),
        )

    def generate_payroll(
        self,
        venue_id: str,
        period_start: str,
        period_end: str,
        period_type: str,
        shifts_data: List[Dict[str, Any]],
    ) -> List[PayrollRecord]:
        """Generate payroll records from shift data.

        Args:
            venue_id: Venue identifier
            period_start: ISO date string
            period_end: ISO date string
            period_type: "weekly", "fortnightly", or "monthly"
            shifts_data: List of shift dicts with employee_id, employee_name, date,
                        start_time, end_time, base_rate, is_public_holiday

        Returns:
            List of generated PayrollRecord objects
        """
        # Group shifts by employee
        shifts_by_employee: Dict[str, List[Dict[str, Any]]] = {}
        for shift in shifts_data:
            emp_id = shift.get("employee_id")
            if emp_id:
                if emp_id not in shifts_by_employee:
                    shifts_by_employee[emp_id] = []
                shifts_by_employee[emp_id].append(shift)

        records = []
        with self._lock:
            for emp_id, employee_shifts in shifts_by_employee.items():
                # Get employee name from first shift
                emp_name = employee_shifts[0].get("employee_name", "Unknown")
                base_rate = employee_shifts[0].get("base_rate", 0.0)

                # Calculate hour breakdown
                hours_breakdown = _calculate_hours_breakdown(employee_shifts)

                # Calculate gross pay
                gross_pay = _calculate_gross_pay(
                    ordinary_hours=hours_breakdown["ordinary_hours"],
                    saturday_hours=hours_breakdown["saturday_hours"],
                    sunday_hours=hours_breakdown["sunday_hours"],
                    public_holiday_hours=hours_breakdown["public_holiday_hours"],
                    evening_hours=hours_breakdown["evening_hours"],
                    overtime_hours=hours_breakdown["overtime_hours"],
                    base_rate=base_rate,
                )

                # Calculate superannuation (11.5% of ordinary time earnings only)
                ordinary_earnings = hours_breakdown["ordinary_hours"] * base_rate
                super_amount = ordinary_earnings * 0.115

                # Create record
                record = PayrollRecord(
                    id=f"payroll_{uuid.uuid4().hex[:12]}",
                    venue_id=venue_id,
                    employee_id=emp_id,
                    employee_name=emp_name,
                    period_start=period_start,
                    period_end=period_end,
                    period_type=PeriodType(period_type.lower()),
                    ordinary_hours=hours_breakdown["ordinary_hours"],
                    saturday_hours=hours_breakdown["saturday_hours"],
                    sunday_hours=hours_breakdown["sunday_hours"],
                    public_holiday_hours=hours_breakdown["public_holiday_hours"],
                    evening_hours=hours_breakdown["evening_hours"],
                    overtime_hours=hours_breakdown["overtime_hours"],
                    base_rate=base_rate,
                    gross_pay=gross_pay,
                    super_amount=super_amount,
                )

                self._records[record.id] = record
                records.append(record)

        # Persist all records
        for record in records:
            self._persist(record)

        return records

    def get_payroll_records(
        self,
        venue_id: str,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[PayrollRecord]:
        """Get payroll records with optional filters."""
        with self._lock:
            records = [r for r in self._records.values() if r.venue_id == venue_id]

            if period_start:
                records = [r for r in records if r.period_start >= period_start]
            if period_end:
                records = [r for r in records if r.period_end <= period_end]
            if status:
                records = [r for r in records if r.status.value == status]

            records.sort(key=lambda r: (r.employee_name, r.period_start))
            return records

    def get_employee_payroll(
        self,
        venue_id: str,
        employee_id: str,
        period_start: Optional[str] = None,
    ) -> List[PayrollRecord]:
        """Get payroll history for an employee."""
        with self._lock:
            records = [
                r for r in self._records.values()
                if r.venue_id == venue_id and r.employee_id == employee_id
            ]

            if period_start:
                records = [r for r in records if r.period_start >= period_start]

            records.sort(key=lambda r: r.period_start)
            return records

    def approve_payroll(self, record_id: str) -> PayrollRecord:
        """Approve a payroll record (DRAFT -> APPROVED)."""
        with self._lock:
            record = self._records.get(record_id)
            if not record:
                raise ValueError(f"Payroll record {record_id} not found")

            record.status = PayrollStatus.APPROVED

        self._persist(record)
        return record

    def add_allowance(self, record_id: str, name: str, amount: float) -> PayrollRecord:
        """Add an allowance to a payroll record."""
        with self._lock:
            record = self._records.get(record_id)
            if not record:
                raise ValueError(f"Payroll record {record_id} not found")

            record.allowances.append(Allowance(name=name, amount=amount))

        self._persist(record)
        return record

    def add_deduction(self, record_id: str, name: str, amount: float) -> PayrollRecord:
        """Add a deduction to a payroll record."""
        with self._lock:
            record = self._records.get(record_id)
            if not record:
                raise ValueError(f"Payroll record {record_id} not found")

            record.deductions.append(Deduction(name=name, amount=amount))

        self._persist(record)
        return record

    def get_payroll_summary(
        self,
        venue_id: str,
        period_start: str,
        period_end: str,
    ) -> Dict[str, Any]:
        """Get payroll summary totals for a period."""
        records = self.get_payroll_records(
            venue_id,
            period_start=period_start,
            period_end=period_end,
        )

        total_gross = sum(r.gross_pay for r in records)
        total_super = sum(r.super_amount for r in records)
        total_allowances = sum(
            sum(a.amount for a in r.allowances) for r in records
        )
        total_deductions = sum(
            sum(d.amount for d in r.deductions) for r in records
        )
        total_employees = len(set(r.employee_id for r in records))
        total_hours = sum(r.ordinary_hours + r.saturday_hours + r.sunday_hours +
                         r.public_holiday_hours + r.evening_hours + r.overtime_hours
                         for r in records)

        return {
            "venue_id": venue_id,
            "period_start": period_start,
            "period_end": period_end,
            "employee_count": total_employees,
            "total_hours": round(total_hours, 2),
            "total_gross_pay": round(total_gross, 2),
            "total_superannuation": round(total_super, 2),
            "total_allowances": round(total_allowances, 2),
            "total_deductions": round(total_deductions, 2),
            "net_pay": round(total_gross - total_deductions, 2),
        }

    def export_xero_csv(
        self,
        venue_id: str,
        period_start: str,
        period_end: str,
    ) -> str:
        """Export payroll records as Xero-compatible CSV."""
        records = self.get_payroll_records(
            venue_id,
            period_start=period_start,
            period_end=period_end,
        )

        output = io.StringIO()
        writer = csv.writer(output)

        # Xero header
        writer.writerow([
            "EmployeeID",
            "EmployeeName",
            "OrdinaryHours",
            "OrdinaryEarnings",
            "SaturdayHours",
            "SaturdayEarnings",
            "SundayHours",
            "SundayEarnings",
            "PublicHolidayHours",
            "PublicHolidayEarnings",
            "EveningHours",
            "EveningEarnings",
            "OvertimeHours",
            "OvertimeEarnings",
            "GrossPay",
            "Super",
            "NetAllowances",
            "NetDeductions",
        ])

        for record in records:
            ordinary_earnings = record.ordinary_hours * record.base_rate
            saturday_earnings = record.saturday_hours * record.base_rate * 1.25
            sunday_earnings = record.sunday_hours * record.base_rate * 1.5
            ph_earnings = record.public_holiday_hours * record.base_rate * 2.5
            evening_earnings = record.evening_hours * record.base_rate * 1.15
            overtime_earnings = record.overtime_hours * record.base_rate * record.overtime_rate

            net_allowances = sum(a.amount for a in record.allowances)
            net_deductions = sum(d.amount for d in record.deductions)

            writer.writerow([
                record.employee_id,
                record.employee_name,
                round(record.ordinary_hours, 2),
                round(ordinary_earnings, 2),
                round(record.saturday_hours, 2),
                round(saturday_earnings, 2),
                round(record.sunday_hours, 2),
                round(sunday_earnings, 2),
                round(record.public_holiday_hours, 2),
                round(ph_earnings, 2),
                round(record.evening_hours, 2),
                round(evening_earnings, 2),
                round(record.overtime_hours, 2),
                round(overtime_earnings, 2),
                round(record.gross_pay, 2),
                round(record.super_amount, 2),
                round(net_allowances, 2),
                round(net_deductions, 2),
            ])

        return output.getvalue()

    def export_myob_csv(
        self,
        venue_id: str,
        period_start: str,
        period_end: str,
    ) -> str:
        """Export payroll records as MYOB-compatible CSV."""
        records = self.get_payroll_records(
            venue_id,
            period_start=period_start,
            period_end=period_end,
        )

        output = io.StringIO()
        writer = csv.writer(output)

        # MYOB header
        writer.writerow([
            "Co./Last Name",
            "First Name",
            "Pay Period Start",
            "Pay Period End",
            "Hours Worked",
            "Hourly Rate",
            "Total Pay",
            "Superannuation",
        ])

        for record in records:
            # Parse employee name
            name_parts = record.employee_name.split(" ", 1)
            last_name = name_parts[0]
            first_name = name_parts[1] if len(name_parts) > 1 else ""

            total_hours = (record.ordinary_hours + record.saturday_hours +
                          record.sunday_hours + record.public_holiday_hours +
                          record.evening_hours + record.overtime_hours)

            writer.writerow([
                last_name,
                first_name,
                record.period_start,
                record.period_end,
                round(total_hours, 2),
                round(record.base_rate, 2),
                round(record.gross_pay, 2),
                round(record.super_amount, 2),
            ])

        return output.getvalue()

    def export_keypay_json(
        self,
        venue_id: str,
        period_start: str,
        period_end: str,
    ) -> Dict[str, Any]:
        """Export payroll records as KeyPay-compatible JSON."""
        records = self.get_payroll_records(
            venue_id,
            period_start=period_start,
            period_end=period_end,
        )

        employees = []
        for record in records:
            earnings = [
                {"type": "ordinary", "hours": record.ordinary_hours, "rate": record.base_rate},
                {"type": "saturday", "hours": record.saturday_hours, "rate": record.base_rate * 1.25},
                {"type": "sunday", "hours": record.sunday_hours, "rate": record.base_rate * 1.5},
                {"type": "public_holiday", "hours": record.public_holiday_hours, "rate": record.base_rate * 2.5},
                {"type": "evening", "hours": record.evening_hours, "rate": record.base_rate * 1.15},
                {"type": "overtime", "hours": record.overtime_hours, "rate": record.base_rate * record.overtime_rate},
            ]

            deductions = [
                {"name": d.name, "amount": d.amount}
                for d in record.deductions
            ]

            employees.append({
                "employeeId": record.employee_id,
                "employeeName": record.employee_name,
                "earnings": earnings,
                "deductions": deductions,
                "super": round(record.super_amount, 2),
                "gross": round(record.gross_pay, 2),
            })

        return {
            "payRun": {
                "periodStart": period_start,
                "periodEnd": period_end,
                "venueId": venue_id,
                "employees": employees,
            }
        }


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_store: Optional[PayrollExportStore] = None
_store_lock = threading.Lock()


def get_payroll_export_store() -> PayrollExportStore:
    """Get the module-level payroll export store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = PayrollExportStore()
    return _store


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _store
    with _store_lock:
        _store = PayrollExportStore.__new__(PayrollExportStore)
        _store._lock = threading.Lock()
        _store._records = {}
