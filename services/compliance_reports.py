"""
Fair Work compliance reporting service for RosterIQ.

Generates comprehensive compliance reports covering:
- Hours compliance (weekly limits, consecutive days, minimum rest)
- Break compliance (duration-based break requirements)
- Penalty rate audit (comparing expected vs actual rates paid)
- Certification status (RSA, food safety, first aid)
- Overall compliance scoring (weighted across all areas)

All reports use Fair Work Australia MA000009 (Hospitality Industry General Award).
"""

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Dict, Optional
from enum import Enum

from rosteriq.models import (
    Employee,
    Shift,
    ShiftStatus,
    EmploymentType,
    State,
)
from rosteriq.award_rules import (
    get_day_type,
    get_minimum_break_minutes,
    get_penalty_multiplier,
    MINIMUM_HOURS_BETWEEN_SHIFTS,
    MAX_CONSECUTIVE_DAYS,
    MAX_SHIFT_LENGTH_HOURS,
    PENALTY_MULTIPLIERS,
)
from rosteriq.cost_calculator import calculate_shift_cost_breakdown


# ============================================================================
# ENUMS AND DATA CLASSES
# ============================================================================


class ViolationSeverity(str, Enum):
    """Severity level for compliance violations."""
    info = "info"
    warning = "warning"
    critical = "critical"


@dataclass
class ComplianceViolation:
    """Single compliance violation found during audit."""
    employee_id: str
    employee_name: str
    violation_type: str
    description: str
    severity: ViolationSeverity
    shift_id: Optional[str] = None
    date: Optional[date] = None


@dataclass
class ComplianceSection:
    """Section of a compliance report with findings and summary."""
    title: str
    description: str
    compliance_percentage: float
    findings: List[str] = field(default_factory=list)
    violations: List[ComplianceViolation] = field(default_factory=list)


@dataclass
class ComplianceReport:
    """Complete compliance report for a venue."""
    venue_id: str
    period_start: date
    period_end: date
    generated_at: datetime
    overall_score: float
    score_rating: str  # "green", "amber", "red"
    sections: List[ComplianceSection] = field(default_factory=list)
    violations: List[ComplianceViolation] = field(default_factory=list)
    summary: str = ""


# ============================================================================
# COMPLIANCE REPORT SERVICE
# ============================================================================


class ComplianceReportService:
    """Generate Fair Work compliance reports."""

    def __init__(self, state: State):
        """
        Initialize the compliance report service.

        Args:
            state: Australian state for public holiday determination
        """
        self.state = state

    def generate_compliance_report(
        self,
        venue_id: str,
        employees: List[Employee],
        shifts: List[Shift],
        period_days: int = 30,
    ) -> ComplianceReport:
        """
        Generate a comprehensive compliance report.

        Args:
            venue_id: The venue identifier
            employees: List of employees to audit
            shifts: List of shifts to audit
            period_days: Number of days to look back (default 30)

        Returns:
            ComplianceReport with all sections and violations
        """
        # Calculate period
        end_date = date.today()
        start_date = end_date - timedelta(days=period_days)

        # Filter shifts to period
        period_shifts = [
            s for s in shifts
            if start_date <= s.date <= end_date and s.status != ShiftStatus.cancelled
        ]

        # Generate sub-reports
        hours_section = self._hours_compliance_section(
            employees, period_shifts, start_date, end_date
        )
        breaks_section = self._breaks_compliance_section(
            employees, period_shifts
        )
        penalties_section = self._penalty_audit_section(
            employees, period_shifts
        )
        certs_section = self._certification_status_section(employees)

        sections = [
            hours_section,
            breaks_section,
            penalties_section,
            certs_section,
        ]

        # Collect all violations
        all_violations = []
        for section in sections:
            all_violations.extend(section.violations)

        # Calculate overall score (weighted average)
        weights = {
            "hours": 0.35,
            "breaks": 0.25,
            "penalties": 0.25,
            "certifications": 0.15,
        }

        overall_score = (
            hours_section.compliance_percentage * weights["hours"]
            + breaks_section.compliance_percentage * weights["breaks"]
            + penalties_section.compliance_percentage * weights["penalties"]
            + certs_section.compliance_percentage * weights["certifications"]
        )

        # Determine rating
        if overall_score >= 90:
            rating = "green"
        elif overall_score >= 70:
            rating = "amber"
        else:
            rating = "red"

        summary = self._generate_summary(
            overall_score, rating, all_violations, sections
        )

        return ComplianceReport(
            venue_id=venue_id,
            period_start=start_date,
            period_end=end_date,
            generated_at=datetime.now(),
            overall_score=overall_score,
            score_rating=rating,
            sections=sections,
            violations=all_violations,
            summary=summary,
        )

    def _hours_compliance_section(
        self,
        employees: List[Employee],
        shifts: List[Shift],
        start_date: date,
        end_date: date,
    ) -> ComplianceSection:
        """
        Audit hours compliance (38h/week, 12h max shift, 10h rest between).
        """
        violations = []
        findings = []
        compliant_count = 0
        total_employees = 0

        for employee in employees:
            emp_shifts = [
                s for s in shifts
                if s.employee_id == employee.id
            ]

            if not emp_shifts:
                continue

            total_employees += 1
            emp_violations = []

            # Weekly analysis
            current_date = start_date
            while current_date <= end_date:
                week_start = current_date
                week_end = week_start + timedelta(days=6)

                week_shifts = [
                    s for s in emp_shifts
                    if week_start <= s.date <= week_end
                ]

                if week_shifts:
                    weekly_hours = sum(s.net_hours for s in week_shifts)

                    # Check 38-hour limit for non-casual
                    if (
                        employee.employment_type != EmploymentType.casual
                        and weekly_hours > 38.0
                    ):
                        if not any(
                            "overtime" in str(s) for s in emp_violations
                        ):
                            emp_violations.append(
                                f"Week {week_start}: {weekly_hours:.1f}h "
                                f"exceeds 38h limit without overtime agreement"
                            )

                current_date += timedelta(days=7)

            # Single shift analysis
            for shift in emp_shifts:
                # 12-hour shift check
                if shift.duration_hours > 12:
                    emp_violations.append(
                        f"{shift.date}: Shift {shift.duration_hours:.1f}h "
                        f"exceeds 12h maximum"
                    )

            # Rest between shifts
            sorted_shifts = sorted(emp_shifts, key=lambda s: (s.date, s.start_time))
            for i in range(1, len(sorted_shifts)):
                prev_shift = sorted_shifts[i - 1]
                curr_shift = sorted_shifts[i]

                if curr_shift.date == prev_shift.date:
                    continue

                # Calculate rest hours
                prev_end_time = prev_shift.end_time
                curr_start_time = curr_shift.start_time
                days_between = (curr_shift.date - prev_shift.date).days

                if days_between == 1:
                    rest_hours = (
                        24 - prev_end_time.hour
                        + (curr_start_time.hour
                           + curr_start_time.minute / 60)
                    )
                    if rest_hours < MINIMUM_HOURS_BETWEEN_SHIFTS:
                        emp_violations.append(
                            f"{curr_shift.date}: Only {rest_hours:.1f}h rest "
                            f"(minimum {MINIMUM_HOURS_BETWEEN_SHIFTS}h required)"
                        )

            # Consecutive days check
            consecutive_map = self._find_consecutive_sequences(emp_shifts)
            for seq_len, dates in consecutive_map.items():
                if seq_len > MAX_CONSECUTIVE_DAYS:
                    emp_violations.append(
                        f"{seq_len} consecutive days worked "
                        f"(maximum {MAX_CONSECUTIVE_DAYS})"
                    )

            if not emp_violations:
                compliant_count += 1

            # Create violations
            for v in emp_violations:
                violations.append(
                    ComplianceViolation(
                        employee_id=employee.id,
                        employee_name=employee.name,
                        violation_type="hours",
                        description=v,
                        severity=ViolationSeverity.critical,
                    )
                )
                findings.append(f"{employee.name}: {v}")

        compliance_pct = (
            (compliant_count / total_employees * 100)
            if total_employees > 0
            else 100
        )

        if not findings:
            findings.append(
                "All employees compliant with weekly hours limits "
                "and rest requirements"
            )

        return ComplianceSection(
            title="Hours Compliance",
            description=(
                "Ensures compliance with 38h/week, 12h max shift, "
                "10h minimum rest between shifts"
            ),
            compliance_percentage=compliance_pct,
            findings=findings,
            violations=violations,
        )

    def _breaks_compliance_section(
        self,
        employees: List[Employee],
        shifts: List[Shift],
    ) -> ComplianceSection:
        """
        Audit break compliance per MA000009.
        """
        violations = []
        findings = []
        compliant_shifts = 0
        total_shifts = 0

        for shift in shifts:
            if shift.status == ShiftStatus.cancelled:
                continue

            total_shifts += 1
            required_break = get_minimum_break_minutes(shift.duration_hours)

            if shift.break_minutes < required_break:
                emp = next(
                    (e for e in employees if e.id == shift.employee_id),
                    None
                )
                if emp:
                    violations.append(
                        ComplianceViolation(
                            employee_id=emp.id,
                            employee_name=emp.name,
                            violation_type="breaks",
                            description=(
                                f"{shift.date}: {shift.duration_hours:.1f}h shift "
                                f"has {shift.break_minutes}m break "
                                f"(required {required_break}m)"
                            ),
                            severity=ViolationSeverity.warning,
                            shift_id=shift.id,
                            date=shift.date,
                        )
                    )
                    findings.append(
                        f"{emp.name} {shift.date}: "
                        f"Insufficient break ({shift.break_minutes}m < {required_break}m)"
                    )
            else:
                compliant_shifts += 1

        compliance_pct = (
            (compliant_shifts / total_shifts * 100)
            if total_shifts > 0
            else 100
        )

        if not findings:
            findings.append("All shifts have required break periods")

        return ComplianceSection(
            title="Break Compliance",
            description=(
                "Ensures required breaks per shift duration: "
                "30m for 5h+, 50m for 7h+, 70m for 10h+"
            ),
            compliance_percentage=compliance_pct,
            findings=findings,
            violations=violations,
        )

    def _penalty_audit_section(
        self,
        employees: List[Employee],
        shifts: List[Shift],
    ) -> ComplianceSection:
        """
        Audit penalty rates applied vs expected rates.
        """
        violations = []
        findings = []

        penalty_totals: Dict[str, Decimal] = {
            "saturday": Decimal("0"),
            "sunday": Decimal("0"),
            "public_holiday": Decimal("0"),
            "evening": Decimal("0"),
            "overnight": Decimal("0"),
        }

        for shift in shifts:
            if shift.status == ShiftStatus.cancelled:
                continue

            emp = next(
                (e for e in employees if e.id == shift.employee_id),
                None
            )
            if not emp:
                continue

            day_type = get_day_type(shift.date, self.state)
            expected_multiplier = PENALTY_MULTIPLIERS[emp.employment_type][day_type]

            # Calculate actual cost breakdown
            breakdown = calculate_shift_cost_breakdown(emp, shift, self.state)

            # Track penalty by type
            if day_type.value == "saturday":
                penalty_totals["saturday"] += breakdown.penalty_cost
            elif day_type.value == "sunday":
                penalty_totals["sunday"] += breakdown.penalty_cost
            elif day_type.value == "public_holiday":
                penalty_totals["public_holiday"] += breakdown.penalty_cost

            # Check for evening/overnight
            if 19 <= shift.start_time.hour < 24:
                penalty_totals["evening"] += breakdown.penalty_cost
            elif 0 <= shift.start_time.hour < 7:
                penalty_totals["overnight"] += breakdown.penalty_cost

            # Flag if actual multiplier differs from expected
            actual_multiplier = Decimal(
                str(
                    (
                        breakdown.base_cost + breakdown.penalty_cost
                    ) / breakdown.base_cost
                    if breakdown.base_cost > 0
                    else 1
                )
            )

            if abs(actual_multiplier - expected_multiplier) > Decimal("0.01"):
                violations.append(
                    ComplianceViolation(
                        employee_id=emp.id,
                        employee_name=emp.name,
                        violation_type="penalty_rate",
                        description=(
                            f"{shift.date}: Expected {float(expected_multiplier):.2f}x, "
                            f"got {float(actual_multiplier):.2f}x"
                        ),
                        severity=ViolationSeverity.warning,
                        shift_id=shift.id,
                        date=shift.date,
                    )
                )

        # Build findings
        findings.append(
            f"Saturday loading: ${penalty_totals['saturday']:.2f}"
        )
        findings.append(
            f"Sunday loading: ${penalty_totals['sunday']:.2f}"
        )
        findings.append(
            f"Public holiday loading: ${penalty_totals['public_holiday']:.2f}"
        )
        findings.append(
            f"Evening loading: ${penalty_totals['evening']:.2f}"
        )
        findings.append(
            f"Overnight loading: ${penalty_totals['overnight']:.2f}"
        )

        total_penalty = sum(penalty_totals.values())
        findings.append(
            f"Total penalty loading paid: ${total_penalty:.2f}"
        )

        compliance_pct = (
            100.0 - (len(violations) / max(len(shifts), 1) * 100)
            if violations
            else 100.0
        )

        return ComplianceSection(
            title="Penalty Rate Audit",
            description="Verifies applied penalty rates match Fair Work requirements",
            compliance_percentage=min(compliance_pct, 100.0),
            findings=findings,
            violations=violations,
        )

    def _certification_status_section(
        self,
        employees: List[Employee],
    ) -> ComplianceSection:
        """
        Audit employee certification status (RSA, food safety, first aid).
        """
        violations = []
        findings = []
        compliant_count = 0

        # For now, using placeholder since certifications aren't in Employee model
        # In production, fetch from external system or DB
        for employee in employees:
            # Placeholder: assume no certification data available
            findings.append(
                f"{employee.name}: Certification status not tracked "
                "(integration required with external system)"
            )

        compliance_pct = 0.0  # Cannot verify without data

        return ComplianceSection(
            title="Certification Status",
            description=(
                "Tracks RSA, food safety, and first aid certifications. "
                "Requires integration with external systems."
            ),
            compliance_percentage=compliance_pct,
            findings=findings,
            violations=violations,
        )

    def _find_consecutive_sequences(
        self,
        shifts: List[Shift],
    ) -> Dict[int, List[date]]:
        """
        Find sequences of consecutive work days.

        Returns dict mapping sequence length to list of start dates.
        """
        if not shifts:
            return {}

        sorted_shifts = sorted(shifts, key=lambda s: s.date)
        sequences = {}

        i = 0
        while i < len(sorted_shifts):
            sequence_start = i
            while (
                i + 1 < len(sorted_shifts)
                and sorted_shifts[i + 1].date
                == sorted_shifts[i].date + timedelta(days=1)
            ):
                i += 1

            length = i - sequence_start + 1
            if length > 1:
                start_date = sorted_shifts[sequence_start].date
                if length not in sequences:
                    sequences[length] = []
                sequences[length].append(start_date)

            i += 1

        return sequences

    def _generate_summary(
        self,
        overall_score: float,
        rating: str,
        violations: List[ComplianceViolation],
        sections: List[ComplianceSection],
    ) -> str:
        """Generate executive summary text."""
        critical_count = sum(
            1 for v in violations if v.severity == ViolationSeverity.critical
        )
        warning_count = sum(
            1 for v in violations if v.severity == ViolationSeverity.warning
        )

        summary = f"""
Fair Work Compliance Report Summary
===================================

Overall Score: {overall_score:.1f}% ({rating.upper()})

Status Summary:
- {len(sections)} compliance areas audited
- {len(violations)} total violations found
- {critical_count} critical violations
- {warning_count} warnings

Section Breakdown:
"""
        for section in sections:
            summary += f"\n- {section.title}: {section.compliance_percentage:.1f}%"

        if critical_count > 0:
            summary += f"\n\nIMPORTANT: {critical_count} critical violations require immediate attention."

        return summary
