"""
Roster comparison and diff engine for RosterIQ.

Provides comprehensive diff analysis between two rosters, tracking:
- Added and removed shifts
- Modified shift details (time, role, cost)
- Cost and hours deltas
- Per-employee impact breakdown
- Risk analysis for changes that may cause conflicts

Core service: RosterDiffService.compare_rosters(roster_a, roster_b) -> RosterDiff

Usage:
    from rosteriq.services.roster_diff import RosterDiffService
    diff_service = RosterDiffService()
    diff = diff_service.compare_rosters(original_roster, new_roster)
    print(diff_service.format_diff_text(diff))
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

from rosteriq.models import Roster, Shift, Employee

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ShiftChange:
    """Represents a single shift that was modified."""
    shift_id: str
    employee_id: str
    date: date
    field_changes: Dict[str, Tuple[Any, Any]]  # field -> (old_val, new_val)

    def to_dict(self) -> dict:
        """Serialize to dict."""
        return {
            "shift_id": self.shift_id,
            "employee_id": self.employee_id,
            "date": self.date.isoformat(),
            "field_changes": {k: (str(v[0]), str(v[1])) for k, v in self.field_changes.items()},
        }


@dataclass
class EmployeeImpact:
    """Impact of roster changes on a single employee."""
    employee_id: str
    name: str
    hours_delta: float
    cost_delta: Decimal
    shifts_added: int = 0
    shifts_removed: int = 0
    shifts_changed: int = 0

    @property
    def total_shift_changes(self) -> int:
        """Total number of shifts affected (in any way)."""
        return self.shifts_added + self.shifts_removed + self.shifts_changed

    def to_dict(self) -> dict:
        """Serialize to dict."""
        return {
            "employee_id": self.employee_id,
            "name": self.name,
            "hours_delta": self.hours_delta,
            "cost_delta": str(self.cost_delta),
            "shifts_added": self.shifts_added,
            "shifts_removed": self.shifts_removed,
            "shifts_changed": self.shifts_changed,
            "total_shift_changes": self.total_shift_changes,
        }


@dataclass
class RosterDiff:
    """Complete diff between two rosters."""
    added_shifts: List[Shift] = field(default_factory=list)
    removed_shifts: List[Shift] = field(default_factory=list)
    modified_shifts: List[ShiftChange] = field(default_factory=list)
    cost_delta: Decimal = Decimal("0")
    hours_delta: float = 0.0
    headcount_delta: int = 0
    shift_count_delta: int = 0
    per_employee_impact: Dict[str, EmployeeImpact] = field(default_factory=dict)
    summary: str = ""

    def to_dict(self) -> dict:
        """Serialize to dict."""
        return {
            "added_shifts": [s.model_dump() for s in self.added_shifts],
            "removed_shifts": [s.model_dump() for s in self.removed_shifts],
            "modified_shifts": [m.to_dict() for m in self.modified_shifts],
            "cost_delta": str(self.cost_delta),
            "hours_delta": self.hours_delta,
            "headcount_delta": self.headcount_delta,
            "shift_count_delta": self.shift_count_delta,
            "per_employee_impact": {
                emp_id: impact.to_dict()
                for emp_id, impact in self.per_employee_impact.items()
            },
            "summary": self.summary,
        }


# ============================================================================
# RosterDiffService
# ============================================================================

class RosterDiffService:
    """
    Service for comparing two rosters and generating detailed diffs.
    """

    def __init__(self):
        """Initialize the service."""
        self.logger = logger

    def compare_rosters(self, roster_a: Roster, roster_b: Roster) -> RosterDiff:
        """
        Compare two rosters and generate a detailed diff.

        Args:
            roster_a: Original roster (baseline)
            roster_b: New roster (comparison target)

        Returns:
            RosterDiff with all changes, deltas, and impacts
        """
        diff = RosterDiff()

        # Index shifts by (employee_id, date) for easy lookup
        shifts_a_by_key = {
            (shift.employee_id, shift.date): shift
            for shift in roster_a.shifts
        }
        shifts_b_by_key = {
            (shift.employee_id, shift.date): shift
            for shift in roster_b.shifts
        }

        # Find added and removed shifts
        for key, shift_b in shifts_b_by_key.items():
            if key not in shifts_a_by_key:
                diff.added_shifts.append(shift_b)

        for key, shift_a in shifts_a_by_key.items():
            if key not in shifts_b_by_key:
                diff.removed_shifts.append(shift_a)

        # Find modified shifts (matching on employee_id + date)
        for key in shifts_a_by_key:
            if key in shifts_b_by_key:
                shift_a = shifts_a_by_key[key]
                shift_b = shifts_b_by_key[key]
                changes = self._detect_shift_changes(shift_a, shift_b)
                if changes:
                    diff.modified_shifts.append(
                        ShiftChange(
                            shift_id=shift_b.id,
                            employee_id=shift_b.employee_id,
                            date=shift_b.date,
                            field_changes=changes,
                        )
                    )

        # Calculate deltas
        diff.cost_delta = (roster_b.total_cost or Decimal("0")) - (
            roster_a.total_cost or Decimal("0")
        )
        diff.hours_delta = roster_b.total_hours - roster_a.total_hours
        diff.headcount_delta = len(roster_b.employees_used) - len(roster_a.employees_used)
        diff.shift_count_delta = roster_b.shift_count - roster_a.shift_count

        # Calculate per-employee impacts
        self._calculate_per_employee_impacts(
            roster_a, roster_b, diff
        )

        # Generate summary
        diff.summary = self.format_diff_text(diff)

        return diff

    def _detect_shift_changes(self, shift_a: Shift, shift_b: Shift) -> Dict[str, Tuple[Any, Any]]:
        """
        Detect field-level changes between two shifts.

        Returns:
            Dict of field -> (old_val, new_val) for changed fields
        """
        changes = {}
        fields_to_check = ["start_time", "end_time", "break_minutes", "role", "cost", "status"]

        for field in fields_to_check:
            val_a = getattr(shift_a, field)
            val_b = getattr(shift_b, field)
            if val_a != val_b:
                changes[field] = (val_a, val_b)

        return changes

    def _calculate_per_employee_impacts(
        self, roster_a: Roster, roster_b: Roster, diff: RosterDiff
    ) -> None:
        """
        Calculate impact breakdown per employee.
        """
        # Start with all employees from both rosters
        all_employees = roster_a.employees_used | roster_b.employees_used

        for emp_id in all_employees:
            shifts_a = [s for s in roster_a.shifts if s.employee_id == emp_id]
            shifts_b = [s for s in roster_b.shifts if s.employee_id == emp_id]

            hours_a = sum(s.net_hours for s in shifts_a)
            hours_b = sum(s.net_hours for s in shifts_b)

            cost_a = sum(s.cost or Decimal("0") for s in shifts_a)
            cost_b = sum(s.cost or Decimal("0") for s in shifts_b)

            hours_delta = hours_b - hours_a
            cost_delta = cost_b - cost_a

            # Count shift changes
            shifts_added = sum(
                1 for s in diff.added_shifts if s.employee_id == emp_id
            )
            shifts_removed = sum(
                1 for s in diff.removed_shifts if s.employee_id == emp_id
            )
            shifts_changed = sum(
                1 for m in diff.modified_shifts if m.employee_id == emp_id
            )

            if hours_delta != 0 or cost_delta != Decimal("0") or shifts_added or shifts_removed or shifts_changed:
                # Get employee name (fallback to ID if unknown)
                name = emp_id

                diff.per_employee_impact[emp_id] = EmployeeImpact(
                    employee_id=emp_id,
                    name=name,
                    hours_delta=hours_delta,
                    cost_delta=cost_delta,
                    shifts_added=shifts_added,
                    shifts_removed=shifts_removed,
                    shifts_changed=shifts_changed,
                )

    def diff_to_dict(self, diff: RosterDiff) -> dict:
        """
        Serialize RosterDiff to a JSON-friendly dict.

        Args:
            diff: RosterDiff instance

        Returns:
            Dict representation suitable for API responses
        """
        return diff.to_dict()

    def format_diff_text(self, diff: RosterDiff) -> str:
        """
        Generate a plain-text summary of the diff.

        Args:
            diff: RosterDiff instance

        Returns:
            Human-readable summary
        """
        lines = []

        # Header
        lines.append("=== ROSTER COMPARISON SUMMARY ===")
        lines.append("")

        # Overall stats
        lines.append("OVERALL CHANGES:")
        lines.append(f"  Shifts added:     {len(diff.added_shifts)}")
        lines.append(f"  Shifts removed:   {len(diff.removed_shifts)}")
        lines.append(f"  Shifts modified:  {len(diff.modified_shifts)}")
        lines.append(f"  Total shift delta: {diff.shift_count_delta:+d}")
        lines.append("")

        # Cost and hours
        lines.append("HOURS & COST IMPACT:")
        lines.append(f"  Hours delta:      {diff.hours_delta:+.1f} hours")
        lines.append(f"  Cost delta:       ${diff.cost_delta:+.2f}")
        lines.append(f"  Headcount delta:  {diff.headcount_delta:+d} employees")
        lines.append("")

        # Per-employee impacts
        if diff.per_employee_impact:
            lines.append("PER-EMPLOYEE IMPACT:")
            for emp_id in sorted(diff.per_employee_impact.keys()):
                impact = diff.per_employee_impact[emp_id]
                lines.append(f"  {impact.name} ({emp_id}):")
                lines.append(f"    Hours:   {impact.hours_delta:+.1f}h")
                lines.append(f"    Cost:    ${impact.cost_delta:+.2f}")
                if impact.shifts_added:
                    lines.append(f"    Added:   {impact.shifts_added} shift(s)")
                if impact.shifts_removed:
                    lines.append(f"    Removed: {impact.shifts_removed} shift(s)")
                if impact.shifts_changed:
                    lines.append(f"    Changed: {impact.shifts_changed} shift(s)")
            lines.append("")

        # Added shifts
        if diff.added_shifts:
            lines.append("ADDED SHIFTS:")
            for shift in diff.added_shifts:
                lines.append(
                    f"  {shift.employee_id} on {shift.date}: "
                    f"{shift.start_time}-{shift.end_time} "
                    f"({shift.net_hours:.1f}h, ${shift.cost or '?'})"
                )
            lines.append("")

        # Removed shifts
        if diff.removed_shifts:
            lines.append("REMOVED SHIFTS:")
            for shift in diff.removed_shifts:
                lines.append(
                    f"  {shift.employee_id} on {shift.date}: "
                    f"{shift.start_time}-{shift.end_time} "
                    f"({shift.net_hours:.1f}h, ${shift.cost or '?'})"
                )
            lines.append("")

        # Modified shifts
        if diff.modified_shifts:
            lines.append("MODIFIED SHIFTS:")
            for mod in diff.modified_shifts:
                lines.append(f"  {mod.employee_id} on {mod.date} ({mod.shift_id}):")
                for field, (old_val, new_val) in mod.field_changes.items():
                    lines.append(f"    {field}: {old_val} -> {new_val}")
            lines.append("")

        return "\n".join(lines)

    def calculate_savings(self, diff: RosterDiff) -> dict:
        """
        Analyze cost savings and categorize them.

        Args:
            diff: RosterDiff instance

        Returns:
            Dict with savings breakdown:
                total_savings: Decimal
                savings_by_category: Dict[str, Decimal]
                headcount_change: int
                cost_per_shift_removed: Decimal
        """
        total_savings = Decimal("0")

        # Savings from removed shifts
        removed_cost = sum(s.cost or Decimal("0") for s in diff.removed_shifts)

        # Costs from added shifts
        added_cost = sum(s.cost or Decimal("0") for s in diff.added_shifts)

        # Net savings (negative = cost increase)
        net_savings = removed_cost - added_cost

        # Average cost per removed shift
        cost_per_shift_removed = Decimal("0")
        if diff.removed_shifts:
            cost_per_shift_removed = removed_cost / len(diff.removed_shifts)

        return {
            "total_savings": net_savings,
            "removed_shifts_cost": removed_cost,
            "added_shifts_cost": added_cost,
            "cost_per_shift_removed": cost_per_shift_removed,
            "headcount_change": diff.headcount_delta,
            "hours_change": diff.hours_delta,
        }

    def identify_risk_changes(self, diff: RosterDiff) -> List[dict]:
        """
        Identify potentially risky changes that may cause conflicts.

        Args:
            diff: RosterDiff instance

        Returns:
            List of risk dictionaries with keys:
                type: str (e.g., "excessive_hours", "consecutive_days", etc.)
                severity: str ("low", "medium", "high")
                description: str
                affected_employees: List[str]
        """
        risks = []

        # Check for excessive hour changes per employee
        for emp_id, impact in diff.per_employee_impact.items():
            if impact.hours_delta > 15:
                risks.append({
                    "type": "excessive_hours_added",
                    "severity": "medium",
                    "description": f"Employee {impact.name} assigned {impact.hours_delta:.1f} additional hours",
                    "affected_employees": [emp_id],
                    "impact": impact.to_dict(),
                })
            elif impact.hours_delta < -15:
                risks.append({
                    "type": "excessive_hours_removed",
                    "severity": "low",
                    "description": f"Employee {impact.name} lost {abs(impact.hours_delta):.1f} hours",
                    "affected_employees": [emp_id],
                    "impact": impact.to_dict(),
                })

        # Check for large shift changes
        if len(diff.removed_shifts) > len(diff.added_shifts) * 1.5:
            risks.append({
                "type": "imbalanced_removal",
                "severity": "high",
                "description": f"Many shifts removed ({len(diff.removed_shifts)}) vs added ({len(diff.added_shifts)})",
                "affected_employees": list(diff.per_employee_impact.keys()),
            })

        # Check for simultaneous significant cost increase
        if diff.cost_delta > Decimal("100"):
            risks.append({
                "type": "cost_spike",
                "severity": "medium",
                "description": f"Total roster cost increased by ${diff.cost_delta:.2f}",
                "affected_employees": list(diff.per_employee_impact.keys()),
            })

        return risks
