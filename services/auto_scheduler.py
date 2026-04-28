"""
Smart Auto-Scheduling Engine for RosterIQ.

Generates full-week rosters from demand forecasts using a multi-step algorithm:
1. Demand → Headcount: Convert forecasts to required staff per hour
2. Shift Templates: Build shift blocks from demand patterns
3. Employee Assignment: Score and assign employees to shifts
4. Validation: Check conflicts and validate compliance
5. Return: ScheduleResult with quality metrics

The scheduler supports multiple strategies:
- "balanced": Equal weight cost + fairness
- "cost_optimized": Minimize total cost (may under-utilize staff)
- "coverage_first": Ensure all slots filled even if over budget
"""

import logging
from dataclasses import dataclass, field
from datetime import date, time, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Dict, Set, Tuple
from enum import Enum
from collections import defaultdict
import time as time_module
import uuid

from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig,
    EmploymentType, ShiftStatus, AwardLevel, State,
)
from rosteriq.award_rules import (
    get_penalty_multiplier, get_day_type, get_minimum_break_minutes,
    get_minimum_engagement_hours, validate_shift_compliance,
    check_consecutive_days, calculate_overtime_hours,
    MAX_SHIFT_LENGTH_HOURS, MINIMUM_HOURS_BETWEEN_SHIFTS,
)
from rosteriq.cost_calculator import calculate_shift_cost_breakdown
from rosteriq.services.conflict_detector import ConflictDetector, RosterConflict
from rosteriq.services.availability_resolver import AvailabilityResolver
from rosteriq.database import get_db

logger = logging.getLogger(__name__)

# Default covers per staff member per hour
DEFAULT_COVERS_PER_STAFF = 15.0

# Standard shift templates (start_hour, end_hour, break_minutes)
SHIFT_TEMPLATES = {
    "morning": (6, 14, 30),       # 8h with 30min break
    "mid": (10, 18, 30),          # 8h with 30min break
    "afternoon": (14, 22, 30),    # 8h with 30min break
    "evening": (17, 23, 0),       # 6h no break needed
    "short_am": (8, 13, 0),       # 5h no break needed
    "short_pm": (17, 22, 0),      # 5h no break needed
    "full_day": (9, 18, 50),      # 9h with 50min break
    "split_eve": (18, 23, 0),     # 5h evening
}


# ============================================================================
# Data Classes
# ============================================================================

class CoverageGap(dataclass):
    """Represents an unfilled shift slot."""
    date: date
    hour: int
    role: str
    reason: str  # "no_available_staff", "all_at_max_hours", etc.


class HiringRecommendation(dataclass):
    """Hiring recommendation based on coverage gaps."""
    role: str
    priority: str  # "urgent", "high", "medium", "low"
    gap_days: int  # Number of days with unfilled shifts
    estimated_hours_per_week: float
    reason: str


class ScheduleResult(dataclass):
    """Result of schedule generation."""
    roster: Roster
    schedule_quality: float  # 0-100 composite score
    coverage_gaps: List[CoverageGap] = field(default_factory=list)
    total_cost: Decimal = Decimal("0")
    cost_breakdown: Dict = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    employees_used: int = 0
    total_shifts: int = 0
    total_hours: float = 0.0
    strategy_used: str = "balanced"
    generation_time_ms: float = 0.0


# ============================================================================
# Employee Scoring
# ============================================================================

@dataclass
class EmployeeScore:
    """Score for assigning employee to a shift."""
    employee_id: str
    total_score: float
    cost_score: float
    fairness_score: float
    fatigue_score: float
    preference_score: float
    reasons: List[str] = field(default_factory=list)


# ============================================================================
# Main Scheduler Class
# ============================================================================

class AutoScheduler:
    """Smart auto-scheduler for generating weekly rosters."""

    def __init__(self, db=None):
        """
        Initialize scheduler.

        Args:
            db: Database connection. If None, uses get_db().
        """
        self.db = db or get_db()
        self.conflict_detector = ConflictDetector()
        self.availability_resolver = AvailabilityResolver(self.db)

    def generate_week(
        self,
        venue_id: str,
        week_start: date,
        strategy: str = "balanced",
        covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
    ) -> ScheduleResult:
        """
        Generate a full week's roster from demand forecasts.

        Args:
            venue_id: Venue to schedule for
            week_start: Monday of the week (ISO 8601)
            strategy: "balanced", "cost_optimized", or "coverage_first"
            covers_per_staff: Covers per staff ratio (override)

        Returns:
            ScheduleResult with generated roster and metrics
        """
        start_time = time_module.time()

        try:
            # Step 1: Fetch inputs
            venue = self.db.get_venue(venue_id)
            if not venue:
                raise ValueError(f"Venue {venue_id} not found")

            # Get all employees (filtered by venue in a real implementation)
            employees = self.db.list_employees()
            if not employees:
                raise ValueError(f"No employees found")

            # Fetch demand forecasts for 7 days
            forecasts_by_date: Dict[date, List[DemandForecast]] = {}
            for i in range(7):
                current_date = week_start + timedelta(days=i)
                forecasts = self.db.get_forecasts(
                    venue_id=venue_id, start_date=current_date, end_date=current_date
                )
                forecasts_by_date[current_date] = forecasts

            # Step 2: Convert demand to required headcount grid
            headcount_grid = self._demand_to_headcount(
                forecasts_by_date, venue, covers_per_staff
            )

            # Step 3: Build shift templates from demand grid
            shift_slots = self._build_shift_slots(headcount_grid, week_start, venue)

            # Step 4: Assign employees to shifts
            shifts = self._assign_employees_to_shifts(
                shift_slots, employees, venue, week_start, strategy
            )

            # Step 5: Create roster object
            week_end = week_start + timedelta(days=6)
            roster = Roster(
                id=str(uuid.uuid4()),
                venue_id=venue_id,
                week_start=week_start,
                week_end=week_end,
                shifts=shifts,
                created_at=datetime.now(),
            )

            # Step 6: Validate and detect conflicts
            conflicts = self.conflict_detector.detect_conflicts(
                roster, venue, employees
            )

            # Step 7: Calculate costs
            total_cost = Decimal("0")
            cost_breakdown = self._calculate_cost_breakdown(roster, employees)
            for cost_item in cost_breakdown.values():
                if isinstance(cost_item, dict) and "total" in cost_item:
                    total_cost += Decimal(str(cost_item["total"]))
                elif isinstance(cost_item, (int, float, Decimal)):
                    total_cost += Decimal(str(cost_item))

            roster.total_cost = total_cost

            # Step 8: Calculate quality score
            quality_score = self._calculate_quality_score(
                roster, headcount_grid, conflicts, employees
            )

            # Step 9: Identify coverage gaps
            coverage_gaps = self._identify_coverage_gaps(
                roster, headcount_grid, shift_slots
            )

            # Generate warnings
            warnings = self._generate_warnings(conflicts, coverage_gaps, strategy)

            # Build result
            elapsed_ms = (time_module.time() - start_time) * 1000
            return ScheduleResult(
                roster=roster,
                schedule_quality=quality_score,
                coverage_gaps=coverage_gaps,
                total_cost=total_cost,
                cost_breakdown=cost_breakdown,
                warnings=warnings,
                employees_used=len(roster.employees_used),
                total_shifts=len(shifts),
                total_hours=roster.total_hours,
                strategy_used=strategy,
                generation_time_ms=elapsed_ms,
            )

        except Exception as e:
            logger.error(f"Schedule generation failed: {e}", exc_info=True)
            raise

    def _demand_to_headcount(
        self,
        forecasts_by_date: Dict[date, List[DemandForecast]],
        venue: VenueConfig,
        covers_per_staff: float,
    ) -> Dict[date, Dict[int, Dict[str, int]]]:
        """
        Convert demand forecasts to required staff per hour.

        Returns:
            dict[date, dict[hour, dict[role, required_count]]]
        """
        headcount_grid = {}

        for current_date, forecasts in forecasts_by_date.items():
            hourly = {}

            for fc in forecasts:
                # Convert covers to staff needed
                staff_needed = max(1, int(fc.predicted_covers / covers_per_staff + 0.5))

                # Apply venue minimum
                if hasattr(venue, 'covers_per_staff'):
                    venue_ratio = venue.covers_per_staff
                else:
                    venue_ratio = covers_per_staff

                if fc.hour not in hourly:
                    hourly[fc.hour] = defaultdict(int)

                hourly[fc.hour]["all"] = staff_needed

            # Fill gaps and distribute by role
            if not hourly:
                hourly = {h: {"all": 1} for h in range(6, 23)}

            # Distribute by role using min_staff config
            final_hourly = {}
            for hour, roles_dict in hourly.items():
                final_hourly[hour] = dict(roles_dict) if roles_dict else {"all": 1}

            headcount_grid[current_date] = final_hourly

        return headcount_grid

    def _build_shift_slots(
        self,
        headcount_grid: Dict[date, Dict[int, Dict[str, int]]],
        week_start: date,
        venue: VenueConfig,
    ) -> List[Dict]:
        """
        Build shift slots from demand patterns.

        Returns:
            List of {date, start_hour, end_hour, role, required_count}
        """
        shift_slots = []

        for current_date in [week_start + timedelta(days=i) for i in range(7)]:
            hourly_demands = headcount_grid.get(current_date, {})

            if not hourly_demands:
                continue

            # Identify contiguous blocks needing staff
            hours_sorted = sorted(hourly_demands.keys())
            blocks = []
            current_block = None

            for hour in hours_sorted:
                required = hourly_demands[hour].get("all", 1)

                if required > 0:
                    if current_block is None:
                        current_block = {
                            "start_hour": hour,
                            "end_hour": hour + 1,
                            "total_required": required,
                        }
                    else:
                        current_block["end_hour"] = hour + 1
                        current_block["total_required"] += required
                else:
                    if current_block:
                        blocks.append(current_block)
                        current_block = None

            if current_block:
                blocks.append(current_block)

            # Generate shift slots from blocks
            for block in blocks:
                start = block["start_hour"]
                end = block["end_hour"]
                required = max(1, int(block["total_required"] / (end - start)))

                # Try to match against shift templates
                shift_slots.append({
                    "date": current_date,
                    "start_hour": start,
                    "end_hour": end,
                    "role": "general",
                    "required_count": required,
                })

        return shift_slots

    def _assign_employees_to_shifts(
        self,
        shift_slots: List[Dict],
        employees: List[Employee],
        venue: VenueConfig,
        week_start: date,
        strategy: str,
    ) -> List[Shift]:
        """
        Assign employees to shift slots using scoring.

        Args:
            shift_slots: List of shift slots to fill
            employees: Available employees
            venue: Venue configuration
            week_start: Start of week
            strategy: Scheduling strategy

        Returns:
            List of scheduled Shift objects
        """
        shifts = []
        employee_hours_this_week = {e.id: 0.0 for e in employees}
        employee_consecutive_days = {e.id: 0 for e in employees}
        employee_last_shift_date = {e.id: None for e in employees}

        for slot in shift_slots:
            slot_date = slot["date"]
            start_hour = slot["start_hour"]
            end_hour = slot["end_hour"]
            required_count = slot["required_count"]

            # Create shift objects for each required position
            for _ in range(required_count):
                # Score eligible employees
                candidates = []

                for emp in employees:
                    if self._is_employee_eligible(
                        emp, slot_date, start_hour, end_hour,
                        employee_hours_this_week, venue
                    ):
                        score = self._score_employee(
                            emp, slot_date, start_hour, end_hour,
                            employee_hours_this_week, employee_last_shift_date,
                            strategy
                        )
                        candidates.append((emp, score))

                if not candidates:
                    logger.warning(
                        f"No eligible employees for {slot_date} "
                        f"{start_hour}-{end_hour}"
                    )
                    continue

                # Select best candidate based on strategy
                candidates.sort(key=lambda x: x[1].total_score, reverse=True)
                selected_emp, score = candidates[0]

                # Create shift
                shift = self._create_shift(
                    selected_emp, slot_date, start_hour, end_hour
                )
                shifts.append(shift)

                # Update tracking
                employee_hours_this_week[selected_emp.id] += shift.net_hours
                employee_last_shift_date[selected_emp.id] = slot_date

        return shifts

    def _is_employee_eligible(
        self,
        employee: Employee,
        shift_date: date,
        start_hour: int,
        end_hour: int,
        hours_so_far: Dict[str, float],
        venue: VenueConfig,
    ) -> bool:
        """Check if employee is eligible for shift."""
        # Check max hours per week
        if hours_so_far[employee.id] + (end_hour - start_hour) > employee.max_hours_per_week:
            return False

        # Check availability (simplified - check if day is in availability)
        day_name = shift_date.strftime("%A").lower()
        if employee.availability and day_name in employee.availability:
            day_avail = employee.availability[day_name]
            if not day_avail:  # Not available this day
                return False

        return True

    def _score_employee(
        self,
        employee: Employee,
        shift_date: date,
        start_hour: int,
        end_hour: int,
        hours_so_far: Dict[str, float],
        last_shift_date: Dict[str, Optional[date]],
        strategy: str,
    ) -> EmployeeScore:
        """Score employee for assignment to shift."""
        shift_duration = end_hour - start_hour

        # Cost score: prefer FT/PT over casual (lower total cost with loadings)
        if employee.employment_type == EmploymentType.full_time:
            cost_score = 10.0
        elif employee.employment_type == EmploymentType.part_time:
            cost_score = 8.0
        else:  # casual
            cost_score = 5.0

        # Fairness score: prefer employees with fewer hours so far this week
        total_hours_this_week = hours_so_far.get(employee.id, 0.0)
        fairness_score = max(0, 10.0 - (total_hours_this_week / 4.0))

        # Fatigue score: prefer employees with more rest since last shift
        last_shift = last_shift_date.get(employee.id)
        if last_shift is None:
            fatigue_score = 10.0
        else:
            days_rest = (shift_date - last_shift).days
            fatigue_score = min(10.0, days_rest * 2.0)

        # Preference score (simplified - could integrate preference_learner)
        preference_score = 5.0

        # Combine based on strategy
        if strategy == "cost_optimized":
            total = cost_score * 0.6 + fairness_score * 0.2 + fatigue_score * 0.1 + preference_score * 0.1
        elif strategy == "coverage_first":
            total = cost_score * 0.2 + fairness_score * 0.2 + fatigue_score * 0.3 + preference_score * 0.3
        else:  # balanced
            total = cost_score * 0.3 + fairness_score * 0.3 + fatigue_score * 0.2 + preference_score * 0.2

        return EmployeeScore(
            employee_id=employee.id,
            total_score=total,
            cost_score=cost_score,
            fairness_score=fairness_score,
            fatigue_score=fatigue_score,
            preference_score=preference_score,
            reasons=[f"Weighted by {strategy} strategy"],
        )

    def _create_shift(
        self,
        employee: Employee,
        shift_date: date,
        start_hour: int,
        end_hour: int,
    ) -> Shift:
        """Create a Shift object."""
        start_time_obj = time(hour=start_hour, minute=0)
        end_time_obj = time(hour=end_hour, minute=0)

        # Determine break duration
        shift_duration_hours = end_hour - start_hour
        if shift_duration_hours >= 6:
            break_minutes = 30
        else:
            break_minutes = 0

        # Calculate cost (simplified)
        day_type = get_day_type(shift_date)
        penalty_mult = get_penalty_multiplier(employee.employment_type, day_type)
        base_cost = Decimal(str(employee.hourly_base_rate)) * Decimal(str(shift_duration_hours - break_minutes / 60.0))
        cost = base_cost * penalty_mult

        return Shift(
            id=str(uuid.uuid4()),
            employee_id=employee.id,
            date=shift_date,
            start_time=start_time_obj,
            end_time=end_time_obj,
            break_minutes=break_minutes,
            status=ShiftStatus.scheduled,
            role="general",
            cost=cost,
            penalty_multiplier=float(penalty_mult),
        )

    def _calculate_cost_breakdown(
        self,
        roster: Roster,
        employees: List[Employee],
    ) -> Dict:
        """Calculate cost breakdown by employment type, day, and role."""
        breakdown = {
            "by_employment_type": defaultdict(Decimal),
            "by_day": defaultdict(Decimal),
            "by_role": defaultdict(Decimal),
            "total": Decimal("0"),
        }

        emp_dict = {e.id: e for e in employees}

        for shift in roster.shifts:
            cost = shift.cost or Decimal("0")
            emp = emp_dict.get(shift.employee_id)

            if emp:
                breakdown["by_employment_type"][emp.employment_type.value] += cost
            breakdown["by_day"][shift.date.isoformat()] += cost
            breakdown["by_role"][shift.role] += cost
            breakdown["total"] += cost

        return dict(breakdown)

    def _calculate_quality_score(
        self,
        roster: Roster,
        headcount_grid: Dict,
        conflicts: List[RosterConflict],
        employees: List[Employee],
    ) -> float:
        """
        Calculate composite quality score (0-100).

        Factors:
        - Coverage completeness (40%)
        - Conflict severity (40%)
        - Cost efficiency (20%)
        """
        # Coverage: compare shifts to demand
        total_hours_demanded = 0.0
        total_hours_scheduled = 0.0

        for date_obj, hourly in headcount_grid.items():
            for hour, roles in hourly.items():
                total_demanded = sum(roles.values())
                total_hours_demanded += total_demanded

        for shift in roster.shifts:
            total_hours_scheduled += shift.net_hours

        coverage_pct = min(100, (total_hours_scheduled / max(1, total_hours_demanded)) * 100)
        coverage_score = coverage_pct

        # Conflicts
        critical_conflicts = sum(1 for c in conflicts if c.severity.value == "critical")
        warning_conflicts = sum(1 for c in conflicts if c.severity.value == "warning")
        conflict_penalty = (critical_conflicts * 10) + (warning_conflicts * 2)
        conflict_score = max(0, 100 - conflict_penalty)

        # Cost efficiency (simplified)
        cost_score = 80.0  # Placeholder

        # Composite
        quality = (coverage_score * 0.4) + (conflict_score * 0.4) + (cost_score * 0.2)
        return min(100, max(0, quality))

    def _identify_coverage_gaps(
        self,
        roster: Roster,
        headcount_grid: Dict,
        shift_slots: List[Dict],
    ) -> List[CoverageGap]:
        """Identify unfilled shift slots."""
        gaps = []

        # Build map of scheduled hours
        scheduled = defaultdict(lambda: defaultdict(int))
        for shift in roster.shifts:
            for hour in range(shift.start_time.hour, shift.end_time.hour):
                scheduled[shift.date][hour] += 1

        # Check against demand
        for date_obj, hourly in headcount_grid.items():
            for hour, roles in hourly.items():
                for role, required in roles.items():
                    actual = scheduled[date_obj].get(hour, 0)
                    if actual < required:
                        gap = CoverageGap(
                            date=date_obj,
                            hour=hour,
                            role=role,
                            reason="no_available_staff",
                        )
                        gaps.append(gap)

        return gaps

    def _generate_warnings(
        self,
        conflicts: List[RosterConflict],
        gaps: List[CoverageGap],
        strategy: str,
    ) -> List[str]:
        """Generate warning messages."""
        warnings = []

        if conflicts:
            critical = sum(1 for c in conflicts if c.severity.value == "critical")
            if critical > 0:
                warnings.append(f"{critical} critical compliance conflicts detected")

        if gaps:
            warnings.append(f"{len(gaps)} coverage gaps identified")

        if strategy == "cost_optimized" and gaps:
            warnings.append("Cost optimization may have reduced coverage")

        return warnings

    def preview_week(
        self,
        venue_id: str,
        week_start: date,
        covers_per_staff: float = DEFAULT_COVERS_PER_STAFF,
    ) -> Dict:
        """
        Preview demand and availability without generating schedule.

        Returns:
            dict with demand_grid, available_staff_by_role, capacity_analysis
        """
        try:
            venue = self.db.get_venue(venue_id)
            employees = self.db.list_employees()

            forecasts_by_date = {}
            for i in range(7):
                current_date = week_start + timedelta(days=i)
                forecasts = self.db.get_forecasts(
                    venue_id=venue_id, start_date=current_date, end_date=current_date
                )
                forecasts_by_date[current_date] = forecasts

            demand_grid = self._demand_to_headcount(
                forecasts_by_date, venue, covers_per_staff
            )

            available_staff = {
                "full_time": sum(1 for e in employees if e.employment_type == EmploymentType.full_time),
                "part_time": sum(1 for e in employees if e.employment_type == EmploymentType.part_time),
                "casual": sum(1 for e in employees if e.employment_type == EmploymentType.casual),
                "total": len(employees),
            }

            return {
                "week_start": week_start.isoformat(),
                "demand_grid": {
                    d.isoformat(): {str(h): v for h, v in hd.items()}
                    for d, hd in demand_grid.items()
                },
                "available_staff": available_staff,
            }

        except Exception as e:
            logger.error(f"Preview failed: {e}")
            raise

    def fill_gaps(self, roster_id: str, venue_id: str) -> List[Shift]:
        """
        Find and fill remaining coverage gaps in existing roster.

        Args:
            roster_id: ID of existing roster
            venue_id: Venue ID

        Returns:
            List of newly created shifts to fill gaps
        """
        try:
            roster = self.db.get_roster(roster_id)
            if not roster:
                raise ValueError(f"Roster {roster_id} not found")

            new_shifts = []
            # Gap filling logic would go here
            logger.info(f"Filled {len(new_shifts)} gaps for roster {roster_id}")
            return new_shifts

        except Exception as e:
            logger.error(f"Gap filling failed: {e}")
            raise

    def suggest_hiring(
        self,
        venue_id: str,
        week_start: date,
    ) -> List[HiringRecommendation]:
        """
        Suggest hiring based on chronic coverage gaps.

        Args:
            venue_id: Venue ID
            week_start: Week to analyze

        Returns:
            List of HiringRecommendation objects
        """
        try:
            recommendations = []

            # Analyze 4-week pattern to identify trends
            gaps_by_role = defaultdict(int)
            gap_days_by_role = defaultdict(set)

            for week_offset in range(4):
                current_week = week_start + timedelta(weeks=week_offset)
                result = self.generate_week(venue_id, current_week)

                for gap in result.coverage_gaps:
                    gaps_by_role[gap.role] += 1
                    gap_days_by_role[gap.role].add(gap.date)

            # Convert to recommendations
            for role, gap_count in gaps_by_role.items():
                if gap_count > 5:  # Threshold: more than 5 gaps in 4 weeks
                    recommendation = HiringRecommendation(
                        role=role,
                        priority="high" if gap_count > 10 else "medium",
                        gap_days=len(gap_days_by_role[role]),
                        estimated_hours_per_week=gap_count * 2,  # Rough estimate
                        reason=f"Chronic undercoverage in {role}",
                    )
                    recommendations.append(recommendation)

            return recommendations

        except Exception as e:
            logger.error(f"Hiring suggestion failed: {e}")
            raise
