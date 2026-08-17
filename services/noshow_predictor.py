"""
Predictive no-show model for RosterIQ.

Identifies high-risk shifts where employees are likely to not show up based on:
- Historical no-show rate for the employee
- Day-of-week patterns
- Shift timing (early morning shifts have higher risk)
- Consecutive days worked
- Fatigue score (if available)
- Weather severity
- Recent pattern weighting (last 2 weeks emphasized)
- Shift confirmation status

Provides:
- Risk-scored shifts with mitigation suggestions
- Employee reliability profiles
- Backup staff recommendations
- Venue-level risk summary
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, time
from typing import Optional, List, Dict, Tuple
from decimal import Decimal
import logging
import math

from rosteriq.database import get_db
from rosteriq.models import Employee, Shift, Roster, ShiftStatus, EmploymentType

logger = logging.getLogger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass
class RiskFactor:
    """Individual factor contributing to no-show risk."""
    name: str
    score: float  # 0-100
    weight: float  # 0-1
    description: str


@dataclass
class BackupEmployee:
    """Backup staff recommendation for a high-risk shift."""
    employee_id: str
    name: str
    availability_match: bool
    cost_delta: Optional[Decimal] = None
    skills_match: bool = True


@dataclass
class ShiftRisk:
    """Risk assessment for a single shift."""
    shift_id: str
    employee_id: str
    employee_name: str
    date: str  # ISO format
    start_time: str  # HH:MM
    end_time: str  # HH:MM
    role: str
    risk_score: float  # 0-100
    risk_level: str  # "low" | "medium" | "high" | "critical"
    risk_factors: List[RiskFactor] = field(default_factory=list)
    suggested_backups: List[BackupEmployee] = field(default_factory=list)
    mitigation: str = ""


@dataclass
class ReliabilityProfile:
    """Historical reliability stats for an employee."""
    employee_id: str
    employee_name: str
    total_shifts: int
    completed: int
    no_shows: int
    late_arrivals: int
    reliability_pct: float  # percentage
    trend: str  # "improving" | "stable" | "declining"
    risk_days: List[str] = field(default_factory=list)  # e.g. ["Monday", "Sunday"]
    days_since_last_noshow: int = 0


@dataclass
class VenueRiskSummary:
    """Aggregate risk summary for a venue."""
    venue_id: str
    date_from: str
    date_to: str
    total_shifts_at_risk: int
    shifts_high_risk: int
    shifts_critical_risk: int
    total_risk_exposure: float  # sum of all risk scores
    avg_risk_score: float
    highest_risk_date: Optional[str] = None
    highest_risk_role: Optional[str] = None
    recommendations: List[str] = field(default_factory=list)


# ============================================================================
# NoShowPredictor class
# ============================================================================


class NoShowPredictor:
    """Predicts no-show risk for shifts and provides mitigation strategies."""

    def __init__(self):
        """Initialize the no-show predictor."""
        self.db = get_db()
        # Factor weights must sum to 1.0
        self.weights = {
            "historical_noshow_rate": 0.30,
            "day_of_week": 0.15,
            "shift_timing": 0.10,
            "consecutive_days": 0.10,
            "fatigue_score": 0.10,
            "weather_severity": 0.05,
            "recent_pattern": 0.10,
            "notice_confirmation": 0.10,
        }

    def predict_risks(
        self,
        venue_id: str,
        date_range_start: str,
        date_range_end: str,
    ) -> List[ShiftRisk]:
        """
        Predict no-show risks for all shifts in a date range.

        Args:
            venue_id: The venue ID
            date_range_start: ISO format date string (YYYY-MM-DD)
            date_range_end: ISO format date string (YYYY-MM-DD)

        Returns:
            List of ShiftRisk objects sorted by risk score (highest first)
        """
        try:
            start_date = date.fromisoformat(date_range_start)
            end_date = date.fromisoformat(date_range_end)
        except ValueError:
            logger.error(f"Invalid date format: {date_range_start}, {date_range_end}")
            return []

        # Get all rosters in date range
        all_rosters = self.db.list_rosters()
        rosters_in_range = [
            r for r in all_rosters
            if (r.venue_id == venue_id and
                r.week_start <= end_date and r.week_end >= start_date)
        ]

        risks: List[ShiftRisk] = []
        for roster in rosters_in_range:
            for shift in roster.shifts:
                # Only assess upcoming/scheduled shifts
                if shift.status in [ShiftStatus.scheduled, ShiftStatus.confirmed]:
                    if start_date <= shift.date <= end_date:
                        risk = self._assess_shift_risk(shift, venue_id)
                        risks.append(risk)

        # Sort by risk score descending
        risks.sort(key=lambda x: x.risk_score, reverse=True)
        return risks

    def _assess_shift_risk(self, shift: Shift, venue_id: str) -> ShiftRisk:
        """
        Calculate risk score for a single shift.

        Combines multiple factors:
        - Historical no-show rate: 30%
        - Day of week pattern: 15%
        - Shift timing (early morning): 10%
        - Consecutive days worked: 10%
        - Fatigue score: 10%
        - Weather severity: 5%
        - Recent pattern (last 2 weeks): 10%
        - Notice/confirmation: 10%
        """
        risk_factors: List[RiskFactor] = []

        # 1. Historical no-show rate
        reliability = self.get_employee_reliability(shift.employee_id)
        noshow_score = min(100, (reliability.no_shows / max(1, reliability.total_shifts)) * 100)
        risk_factors.append(RiskFactor(
            name="Historical No-Show Rate",
            score=noshow_score,
            weight=self.weights["historical_noshow_rate"],
            description=f"{reliability.no_shows}/{reliability.total_shifts} shifts no-show"
        ))

        # 2. Day of week pattern
        day_score = self._assess_day_of_week_risk(shift.employee_id, shift.date)
        risk_factors.append(RiskFactor(
            name="Day of Week Pattern",
            score=day_score,
            weight=self.weights["day_of_week"],
            description=f"Employee has pattern risk on {shift.date.strftime('%A')}"
        ))

        # 3. Shift timing (early morning shifts higher risk)
        timing_score = self._assess_shift_timing_risk(shift.start_time)
        risk_factors.append(RiskFactor(
            name="Shift Timing",
            score=timing_score,
            weight=self.weights["shift_timing"],
            description=f"Start time {shift.start_time.strftime('%H:%M')} has baseline risk"
        ))

        # 4. Consecutive days worked
        consecutive_score = self._assess_consecutive_days_risk(
            shift.employee_id, shift.date
        )
        risk_factors.append(RiskFactor(
            name="Consecutive Days Worked",
            score=consecutive_score,
            weight=self.weights["consecutive_days"],
            description=f"Employee approaching fatigue threshold"
        ))

        # 5. Fatigue score (if we have fatigue predictor available)
        fatigue_score = self._assess_fatigue_risk(shift.employee_id)
        risk_factors.append(RiskFactor(
            name="Fatigue Level",
            score=fatigue_score,
            weight=self.weights["fatigue_score"],
            description=f"Fatigue assessment indicates stress level"
        ))

        # 6. Weather severity (placeholder - would integrate with weather API)
        weather_score = self._assess_weather_risk(venue_id, shift.date)
        risk_factors.append(RiskFactor(
            name="Weather Severity",
            score=weather_score,
            weight=self.weights["weather_severity"],
            description=f"Weather conditions may impact attendance"
        ))

        # 7. Recent pattern (last 2 weeks weighted more)
        recent_score = self._assess_recent_pattern_risk(shift.employee_id)
        risk_factors.append(RiskFactor(
            name="Recent Pattern",
            score=recent_score,
            weight=self.weights["recent_pattern"],
            description=f"Recent 2-week performance trend"
        ))

        # 8. Notice/confirmation
        confirmation_score = self._assess_confirmation_risk(shift)
        risk_factors.append(RiskFactor(
            name="Notice/Confirmation",
            score=confirmation_score,
            weight=self.weights["notice_confirmation"],
            description=f"Shift confirmation status: {shift.status}"
        ))

        # Calculate weighted risk score
        total_score = 0.0
        total_weight = 0.0
        for factor in risk_factors:
            total_score += factor.score * factor.weight
            total_weight += factor.weight

        risk_score = total_score / max(total_weight, 0.01)  # Normalize to 0-100
        risk_score = max(0, min(100, risk_score))  # Clamp to 0-100

        # Determine risk level
        if risk_score < 25:
            risk_level = "low"
        elif risk_score < 50:
            risk_level = "medium"
        elif risk_score < 75:
            risk_level = "high"
        else:
            risk_level = "critical"

        # Get suggested backups for high-risk shifts
        suggested_backups: List[BackupEmployee] = []
        if risk_score >= 50:
            suggested_backups = self.suggest_backups(shift.id)

        # Generate mitigation suggestion
        mitigation = self._generate_mitigation(risk_score, risk_factors, shift)

        return ShiftRisk(
            shift_id=shift.id,
            employee_id=shift.employee_id,
            employee_name=self.db.get_employee(shift.employee_id).name,
            date=shift.date.isoformat(),
            start_time=shift.start_time.strftime("%H:%M"),
            end_time=shift.end_time.strftime("%H:%M"),
            role=shift.role,
            risk_score=round(risk_score, 1),
            risk_level=risk_level,
            risk_factors=risk_factors,
            suggested_backups=suggested_backups,
            mitigation=mitigation,
        )

    def _assess_day_of_week_risk(self, employee_id: str, shift_date: date) -> float:
        """Score based on historical no-show pattern for day of week."""
        employee = self.db.get_employee(employee_id)
        if not employee:
            return 0.0

        # Get all shifts for this employee in last 90 days
        all_rosters = self.db.list_rosters()
        lookback_date = shift_date - timedelta(days=90)

        day_name = shift_date.strftime("%A")
        day_noshows = 0
        day_total = 0

        for roster in all_rosters:
            for shift in roster.shifts:
                if (shift.employee_id == employee_id and
                    shift.date >= lookback_date and
                    shift.date.strftime("%A") == day_name):
                    day_total += 1
                    if shift.status == ShiftStatus.no_show:
                        day_noshows += 1

        if day_total == 0:
            return 0.0

        return min(100, (day_noshows / day_total) * 100)

    def _assess_shift_timing_risk(self, start_time: time) -> float:
        """Early morning shifts (5-8am) have higher baseline no-show risk."""
        hour = start_time.hour
        minute = start_time.minute
        total_minutes = hour * 60 + minute

        # Risk curve: early morning (5-8am) peaks, afternoon lower
        if 5 * 60 <= total_minutes < 8 * 60:  # 5am-8am
            return 35.0  # Higher baseline risk
        elif 4 * 60 <= total_minutes < 5 * 60:  # 4am-5am
            return 45.0  # Very high
        elif 8 * 60 <= total_minutes < 10 * 60:  # 8am-10am
            return 25.0
        elif 10 * 60 <= total_minutes < 18 * 60:  # 10am-6pm
            return 15.0  # Afternoon low
        elif 18 * 60 <= total_minutes < 22 * 60:  # 6pm-10pm
            return 20.0
        else:  # Late night/overnight
            return 30.0

    def _assess_consecutive_days_risk(self, employee_id: str, shift_date: date) -> float:
        """Score increases after 4+ consecutive days worked."""
        employee = self.db.get_employee(employee_id)
        if not employee:
            return 0.0

        # Count consecutive days up to shift_date
        all_rosters = self.db.list_rosters()
        consecutive_count = 0

        for i in range(10):  # Check up to 10 days back
            check_date = shift_date - timedelta(days=i)
            has_shift = False

            for roster in all_rosters:
                for shift in roster.shifts:
                    if (shift.employee_id == employee_id and
                        shift.date == check_date and
                        shift.status not in [ShiftStatus.cancelled]):
                        has_shift = True
                        break
                if has_shift:
                    break

            if has_shift:
                consecutive_count += 1
            else:
                break

        # Risk increases after 4 consecutive days
        if consecutive_count < 4:
            return 0.0
        elif consecutive_count == 4:
            return 20.0
        elif consecutive_count == 5:
            return 35.0
        elif consecutive_count == 6:
            return 50.0
        else:
            return 65.0  # Cap at 65 to not dominate

    def _assess_fatigue_risk(self, employee_id: str) -> float:
        """Integrate with fatigue predictor if available."""
        try:
            from rosteriq.services.fatigue_predictor import FatiguePredictor
            fatigue_predictor = FatiguePredictor()
            assessment = fatigue_predictor.assess_fatigue(employee_id, lookback_weeks=4)
            if assessment:
                # Map fatigue risk level to no-show risk
                if assessment.risk_level == "low":
                    return 10.0
                elif assessment.risk_level == "medium":
                    return 30.0
                elif assessment.risk_level == "high":
                    return 50.0
                else:  # critical
                    return 70.0
        except (ImportError, Exception) as e:
            logger.debug(f"Fatigue predictor not available: {e}")

        return 0.0

    def _assess_weather_risk(self, venue_id: str, shift_date: date) -> float:
        """Score based on weather severity (placeholder for API integration)."""
        # TODO: Integrate with weather API (BOM for Australia)
        # For now return baseline
        return 0.0

    def _assess_recent_pattern_risk(self, employee_id: str) -> float:
        """Weight recent 2-week performance higher than older history."""
        employee = self.db.get_employee(employee_id)
        if not employee:
            return 0.0

        all_rosters = self.db.list_rosters()
        today = date.today()
        two_weeks_ago = today - timedelta(days=14)

        recent_noshows = 0
        recent_total = 0

        for roster in all_rosters:
            for shift in roster.shifts:
                if (shift.employee_id == employee_id and
                    shift.date >= two_weeks_ago):
                    recent_total += 1
                    if shift.status == ShiftStatus.no_show:
                        recent_noshows += 1

        if recent_total == 0:
            return 0.0

        return min(100, (recent_noshows / recent_total) * 100)

    def _assess_confirmation_risk(self, shift: Shift) -> float:
        """Unconfirmed shifts have higher no-show risk."""
        if shift.status == ShiftStatus.confirmed:
            return 10.0  # Low risk - employee confirmed
        elif shift.status == ShiftStatus.scheduled:
            return 50.0  # Medium risk - not yet confirmed
        else:
            return 25.0

    def _generate_mitigation(
        self, risk_score: float, factors: List[RiskFactor], shift: Shift
    ) -> str:
        """Generate a mitigation suggestion based on risk profile."""
        if risk_score >= 75:
            return (
                "CRITICAL: Contact employee immediately to confirm attendance. "
                "Have backup staff on standby. Consider split shift or call-in."
            )
        elif risk_score >= 50:
            return (
                "HIGH RISK: Confirm shift with employee 48 hours prior. "
                "Brief backup staff. Monitor closely."
            )
        elif risk_score >= 25:
            return (
                "MEDIUM RISK: Confirm shift with employee 24 hours prior. "
                "Keep secondary backup available."
            )
        else:
            return "Low risk. Standard monitoring."

    def get_employee_reliability(self, employee_id: str) -> ReliabilityProfile:
        """
        Calculate historical reliability statistics for an employee.

        Returns stats for last 90 days:
        - Total shifts, completed, no-shows, late arrivals
        - Reliability percentage
        - Trend (improving/stable/declining)
        - Risk days (days with high no-show rate)
        """
        employee = self.db.get_employee(employee_id)
        if not employee:
            return ReliabilityProfile(
                employee_id=employee_id,
                employee_name="Unknown",
                total_shifts=0,
                completed=0,
                no_shows=0,
                late_arrivals=0,
                reliability_pct=0.0,
                trend="stable",
            )

        all_rosters = self.db.list_rosters()
        lookback_date = date.today() - timedelta(days=90)

        total_shifts = 0
        completed = 0
        no_shows = 0
        late_arrivals = 0
        day_stats: Dict[str, Tuple[int, int]] = {}  # day -> (noshows, total)

        for roster in all_rosters:
            for shift in roster.shifts:
                if (shift.employee_id == employee_id and
                    shift.date >= lookback_date):
                    total_shifts += 1

                    day_name = shift.date.strftime("%A")
                    if day_name not in day_stats:
                        day_stats[day_name] = (0, 0)
                    noshows, total = day_stats[day_name]
                    day_stats[day_name] = (noshows, total + 1)

                    if shift.status == ShiftStatus.completed:
                        completed += 1
                    elif shift.status == ShiftStatus.no_show:
                        no_shows += 1
                        noshows, total = day_stats[day_name]
                        day_stats[day_name] = (noshows + 1, total)
                    # TODO: Late arrivals tracking (would need to integrate with check-in system)

        reliability_pct = (completed / total_shifts * 100) if total_shifts > 0 else 0.0

        # Determine trend (compare first 45 days vs last 45 days)
        mid_date = date.today() - timedelta(days=45)
        first_period_rate = self._calculate_period_reliability(
            employee_id, lookback_date, mid_date, all_rosters
        )
        second_period_rate = self._calculate_period_reliability(
            employee_id, mid_date, date.today(), all_rosters
        )

        if second_period_rate > first_period_rate + 5:
            trend = "improving"
        elif second_period_rate < first_period_rate - 5:
            trend = "declining"
        else:
            trend = "stable"

        # Identify risk days (>40% no-show rate)
        risk_days = [
            day for day, (noshows, total) in day_stats.items()
            if total > 0 and (noshows / total) > 0.4
        ]

        # Days since last no-show
        days_since_noshow = 1000
        for roster in all_rosters:
            for shift in roster.shifts:
                if (shift.employee_id == employee_id and
                    shift.status == ShiftStatus.no_show):
                    days_diff = (date.today() - shift.date).days
                    if days_diff >= 0:
                        days_since_noshow = min(days_since_noshow, days_diff)

        if days_since_noshow == 1000:
            days_since_noshow = 999  # Never had a no-show

        return ReliabilityProfile(
            employee_id=employee_id,
            employee_name=employee.name,
            total_shifts=total_shifts,
            completed=completed,
            no_shows=no_shows,
            late_arrivals=late_arrivals,
            reliability_pct=round(reliability_pct, 1),
            trend=trend,
            risk_days=risk_days,
            days_since_last_noshow=days_since_noshow,
        )

    def _calculate_period_reliability(
        self,
        employee_id: str,
        start_date: date,
        end_date: date,
        rosters: List[Roster],
    ) -> float:
        """Calculate reliability percentage for a date range."""
        total = 0
        completed = 0

        for roster in rosters:
            for shift in roster.shifts:
                if (shift.employee_id == employee_id and
                    start_date <= shift.date <= end_date):
                    total += 1
                    if shift.status == ShiftStatus.completed:
                        completed += 1

        return (completed / total * 100) if total > 0 else 0.0

    def record_outcome(self, shift_id: str, outcome: str) -> None:
        """
        Record the actual outcome of a shift (feedback loop for model learning).

        Args:
            shift_id: The shift ID
            outcome: "completed", "no_show", "late_arrival", "cancelled"
        """
        # This would integrate with the database to update shift status
        # and trigger model retraining
        try:
            valid_outcomes = ["completed", "no_show", "late_arrival", "cancelled"]
            if outcome not in valid_outcomes:
                logger.warning(f"Invalid outcome: {outcome}")
                return

            # Find and update the shift
            all_rosters = self.db.list_rosters()
            for roster in all_rosters:
                for shift in roster.shifts:
                    if shift.id == shift_id:
                        # Map outcome to ShiftStatus
                        if outcome == "completed":
                            shift.status = ShiftStatus.completed
                        elif outcome == "no_show":
                            shift.status = ShiftStatus.no_show
                        elif outcome == "cancelled":
                            shift.status = ShiftStatus.cancelled

                        self.db.save_shift(shift)
                        logger.info(
                            f"Recorded outcome for shift {shift_id}: {outcome}"
                        )
                        return

            logger.warning(f"Shift {shift_id} not found")
        except Exception as e:
            logger.error(f"Error recording outcome: {e}")

    def get_venue_risk_summary(
        self, venue_id: str, date_from: str, date_to: str
    ) -> VenueRiskSummary:
        """
        Get aggregate risk summary for a venue over a date range.

        Returns: shifts at risk, total exposure, highest risk date/role, recommendations
        """
        risks = self.predict_risks(venue_id, date_from, date_to)

        if not risks:
            return VenueRiskSummary(
                venue_id=venue_id,
                date_from=date_from,
                date_to=date_to,
                total_shifts_at_risk=0,
                shifts_high_risk=0,
                shifts_critical_risk=0,
                total_risk_exposure=0.0,
                avg_risk_score=0.0,
            )

        high_risk = [r for r in risks if r.risk_level == "high"]
        critical_risk = [r for r in risks if r.risk_level == "critical"]

        total_exposure = sum(r.risk_score for r in risks)
        avg_score = total_exposure / len(risks) if risks else 0.0

        # Find highest risk date
        highest_risk_date = None
        highest_risk_role = None
        max_score = 0.0
        for risk in risks:
            if risk.risk_score > max_score:
                max_score = risk.risk_score
                highest_risk_date = risk.date
                highest_risk_role = risk.role

        # Generate recommendations
        recommendations = []
        if len(critical_risk) > 0:
            recommendations.append(
                f"URGENT: {len(critical_risk)} critical no-show risks. "
                "Immediate confirmation and backup planning required."
            )
        if len(high_risk) > 0:
            recommendations.append(
                f"Monitor {len(high_risk)} high-risk shifts closely. "
                "Confirm with employees 48 hours prior."
            )
        if avg_score > 50:
            recommendations.append(
                "Venue has elevated average risk. Consider temporary staffing agency on standby."
            )

        return VenueRiskSummary(
            venue_id=venue_id,
            date_from=date_from,
            date_to=date_to,
            total_shifts_at_risk=len(risks),
            shifts_high_risk=len(high_risk),
            shifts_critical_risk=len(critical_risk),
            total_risk_exposure=round(total_exposure, 1),
            avg_risk_score=round(avg_score, 1),
            highest_risk_date=highest_risk_date,
            highest_risk_role=highest_risk_role,
            recommendations=recommendations,
        )

    def suggest_backups(self, shift_id: str) -> List[BackupEmployee]:
        """
        Find available backup staff for a high-risk shift.

        Considers:
        - Availability on the shift date/time
        - Skill match with the role
        - Employment flexibility (casuals > part-time)
        - Cost differential
        """
        # Get the shift
        all_rosters = self.db.list_rosters()
        target_shift: Optional[Shift] = None

        target_venue_id = None
        for roster in all_rosters:
            for shift in roster.shifts:
                if shift.id == shift_id:
                    target_shift = shift
                    target_venue_id = getattr(roster, "venue_id", None)
                    break
            if target_shift:
                break

        if not target_shift:
            logger.warning(f"Shift {shift_id} not found")
            return []

        backups: List[BackupEmployee] = []
        # Only this venue's people can cover this venue's shift (never suggest
        # — or reveal — another tenant's staff).
        all_employees = [
            e for e in self.db.list_employees()
            if not target_venue_id or getattr(e, "venue_id", None) == target_venue_id
        ]

        for employee in all_employees:
            if employee.id == target_shift.employee_id:
                continue  # Skip the primary employee

            # Check if available
            available = self._is_employee_available(
                employee, target_shift.date, target_shift.start_time, target_shift.end_time
            )

            if not available:
                continue

            # Check skill match
            skills_match = (
                target_shift.role in employee.skills or
                not employee.skills  # If no skills listed, assume flexible
            )

            # Calculate cost delta (higher for full-time, lower for casuals)
            cost_delta: Optional[Decimal] = None
            if target_shift.cost and employee.hourly_base_rate:
                cost_delta = employee.hourly_base_rate - (
                    target_shift.cost / target_shift.duration_hours
                    if target_shift.duration_hours > 0
                    else Decimal(0)
                )

            backups.append(BackupEmployee(
                employee_id=employee.id,
                name=employee.name,
                availability_match=True,
                cost_delta=cost_delta,
                skills_match=skills_match,
            ))

        # Sort by: skills match (desc), cost delta (asc), casual employment (asc)
        backups.sort(
            key=lambda x: (
                -1 if x.skills_match else 1,
                x.cost_delta if x.cost_delta else Decimal(999),
                -1 if any(
                    emp.id == x.employee_id and emp.employment_type == EmploymentType.casual
                    for emp in all_employees
                ) else 1,
            )
        )

        return backups[:5]  # Return top 5 backups

    def _is_employee_available(
        self,
        employee: Employee,
        shift_date: date,
        shift_start: time,
        shift_end: time,
    ) -> bool:
        """Check if employee is available for the shift time."""
        if not employee.availability:
            return True  # Assume available if no constraints

        day_name = shift_date.strftime("%A").lower()
        if day_name not in employee.availability:
            return False

        # Check if shift falls within availability windows
        availability_windows = employee.availability[day_name]
        shift_minutes_start = shift_start.hour * 60 + shift_start.minute
        shift_minutes_end = shift_end.hour * 60 + shift_end.minute

        for window in availability_windows:
            window_start = int(window.get("start", "00:00").replace(":", "")) // 100 * 60
            window_end = int(window.get("end", "23:59").replace(":", "")) // 100 * 60

            if (window_start <= shift_minutes_start and
                shift_minutes_end <= window_end):
                return True

        return False
