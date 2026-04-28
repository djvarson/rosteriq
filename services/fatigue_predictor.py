"""
Staff fatigue predictor for RosterIQ.

Analyzes roster patterns to predict burnout risk based on:
- Consecutive days worked
- Weekly hour trends
- Hours vs contract limits
- Late-to-early turnarounds (clopenings)
- Weekend ratio
- Overtime hours
- Leave balance
- Shift variety

Provides individualized assessments and team-wide fatigue reports.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, time
from typing import Optional, List, Dict, Tuple
import logging

from rosteriq.database import get_db
from rosteriq.models import Employee, Shift, Roster, ShiftStatus, EmploymentType

logger = logging.getLogger(__name__)


@dataclass
class FatigueAssessment:
    """Individual employee fatigue assessment."""
    employee_id: str
    employee_name: str
    overall_score: float
    risk_level: str
    factor_scores: Dict[str, float]
    consecutive_days_current: int
    avg_weekly_hours_4w: float
    clopening_count_4w: int
    weekend_ratio: float
    overtime_hours_4w: float
    days_since_last_leave: int
    recommendations: List[str] = field(default_factory=list)
    trend: str = "stable"  # "improving", "stable", "worsening"
    assessed_at: datetime = field(default_factory=datetime.now)


@dataclass
class TeamFatigueReport:
    """Team-wide fatigue report for a venue."""
    venue_id: str
    assessed_count: int
    avg_score: float
    high_risk_count: int
    employees: List[FatigueAssessment] = field(default_factory=list)
    team_recommendations: List[str] = field(default_factory=list)
    assessed_at: datetime = field(default_factory=datetime.now)


class FatiguePredictor:
    """Predicts staff burnout risk by analyzing roster patterns."""

    def __init__(self):
        """Initialize the fatigue predictor."""
        self.db = get_db()
        # Factor weights must sum to 1.0
        self.weights = {
            "consecutive_days": 0.20,
            "weekly_hours_trend": 0.20,
            "hours_vs_contract": 0.15,
            "clopening": 0.15,
            "weekend_ratio": 0.10,
            "overtime_ratio": 0.10,
            "leave_balance": 0.05,
            "shift_variety": 0.05,
        }

    def assess_fatigue(
        self, employee_id: str, lookback_weeks: int = 4
    ) -> Optional[FatigueAssessment]:
        """
        Assess fatigue and burnout risk for an employee.

        Args:
            employee_id: Employee ID to assess
            lookback_weeks: Number of weeks to analyze (default 4)

        Returns:
            FatigueAssessment with scores and recommendations, or None if no data
        """
        employee = self.db.get_employee(employee_id)
        if not employee:
            logger.warning(f"Employee {employee_id} not found")
            return None

        # Get shifts for lookback period
        end_date = date.today()
        start_date = end_date - timedelta(weeks=lookback_weeks)
        shifts = self._get_employee_shifts(employee_id, start_date, end_date)

        if not shifts:
            logger.warning(
                f"No shifts found for {employee_id} in last {lookback_weeks} weeks"
            )
            return None

        # Calculate factor scores
        factor_scores = {}

        # 1. Consecutive days worked (0-100)
        consecutive_days_current = self._get_consecutive_days(shifts)
        factor_scores["consecutive_days"] = self._score_consecutive_days(
            consecutive_days_current
        )

        # 2. Weekly hours trend (0-100)
        weekly_hours = self._calculate_weekly_hours(shifts, start_date, end_date)
        avg_weekly_hours_4w = sum(weekly_hours) / len(weekly_hours) if weekly_hours else 0
        factor_scores["weekly_hours_trend"] = self._score_hours_trend(weekly_hours)

        # 3. Hours vs contract (0-100)
        factor_scores["hours_vs_contract"] = self._score_hours_vs_contract(
            avg_weekly_hours_4w, employee.max_hours_per_week
        )

        # 4. Late-to-early turnarounds / clopenings (0-100)
        clopening_count_4w = len(self._get_clopening_shifts(employee_id, lookback_weeks))
        factor_scores["clopening"] = self._score_clopenings(clopening_count_4w)

        # 5. Weekend ratio (0-100)
        weekend_ratio = self._calculate_weekend_ratio(shifts)
        factor_scores["weekend_ratio"] = self._score_weekend_ratio(weekend_ratio)

        # 6. Overtime ratio (0-100)
        overtime_hours_4w = self._calculate_overtime_hours(
            shifts, employee.max_hours_per_week
        )
        overtime_ratio = (
            overtime_hours_4w / (avg_weekly_hours_4w * lookback_weeks)
            if avg_weekly_hours_4w > 0
            else 0
        )
        factor_scores["overtime_ratio"] = self._score_overtime_ratio(overtime_ratio)

        # 7. Leave balance (0-100)
        days_since_last_leave = self._get_days_since_last_leave(employee_id, start_date)
        factor_scores["leave_balance"] = self._score_leave_balance(days_since_last_leave)

        # 8. Shift variety (0-100)
        factor_scores["shift_variety"] = self._score_shift_variety(shifts)

        # Calculate weighted overall score
        overall_score = sum(
            factor_scores.get(factor, 0) * weight
            for factor, weight in self.weights.items()
        )

        # Determine risk level
        risk_level = self._determine_risk_level(overall_score)

        # Calculate trend
        trend = self._calculate_trend(employee_id, start_date)

        # Generate recommendations
        recommendations = self._generate_recommendations(
            factor_scores, consecutive_days_current, weekend_ratio, overtime_ratio
        )

        return FatigueAssessment(
            employee_id=employee_id,
            employee_name=employee.name,
            overall_score=round(overall_score, 1),
            risk_level=risk_level,
            factor_scores={k: round(v, 1) for k, v in factor_scores.items()},
            consecutive_days_current=consecutive_days_current,
            avg_weekly_hours_4w=round(avg_weekly_hours_4w, 1),
            clopening_count_4w=clopening_count_4w,
            weekend_ratio=round(weekend_ratio, 2),
            overtime_hours_4w=round(overtime_hours_4w, 1),
            days_since_last_leave=days_since_last_leave,
            recommendations=recommendations,
            trend=trend,
        )

    def assess_team(self, venue_id: str) -> Optional[TeamFatigueReport]:
        """
        Assess fatigue for all employees at a venue.

        Args:
            venue_id: Venue ID to assess

        Returns:
            TeamFatigueReport with all employees sorted by risk
        """
        employees = self.db.list_employees()
        # Filter to employees who have shifts at this venue
        venue_employees = [
            e for e in employees
            if self._has_shifts_at_venue(e.id, venue_id)
        ]

        if not venue_employees:
            logger.warning(f"No employees found for venue {venue_id}")
            return None

        assessments = []
        total_score = 0

        for emp in venue_employees:
            assessment = self.assess_fatigue(emp.id)
            if assessment:
                assessments.append(assessment)
                total_score += assessment.overall_score

        # Sort by risk score descending
        assessments.sort(key=lambda x: x.overall_score, reverse=True)

        avg_score = total_score / len(assessments) if assessments else 0
        high_risk_count = sum(1 for a in assessments if a.risk_level in ["orange", "red"])

        # Generate team-wide recommendations
        team_recommendations = self._generate_team_recommendations(assessments)

        return TeamFatigueReport(
            venue_id=venue_id,
            assessed_count=len(assessments),
            avg_score=round(avg_score, 1),
            high_risk_count=high_risk_count,
            employees=assessments,
            team_recommendations=team_recommendations,
        )

    def get_clopening_shifts(
        self, employee_id: str, weeks: int = 4
    ) -> List[Tuple[Shift, Shift]]:
        """
        Get all late-to-early shift pairs (clopenings).

        A clopening is when an employee works a late shift (e.g., closing at 10pm)
        and then works an early shift the next day (e.g., opening at 6am),
        with less than 10 hours between end and start.

        Args:
            employee_id: Employee ID to analyze
            weeks: Number of weeks to look back

        Returns:
            List of (late_shift, early_shift) tuples
        """
        return self._get_clopening_shifts(employee_id, weeks)

    def predict_burnout_date(self, employee_id: str) -> Optional[date]:
        """
        Predict when an employee might reach critical burnout.

        If trend is worsening, extrapolate fatigue score to estimate
        when it will reach critical (71+) threshold.

        Args:
            employee_id: Employee ID to predict for

        Returns:
            Estimated date of critical burnout, or None if trend is not worsening
        """
        assessment = self.assess_fatigue(employee_id, lookback_weeks=4)
        if not assessment or assessment.trend != "worsening":
            return None

        # Simple linear extrapolation
        # Get scores from 4 weeks and 2 weeks ago
        today = date.today()
        score_4w_ago = self._get_fatigue_score_at_date(employee_id, today - timedelta(weeks=4))
        score_2w_ago = self._get_fatigue_score_at_date(employee_id, today - timedelta(weeks=2))
        score_now = assessment.overall_score

        if not score_4w_ago or not score_2w_ago:
            return None

        # Calculate weekly increase rate
        increase_per_2w = score_2w_ago - score_4w_ago
        if increase_per_2w <= 0:
            return None

        # Project to critical threshold (71)
        critical_threshold = 71
        weeks_to_critical = (critical_threshold - score_now) / (increase_per_2w / 2)

        if weeks_to_critical <= 0:
            # Already critical or was critical
            return today

        return today + timedelta(weeks=weeks_to_critical)

    def suggest_recovery_roster(self, employee_id: str) -> Dict:
        """
        Suggest an ideal recovery roster for next week.

        Recommends:
        - Reduced hours (20-25% less than usual)
        - No closing shifts
        - At least one full day off
        - Spread shifts to avoid back-to-backs

        Args:
            employee_id: Employee ID to suggest for

        Returns:
            Dictionary with recovery roster suggestions
        """
        employee = self.db.get_employee(employee_id)
        if not employee:
            return {}

        assessment = self.assess_fatigue(employee_id)
        if not assessment:
            return {}

        # Calculate recommended hours
        recommended_hours = assessment.avg_weekly_hours_4w * 0.75  # 25% reduction
        recommended_hours = min(recommended_hours, employee.max_hours_per_week)

        suggestions = {
            "employee_id": employee_id,
            "recommended_total_hours": round(recommended_hours, 1),
            "recommended_shifts": 4,  # Fewer, longer shifts
            "avoid_patterns": [
                "No closing shifts (end after 9pm)",
                "No shifts before 8am",
                "No consecutive days exceeding 3",
                "Minimum 11 hours between shifts"
            ],
            "days_off": 3,  # More days off than usual
            "ideal_shift_timing": "9am-5pm or 10am-6pm",
            "reason": f"Employee at {assessment.risk_level} risk level",
        }

        return suggestions

    # ========================================================================
    # Private helper methods
    # ========================================================================

    def _get_employee_shifts(
        self, employee_id: str, start_date: date, end_date: date
    ) -> List[Shift]:
        """Get all shifts for an employee in a date range."""
        all_shifts = self.db.list_shifts()
        filtered = [
            s for s in all_shifts
            if s.employee_id == employee_id
            and start_date <= s.date <= end_date
            and s.status in [ShiftStatus.completed, ShiftStatus.confirmed, ShiftStatus.scheduled]
        ]
        return sorted(filtered, key=lambda s: s.date)

    def _get_consecutive_days(self, shifts: List[Shift]) -> int:
        """Calculate current consecutive days worked."""
        if not shifts:
            return 0

        today = date.today()
        consecutive = 0
        current_date = today

        while True:
            # Check if there's a shift on current_date
            has_shift = any(s.date == current_date for s in shifts)
            if not has_shift:
                break
            consecutive += 1
            current_date -= timedelta(days=1)

            if current_date < min(s.date for s in shifts):
                break

        return consecutive

    def _calculate_weekly_hours(
        self, shifts: List[Shift], start_date: date, end_date: date
    ) -> List[float]:
        """Calculate hours per week for the lookback period."""
        weekly_hours = []
        current = start_date

        while current <= end_date:
            week_end = current + timedelta(days=6)
            week_hours = sum(
                s.net_hours
                for s in shifts
                if current <= s.date <= week_end
            )
            weekly_hours.append(week_hours)
            current = week_end + timedelta(days=1)

        return weekly_hours

    def _score_consecutive_days(self, consecutive_days: int) -> float:
        """Score consecutive days worked. >5 = high, 6+ = critical."""
        if consecutive_days <= 3:
            return 10.0
        elif consecutive_days == 4:
            return 30.0
        elif consecutive_days == 5:
            return 50.0
        elif consecutive_days == 6:
            return 80.0
        else:
            return 100.0

    def _score_hours_trend(self, weekly_hours: List[float]) -> float:
        """Score weekly hours trend. Increasing = risk."""
        if len(weekly_hours) < 2:
            return 50.0

        # Check if trend is increasing
        increases = sum(
            1 for i in range(1, len(weekly_hours))
            if weekly_hours[i] > weekly_hours[i - 1]
        )

        # Percentage of weeks with increases
        increase_ratio = increases / (len(weekly_hours) - 1)

        # Score: 0 (decreasing) to 100 (consistently increasing)
        return increase_ratio * 100.0

    def _score_hours_vs_contract(
        self, avg_hours: float, max_hours: float
    ) -> float:
        """Score hours vs contract. Near max = risk."""
        if max_hours == 0:
            return 50.0

        ratio = avg_hours / max_hours

        if ratio < 0.7:
            return 10.0
        elif ratio < 0.85:
            return 30.0
        elif ratio < 0.95:
            return 50.0
        elif ratio < 1.05:
            return 70.0
        else:
            return 100.0

    def _score_clopenings(self, clopening_count: int) -> float:
        """Score clopenings. Each is a fatigue risk."""
        # 0 clopenings = low risk, 5+ = critical
        if clopening_count == 0:
            return 10.0
        elif clopening_count == 1:
            return 30.0
        elif clopening_count == 2:
            return 50.0
        elif clopening_count == 3:
            return 70.0
        else:
            return min(100.0, 70.0 + (clopening_count - 3) * 10.0)

    def _score_weekend_ratio(self, weekend_ratio: float) -> float:
        """Score weekend work. >60% = fatigue risk."""
        if weekend_ratio < 0.3:
            return 10.0
        elif weekend_ratio < 0.5:
            return 30.0
        elif weekend_ratio < 0.6:
            return 50.0
        elif weekend_ratio < 0.75:
            return 70.0
        else:
            return 100.0

    def _score_overtime_ratio(self, overtime_ratio: float) -> float:
        """Score overtime. >10% = risk."""
        if overtime_ratio < 0.05:
            return 10.0
        elif overtime_ratio < 0.10:
            return 30.0
        elif overtime_ratio < 0.15:
            return 50.0
        elif overtime_ratio < 0.20:
            return 70.0
        else:
            return 100.0

    def _score_leave_balance(self, days_since_last_leave: int) -> float:
        """Score leave balance. Long gaps = risk."""
        if days_since_last_leave < 30:
            return 10.0
        elif days_since_last_leave < 60:
            return 30.0
        elif days_since_last_leave < 90:
            return 50.0
        elif days_since_last_leave < 120:
            return 70.0
        else:
            return 100.0

    def _score_shift_variety(self, shifts: List[Shift]) -> float:
        """Score shift variety. Same pattern every week = monotony risk."""
        if not shifts:
            return 50.0

        # Group shifts by day of week
        dow_count = {}
        for shift in shifts:
            dow = shift.date.weekday()
            dow_count[dow] = dow_count.get(dow, 0) + 1

        # Calculate entropy (variety)
        total = len(shifts)
        if total == 0:
            return 50.0

        # Calculate how uniform the distribution is
        max_entropy = 7.0  # log2(7 days)
        entropy = 0.0
        for count in dow_count.values():
            if count > 0:
                p = count / total
                entropy -= p * (p ** 0.5)  # Simplified entropy

        # Score: higher entropy = more variety = lower risk
        variety_score = (entropy / max_entropy) * 100 if max_entropy > 0 else 50.0
        return min(100.0, max(10.0, variety_score))

    def _calculate_weekend_ratio(self, shifts: List[Shift]) -> float:
        """Calculate ratio of weekend shifts."""
        if not shifts:
            return 0.0

        weekend_shifts = sum(
            1 for s in shifts
            if s.date.weekday() >= 5  # Saturday=5, Sunday=6
        )

        return weekend_shifts / len(shifts)

    def _calculate_overtime_hours(
        self, shifts: List[Shift], max_hours_per_week: float
    ) -> float:
        """Calculate total overtime hours in lookback period."""
        weekly_hours = {}

        for shift in shifts:
            week_key = shift.date.isocalendar()[1]
            if week_key not in weekly_hours:
                weekly_hours[week_key] = 0.0
            weekly_hours[week_key] += shift.net_hours

        overtime = 0.0
        for week_hours in weekly_hours.values():
            if week_hours > max_hours_per_week:
                overtime += week_hours - max_hours_per_week

        return overtime

    def _get_clopening_shifts(
        self, employee_id: str, weeks: int = 4
    ) -> List[Tuple[Shift, Shift]]:
        """Get all clopening shift pairs for an employee."""
        end_date = date.today()
        start_date = end_date - timedelta(weeks=weeks)
        shifts = self._get_employee_shifts(employee_id, start_date, end_date)
        shifts.sort(key=lambda s: s.date)

        clopenings = []
        for i in range(len(shifts) - 1):
            current = shifts[i]
            next_shift = shifts[i + 1]

            # Check if consecutive days and late->early
            if next_shift.date == current.date + timedelta(days=1):
                # Calculate gap between end of current and start of next
                current_end = datetime.combine(
                    current.date, current.end_time
                )
                next_start = datetime.combine(
                    next_shift.date, next_shift.start_time
                )

                gap_hours = (next_start - current_end).total_seconds() / 3600.0

                # Clopening if <10 hours between shifts
                if gap_hours < 10:
                    clopenings.append((current, next_shift))

        return clopenings

    def _get_days_since_last_leave(
        self, employee_id: str, start_date: date
    ) -> int:
        """Get days since employee last took leave."""
        # This would integrate with leave management system
        # For now, return a placeholder calculation
        today = date.today()
        return (today - start_date).days

    def _determine_risk_level(self, score: float) -> str:
        """Determine risk level from score."""
        if score <= 30:
            return "green"
        elif score <= 50:
            return "yellow"
        elif score <= 70:
            return "orange"
        else:
            return "red"

    def _calculate_trend(self, employee_id: str, start_date: date) -> str:
        """Calculate if fatigue trend is improving, stable, or worsening."""
        score_4w = self._get_fatigue_score_at_date(
            employee_id, start_date
        )
        score_now = self.assess_fatigue(employee_id, lookback_weeks=2)

        if not score_4w or not score_now:
            return "stable"

        diff = score_now.overall_score - score_4w
        if diff < -5:
            return "improving"
        elif diff > 5:
            return "worsening"
        else:
            return "stable"

    def _get_fatigue_score_at_date(self, employee_id: str, on_date: date) -> Optional[float]:
        """Get historical fatigue score for a specific date."""
        # This would require historical data storage
        # Placeholder: return None for now
        return None

    def _generate_recommendations(
        self,
        factor_scores: Dict[str, float],
        consecutive_days: int,
        weekend_ratio: float,
        overtime_ratio: float,
    ) -> List[str]:
        """Generate actionable recommendations based on factors."""
        recommendations = []

        if consecutive_days >= 5:
            recommendations.append(
                f"Schedule a day off soon - currently {consecutive_days} consecutive days"
            )

        if factor_scores.get("weekly_hours_trend", 0) > 60:
            recommendations.append("Hours are increasing - consider reducing next week")

        if factor_scores.get("hours_vs_contract", 0) > 70:
            recommendations.append("Working near or above contract maximum - reduce scope")

        if factor_scores.get("clopening", 0) > 60:
            recommendations.append(
                "Reduce late-to-early turnarounds - add 2+ hour buffer minimum"
            )

        if weekend_ratio > 0.6:
            recommendations.append("Over 60% weekend shifts - provide more weekday work")

        if overtime_ratio > 0.15:
            recommendations.append("Significant overtime - redistribute across more staff")

        if not recommendations:
            recommendations.append("Fatigue level is healthy - maintain current schedule")

        return recommendations

    def _generate_team_recommendations(self, assessments: List[FatigueAssessment]) -> List[str]:
        """Generate team-wide recommendations."""
        recommendations = []

        high_risk = sum(1 for a in assessments if a.risk_level in ["orange", "red"])
        if high_risk > len(assessments) * 0.25:
            recommendations.append(
                f"{high_risk} staff members at elevated fatigue risk - review scheduling"
            )

        avg_score = sum(a.overall_score for a in assessments) / len(assessments) if assessments else 0
        if avg_score > 50:
            recommendations.append(
                "Team average fatigue score is elevated - increase staffing levels"
            )

        worsening = sum(1 for a in assessments if a.trend == "worsening")
        if worsening > 0:
            recommendations.append(
                f"{worsening} staff showing worsening trends - prioritize recovery time"
            )

        if not recommendations:
            recommendations.append("Team fatigue levels are healthy")

        return recommendations

    def _has_shifts_at_venue(self, employee_id: str, venue_id: str) -> bool:
        """Check if employee has any shifts at a venue."""
        shifts = self.db.list_shifts()
        for shift in shifts:
            if shift.employee_id == employee_id:
                roster = self.db.get_roster(shift.id.split("_")[0])
                if roster and roster.venue_id == venue_id:
                    return True
        return False
