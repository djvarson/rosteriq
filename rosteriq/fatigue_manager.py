"""Fatigue Management & Safe-Hours Tracker for Australian hospitality.

Implements AU WHS duty of care for fatigue prevention. Common hospitality rules:
- Max weekly hours: 38 ordinary + reasonable overtime (typically cap at 50-55 hours)
- Max consecutive days: 6 days without a day off (some EBAs allow 7 with mutual agreement)
- Minimum weekly rest: 24 consecutive hours off per 7-day period
- Cumulative fatigue: risk increases after 5+ consecutive days or 48+ hours in a week
- Night shift fatigue: shifts ending after midnight carry higher fatigue weight (1.5x)

Data persisted to SQLite for assessments and alerts.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("rosteriq.fatigue_manager")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class FatigueRiskLevel(str, Enum):
    """Fatigue risk levels."""
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class FatigueRule:
    """Definition of a fatigue management rule."""
    rule_id: str
    name: str
    description: str
    max_value: float
    unit: str  # "hours_per_week", "consecutive_days", "hours_per_fortnight", "min_rest_hours"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "description": self.description,
            "max_value": self.max_value,
            "unit": self.unit,
        }


@dataclass
class FatigueAssessment:
    """Comprehensive fatigue risk assessment for an employee."""
    employee_id: str
    employee_name: str
    venue_id: str
    assessment_date: date
    risk_level: FatigueRiskLevel
    weekly_hours: float
    fortnightly_hours: float
    consecutive_days: int
    last_day_off: Optional[date]
    night_shift_count: int
    violations: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    score: int = 0  # 0-100, higher = more fatigued

    def to_dict(self) -> Dict[str, Any]:
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "assessment_date": self.assessment_date.isoformat(),
            "risk_level": self.risk_level.value,
            "weekly_hours": self.weekly_hours,
            "fortnightly_hours": self.fortnightly_hours,
            "consecutive_days": self.consecutive_days,
            "last_day_off": self.last_day_off.isoformat() if self.last_day_off else None,
            "night_shift_count": self.night_shift_count,
            "violations": self.violations,
            "recommendations": self.recommendations,
            "score": self.score,
        }


@dataclass
class FatigueAlert:
    """Fatigue alert triggered by risk level threshold."""
    alert_id: str
    employee_id: str
    employee_name: str
    venue_id: str
    risk_level: FatigueRiskLevel
    trigger: str  # e.g., "exceeded_weekly_hours", "6_consecutive_days_worked"
    hours_worked: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    acknowledged_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "venue_id": self.venue_id,
            "risk_level": self.risk_level.value,
            "trigger": self.trigger,
            "hours_worked": self.hours_worked,
            "created_at": self.created_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
        }


# ---------------------------------------------------------------------------
# Default Rules
# ---------------------------------------------------------------------------

DEFAULT_RULES = {
    "max_weekly_hours": FatigueRule(
        rule_id="max_weekly_hours",
        name="Maximum Weekly Hours",
        description="Maximum hours worked per week (38 ordinary + reasonable overtime)",
        max_value=50.0,
        unit="hours_per_week",
    ),
    "max_fortnightly_hours": FatigueRule(
        rule_id="max_fortnightly_hours",
        name="Maximum Fortnightly Hours",
        description="Maximum hours worked per fortnight (2-week period)",
        max_value=95.0,
        unit="hours_per_fortnight",
    ),
    "max_consecutive_days": FatigueRule(
        rule_id="max_consecutive_days",
        name="Maximum Consecutive Days",
        description="Maximum consecutive days worked without a day off",
        max_value=6.0,
        unit="consecutive_days",
    ),
    "min_weekly_rest_hours": FatigueRule(
        rule_id="min_weekly_rest_hours",
        name="Minimum Weekly Rest",
        description="Minimum consecutive hours off per 7-day period",
        max_value=24.0,
        unit="min_rest_hours",
    ),
}


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class FatigueStore:
    """Thread-safe in-memory store with SQLite persistence for fatigue data."""

    def __init__(self):
        self.assessments: Dict[str, Dict[str, FatigueAssessment]] = {}  # {venue_id: {employee_id: assessment}}
        self.alerts: List[FatigueAlert] = []
        self._lock = threading.Lock()
        self._register_schema()
        self._rehydrate()

    @staticmethod
    def _register_schema() -> None:
        """Register SQLite schema for fatigue data."""
        try:
            from rosteriq import persistence as _p
            _p.register_schema(
                "fatigue_assessments",
                """
                CREATE TABLE IF NOT EXISTS fatigue_assessments (
                    id TEXT PRIMARY KEY,
                    employee_id TEXT NOT NULL,
                    employee_name TEXT NOT NULL,
                    venue_id TEXT NOT NULL,
                    assessment_date TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    weekly_hours REAL NOT NULL,
                    fortnightly_hours REAL NOT NULL,
                    consecutive_days INTEGER NOT NULL,
                    last_day_off TEXT,
                    night_shift_count INTEGER NOT NULL,
                    violations TEXT,
                    recommendations TEXT,
                    score INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(venue_id, employee_id, assessment_date)
                )
                """,
            )
            _p.register_schema(
                "fatigue_alerts",
                """
                CREATE TABLE IF NOT EXISTS fatigue_alerts (
                    alert_id TEXT PRIMARY KEY,
                    employee_id TEXT NOT NULL,
                    employee_name TEXT NOT NULL,
                    venue_id TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    hours_worked REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    acknowledged_at TEXT,
                    UNIQUE(venue_id, employee_id, trigger, created_at)
                )
                """,
            )
            _p.on_init(FatigueStore._rehydrate_all)
        except ImportError:
            pass

    @staticmethod
    def _rehydrate_all() -> None:
        """Rehydrate store from SQLite at startup."""
        try:
            from rosteriq import persistence as _p
            rows = _p.fetchall("SELECT * FROM fatigue_assessments")
            for row in rows:
                assessment = FatigueAssessment(
                    employee_id=row["employee_id"],
                    employee_name=row["employee_name"],
                    venue_id=row["venue_id"],
                    assessment_date=datetime.fromisoformat(row["assessment_date"]).date(),
                    risk_level=FatigueRiskLevel(row["risk_level"]),
                    weekly_hours=row["weekly_hours"],
                    fortnightly_hours=row["fortnightly_hours"],
                    consecutive_days=row["consecutive_days"],
                    last_day_off=datetime.fromisoformat(row["last_day_off"]).date() if row["last_day_off"] else None,
                    night_shift_count=row["night_shift_count"],
                    violations=_p.json_loads(row["violations"], default=[]),
                    recommendations=_p.json_loads(row["recommendations"], default=[]),
                    score=row["score"],
                )
                if assessment.venue_id not in _store.assessments:
                    _store.assessments[assessment.venue_id] = {}
                _store.assessments[assessment.venue_id][assessment.employee_id] = assessment

            rows = _p.fetchall("SELECT * FROM fatigue_alerts")
            for row in rows:
                alert = FatigueAlert(
                    alert_id=row["alert_id"],
                    employee_id=row["employee_id"],
                    employee_name=row["employee_name"],
                    venue_id=row["venue_id"],
                    risk_level=FatigueRiskLevel(row["risk_level"]),
                    trigger=row["trigger"],
                    hours_worked=row["hours_worked"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    acknowledged_at=datetime.fromisoformat(row["acknowledged_at"]) if row["acknowledged_at"] else None,
                )
                _store.alerts.append(alert)
        except ImportError:
            pass

    def _rehydrate(self) -> None:
        """Instance rehydration (called from __init__)."""
        FatigueStore._rehydrate_all()

    def store_assessment(self, assessment: FatigueAssessment) -> None:
        """Store an assessment in memory and persist to SQLite."""
        with self._lock:
            if assessment.venue_id not in self.assessments:
                self.assessments[assessment.venue_id] = {}
            self.assessments[assessment.venue_id][assessment.employee_id] = assessment

        # Persist to SQLite
        try:
            from rosteriq import persistence as _p
            row = {
                "id": f"{assessment.venue_id}_{assessment.employee_id}_{assessment.assessment_date.isoformat()}",
                "employee_id": assessment.employee_id,
                "employee_name": assessment.employee_name,
                "venue_id": assessment.venue_id,
                "assessment_date": assessment.assessment_date.isoformat(),
                "risk_level": assessment.risk_level.value,
                "weekly_hours": assessment.weekly_hours,
                "fortnightly_hours": assessment.fortnightly_hours,
                "consecutive_days": assessment.consecutive_days,
                "last_day_off": assessment.last_day_off.isoformat() if assessment.last_day_off else None,
                "night_shift_count": assessment.night_shift_count,
                "violations": _p.json_dumps(assessment.violations),
                "recommendations": _p.json_dumps(assessment.recommendations),
                "score": assessment.score,
                "created_at": _p.now_iso(),
            }
            _p.upsert("fatigue_assessments", row, pk="id")
        except ImportError:
            pass

    def store_alert(self, alert: FatigueAlert) -> None:
        """Store an alert in memory and persist to SQLite."""
        with self._lock:
            self.alerts.append(alert)

        # Persist to SQLite
        try:
            from rosteriq import persistence as _p
            row = {
                "alert_id": alert.alert_id,
                "employee_id": alert.employee_id,
                "employee_name": alert.employee_name,
                "venue_id": alert.venue_id,
                "risk_level": alert.risk_level.value,
                "trigger": alert.trigger,
                "hours_worked": alert.hours_worked,
                "created_at": alert.created_at.isoformat(),
                "acknowledged_at": alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
            }
            _p.upsert("fatigue_alerts", row, pk="alert_id")
        except ImportError:
            pass

    def get_assessment(self, venue_id: str, employee_id: str) -> Optional[FatigueAssessment]:
        """Get latest assessment for an employee."""
        with self._lock:
            if venue_id in self.assessments:
                return self.assessments[venue_id].get(employee_id)
        return None

    def get_venue_assessments(self, venue_id: str) -> List[FatigueAssessment]:
        """Get all assessments for a venue."""
        with self._lock:
            return list(self.assessments.get(venue_id, {}).values())

    def get_alerts(self, venue_id: str, risk_level: Optional[FatigueRiskLevel] = None) -> List[FatigueAlert]:
        """Get alerts for a venue, optionally filtered by risk level."""
        with self._lock:
            alerts = [a for a in self.alerts if a.venue_id == venue_id]
            if risk_level:
                alerts = [a for a in alerts if a.risk_level == risk_level]
            return alerts


# Global store instance
_store = FatigueStore()


# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------


def calculate_fatigue_score(
    weekly_hours: float,
    consecutive_days: int,
    night_shifts: int,
    last_day_off_days_ago: Optional[int],
) -> int:
    """Calculate fatigue score (0-100, higher = more fatigued).

    Factors:
    - Weekly hours: 50+ is HIGH risk
    - Consecutive days: 6+ is HIGH risk
    - Night shifts: weighted at 1.5x
    - Rest recency: fewer days since last day off = higher fatigue
    """
    score = 0

    # Weekly hours: max 50 = 0 points, >50 = penalty
    if weekly_hours > 50:
        score += min(30, (weekly_hours - 50) * 6)

    # Consecutive days: max 6 = 0 points, 7+ = penalty
    if consecutive_days > 6:
        score += min(30, (consecutive_days - 6) * 8)

    # Night shifts: each counts as 1.5x, so add premium
    score += min(20, night_shifts * 3)

    # Rest recency: if no rest in 7 days, full penalty
    if last_day_off_days_ago is not None:
        if last_day_off_days_ago > 7:
            score += 25
        elif last_day_off_days_ago > 5:
            score += 15

    # Fortnightly accumulation: working close to 95-hour limit
    if weekly_hours > 48:  # 2 weeks at 48+ = 96+
        score += 20

    return min(100, score)


def classify_risk(score: int) -> FatigueRiskLevel:
    """Classify fatigue risk from score.

    - LOW: 0-29
    - MODERATE: 30-49
    - HIGH: 50-74
    - CRITICAL: 75+
    """
    if score >= 75:
        return FatigueRiskLevel.CRITICAL
    elif score >= 50:
        return FatigueRiskLevel.HIGH
    elif score >= 30:
        return FatigueRiskLevel.MODERATE
    else:
        return FatigueRiskLevel.LOW


def assess_fatigue(
    employee_id: str,
    employee_name: str,
    venue_id: str,
    shifts_7_days: List[Dict[str, Any]],
    shifts_14_days: List[Dict[str, Any]],
    rules: Optional[Dict[str, FatigueRule]] = None,
) -> FatigueAssessment:
    """Comprehensive fatigue risk assessment for an employee.

    Args:
        employee_id: Unique employee identifier
        employee_name: Employee's name
        venue_id: Venue identifier
        shifts_7_days: List of shifts in last 7 days {date: date, start: "HH:MM", end: "HH:MM"}
        shifts_14_days: List of shifts in last 14 days
        rules: Fatigue rules dict (defaults to DEFAULT_RULES)

    Returns:
        FatigueAssessment with risk level, violations, recommendations, and score
    """
    if rules is None:
        rules = DEFAULT_RULES

    assessment_date = date.today()
    violations = []
    recommendations = []

    # Calculate weekly hours (last 7 days)
    weekly_hours = sum(_shift_duration(s) for s in shifts_7_days)

    # Calculate fortnightly hours (last 14 days)
    fortnightly_hours = sum(_shift_duration(s) for s in shifts_14_days)

    # Count consecutive days worked
    consecutive_days = _count_consecutive_days(shifts_7_days)

    # Find last day off
    last_day_off = _find_last_day_off(shifts_14_days)
    last_day_off_days_ago = (assessment_date - last_day_off).days if last_day_off else None

    # Count night shifts (ending after midnight)
    night_shift_count = sum(1 for s in shifts_7_days if _is_night_shift(s))

    # Check violations
    if weekly_hours > rules["max_weekly_hours"].max_value:
        violations.append(
            f"Exceeded max weekly hours: {weekly_hours:.1f}h > {rules['max_weekly_hours'].max_value}h"
        )

    if fortnightly_hours > rules["max_fortnightly_hours"].max_value:
        violations.append(
            f"Exceeded max fortnightly hours: {fortnightly_hours:.1f}h > {rules['max_fortnightly_hours'].max_value}h"
        )

    if consecutive_days > int(rules["max_consecutive_days"].max_value):
        violations.append(
            f"Exceeded max consecutive days: {consecutive_days}d > {int(rules['max_consecutive_days'].max_value)}d"
        )

    if last_day_off_days_ago and last_day_off_days_ago > int(rules["min_weekly_rest_hours"].max_value / 24):
        violations.append(
            f"No day off in {last_day_off_days_ago} days (should have 24h off per 7 days)"
        )

    # Calculate fatigue score
    score = calculate_fatigue_score(weekly_hours, consecutive_days, night_shift_count, last_day_off_days_ago)

    # Classify risk
    risk_level = classify_risk(score)

    # Generate recommendations
    recommendations = generate_recommendations(
        FatigueAssessment(
            employee_id=employee_id,
            employee_name=employee_name,
            venue_id=venue_id,
            assessment_date=assessment_date,
            risk_level=risk_level,
            weekly_hours=weekly_hours,
            fortnightly_hours=fortnightly_hours,
            consecutive_days=consecutive_days,
            last_day_off=last_day_off,
            night_shift_count=night_shift_count,
            violations=violations,
            recommendations=[],
            score=score,
        )
    )

    assessment = FatigueAssessment(
        employee_id=employee_id,
        employee_name=employee_name,
        venue_id=venue_id,
        assessment_date=assessment_date,
        risk_level=risk_level,
        weekly_hours=weekly_hours,
        fortnightly_hours=fortnightly_hours,
        consecutive_days=consecutive_days,
        last_day_off=last_day_off,
        night_shift_count=night_shift_count,
        violations=violations,
        recommendations=recommendations,
        score=score,
    )

    # Store assessment
    _store.store_assessment(assessment)

    # Create alert if risk is HIGH or CRITICAL
    if risk_level in (FatigueRiskLevel.HIGH, FatigueRiskLevel.CRITICAL):
        trigger = _determine_trigger(assessment)
        alert = FatigueAlert(
            alert_id=str(uuid.uuid4()),
            employee_id=employee_id,
            employee_name=employee_name,
            venue_id=venue_id,
            risk_level=risk_level,
            trigger=trigger,
            hours_worked=weekly_hours,
        )
        _store.store_alert(alert)

    return assessment


def check_roster_fatigue(
    venue_id: str,
    all_shifts_by_employee: Dict[str, List[Dict[str, Any]]],
    rules: Optional[Dict[str, FatigueRule]] = None,
) -> List[FatigueAssessment]:
    """Bulk fatigue check for entire roster.

    Args:
        venue_id: Venue identifier
        all_shifts_by_employee: {employee_id: [shifts]}
        rules: Fatigue rules

    Returns:
        List of FatigueAssessment for all employees
    """
    assessments = []

    for employee_id, shifts in all_shifts_by_employee.items():
        if not shifts:
            continue

        # Get employee name from first shift or use ID
        employee_name = shifts[0].get("employee_name", employee_id)

        # Split into 7-day and 14-day windows
        today = date.today()
        seven_days_ago = today - timedelta(days=7)
        fourteen_days_ago = today - timedelta(days=14)

        shifts_7 = [s for s in shifts if _parse_shift_date(s) >= seven_days_ago]
        shifts_14 = [s for s in shifts if _parse_shift_date(s) >= fourteen_days_ago]

        assessment = assess_fatigue(
            employee_id=employee_id,
            employee_name=employee_name,
            venue_id=venue_id,
            shifts_7_days=shifts_7,
            shifts_14_days=shifts_14,
            rules=rules,
        )
        assessments.append(assessment)

    return assessments


def generate_recommendations(assessment: FatigueAssessment) -> List[str]:
    """Generate actionable recommendations based on assessment."""
    recommendations = []

    if assessment.risk_level == FatigueRiskLevel.CRITICAL:
        recommendations.append("URGENT: Reduce hours immediately - employee is at critical fatigue risk")
        recommendations.append("Contact manager/roster maker to remove from schedule this week")

    if assessment.risk_level == FatigueRiskLevel.HIGH:
        if assessment.weekly_hours > 50:
            recommendations.append(f"Reduce weekly hours from {assessment.weekly_hours:.1f}h to <50h next week")

    if assessment.consecutive_days >= 6:
        recommendations.append("Schedule a day off within 48 hours (approaching max consecutive days)")

    if assessment.night_shift_count >= 3:
        recommendations.append(f"Reduce night shifts: {assessment.night_shift_count} in last 7 days")

    if assessment.last_day_off and (date.today() - assessment.last_day_off).days > 5:
        recommendations.append(f"Last day off was {(date.today() - assessment.last_day_off).days} days ago - schedule rest soon")

    if assessment.fortnightly_hours > 90:
        recommendations.append(f"Reduce fortnightly hours from {assessment.fortnightly_hours:.1f}h - approaching limit")

    if not recommendations:
        if assessment.risk_level == FatigueRiskLevel.MODERATE:
            recommendations.append("Monitor fatigue levels - consider adding more rest days next week")
        elif assessment.risk_level == FatigueRiskLevel.LOW:
            recommendations.append("Fatigue levels normal - continue current schedule")

    return recommendations


def would_exceed_limits(
    employee_id: str,
    proposed_shift: Dict[str, Any],
    existing_shifts: List[Dict[str, Any]],
    rules: Optional[Dict[str, FatigueRule]] = None,
) -> Tuple[bool, List[str]]:
    """Pre-check if a proposed shift would exceed fatigue limits.

    Args:
        employee_id: Employee ID
        proposed_shift: Proposed shift {date: date, start: "HH:MM", end: "HH:MM"}
        existing_shifts: Current shifts
        rules: Fatigue rules

    Returns:
        (would_exceed, list of reasons)
    """
    if rules is None:
        rules = DEFAULT_RULES

    reasons = []
    today = date.today()

    # Check if adding this shift would exceed weekly hours
    shifts_7 = existing_shifts + [proposed_shift]
    weekly_hours = sum(_shift_duration(s) for s in shifts_7 if (today - _parse_shift_date(s)).days < 7)

    if weekly_hours > rules["max_weekly_hours"].max_value:
        reasons.append(
            f"Adding this shift would exceed weekly hours: {weekly_hours:.1f}h > {rules['max_weekly_hours'].max_value}h"
        )

    # Check consecutive days
    consecutive = _count_consecutive_days(shifts_7)
    if consecutive > int(rules["max_consecutive_days"].max_value):
        reasons.append(
            f"Adding this shift would exceed consecutive days: {consecutive}d > {int(rules['max_consecutive_days'].max_value)}d"
        )

    return len(reasons) > 0, reasons


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _shift_duration(shift: Dict[str, Any]) -> float:
    """Calculate shift duration in hours."""
    start = shift.get("start", "00:00")
    end = shift.get("end", "00:00")
    return _time_diff_hours(start, end)


def _time_diff_hours(start: str, end: str) -> float:
    """Calculate hours between start and end times (handling overnight)."""
    start_parts = start.split(":")
    end_parts = end.split(":")

    start_mins = int(start_parts[0]) * 60 + int(start_parts[1])
    end_mins = int(end_parts[0]) * 60 + int(end_parts[1])

    # If end < start, assume overnight shift
    if end_mins < start_mins:
        end_mins += 24 * 60

    return (end_mins - start_mins) / 60.0


def _count_consecutive_days(shifts: List[Dict[str, Any]]) -> int:
    """Count consecutive days worked from most recent."""
    if not shifts:
        return 0

    sorted_shifts = sorted(shifts, key=lambda s: _parse_shift_date(s), reverse=True)
    consecutive = 1
    current_date = _parse_shift_date(sorted_shifts[0])

    for i in range(1, len(sorted_shifts)):
        prev_date = _parse_shift_date(sorted_shifts[i])
        if (current_date - prev_date).days == 1:
            consecutive += 1
            current_date = prev_date
        else:
            break

    return consecutive


def _find_last_day_off(shifts: List[Dict[str, Any]]) -> Optional[date]:
    """Find the most recent day off (day not in shifts list)."""
    if not shifts:
        return None

    sorted_dates = sorted(set(_parse_shift_date(s) for s in shifts), reverse=True)
    current = date.today()

    for i in range(14):  # Check last 14 days
        check_date = current - timedelta(days=i)
        if check_date not in sorted_dates:
            return check_date

    return None


def _is_night_shift(shift: Dict[str, Any]) -> bool:
    """Check if shift ends after midnight."""
    end = shift.get("end", "00:00")
    end_parts = end.split(":")
    end_hour = int(end_parts[0])

    # Shift ends between midnight and 6am
    return end_hour < 6 or end_hour >= 22


def _parse_shift_date(shift: Dict[str, Any]) -> date:
    """Parse shift date from shift dict."""
    if "date" in shift:
        d = shift["date"]
        if isinstance(d, str):
            return datetime.fromisoformat(d).date()
        return d
    return date.today()


def _determine_trigger(assessment: FatigueAssessment) -> str:
    """Determine the primary trigger for an alert."""
    if assessment.weekly_hours > 50:
        return "exceeded_weekly_hours"
    if assessment.consecutive_days >= 6:
        return "max_consecutive_days_worked"
    if assessment.night_shift_count >= 3:
        return "excessive_night_shifts"
    if assessment.fortnightly_hours > 95:
        return "exceeded_fortnightly_hours"
    return "high_fatigue_score"


def get_fatigue_store() -> FatigueStore:
    """Get the global fatigue store instance."""
    return _store


def _reset_for_tests() -> None:
    """Reset store for testing."""
    global _store
    _store = FatigueStore()
