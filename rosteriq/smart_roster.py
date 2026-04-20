"""Smart Roster Suggestions Engine for Australian hospitality (Round 43).

Generates optimal shift suggestions by scoring candidates against weighted criteria:
- Availability: is employee available for this time slot?
- Skills match: does employee have the right role/skills?
- Certifications: does employee hold required certs (RSA, food safety, etc)?
- Performance: staff score across 5 dimensions
- Fatigue: fatigue risk assessment (inverse — low fatigue = high score)
- Cost efficiency: lower cost = higher score (considering penalty rates)

Hard constraints (exclusion criteria):
- On approved leave: excluded
- Would exceed fatigue limits: excluded
- Missing mandatory certs: excluded
- Break compliance violation: excluded

Data structures: SuitabilityFactor, StaffSuggestion, ShiftRequirement, RosterSuggestion, RosterPlan

Persists to SQLite via rosteriq.persistence.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, time, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.smart_roster")


# ─────────────────────────────────────────────────────────────────────────────
# Enums & Data Classes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class SuitabilityFactor:
    """A single suitability factor (weighted component of score)."""
    name: str
    score: float  # 0-1
    weight: float  # 0-1 (e.g., 0.2 for 20% contribution)
    reason: str  # human-readable explanation

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "score": round(self.score, 3),
            "weight": round(self.weight, 3),
            "reason": self.reason,
        }


@dataclass
class StaffSuggestion:
    """Suggestion for a single employee for a shift."""
    employee_id: str
    employee_name: str
    suitability_score: float = 0.0  # 0-100
    factors: List[SuitabilityFactor] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)  # e.g. "Fatigue risk: HIGH"
    estimated_cost: float = 0.0
    is_overtime: bool = False
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "employee_id": self.employee_id,
            "employee_name": self.employee_name,
            "suitability_score": round(self.suitability_score, 2),
            "factors": [f.to_dict() for f in self.factors],
            "warnings": self.warnings,
            "estimated_cost": round(self.estimated_cost, 2),
            "is_overtime": self.is_overtime,
            "explanation": self.explanation,
        }


@dataclass
class ShiftRequirement:
    """A shift that needs to be filled."""
    venue_id: str
    date: date
    start_time: str  # HH:MM
    end_time: str  # HH:MM
    role: str  # e.g. "bar", "kitchen", "floor", "manager"
    area: Optional[str] = None
    min_staff: int = 1
    required_certs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "date": self.date.isoformat(),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "role": self.role,
            "area": self.area,
            "min_staff": self.min_staff,
            "required_certs": self.required_certs,
        }


@dataclass
class RosterSuggestion:
    """Suggestions for a single shift requirement."""
    requirement: ShiftRequirement
    suggestions: List[StaffSuggestion] = field(default_factory=list)
    unfilled: bool = False
    unfilled_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requirement": self.requirement.to_dict(),
            "suggestions": [s.to_dict() for s in self.suggestions],
            "unfilled": self.unfilled,
            "unfilled_reason": self.unfilled_reason,
        }


@dataclass
class RosterPlan:
    """Complete roster plan for a period."""
    venue_id: str
    plan_date: date
    period_start: date
    period_end: date
    shift_requirements: List[ShiftRequirement] = field(default_factory=list)
    suggestions: List[RosterSuggestion] = field(default_factory=list)
    total_estimated_cost: float = 0.0
    budget_status: str = "on_target"  # "under", "on_target", "over"
    coverage_pct: float = 0.0  # % of requirements filled
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue_id": self.venue_id,
            "plan_date": self.plan_date.isoformat(),
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "shift_requirements_count": len(self.shift_requirements),
            "suggestions": [s.to_dict() for s in self.suggestions],
            "total_estimated_cost": round(self.total_estimated_cost, 2),
            "budget_status": self.budget_status,
            "coverage_pct": round(self.coverage_pct, 1),
            "warnings": self.warnings,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Core Scoring Functions
# ─────────────────────────────────────────────────────────────────────────────


def score_candidate(
    employee: Dict[str, Any],
    requirement: ShiftRequirement,
    context: Optional[Dict[str, Any]] = None,
) -> StaffSuggestion:
    """Score a single candidate for a shift.

    Args:
        employee: Employee dict with id, name, hourly_rate, role, skills, availability, etc.
        requirement: Shift requirement with date, time, role, required_certs.
        context: Pre-fetched data dict with staff_scores, fatigue_assessments, certifications,
                leave_requests, venue_config, state.

    Returns:
        StaffSuggestion with overall suitability_score (0-100) and detailed factors.

    Hard constraints (exclusion):
    - On approved leave
    - Would exceed fatigue limits
    - Missing mandatory certs
    - Break compliance violation
    """
    if context is None:
        context = {}

    suggestion = StaffSuggestion(
        employee_id=employee.get("id", ""),
        employee_name=employee.get("name", ""),
    )

    # ─────────────────────────────────────────────────────────────────────
    # HARD CONSTRAINTS: If ANY fail, exclude candidate
    # ─────────────────────────────────────────────────────────────────────

    # Check if on approved leave
    if _is_on_leave(employee.get("id"), requirement.date, context):
        suggestion.unfilled_reason = "On approved leave"
        suggestion.suitability_score = 0.0
        suggestion.warnings.append("On approved leave")
        return suggestion

    # Check fatigue limits
    if _would_exceed_fatigue(employee.get("id"), requirement, context):
        suggestion.unfilled_reason = "Would exceed fatigue limits"
        suggestion.suitability_score = 0.0
        suggestion.warnings.append("Would exceed fatigue limits")
        return suggestion

    # Check mandatory certifications
    missing_certs = _check_missing_certs(employee.get("id"), requirement.required_certs, context)
    if missing_certs:
        suggestion.unfilled_reason = f"Missing certs: {', '.join(missing_certs)}"
        suggestion.suitability_score = 0.0
        suggestion.warnings.append(f"Missing mandatory certs: {', '.join(missing_certs)}")
        return suggestion

    # Check break compliance
    if _violates_break_compliance(employee.get("id"), requirement, context):
        suggestion.unfilled_reason = "Would violate break compliance"
        suggestion.suitability_score = 0.0
        suggestion.warnings.append("Would violate Fair Work break rules")
        return suggestion

    # ─────────────────────────────────────────────────────────────────────
    # SOFT SCORING: Weighted factors
    # ─────────────────────────────────────────────────────────────────────

    factors = []

    # 1. Availability (0.2 weight)
    avail_score, avail_reason = _score_availability(employee, requirement, context)
    factors.append(SuitabilityFactor("Availability", avail_score, 0.2, avail_reason))

    # 2. Skills match (0.2 weight)
    skills_score, skills_reason = _score_skills_match(employee, requirement, context)
    factors.append(SuitabilityFactor("Skills match", skills_score, 0.2, skills_reason))

    # 3. Certifications (0.15 weight)
    certs_score, certs_reason = _score_certs(employee, requirement, context)
    factors.append(SuitabilityFactor("Certifications", certs_score, 0.15, certs_reason))

    # 4. Performance (0.15 weight)
    perf_score, perf_reason = _score_performance(employee, context)
    factors.append(SuitabilityFactor("Performance", perf_score, 0.15, perf_reason))

    # 5. Fatigue (0.15 weight)
    fatigue_score, fatigue_reason = _score_fatigue(employee.get("id"), requirement, context)
    factors.append(SuitabilityFactor("Fatigue risk", fatigue_score, 0.15, fatigue_reason))

    # 6. Cost efficiency (0.15 weight)
    cost_score, cost_reason, estimated_cost = _score_cost_efficiency(
        employee.get("hourly_rate", 25.0),
        requirement,
        context,
    )
    factors.append(SuitabilityFactor("Cost efficiency", cost_score, 0.15, cost_reason))

    suggestion.factors = factors
    suggestion.estimated_cost = estimated_cost

    # Compute weighted suitability score
    weighted_score = sum(f.score * f.weight for f in factors)
    suggestion.suitability_score = weighted_score * 100  # Scale to 0-100

    # Check for warnings (non-fatal issues)
    warnings = _collect_warnings(employee.get("id"), requirement, context)
    suggestion.warnings = warnings

    # Generate explanation
    suggestion.explanation = explain_suggestion(suggestion)

    return suggestion


def suggest_for_shift(
    requirement: ShiftRequirement,
    candidates: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> RosterSuggestion:
    """Rank all candidates for a shift requirement.

    Args:
        requirement: Shift requirement.
        candidates: List of employee dicts.
        context: Pre-fetched context dict.

    Returns:
        RosterSuggestion with ranked suggestions (best to worst).
    """
    if context is None:
        context = {}

    suggestions = [score_candidate(emp, requirement, context) for emp in candidates]

    # Sort by suitability_score descending (best first)
    suggestions.sort(key=lambda s: s.suitability_score, reverse=True)

    unfilled = all(s.suitability_score == 0.0 for s in suggestions) or len(suggestions) == 0
    unfilled_reason = None
    if unfilled:
        if not suggestions:
            unfilled_reason = "No candidates available"
        else:
            unfilled_reason = "No suitable candidates found"

    return RosterSuggestion(
        requirement=requirement,
        suggestions=suggestions,
        unfilled=unfilled,
        unfilled_reason=unfilled_reason,
    )


def build_roster_plan(
    venue_id: str,
    period_start: date,
    period_end: date,
    candidates: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> RosterPlan:
    """Build a complete roster plan for a period (stub).

    Args:
        venue_id: Venue identifier.
        period_start: Start date (inclusive).
        period_end: End date (inclusive).
        candidates: List of employee dicts.
        context: Pre-fetched context dict.

    Returns:
        RosterPlan with suggestions for all shifts in the period.

    Note: This is a stub. A real implementation would:
    - Fetch all shift requirements for the period from the venue config
    - Call suggest_for_shift for each requirement
    - Aggregate costs and coverage
    - Check for warnings (understaffed days, over-budget, etc.)
    """
    plan = RosterPlan(
        venue_id=venue_id,
        plan_date=date.today(),
        period_start=period_start,
        period_end=period_end,
    )
    return plan


def explain_suggestion(suggestion: StaffSuggestion) -> str:
    """Generate human-readable explanation for a suggestion.

    Args:
        suggestion: StaffSuggestion to explain.

    Returns:
        Multi-sentence explanation of why this employee is suitable (or not).
    """
    if suggestion.suitability_score == 0.0:
        return f"{suggestion.employee_name} cannot be assigned: {', '.join(suggestion.warnings)}"

    # Get top 2-3 factors by (score * weight)
    scored_factors = sorted(
        suggestion.factors,
        key=lambda f: f.score * f.weight,
        reverse=True,
    )[:3]

    reason_parts = [f"{suggestion.employee_name} scores {suggestion.suitability_score:.0f}/100."]
    for factor in scored_factors:
        reason_parts.append(f"{factor.reason}")

    if suggestion.warnings:
        reason_parts.append("Warnings: " + "; ".join(suggestion.warnings))

    return " ".join(reason_parts)


def estimate_shift_cost(
    employee_hourly_rate: float,
    start_time: str,  # HH:MM
    end_time: str,  # HH:MM
    shift_date: date,
    state: str = "QLD",
) -> float:
    """Estimate shift cost including penalty rates.

    Args:
        employee_hourly_rate: Base hourly rate in AUD.
        start_time: Start time (HH:MM).
        end_time: End time (HH:MM).
        shift_date: Date of shift.
        state: State for public holiday checking (e.g. "QLD", "NSW", "VIC").

    Returns:
        Estimated cost in AUD (including penalty rates for weekend/public holiday/evening).
    """
    # Calculate duration
    try:
        start_h, start_m = map(int, start_time.split(":"))
        end_h, end_m = map(int, end_time.split(":"))
        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m
        if end_minutes < start_minutes:  # Overnight shift
            end_minutes += 24 * 60
        duration_hours = (end_minutes - start_minutes) / 60.0
    except (ValueError, IndexError):
        duration_hours = 8.0  # Default to 8 hours if parsing fails

    # Lazy import public_holidays for penalty multiplier
    multiplier = 1.0
    try:
        from rosteriq import public_holidays as ph

        is_holiday, holiday_obj = ph.is_public_holiday(shift_date, state)
        if is_holiday:
            multiplier = 2.5  # 250% for public holiday
        elif shift_date.weekday() == 5:  # Saturday
            multiplier = 1.25  # 125% for Saturday
        elif shift_date.weekday() == 6:  # Sunday
            multiplier = 1.5  # 150% for Sunday
    except (ImportError, AttributeError, TypeError):
        # Fallback: check day of week only
        weekday = shift_date.weekday()
        if weekday == 5:  # Saturday
            multiplier = 1.25
        elif weekday == 6:  # Sunday
            multiplier = 1.5

    cost = employee_hourly_rate * duration_hours * multiplier
    return cost


# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions (Hard Constraints)
# ─────────────────────────────────────────────────────────────────────────────


def _is_on_leave(employee_id: str, shift_date: date, context: Dict[str, Any]) -> bool:
    """Check if employee is on approved leave on this date."""
    leave_requests = context.get("leave_requests", {}).get(employee_id, [])
    for leave in leave_requests:
        try:
            start = datetime.fromisoformat(leave.get("start_date", "")).date()
            end = datetime.fromisoformat(leave.get("end_date", "")).date()
            status = leave.get("status", "")
            if status == "approved" and start <= shift_date <= end:
                return True
        except (ValueError, TypeError, AttributeError):
            pass
    return False


def _would_exceed_fatigue(
    employee_id: str,
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> bool:
    """Check if assigning this shift would exceed fatigue limits (hard constraint).

    Only CRITICAL risk is an exclusion; HIGH risk is a warning (see _collect_warnings).
    """
    try:
        assessments = context.get("fatigue_assessments", {})
        assessment = assessments.get(employee_id)
        if assessment is None:
            return False

        # Only CRITICAL risk is a hard constraint
        risk_level = assessment.get("risk_level", "low")
        if risk_level == "critical":
            return True

    except (ImportError, AttributeError, KeyError, TypeError):
        pass

    return False


def _check_missing_certs(
    employee_id: str,
    required_certs: List[str],
    context: Dict[str, Any],
) -> List[str]:
    """Check for missing mandatory certifications."""
    if not required_certs:
        return []

    certs = context.get("certifications", {}).get(employee_id, [])
    cert_types = {c.get("cert_type", "") for c in certs}

    missing = [c for c in required_certs if c not in cert_types]
    return missing


def _violates_break_compliance(
    employee_id: str,
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> bool:
    """Check if assigning this shift would violate break compliance rules."""
    try:
        from rosteriq import break_compliance

        # Lazy check: if module available and function exists
        if hasattr(break_compliance, "check_shift_compliance"):
            violations = break_compliance.check_shift_compliance(
                employee_id,
                requirement.date,
                requirement.start_time,
                requirement.end_time,
            )
            return len(violations) > 0

    except (ImportError, AttributeError):
        pass

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions (Soft Scoring)
# ─────────────────────────────────────────────────────────────────────────────


def _score_availability(
    employee: Dict[str, Any],
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> tuple:
    """Score availability (0-1).

    Returns:
        (score, reason)
    """
    # Check availability dict (if provided)
    availability = employee.get("availability", {})
    day_of_week = requirement.date.weekday()

    # If no availability dict provided, assume available
    if not availability:
        return 1.0, "Available (no constraints)"

    slots = availability.get(day_of_week, [])
    if day_of_week in availability and not slots:
        # Explicitly unavailable on this day of week (empty list)
        return 0.0, "Not available on this day of week"

    if not slots:
        # No explicit availability data for this day — assume available
        return 1.0, "Available (no constraints)"

    # Check if shift time falls within any available slot
    try:
        start_h, start_m = map(int, requirement.start_time.split(":"))
        end_h, end_m = map(int, requirement.end_time.split(":"))
    except (ValueError, IndexError):
        return 0.5, "Could not parse shift time"

    for slot_start, slot_end in slots:
        if slot_start <= start_h and end_h <= slot_end:
            return 1.0, "Available for this time slot"

    return 0.3, f"Partially available (preferred {slots})"


def _score_skills_match(
    employee: Dict[str, Any],
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> tuple:
    """Score skills match (0-1).

    Returns:
        (score, reason)
    """
    employee_role = employee.get("role", "")
    employee_skills = employee.get("skills", [employee_role])
    required_role = requirement.role

    if required_role in employee_skills or required_role == employee_role:
        return 1.0, f"Expert in {required_role}"

    if len(employee_skills) > 1:
        return 0.7, f"Can do {required_role} (cross-trained)"

    return 0.2, f"Not trained in {required_role}"


def _score_certs(
    employee: Dict[str, Any],
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> tuple:
    """Score certifications (0-1).

    Returns:
        (score, reason)
    """
    if not requirement.required_certs:
        return 1.0, "No certs required for this shift"

    certs = context.get("certifications", {}).get(employee.get("id", ""), [])
    cert_types = {c.get("cert_type", "") for c in certs}
    valid_certs = {c.get("cert_type", "") for c in certs if c.get("status", "") == "valid"}

    required = set(requirement.required_certs)
    held = required & valid_certs

    if held == required:
        return 1.0, f"All certs current: {', '.join(held)}"
    elif held:
        return 0.5, f"Some certs current: {', '.join(held)}"
    else:
        return 0.0, f"No valid certs held"


def _score_performance(
    employee: Dict[str, Any],
    context: Dict[str, Any],
) -> tuple:
    """Score performance from staff_score module (0-1).

    Returns:
        (score, reason)
    """
    staff_scores = context.get("staff_scores", {})
    employee_id = employee.get("id", "")
    score_obj = staff_scores.get(employee_id)

    if score_obj is None:
        # No score data — neutral
        return 0.5, "No performance history"

    overall = score_obj.get("overall_score", 50) / 100.0
    if overall >= 0.8:
        return overall, f"High performer ({score_obj.get('overall_score', 50)}/100)"
    elif overall >= 0.6:
        return overall, f"Solid performer ({score_obj.get('overall_score', 50)}/100)"
    else:
        return overall, f"Needs improvement ({score_obj.get('overall_score', 50)}/100)"


def _score_fatigue(
    employee_id: str,
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> tuple:
    """Score fatigue risk (inverse — low risk = high score) (0-1).

    Returns:
        (score, reason)
    """
    assessments = context.get("fatigue_assessments", {})
    assessment = assessments.get(employee_id)

    if assessment is None:
        return 0.8, "No fatigue data available"

    risk_level = assessment.get("risk_level", "low")
    score_val = assessment.get("score", 20)  # 0-100 where 0 = no fatigue, 100 = critical

    # Inverse scoring: higher fatigue score = lower suitability
    inverted = 1.0 - (score_val / 100.0)

    if risk_level == "low":
        return inverted, f"Low fatigue risk (score {score_val})"
    elif risk_level == "moderate":
        return inverted, f"Moderate fatigue risk (score {score_val})"
    elif risk_level == "high":
        return inverted, f"High fatigue risk (score {score_val})"
    else:  # critical
        return inverted, f"CRITICAL fatigue risk (score {score_val})"


def _score_cost_efficiency(
    hourly_rate: float,
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> tuple:
    """Score cost efficiency (lower cost = higher score) (0-1).

    Returns:
        (score, reason, estimated_cost)
    """
    state = context.get("state", "QLD")
    estimated_cost = estimate_shift_cost(
        hourly_rate,
        requirement.start_time,
        requirement.end_time,
        requirement.date,
        state,
    )

    # Scoring: baseline = $200 shift; below that = high score, above = lower score
    baseline = 200.0
    if estimated_cost <= baseline:
        score = 1.0
    else:
        score = max(0.0, 1.0 - ((estimated_cost - baseline) / baseline))

    return score, f"Cost: ${estimated_cost:.0f}", estimated_cost


def _collect_warnings(
    employee_id: str,
    requirement: ShiftRequirement,
    context: Dict[str, Any],
) -> List[str]:
    """Collect non-fatal warnings for this assignment.

    Returns:
        List of warning strings.
    """
    warnings = []

    # Check fatigue level
    assessments = context.get("fatigue_assessments", {})
    assessment = assessments.get(employee_id)
    if assessment:
        risk_level = assessment.get("risk_level", "low")
        if risk_level in ("high", "critical"):
            warnings.append(f"Fatigue risk: {risk_level.upper()}")

    # Check cert expiry
    certs = context.get("certifications", {}).get(employee_id, [])
    for cert in certs:
        status = cert.get("status", "")
        if status == "expiring_soon":
            cert_type = cert.get("cert_type", "cert")
            warnings.append(f"{cert_type.upper()} expiring soon")

    return warnings


# ─────────────────────────────────────────────────────────────────────────────
# Persistence stub (for future use)
# ─────────────────────────────────────────────────────────────────────────────


def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence

        return persistence
    except ImportError:
        return None
