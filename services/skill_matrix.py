"""
Staff skill matrix and training gap analysis engine for RosterIQ.

Provides comprehensive skills assessment and training recommendations:
- Skill matrix building across venue staff
- Coverage analysis per role/skill
- Training gap identification (critical gaps, single points of failure)
- Cross-training recommendations
- Resilience scoring (0-100 venue coverage score)
- Absence impact simulation
- Hiring profile recommendations

All analysis is based on employee skills data stored in the database.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Set, Tuple
from enum import Enum

from rosteriq.models import Employee
from rosteriq.database import get_db


# ============================================================================
# ENUMS
# ============================================================================


class UrgencyLevel(str, Enum):
    """Training urgency classification."""

    high = "high"
    medium = "medium"
    low = "low"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass
class SkillCoverage:
    """Coverage statistics for a single role/skill."""

    role: str
    trained_count: int
    total_employees: int
    coverage_pct: float
    is_critical: bool
    single_point_of_failure: bool
    recommended_additional: int


@dataclass
class SkillMatrix:
    """Complete skill matrix for a venue."""

    venue_id: str
    roles: List[str]
    employees: List[str]
    matrix: Dict[str, Dict[str, bool]]  # employee_id -> {role: has_skill}
    coverage: Dict[str, SkillCoverage]
    overall_score: float
    generated_at: str


@dataclass
class TrainingGap:
    """A single training gap requiring attention."""

    role: str
    current_count: int
    minimum_needed: int
    urgency: UrgencyLevel


@dataclass
class SPOFRisk:
    """Single point of failure risk: only one person can do a role."""

    role: str
    sole_employee_id: str
    sole_employee_name: str
    backup_candidates: List[Dict[str, str]]  # [{id, name}, ...]


@dataclass
class TrainingPriority:
    """Prioritized training action for an employee."""

    role: str
    employee_id: str
    employee_name: str
    reason: str
    impact_score: float  # 0-100, higher = more impactful


@dataclass
class CrossTrainSuggestion:
    """Cross-training recommendation for an employee."""

    employee_id: str
    employee_name: str
    current_skills: List[str]
    suggested_skill: str
    reason: str


@dataclass
class TrainingGapReport:
    """Complete training gap analysis for a venue."""

    venue_id: str
    critical_gaps: List[TrainingGap]
    spof_risks: List[SPOFRisk]
    training_priorities: List[TrainingPriority]
    cross_training_suggestions: List[CrossTrainSuggestion]
    estimated_training_hours: float
    resilience_score: float
    generated_at: str


@dataclass
class EmployeeVersatility:
    """Employee versatility profile."""

    employee_id: str
    employee_name: str
    skills: List[str]
    skill_count: int
    versatility_score: float  # 0-100
    critical_roles: List[str]  # Roles where this employee is sole trainer
    can_substitute_for: List[str]  # Roles this employee can back up


@dataclass
class AbsenceImpact:
    """Impact analysis of an employee absence."""

    employee_id: str
    employee_name: str
    affected_roles: List[str]
    coverage_loss: Dict[str, float]  # role -> coverage_pct loss
    critical_gaps_created: List[str]
    resilience_score_without: float
    resilience_loss: float


@dataclass
class HiringProfile:
    """Recommended skills for next hire based on gaps."""

    primary_skill: str
    secondary_skills: List[str]
    justification: str
    expected_impact_on_resilience: float


# ============================================================================
# SKILL MATRIX SERVICE
# ============================================================================


class SkillMatrixService:
    """Service for building and analyzing skill matrices."""

    # Minimum number of trained staff for a role to avoid criticality
    MIN_TRAINED_THRESHOLD = 2

    def __init__(self, db=None):
        """Initialize the service with optional database connection."""
        self.db = db or get_db()

    def build_skill_matrix(self, venue_id: str) -> SkillMatrix:
        """
        Build a complete skill matrix for a venue.

        Returns SkillMatrix with employee-role coverage map and statistics.
        """
        employees = self.db.list_employees()

        # Filter to venue if needed (assumes all employees for now)
        # In practice, would filter by venue_id

        if not employees:
            return SkillMatrix(
                venue_id=venue_id,
                roles=[],
                employees=[],
                matrix={},
                coverage={},
                overall_score=0.0,
                generated_at=datetime.utcnow().isoformat(),
            )

        # Extract all unique roles/skills across all employees
        all_roles = set()
        for emp in employees:
            all_roles.update(emp.skills)

        all_roles = sorted(list(all_roles))
        employee_ids = sorted([e.id for e in employees])

        # Build matrix: employee_id -> {role: has_skill}
        matrix = {}
        for emp in employees:
            matrix[emp.id] = {role: role in emp.skills for role in all_roles}

        # Calculate coverage for each role
        coverage = {}
        for role in all_roles:
            trained = sum(1 for emp in employees if role in emp.skills)
            pct = (trained / len(employees) * 100) if employees else 0
            is_critical = trained < self.MIN_TRAINED_THRESHOLD
            is_spof = trained == 1

            # Recommend additional training
            if trained < self.MIN_TRAINED_THRESHOLD:
                recommended = self.MIN_TRAINED_THRESHOLD - trained
            else:
                # For non-critical roles, maintain 3+ for resilience
                recommended = max(0, 3 - trained)

            coverage[role] = SkillCoverage(
                role=role,
                trained_count=trained,
                total_employees=len(employees),
                coverage_pct=pct,
                is_critical=is_critical,
                single_point_of_failure=is_spof,
                recommended_additional=recommended,
            )

        # Calculate overall resilience score
        overall_score = self.calculate_resilience_score_for_matrix(
            matrix, employees, coverage
        )

        return SkillMatrix(
            venue_id=venue_id,
            roles=all_roles,
            employees=employee_ids,
            matrix=matrix,
            coverage=coverage,
            overall_score=overall_score,
            generated_at=datetime.utcnow().isoformat(),
        )

    def identify_training_gaps(self, venue_id: str) -> TrainingGapReport:
        """
        Analyze training gaps and generate recommendations.

        Returns TrainingGapReport with priorities and action items.
        """
        matrix = self.build_skill_matrix(venue_id)
        employees = self.db.list_employees()

        # 1. Identify critical gaps (roles with < 2 trained staff)
        critical_gaps = []
        for role, cov in matrix.coverage.items():
            if cov.trained_count < self.MIN_TRAINED_THRESHOLD:
                gap = TrainingGap(
                    role=role,
                    current_count=cov.trained_count,
                    minimum_needed=self.MIN_TRAINED_THRESHOLD,
                    urgency=UrgencyLevel.high if cov.trained_count == 0 else UrgencyLevel.medium,
                )
                critical_gaps.append(gap)

        # 2. Identify single points of failure
        spof_risks = []
        for role, cov in matrix.coverage.items():
            if cov.single_point_of_failure:
                # Find the sole employee
                sole_emp = None
                for emp in employees:
                    if role in emp.skills:
                        sole_emp = emp
                        break

                if sole_emp:
                    # Find backup candidates (employees without this skill)
                    candidates = [
                        e for e in employees
                        if e.id != sole_emp.id and role not in e.skills
                    ]

                    backup_candidates = [
                        {"id": e.id, "name": e.name} for e in candidates
                    ]

                    spof_risks.append(SPOFRisk(
                        role=role,
                        sole_employee_id=sole_emp.id,
                        sole_employee_name=sole_emp.name,
                        backup_candidates=backup_candidates,
                    ))

        # 3. Generate training priorities (ranked by impact)
        training_priorities = self._generate_training_priorities(
            employees, matrix
        )

        # 4. Generate cross-training suggestions
        cross_training = self._generate_cross_training_suggestions(
            employees, matrix, critical_gaps
        )

        # 5. Estimate training hours
        estimated_hours = self._estimate_training_hours(
            critical_gaps, len(employees)
        )

        return TrainingGapReport(
            venue_id=venue_id,
            critical_gaps=critical_gaps,
            spof_risks=spof_risks,
            training_priorities=training_priorities,
            cross_training_suggestions=cross_training,
            estimated_training_hours=estimated_hours,
            resilience_score=matrix.overall_score,
            generated_at=datetime.utcnow().isoformat(),
        )

    def get_employee_versatility(self, employee_id: str) -> EmployeeVersatility:
        """
        Get versatility profile for a single employee.

        Measures how many roles they can fill and their criticality.
        """
        emp = self.db.get_employee(employee_id)
        if not emp:
            raise ValueError(f"Employee {employee_id} not found")

        employees = self.db.list_employees()

        # Find roles where this employee is the sole trainer
        critical_roles = []
        for other_emp in employees:
            for skill in emp.skills:
                if skill not in other_emp.skills and other_emp.id != employee_id:
                    # Check if emp is sole trainer of this skill
                    trained_count = sum(
                        1 for e in employees if skill in e.skills
                    )
                    if trained_count == 1:  # Only emp has it
                        critical_roles.append(skill)

        critical_roles = list(set(critical_roles))

        # Find roles employee can back up (already trained)
        can_substitute = emp.skills.copy()

        # Calculate versatility score (0-100)
        # Based on: skill count, criticality, and diversity
        all_roles = set()
        for e in employees:
            all_roles.update(e.skills)

        skill_count = len(emp.skills)
        max_skills = len(all_roles) if all_roles else 1
        skill_pct = (skill_count / max_skills * 100) if max_skills > 0 else 0

        # Criticality bonus: up to 20 points for being sole trainer
        criticality_score = min(20, len(critical_roles) * 10)

        versatility_score = min(100, skill_pct + criticality_score)

        return EmployeeVersatility(
            employee_id=employee_id,
            employee_name=emp.name,
            skills=emp.skills,
            skill_count=skill_count,
            versatility_score=versatility_score,
            critical_roles=critical_roles,
            can_substitute_for=can_substitute,
        )

    def simulate_absence(
        self, venue_id: str, absent_employee_id: str
    ) -> AbsenceImpact:
        """
        Simulate the impact of an employee's absence on coverage.

        Shows which roles lose coverage and by how much.
        """
        emp = self.db.get_employee(absent_employee_id)
        if not emp:
            raise ValueError(f"Employee {absent_employee_id} not found")

        # Build matrix for current state
        matrix_before = self.build_skill_matrix(venue_id)

        # Build hypothetical matrix without this employee
        employees = self.db.list_employees()
        matrix_after_data = {
            e.id: {role: role in e.skills for role in matrix_before.roles}
            for e in employees
            if e.id != absent_employee_id
        }

        # Calculate coverage loss
        coverage_loss = {}
        critical_gaps_created = []

        for role in matrix_before.roles:
            before_trained = sum(
                1 for e in employees if role in e.skills
            )
            after_trained = sum(
                1 for e in employees
                if e.id != absent_employee_id and role in e.skills
            )

            before_pct = (before_trained / len(employees) * 100) if employees else 0
            after_pct = (after_trained / len(employees) * 100) if employees else 0
            coverage_loss[role] = after_pct - before_pct

            # Check if this absence creates a critical gap
            if after_trained < self.MIN_TRAINED_THRESHOLD:
                critical_gaps_created.append(role)

        # Recalculate resilience without this employee
        resilience_after = self._calculate_resilience_without_employee(
            matrix_before, absent_employee_id
        )

        return AbsenceImpact(
            employee_id=absent_employee_id,
            employee_name=emp.name,
            affected_roles=emp.skills,
            coverage_loss=coverage_loss,
            critical_gaps_created=critical_gaps_created,
            resilience_score_without=resilience_after,
            resilience_loss=matrix_before.overall_score - resilience_after,
        )

    def suggest_hiring_profile(self, venue_id: str) -> HiringProfile:
        """
        Recommend ideal skills for the next hire based on gaps.

        Identifies the role with greatest impact on resilience.
        """
        report = self.identify_training_gaps(venue_id)

        if not report.critical_gaps and not report.spof_risks:
            return HiringProfile(
                primary_skill="any",
                secondary_skills=[],
                justification="No critical gaps identified. Hire for growth.",
                expected_impact_on_resilience=5.0,
            )

        # Prioritize: SPOF > Critical gap > Medium gap
        primary_skill = None
        impact = 0.0

        if report.spof_risks:
            # Hire someone who can back up the SPOF
            primary_skill = report.spof_risks[0].role
            impact = 25.0  # SPOF elimination is high impact
        elif report.critical_gaps:
            primary_skill = report.critical_gaps[0].role
            impact = 20.0  # Critical gap elimination

        # Secondary skills: most common among current staff
        employees = self.db.list_employees()
        skill_frequency = {}
        for emp in employees:
            for skill in emp.skills:
                if skill != primary_skill:
                    skill_frequency[skill] = skill_frequency.get(skill, 0) + 1

        secondary_skills = sorted(
            skill_frequency.keys(),
            key=lambda s: skill_frequency[s],
            reverse=True,
        )[:2]

        justification = f"Hiring someone with {primary_skill} skill will "
        if report.spof_risks:
            justification += "eliminate single point of failure risk."
        else:
            justification += "address critical gap in coverage."

        return HiringProfile(
            primary_skill=primary_skill or "flexible",
            secondary_skills=secondary_skills,
            justification=justification,
            expected_impact_on_resilience=impact,
        )

    def calculate_resilience_score(self, matrix: SkillMatrix) -> float:
        """
        Calculate overall resilience score (0-100).

        Based on:
        - Coverage breadth: do most roles have trained staff?
        - Coverage depth: how many can do each role?
        - SPOF risk: are any roles at risk?
        """
        if not matrix.coverage:
            return 0.0

        coverages = list(matrix.coverage.values())

        # Score 1: Coverage breadth (0-40 points)
        # Percentage of roles with at least MIN_TRAINED_THRESHOLD staff
        covered_roles = sum(1 for c in coverages if c.trained_count >= self.MIN_TRAINED_THRESHOLD)
        breadth_score = (covered_roles / len(coverages) * 40) if coverages else 0

        # Score 2: Coverage depth (0-40 points)
        # Average coverage percentage across all roles
        avg_coverage = sum(c.coverage_pct for c in coverages) / len(coverages) if coverages else 0
        depth_score = avg_coverage * 0.4

        # Score 3: SPOF risk penalty (0-20 points)
        # Penalize for roles with only 1 person
        spof_count = sum(1 for c in coverages if c.single_point_of_failure)
        spof_score = max(0, 20 - (spof_count * 5))

        total = breadth_score + depth_score + spof_score
        return min(100, max(0, total))

    # ============================================================================
    # PRIVATE METHODS
    # ============================================================================

    def _generate_training_priorities(
        self, employees: List[Employee], matrix: SkillMatrix
    ) -> List[TrainingPriority]:
        """Generate prioritized training recommendations."""
        priorities = []

        for role, coverage in matrix.coverage.items():
            if coverage.is_critical:
                # Find employees without this skill to train
                candidates = [
                    e for e in employees if role not in e.skills
                ]

                for candidate in candidates:
                    # Impact score: how much does training this person help?
                    # Higher if we're critical, if person is versatile, if it helps SPOF
                    base_impact = 30 if coverage.single_point_of_failure else 15
                    versatility_bonus = len(candidate.skills) * 2
                    impact = min(100, base_impact + versatility_bonus)

                    reason = f"Training {candidate.name} in {role} addresses critical gap"

                    priorities.append(TrainingPriority(
                        role=role,
                        employee_id=candidate.id,
                        employee_name=candidate.name,
                        reason=reason,
                        impact_score=impact,
                    ))

        # Sort by impact, descending
        priorities.sort(key=lambda p: p.impact_score, reverse=True)
        return priorities[:10]  # Top 10 priorities

    def _generate_cross_training_suggestions(
        self,
        employees: List[Employee],
        matrix: SkillMatrix,
        critical_gaps: List[TrainingGap],
    ) -> List[CrossTrainSuggestion]:
        """Generate cross-training suggestions."""
        suggestions = []

        # For each critical gap, suggest versatile employees to train
        gap_roles = {gap.role for gap in critical_gaps}

        for emp in employees:
            for gap_role in gap_roles:
                if gap_role not in emp.skills:
                    reason = f"Cross-train to cover {gap_role} gap"
                    suggestions.append(CrossTrainSuggestion(
                        employee_id=emp.id,
                        employee_name=emp.name,
                        current_skills=emp.skills,
                        suggested_skill=gap_role,
                        reason=reason,
                    ))

        return suggestions[:15]  # Top 15 suggestions

    def _estimate_training_hours(
        self, critical_gaps: List[TrainingGap], total_employees: int
    ) -> float:
        """Estimate total training hours needed."""
        # Rough estimate: 20 hours per gap per person to train
        hours_per_gap = 20

        total = 0
        for gap in critical_gaps:
            people_to_train = max(1, gap.minimum_needed - gap.current_count)
            total += people_to_train * hours_per_gap

        return float(total)

    def calculate_resilience_score_for_matrix(
        self,
        matrix: Dict[str, Dict[str, bool]],
        employees: List[Employee],
        coverage: Dict[str, SkillCoverage],
    ) -> float:
        """Calculate resilience score from raw matrix data."""
        if not coverage:
            return 0.0

        # Create a fake SkillMatrix to reuse scoring logic
        fake_matrix = SkillMatrix(
            venue_id="temp",
            roles=list(coverage.keys()),
            employees=list(matrix.keys()),
            matrix=matrix,
            coverage=coverage,
            overall_score=0.0,
            generated_at="",
        )

        return self.calculate_resilience_score(fake_matrix)

    def _calculate_resilience_without_employee(
        self, matrix: SkillMatrix, employee_id: str
    ) -> float:
        """Recalculate resilience score if an employee is removed."""
        employees = self.db.list_employees()

        # Rebuild coverage without this employee
        new_coverage = {}
        for role in matrix.roles:
            trained = sum(
                1 for e in employees
                if e.id != employee_id and role in e.skills
            )
            pct = (trained / (len(employees) - 1) * 100) if len(employees) > 1 else 0

            new_coverage[role] = SkillCoverage(
                role=role,
                trained_count=trained,
                total_employees=len(employees) - 1,
                coverage_pct=pct,
                is_critical=trained < self.MIN_TRAINED_THRESHOLD,
                single_point_of_failure=trained == 1,
                recommended_additional=max(0, self.MIN_TRAINED_THRESHOLD - trained),
            )

        return self.calculate_resilience_score_for_matrix(
            {e.id: matrix.matrix[e.id] for e in employees if e.id != employee_id},
            [e for e in employees if e.id != employee_id],
            new_coverage,
        )
