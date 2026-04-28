"""
Skill matrix and training gap analysis routes.

Provides REST endpoints for:
- Venue skill matrix (full employee-role coverage map)
- Training gap analysis and recommendations
- Resilience scoring
- Employee versatility profiles
- Absence impact simulation
- Hiring profile recommendations
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
import uuid

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.services.skill_matrix import (
    SkillMatrixService,
    SkillMatrix,
    SkillCoverage,
    TrainingGapReport,
    EmployeeVersatility,
    AbsenceImpact,
    HiringProfile,
)


router = APIRouter(prefix="/api/v1/venues", tags=["skill-matrix"])


# ============================================================================
# Response Models
# ============================================================================


class SkillCoverageResponse(BaseModel):
    """Coverage statistics for a role."""

    role: str
    trained_count: int
    total_employees: int
    coverage_pct: float
    is_critical: bool
    single_point_of_failure: bool
    recommended_additional: int

    @staticmethod
    def from_domain(cov: SkillCoverage) -> "SkillCoverageResponse":
        """Convert domain model to response."""
        return SkillCoverageResponse(
            role=cov.role,
            trained_count=cov.trained_count,
            total_employees=cov.total_employees,
            coverage_pct=cov.coverage_pct,
            is_critical=cov.is_critical,
            single_point_of_failure=cov.single_point_of_failure,
            recommended_additional=cov.recommended_additional,
        )


class SkillMatrixResponse(BaseModel):
    """Full skill matrix response."""

    venue_id: str
    roles: List[str]
    employees: List[str]
    matrix: Dict[str, Dict[str, bool]]
    coverage: Dict[str, SkillCoverageResponse]
    overall_score: float = Field(..., ge=0, le=100)
    generated_at: str

    @staticmethod
    def from_domain(matrix: SkillMatrix) -> "SkillMatrixResponse":
        """Convert domain model to response."""
        return SkillMatrixResponse(
            venue_id=matrix.venue_id,
            roles=matrix.roles,
            employees=matrix.employees,
            matrix=matrix.matrix,
            coverage={
                role: SkillCoverageResponse.from_domain(cov)
                for role, cov in matrix.coverage.items()
            },
            overall_score=matrix.overall_score,
            generated_at=matrix.generated_at,
        )


class TrainingGapResponse(BaseModel):
    """Single training gap."""

    role: str
    current_count: int
    minimum_needed: int
    urgency: str


class SPOFRiskResponse(BaseModel):
    """Single point of failure risk."""

    role: str
    sole_employee_id: str
    sole_employee_name: str
    backup_candidates: List[Dict[str, str]]


class TrainingPriorityResponse(BaseModel):
    """Prioritized training action."""

    role: str
    employee_id: str
    employee_name: str
    reason: str
    impact_score: float = Field(..., ge=0, le=100)


class CrossTrainSuggestionResponse(BaseModel):
    """Cross-training recommendation."""

    employee_id: str
    employee_name: str
    current_skills: List[str]
    suggested_skill: str
    reason: str


class TrainingGapReportResponse(BaseModel):
    """Complete training gap analysis."""

    venue_id: str
    critical_gaps: List[TrainingGapResponse]
    spof_risks: List[SPOFRiskResponse]
    training_priorities: List[TrainingPriorityResponse]
    cross_training_suggestions: List[CrossTrainSuggestionResponse]
    estimated_training_hours: float
    resilience_score: float = Field(..., ge=0, le=100)
    generated_at: str

    @staticmethod
    def from_domain(report: TrainingGapReport) -> "TrainingGapReportResponse":
        """Convert domain model to response."""
        return TrainingGapReportResponse(
            venue_id=report.venue_id,
            critical_gaps=[
                TrainingGapResponse(
                    role=gap.role,
                    current_count=gap.current_count,
                    minimum_needed=gap.minimum_needed,
                    urgency=gap.urgency.value,
                )
                for gap in report.critical_gaps
            ],
            spof_risks=[
                SPOFRiskResponse(
                    role=risk.role,
                    sole_employee_id=risk.sole_employee_id,
                    sole_employee_name=risk.sole_employee_name,
                    backup_candidates=risk.backup_candidates,
                )
                for risk in report.spof_risks
            ],
            training_priorities=[
                TrainingPriorityResponse(
                    role=priority.role,
                    employee_id=priority.employee_id,
                    employee_name=priority.employee_name,
                    reason=priority.reason,
                    impact_score=priority.impact_score,
                )
                for priority in report.training_priorities
            ],
            cross_training_suggestions=[
                CrossTrainSuggestionResponse(
                    employee_id=suggest.employee_id,
                    employee_name=suggest.employee_name,
                    current_skills=suggest.current_skills,
                    suggested_skill=suggest.suggested_skill,
                    reason=suggest.reason,
                )
                for suggest in report.cross_training_suggestions
            ],
            estimated_training_hours=report.estimated_training_hours,
            resilience_score=report.resilience_score,
            generated_at=report.generated_at,
        )


class ResilienceScoreResponse(BaseModel):
    """Just the resilience score."""

    venue_id: str
    score: float = Field(..., ge=0, le=100)
    rating: str  # "excellent", "good", "fair", "poor"
    generated_at: str


class EmployeeVersatilityResponse(BaseModel):
    """Employee versatility profile."""

    employee_id: str
    employee_name: str
    skills: List[str]
    skill_count: int
    versatility_score: float = Field(..., ge=0, le=100)
    critical_roles: List[str]
    can_substitute_for: List[str]

    @staticmethod
    def from_domain(versatility: EmployeeVersatility) -> "EmployeeVersatilityResponse":
        """Convert domain model to response."""
        return EmployeeVersatilityResponse(
            employee_id=versatility.employee_id,
            employee_name=versatility.employee_name,
            skills=versatility.skills,
            skill_count=versatility.skill_count,
            versatility_score=versatility.versatility_score,
            critical_roles=versatility.critical_roles,
            can_substitute_for=versatility.can_substitute_for,
        )


class AbsenceImpactResponse(BaseModel):
    """Impact of employee absence."""

    employee_id: str
    employee_name: str
    affected_roles: List[str]
    coverage_loss: Dict[str, float]
    critical_gaps_created: List[str]
    resilience_score_without: float = Field(..., ge=0, le=100)
    resilience_loss: float

    @staticmethod
    def from_domain(impact: AbsenceImpact) -> "AbsenceImpactResponse":
        """Convert domain model to response."""
        return AbsenceImpactResponse(
            employee_id=impact.employee_id,
            employee_name=impact.employee_name,
            affected_roles=impact.affected_roles,
            coverage_loss=impact.coverage_loss,
            critical_gaps_created=impact.critical_gaps_created,
            resilience_score_without=impact.resilience_score_without,
            resilience_loss=impact.resilience_loss,
        )


class HiringProfileResponse(BaseModel):
    """Recommended hiring profile."""

    primary_skill: str
    secondary_skills: List[str]
    justification: str
    expected_impact_on_resilience: float

    @staticmethod
    def from_domain(profile: HiringProfile) -> "HiringProfileResponse":
        """Convert domain model to response."""
        return HiringProfileResponse(
            primary_skill=profile.primary_skill,
            secondary_skills=profile.secondary_skills,
            justification=profile.justification,
            expected_impact_on_resilience=profile.expected_impact_on_resilience,
        )


# ============================================================================
# ROUTE HANDLERS
# ============================================================================


@router.get(
    "/{venue_id}/skill-matrix",
    response_model=SkillMatrixResponse,
    summary="Get skill matrix for venue",
    description="Returns complete employee-skill matrix with coverage statistics",
)
async def get_skill_matrix(
    venue_id: str = Path(..., description="Venue ID"),
    current_user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get the complete skill matrix for a venue.

    Shows all employees, all roles, and who can do what.
    Includes coverage statistics per role.
    """
    service = SkillMatrixService(db)
    try:
        matrix = service.build_skill_matrix(venue_id)
        return SkillMatrixResponse.from_domain(matrix)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build skill matrix: {str(e)}",
        )


@router.get(
    "/{venue_id}/training-gaps",
    response_model=TrainingGapReportResponse,
    summary="Get training gap analysis",
    description="Returns critical gaps, SPOF risks, and training recommendations",
)
async def get_training_gaps(
    venue_id: str = Path(..., description="Venue ID"),
    current_user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get comprehensive training gap analysis for a venue.

    Identifies:
    - Critical gaps (roles with fewer than 2 trained staff)
    - Single points of failure (only 1 person can do a role)
    - Prioritized training actions
    - Cross-training recommendations
    """
    service = SkillMatrixService(db)
    try:
        report = service.identify_training_gaps(venue_id)
        return TrainingGapReportResponse.from_domain(report)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze training gaps: {str(e)}",
        )


@router.get(
    "/{venue_id}/resilience-score",
    response_model=ResilienceScoreResponse,
    summary="Get resilience score",
    description="Returns overall venue staffing resilience score (0-100)",
)
async def get_resilience_score(
    venue_id: str = Path(..., description="Venue ID"),
    current_user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get the overall resilience score for a venue.

    Score (0-100) reflects ability to maintain operations given staff capabilities.

    Rating:
    - 80-100: Excellent - redundancy across all roles
    - 60-79: Good - most roles have backups
    - 40-59: Fair - some gaps, some SPOF risks
    - 0-39: Poor - critical gaps or many SPOF risks
    """
    service = SkillMatrixService(db)
    try:
        matrix = service.build_skill_matrix(venue_id)
        score = matrix.overall_score

        if score >= 80:
            rating = "excellent"
        elif score >= 60:
            rating = "good"
        elif score >= 40:
            rating = "fair"
        else:
            rating = "poor"

        return ResilienceScoreResponse(
            venue_id=venue_id,
            score=score,
            rating=rating,
            generated_at=datetime.utcnow().isoformat(),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate resilience score: {str(e)}",
        )


@router.get(
    "/employees/{employee_id}/versatility",
    response_model=EmployeeVersatilityResponse,
    summary="Get employee versatility profile",
    description="Returns how many roles/skills an employee has and criticality",
)
async def get_employee_versatility(
    employee_id: str = Path(..., description="Employee ID"),
    current_user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get versatility profile for a single employee.

    Shows:
    - All skills/roles the employee can perform
    - Versatility score (0-100)
    - Critical roles where they're the sole trainer
    - Roles they can back up
    """
    service = SkillMatrixService(db)
    try:
        versatility = service.get_employee_versatility(employee_id)
        return EmployeeVersatilityResponse.from_domain(versatility)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get versatility profile: {str(e)}",
        )


@router.post(
    "/{venue_id}/simulate-absence",
    response_model=AbsenceImpactResponse,
    summary="Simulate employee absence impact",
    description="Shows coverage loss and resilience impact if employee is absent",
)
async def simulate_absence(
    venue_id: str = Path(..., description="Venue ID"),
    employee_id: str = Query(..., description="Employee ID to simulate absence for"),
    current_user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Simulate what happens if a specific employee is absent.

    Shows:
    - Which roles lose coverage
    - How much coverage is lost per role
    - Critical gaps that would be created
    - Overall resilience score without this person
    """
    service = SkillMatrixService(db)
    try:
        impact = service.simulate_absence(venue_id, employee_id)
        return AbsenceImpactResponse.from_domain(impact)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to simulate absence: {str(e)}",
        )


@router.get(
    "/{venue_id}/hiring-profile",
    response_model=HiringProfileResponse,
    summary="Get recommended hiring profile",
    description="Returns ideal skills for next hire based on current gaps",
)
async def get_hiring_profile(
    venue_id: str = Path(..., description="Venue ID"),
    current_user: UserContext = Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Get the ideal hiring profile for the next staff member.

    Recommends primary and secondary skills based on:
    - Critical gaps
    - Single points of failure
    - Current skill distribution
    - Expected impact on resilience
    """
    service = SkillMatrixService(db)
    try:
        profile = service.suggest_hiring_profile(venue_id)
        return HiringProfileResponse.from_domain(profile)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate hiring profile: {str(e)}",
        )
