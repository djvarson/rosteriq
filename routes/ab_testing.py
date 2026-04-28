"""
REST API routes for A/B testing experiments.

Provides endpoints for creating, managing, and analysing roster generation experiments.
"""

import logging
from datetime import date
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.services.ab_testing import (
    ExperimentEngine, StrategyType, ExperimentStatus, ExperimentResult
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/experiments",
    tags=["ab-testing"],
)


# ============================================================================
# Request/Response models
# ============================================================================

class CreateExperimentRequest(BaseModel):
    """Request to create a new experiment."""
    name: str = Field(..., min_length=1, max_length=200)
    description: str = Field(..., min_length=1, max_length=1000)
    control_strategy: StrategyType
    variant_strategy: StrategyType
    start_date: date
    end_date: date


class AssignVenuesRandomRequest(BaseModel):
    """Request to randomly assign venues."""
    venue_ids: List[str] = Field(..., min_items=2)
    control_ratio: float = Field(default=0.5, ge=0.2, le=0.8)
    seed: Optional[int] = None


class AssignVenuesManualRequest(BaseModel):
    """Request to manually assign venues."""
    control_venues: List[str] = Field(..., min_items=1)
    variant_venues: List[str] = Field(..., min_items=1)


class RecordOutcomeRequest(BaseModel):
    """Request to record a roster outcome."""
    venue_id: str
    roster_id: str
    total_labour_cost: float = Field(ge=0)
    labour_percentage: float = Field(ge=0, le=100)
    demand_coverage_pct: float = Field(ge=0, le=100)
    compliance_score: float = Field(ge=0, le=100)
    staff_satisfaction_proxy: float = Field(ge=0, le=100)
    overtime_hours: float = Field(ge=0)
    penalty_hours: float = Field(ge=0)


class ExperimentResponse(BaseModel):
    """Experiment with metadata."""
    id: str
    name: str
    description: str
    control_strategy: str
    variant_strategy: str
    start_date: str
    end_date: str
    status: str
    created_at: str
    updated_at: str
    control_venues: List[str]
    variant_venues: List[str]
    minimum_sample_size: int


class AssignmentResponse(BaseModel):
    """Result of venue assignment."""
    experiment_id: str
    control_venues: List[str]
    variant_venues: List[str]
    seed: Optional[int] = None


class StrategyResponse(BaseModel):
    """Strategy for a venue in an experiment."""
    experiment_id: str
    venue_id: str
    strategy: str


class RecordOutcomeResponse(BaseModel):
    """Confirmation of recorded outcome."""
    outcome_id: str
    experiment_id: str
    venue_id: str
    roster_id: str
    group: str
    recorded_at: str


class MetricComparisonResponse(BaseModel):
    """Comparison of a single metric."""
    metric_name: str
    control_mean: float
    control_stddev: float
    control_n: int
    variant_mean: float
    variant_stddev: float
    variant_n: int
    t_statistic: Optional[float]
    p_value: Optional[float]
    cohens_d: Optional[float]
    is_significant: bool
    effect_direction: str


class ExperimentResultResponse(BaseModel):
    """Complete analysis results for an experiment."""
    experiment_id: str
    is_ready: bool
    readiness_message: str
    analysis_time: Optional[str]
    metrics: dict[str, MetricComparisonResponse]


# ============================================================================
# Route handlers
# ============================================================================

@router.post("", response_model=ExperimentResponse)
async def create_experiment(request: CreateExperimentRequest):
    """
    Create a new A/B experiment.

    - **name**: Human-readable experiment name
    - **description**: Detailed description
    - **control_strategy**: Strategy for control group
    - **variant_strategy**: Strategy for treatment group
    - **start_date**: Experiment start date
    - **end_date**: Experiment end date
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        experiment = engine.create_experiment(
            name=request.name,
            description=request.description,
            control_strategy=request.control_strategy,
            variant_strategy=request.variant_strategy,
            start_date=request.start_date,
            end_date=request.end_date,
        )

        return ExperimentResponse(**experiment)

    except Exception as e:
        logger.exception("Error creating experiment")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[ExperimentResponse])
async def list_experiments(
    active_only: bool = Query(False, description="Show only active experiments")
):
    """
    List all experiments, optionally filtered to active only.

    - **active_only**: If true, only return experiments with status='active'
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        experiments = engine.list_experiments(active_only=active_only)
        return [ExperimentResponse(**exp) for exp in experiments]

    except Exception as e:
        logger.exception("Error listing experiments")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/active", response_model=List[ExperimentResponse])
async def list_active_experiments():
    """List all active experiments."""
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        experiments = engine.list_experiments(active_only=True)
        return [ExperimentResponse(**exp) for exp in experiments]

    except Exception as e:
        logger.exception("Error listing active experiments")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{experiment_id}", response_model=ExperimentResponse)
async def get_experiment(experiment_id: str):
    """Get a single experiment by ID."""
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        experiment = engine.get_experiment(experiment_id)
        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")

        return ExperimentResponse(**experiment)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting experiment {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{experiment_id}/assign/random", response_model=AssignmentResponse)
async def assign_venues_random(
    experiment_id: str,
    request: AssignVenuesRandomRequest,
):
    """
    Randomly assign venues to control/variant groups.

    - **venue_ids**: List of venue IDs to assign
    - **control_ratio**: Proportion for control group (default 0.5 = 50%)
    - **seed**: Optional random seed for reproducibility
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        result = engine.assign_venues_random(
            experiment_id=experiment_id,
            venue_ids=request.venue_ids,
            control_ratio=request.control_ratio,
            seed=request.seed,
        )

        return AssignmentResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error assigning venues for {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{experiment_id}/assign/manual", response_model=AssignmentResponse)
async def assign_venues_manual(
    experiment_id: str,
    request: AssignVenuesManualRequest,
):
    """
    Manually assign venues to control/variant groups.

    - **control_venues**: Venue IDs for control group
    - **variant_venues**: Venue IDs for variant group
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        result = engine.assign_venues_manual(
            experiment_id=experiment_id,
            control_venues=request.control_venues,
            variant_venues=request.variant_venues,
        )

        return AssignmentResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error assigning venues for {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{experiment_id}/strategy/{venue_id}", response_model=StrategyResponse)
async def get_strategy_for_venue(experiment_id: str, venue_id: str):
    """
    Get the strategy a venue should use in an experiment.

    Returns the control strategy, variant strategy, or balanced (default).
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        strategy = engine.get_strategy_for_venue(experiment_id, venue_id)

        return StrategyResponse(
            experiment_id=experiment_id,
            venue_id=venue_id,
            strategy=strategy.value,
        )

    except Exception as e:
        logger.exception(f"Error getting strategy for {experiment_id}/{venue_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{experiment_id}/record", response_model=RecordOutcomeResponse)
async def record_outcome(
    experiment_id: str,
    request: RecordOutcomeRequest,
):
    """
    Record the outcome of a roster generation.

    Call this after generating a roster in an active experiment to track metrics.

    Metrics:
    - **total_labour_cost**: Total cost in dollars
    - **labour_percentage**: Labour cost as % of revenue (0-100)
    - **demand_coverage_pct**: % of forecasted demand met (0-100)
    - **compliance_score**: Award compliance score (0-100)
    - **staff_satisfaction_proxy**: % of preferred shifts honoured (0-100)
    - **overtime_hours**: Total overtime hours
    - **penalty_hours**: Hours subject to penalty rates
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        # Verify experiment exists
        experiment = engine.get_experiment(experiment_id)
        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")

        engine.record_outcome(
            experiment_id=experiment_id,
            venue_id=request.venue_id,
            roster_id=request.roster_id,
            metrics={
                "total_labour_cost": request.total_labour_cost,
                "labour_percentage": request.labour_percentage,
                "demand_coverage_pct": request.demand_coverage_pct,
                "compliance_score": request.compliance_score,
                "staff_satisfaction_proxy": request.staff_satisfaction_proxy,
                "overtime_hours": request.overtime_hours,
                "penalty_hours": request.penalty_hours,
            },
        )

        # Determine which group the venue is in
        is_control = request.venue_id in experiment.get("control_venues", [])
        group = "control" if is_control else "variant"

        return RecordOutcomeResponse(
            outcome_id=str(__import__("uuid").uuid4()),
            experiment_id=experiment_id,
            venue_id=request.venue_id,
            roster_id=request.roster_id,
            group=group,
            recorded_at=__import__("datetime").datetime.utcnow().isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error recording outcome for {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{experiment_id}/results", response_model=ExperimentResultResponse)
async def get_experiment_results(experiment_id: str):
    """
    Get analysis results for an experiment.

    Returns statistical comparisons of control vs variant strategies:
    - Mean and standard deviation per metric per group
    - t-statistics and p-values (significance testing)
    - Cohen's d effect sizes
    - Whether differences are statistically significant (p < 0.05)
    - Direction of effect (improved/degraded/no_change)

    Note: Analysis requires at least 30 rosters per group to be ready.
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        result = engine.analyse_experiment(experiment_id)

        # Convert to response format
        metrics_dict = {}
        for name, comparison in result.metric_comparisons.items():
            metrics_dict[name] = MetricComparisonResponse(**comparison.to_dict())

        return ExperimentResultResponse(
            experiment_id=experiment_id,
            is_ready=result.is_ready,
            readiness_message=result.readiness_message,
            analysis_time=result.analysis_time.isoformat() if result.analysis_time else None,
            metrics=metrics_dict,
        )

    except Exception as e:
        logger.exception(f"Error getting results for {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{experiment_id}/end", response_model=ExperimentResponse)
async def end_experiment(experiment_id: str):
    """
    End an active experiment (mark as 'ended').

    You can still view results of ended experiments.
    """
    try:
        db = get_db()
        engine = ExperimentEngine(db)

        experiment = engine.end_experiment(experiment_id)
        return ExperimentResponse(**experiment)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error ending experiment {experiment_id}")
        raise HTTPException(status_code=500, detail=str(e))
