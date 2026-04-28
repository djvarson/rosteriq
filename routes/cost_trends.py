"""
Cost trends analytics API routes for RosterIQ.

Provides REST endpoints for:
- Labour cost trends with multi-dimensional breakdowns
- Multi-venue cost comparison
- Cost forecasting
- Overtime analysis
- Casual workforce dependency analysis

All dates in ISO 8601 format (YYYY-MM-DD).
All monetary values in AUD, returned as floats.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from rosteriq.database import get_db, BaseStore
from rosteriq.services.cost_trends import (
    CostTrendsService, CostTrendReport, VenueCostComparison,
    OvertimeAnalysis, CasualDependencyReport,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics", tags=["cost-trends"])


# ============================================================================
# Pydantic Response Models
# ============================================================================

class PeriodCostData(BaseModel):
    """Cost data for a single time period."""
    period_start: str
    period_end: str
    total_cost: float
    hours: float
    shifts: int
    base_cost: float
    penalty_cost: float
    casual_loading: float
    super_cost: float


class CostTrendResponseModel(BaseModel):
    """Labour cost trends response."""
    venue_id: str
    start_date: str
    end_date: str
    group_by: str
    periods: List[PeriodCostData]
    breakdown_by_employment_type: Dict[str, List[PeriodCostData]]
    breakdown_by_day_of_week: Dict[str, float]
    breakdown_by_role: Dict[str, float]
    breakdown_by_cost_type: Dict[str, float]
    cost_per_cover: Optional[List[Dict[str, Any]]] = None
    total_cost: float
    total_hours: float
    average_hourly_cost: float
    trend_direction: str
    trend_percentage: float


class VenueCostMetrics(BaseModel):
    """Cost metrics for a single venue."""
    total_cost: float
    total_hours: float
    unique_employees: int
    average_hourly_cost: float
    casual_hours: float
    casual_cost: float
    casual_percentage: float


class VenueComparisonResponseModel(BaseModel):
    """Multi-venue cost comparison response."""
    start_date: str
    end_date: str
    venues: Dict[str, VenueCostMetrics]
    best_venue: str
    highest_cost_venue: str
    average_cost: float
    cost_variance: float


class CostForecastData(BaseModel):
    """Forecasted cost data for a period."""
    period_start: str
    period_end: str
    predicted_cost: float
    estimated_hours: Optional[float] = None


class CostForecastResponseModel(BaseModel):
    """Cost forecast response."""
    venue_id: str
    forecast_weeks: int
    last_updated: str
    forecasts: List[CostForecastData]


class EmployeeOvertimeData(BaseModel):
    """Overtime data for a single employee."""
    employee_id: str
    employee_name: str
    overtime_hours: float
    overtime_cost: float


class OvertimeAnalysisResponseModel(BaseModel):
    """Overtime analysis response."""
    venue_id: str
    start_date: str
    end_date: str
    total_overtime_hours: float
    total_overtime_cost: float
    employees: List[EmployeeOvertimeData]
    average_overtime_per_employee: float
    overtime_percentage: float


class WeeklyCasualTrendData(BaseModel):
    """Weekly casual workforce trend data."""
    week_start: str
    casual_hours: float
    casual_cost: float
    total_hours: float
    total_cost: float
    casual_percentage: float


class CasualDependencyResponseModel(BaseModel):
    """Casual dependency analysis response."""
    venue_id: str
    start_date: str
    end_date: str
    casual_hours: float
    casual_cost: float
    casual_hours_pct: float
    casual_cost_pct: float
    trend_points: List[WeeklyCasualTrendData]
    recommendation: str


# ============================================================================
# Endpoints
# ============================================================================

@router.get(
    "/cost-trends",
    response_model=CostTrendResponseModel,
    summary="Get labour cost trends",
    description="Analyse labour costs over time with breakdowns by employment type, role, and cost component.",
)
async def get_cost_trends(
    venue_id: str = Query(..., description="Venue ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    group_by: str = Query("weekly", description="Grouping period: daily, weekly, or monthly"),
    db: BaseStore = Depends(get_db),
) -> CostTrendResponseModel:
    """
    Get labour cost trends with multi-dimensional breakdowns.

    Query parameters:
    - venue_id: Venue identifier
    - start_date: Analysis period start (YYYY-MM-DD)
    - end_date: Analysis period end (YYYY-MM-DD)
    - group_by: Aggregation period (daily, weekly, monthly)

    Returns cost breakdown by:
    - Time period
    - Employment type (FT/PT/casual)
    - Day of week
    - Role
    - Cost component (base, penalty, casual loading, super)
    - Cost per cover (if POS data available)

    Includes trend analysis showing cost direction and % change.
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        # Validate inputs
        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        if group_by not in ("daily", "weekly", "monthly"):
            raise HTTPException(
                status_code=400,
                detail="group_by must be one of: daily, weekly, monthly"
            )

        # Get trends
        service = CostTrendsService(db)
        report = service.get_cost_trends(venue_id, start, end, group_by)

        # Convert to response model
        response_dict = report.to_dict()
        response_dict["venue_id"] = venue_id
        response_dict["start_date"] = start_date
        response_dict["end_date"] = end_date
        response_dict["group_by"] = group_by

        return CostTrendResponseModel(**response_dict)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Error getting cost trends: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to calculate cost trends")


@router.get(
    "/cost-trends/compare",
    response_model=VenueComparisonResponseModel,
    summary="Compare costs across venues",
    description="Compare labour costs and efficiency metrics across multiple venues.",
)
async def compare_venues(
    venue_ids: str = Query(..., description="Comma-separated list of venue IDs"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
) -> VenueComparisonResponseModel:
    """
    Compare labour costs across multiple venues.

    Query parameters:
    - venue_ids: Comma-separated list of venue IDs to compare
    - start_date: Analysis period start (YYYY-MM-DD)
    - end_date: Analysis period end (YYYY-MM-DD)

    Returns metrics for each venue:
    - Total cost and hours
    - Average hourly cost
    - Casual workforce breakdown
    - Unique employee count

    Identifies best-performing venue and highest-cost venue.
    """
    try:
        # Parse inputs
        venue_list = [v.strip() for v in venue_ids.split(",")]
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        if not venue_list:
            raise HTTPException(status_code=400, detail="At least one venue_id required")

        # Get comparison
        service = CostTrendsService(db)
        comparison = service.compare_venues(venue_list, start, end)

        # Convert to response model
        response_dict = comparison.to_dict()
        response_dict["start_date"] = start_date
        response_dict["end_date"] = end_date

        # Map venues dict to proper format
        venues_formatted = {
            vid: VenueCostMetrics(**metrics)
            for vid, metrics in response_dict["venues"].items()
        }
        response_dict["venues"] = venues_formatted

        return VenueComparisonResponseModel(**response_dict)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Error comparing venues: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to compare venues")


@router.get(
    "/cost-forecast",
    response_model=CostForecastResponseModel,
    summary="Forecast future labour costs",
    description="Project labour costs for coming weeks using historical trends.",
)
async def get_cost_forecast(
    venue_id: str = Query(..., description="Venue ID"),
    weeks_ahead: int = Query(4, ge=1, le=52, description="Number of weeks to forecast (1-52)"),
    db: BaseStore = Depends(get_db),
) -> CostForecastResponseModel:
    """
    Forecast labour costs using linear regression on historical data.

    Query parameters:
    - venue_id: Venue identifier
    - weeks_ahead: Number of weeks to forecast (default 4, max 52)

    Uses last 12 weeks of data to train a simple linear regression model.
    Projects trend forward to estimate future weekly labour costs.

    Returns:
    - Forecasted cost for each week ahead
    - Last updated timestamp
    """
    try:
        service = CostTrendsService(db)
        forecasts = service.get_cost_forecast(venue_id, weeks_ahead)

        # Convert to response format
        forecast_data = [
            CostForecastData(
                period_start=f.period_start.isoformat(),
                period_end=f.period_end.isoformat(),
                predicted_cost=float(f.total_cost),
                estimated_hours=float(f.hours) if f.hours > 0 else None,
            )
            for f in forecasts
        ]

        return CostForecastResponseModel(
            venue_id=venue_id,
            forecast_weeks=weeks_ahead,
            last_updated=datetime.now().isoformat(),
            forecasts=forecast_data,
        )

    except Exception as e:
        logger.error(f"Error forecasting costs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to forecast labour costs")


@router.get(
    "/overtime",
    response_model=OvertimeAnalysisResponseModel,
    summary="Analyse overtime hours and costs",
    description="Break down overtime by employee and calculate impact.",
)
async def get_overtime_analysis(
    venue_id: str = Query(..., description="Venue ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
) -> OvertimeAnalysisResponseModel:
    """
    Analyse overtime hours and costs.

    Query parameters:
    - venue_id: Venue identifier
    - start_date: Analysis period start (YYYY-MM-DD)
    - end_date: Analysis period end (YYYY-MM-DD)

    Identifies employees exceeding their max hours per week contract.
    Calculates overtime hours and estimated cost at penalty rate (1.5x).

    Returns:
    - Total overtime hours and cost
    - Employee-level breakdown (ranked by overtime)
    - Percentage of total hours that are overtime
    - Average overtime per affected employee
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        # Get overtime analysis
        service = CostTrendsService(db)
        analysis = service.get_overtime_analysis(venue_id, start, end)

        # Convert to response format
        employees_formatted = [
            EmployeeOvertimeData(**emp)
            for emp in analysis.employees
        ]

        return OvertimeAnalysisResponseModel(
            venue_id=venue_id,
            start_date=start_date,
            end_date=end_date,
            total_overtime_hours=float(analysis.total_overtime_hours),
            total_overtime_cost=float(analysis.total_overtime_cost),
            employees=employees_formatted,
            average_overtime_per_employee=float(analysis.average_overtime_per_employee),
            overtime_percentage=analysis.overtime_percentage,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Error analysing overtime: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to analyse overtime")


@router.get(
    "/casual-dependency",
    response_model=CasualDependencyResponseModel,
    summary="Analyse casual workforce dependency",
    description="Measure reliance on casual staff and identify trends.",
)
async def get_casual_dependency(
    venue_id: str = Query(..., description="Venue ID"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    db: BaseStore = Depends(get_db),
) -> CasualDependencyResponseModel:
    """
    Analyse casual workforce dependency over time.

    Query parameters:
    - venue_id: Venue identifier
    - start_date: Analysis period start (YYYY-MM-DD)
    - end_date: Analysis period end (YYYY-MM-DD)

    Calculates:
    - Percentage of hours worked by casual staff
    - Percentage of labour cost from casual workers
    - Weekly trend showing casual dependency changes
    - Contextual recommendation based on metrics

    A healthy mix is typically 30-40% casual for flexibility.
    >60% indicates over-reliance on casual labour.
    <20% may indicate insufficient flexibility.
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")

        # Get casual analysis
        service = CostTrendsService(db)
        report = service.get_casual_dependency_report(venue_id, start, end)

        # Convert to response format
        trend_data = [
            WeeklyCasualTrendData(**point)
            for point in report.trend_points
        ]

        return CasualDependencyResponseModel(
            venue_id=venue_id,
            start_date=start_date,
            end_date=end_date,
            casual_hours=float(report.casual_hours),
            casual_cost=float(report.casual_cost),
            casual_hours_pct=report.casual_hours_pct,
            casual_cost_pct=report.casual_cost_pct,
            trend_points=trend_data,
            recommendation=report.recommendation,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Error analysing casual dependency: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to analyse casual dependency")
