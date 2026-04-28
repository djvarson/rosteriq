"""
Venue benchmarking API routes.

Provides REST endpoints for multi-venue comparison, rankings, industry
benchmarking, and efficiency scoring.

All dates in ISO 8601 format. All monetary values in AUD.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

from rosteriq.database import get_db, BaseStore
from rosteriq.services.venue_benchmarks import (
    VenueBenchmarkService, BenchmarkReport, VenueBenchmark,
    ImprovementSuggestion, IndustryComparison,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics/venue-benchmarks", tags=["benchmarks"])


# ============================================================================
# Pydantic Response Models
# ============================================================================


class VenueBenchmarkResponse(BaseModel):
    """Response wrapper for venue benchmark data."""
    venue_id: str
    venue_name: str
    period_start: str
    period_end: str
    labour_pct_of_revenue: float
    cost_per_cover: float
    staff_utilisation: float
    overtime_ratio: float
    casual_dependency: float
    forecast_accuracy: Optional[float]
    roster_efficiency_score: float
    avg_shift_cost: float
    employee_retention_indicator: Optional[float]
    compliance_score: float
    total_labour_cost: float
    total_revenue: float
    total_hours: float
    unique_staff_count: int
    total_shifts: int


class RankingResponse(BaseModel):
    """Metric ranking with venue positions."""
    metric: str
    rankings: List[tuple[str, float]]


class IndustryComparisonResponse(BaseModel):
    """Industry comparison result."""
    venue_id: str
    venue_name: str
    metric_name: str
    current_value: float
    industry_target: float
    industry_min: float
    industry_max: float
    percentile_position: float
    status: str


class EfficiencyScoreResponse(BaseModel):
    """Efficiency score result."""
    venue_id: str
    venue_name: str
    period_start: str
    period_end: str
    efficiency_score: float
    score_breakdown: dict
    recommendation: str


class BenchmarkComparisonResponse(BaseModel):
    """Complete benchmark comparison report."""
    report_generated_at: str
    period_start: str
    period_end: str
    venue_count: int
    venue_benchmarks: List[VenueBenchmarkResponse]
    rankings: dict
    best_practices: List[str]
    improvement_opportunities: List[dict]


class InsightsResponse(BaseModel):
    """Auto-generated insights."""
    insights: List[str]
    generated_at: str
    venue_count: int


# ============================================================================
# Helper function for dependency injection
# ============================================================================


def get_benchmark_service(db: BaseStore = Depends(get_db)) -> VenueBenchmarkService:
    """Get benchmark service with database connection."""
    return VenueBenchmarkService(db)


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/compare")
async def compare_venues(
    venue_ids: List[str] = Query(..., description="Venue IDs to compare"),
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: VenueBenchmarkService = Depends(get_benchmark_service),
) -> BenchmarkComparisonResponse:
    """
    Compare metrics across multiple venues.

    Returns comprehensive benchmarking report with:
    - Individual venue metrics
    - Rankings by metric
    - Best practices from top performers
    - Improvement opportunities with estimated savings

    Query params:
    - venue_ids: List of venue IDs (required)
    - start_date: ISO date, default 90 days ago
    - end_date: ISO date, default today
    """
    try:
        # Parse dates
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None

        # Generate report
        report: BenchmarkReport = service.benchmark_venues(venue_ids, start, end)

        # Convert to response
        venue_responses = [
            VenueBenchmarkResponse(
                venue_id=b.venue_id,
                venue_name=b.venue_name,
                period_start=b.period_start,
                period_end=b.period_end,
                labour_pct_of_revenue=float(b.labour_pct_of_revenue),
                cost_per_cover=float(b.cost_per_cover),
                staff_utilisation=b.staff_utilisation,
                overtime_ratio=b.overtime_ratio,
                casual_dependency=b.casual_dependency,
                forecast_accuracy=b.forecast_accuracy,
                roster_efficiency_score=b.roster_efficiency_score,
                avg_shift_cost=float(b.avg_shift_cost),
                employee_retention_indicator=b.employee_retention_indicator,
                compliance_score=b.compliance_score,
                total_labour_cost=float(b.total_labour_cost),
                total_revenue=float(b.total_revenue),
                total_hours=float(b.total_hours),
                unique_staff_count=b.unique_staff_count,
                total_shifts=b.total_shifts,
            )
            for b in report.venue_benchmarks
        ]

        # Convert improvement opportunities
        improvements = [
            {
                "venue_id": s.venue_id,
                "venue_name": s.venue_name,
                "metric": s.metric,
                "current_value": s.current_value,
                "target_value": s.target_value,
                "potential_saving": float(s.potential_saving),
                "suggestion": s.suggestion,
            }
            for s in report.improvement_opportunities
        ]

        return BenchmarkComparisonResponse(
            report_generated_at=report.report_generated_at,
            period_start=report.period_start,
            period_end=report.period_end,
            venue_count=report.venue_count,
            venue_benchmarks=venue_responses,
            rankings=report.rankings,
            best_practices=report.best_practices,
            improvement_opportunities=improvements,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Benchmarking error: {str(e)}")
        raise HTTPException(status_code=500, detail="Benchmarking failed")


@router.get("/rankings")
async def get_rankings(
    metric: str = Query(..., description="Metric to rank by (e.g., labour_pct_of_revenue)"),
    venue_ids: Optional[List[str]] = Query(None, description="Specific venues (optional)"),
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: VenueBenchmarkService = Depends(get_benchmark_service),
) -> RankingResponse:
    """
    Rank venues by a specific metric.

    Supported metrics:
    - labour_pct_of_revenue: Lower is better
    - cost_per_cover: Lower is better
    - staff_utilisation: Higher is better
    - casual_dependency: Lower is better
    - roster_efficiency_score: Higher is better
    - compliance_score: Higher is better
    """
    try:
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None

        rankings = service.rank_venues(metric, venue_ids, start, end)

        return RankingResponse(
            metric=metric,
            rankings=rankings,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Ranking error: {str(e)}")
        raise HTTPException(status_code=500, detail="Ranking failed")


@router.get("/{venue_id}/industry")
async def get_industry_comparison(
    venue_id: str,
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: VenueBenchmarkService = Depends(get_benchmark_service),
) -> dict:
    """
    Compare venue against AU hospitality industry benchmarks.

    Returns comparisons for:
    - Labour % of revenue (target 28-35%, optimal 32%)
    - Cost per cover (target $8-12, optimal $10)
    - Casual dependency (target <35%)

    For each metric:
    - Current value
    - Industry target & range
    - Percentile position (0-100)
    - Status: exceeds, on_track, below, critical
    """
    try:
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None

        comparisons: List[IndustryComparison] = service.get_industry_comparison(
            venue_id, start, end
        )

        return {
            "venue_id": venue_id,
            "comparisons": [
                {
                    "metric_name": c.metric_name,
                    "current_value": c.current_value,
                    "industry_target": c.industry_target,
                    "industry_min": c.industry_min,
                    "industry_max": c.industry_max,
                    "percentile_position": c.percentile_position,
                    "status": c.status,
                }
                for c in comparisons
            ],
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Industry comparison error: {str(e)}")
        raise HTTPException(status_code=500, detail="Industry comparison failed")


@router.post("/insights")
async def get_insights(
    venue_ids: List[str] = Query(..., description="Venue IDs to analyse"),
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: VenueBenchmarkService = Depends(get_benchmark_service),
) -> InsightsResponse:
    """
    Generate auto-generated insights comparing venues.

    Examples:
    - "Venue X spends 15% more on labour than Venue Y primarily due to higher casual dependency"
    - "Venue B has the best staff utilisation at 92% vs venue A's 78%"

    Compares metrics and identifies patterns across venues.
    """
    try:
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None

        insights = service.generate_insights(venue_ids, start, end)

        return InsightsResponse(
            insights=insights,
            generated_at=datetime.now().isoformat(),
            venue_count=len(venue_ids),
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Insights generation error: {str(e)}")
        raise HTTPException(status_code=500, detail="Insights generation failed")


@router.get("/{venue_id}/efficiency")
async def get_efficiency_score(
    venue_id: str,
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: VenueBenchmarkService = Depends(get_benchmark_service),
) -> EfficiencyScoreResponse:
    """
    Get composite efficiency score (0-100) for a venue.

    Score weighs:
    - Labour % of revenue (30%): closer to 32% target is better
    - Staff utilisation (25%): higher is better
    - Cost per cover (20%): lower is better
    - Casual dependency (15%): lower is better
    - Compliance (10%): higher is better

    Includes breakdown and recommendations.
    """
    try:
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None

        score = service.calculate_efficiency_score(venue_id, start, end)

        # Get benchmark for detailed breakdown
        benchmark = service._calculate_venue_benchmark(
            venue_id,
            start or (date.today() - timedelta(days=90)),
            end or date.today(),
        )

        venue = service.db.get_venue(venue_id)

        # Calculate component scores for breakdown
        labour_pct = float(benchmark.labour_pct_of_revenue)
        labour_target = 0.32
        labour_deviation = abs(labour_pct - labour_target)
        labour_component = max(0, 100 - (labour_deviation * 200))

        util_component = min(100, benchmark.staff_utilisation)

        cost_per_cover = float(benchmark.cost_per_cover)
        cost_target = 10.0
        cost_deviation = abs(cost_per_cover - cost_target)
        cost_component = max(0, 100 - (cost_deviation * 10))

        casual_component = max(0, 100 - (benchmark.casual_dependency * 2))

        # Generate recommendation
        if score >= 85:
            recommendation = "Excellent performance. Maintain current practices and consider sharing insights across other venues."
        elif score >= 70:
            recommendation = "Good performance. Focus on casual dependency and cost per cover to reach top tier."
        elif score >= 55:
            recommendation = "Moderate performance. Implement improvements to labour % and staff utilisation."
        else:
            recommendation = "Below target. Prioritise reducing labour costs and improving staff scheduling efficiency."

        return EfficiencyScoreResponse(
            venue_id=venue_id,
            venue_name=venue.name if venue else "Unknown",
            period_start=(start or (date.today() - timedelta(days=90))).isoformat(),
            period_end=(end or date.today()).isoformat(),
            efficiency_score=score,
            score_breakdown={
                "labour_pct_component": labour_component,
                "staff_utilisation_component": util_component,
                "cost_per_cover_component": cost_component,
                "casual_dependency_component": casual_component,
                "compliance_component": benchmark.compliance_score,
            },
            recommendation=recommendation,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Efficiency score error: {str(e)}")
        raise HTTPException(status_code=500, detail="Efficiency scoring failed")
