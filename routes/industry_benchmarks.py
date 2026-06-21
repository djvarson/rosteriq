"""
Industry benchmarking API routes.

Provides REST endpoints for comparing venue labour costs against AU hospitality
industry benchmarks, with percentile rankings and actionable recommendations.

All dates in ISO 8601 format. All monetary values in AUD.
"""

import logging
from datetime import date, datetime
from typing import Optional, List, Dict

from fastapi import APIRouter, HTTPException, Query, Path, Depends
from pydantic import BaseModel

from rosteriq.database import get_db, BaseStore
from rosteriq.services.industry_benchmarks import (
    IndustryBenchmarkService, BenchmarkComparison, BenchmarkRecommendation,
    IndustryBenchmark,
)


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/venues",
    tags=["industry-benchmarks"],
)


# ============================================================================
# Pydantic Response Models
# ============================================================================


class BenchmarkComparisonResponse(BaseModel):
    """Response for venue vs industry benchmark comparison."""
    venue_id: str
    venue_type: str
    period_start: str
    period_end: str
    actual_labour_pct: float
    benchmark_labour_pct: float
    labour_pct_gap: float
    labour_pct_status: str  # "exceeds", "at_target", "below", "critical"
    actual_cost_per_cover: float
    benchmark_cost_per_cover: float
    staff_utilisation: float
    covers_per_staff_hour_actual: float
    covers_per_staff_hour_benchmark: int
    percentile_rank: float
    gap_amount_aud: float
    total_revenue: float
    total_labour_cost: float


class PercentileResponse(BaseModel):
    """Response for percentile ranking."""
    venue_id: str
    metric: str
    percentile_rank: float
    interpretation: str


class RecommendationResponse(BaseModel):
    """Single recommendation."""
    area: str
    current_value: float
    target_value: float
    potential_savings_aud: float
    priority: str
    action: str


class RecommendationsResponse(BaseModel):
    """Response for recommendations list."""
    venue_id: str
    total_potential_savings_aud: float
    recommendations: List[RecommendationResponse]


class IndustryBenchmarkResponse(BaseModel):
    """Industry benchmark data for a venue type."""
    venue_type: str
    description: str
    labour_pct_min: float
    labour_pct_target: float
    labour_pct_max: float
    avg_hourly_cost: float
    covers_per_staff_hour: int


class MultiVenueComparisonResponse(BaseModel):
    """Response for multi-venue comparison."""
    period_start: str
    period_end: str
    venues_compared: int
    comparisons: List[BenchmarkComparisonResponse]
    summary: Dict


class MultiVenueComparisonRequest(BaseModel):
    """Request body for comparing multiple venues against industry benchmarks."""
    venue_configs: List[Dict[str, str]]  # [{venue_id, venue_type}, ...]
    start_date: Optional[str] = None  # Period start (YYYY-MM-DD)
    end_date: Optional[str] = None    # Period end (YYYY-MM-DD)


# ============================================================================
# Helper function for dependency injection
# ============================================================================


def get_industry_benchmark_service(
    db: BaseStore = Depends(get_db),
) -> IndustryBenchmarkService:
    """Get industry benchmark service with database connection."""
    return IndustryBenchmarkService(db)


# ============================================================================
# Endpoints
# ============================================================================


@router.get("/{venue_id}/industry-benchmark")
async def get_venue_industry_benchmark(
    venue_id: str,
    venue_type: str = Query(
        ...,
        description="Venue type (e.g., cafe, restaurant_casual, restaurant_fine_dining, "
                   "bar_pub, hotel, fast_food_qsr, catering)",
    ),
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: IndustryBenchmarkService = Depends(get_industry_benchmark_service),
) -> BenchmarkComparisonResponse:
    """
    Compare venue labour costs against AU hospitality industry benchmark.

    Returns:
    - Actual vs benchmark labour percentage
    - Cost per cover comparison
    - Staff utilisation metrics
    - Percentile ranking (0-100)
    - Gap analysis with AUD amount

    Query params:
    - venue_id: Venue ID (required, in path)
    - venue_type: Industry type (required)
    - start_date: ISO date, default 90 days ago
    - end_date: ISO date, default today
    """
    try:
        comparison: BenchmarkComparison = service.compare_venue(
            venue_id, venue_type, start_date, end_date
        )

        return BenchmarkComparisonResponse(
            venue_id=comparison.venue_id,
            venue_type=comparison.venue_type,
            period_start=comparison.period_start,
            period_end=comparison.period_end,
            actual_labour_pct=comparison.actual_labour_pct,
            benchmark_labour_pct=comparison.benchmark_labour_pct,
            labour_pct_gap=comparison.labour_pct_gap,
            labour_pct_status=comparison.status,
            actual_cost_per_cover=float(comparison.actual_cost_per_cover),
            benchmark_cost_per_cover=float(comparison.benchmark_cost_per_cover),
            staff_utilisation=comparison.staff_utilisation,
            covers_per_staff_hour_actual=comparison.covers_per_staff_hour_actual,
            covers_per_staff_hour_benchmark=comparison.covers_per_staff_hour_benchmark,
            percentile_rank=comparison.percentile_rank,
            gap_amount_aud=float(comparison.gap_amount),
            total_revenue=float(comparison.total_revenue),
            total_labour_cost=float(comparison.total_labour_cost),
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Industry benchmark error for {venue_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Benchmark comparison failed")


@router.get("/{venue_id}/benchmark-percentile")
async def get_venue_percentile(
    venue_id: str,
    venue_type: str = Query(..., description="Venue type"),
    metric: str = Query(
        "labour_pct",
        description="Metric to rank by: labour_pct, cost_per_cover, covers_per_staff",
    ),
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: IndustryBenchmarkService = Depends(get_industry_benchmark_service),
) -> PercentileResponse:
    """
    Get percentile ranking for a venue metric vs industry.

    Percentile range:
    - 0: Worst performer
    - 50: Average performer
    - 100: Best performer (lowest labour %, lowest cost per cover, etc)

    Supported metrics:
    - labour_pct: Labour percentage of revenue (lower is better)
    - cost_per_cover: Cost per cover in AUD (lower is better)
    - covers_per_staff: Covers per staff hour (higher is better)
    """
    try:
        percentile = service.get_percentile(
            venue_id, venue_type, metric, start_date, end_date
        )

        # Generate interpretation
        if percentile >= 85:
            interpretation = "Excellent - top performer in industry"
        elif percentile >= 70:
            interpretation = "Good - above average performance"
        elif percentile >= 50:
            interpretation = "Average - at industry median"
        elif percentile >= 30:
            interpretation = "Below average - improvement opportunities"
        else:
            interpretation = "Poor - significant improvement needed"

        return PercentileResponse(
            venue_id=venue_id,
            metric=metric,
            percentile_rank=percentile,
            interpretation=interpretation,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Percentile error for {venue_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Percentile calculation failed")


@router.get("/{venue_id}/benchmark-recommendations")
async def get_venue_recommendations(
    venue_id: str,
    venue_type: str = Query(..., description="Venue type"),
    start_date: Optional[str] = Query(None, description="Period start (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Period end (YYYY-MM-DD)"),
    service: IndustryBenchmarkService = Depends(get_industry_benchmark_service),
) -> RecommendationsResponse:
    """
    Get actionable recommendations to close industry benchmark gaps.

    Returns prioritised recommendations with:
    - Current vs target values
    - Potential monthly/annual savings in AUD
    - Specific actions to take
    - Priority level (high/medium/low)

    Covers:
    - Labour percentage optimisation
    - Cost per cover improvement
    - Staff utilisation gains
    - Covers per staff hour targets
    """
    try:
        recommendations: List[BenchmarkRecommendation] = service.get_recommendations(
            venue_id, venue_type, start_date, end_date
        )

        total_savings = sum(r.potential_savings for r in recommendations)

        return RecommendationsResponse(
            venue_id=venue_id,
            total_potential_savings_aud=float(total_savings),
            recommendations=[
                RecommendationResponse(
                    area=r.area,
                    current_value=r.current_value,
                    target_value=r.target_value,
                    potential_savings_aud=float(r.potential_savings),
                    priority=r.priority,
                    action=r.action,
                )
                for r in recommendations
            ],
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Recommendations error for {venue_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Recommendation generation failed")


@router.get("/benchmarks/{venue_type}")
async def get_industry_benchmarks(
    venue_type: str = Path(
        ...,
        description="Venue type (cafe, restaurant_casual, restaurant_fine_dining, "
                   "bar_pub, hotel, fast_food_qsr, catering)",
    ),
    service: IndustryBenchmarkService = Depends(get_industry_benchmark_service),
) -> IndustryBenchmarkResponse:
    """
    Get industry benchmark data for a specific venue type.

    Returns standard targets and ranges for:
    - Labour percentage of revenue
    - Average hourly labour cost
    - Covers per staff hour

    Useful for understanding industry standards before comparing venues.
    """
    try:
        benchmark: IndustryBenchmark = service.get_benchmarks(venue_type)

        return IndustryBenchmarkResponse(
            venue_type=benchmark.venue_type,
            description=benchmark.description,
            labour_pct_min=benchmark.labour_pct_min,
            labour_pct_target=benchmark.labour_pct_target,
            labour_pct_max=benchmark.labour_pct_max,
            avg_hourly_cost=float(benchmark.avg_hourly_cost),
            covers_per_staff_hour=benchmark.covers_per_staff_hour,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Benchmark lookup error: {str(e)}")
        raise HTTPException(status_code=500, detail="Benchmark lookup failed")


@router.post("/benchmarks/compare-venues")
async def compare_multiple_venues(
    request: MultiVenueComparisonRequest,
    service: IndustryBenchmarkService = Depends(get_industry_benchmark_service),
) -> MultiVenueComparisonResponse:
    """
    Compare multiple venues against their respective industry benchmarks.

    Request body:
    ```
    {
      "venue_configs": [
        {"venue_id": "venue1", "venue_type": "restaurant_casual"},
        {"venue_id": "venue2", "venue_type": "cafe"}
      ],
      "start_date": "2026-01-27",
      "end_date": "2026-04-27"
    }
    ```

    Returns:
    - Individual benchmark comparisons for each venue
    - Summary statistics across venues
    - Ranking by percentile performance
    """
    try:
        comparisons = service.compare_venues(
            request.venue_configs, request.start_date, request.end_date
        )

        if not comparisons:
            raise ValueError("No valid venue data to compare")

        # Build summary
        percentiles = [c.percentile_rank for c in comparisons]
        labour_gaps = [c.labour_pct_gap for c in comparisons]
        total_gap_amount = sum(c.gap_amount for c in comparisons)

        summary = {
            "best_percentile": max(percentiles) if percentiles else 0,
            "avg_percentile": sum(percentiles) / len(percentiles) if percentiles else 0,
            "worst_percentile": min(percentiles) if percentiles else 0,
            "avg_labour_gap": float(sum(labour_gaps) / len(labour_gaps)) if labour_gaps else 0,
            "total_gap_amount_aud": float(total_gap_amount),
        }

        return MultiVenueComparisonResponse(
            period_start=comparisons[0].period_start if comparisons else "",
            period_end=comparisons[0].period_end if comparisons else "",
            venues_compared=len(comparisons),
            comparisons=[
                BenchmarkComparisonResponse(
                    venue_id=c.venue_id,
                    venue_type=c.venue_type,
                    period_start=c.period_start,
                    period_end=c.period_end,
                    actual_labour_pct=c.actual_labour_pct,
                    benchmark_labour_pct=c.benchmark_labour_pct,
                    labour_pct_gap=c.labour_pct_gap,
                    labour_pct_status=c.status,
                    actual_cost_per_cover=float(c.actual_cost_per_cover),
                    benchmark_cost_per_cover=float(c.benchmark_cost_per_cover),
                    staff_utilisation=c.staff_utilisation,
                    covers_per_staff_hour_actual=c.covers_per_staff_hour_actual,
                    covers_per_staff_hour_benchmark=c.covers_per_staff_hour_benchmark,
                    percentile_rank=c.percentile_rank,
                    gap_amount_aud=float(c.gap_amount),
                    total_revenue=float(c.total_revenue),
                    total_labour_cost=float(c.total_labour_cost),
                )
                for c in comparisons
            ],
            summary=summary,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Multi-venue comparison error: {str(e)}")
        raise HTTPException(status_code=500, detail="Comparison failed")


@router.get("/benchmarks/venue-types")
async def list_venue_types(
    service: IndustryBenchmarkService = Depends(get_industry_benchmark_service),
) -> Dict[str, str]:
    """
    Get list of all supported venue types with descriptions.

    Returns:
    ```
    {
      "cafe": "Café - espresso, takeaway focused",
      "restaurant_casual": "Casual Dining Restaurant",
      ...
    }
    ```
    """
    return service.get_venue_type_descriptions()
