"""ROI calculator API endpoints."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List

from .roi_calculator import (
    calculate_roi,
    ROIInputs as CalcROIInputs,
    ROIResult as CalcROIResult,
)


# Pydantic models for API
class ROIInputs(BaseModel):
    """ROI calculation request body."""
    venue_name: str = Field(default="", description="Venue name (optional)")
    staff_count: int = Field(..., gt=0, description="Number of staff")
    weekly_wage_cost: float = Field(..., gt=0, description="Weekly wage cost in AUD")
    tier: str = Field(default="pro", description="Pricing tier: startup, pro, enterprise")
    wage_reduction_pct_low: float = Field(default=0.08, description="Low wage reduction % (0-1)")
    wage_reduction_pct_high: float = Field(default=0.15, description="High wage reduction % (0-1)")


class ROIResult(BaseModel):
    """ROI calculation response."""
    annual_wage_cost: float
    annual_saving_low: float
    annual_saving_high: float
    annual_saving_midpoint: float
    monthly_subscription: float
    annual_subscription: float
    net_annual_saving_low: float
    net_annual_saving_high: float
    roi_ratio_low: float
    roi_ratio_high: float
    payback_weeks: int
    assumptions: List[str]


router = APIRouter(tags=["roi"])


@router.post("/api/v1/roi/calculate", response_model=ROIResult)
def calculate_roi_endpoint(inputs: ROIInputs) -> ROIResult:
    """
    Calculate ROI for a venue.

    Public endpoint — ROI calculator is a lead-gen tool (no auth).

    Request body:
    - venue_name: str (optional)
    - staff_count: int (>= 1)
    - weekly_wage_cost: float (> 0)
    - tier: str (one of: startup, pro, enterprise)
    - wage_reduction_pct_low: float (default 0.08)
    - wage_reduction_pct_high: float (default 0.15)

    Returns ROI breakdown with assumptions.
    """
    try:
        calc_inputs = CalcROIInputs(
            venue_name=inputs.venue_name,
            staff_count=inputs.staff_count,
            weekly_wage_cost=inputs.weekly_wage_cost,
            tier=inputs.tier,
            wage_reduction_pct_low=inputs.wage_reduction_pct_low,
            wage_reduction_pct_high=inputs.wage_reduction_pct_high,
        )
        result: CalcROIResult = calculate_roi(calc_inputs)
        return ROIResult(
            annual_wage_cost=result.annual_wage_cost,
            annual_saving_low=result.annual_saving_low,
            annual_saving_high=result.annual_saving_high,
            annual_saving_midpoint=result.annual_saving_midpoint,
            monthly_subscription=result.monthly_subscription,
            annual_subscription=result.annual_subscription,
            net_annual_saving_low=result.net_annual_saving_low,
            net_annual_saving_high=result.net_annual_saving_high,
            roi_ratio_low=result.roi_ratio_low,
            roi_ratio_high=result.roi_ratio_high,
            payback_weeks=result.payback_weeks,
            assumptions=result.assumptions,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/v1/roi/sample", response_model=ROIResult)
def sample_roi() -> ROIResult:
    """
    Return a sample calculation for a typical 30-staff venue at $35k/week, pro tier.

    Public endpoint — ROI calculator is a lead-gen tool (no auth).
    """
    calc_inputs = CalcROIInputs(
        venue_name="Sample Venue",
        staff_count=30,
        weekly_wage_cost=35000,
        tier="pro",
        wage_reduction_pct_low=0.08,
        wage_reduction_pct_high=0.15,
    )
    result: CalcROIResult = calculate_roi(calc_inputs)
    return ROIResult(
        annual_wage_cost=result.annual_wage_cost,
        annual_saving_low=result.annual_saving_low,
        annual_saving_high=result.annual_saving_high,
        annual_saving_midpoint=result.annual_saving_midpoint,
        monthly_subscription=result.monthly_subscription,
        annual_subscription=result.annual_subscription,
        net_annual_saving_low=result.net_annual_saving_low,
        net_annual_saving_high=result.net_annual_saving_high,
        roi_ratio_low=result.roi_ratio_low,
        roi_ratio_high=result.roi_ratio_high,
        payback_weeks=result.payback_weeks,
        assumptions=result.assumptions,
    )
