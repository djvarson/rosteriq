"""ROI calculator for RosterIQ landing page."""

from dataclasses import dataclass, field
from typing import List


# Tier pricing: per employee per month (AUD)
TIER_PRICING = {
    "startup": 1.50,
    "pro": 3.00,
    "enterprise": 5.50,
}


@dataclass
class ROIInputs:
    """User inputs for ROI calculation."""
    venue_name: str = ""
    staff_count: int = 0
    weekly_wage_cost: float = 0.0
    tier: str = "pro"
    wage_reduction_pct_low: float = 0.08
    wage_reduction_pct_high: float = 0.15


@dataclass
class ROIResult:
    """ROI calculation result."""
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
    assumptions: List[str] = field(default_factory=list)


def calculate_roi(inputs: ROIInputs) -> ROIResult:
    """
    Calculate ROI for a venue.

    Args:
        inputs: ROIInputs with venue details and tier.

    Returns:
        ROIResult with full financial breakdown and assumptions.

    Raises:
        ValueError: If inputs are invalid.
    """
    # Validate inputs
    if inputs.staff_count < 1:
        raise ValueError("staff_count must be >= 1")
    if inputs.weekly_wage_cost <= 0:
        raise ValueError("weekly_wage_cost must be > 0")
    if inputs.tier not in TIER_PRICING:
        raise ValueError(f"tier must be one of {list(TIER_PRICING.keys())}")
    if not (0 < inputs.wage_reduction_pct_low <= inputs.wage_reduction_pct_high < 1):
        raise ValueError(
            "wage_reduction_pct_low and _high must satisfy: "
            "0 < low <= high < 1"
        )

    # Calculate annual wage cost (52 weeks)
    annual_wage_cost = 52 * inputs.weekly_wage_cost

    # Calculate potential savings
    annual_saving_low = annual_wage_cost * inputs.wage_reduction_pct_low
    annual_saving_high = annual_wage_cost * inputs.wage_reduction_pct_high
    annual_saving_midpoint = (annual_saving_low + annual_saving_high) / 2

    # Calculate subscription cost
    monthly_subscription = inputs.staff_count * TIER_PRICING[inputs.tier]
    annual_subscription = monthly_subscription * 12

    # Calculate net savings
    net_annual_saving_low = annual_saving_low - annual_subscription
    net_annual_saving_high = annual_saving_high - annual_subscription

    # Calculate ROI ratio (saving / subscription)
    roi_ratio_low = annual_saving_low / annual_subscription if annual_subscription > 0 else 0
    roi_ratio_high = annual_saving_high / annual_subscription if annual_subscription > 0 else 0

    # Calculate payback period in weeks
    # How many weeks until cumulative saving covers first year subscription?
    weekly_saving_low = annual_saving_low / 52
    if weekly_saving_low > 0:
        payback_weeks = max(1, int((annual_subscription / weekly_saving_low)))
    else:
        payback_weeks = 52  # Full year if no saving

    # Assumptions list
    assumptions = [
        "52 operating weeks assumed",
        "Savings based on AU hospitality labour cost averages",
        "Actual savings vary with venue mix, labour mix, and award complexity",
        "Subscription cost excludes GST",
        f"Wage reduction of {inputs.wage_reduction_pct_low * 100:.0f}% to {inputs.wage_reduction_pct_high * 100:.0f}% conservative to optimistic case",
    ]

    return ROIResult(
        annual_wage_cost=annual_wage_cost,
        annual_saving_low=annual_saving_low,
        annual_saving_high=annual_saving_high,
        annual_saving_midpoint=annual_saving_midpoint,
        monthly_subscription=monthly_subscription,
        annual_subscription=annual_subscription,
        net_annual_saving_low=net_annual_saving_low,
        net_annual_saving_high=net_annual_saving_high,
        roi_ratio_low=roi_ratio_low,
        roi_ratio_high=roi_ratio_high,
        payback_weeks=payback_weeks,
        assumptions=assumptions,
    )
