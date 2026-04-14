"""Tests for ROI calculator."""

import pytest
from rosteriq.roi_calculator import ROIInputs, calculate_roi


class TestROICalculator:
    """Test ROI calculation functionality."""

    def test_typical_venue_calculation(self):
        """Test calculation for typical 30-staff venue at $35k/week, pro tier."""
        inputs = ROIInputs(
            venue_name="Test Venue",
            staff_count=30,
            weekly_wage_cost=35000,
            tier="pro",
            wage_reduction_pct_low=0.08,
            wage_reduction_pct_high=0.15,
        )
        result = calculate_roi(inputs)

        # Annual wage: 52 * 35000 = 1,820,000
        assert result.annual_wage_cost == 52 * 35000

        # Savings: 8-15% of 1,820,000
        assert result.annual_saving_low == pytest.approx(145600, abs=1)
        assert result.annual_saving_high == pytest.approx(273000, abs=1)
        assert result.annual_saving_midpoint == pytest.approx(
            (145600 + 273000) / 2, abs=1
        )

        # Subscription: 30 * $3/month * 12 = 1,080
        assert result.monthly_subscription == 30 * 3
        assert result.annual_subscription == 30 * 3 * 12

        # Net savings
        assert result.net_annual_saving_low > 0
        assert result.net_annual_saving_high > 0

        # ROI ratio
        assert result.roi_ratio_low > 1
        assert result.roi_ratio_high > 1

        # Payback in weeks
        assert result.payback_weeks >= 1

    def test_payback_weeks_calculation(self):
        """Test that payback_weeks is integer and reasonable."""
        inputs = ROIInputs(
            staff_count=20,
            weekly_wage_cost=20000,
            tier="pro",
        )
        result = calculate_roi(inputs)
        assert isinstance(result.payback_weeks, int)
        assert result.payback_weeks >= 1

    def test_tier_pricing_hierarchy(self):
        """Test that enterprise > pro > startup for same venue."""
        base_inputs = {
            "staff_count": 30,
            "weekly_wage_cost": 35000,
        }

        startup = calculate_roi(ROIInputs(**base_inputs, tier="startup"))
        pro = calculate_roi(ROIInputs(**base_inputs, tier="pro"))
        enterprise = calculate_roi(ROIInputs(**base_inputs, tier="enterprise"))

        assert startup.annual_subscription < pro.annual_subscription
        assert pro.annual_subscription < enterprise.annual_subscription

    def test_validation_staff_count_zero(self):
        """Test that staff_count < 1 raises ValueError."""
        inputs = ROIInputs(
            staff_count=0,
            weekly_wage_cost=10000,
            tier="pro",
        )
        with pytest.raises(ValueError, match="staff_count"):
            calculate_roi(inputs)

    def test_validation_negative_staff(self):
        """Test that negative staff_count raises ValueError."""
        inputs = ROIInputs(
            staff_count=-5,
            weekly_wage_cost=10000,
            tier="pro",
        )
        with pytest.raises(ValueError, match="staff_count"):
            calculate_roi(inputs)

    def test_validation_zero_wage(self):
        """Test that weekly_wage_cost <= 0 raises ValueError."""
        inputs = ROIInputs(
            staff_count=10,
            weekly_wage_cost=0,
            tier="pro",
        )
        with pytest.raises(ValueError, match="weekly_wage_cost"):
            calculate_roi(inputs)

    def test_validation_invalid_tier(self):
        """Test that invalid tier raises ValueError."""
        inputs = ROIInputs(
            staff_count=10,
            weekly_wage_cost=10000,
            tier="invalid",
        )
        with pytest.raises(ValueError, match="tier"):
            calculate_roi(inputs)

    def test_validation_reduction_bounds(self):
        """Test that reduction percentages must satisfy 0 < low <= high < 1."""
        # low > high
        with pytest.raises(ValueError, match="wage_reduction_pct"):
            calculate_roi(
                ROIInputs(
                    staff_count=10,
                    weekly_wage_cost=10000,
                    wage_reduction_pct_low=0.20,
                    wage_reduction_pct_high=0.10,
                )
            )

        # low at 0
        with pytest.raises(ValueError, match="wage_reduction_pct"):
            calculate_roi(
                ROIInputs(
                    staff_count=10,
                    weekly_wage_cost=10000,
                    wage_reduction_pct_low=0.0,
                    wage_reduction_pct_high=0.15,
                )
            )

        # high at 1.0
        with pytest.raises(ValueError, match="wage_reduction_pct"):
            calculate_roi(
                ROIInputs(
                    staff_count=10,
                    weekly_wage_cost=10000,
                    wage_reduction_pct_low=0.08,
                    wage_reduction_pct_high=1.0,
                )
            )

    def test_assumptions_list(self):
        """Test that assumptions list is populated."""
        inputs = ROIInputs(
            staff_count=10,
            weekly_wage_cost=10000,
            tier="pro",
        )
        result = calculate_roi(inputs)
        assert len(result.assumptions) >= 4
        assert any("52" in a for a in result.assumptions)
        assert any("hospitality" in a.lower() for a in result.assumptions)

    def test_sample_calculation(self):
        """Test sample calculation: 30 staff, $35k/week, pro."""
        inputs = ROIInputs(
            venue_name="Sample Venue",
            staff_count=30,
            weekly_wage_cost=35000,
            tier="pro",
        )
        result = calculate_roi(inputs)

        # Just verify it's sensible
        assert result.annual_wage_cost == 52 * 35000
        assert result.monthly_subscription == 90  # 30 * 3
        assert result.annual_subscription == 1080
        assert result.roi_ratio_low > 1
