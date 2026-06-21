"""Tests for the variance engine module."""

import pytest
from datetime import datetime

from rosteriq.models import VarianceSignal, SignalType
from rosteriq.variance_engine import (
    calculate_weighted_variance, detect_threshold_breach,
    create_signal, combine_forecasts, get_signal_summary,
    should_trigger_alert, DEFAULT_WEIGHTS,
)


# ============================================================================
# Helpers
# ============================================================================

def make_signal(
    signal_type=SignalType.historical, value=0.0,
    weight=0.3, confidence=0.8, source="test",
) -> VarianceSignal:
    return VarianceSignal(
        signal_type=signal_type, value=value,
        weight=weight, confidence=confidence,
        source=source, timestamp=datetime.now(),
    )


# ============================================================================
# Default weights tests
# ============================================================================

class TestDefaultWeights:
    def test_weights_sum_to_one(self):
        assert sum(DEFAULT_WEIGHTS.values()) == pytest.approx(1.0)

    def test_all_signal_types_present(self):
        # DEFAULT_WEIGHTS is the 5-signal core weighting model (see module
        # docstring); these weights must sum to 1.0. The SignalType enum was
        # later expanded with additional auxiliary signals that fall back to a
        # default weight via DEFAULT_WEIGHTS.get(type, 0.1) and are not part of
        # the core weighted set. Assert the core weighted signals are present.
        core_weighted = {
            SignalType.historical,
            SignalType.bookings,
            SignalType.pos_trends,
            SignalType.weather,
            SignalType.events,
        }
        for st in core_weighted:
            assert st in DEFAULT_WEIGHTS

    def test_historical_highest_weight(self):
        assert DEFAULT_WEIGHTS[SignalType.historical] == 0.30

    def test_events_lowest_weight(self):
        assert DEFAULT_WEIGHTS[SignalType.events] == 0.10


# ============================================================================
# Weighted variance calculation tests
# ============================================================================

class TestCalculateWeightedVariance:
    def test_empty_signals(self):
        assert calculate_weighted_variance([]) == 0.0

    def test_single_positive_signal(self):
        signals = [make_signal(value=0.5, weight=0.3, confidence=1.0)]
        result = calculate_weighted_variance(signals)
        assert result == pytest.approx(0.5)

    def test_single_negative_signal(self):
        signals = [make_signal(value=-0.5, weight=0.3, confidence=1.0)]
        result = calculate_weighted_variance(signals)
        assert result == pytest.approx(-0.5)

    def test_balanced_signals_cancel(self):
        signals = [
            make_signal(value=0.5, weight=0.5, confidence=1.0),
            make_signal(value=-0.5, weight=0.5, confidence=1.0),
        ]
        result = calculate_weighted_variance(signals)
        assert result == pytest.approx(0.0)

    def test_confidence_weighting(self):
        """High-confidence signal should dominate low-confidence."""
        signals = [
            make_signal(value=0.8, weight=0.5, confidence=1.0),
            make_signal(value=-0.8, weight=0.5, confidence=0.1),
        ]
        result = calculate_weighted_variance(signals)
        # High confidence positive should dominate
        assert result > 0.4

    def test_weight_weighting(self):
        """Higher weight signal should contribute more."""
        signals = [
            make_signal(value=0.8, weight=0.9, confidence=1.0),
            make_signal(value=-0.8, weight=0.1, confidence=1.0),
        ]
        result = calculate_weighted_variance(signals)
        assert result > 0.5

    def test_clamped_to_one(self):
        signals = [make_signal(value=5.0, weight=1.0, confidence=1.0)]
        # Value 5.0 is outside [-1, 1] but the formula just divides,
        # then clamps the result
        result = calculate_weighted_variance(signals)
        assert -1.0 <= result <= 1.0

    def test_all_zero_weights(self):
        signals = [make_signal(value=0.5, weight=0.0, confidence=1.0)]
        assert calculate_weighted_variance(signals) == 0.0

    def test_all_zero_confidence(self):
        signals = [make_signal(value=0.5, weight=0.5, confidence=0.0)]
        assert calculate_weighted_variance(signals) == 0.0

    def test_five_signals_realistic(self):
        """Test with all 5 signal types at realistic values."""
        signals = [
            make_signal(SignalType.historical, value=0.2, weight=0.30, confidence=0.9),
            make_signal(SignalType.bookings, value=0.3, weight=0.25, confidence=0.85),
            make_signal(SignalType.pos_trends, value=0.1, weight=0.20, confidence=0.7),
            make_signal(SignalType.weather, value=-0.1, weight=0.15, confidence=0.6),
            make_signal(SignalType.events, value=0.5, weight=0.10, confidence=0.5),
        ]
        result = calculate_weighted_variance(signals)
        # Should be positive (most signals positive)
        assert result > 0.0
        assert result < 1.0


# ============================================================================
# Threshold breach detection tests
# ============================================================================

class TestDetectThresholdBreach:
    def test_no_breach(self):
        assert detect_threshold_breach(0.1) is None

    def test_overstaffed(self):
        assert detect_threshold_breach(-0.2) == "overstaffed"

    def test_understaffed(self):
        assert detect_threshold_breach(0.2) == "understaffed"

    def test_exact_threshold_no_breach(self):
        assert detect_threshold_breach(0.15) is None
        assert detect_threshold_breach(-0.15) is None

    def test_custom_threshold(self):
        assert detect_threshold_breach(0.1, threshold=0.05) == "understaffed"
        assert detect_threshold_breach(-0.1, threshold=0.05) == "overstaffed"

    def test_zero_variance(self):
        assert detect_threshold_breach(0.0) is None


# ============================================================================
# Create signal tests
# ============================================================================

class TestCreateSignal:
    def test_positive_variance(self):
        signal = create_signal(SignalType.bookings, actual=120, expected=100)
        assert signal.value == pytest.approx(0.2)
        assert signal.signal_type == SignalType.bookings

    def test_negative_variance(self):
        signal = create_signal(SignalType.bookings, actual=80, expected=100)
        assert signal.value == pytest.approx(-0.2)

    def test_zero_expected(self):
        signal = create_signal(SignalType.bookings, actual=0.5, expected=0)
        assert -1.0 <= signal.value <= 1.0

    def test_large_variance_clamped(self):
        signal = create_signal(SignalType.bookings, actual=500, expected=100)
        assert signal.value <= 1.0

    def test_large_negative_clamped(self):
        signal = create_signal(SignalType.bookings, actual=0, expected=100)
        assert signal.value >= -1.0

    def test_default_weight_used(self):
        signal = create_signal(SignalType.historical, actual=100, expected=100)
        assert signal.weight == DEFAULT_WEIGHTS[SignalType.historical]

    def test_custom_weight(self):
        signal = create_signal(SignalType.historical, actual=100, expected=100, weight=0.5)
        assert signal.weight == 0.5

    def test_default_confidence(self):
        signal = create_signal(SignalType.weather, actual=10, expected=10)
        assert signal.confidence == 0.8


# ============================================================================
# Combine forecasts tests
# ============================================================================

class TestCombineForecasts:
    def test_no_variance(self):
        signals = [make_signal(value=0.0)]
        result = combine_forecasts(100.0, signals)
        assert result == pytest.approx(100.0)

    def test_positive_adjustment(self):
        signals = [make_signal(value=0.2, weight=1.0, confidence=1.0)]
        result = combine_forecasts(100.0, signals)
        assert result == pytest.approx(120.0)

    def test_negative_adjustment(self):
        signals = [make_signal(value=-0.3, weight=1.0, confidence=1.0)]
        result = combine_forecasts(100.0, signals)
        assert result == pytest.approx(70.0)

    def test_never_negative(self):
        signals = [make_signal(value=-1.0, weight=1.0, confidence=1.0)]
        result = combine_forecasts(50.0, signals)
        assert result >= 0.0

    def test_zero_forecast(self):
        signals = [make_signal(value=0.5)]
        assert combine_forecasts(0.0, signals) == 0.0

    def test_empty_signals(self):
        result = combine_forecasts(100.0, [])
        assert result == pytest.approx(100.0)


# ============================================================================
# Signal summary tests
# ============================================================================

class TestGetSignalSummary:
    def test_empty_signals(self):
        summary = get_signal_summary([])
        assert summary["overall_variance"] == 0.0
        assert summary["dominant_signal"] is None
        assert summary["signal_count"] == 0

    def test_basic_summary(self):
        signals = [
            make_signal(SignalType.historical, value=0.3, weight=0.5, confidence=0.9),
            make_signal(SignalType.weather, value=-0.1, weight=0.2, confidence=0.5),
        ]
        summary = get_signal_summary(signals)
        assert summary["signal_count"] == 2
        assert summary["dominant_signal"] == SignalType.historical
        assert len(summary["signal_details"]) == 2

    def test_breach_status_included(self):
        signals = [make_signal(value=0.5, weight=1.0, confidence=1.0)]
        summary = get_signal_summary(signals)
        assert summary["breach_status"] == "understaffed"


# ============================================================================
# Should trigger alert tests
# ============================================================================

class TestShouldTriggerAlert:
    def test_no_alert(self):
        signals = [make_signal(value=0.05, weight=1.0, confidence=1.0)]
        should, alert_type = should_trigger_alert(signals)
        assert should is False
        assert alert_type is None

    def test_understaffed_alert(self):
        signals = [make_signal(value=0.5, weight=1.0, confidence=1.0)]
        should, alert_type = should_trigger_alert(signals)
        assert should is True
        assert alert_type == "understaffed"

    def test_overstaffed_alert(self):
        signals = [make_signal(value=-0.5, weight=1.0, confidence=1.0)]
        should, alert_type = should_trigger_alert(signals)
        assert should is True
        assert alert_type == "overstaffed"

    def test_custom_threshold(self):
        signals = [make_signal(value=0.1, weight=1.0, confidence=1.0)]
        should, _ = should_trigger_alert(signals, threshold=0.05)
        assert should is True
