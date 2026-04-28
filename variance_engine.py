"""
Demand variance detection engine for RosterIQ.

Takes multiple real-time signals and produces a weighted variance score
indicating whether current demand is above or below forecast. This score
drives the decision engine's cut/call-in recommendations.

The 5 signals and default weights:
- historical (0.30): comparison to same day/time last week and last year
- bookings (0.25): current booking count vs forecast
- pos_trends (0.20): real-time POS transaction rate vs expected
- weather (0.15): weather impact on venue traffic
- events (0.10): local events (sports, concerts, holidays) impact
"""

from datetime import datetime
from typing import Optional

from rosteriq.models import VarianceSignal, SignalType, DemandForecast


# Default signal weights — must sum to 1.0
DEFAULT_WEIGHTS: dict[SignalType, float] = {
    SignalType.historical: 0.30,
    SignalType.bookings: 0.25,
    SignalType.pos_trends: 0.20,
    SignalType.weather: 0.15,
    SignalType.events: 0.10,
}


def calculate_weighted_variance(signals: list[VarianceSignal]) -> float:
    """
    Calculate overall variance score from multiple signals.

    Each signal's contribution is weighted by both its configured weight
    and its confidence score. The result is normalised to [-1.0, +1.0]:
    - Negative values = demand below forecast (overstaffed)
    - Positive values = demand above forecast (understaffed)
    - Zero = on forecast

    Formula:
        sum(signal.value * signal.weight * signal.confidence)
        / sum(signal.weight * signal.confidence)

    Args:
        signals: List of VarianceSignal objects with value, weight, and confidence.

    Returns:
        Weighted variance score clamped to [-1.0, 1.0].
        Returns 0.0 for empty or all-zero-weight inputs.
    """
    if not signals:
        return 0.0

    numerator = 0.0
    denominator = 0.0

    for signal in signals:
        effective_weight = signal.weight * signal.confidence
        numerator += signal.value * effective_weight
        denominator += effective_weight

    if denominator == 0.0:
        return 0.0

    variance = numerator / denominator
    return max(-1.0, min(1.0, variance))


def detect_threshold_breach(
    variance: float, threshold: float = 0.15
) -> Optional[str]:
    """
    Check if variance breaches the action threshold.

    Args:
        variance: Weighted variance score (-1.0 to 1.0).
        threshold: Absolute variance level that triggers action (default 0.15).

    Returns:
        'overstaffed' if variance < -threshold,
        'understaffed' if variance > threshold,
        None if within acceptable range.
    """
    if variance < -threshold:
        return "overstaffed"
    elif variance > threshold:
        return "understaffed"
    return None


def create_signal(
    signal_type: SignalType,
    actual: float,
    expected: float,
    confidence: float = 0.8,
    source: str = "system",
    weight: Optional[float] = None,
) -> VarianceSignal:
    """
    Create a VarianceSignal from actual vs expected values.

    The signal value is calculated as (actual - expected) / expected,
    clamped to [-1.0, 1.0]. If expected is zero, uses actual as the
    raw value (clamped).

    Args:
        signal_type: Type of signal (weather, events, historical, etc.)
        actual: Actual observed value.
        expected: Expected/forecast value.
        confidence: Confidence in this signal (0-1, default 0.8).
        source: Source identifier for this signal.
        weight: Override weight (uses DEFAULT_WEIGHTS if None).

    Returns:
        VarianceSignal ready for use in calculate_weighted_variance.
    """
    if expected == 0:
        raw_value = max(-1.0, min(1.0, actual))
    else:
        raw_value = (actual - expected) / expected
        raw_value = max(-1.0, min(1.0, raw_value))

    if weight is None:
        weight = DEFAULT_WEIGHTS.get(signal_type, 0.1)

    return VarianceSignal(
        signal_type=signal_type,
        value=raw_value,
        weight=weight,
        confidence=confidence,
        source=source,
        timestamp=datetime.now(),
    )


def combine_forecasts(
    historical_forecast: float,
    realtime_signals: list[VarianceSignal],
) -> float:
    """
    Adjust a historical forecast using real-time variance signals.

    Applies the weighted variance as a multiplier to the base forecast.

    Args:
        historical_forecast: Base predicted covers from the ensemble model.
        realtime_signals: Current real-time signals.

    Returns:
        Adjusted predicted covers (never negative).
    """
    if historical_forecast <= 0:
        return 0.0

    variance = calculate_weighted_variance(realtime_signals)
    adjusted = historical_forecast * (1.0 + variance)
    return max(0.0, adjusted)


def get_signal_summary(signals: list[VarianceSignal]) -> dict:
    """
    Produce a summary of the current signal state.

    Args:
        signals: List of current VarianceSignal objects.

    Returns:
        Dict with keys:
        - overall_variance: float (-1 to 1)
        - dominant_signal: SignalType or None
        - breach_status: 'overstaffed', 'understaffed', or None
        - signal_count: int
        - signal_details: list of dicts with per-signal info
    """
    if not signals:
        return {
            "overall_variance": 0.0,
            "dominant_signal": None,
            "breach_status": None,
            "signal_count": 0,
            "signal_details": [],
        }

    overall = calculate_weighted_variance(signals)
    breach = detect_threshold_breach(overall)

    # Find dominant signal (highest absolute contribution)
    dominant = None
    max_contribution = 0.0
    details = []

    for signal in signals:
        contribution = abs(signal.value * signal.weight * signal.confidence)
        details.append({
            "type": signal.signal_type,
            "value": signal.value,
            "weight": signal.weight,
            "confidence": signal.confidence,
            "contribution": contribution,
            "source": signal.source,
        })
        if contribution > max_contribution:
            max_contribution = contribution
            dominant = signal.signal_type

    return {
        "overall_variance": overall,
        "dominant_signal": dominant,
        "breach_status": breach,
        "signal_count": len(signals),
        "signal_details": details,
    }


def should_trigger_alert(
    signals: list[VarianceSignal], threshold: float = 0.15
) -> tuple[bool, Optional[str]]:
    """
    Convenience function to check if signals warrant an alert.

    Args:
        signals: List of current VarianceSignal objects.
        threshold: Variance threshold for triggering alerts.

    Returns:
        Tuple of (should_alert: bool, alert_type: str or None).
    """
    variance = calculate_weighted_variance(signals)
    breach = detect_threshold_breach(variance, threshold)
    return (breach is not None, breach)
