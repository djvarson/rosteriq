# Enhanced Demand Forecasting Engine (Forecast v2)

## Overview

The Enhanced Forecasting Engine (v2) extends RosterIQ's base ensemble forecaster with sophisticated demand prediction capabilities, combining:

- **Seasonal decomposition** — learned day-of-week, month-of-year, and hour-of-day patterns
- **Weather impact modeling** — rain, temperature, wind adjustments
- **Event overlays** — public holidays, school holidays, sports events
- **Confidence intervals** — 80% and 95% bootstrapped confidence bands
- **Auto-retraining** — automatic component refresh when data stales or errors spike
- **Accuracy evaluation** — MAPE, bias, worst-day analysis

## Architecture

### Core Components

#### 1. SeasonalDecomposer
Extracts repeating patterns from 180 days of historical demand data:

```python
decomposer = SeasonalDecomposer()
components = decomposer.decompose(historical_data, lookback_days=180)
# Returns: SeasonalComponents with day/month/hour multipliers + trend
```

**Day-of-week patterns** (Monday=0 → Sunday=6):
- Weekdays: 0.95–1.05 (quieter)
- Friday: ~1.2 (pre-weekend)
- Saturday: ~1.35 (peak)
- Sunday: ~1.15

**Month-of-year patterns** (1–12):
- January: 0.75 (post-Christmas quiet)
- July: 0.85 (school holidays, mid-year slump)
- December: 1.5 (Christmas rush)

**Hour-of-day curves** (0–23):
- Peak hours vary by venue type
- Default: 7–9am (breakfast), 12–2pm (lunch), 6–8pm (dinner)
- Off-peak: 2–6am

**Trend component**:
- Weekly growth/decline rate (e.g., +2% per week)

#### 2. WeatherModeler
Calculates demand multipliers based on real-time weather:

```python
weather = WeatherForecast(
    date=date(2026, 4, 28),
    hour=12,
    temp_celsius=22.0,
    wind_kmh=15.0,
    rain_mm=2.5,
    cloud_cover_pct=60,
)
modifier = weather_modeler.calculate_modifier("beer_garden", weather)
# Returns: float 0.5–1.5 multiplier
```

**Rain impact**:
- Heavy (>10mm): Outdoor –30%, Indoor +5%
- Light (0–10mm): Outdoor –15%, Indoor +2%

**Temperature bands**:
- <5°C: –25% (cold penalty)
- 5–10°C: –15%
- 10–25°C: Neutral (baseline)
- 25–35°C: Outdoor +15% (beer gardens), Indoor –5%
- >35°C: Outdoor –20%, Indoor –10% (heat stress)

**Wind impact**:
- >40 km/h: Outdoor –15%
- >50 km/h: Outdoor –25%

**Cloud cover**:
- Clear (<20%): Outdoor +10%

#### 3. EventOverlay
Applies modifiers for holidays, events, school breaks:

```python
overlay = EventOverlay()
modifier = overlay.calculate_modifier(
    venue_id="venue_001",
    target_date=date(2026, 1, 26),  # Australia Day
    venue_type="restaurant",
    custom_events=[
        {"date": date(2026, 5, 1), "name": "Local festival", "multiplier": 1.5}
    ],
)
# Returns: float 0.5–1.5 multiplier
```

**Built-in Australian holidays**:
- Australia Day (Jan 26): +30% (parties)
- Anzac Day (Apr 25): +20%
- Queen's Birthday (Jun 10): +10%
- Christmas (Dec 25): –50% (most venues closed)
- Boxing Day (Dec 26): +10%

**School holidays** (Apr, Jul, Sep, Dec):
- Family venues: +10%
- Others: –5%

#### 4. EnhancedForecaster
Main forecasting class combining all components:

```python
forecaster = EnhancedForecaster(
    venue_config=venue,
    base_forecaster=ensemble_forecaster,  # optional
)

forecaster.add_historical_data(data)
forecaster.retrain()  # Extract seasonal components

result = forecaster.predict(
    target_date=date(2026, 4, 29),
    target_hour=12,
    weather=weather_forecast,  # optional
    custom_events=[],  # optional
)
# Returns: ForecastResult
```

**ForecastResult fields**:
- `point_estimate`: Central prediction (covers/transactions)
- `confidence_interval_80`: (low, high) at 80% confidence
- `confidence_interval_95`: (low, high) at 95% confidence
- `components`: Dict of seasonal/weather/event contributions
- `confidence_score`: 0–1 (higher = more training data)
- `signals_used`: List of SignalType enums used

### Prediction Formula

```
Prediction = Base × SeasonalFactors × WeatherModifier × EventModifier

Where:
  Base = ensemble forecast or heuristic estimate
  SeasonalFactors = dow_factor × month_factor × hour_factor × trend_factor
  WeatherModifier = 0.5–1.5 (rain/temp/wind)
  EventModifier = 0.5–1.5 (holidays/events)
```

### Confidence Intervals

Intervals calculated via bootstrap resampling from historical residuals:

```python
residuals = (actual - predicted) / predicted  # From historical data

# 80% interval: 10th–90th percentile
# 95% interval: 2.5th–97.5th percentile
```

Falls back to ±25% (80%) and ±50% (95%) when insufficient data.

### Auto-Retraining

Forecaster automatically retrains when:

1. **Age**: >7 days since last training
2. **Error rate**: Mean Absolute Percentage Error (MAPE) > 15% over last 50 predictions

```python
if forecaster.should_retrain():
    forecaster.retrain()
```

### Accuracy Evaluation

```python
report = forecaster.evaluate_accuracy(period_days=30)
# Returns: AccuracyReport with:
#   - mape: Mean Absolute Percentage Error
#   - bias: Average (actual - predicted) / actual
#   - worst_days: List of top 5 worst-performing days
#   - sample_count: Number of predictions evaluated
```

## API Endpoints

### Training

**POST** `/api/forecast/v2/train/{venue_id}`

Trigger seasonal component extraction/update.

```bash
curl -X POST http://localhost:8000/api/forecast/v2/train/venue_001
```

**Response**:
```json
{
  "venue_id": "venue_001",
  "status": "success",
  "message": "Trained on 180 data points",
  "training_days": 180
}
```

### Single Prediction

**GET** `/api/forecast/v2/predict`

Predict demand for a specific hour.

**Query parameters**:
- `venue_id` (required): Venue ID
- `date` (required): Target date (YYYY-MM-DD)
- `hour` (required): Hour (0–23)
- `temp_celsius` (optional): Temperature
- `wind_kmh` (optional): Wind speed
- `rain_mm` (optional): Rainfall
- `cloud_cover_pct` (optional): Cloud cover (0–100)

```bash
curl "http://localhost:8000/api/forecast/v2/predict?venue_id=venue_001&date=2026-04-29&hour=12&temp_celsius=22&rain_mm=0"
```

**Response**:
```json
{
  "venue_id": "venue_001",
  "date": "2026-04-29",
  "hour": 12,
  "point_estimate": 52.3,
  "confidence_interval_80": [44.2, 60.4],
  "confidence_interval_95": [37.1, 67.5],
  "components": {
    "base": 50.0,
    "day_of_week": 1.0,
    "month_of_year": 1.0,
    "hour_of_day": 1.2,
    "trend": 1.01,
    "weather": 0.98,
    "event": 1.0
  },
  "confidence_score": 0.68,
  "model_version": "enhanced_v2"
}
```

### Full Week Forecast

**GET** `/api/forecast/v2/week/{venue_id}`

Generate 168 hourly predictions (7 days × 24 hours).

**Query parameters**:
- `venue_id` (required): Venue ID
- `week_start` (required): Monday of target week (YYYY-MM-DD)

```bash
curl "http://localhost:8000/api/forecast/v2/week/venue_001?week_start=2026-04-27"
```

**Response**:
```json
{
  "venue_id": "venue_001",
  "week_start": "2026-04-27",
  "forecasts": [
    { "date": "2026-04-27", "hour": 0, "point_estimate": 2.1, ... },
    { "date": "2026-04-27", "hour": 1, "point_estimate": 1.8, ... },
    ...
  ]
}
```

### Staffing Recommendations

**GET** `/api/forecast/v2/staffing/{venue_id}/{date_str}`

Convert demand forecasts to staffing headcount.

**Query parameters**:
- `covers_per_staff` (optional, default=20): Covers per FTE

```bash
curl "http://localhost:8000/api/forecast/v2/staffing/venue_001/2026-04-29?covers_per_staff=18"
```

**Response**:
```json
{
  "venue_id": "venue_001",
  "date": "2026-04-29",
  "recommendations": [
    {
      "date": "2026-04-29",
      "hour": 0,
      "recommended_staff": 0.1,
      "confidence": 0.68,
      "low": 0.1,
      "high": 0.3
    },
    {
      "date": "2026-04-29",
      "hour": 7,
      "recommended_staff": 2.8,
      "confidence": 0.68,
      "low": 2.4,
      "high": 3.4
    },
    ...
  ]
}
```

### Accuracy Report

**GET** `/api/forecast/v2/accuracy/{venue_id}`

View model accuracy metrics.

**Query parameters**:
- `period_days` (optional, default=30): Evaluation window

```bash
curl "http://localhost:8000/api/forecast/v2/accuracy/venue_001?period_days=30"
```

**Response**:
```json
{
  "venue_id": "venue_001",
  "mape": 0.082,
  "bias": -0.015,
  "worst_days": [
    ["2026-04-10", 0.25],
    ["2026-04-15", 0.18],
    ["2026-04-20", 0.14]
  ],
  "evaluation_period_days": 30,
  "sample_count": 720
}
```

### Seasonal Components

**GET** `/api/forecast/v2/components/{venue_id}`

View extracted seasonal patterns.

```bash
curl "http://localhost:8000/api/forecast/v2/components/venue_001"
```

**Response**:
```json
{
  "venue_id": "venue_001",
  "day_of_week": {
    "0": 0.95,
    "1": 0.98,
    "2": 1.0,
    "3": 1.05,
    "4": 1.2,
    "5": 1.35,
    "6": 1.15
  },
  "month_of_year": {
    "1": 0.75,
    "7": 0.85,
    "12": 1.5
  },
  "hour_of_day": {
    "6": 0.3,
    "7": 0.6,
    "8": 0.8,
    "12": 1.2,
    "18": 1.0,
    "19": 1.15
  },
  "trend": 0.008,
  "training_days": 180,
  "last_retrain": "2026-04-27T12:34:56.123456"
}
```

## Usage Examples

### Python Integration

```python
from rosteriq.services.forecast_v2 import EnhancedForecaster, WeatherForecast
from rosteriq.models import VenueConfig, State
from datetime import date

# Create venue
venue = VenueConfig(
    id="venue_001",
    name="The Local Cafe",
    tanda_org_id="tanda_001",
    state=State.vic,
    max_labour_pct=30.0,
    pos_system="cafe",
    created_at=datetime.now(),
)

# Initialize forecaster
forecaster = EnhancedForecaster(venue)

# Load historical data (from your database)
historical = [
    {"date": date(2026, 1, 1), "hour": 8, "covers": 45.2},
    {"date": date(2026, 1, 1), "hour": 9, "covers": 62.1},
    # ... 180+ days of data
]
forecaster.add_historical_data(historical)

# Train
forecaster.retrain()

# Predict next Tuesday at 12pm with weather forecast
weather = WeatherForecast(
    date=date(2026, 4, 29),
    hour=12,
    temp_celsius=22.5,
    wind_kmh=12.0,
    rain_mm=0.0,
    cloud_cover_pct=30,
)

result = forecaster.predict(
    date(2026, 4, 29),
    12,
    weather=weather,
)

print(f"Expected covers: {result.point_estimate}")
print(f"80% confidence: {result.confidence_interval_80}")
print(f"95% confidence: {result.confidence_interval_95}")

# Get staffing recommendations for the day
staffing = forecaster.forecast_staffing_needs(
    date(2026, 4, 29),
    covers_per_staff=20.0,
)

for rec in staffing:
    if rec["hour"] in [7, 12, 18]:  # Peak hours
        print(f"{rec['hour']:02d}:00 - Staff: {rec['recommended_staff']:.1f} "
              f"(80% CI: {rec['low']:.1f}–{rec['high']:.1f})")
```

### Batch Processing

```python
# Forecast entire week
week_start = date(2026, 4, 27)  # Monday
results = forecaster.forecast_week(week_start)
print(f"Generated {len(results)} hourly forecasts")

# Log actual results for accuracy tracking
actual_covers = 53.2
forecaster.log_prediction(
    predicted=result.point_estimate,
    actual=actual_covers,
    target_date=date(2026, 4, 29),
)

# Evaluate after sufficient predictions logged
accuracy = forecaster.evaluate_accuracy(period_days=30)
print(f"MAPE: {accuracy.mape:.1%}")
print(f"Bias: {accuracy.bias:.1%}")
print(f"Worst day: {accuracy.worst_days[0]}")
```

## Data Requirements

### Minimum Training Data

- **14 days**: Cold-start mode (heuristic fallback, confidence ~0.3)
- **30 days**: Basic patterns (confidence ~0.5)
- **90+ days**: Robust components (confidence ~0.7+)
- **180 days**: Optimal (captures seasonal cycles)

### Data Format

```python
{
    "date": date(2026, 4, 27),  # date object
    "hour": 12,                   # 0–23
    "covers": 52.3,               # float, number of covers/transactions
}
```

## Configuration

### Venue Types

Forecast respects venue type for weather/event modifiers:
- `cafe` — breakfast-heavy, family-friendly
- `restaurant` — standard day/evening focus
- `bar`, `pub` — evening peak, weather-sensitive
- `beer_garden` — outdoor, highly weather-sensitive
- `rooftop`, `terrace` — outdoor hybrid

### Default Parameters

```python
# Decomposition
DEFAULT_LOOKBACK = 180  # days of historical data to use
DEFAULT_TREND_SPAN = 52  # weeks to calculate trend over

# Weather
RAIN_THRESHOLD_HEAVY = 10  # mm
WIND_THRESHOLD_MEDIUM = 40  # km/h
WIND_THRESHOLD_STRONG = 50  # km/h

# Retraining
RETRAIN_INTERVAL = 7  # days
ERROR_RATE_THRESHOLD = 0.15  # MAPE threshold

# Confidence intervals
CI_80_PERCENTILE = (10, 90)
CI_95_PERCENTILE = (2.5, 97.5)
```

## Troubleshooting

### Low Confidence Scores (<0.3)

- **Cause**: Insufficient training data (<30 days)
- **Fix**: Add more historical demand data, wait 1–2 weeks

### High MAPE (>20%)

- **Cause**: Unstable/seasonal venue, incomplete data
- **Check**: 
  - Are all major holidays/events captured?
  - Any unusual demand patterns?
  - Staff changes affecting service capacity?
- **Fix**: Add custom event multipliers, retrain with extended window

### Prediction Outliers

- **Cause**: Rare events not in training set
- **Fix**: Use custom_events parameter to inject known impacts

### Model Not Auto-Retraining

- **Cause**: Less than 7 days since last train OR MAPE < 15%
- **Check**: Call `forecast.should_retrain()` to diagnose
- **Fix**: Manually call `forecast.retrain()` if needed

## Integration with Ensemble Forecaster

EnhancedForecaster wraps the existing EnsembleForecaster:

```python
from rosteriq.ensemble import EnsembleForecaster

# Optional: use ensemble as base prediction
base = EnsembleForecaster(venue_config)
base.add_historical_data(data)
base.train()

enhanced = EnhancedForecaster(venue_config, base_forecaster=base)
```

When base_forecaster is provided:
- Uses ensemble's weighted XGBoost/Prophet predictions as baseline
- Applies seasonal + weather + event modifiers on top
- Falls back to heuristics if ensemble unavailable

## Performance Notes

- **Training**: O(n) in historical data points, typically <100ms for 180 days
- **Prediction**: O(1) constant time per hour (~0.5ms)
- **Weekly forecast**: 168 predictions ≈ 84ms
- **Memory**: ~5MB per 10,000 historical data points

## Testing

Comprehensive test suite in `tests/test_forecast_v2.py`:

```bash
pytest tests/test_forecast_v2.py -v

# Key test classes:
# - TestSeasonalDecomposer: Component extraction
# - TestWeatherModeler: Weather impact calculation
# - TestEventOverlay: Holiday/event modifiers
# - TestEnhancedForecaster: Integration tests
```

## Future Enhancements

1. **Anomaly detection** — flag unusual demand patterns
2. **Causal inference** — quantify impact of staff changes, menu updates
3. **Multi-step forecasting** — longer-range forecasts with uncertainty growth
4. **External regressors** — integrate foot traffic, nearby events, transport disruptions
5. **Ensemble combinations** — blend multiple forecasting models with learned weights

## License

Part of RosterIQ, Australian hospitality rostering AI.
