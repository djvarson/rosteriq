# Forecast v2 Integration Guide

This document explains how to integrate the enhanced forecasting engine with existing RosterIQ code.

## Files Delivered

```
services/forecast_v2.py (656 lines)
├── SeasonalDecomposer class
├── WeatherModeler class  
├── EventOverlay class
└── EnhancedForecaster class

routes/forecast_v2.py (356 lines)
├── POST /api/forecast/v2/train/{venue_id}
├── GET /api/forecast/v2/predict
├── GET /api/forecast/v2/week/{venue_id}
├── GET /api/forecast/v2/staffing/{venue_id}/{date}
├── GET /api/forecast/v2/accuracy/{venue_id}
└── GET /api/forecast/v2/components/{venue_id}

tests/test_forecast_v2.py (350 lines)
└── 23 comprehensive tests

FORECAST_V2_README.md (full documentation)
FORECAST_V2_INTEGRATION.md (this file)
```

## Step 1: Verify Router Registration

The `api.py` file has been updated to include:

```python
# Enhanced demand forecasting v2 routes
try:
    from rosteriq.routes.forecast_v2 import router as forecast_v2_router
    app.include_router(forecast_v2_router)
    logger.info("Enhanced forecast v2 routes registered")
except ImportError:
    logger.warning("Enhanced forecast v2 routes unavailable")
except Exception as e:
    logger.error(f"Failed to register forecast v2 routes: {e}")
```

**Status**: ✓ Already done. No further action needed.

## Step 2: Load Historical Data (Required for Production)

The routes use a placeholder `_get_forecaster()` function that needs to load historical demand data from your database.

### Current Implementation (Placeholder)

```python
def _get_forecaster(venue_id: str, db: BaseStore) -> EnhancedForecaster:
    """Get or create forecaster for a venue."""
    if venue_id in _forecasters:
        return _forecasters[venue_id]

    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    forecaster = EnhancedForecaster(venue, base_forecaster=None)
    
    # TODO: Load historical data from database
    # forecasts = db.get_forecasts(venue_id=venue_id, start_date=..., end_date=...)
    # forecaster.add_historical_data([...])

    _forecasters[venue_id] = forecaster
    return forecaster
```

### Adapt to Your Data Source

Edit `routes/forecast_v2.py` to load historical data:

```python
def _get_forecaster(venue_id: str, db: BaseStore) -> EnhancedForecaster:
    """Get or create forecaster for a venue."""
    if venue_id in _forecasters:
        return _forecasters[venue_id]

    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    forecaster = EnhancedForecaster(venue, base_forecaster=None)
    
    # OPTION 1: Load from DemandForecast table
    start_date = date.today() - timedelta(days=180)
    forecasts = db.get_forecasts(
        venue_id=venue_id,
        start_date=start_date,
        end_date=date.today(),
    )
    if forecasts:
        forecaster.add_historical_data([
            {
                "date": f.date,
                "hour": f.hour,
                "covers": f.predicted_covers,  # Or actual covers if available
            }
            for f in forecasts
        ])
    
    # OPTION 2: Load from POS data (if available)
    # pos_data = db.get_pos_data(venue_id, start_date, date.today())
    # forecaster.add_historical_data([...])
    
    # OPTION 3: Load from shift covers (covers per shift)
    # shifts = db.get_completed_shifts(venue_id, start_date, date.today())
    # forecaster.add_historical_data([...])

    _forecasters[venue_id] = forecaster
    return forecaster
```

### Data Format Expected

Each data point must have:
- `date`: Python `date` object
- `hour`: Integer 0–23
- `covers`: Float (number of covers/transactions)

```python
[
    {"date": date(2026, 1, 1), "hour": 7, "covers": 23.5},
    {"date": date(2026, 1, 1), "hour": 8, "covers": 45.2},
    {"date": date(2026, 1, 1), "hour": 9, "covers": 62.1},
    # ... 180+ days of data per venue
]
```

## Step 3: (Optional) Integrate with Ensemble Forecaster

If you want to use the existing `EnsembleForecaster` as the base:

### Current Setup

```python
forecaster = EnhancedForecaster(venue, base_forecaster=None)
# Falls back to heuristics
```

### With Ensemble Base

Edit `routes/forecast_v2.py`:

```python
from rosteriq.ensemble import EnsembleForecaster

def _get_forecaster(venue_id: str, db: BaseStore) -> EnhancedForecaster:
    """Get or create forecaster for a venue."""
    if venue_id in _forecasters:
        return _forecasters[venue_id]

    venue = db.get_venue(venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail=f"Venue {venue_id} not found")

    # Create base ensemble forecaster
    base_forecaster = EnsembleForecaster(venue)
    
    # Load historical data for ensemble
    start_date = date.today() - timedelta(days=180)
    forecasts = db.get_forecasts(venue_id, start_date, date.today())
    if forecasts:
        base_forecaster.add_historical_data([...])
        base_forecaster.train()
    
    # Wrap with enhanced forecaster
    forecaster = EnhancedForecaster(venue, base_forecaster=base_forecaster)
    forecaster.add_historical_data([...])

    _forecasters[venue_id] = forecaster
    return forecaster
```

## Step 4: Test the API

### Manual Testing

```bash
# Train forecaster for a venue
curl -X POST http://localhost:8000/api/forecast/v2/train/venue_001

# Get single prediction with optional weather
curl "http://localhost:8000/api/forecast/v2/predict?venue_id=venue_001&date=2026-04-29&hour=12&temp_celsius=22&wind_kmh=15&rain_mm=0&cloud_cover_pct=30"

# Get full week forecast
curl "http://localhost:8000/api/forecast/v2/week/venue_001?week_start=2026-04-27"

# Get staffing recommendations
curl "http://localhost:8000/api/forecast/v2/staffing/venue_001/2026-04-29?covers_per_staff=20"

# Get accuracy report
curl "http://localhost:8000/api/forecast/v2/accuracy/venue_001?period_days=30"

# Get seasonal components
curl "http://localhost:8000/api/forecast/v2/components/venue_001"
```

### Automated Testing

```bash
# Requires pytest
pip install pytest

# Run all forecast_v2 tests
pytest tests/test_forecast_v2.py -v

# Run specific test class
pytest tests/test_forecast_v2.py::TestEnhancedForecaster -v

# Run with coverage
pytest tests/test_forecast_v2.py --cov=services.forecast_v2 --cov=routes.forecast_v2
```

## Step 5: Database Schema Considerations

### Forecast Storage

Existing `DemandForecast` table works as-is. The forecaster reads from it:

```sql
-- From schema.sql (existing)
CREATE TABLE IF NOT EXISTS demand_forecasts (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    predicted_covers NUMERIC(8, 1) NOT NULL,
    confidence NUMERIC(3, 2) NOT NULL,
    signals_used JSONB,
    model_version TEXT,
    FOREIGN KEY (venue_id) REFERENCES venues(id)
);
```

### Optional: Add Accuracy Tracking

To log and track forecast accuracy over time, consider adding:

```sql
CREATE TABLE IF NOT EXISTS forecast_accuracy_log (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_hour INTEGER NOT NULL,
    predicted_covers NUMERIC(8, 1) NOT NULL,
    actual_covers NUMERIC(8, 1),
    error_pct NUMERIC(5, 2),
    logged_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (venue_id) REFERENCES venues(id)
);

CREATE TABLE IF NOT EXISTS seasonal_components (
    venue_id TEXT PRIMARY KEY,
    day_of_week JSONB NOT NULL,  -- {0: 0.95, 1: 0.98, ...}
    month_of_year JSONB NOT NULL, -- {1: 0.75, 7: 0.85, 12: 1.5}
    hour_of_day JSONB NOT NULL,   -- {6: 0.3, 7: 0.6, ..., 23: 0.4}
    trend NUMERIC(6, 4) NOT NULL,
    training_days INTEGER NOT NULL,
    last_retrain TIMESTAMP NOT NULL,
    FOREIGN KEY (venue_id) REFERENCES venues(id)
);
```

## Step 6: Background Training Task (Optional)

For production, schedule periodic retraining:

```python
# In services/task_scheduler.py or similar
from rosteriq.routes.forecast_v2 import _forecasters

async def retrain_forecasters():
    """Periodic task to retrain all forecasters."""
    db = get_db()
    venues = db.list_venues()
    
    for venue in venues:
        if venue.id in _forecasters:
            forecaster = _forecasters[venue.id]
            if forecaster.should_retrain():
                logger.info(f"Retraining forecaster for {venue.name}")
                forecaster.retrain()

# Schedule every hour
scheduler.add_job(retrain_forecasters, 'interval', hours=1)
```

## Step 7: Monitor and Log

### Key Metrics to Track

```python
# In your monitoring system
metrics = {
    "forecast_v2_predictions_total": Counter("Total v2 predictions"),
    "forecast_v2_accuracy_mape": Gauge("Mean absolute % error"),
    "forecast_v2_retrain_count": Counter("Retraining events"),
    "forecast_v2_response_time_ms": Histogram("API response time"),
}

# Example logging
logger.info(f"Forecast for {venue_id}: {result.point_estimate:.1f} "
            f"±{(result.confidence_interval_80[1] - result.confidence_interval_80[0])/2:.1f} "
            f"(confidence: {result.confidence_score:.0%})")
```

### Health Check Endpoint (Optional)

```python
@app.get("/api/forecast/v2/health")
async def forecast_health() -> dict:
    """Health check for forecast v2."""
    return {
        "status": "healthy",
        "forecasters_loaded": len(_forecasters),
        "timestamp": datetime.now().isoformat(),
    }
```

## Integration with Other RosterIQ Components

### Decision Engine Integration

The forecasts can feed into the existing decision engine:

```python
from rosteriq.decision_engine import make_decision

# Get forecast
forecast = forecaster.predict(target_date, target_hour, weather=...)

# Pass to decision engine
decision = make_decision(
    forecast=forecast,
    current_staff=current_count,
    available_staff=available_list,
)
```

### Roster Optimization Integration

Use staffing recommendations to guide optimization:

```python
from rosteriq.roster_optimiser import generate_daily_roster

staffing_needs = forecaster.forecast_staffing_needs(target_date)

# Use as constraint
roster = generate_daily_roster(
    venue=venue,
    date=target_date,
    target_staffing=staffing_needs,  # New parameter
    employees=available_employees,
)
```

### Analytics Integration

Log predictions for analytics:

```python
from rosteriq.services.analytics import AnalyticsService

analytics = AnalyticsService(db)

for rec in staffing_recommendations:
    analytics.log_forecast(
        venue_id=venue_id,
        date=target_date,
        hour=rec["hour"],
        predicted_staff=rec["recommended_staff"],
        confidence=rec["confidence"],
    )
```

## Troubleshooting

### Issue: Routes Not Registering

**Symptom**: 404 on `/api/forecast/v2/...` endpoints

**Check**:
1. Verify `api.py` includes the router (already done)
2. Check logs for import errors
3. Ensure `services/forecast_v2.py` exists and compiles

**Fix**:
```bash
python -c "from rosteriq.routes.forecast_v2 import router; print('OK')"
```

### Issue: No Historical Data Loaded

**Symptom**: Low confidence scores, predictions all fall back to defaults

**Check**:
1. Verify `_get_forecaster()` loads data from DB
2. Confirm DemandForecast table has 180+ days of data
3. Check database connection works

**Fix**:
```python
# Add debug logging
logger.info(f"Loaded {len(forecaster.historical_data)} historical data points")
```

### Issue: High MAPE (>20%)

**Symptom**: Predictions consistently off by >20%

**Check**:
1. Data quality — are covers accurate?
2. Data completeness — any gaps in historical data?
3. Unusual demand patterns — events not captured?

**Fix**:
1. Verify actual covers match forecast column
2. Fill data gaps or exclude bad days
3. Add custom events for known disruptions

### Issue: OOM (Out of Memory)

**Symptom**: Server crashes loading large historical datasets

**Mitigation**:
1. Limit lookback: `decompose(data, lookback_days=90)` instead of 180
2. Sample data: take every 3rd day instead of daily
3. Clear old forecasters: implement `_forecasters` cleanup

```python
# Add in routes/forecast_v2.py
import time

MAX_FORECASTER_AGE = 3600  # seconds
_forecaster_created = {}

def _cleanup_old_forecasters():
    now = time.time()
    for venue_id in list(_forecasters.keys()):
        if now - _forecaster_created[venue_id] > MAX_FORECASTER_AGE:
            del _forecasters[venue_id]
            del _forecaster_created[venue_id]
```

## Performance Tuning

### For High-Traffic Scenarios

1. **Cache forecasters** (already done with `_forecasters` dict)
2. **Batch predictions**: Use `/api/forecast/v2/week/` instead of 168 individual calls
3. **Lazy loading**: Load historical data only on first request, not on startup

### For Large Datasets (100+ venues)

1. **Implement forecaster cleanup** as shown above
2. **Use separate process** for background retraining
3. **Consider async caching**: Load data asynchronously
4. **Implement write-through cache** to DB

## Migration Path

### Phase 1: Parallel Running
- Run forecast_v2 alongside existing forecasts
- Compare predictions (don't use in decisions yet)
- Monitor accuracy for 2–4 weeks

### Phase 2: Gradual Adoption
- Enable for 20% of venues
- Monitor MAPE and forecaster health
- Gather feedback on staffing recommendations

### Phase 3: Full Rollout
- Enable for all venues
- Integrate with decision engine
- Feed into roster optimization
- Retire v1 forecaster if desired

## Support and Monitoring

### Key Logs to Watch

```
"Enhanced forecast v2 routes registered"          # Startup success
"Retraining forecaster for {venue}"               # Auto-retraining
"Training failed for {venue_id}: {error}"         # Data issues
"Week forecast failed for {venue_id}: {error}"    # Runtime errors
```

### Metrics Dashboard

Create dashboards tracking:
- Forecast count by hour
- Average MAPE by venue
- Retrain frequency
- API response times
- Confidence score distribution

### Alerting Rules

```yaml
alerts:
  - name: ForecastAccuracyDegrading
    condition: venue_mape > 0.25
    action: notify_ops
  
  - name: FrequentRetraining
    condition: retrain_count_1h > 5
    action: investigate_data_quality
  
  - name: APISlowdown
    condition: forecast_response_time_p95 > 500ms
    action: scale_resources
```

## Reference

- **Full Documentation**: `FORECAST_V2_README.md`
- **Test Suite**: `tests/test_forecast_v2.py`
- **Service Code**: `services/forecast_v2.py`
- **Routes Code**: `routes/forecast_v2.py`

## Questions?

Refer to the comprehensive README for:
- Architecture details
- API examples
- Data formats
- Troubleshooting
- Future enhancements
