# RosterIQ Load Testing Suite

Comprehensive load and performance testing for RosterIQ API endpoints using Locust.

## Installation

Install Locust and dependencies:

```bash
pip install locust
```

Optional: Install for performance monitoring:

```bash
pip install locust[zmq] locustio  # For distributed testing
```

## Quick Start

### 1. Run a Smoke Test (Quick Sanity Check)

```bash
./run_load_test.sh smoke
```

This runs 5 concurrent users for 30 seconds against http://localhost:8000.

### 2. Run a Production Load Test

```bash
./run_load_test.sh load
```

This ramps up to 50 concurrent users over 5 minutes, maintaining load for 5 minutes total.

### 3. Run with Custom Target

```bash
./run_load_test.sh load --host http://production.example.com
```

## Available Profiles

### Smoke (Quick Sanity Check)
- **Users**: 5
- **Duration**: 30 seconds
- **Spawn Rate**: 1 user/second
- **Use Case**: Verify basic API functionality before running full tests

```bash
./run_load_test.sh smoke
```

### Load (Moderate Production Load)
- **Users**: 50
- **Duration**: 5 minutes
- **Spawn Rate**: 10 users/second
- **Use Case**: Test normal production-like load

```bash
./run_load_test.sh load
```

### Stress (High Stress)
- **Users**: 100
- **Duration**: 10 minutes
- **Spawn Rate**: 20 users/second
- **Use Case**: Find performance breaking points

```bash
./run_load_test.sh stress
```

### Spike (Sudden Traffic Spike)
- **Users**: 200 (peak)
- **Duration**: 5 minutes
- **Spawn Rate**: 50 users/second
- **Use Case**: Test how system handles sudden spike (e.g., opening time rush)

```bash
./run_load_test.sh spike
```

### Endurance (Long-Running)
- **Users**: 50
- **Duration**: 30 minutes
- **Spawn Rate**: 5 users/second
- **Use Case**: Detect memory leaks or performance degradation over time

```bash
./run_load_test.sh endurance
```

## Manual Testing with Locust UI

Run with interactive web dashboard:

```bash
locust -f tests/load/locustfile.py --host=http://localhost:8000
```

Then open http://localhost:8089 in your browser to start the test and monitor in real-time.

## Environment Variables

```bash
# Target API host
export TARGET_HOST=http://api.example.com:8000

# Tanda webhook signing secret (if using webhook tests)
export TANDA_WEBHOOK_SECRET=your-secret-key

# Number of Locust worker processes
export LOCUST_WORKERS=2

./run_load_test.sh load
```

## Output and Reports

Each test run generates:

1. **HTML Report**: `tests/load/reports/rosteriq_{profile}_{timestamp}.html`
   - Visual charts of request rates, response times, failures
   - Statistics by endpoint
   - Error logs

2. **CSV Data**: `tests/load/reports/rosteriq_{profile}_{timestamp}.csv`
   - Raw metrics for further analysis in Excel/Python

Example:

```
tests/load/reports/
├── rosteriq_smoke_20260426_143025.html
├── rosteriq_smoke_20260426_143025.csv
├── rosteriq_load_20260426_144500.html
└── rosteriq_load_20260426_144500.csv
```

## Expected Baseline Performance

These are target metrics for healthy system performance:

### Response Times (Percentiles)

| Endpoint | P50 (ms) | P95 (ms) | P99 (ms) |
|----------|----------|----------|----------|
| GET /health | 5 | 15 | 30 |
| POST /api/auth/login | 50 | 150 | 300 |
| POST /rosters/generate | 2000 | 5000 | 10000 |
| GET /venues | 20 | 50 | 100 |
| GET /forecasts | 30 | 100 | 200 |
| POST /tanda/webhook | 10 | 30 | 50 |
| GET /api/staff/my-shifts | 40 | 120 | 200 |

### Request Rates

- **Smoke Test**: 5-10 requests/sec (low load)
- **Load Test**: 50-100 requests/sec (normal production)
- **Stress Test**: 150-250 requests/sec (high load)

### Error Rates

- Target: < 1% errors under normal load
- Acceptable under stress: < 5% errors
- Never accept: > 5% errors during smoke test (indicates bugs)

## Interpreting Results

### Key Metrics

**Response Times**: How long requests take
- **Good**: Median < 200ms, P95 < 1s
- **Warning**: P95 > 1s, P99 > 5s
- **Critical**: Timeouts or errors

**Throughput**: Requests per second the system handles
- Under load test: Should handle target RPS without significant slowdown
- Under stress: Throughput should remain stable (not degrade with more users)

**Error Rate**: Percentage of failed requests
- Target: < 1%
- Under load: Watch for auth errors, database timeouts, resource exhaustion

**Concurrent Users**: How many simultaneous users the system handles
- Load profile reaches target without > 1% errors = good
- Stress profile shows degradation = identify bottleneck

## Common Issues and Troubleshooting

### Connection Refused

```
ERROR | Failure in auth: Connection refused / timeout
```

**Solution**: Ensure API is running on the specified host:

```bash
./run_load_test.sh load --host http://localhost:8000
```

### High Error Rates During Auth

```
Failed Auth Operations: 50
```

**Cause**: User registration/login endpoints timing out or failing

**Solution**: Check database, auth service logs

### Rosters Always Fail

```
Failed Roster Generations: 100
```

**Cause**: Roster optimization is slow or crashes on large employee counts

**Solution**: Reduce employee count in `generate_roster_payload()`, profile optimization code

### Memory Leak in Endurance Test

Response times degrade over 30 minutes:

```
First 5 min:  Avg response 100ms
Last 5 min:   Avg response 500ms
```

**Solution**: Check for connection leaks, unbounded caches, missing cleanup in request handlers

## Advanced: Distributed Load Testing

For very high loads (> 500 concurrent users), run Locust in distributed mode:

```bash
# Master node
locust -f tests/load/locustfile.py --master --host=http://api.example.com

# Worker nodes (on different machines)
locust -f tests/load/locustfile.py --worker --master-host=master-ip:5557
```

## Custom Load Profiles

To create a custom profile, edit `tests/load/config.py`:

```python
PROFILES["custom"] = LoadProfile(
    name="custom",
    num_users=75,
    spawn_rate=15.0,
    duration_seconds=900,  # 15 minutes
    description="Custom profile: 75 users over 15 min",
)
```

Then run:

```bash
./run_load_test.sh custom
```

## Analyzing Results

### Export to Excel

```bash
python -c "
import pandas as pd
df = pd.read_csv('tests/load/reports/rosteriq_load_20260426_144500.csv')
df.to_excel('results.xlsx', index=False)
"
```

### Plot Response Times Over Time

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('tests/load/reports/rosteriq_load_20260426_144500.csv')
df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
df.plot(x='Timestamp', y='Response time')
plt.show()
```

## See Also

- [Locust Documentation](https://docs.locust.io/)
- [RosterIQ API Docs](../../docs/api.md)
- [Performance Benchmarks](../benchmarks/bench_core.py)
