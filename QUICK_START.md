# Test Runner - Quick Start

## Three Ways to Use the Test Runner

### 1. Command Line (Fastest)

```bash
cd RosterIQ
python -m tests.run_all_tests
```

Output:
- Human-readable text report to terminal
- JSON report saved to `tests/test_report.json`
- Exit code 0 (success) or 1 (failures)

### 2. Python API (Programmatic)

```python
from RosterIQ.tests.run_all_tests import TestRunner

# Create runner
runner = TestRunner()

# Run all tests
report = runner.run_all(verbose=True)

# Get text report
print(runner.generate_report("text"))

# Get JSON report
json_str = runner.generate_report("json")

# Get coverage
coverage = runner.generate_coverage_report()
print(f"Coverage: {coverage.coverage_percentage:.1f}%")
```

### 3. HTTP API (FastAPI Endpoints)

```bash
# Start RosterIQ
uvicorn RosterIQ.api:app --reload

# In another terminal:

# Trigger async test run
curl -X POST http://localhost:8000/api/v1/admin/run-tests \
  -H "Content-Type: application/json" \
  -d '{"verbose": false}'

# Get last test results
curl http://localhost:8000/api/v1/admin/test-report

# Get coverage report
curl http://localhost:8000/api/v1/admin/test-coverage
```

## What Gets Generated

### Text Report Example

```
================================================================================
TEST RUN REPORT
================================================================================
Timestamp: 2026-04-27T14:30:00.123456
Total Duration: 12.45s

SUMMARY
--------------------------------------------------------------------------------
Total Tests:     156
Passed:          152 (97.4%)
Failed:          2
Errors:          2
Skipped:         0

PER-FILE RESULTS
--------------------------------------------------------------------------------
test_auth_service.py                 45 tests |  45 pass |   0 fail (100.0%)
test_api_e2e.py                      32 tests |  31 pass |   1 fail (96.9%)

TOP 10 SLOWEST TESTS
--------------------------------------------------------------------------------
 1. test_tanda_e2e.test_full_sync                      1234.5ms
 2. test_roster_optimiser.test_complex_scenario        987.2ms

SERVICE COVERAGE
--------------------------------------------------------------------------------
Services with tests: 42/50 (84.0%)
Total test functions: 156
================================================================================
```

### JSON Report Example

```json
{
  "timestamp": "2026-04-27T14:30:00.123456",
  "duration_seconds": 12.45,
  "summary": {
    "total": 156,
    "passed": 152,
    "failed": 2,
    "errors": 2,
    "skipped": 0
  },
  "file_results": [
    {
      "file_path": "/path/to/test_auth_service.py",
      "test_count": 45,
      "passed": 45,
      "failed": 0,
      "errors": 0,
      "duration_seconds": 2.34
    }
  ],
  "coverage": {
    "total_services": 50,
    "covered_services": 42,
    "coverage_percentage": 84.0,
    "covered": ["auth", "billing", ...],
    "uncovered": ["experimental_feature", ...]
  }
}
```

## Key Features

- **29 test files** discovered automatically
- **AST-based** discovery (no dynamic imports)
- **Class-based** tests supported (TestClass.test_method)
- **Exception handling** (AssertionError → fail, others → error)
- **Performance tracking** (slowest 10 tests identified)
- **Coverage mapping** (services → tests)
- **Two output formats** (text for humans, JSON for CI/CD)
- **Zero dependencies** (Python stdlib only)

## File Locations

```
RosterIQ/
├── tests/
│   ├── run_all_tests.py              # Main test runner
│   ├── test_*.py                     # 29 test files
│   └── test_report.json              # Generated report
├── routes/
│   └── test_report.py                # FastAPI endpoints
├── api.py                            # Updated with routes
├── TEST_RUNNER_README.md             # Full documentation
└── QUICK_START.md                    # This file
```

## Next Steps

1. Read `TEST_RUNNER_README.md` for full documentation
2. Run tests: `python -m tests.run_all_tests`
3. Check results: `cat tests/test_report.json`
4. Integrate with CI/CD using JSON report
5. Customize admin auth in `routes/test_report.py`

## Questions?

See `TEST_RUNNER_README.md` for:
- Detailed API documentation
- Architecture diagrams
- Troubleshooting guide
- CI/CD integration examples
- Performance tuning tips
