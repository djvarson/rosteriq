# RosterIQ Test Runner

Automated test discovery, execution, and coverage reporting system for RosterIQ.

## Files Created

### 1. `tests/run_all_tests.py` (~450 lines)
The core test runner module with:

**Classes:**
- `TestRunner` - Main test discovery and execution engine
- `TestResult` - Result of a single test
- `FileResult` - Aggregated results for a test file
- `TestRunReport` - Overall test run report
- `CoverageReport` - Service coverage analysis

**Methods:**

`TestRunner.discover_tests(test_dir="tests") -> List[Path]`
- Discovers all `test_*.py` files in the test directory
- Returns sorted list of test file paths

`TestRunner._extract_test_functions(file_path: Path) -> List[str]`
- Uses AST (Abstract Syntax Trees) to find all test functions
- Handles both module-level functions and class methods
- Returns list like `["test_function_name", "TestClass.test_method_name"]`

`TestRunner._run_single_test(test_func, test_name: str) -> TestResult`
- Executes a single test function with exception handling
- Captures stdout/stderr during test execution
- Records pass/fail/error status and duration in milliseconds
- Preserves full traceback for failed tests

`TestRunner.run_all(verbose: bool = False) -> TestRunReport`
- Main entry point for running all discovered tests
- Returns comprehensive report with:
  - Total/passed/failed/error/skipped counts
  - Per-file breakdown with test results
  - Top 10 slowest tests (for optimization)
  - Overall execution duration
  - ISO timestamp of run

`TestRunner.generate_coverage_report() -> CoverageReport`
- Maps RosterIQ services to test files
- Heuristic: `test_auth.py` covers `services/auth.py`
- Reports:
  - Total services and coverage percentage
  - List of covered and uncovered services
  - Total test function count

`TestRunner.generate_report(format: str = "text") -> str`
- Formats test results as human-readable text or JSON
- **text format**: Pretty-printed with tables and summaries
- **json format**: Machine-readable with full nested structure

**Data Classes:**
- `TestResult(name, status, duration_ms, error_message, error_type, traceback_text)`
- `FileResult(file_path, test_count, passed, failed, errors, skipped, duration_seconds, test_results)`
- `TestRunReport(total, passed, failed, errors, skipped, duration_seconds, file_results, timestamp, slowest_tests)`
- `CoverageReport(total_services, covered_services, uncovered_services, coverage_percentage, covered, uncovered, total_test_functions)`

### 2. `routes/test_report.py` (~150 lines)
FastAPI routes for test reporting:

**Endpoints:**

`GET /api/v1/admin/test-report`
- Returns last test run results
- Response: `TestResultResponse` with summary, file results, slowest tests, coverage
- Admin-only endpoint
- Returns helpful message if no tests have been run yet

`POST /api/v1/admin/run-tests`
- Triggers a new test run asynchronously
- Request body: `TestRunRequest { verbose: bool }`
- Runs in background; check GET endpoint for results
- Returns confirmation with status "running"
- Admin-only endpoint

`GET /api/v1/admin/test-coverage`
- Returns service-to-test coverage mapping
- Response: `CoverageResponse` with coverage statistics
- Auto-runs tests if not yet executed
- Shows which services have/don't have tests
- Admin-only endpoint

**Data Models:**
- `TestSummary` - Summary counts and timestamp
- `TestResultResponse` - Full test results with coverage
- `TestRunRequest` - Request parameters for running tests
- `TestRunResponse` - Confirmation response
- `CoverageResponse` - Coverage statistics

**Admin Authorization:**
- Currently a placeholder `_admin_required()` dependency
- In production, verify JWT token and admin role
- All three endpoints require admin authorization

### 3. `api.py` (Updated)
Added route registration for test report endpoints:

```python
# Test reporting and coverage analysis routes (admin endpoints)
try:
    from rosteriq.routes.test_report import router as test_report_router
    app.include_router(test_report_router)
    logger.info("Test reporting routes registered at /api/v1/admin/test-*")
except ImportError:
    logger.warning("Test reporting routes unavailable")
except Exception as e:
    logger.error(f"Failed to register test reporting routes: {e}")
```

Registered after skill_matrix routes and before GraphQL schema.

## Usage

### 1. Run All Tests (CLI)

```bash
cd RosterIQ
python -m tests.run_all_tests
```

This will:
1. Discover all test files in `tests/test_*.py`
2. Extract test functions using AST
3. Run each test with error handling
4. Print a human-readable report
5. Save JSON report to `tests/test_report.json`
6. Exit with code 0 (success) or 1 (failures/errors)

### 2. Programmatic Usage

```python
from RosterIQ.tests.run_all_tests import TestRunner

# Create runner
runner = TestRunner()

# Discover tests
test_files = runner.discover_tests()
print(f"Found {len(test_files)} test files")

# Run all tests
report = runner.run_all(verbose=True)

# Print text report
print(runner.generate_report("text"))

# Get JSON report
json_report = runner.generate_report("json")

# Get coverage
coverage = runner.generate_coverage_report()
print(f"Coverage: {coverage.coverage_percentage:.1f}%")
print(f"Services covered: {coverage.covered_services}/{coverage.total_services}")
```

### 3. API Endpoints (FastAPI)

Once RosterIQ is running:

```bash
# Trigger a test run asynchronously
curl -X POST http://localhost:8000/api/v1/admin/run-tests \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -d '{"verbose": false}'

# Get last test results
curl http://localhost:8000/api/v1/admin/test-report \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN"

# Get coverage report
curl http://localhost:8000/api/v1/admin/test-coverage \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

## Test Discovery & Execution Details

### AST-Based Discovery
- Uses Python's `ast` module to parse test files
- Finds all functions starting with `test_`
- Handles class-based tests: `TestClass.test_method`
- No dynamic imports during discovery (safe for large codebases)

### Test Execution
- Each test runs in a try/except block
- Captures stdout/stderr during execution
- Measures execution time in milliseconds
- Preserves full traceback for failed tests
- Isolates tests from each other (module reloading)

### Exception Handling
- **AssertionError** → status="fail" (test assertion failed)
- **Other exceptions** → status="error" (test code error)
- All exceptions preserve type and message

### Performance Tracking
- Records per-test duration
- Tracks per-file total time
- Identifies and reports top 10 slowest tests
- Useful for optimization and CI/CD optimization

## Report Formats

### Text Format (Human-Readable)

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
test_auth_service.py                 45 tests |  45 pass |   0 fail |   0 error (100.0%)
test_api_e2e.py                      32 tests |  31 pass |   1 fail |   0 error (96.9%)
...

TOP 10 SLOWEST TESTS
--------------------------------------------------------------------------------
 1. test_tanda_e2e.test_full_sync                      1234.5ms
 2. test_roster_optimiser.test_complex_scenario        987.2ms
...

SERVICE COVERAGE
--------------------------------------------------------------------------------
Services with tests: 42/50 (84.0%)
Total test functions: 156

Uncovered services:
  - deprecated_module
  - experimental_feature
  ...
================================================================================
```

### JSON Format (Machine-Readable)

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
      "file_path": "/path/to/RosterIQ/tests/test_auth_service.py",
      "test_count": 45,
      "passed": 45,
      "failed": 0,
      "errors": 0,
      "duration_seconds": 2.34,
      "tests": [
        {
          "name": "TestPasswordHashing.test_hash_password_creates_bcrypt_hash",
          "status": "pass",
          "duration_ms": 12.3,
          "error_message": null,
          "error_type": null
        },
        ...
      ]
    },
    ...
  ],
  "slowest_tests": [
    {"name": "test_tanda_e2e.test_full_sync", "duration_ms": 1234.5},
    ...
  ],
  "coverage": {
    "total_services": 50,
    "covered_services": 42,
    "uncovered_services": 8,
    "coverage_percentage": 84.0,
    "covered": ["auth", "billing", "conflicts", ...],
    "uncovered": ["deprecated_module", "experimental_feature", ...],
    "total_test_functions": 156
  }
}
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: Test Suite

on: [push, pull_request]

jobs:
  tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run tests
        run: python -m RosterIQ.tests.run_all_tests
      
      - name: Upload coverage report
        uses: actions/upload-artifact@v3
        with:
          name: test-report
          path: RosterIQ/tests/test_report.json
      
      - name: Comment PR with results
        if: github.event_name == 'pull_request'
        uses: actions/github-script@v6
        with:
          script: |
            # Parse test_report.json and post summary as PR comment
```

## Performance Notes

### Execution Time
- Test discovery: ~100ms for 29 test files
- Test execution: Depends on test count and complexity
- Typical: 10-50 tests/second on modern hardware

### Memory Usage
- AST parsing: Minimal (no imports during discovery)
- Test execution: Depends on test implementation
- Report generation: < 1MB for typical reports

### Optimization Tips
1. **Parallel execution**: Could use `multiprocessing` for independent tests
2. **Caching**: Cache AST parse results across runs
3. **Filtering**: Add `--filter pattern` to run subset of tests
4. **Isolation**: Consider pytest fixtures for better test isolation

## Future Enhancements

1. **Parallel Test Execution**
   - Use `concurrent.futures` to run tests in parallel
   - Maintain test isolation

2. **Test Filtering**
   - `--filter test_name` to run specific tests
   - `--file test_file.py` to run single file

3. **Performance Profiling**
   - Track CPU and memory per test
   - Identify resource-heavy tests

4. **Mock Injection**
   - Provide fixtures for common dependencies
   - Reduce test setup boilerplate

5. **Web Dashboard**
   - Real-time test execution view
   - Historical trend analysis
   - Team notifications

6. **Integration with pytest**
   - Migrate to pytest fixtures
   - Use pytest plugins
   - Support pytest hooks

## Troubleshooting

### "ModuleNotFoundError: No module named 'rosteriq'"
- Ensure RosterIQ package is properly installed
- Check Python path includes RosterIQ root directory

### Tests not discovered
- Verify test files follow `test_*.py` naming convention
- Check test functions start with `test_`
- Use `runner.discover_tests()` to list files

### Tests fail with import errors
- Install all dependencies: `pip install -r requirements.txt`
- Verify DATABASE_URL environment variable
- Check for missing external integrations (Tanda, Xero, etc.)

### Endpoints return 401 Unauthorized
- Implement proper admin authentication in `_admin_required()`
- Add JWT token verification
- Consider using existing auth middleware

## Architecture

```
RosterIQ/
├── tests/
│   ├── run_all_tests.py          # Test runner (this file)
│   ├── test_auth_service.py       # Test modules
│   ├── test_api_e2e.py
│   └── ...
├── routes/
│   ├── test_report.py             # FastAPI endpoints
│   └── ...
├── api.py                         # Updated to register routes
└── services/
    ├── auth.py                    # Services being tested
    └── ...
```

## License

Part of RosterIQ - Australian hospitality AI rostering platform.
