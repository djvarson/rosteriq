# RosterIQ End-to-End Integration Tests

Comprehensive integration tests for the RosterIQ FastAPI application. Tests exercise real API workflows end-to-end using TestClient.

## Overview

- **test_api_e2e.py**: 1,102 lines, 16 test classes, 60 test methods
- **test_api_auth_e2e.py**: 758 lines, 9 test classes, 30 test methods
- **Total**: 25 test classes, 90 test methods, 40+ endpoints covered

## Quick Start

### Install Dependencies

```bash
pip install pytest pytest-asyncio fastapi httpx pydantic
```

### Run Tests

```bash
# All tests
pytest tests/test_api_e2e.py tests/test_api_auth_e2e.py -v

# Just main API tests
pytest tests/test_api_e2e.py -v

# Just auth tests
pytest tests/test_api_auth_e2e.py -v

# Specific test class
pytest tests/test_api_e2e.py::TestVenueCRUD -v

# With coverage
pytest tests/test_api_e2e.py tests/test_api_auth_e2e.py --cov=rosteriq
```

## Test Architecture

### test_api_e2e.py

Main API integration tests covering all major workflows.

#### Test Classes

1. **TestHealthMonitoring** (4 tests)
   - Health checks, readiness probes, metrics

2. **TestVenueCRUD** (7 tests)
   - Venue creation, retrieval, listing, pagination

3. **TestEmployeeManagement** (7 tests)
   - Employee CRUD, bulk operations, filtering

4. **TestForecastManagement** (5 tests)
   - Add forecasts, list, filter by venue/date range, paginate

5. **TestRosterGenerationFlow** (6 tests)
   - Complete setup, roster generation, error handling

6. **TestRosterAnalysis** (2 tests)
   - Roster analysis and suggestions

7. **TestWebhookFlow** (3 tests)
   - Tanda webhook reception and handling

8. **TestDemoDataFlow** (4 tests)
   - Demo data seeding and verification

9. **TestErrorHandling** (9 tests)
   - 404, 422, invalid inputs, missing resources

10. **TestCostAndVariance** (3 tests)
    - Cost calculations, variance detection

11. **TestAwardRules** (3 tests)
    - Day types, penalty rates, public holidays

12. **TestForecastingEndpoints** (3 tests)
    - Required staff calculations

13. **TestPOSImport** (2 tests)
    - POS CSV import workflow

14. **TestDailyRosterGeneration** (2 tests)
    - Daily roster generation

15. **TestCacheStats** (1 test)
    - Cache statistics endpoint

16. **TestFullWorkflow** (2 tests)
    - End-to-end real-world scenarios

### test_api_auth_e2e.py

Authentication and authorization tests.

#### Test Classes

1. **TestUserRegistration** (5 tests)
   - New user signup, role assignment, duplicate handling

2. **TestUserLogin** (4 tests)
   - Valid/invalid credentials, missing fields

3. **TestTokenRefresh** (3 tests)
   - Token refresh, expiration handling

4. **TestCurrentUserProfile** (4 tests)
   - Get/update user profile, auth validation

5. **TestProtectedEndpoints** (3 tests)
   - Auth requirement verification

6. **TestUserLogout** (3 tests)
   - Token revocation, cleanup

7. **TestAPIKeyGeneration** (3 tests)
   - API key creation and management

8. **TestUpdateUserProfile** (3 tests)
   - Name updates, empty field handling

9. **TestAuthenticationFlowIntegration** (2 tests)
   - Full auth lifecycle (register → login → access → refresh → logout)

## Key Features

### Independent Tests
- Each test runs with fresh database (reset_database fixture)
- No shared state between tests
- No test-order dependencies

### Comprehensive Validation
- HTTP status code assertions
- Response body structure validation
- Data type verification
- Field presence checks
- Value correctness assertions

### Helper Functions
Reduce code duplication with reusable setup helpers:
- `create_test_venue()`: Create test venue
- `create_test_employee()`: Create test employee
- `create_test_forecasts()`: Create week of forecasts
- `setup_roster_prerequisites()`: Full roster setup
- `setup_user_and_login()`: Auth user setup

### Error Scenarios
- 404 Not Found
- 422 Unprocessable Entity (validation)
- 401 Unauthorized
- 409 Conflict (duplicates)
- 400 Bad Request
- Invalid JSON
- Missing fields

### Real-World Workflows
- Complete roster generation flow
- Full authentication lifecycle
- Multi-venue data isolation
- Token refresh cycles
- Pagination and filtering
- Bulk operations

## API Endpoints Tested (40+)

### Health & Monitoring
- GET /health
- GET /ready
- GET /metrics
- GET /

### Venue Management
- POST /venues
- GET /venues
- GET /venues/{id}

### Employee Management
- POST /employees
- POST /employees/bulk
- GET /employees
- GET /employees/{id}

### Forecasts
- POST /forecasts
- GET /forecasts
- GET /forecasts/required-staff

### Roster Generation
- POST /rosters/generate
- POST /rosters/generate-daily
- GET /rosters
- GET /rosters/{id}

### Roster Analysis
- GET /rosters/{id}/analyse
- GET /rosters/{id}/suggestions

### Cost & Variance
- GET /costs/shift/{id}
- POST /variance/calculate

### Award Rules
- GET /awards/day-type
- GET /awards/penalty-rate
- GET /awards/public-holidays/{state}

### POS Import
- POST /pos/import

### Demo Data
- POST /demo/load

### Webhooks
- POST /tanda/webhook

### Authentication
- POST /api/auth/register
- POST /api/auth/login
- POST /api/auth/refresh
- GET /api/auth/me
- PUT /api/auth/me
- POST /api/auth/logout
- POST /api/auth/api-key/generate

## Test Examples

### Testing Venue Creation
```python
def test_create_venue(self, client):
    """POST /venues creates a venue with correct response."""
    response = create_test_venue(client, venue_id="test-v1")
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "test-v1"
    assert data["status"] == "created"
```

### Testing Roster Generation Flow
```python
def test_generate_roster_success(self, client):
    """POST /rosters/generate creates a roster with shifts."""
    # Setup
    week_start = self.setup_roster_prerequisites(client)
    
    # Generate
    response = client.post(
        "/rosters/generate",
        json={
            "venue_id": "test-v1",
            "week_start": week_start.isoformat(),
            "covers_per_staff": 15.0,
        }
    )
    
    # Assert
    assert response.status_code == 200
    data = response.json()
    assert "id" in data
    assert len(data["shifts"]) > 0
```

### Testing Authentication Flow
```python
def test_full_auth_flow(self, client):
    """Complete authentication flow."""
    # Register
    reg = client.post(
        "/api/auth/register",
        json={
            "email": "alice@example.com",
            "password": "SecurePassword123!",
            "name": "Alice",
        }
    )
    assert reg.status_code == 201
    
    # Login
    login = client.post(
        "/api/auth/login",
        json={"email": "alice@example.com", "password": "SecurePassword123!"}
    )
    assert login.status_code == 200
    
    # Access protected endpoint
    access_token = login.json()["access_token"]
    profile = client.get(
        "/api/auth/me",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    assert profile.status_code == 200
```

## Bug Detection

These tests would catch:
- Missing endpoints
- Incorrect HTTP methods
- Wrong status codes
- Missing response fields
- Data persistence issues
- Pagination bugs
- Filtering logic errors
- Authentication bypass
- Authorization violations
- Input validation failures
- State management bugs
- Duplicate handling issues
- Cascade delete problems
- Data integrity issues
- Race conditions
- Token expiration issues

## Running with Coverage

Generate HTML coverage report:

```bash
pytest tests/test_api_e2e.py tests/test_api_auth_e2e.py \
  --cov=rosteriq \
  --cov-report=html \
  --cov-report=term-missing
```

View report in `htmlcov/index.html`.

## Continuous Integration

Add to CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run E2E Tests
  run: |
    pip install pytest pytest-asyncio fastapi httpx pydantic
    pytest tests/test_api_e2e.py tests/test_api_auth_e2e.py -v --tb=short
```

## Troubleshooting

### Import Errors
Ensure the RosterIQ package is in the Python path:
```bash
cd /path/to/RosterIQ
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest tests/test_api_e2e.py -v
```

### Database Issues
Tests automatically reset the database via conftest fixtures. If there are issues:
1. Check conftest.py exists in tests/
2. Verify reset_db() is imported
3. Run: `pytest --fixtures` to see available fixtures

### Slow Tests
- Use `-x` to stop on first failure
- Use `-k pattern` to run specific tests
- Add `-v -s` to see output

## Contributing

When adding new API endpoints:
1. Create corresponding test methods
2. Test both happy path and error cases
3. Validate response structure and data
4. Use helper functions for common setup
5. Document the test purpose in docstrings

## Test Statistics

| Metric | Count |
|--------|-------|
| Test Files | 2 |
| Test Classes | 25 |
| Test Methods | 90 |
| Lines of Code | 1,860 |
| Endpoints Covered | 40+ |
| Fixtures Used | 3 |
| Helper Functions | 5 |

## Related Files

- `conftest.py`: Shared fixtures (client, reset_database)
- `api.py`: FastAPI app being tested
- `database.py`: MemoryStore used in tests

