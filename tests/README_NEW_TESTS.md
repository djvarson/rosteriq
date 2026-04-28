# New Test Files for RosterIQ

This directory contains comprehensive pytest test suites for three recently rewritten/created RosterIQ modules.

## Files Created

### 1. **test_auth_service.py** (36 test methods)
Tests for `services/auth.py` covering:
- Password hashing and bcrypt verification
- JWT access token generation, verification, expiration
- Refresh token lifecycle (create, verify, revoke)
- API key management (generate, store hash, verify)
- User creation and duplicate detection
- Login rate limiting per IP address
- Last login timestamp tracking

**Test Classes:**
- `TestPasswordHashing` (4 tests)
- `TestAccessTokens` (7 tests)
- `TestRefreshTokens` (6 tests)
- `TestAPIKeys` (5 tests)
- `TestUserCreation` (6 tests)
- `TestRateLimiting` (5 tests)
- `TestAuthServiceIntegration` (2 tests)

### 2. **test_webhook_idempotency.py** (17 test methods)
Tests for `routes/webhook_routes.py` covering:
- HMAC-SHA256 signature verification
- Webhook duplicate detection (idempotency)
- Database-backed webhook event tracking
- Payload hashing
- Auto-cleanup of webhook event storage

**Test Classes:**
- `TestHMACSignatureVerification` (9 tests)
- `TestWebhookIdempotency` (8 tests)

### 3. **test_notifications.py** (24 test methods)
Tests for `services/notifications.py` covering:
- Daily digest email template rendering
- Roster published notification
- Compliance alert templates (break violation, fatigue)
- Certification expiry warnings with urgency levels
- Variance alerts with severity levels
- Email template wrapping with RosterIQ branding
- Async email sending (mocked SMTP)

**Test Classes:**
- `TestNotificationServiceInitialization` (3 tests)
- `TestDailyDigestTemplate` (4 tests)
- `TestRosterPublishedTemplate` (2 tests)
- `TestComplianceAlertTemplate` (2 tests)
- `TestCertificationExpiryTemplate` (3 tests)
- `TestVarianceAlertTemplate` (3 tests)
- `TestEmailTemplateWrapping` (3 tests)
- `TestEmailSending` (3 tests)

### 4. **conftest.py** (pytest configuration)
Shared fixtures for all tests:
- `memory_store` — Fresh MemoryStore instance per test
- `auth_service` — AuthService with MemoryStore backend
- `reset_global_db` — Auto-reset database singleton (autouse)

## Quick Start

### Install Dependencies
```bash
pip install pytest pytest-asyncio
pip install pydantic fastapi
pip install passlib bcrypt PyJWT  # For auth tests
```

### Run All New Tests
```bash
cd /sessions/fervent-adoring-goodall/dropbox_rosteriq/RosterIQ
pytest tests/test_auth_service.py tests/test_webhook_idempotency.py tests/test_notifications.py -v
```

### Run Individual Test Files
```bash
pytest tests/test_auth_service.py -v
pytest tests/test_webhook_idempotency.py -v
pytest tests/test_notifications.py -v
```

### Run Specific Test Class
```bash
pytest tests/test_auth_service.py::TestPasswordHashing -v
pytest tests/test_webhook_idempotency.py::TestHMACSignatureVerification -v
```

### Run with Coverage Report
```bash
pytest tests/test_auth_service.py tests/test_webhook_idempotency.py tests/test_notifications.py --cov=rosteriq --cov-report=html
```

## Test Statistics

- **New Test Files:** 3
- **New Test Methods:** 77
- **Test Classes:** 20
- **Total Lines of Test Code:** ~2,400
- **Framework:** pytest + pytest-asyncio
- **Database Layer:** MemoryStore (in-memory, no PostgreSQL needed)

## Key Features

### 1. Test Isolation
Each test gets a fresh `MemoryStore` instance via pytest fixtures, ensuring tests don't interfere.

### 2. No External Dependencies
- Tests use in-memory database (`MemoryStore`)
- SMTP is mocked (no email sent)
- No PostgreSQL required

### 3. Async Support
Uses `@pytest.mark.asyncio` for async email sending tests.

### 4. Comprehensive Coverage
- Positive cases (happy path)
- Negative cases (errors, validation)
- Edge cases (empty values, timeouts)
- Integration scenarios (full auth flow, multi-step operations)

### 5. Clear Test Names
Test method names clearly describe what they test:
- `test_hash_password_creates_bcrypt_hash`
- `test_verify_password_correct`
- `test_verify_hmac_signature_valid`
- `test_is_webhook_duplicate_second_call`

## File Locations

```
/sessions/fervent-adoring-goodall/dropbox_rosteriq/RosterIQ/
├── tests/
│   ├── conftest.py                    ← Pytest configuration & fixtures
│   ├── test_auth_service.py           ← 36 auth tests
│   ├── test_webhook_idempotency.py    ← 17 webhook tests
│   ├── test_notifications.py          ← 24 notification tests
│   ├── TEST_SUMMARY.md                ← Detailed test documentation
│   └── README_NEW_TESTS.md            ← This file
├── services/
│   ├── auth.py                        ← AuthService (tested)
│   └── notifications.py               ← NotificationService (tested)
├── routes/
│   └── webhook_routes.py              ← Webhook routes (tested)
└── database.py                        ← MemoryStore (used in tests)
```

## Module Coverage

### services/auth.py
✓ Password hashing with bcrypt
✓ JWT access token generation & verification
✓ Refresh token lifecycle
✓ API key management
✓ User creation & retrieval
✓ Login rate limiting
✓ Last login tracking

### routes/webhook_routes.py
✓ HMAC-SHA256 signature verification
✓ Webhook duplicate detection
✓ Database-backed idempotency
✓ Webhook event persistence
✓ Payload hashing

### services/notifications.py
✓ Template rendering for all notification types
✓ Email header/footer wrapping
✓ Content validation (venues, shifts, costs)
✓ Async email sending (mocked)
✓ Template structure (HTML)

## Database Methods Tested

- `save_user()`, `get_user_by_email()`, `get_user_by_id()`
- `save_refresh_token()`, `get_refresh_token()`, `revoke_refresh_token()`
- `record_login_attempt()`, `check_login_rate_limit()`
- `is_webhook_processed()`, `save_webhook_event()`

## Notes for Development

1. **Imports**: All tests use `from rosteriq.` prefix (not relative imports)
2. **Fixtures**: Shared fixtures in `conftest.py` provide `MemoryStore` and `AuthService`
3. **Async Tests**: Use `@pytest.mark.asyncio` decorator for async methods
4. **Mocking**: External dependencies (SMTP, WebSocket) are mocked
5. **No Side Effects**: Tests clean up after themselves via pytest fixtures

## Next Steps

1. Run tests: `pytest tests/test_auth_service.py tests/test_webhook_idempotency.py tests/test_notifications.py -v`
2. Check coverage: `pytest --cov=rosteriq tests/`
3. Integrate with CI/CD: Add to GitHub Actions / GitLab CI
4. Consider adding additional tests for:
   - PostgreSQL store implementation
   - FastAPI route integration
   - WebSocket delivery
   - Production email sending
   - Rate limit time window behavior

## Troubleshooting

**ImportError: No module named 'pytest'**
```bash
pip install pytest pytest-asyncio
```

**ImportError: No module named 'rosteriq'**
Ensure tests are run from the RosterIQ directory:
```bash
cd /sessions/fervent-adoring-goodall/dropbox_rosteriq/RosterIQ
pytest tests/
```

**pydantic ImportError**
```bash
pip install pydantic>=2.0
```

**SMTP connection errors during tests**
SMTP is mocked in tests, so no actual connection is made. If tests fail, check mock setup in test methods.

---

For detailed test documentation, see `TEST_SUMMARY.md`.
