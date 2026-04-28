# RosterIQ Test Suite Summary

This document summarizes the comprehensive pytest test suite created for three recently rewritten/created RosterIQ modules.

## Overview

- **Total Test Files**: 3
- **Total Test Cases**: 77
- **Test Framework**: pytest + pytest-asyncio

## Test Files Created

### 1. `test_auth_service.py` (36 tests)

Comprehensive tests for the `AuthService` class in `services/auth.py`.

#### Test Classes & Coverage

**TestPasswordHashing** (4 tests)
- ✓ Hash password creates bcrypt hash
- ✓ Verify correct password
- ✓ Verify incorrect password
- ✓ Verify empty password fails

**TestAccessTokens** (7 tests)
- ✓ Create access token
- ✓ Access token expiration timing
- ✓ Verify valid access token
- ✓ Reject invalid signature
- ✓ Reject wrong token type
- ✓ Reject expired token
- ✓ Verify token payload structure

**TestRefreshTokens** (6 tests)
- ✓ Create refresh token
- ✓ Refresh token expiration timing
- ✓ Verify valid refresh token
- ✓ Reject invalid refresh token
- ✓ Reject revoked refresh token
- ✓ Revoke refresh token

**TestAPIKeys** (5 tests)
- ✓ Generate API key
- ✓ API key hash is stored
- ✓ Verify valid API key
- ✓ Reject invalid API key
- ✓ Reject API key from inactive user

**TestUserCreation** (6 tests)
- ✓ Create user successfully
- ✓ Reject duplicate email
- ✓ Default role assignment
- ✓ Get user by email
- ✓ Get user by email (not found)
- ✓ Get user by ID
- ✓ Get user by ID (not found)

**TestRateLimiting** (5 tests)
- ✓ Rate limit under threshold
- ✓ Rate limit over threshold
- ✓ Successful attempts ignored
- ✓ Rate limit per IP
- ✓ Record login attempt
- ✓ Update last login timestamp

**TestAuthServiceIntegration** (2 tests)
- ✓ Full login flow
- ✓ API key authentication

### 2. `test_webhook_idempotency.py` (17 tests)

Tests for webhook processing in `routes/webhook_routes.py`, focusing on HMAC signature verification and database-backed idempotency.

#### Test Classes & Coverage

**TestHMACSignatureVerification** (9 tests)
- ✓ Valid HMAC signature verified
- ✓ Invalid signature rejected
- ✓ Empty secret rejected
- ✓ Empty signature rejected
- ✓ Both empty rejected
- ✓ Different payloads produce different signatures
- ✓ Case-sensitive signature verification
- ✓ Tampered payload fails verification
- ✓ HMAC constant-time comparison

**TestWebhookIdempotency** (8 tests)
- ✓ First webhook call not duplicate
- ✓ Second call with same ID is duplicate
- ✓ Different IDs not duplicates
- ✓ Database unavailable disables idempotency
- ✓ Webhook event saved
- ✓ Webhook payload hash computed
- ✓ Multiple events tracked separately
- ✓ Auto-cleanup on event storage limit
- ✓ HMAC signature + idempotency together

### 3. `test_notifications.py` (24 tests)

Tests for the `NotificationService` class in `services/notifications.py`. Tests template rendering without mocking SMTP.

#### Test Classes & Coverage

**TestNotificationServiceInitialization** (3 tests)
- ✓ Service initialization with defaults
- ✓ Service initialization with env vars
- ✓ Singleton pattern

**TestDailyDigestTemplate** (4 tests)
- ✓ Daily digest basic content
- ✓ Template structure
- ✓ Shifts table rendering
- ✓ Empty shifts handling
- ✓ Compliance alerts

**TestRosterPublishedTemplate** (2 tests)
- ✓ Template content
- ✓ Cost display

**TestComplianceAlertTemplate** (2 tests)
- ✓ Break violation alert
- ✓ Fatigue warning alert

**TestCertificationExpiryTemplate** (3 tests)
- ✓ Critical urgency (7 days)
- ✓ Urgent urgency (30 days)
- ✓ Reminder urgency (60 days)

**TestVarianceAlertTemplate** (3 tests)
- ✓ High variance (>50%) CRITICAL
- ✓ Moderate variance (30-50%) HIGH
- ✓ Low variance (<30%) ALERT

**TestEmailTemplateWrapping** (3 tests)
- ✓ Template wrapping adds header/footer
- ✓ Footer with links
- ✓ Branded header

**TestEmailSending** (3 tests)
- ✓ Email sent successfully
- ✓ Missing credentials rejected
- ✓ No recipient rejected

## Fixture Structure (`conftest.py`)

Shared pytest fixtures for all test modules:

```python
@pytest.fixture
def memory_store():
    """Fresh MemoryStore instance per test."""
    
@pytest.fixture
def auth_service(memory_store):
    """AuthService with MemoryStore backend."""
    
@pytest.fixture(autouse=True)
def reset_global_db():
    """Reset database singleton before each test."""
```

## Running the Tests

### Prerequisites

```bash
pip install pytest pytest-asyncio pytest-cov pydantic fastapi
pip install passlib bcrypt PyJWT  # For auth tests
```

### Run All Tests

```bash
cd /sessions/fervent-adoring-goodall/dropbox_rosteriq/RosterIQ
pytest tests/ -v
```

### Run Specific Test File

```bash
pytest tests/test_auth_service.py -v
pytest tests/test_webhook_idempotency.py -v
pytest tests/test_notifications.py -v
```

### Run Specific Test Class

```bash
pytest tests/test_auth_service.py::TestPasswordHashing -v
pytest tests/test_auth_service.py::TestAccessTokens -v
```

### Run with Coverage

```bash
pytest tests/ --cov=rosteriq --cov-report=html
```

## Module Coverage

### services/auth.py
- ✓ Password hashing (bcrypt)
- ✓ JWT access token generation/verification
- ✓ Refresh token creation/revocation
- ✓ API key generation/verification
- ✓ User creation and retrieval
- ✓ Login rate limiting (per IP)
- ✓ Last login tracking

### routes/webhook_routes.py
- ✓ HMAC-SHA256 signature verification
- ✓ Webhook duplicate detection
- ✓ Database-backed idempotency
- ✓ Webhook event persistence
- ✓ Payload hashing

### services/notifications.py
- ✓ Daily digest template rendering
- ✓ Roster published notification
- ✓ Compliance alert templates
- ✓ Certification expiry warnings (3 urgency levels)
- ✓ Variance alerts (3 severity levels)
- ✓ Email template wrapping with branding
- ✓ HTML email structure

## Database Layer Testing

All auth and webhook tests use `MemoryStore` directly for test isolation:

```python
from rosteriq.database import MemoryStore

@pytest.fixture
def memory_store():
    store = MemoryStore()
    yield store
    # Auto-cleanup
```

### MemoryStore Methods Tested

- `save_user()` and `get_user_by_email()` / `get_user_by_id()`
- `save_refresh_token()`, `get_refresh_token()`, `revoke_refresh_token()`
- `record_login_attempt()`, `check_login_rate_limit()`
- `is_webhook_processed()`, `save_webhook_event()`

## Key Testing Patterns

### 1. Isolation
Each test gets a fresh `MemoryStore` instance, ensuring tests don't interfere.

### 2. Fixtures with Cleanup
```python
@pytest.fixture(autouse=True)
def reset_global_db():
    reset_db()
    yield
    reset_db()
```

### 3. Async Testing
Async email sending tests use `@pytest.mark.asyncio`:
```python
@pytest.mark.asyncio
async def test_send_email_success(self, notification_service):
    ...
```

### 4. Mocking External Dependencies
- SMTP sending is mocked to avoid actual email transmission
- WebSocket hub injection allows testing webhook routing
- Database can be mocked for idempotency tests

### 5. Template Validation
Tests verify email templates contain:
- Venue names and details
- Expected content sections
- Correct urgency/severity indicators
- RosterIQ branding

## Notes

- Tests do NOT require full environment setup (no PostgreSQL needed)
- All tests use in-memory database layer for speed
- SMTP tests are mocked to avoid email transmission
- Templates are validated by checking content, not rendering to browser
- Test files import via `rosteriq.` prefix as per project standard

## Future Test Additions

Consider adding tests for:
- PostgreSQL store implementation
- Integration with FastAPI routes
- WebSocket notification delivery
- Full webhook processing pipeline
- Email delivery failure handling
- Token revocation lists (TRL)
- Concurrent login attempt handling
