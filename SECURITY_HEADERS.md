# Security Headers Implementation

## Overview

RosterIQ implements comprehensive security headers middleware following OWASP best practices. This middleware automatically adds protective headers to all HTTP responses, defending against common web vulnerabilities including XSS, clickjacking, MIME sniffing, and protocol downgrade attacks.

## Components

### 1. SecurityHeadersMiddleware (`middleware/security_headers.py`)

**Class**: `SecurityHeadersMiddleware(BaseHTTPMiddleware)`

FastAPI middleware that intercepts all responses and adds security headers. Implemented as a pass-through middleware that modifies response headers before sending to clients.

**Key Features**:
- Automatic header injection on all responses
- Environment-aware configuration (dev/staging/production)
- Request ID tracking and propagation
- CORS origin validation with explicit allowlists
- Cache-Control optimization by response type
- CSP policy reporting

### 2. SecurityConfig (`middleware/security_headers.py`)

**Class**: `SecurityConfig(dataclass)`

Configuration object for security headers. Automatically adapts to environment (development mode relaxes CSP and HSTS for developer convenience).

**Configuration Options**:
```python
SecurityConfig(
    environment="production",           # development|staging|production
    enable_hsts=True,                   # HSTS enforcement
    hsts_max_age=31536000,             # 1 year in seconds
    csp_report_uri=None,               # Optional CSP violation endpoint
    allowed_origins=[...],             # CORS origin allowlist
    additional_csp_directives={},      # Override CSP directives
    enable_cors_preflight_caching=True, # Cache CORS preflight
    cors_max_age=86400,                # 24 hours in seconds
)
```

### 3. Security Routes (`routes/security.py`)

**Module**: `create_security_router(middleware)`

Provides admin endpoints for security configuration and CSP violation analysis:

- **GET /api/v1/admin/security/config** — View current security configuration (admin only)
- **POST /api/v1/admin/security/csp-report** — Receive CSP violation reports (public, rate-limited)
- **GET /api/v1/admin/security/csp-violations** — View CSP violation summary (admin only)
- **DELETE /api/v1/admin/security/csp-violations** — Clear violation records (admin only)

## Security Headers Implemented

### 1. Content-Security-Policy (CSP)

Prevents inline script execution and restricts resource loading sources.

**Production Configuration**:
```
default-src 'self'
script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com
style-src 'self' 'unsafe-inline' https://fonts.googleapis.com
img-src 'self' data: blob:
font-src 'self' https://fonts.gstatic.com
connect-src 'self' wss: https://api.stripe.com
frame-ancestors 'none'
form-action 'self'
base-uri 'self'
report-uri /api/v1/admin/security/csp-report
```

**Development Configuration** (more permissive for hot reload):
```
script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdnjs.cloudflare.com
connect-src 'self' 'unsafe-inline' ws: wss: http://localhost:* http://127.0.0.1:*
```

**Supported Inline CDNs**:
- Chart.js (cdnjs.cloudflare.com)
- Google Fonts (fonts.googleapis.com, fonts.gstatic.com)
- Stripe API (api.stripe.com)

### 2. Strict-Transport-Security (HSTS)

Enforces HTTPS and prevents protocol downgrade attacks.

**Header**: `Strict-Transport-Security: max-age=31536000; includeSubDomains; preload`

**Behavior**:
- Enabled in staging and production only (disabled in development)
- Preload directive allows inclusion in HSTS preload lists
- 1-year validity period (31536000 seconds)
- Applies to all subdomains

### 3. X-Frame-Options

Prevents clickjacking by disallowing iframe embedding.

**Header**: `X-Frame-Options: DENY`

**Effect**: Page cannot be embedded in iframes on any origin

### 4. X-Content-Type-Options

Prevents MIME type sniffing attacks.

**Header**: `X-Content-Type-Options: nosniff`

**Effect**: Browsers must respect Content-Type header; prevents HTML/JS execution in PDF/image contexts

### 5. X-XSS-Protection

Legacy XSS protection header for older browser support.

**Header**: `X-XSS-Protection: 0`

**Note**: Modern CSP is preferred; this is kept for compatibility

### 6. Referrer-Policy

Controls what referrer information is sent to external sites.

**Header**: `Referrer-Policy: strict-origin-when-cross-origin`

**Behavior**:
- Same-origin: Full URL sent
- Cross-origin: Origin only (no path/query)
- Downgrade: No referrer sent (HTTPS → HTTP)

### 7. Permissions-Policy (formerly Feature-Policy)

Restricts browser features and APIs.

**Header**: `Permissions-Policy: camera=(), microphone=(), geolocation=(), payment=(self)`

**Restrictions**:
- Camera: Disabled everywhere
- Microphone: Disabled everywhere
- Geolocation: Disabled everywhere
- Payment Request: Allowed only on same origin

### 8. Cache-Control

Optimizes caching behavior based on response type.

**API Responses** (`/api/*`):
```
Cache-Control: no-store, no-cache, must-revalidate, proxy-revalidate
Pragma: no-cache
Expires: 0
```

**Static Assets** (`/static/*`, `/docs`, `/redoc`):
```
Cache-Control: public, max-age=3600, immutable
```

**Other Responses**:
```
Cache-Control: no-store
```

### 9. X-Request-ID

Unique identifier for request tracing and correlation.

**Header**: `X-Request-ID: <UUID>`

**Behavior**:
- Generated as UUID v4 if not present in incoming request
- Propagated to all downstream systems
- Used for logging and error tracking

## Environment-Specific Behavior

### Development Environment

**Features**:
- HSTS disabled (allows local HTTP testing)
- CSP relaxed with `unsafe-eval` (hot module reload)
- Localhost origins auto-added (3000, 5173 ports)
- Cross-origin WebSocket allowed

**Configuration**:
```python
SecurityConfig(
    environment=Environment.DEVELOPMENT,
    enable_hsts=False,  # Auto-set by __post_init__
    allowed_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
    ]
)
```

### Staging Environment

**Features**:
- HSTS enabled with 1-year max-age
- Strict CSP (no unsafe-eval)
- Limited origin allowlist
- CSP report-uri enabled

**Configuration**:
```python
SecurityConfig(
    environment=Environment.STAGING,
    enable_hsts=True,
    allowed_origins=[
        "https://staging.app.rosteriq.com",
        "https://staging.dashboard.rosteriq.com",
    ],
    csp_report_uri="/api/v1/admin/security/csp-report",
)
```

### Production Environment

**Features**:
- HSTS enabled with preload directive
- Maximum CSP strictness
- Explicit origin allowlist only (no wildcards)
- CSP violations logged and reportable
- All security features enabled

**Configuration**:
```python
SecurityConfig(
    environment=Environment.PRODUCTION,
    enable_hsts=True,
    allowed_origins=[
        "https://app.rosteriq.com",
        "https://dashboard.rosteriq.com",
    ],
    csp_report_uri="/api/v1/admin/security/csp-report",
)
```

## CORS Configuration

CORS (Cross-Origin Resource Sharing) is handled by the middleware with explicit origin allowlists.

**Behavior**:
- Only origins in `allowed_origins` list receive CORS headers
- No wildcard (`*`) allowed in production (security risk)
- Preflight requests cached for 24 hours (configurable)
- Credentials allowed only for allowlisted origins

**Preflight Response Headers**:
```
Access-Control-Allow-Origin: <matched-origin>
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, PATCH, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization, X-API-Key, X-Request-ID
Access-Control-Allow-Credentials: true
Access-Control-Max-Age: 86400
```

## CSP Violation Reporting

When CSP violations occur in browsers, reports are sent to the `csp-report-uri` endpoint where they're collected for analysis.

### Violation Report Format (Browser → Server)

```json
{
  "csp-report": {
    "violated-directive": "script-src 'self'",
    "original-policy": "default-src 'self'; script-src 'self'",
    "document-uri": "https://app.rosteriq.com/dashboard",
    "effective-directive": "script-src",
    "source-file": "https://evil.com/malicious.js",
    "line-number": 42,
    "column-number": 10,
    "status-code": 200,
    "referrer": "https://app.rosteriq.com/"
  }
}
```

### Admin Endpoints

**View CSP Violations Summary**:
```bash
GET /api/v1/admin/security/csp-violations
```

**Response**:
```json
{
  "total_violations": 5,
  "violations_by_directive": {
    "script-src": 3,
    "img-src": 2
  },
  "recent_violations": [
    {
      "timestamp": "2026-04-27T10:30:45.123Z",
      "violated_directive": "script-src",
      "document_uri": "https://app.rosteriq.com/dashboard",
      "user_agent": "Mozilla/5.0...",
      "source_file": "https://example.com/script.js"
    }
  ],
  "top_violated_origins": {
    "https://app.rosteriq.com/dashboard": 5
  }
}
```

**Clear Violations**:
```bash
DELETE /api/v1/admin/security/csp-violations
```

**View Configuration**:
```bash
GET /api/v1/admin/security/config
```

**Response**:
```json
{
  "environment": "production",
  "hsts_enabled": true,
  "hsts_max_age": 31536000,
  "csp_header": "default-src 'self'; ...",
  "allowed_origins": [
    "https://app.rosteriq.com",
    "https://dashboard.rosteriq.com"
  ],
  "csp_report_uri": "/api/v1/admin/security/csp-report"
}
```

## Integration with FastAPI

### Registration (in `api.py`)

```python
from rosteriq.middleware.security_headers import (
    SecurityHeadersMiddleware, SecurityConfig, Environment
)
from rosteriq.routes.security import create_security_router

# Configure based on environment
_env = os.getenv("ENVIRONMENT", "development")
_security_config = SecurityConfig(
    environment=Environment(_env),
    enable_hsts=_env == "production",
    allowed_origins=[
        "https://app.rosteriq.com",
        "https://dashboard.rosteriq.com",
    ],
    csp_report_uri="/api/v1/admin/security/csp-report",
)

# Register middleware (BEFORE other middleware for full coverage)
app.add_middleware(SecurityHeadersMiddleware, config=_security_config)

# Register admin routes
security_router = create_security_router(_security_middleware)
app.include_router(security_router)
```

### Middleware Ordering

Security headers middleware should be registered early in the middleware stack:

```
1. StructuredLoggingMiddleware    (correlation IDs)
2. SecurityHeadersMiddleware      ← MUST be early
3. RateLimiterMiddleware
4. RequestLoggingMiddleware
5. CORSMiddleware
6. APIVersionMiddleware
7. TenantMiddleware
8. ThemeInjectorMiddleware
```

**Why early?** Ensures security headers apply to ALL responses, including error responses.

## Customization

### Override CSP Directives

```python
config = SecurityConfig(
    environment=Environment.PRODUCTION,
    additional_csp_directives={
        "script-src": "'self' https://custom-cdn.com",
        "img-src": "'self' data: blob: https://cdn.example.com",
    }
)
```

### Custom CORS Origins

```python
config = SecurityConfig(
    environment=Environment.PRODUCTION,
    allowed_origins=[
        "https://app.rosteriq.com",
        "https://staging.app.rosteriq.com",  # Staging for testing
        "https://partner.example.com",       # Partner integration
    ]
)
```

### Custom Report URI

```python
config = SecurityConfig(
    csp_report_uri="https://external-security-service.com/reports"
)
```

## Testing

Run unit tests:
```bash
pytest RosterIQ/tests/test_security_headers.py -v
```

Test headers with curl:
```bash
curl -i https://api.rosteriq.com/health | grep -E "^(Content-Security-Policy|Strict-Transport-Security|X-Frame-Options)"
```

Browser console CSP violations:
```javascript
// Check CSP violations in browser console
// Violations appear as warning messages
console.warn("CSP violation detected...")
```

## Monitoring & Alerting

CSP violations should be monitored to detect:
- **Injection attacks**: Sudden spike in CSP violations
- **Configuration issues**: Legitimate resources blocked by overly strict CSP
- **Third-party problems**: Unexpected resources from partner integrations
- **Malware**: Suspicious script sources

**Recommended Alerting**:
- Critical: >10 violations/minute (possible attack)
- Warning: >5 violations/minute (configuration issue)
- Info: New violation sources (investigate)

## Compliance

This implementation addresses:
- OWASP Top 10 2021 (A01:2021 - Broken Access Control)
- OWASP Top 10 2021 (A03:2021 - Injection)
- OWASP Top 10 2021 (A07:2021 - Identification and Authentication Failures)
- CWE-79 (Improper Neutralization of Input During Web Page Generation)
- CWE-1021 (Improper Restriction of Rendered UI Layers)

## Browser Support

| Header | Support |
|--------|---------|
| Content-Security-Policy | All modern browsers |
| Strict-Transport-Security | All modern browsers |
| X-Frame-Options | All browsers |
| X-Content-Type-Options | All browsers |
| Referrer-Policy | 95%+ of browsers |
| Permissions-Policy | 75%+ of browsers |

Legacy support is maintained through X-XSS-Protection and fallbacks.

## References

- [OWASP Secure Headers Project](https://owasp.org/www-project-secure-headers/)
- [Content Security Policy MDN](https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP)
- [HSTS Preload List](https://hstspreload.org/)
- [CSP Reporting W3C Spec](https://w3c.github.io/webappsec-csp/#violation-report)
- [Permissions Policy](https://github.com/w3c/webappsec-permissions-policy)
