# Xero Integration Deployment Checklist

## Pre-Deployment

- [ ] **Xero API App Created**
  - [ ] App registered in Xero Developer Portal
  - [ ] OAuth scopes configured (offline_access, openid, profile, email, accounting)
  - [ ] Redirect URI set correctly
  - [ ] Client ID and Secret copied

- [ ] **Environment Variables Set**
  - [ ] `XERO_CLIENT_ID` exported
  - [ ] `XERO_CLIENT_SECRET` exported
  - [ ] `XERO_REDIRECT_URI` exported
  - [ ] `DATABASE_URL` set to PostgreSQL

- [ ] **Database Setup**
  - [ ] PostgreSQL running
  - [ ] `psql -d rosteriq -f schema.sql` executed
  - [ ] `xero_credentials` table created
  - [ ] `xero_revenue_snapshots` table created
  - [ ] `xero_labour_journals` table created

- [ ] **Code Integration**
  - [ ] `xero_integration.py` copied to `rosteriq/`
  - [ ] `xero_routes.py` copied to `rosteriq/`
  - [ ] `examples_xero_usage.py` copied to `rosteriq/` (optional)
  - [ ] `models.py` updated with `XeroCredentials`
  - [ ] `database.py` updated with credential methods
  - [ ] `xero_routes.py` registered in `api.py`
    ```python
    from rosteriq.xero_routes import setup_xero_routes
    setup_xero_routes(app, db)
    ```

## Session Storage Implementation (CRITICAL)

- [ ] **Session Store Configured**
  - [ ] Redis, Memcached, or encrypted cookie session store set up
  - [ ] Session store tested with basic key/value write/read

- [ ] **OAuth State/Code Verifier Storage**
  - [ ] `code_verifier` generated in `/api/xero/connect`
  - [ ] `code_verifier` stored with `state` key in session
  - [ ] `code_verifier` retrieved in `/api/xero/callback`
  - [ ] `/api/xero/callback` uses `code_verifier` to exchange code

- [ ] **Session Cleanup**
  - [ ] States expire after 10 minutes
  - [ ] Failed exchanges clean up state
  - [ ] Manual cleanup job scheduled (if needed)

## Testing

### Unit Tests
- [ ] Run syntax check: `python -m py_compile xero_integration.py`
- [ ] Run syntax check: `python -m py_compile xero_routes.py`
- [ ] Unit tests pass: `pytest tests/test_xero_*.py -v`

### Integration Tests
- [ ] Sandbox Xero account created
- [ ] OAuth flow tested end-to-end (authorize → callback → tokens)
- [ ] Revenue sync tested (pull last 7 days)
- [ ] P&L report tested
- [ ] Labour cost journal tested (draft mode)
- [ ] Token refresh tested (wait until expiry or mock)
- [ ] Rate limiting tested (verify 60 req/min enforced)
- [ ] Error handling tested:
  - [ ] 401 (expired token) retry
  - [ ] 429 (rate limit) backoff
  - [ ] 500 error exponential backoff

### Load Testing
- [ ] Backfill 90 days revenue (should take ~90 seconds)
- [ ] Concurrent venues syncing (test 5+ venues)
- [ ] Rate limiter under high load (mock 70 req/min)

## Security

- [ ] **Secrets Management**
  - [ ] No hardcoded credentials in code
  - [ ] All secrets in environment variables
  - [ ] Environment variables not logged
  - [ ] Production uses encrypted secret store (e.g., AWS Secrets Manager)

- [ ] **Token Security**
  - [ ] Refresh tokens never exposed in logs
  - [ ] Access tokens cleared from memory after use
  - [ ] Token expiry checked before each API call
  - [ ] Database credentials encrypted at rest

- [ ] **API Security**
  - [ ] HTTPS enforced for all `/api/xero/` endpoints
  - [ ] CORS configured correctly (restrict to RosterIQ domains)
  - [ ] Rate limiting active (60 req/min)
  - [ ] State validation in OAuth callback

- [ ] **Audit Trail**
  - [ ] OAuth events logged (connect, disconnect)
  - [ ] Sync events logged (revenue, labour costs)
  - [ ] Errors logged with context (not tokens)
  - [ ] Access audit: who accessed what, when

## Monitoring & Alerts

- [ ] **Logging**
  - [ ] Info: Successful syncs, token refreshes
  - [ ] Warning: Rate limiting, token expiry
  - [ ] Error: API failures, sync failures
  - [ ] Debug: Request/response payloads (production: disabled)

- [ ] **Metrics**
  - [ ] Sync success rate per venue
  - [ ] Average sync duration (target: <30s)
  - [ ] API error rate by endpoint
  - [ ] Token refresh frequency

- [ ] **Alerts**
  - [ ] Sync failure: Alert after 3 retries
  - [ ] Rate limiting: Alert if >90% quota used
  - [ ] Token refresh failure: Alert immediately
  - [ ] Database issues: Alert on connection failure

- [ ] **Dashboard Health Check**
  - [ ] Add "Xero Status" indicator to venue dashboard
  - [ ] Show last synced timestamp
  - [ ] Show labour % metric on dashboard overview

## Payroll Integration (Phase 2)

- [ ] **Labour Cost Sync Ready**
  - [ ] Rosters persisted to database
  - [ ] Employees have hourly rates + award level
  - [ ] `cost_calculator.py` available for cost breakdown
  - [ ] `sync_labour_costs()` route implemented (currently placeholder)

- [ ] **Penalty Rates Integration**
  - [ ] Weekend rates calculated (typically 1.5–2x)
  - [ ] Public holiday rates calculated
  - [ ] Overtime rates tracked
  - [ ] Award level breakdown in journal entries

- [ ] **Superannuation Tracking**
  - [ ] Super calculated at 11.5% of gross wages
  - [ ] Threshold check (no super below ~$20,800/year)
  - [ ] Separate super journal entries (credit 2010)

## Post-Deployment

- [ ] **Documentation Updated**
  - [ ] XERO_INTEGRATION_GUIDE.md reviewed
  - [ ] Setup instructions clear and tested
  - [ ] API endpoints documented in OpenAPI/Swagger
  - [ ] Team trained on Xero integration

- [ ] **Monitoring Active**
  - [ ] Log aggregation configured (CloudWatch, Datadog, ELK)
  - [ ] Alert channels set up (Slack, PagerDuty, email)
  - [ ] Dashboard metrics visible

- [ ] **First Venues Connected**
  - [ ] Pilot venue(s) connected via OAuth
  - [ ] Revenue sync tested (verify data matches Xero)
  - [ ] P&L metrics visible on dashboard
  - [ ] Stakeholder approval received

## Handoff Checklist

- [ ] **Code Review**
  - [ ] PR approved by tech lead
  - [ ] No hardcoded secrets or debug code
  - [ ] Tests passing, coverage >80%
  - [ ] Linting passed (black, flake8)

- [ ] **Documentation Handoff**
  - [ ] Code comments present and clear
  - [ ] XERO_INTEGRATION_GUIDE.md complete
  - [ ] Examples provided (examples_xero_usage.py)
  - [ ] Troubleshooting guide written

- [ ] **On-Call Readiness**
  - [ ] Team familiar with Xero integration
  - [ ] Runbook created for common issues
  - [ ] Escalation path documented
  - [ ] On-call rotation includes Xero knowledge

## Rollback Plan

- [ ] **If Issues Arise**
  - [ ] Disable Xero routes: comment out `setup_xero_routes(app, db)`
  - [ ] Revert schema.sql changes (drop tables if needed)
  - [ ] Clear environment variables
  - [ ] Notify users: Xero sync temporarily offline

- [ ] **Data Safety**
  - [ ] Backups of xero_credentials table before any changes
  - [ ] Revenue snapshots preserved (may lose recent data)
  - [ ] Labour journals kept as draft (not posted to Xero)

---

## Sign-Off

- [ ] Development Lead: _______________  Date: _______
- [ ] QA Lead: _______________  Date: _______
- [ ] DevOps/Infrastructure: _______________  Date: _______
- [ ] Product Manager: _______________  Date: _______

---

**Notes & Issues:**
```
(Any outstanding issues, workarounds, or deferred items)
```

---

Created: 2026-04-23
Version: 1.0
Status: Ready for Production (session storage pending)
