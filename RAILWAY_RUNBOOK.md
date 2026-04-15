# RosterIQ Railway Deployment Runbook

> Last updated: 2026-04-16 (Round 10)
> Audience: ops/dev pushing the next deploy
> Tone: copy-paste-runnable

This runbook covers a fresh deploy and an update-in-place deploy of
RosterIQ to Railway. Pair it with `scripts/e2e_smoke.py` which validates
the running instance after the deploy finishes.

---

## Prerequisites

* Railway CLI: `npm i -g @railway/cli` (or use the Railway web UI).
* `railway login` once on your machine.
* Local clone with `main` checked out at the commit you intend to ship.
* Optional but recommended: `gh auth status` (so the GitHub release/tag
  step works).

## Service shape

A single web service, Python 3.11, listens on `$PORT`, served by Uvicorn.
The entry point is `rosteriq.api_v2:app`.

Static assets live in `static/` and are served by FastAPI's `StaticFiles`
mount.

---

## Environment variables

Required:

| Variable | Purpose |
| --- | --- |
| `PORT` | Set by Railway automatically; do not override. |
| `JWT_SECRET` | HMAC secret for JWT signing (`auth.py`). Use a 32+ byte random string. |
| `AUTH_ENABLED` | `true` in production, `false` for demo/sandbox. |

Recommended:

| Variable | Purpose |
| --- | --- |
| `TANDA_API_BASE` | Tanda API host (`https://my.tanda.co/api/v2` for prod). |
| `TANDA_WEBHOOK_SECRET` | HMAC-SHA256 secret for Tanda webhooks (`/api/v1/tanda/webhook/`). |
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` / `TWILIO_FROM` | SMS call-in flow. |
| `SENDGRID_API_KEY` / `SENDGRID_FROM` | Brief delivery email channel. |
| `STRIPE_SECRET_KEY` | Stripe REST. |
| `STRIPE_WEBHOOK_SECRET` | HMAC-SHA256 secret for Stripe events. |
| `STARTUP_PRICE_ID` / `PRO_PRICE_ID` / `ENTERPRISE_PRICE_ID` | Stripe price IDs. |
| `STRIPE_SUCCESS_URL` / `STRIPE_CANCEL_URL` | Post-checkout redirects. |
| `ANTHROPIC_API_KEY` *or* `OPENAI_API_KEY` | LLM backend for the Ask + concierge agents. |
| `BOM_API_KEY` | Australian Bureau of Meteorology key (if using paid tier). |

Notes:
* Anything missing falls through to the demo path — features that
  depend on the missing key will return clearly-marked demo data, not
  500. This keeps the dashboard usable even if Stripe/Tanda credentials
  haven't been wired yet.
* Secrets can be set with `railway variables set NAME=value`.

---

## First-time deploy

```bash
# 1. Provision the service (only once)
railway init        # follow prompts; pick "Empty Service"
railway link        # link this clone to the project

# 2. Set required env (one-liner — splice your real values in)
railway variables set \
    JWT_SECRET="$(python -c 'import secrets;print(secrets.token_hex(32))')" \
    AUTH_ENABLED=true

# 3. Deploy current commit
railway up

# 4. Capture the public URL
railway status
# → copy the https://<service>.up.railway.app URL
```

A successful deploy ends with `Build succeeded` and `Deploy successful`.
The first request after a cold start may take ~15s while the worker boots.

---

## Update-in-place deploy

```bash
# Fast-forward main, then:
railway up

# Watch logs in another terminal:
railway logs --follow
```

If a deploy goes wrong, roll back via the Railway dashboard
(`Deployments → previous → Rollback`). The CLI does not currently expose
this — use the web UI.

---

## Post-deploy validation

Run the smoke test against the new URL. It exercises eight critical
endpoints and exits non-zero on any failure:

```bash
python scripts/e2e_smoke.py \
    --base-url https://<service>.up.railway.app

# With auth enabled, supply a JWT for the gated checks:
python scripts/e2e_smoke.py \
    --base-url https://<service>.up.railway.app \
    --token "$(cat ~/rosteriq.jwt)"
```

Expected output on a healthy deploy:

```
CHECK                  STATUS  TIME    DETAIL
---------------------  ------  ------  ------
root                   200 PASS    50ms
openapi.json           200 PASS    80ms  31 paths
onboarding/spec        200 PASS    40ms
concierge/ask          200 PASS    65ms
roi/calculate          200 PASS    72ms
rosters/generate       401 PASS   110ms  auth required (expected without token)
tanda/history/status   401 PASS    45ms  auth required (expected without token)
billing/tiers          200 PASS    40ms

OK: all 8 checks passed
```

If anything fails, `railway logs` is the first stop. Most failures are
either a missing env var or the worker still warming up.

---

## Health checks Railway should know about

Set these in the Railway dashboard under *Service → Settings*:

* **Healthcheck path:** `/`
* **Restart policy:** `On Failure`
* **Replicas:** 1 (until traffic justifies more — RosterIQ keeps
  in-memory state, so multi-replica needs an external store first).

---

## Backups and stateful concerns

Several module-level singletons currently store data in memory:

* `ShiftEventStore` (events + learned patterns)
* `BookingsStore` (CSV uploads)
* `TandaHistoryStore` (rolled-up actuals — re-ingestible)
* `OnboardingStore` (per-tenant wizard state)
* `ConciergeKnowledgeBase` (FAQs + live context)

A worker restart drops all of this. For production:

1. Re-ingest Tanda history with `POST /api/v1/tanda/history/ingest` per
   venue.
2. Re-upload bookings CSVs if they weren't fed via the API.
3. Custom concierge FAQs need to be re-`POST`ed.

Persistence is on the roadmap (Postgres-backed stores). Until then,
treat restarts as a feature event and run the smoke test after each.

---

## Rollback decision tree

* `e2e_smoke.py` reports a 500 on `/openapi.json` → bad deploy, roll back.
* Reports a 502 / connection refused → worker crash, check logs for
  Python traceback. Likely a missing env var.
* Reports auth-related FAIL on `roi/calculate` (which is open) → JWT
  middleware misconfiguration; verify `AUTH_ENABLED` value.
* Reports OK but the dashboard shows demo data → expected when keys
  aren't wired; not a rollback trigger.

---

## Common errors and fixes

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Worker boots then crashes with `ModuleNotFoundError: pyjwt` | `JWT_SECRET` set but `pyjwt` missing | Add `pyjwt` to `requirements.txt` |
| `/api/v1/billing/checkout` returns 503 | `STRIPE_SECRET_KEY` not set | Set the var; redeploy |
| Concierge always escalates | KB never seeded with `live_context` | `POST /api/v1/concierge/{venue_id}/live-context` |
| Tanda webhook always 401 | `TANDA_WEBHOOK_SECRET` mismatched in Tanda admin | Re-copy the secret from Tanda admin |
| Ask agent uses demo answers | LLM key missing | Set `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` |
