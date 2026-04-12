# RosterIQ — Project Guide

AI-powered rostering system for Australian hospitality venues. Deployed on Railway, serves a single-page dashboard and REST API.

## Quick Start

```bash
# Run locally
uvicorn rosteriq.api_v2:app --reload --port 8000

# Run tests (no external deps required)
python run_all_tests.py --fast

# Run full suite (needs httpx, fastapi, pyjwt, passlib)
python run_all_tests.py
```

## Architecture

**Entry point:** `rosteriq/api_v2.py` — FastAPI app, all `/api/v1/*` endpoints.
**Dashboard:** `static/dashboard.html` — single-file SPA (vanilla JS, no build step).
**Deployment:** `Dockerfile` → Railway via `railway.toml`.

### Core Modules (pure-stdlib, no FastAPI imports)

| Module | Purpose |
|---|---|
| `roster_engine.py` | Constraint-based roster generation, scoring, optimization |
| `award_engine.py` | Australian award interpretation (penalty rates, overtime, allowances) |
| `forecast_engine.py` | Demand forecasting from signals + historical data |
| `pipeline.py` | Orchestrates engine → forecast → roster flow per venue |
| `signal_feeds.py` | Weather, events, bookings, foot traffic, delivery feeds |
| `query_library.py` | Natural language query routing for /ask endpoint |
| `brief_dispatcher.py` | Morning brief + weekly digest email dispatch |
| `accountability_store.py` | Recommendation tracking (accepted/dismissed/expired) |
| `headcount_store.py` | Real-time head count clicker state |
| `shift_recap.py` | End-of-day shift performance summaries |
| `morning_brief.py` | Pre-shift intelligence briefing builder |
| `weekly_digest.py` | Multi-day trend digest builder |
| `trends.py` | Labour efficiency trend analysis |
| `portfolio_recap.py` | Multi-venue portfolio summary |
| `pulse_rec_bridge.py` | Links wage pulse data to actionable recommendations |
| `scheduled_jobs.py` | Deterministic tick-driven in-process scheduler |
| `tanda_writeback.py` | Write roster changes back to Tanda (journal + API sinks) |

### Integration Modules (need external deps)

| Module | Deps | Purpose |
|---|---|---|
| `auth.py` | pydantic, pyjwt, passlib | JWT auth, API keys, user management |
| `tanda_adapter.py` | httpx | Tanda API client (rosters, shifts, employees) |
| `tanda_integration.py` | httpx | Tanda webhook receiver + sync |
| `shift_swap.py` | fastapi | Shift swap + notification system with API router |
| `data_feeds/swiftpos.py` | httpx | SwiftPOS sales feed adapter |
| `data_feeds/lightspeed.py` | httpx | Lightspeed K-Series POS adapter |
| `data_feeds/square.py` | httpx | Square POS adapter |
| `data_feeds/pos_aggregator.py` | httpx | Unified multi-POS signal aggregator |
| `feed_runner.py` | httpx | Background feed polling orchestrator |

### Key Patterns

- **Pure-stdlib business logic:** Core modules avoid FastAPI/Pydantic imports so tests run without heavy deps. api_v2.py handles HTTP plumbing.
- **Venue-scoped:** Most endpoints and stores are keyed by `venue_id`. Pipeline instances cached per-venue.
- **Demo mode (default):** Auth disabled, synthetic data generated deterministically per venue_id. Set `ROSTERIQ_AUTH_ENABLED=1` for JWT enforcement.
- **Dead-letter pattern:** Tanda writeback failures go to a dead-letter file, swept periodically by the scheduler.
- **Tick-driven scheduler:** `scheduled_jobs.py` uses injectable monotonic clock for deterministic testing. Jobs registered in api_v2.py lifespan.

### Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `ROSTERIQ_AUTH_ENABLED` | `""` (off) | Enable JWT auth on all /api/v1/* endpoints |
| `RIQ_JWT_SECRET` | dev key | JWT signing secret (CHANGE IN PROD) |
| `ROSTERIQ_SCHEDULER_ENABLED` | `""` (off) | Enable background scheduler thread |
| `ROSTERIQ_VENUES` | 3 demo venues | JSON array of `{id, label}` venue configs |
| `TANDA_*` | — | Tanda API credentials (see tanda_adapter.py) |

## Testing

23 test files, ~500+ tests. Run with `python run_all_tests.py`.

- `--fast` skips tests needing external deps (httpx, fastapi, pyjwt)
- `--only NAME` runs a single test file by partial name match
- Tests that fail with ModuleNotFoundError are classified as SKIP, not FAIL
- CI: `.github/workflows/ci.yml` runs on Python 3.11 + 3.12

## Git Workflow

- Main branch: `main`
- Isolated index workaround for clean commits: use `GIT_INDEX_FILE` to avoid stale staged files
- Commits follow conventional format: `area: description`
