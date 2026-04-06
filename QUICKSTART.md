# RosterIQ Quick Start Guide

Get RosterIQ running locally in 3 commands, or deploy to Railway in 5 commands.

## Prerequisites

### For Local Development
- **Python 3.11** or later: [python.org](https://www.python.org/downloads/)
- **Docker & Docker Compose**: [docker.com](https://www.docker.com/get-started)
- **PostgreSQL** (if running natively): [postgresql.org](https://www.postgresql.org/download/)

### For Railway Deployment
- **Railway account**: [railway.app](https://railway.app) (free tier available)
- **GitHub account** with your repo pushed (Railway auto-deploys on git push)
- **Railway CLI** (optional): `npm install -g @railway/cli`
- **Python 3.11** (for local testing before deploy)

---

## Local Development (3 Commands)

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

This installs all Python dependencies including FastAPI, psycopg2, xgboost, prophet, and more.

### 2. Start Database & API with Docker Compose

```bash
docker-compose up -d
```

This starts:
- PostgreSQL 16 on `localhost:5432`
- RosterIQ API on `localhost:8000` (with hot-reload)
- Auto-seeds demo data on first run

### 3. Access the Application

- **Dashboard**: http://localhost:8000 (login with `demo@rosteriq.local` / `DemoPass123!`)
- **API Docs**: http://localhost:8000/docs (interactive Swagger UI)
- **Health Check**: http://localhost:8000/health

#### Demo Venue
- **Venue**: The Royal Oak (Fitzroy, VIC)
- **Staff**: 12 team members
- **Forecast**: 2 weeks of demand data

#### Stop the Stack
```bash
docker-compose down
```

Remove database volume:
```bash
docker-compose down -v
```

---

## Railway Deployment (5 Commands)

### Prerequisites Setup

1. **Create Railway Project**
   - Go to https://railway.app
   - Click "Create New Project"
   - Select "Deploy from GitHub repo"
   - Choose your RosterIQ GitHub repository

2. **Add Environment Variables**
   - In Railway dashboard: Project → Variables
   - Add these variables:
     - `RIQ_JWT_SECRET`: Generate with `python -c "import secrets; print(secrets.token_urlsafe(32))"`
     - `CORS_ORIGINS`: Set to your domain (e.g., `https://myapp.railway.app`)

3. **Add PostgreSQL Plugin**
   - In Railway: "+ Add" → Marketplace → PostgreSQL
   - Railway automatically sets `DATABASE_URL`

### Deploy (5 Commands)

Railway auto-deploys on `git push main` via GitHub Actions, but here's the manual process:

```bash
# 1. Install Railway CLI
npm install -g @railway/cli

# 2. Login to Railway
railway login

# 3. Link your Railway project
railway link

# 4. Deploy your code
railway up

# 5. View logs in real-time
railway logs
```

**That's it!** Railway will:
- Build Docker image
- Run `scripts/railway_setup.sh` (init DB, seed data, start server)
- Auto-scale based on traffic
- Provide HTTPS URL

### After Deployment

1. **Get Your URL**
   ```bash
   railway status
   ```
   Shows your public Railway URL

2. **Access Your App**
   - Dashboard: `https://your-railway-url.railway.app`
   - API: `https://your-railway-url.railway.app/docs`
   - Health: `https://your-railway-url.railway.app/health`

3. **Monitor in Dashboard**
   - Railway dashboard shows real-time logs
   - CPU/memory usage
   - Build and deployment status

---

## First Venue Onboarding

### Demo Data (Already Set Up)
- Venue is auto-seeded: **The Royal Oak**
- Location: Fitzroy, VIC
- Staff: 12 team members
- Data: 2 weeks of forecast

Login with:
- Email: `demo@rosteriq.local`
- Password: `DemoPass123!`

### Adding Your Own Venue

To onboard a real venue (after demo):

1. **Register a New User** (if needed)
   - Call `/auth/register` endpoint
   - Or use dashboard registration UI

2. **Create Venue**
   - Visit `/onboarding.html`
   - Enter venue details:
     - Venue name
     - Address (autocomplete via Google Places)
     - State (VIC, NSW, QLD, etc.)
     - Venue type (suburban, city, regional)
     - Manager name

3. **Configure Data Feeds**
   - Access feed configuration at `/feeds/status`
   - Free feeds auto-enabled (school holidays, sports, weather)
   - Optional paid feeds with API keys:
     - Google Places (foot traffic)
     - Ticketmaster (events)
     - PredictHQ (global events)
     - ResDiary (reservations)
     - NowBookIt (bookings)
     - Uber Eats (delivery)
     - SportRadar (sports events)

4. **Generate API Key** (for integrations)
   - POST `/auth/api-keys` with owner/admin role
   - Returns key in format: `riq_...`
   - Use in `X-API-Key` header for programmatic access

---

## Environment Variables Reference

### Core Configuration (Required in Production)

```bash
# PostgreSQL connection string
DATABASE_URL=postgresql://username:password@host:5432/database

# JWT secret for authentication tokens
# Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
RIQ_JWT_SECRET=your-strong-random-secret-here

# Comma-separated list of allowed origins for CORS
CORS_ORIGINS=https://app.rosteriq.com,https://dashboard.rosteriq.com
```

### Third-Party Integrations (Optional, for Full Features)

```bash
# Tanda POS Integration
TANDA_CLIENT_ID=your-tanda-client-id
TANDA_CLIENT_SECRET=your-tanda-secret

# Google Places (venue lookups, foot traffic)
GOOGLE_PLACES_API_KEY=your-google-api-key

# Weather Forecasting
OPENWEATHERMAP_API_KEY=your-openweathermap-key

# Events & Entertainment
TICKETMASTER_API_KEY=your-ticketmaster-key
EVENTBRITE_TOKEN=your-eventbrite-token
PREDICTHQ_TOKEN=your-predicthq-token

# Restaurant Systems
RESDIARY_API_KEY=your-resdiary-key
NOWBOOKIT_API_KEY=your-nowbookit-key

# Food Delivery
UBEREATS_CLIENT_ID=your-ubereats-id
UBEREATS_CLIENT_SECRET=your-ubereats-secret

# Sports Events
SPORTRADAR_API_KEY=your-sportradar-key
```

### Application Settings (Optional)

```bash
# Environment mode (development or production)
ENVIRONMENT=production

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL=INFO

# API Port (set by Railway via $PORT)
PORT=8000
```

---

## Troubleshooting

### Local Development Issues

**Problem**: Docker container fails to start
```bash
# Check logs
docker-compose logs -f api

# Rebuild image
docker-compose down
docker-compose build --no-cache
docker-compose up
```

**Problem**: Database connection error
```bash
# Verify PostgreSQL is running
docker-compose logs postgres

# Check connection string in .env
echo $DATABASE_URL

# Test connection manually
psql $DATABASE_URL -c "SELECT 1;"
```

**Problem**: Demo user not created
```bash
# Manually run seed script
docker-compose exec api python scripts/seed_demo.py

# Or reinitialize with fresh database
docker-compose down -v
docker-compose up -d
```

### Railway Deployment Issues

**Problem**: Deployment fails with database error
```bash
# Check if PostgreSQL plugin is added
# In Railway dashboard: check "Plugins" tab

# Verify DATABASE_URL is set
# In Variables tab, check DATABASE_URL exists

# View deployment logs
railway logs
```

**Problem**: Application starts but returns 502 errors
```bash
# Check health endpoint
curl https://your-url.railway.app/health

# View server logs
railway logs

# Check environment variables are set
railway variables
```

**Problem**: Demo data not seeding on Railway
```bash
# The seed runs automatically on first deploy
# If it failed, manually trigger it:
railway run python scripts/seed_demo.py

# Or re-deploy to trigger seed again
git push main
```

### API Issues

**Problem**: 401 Unauthorized
- Ensure `X-API-Key` header is set (for API key auth)
- Or ensure `Authorization: Bearer <token>` header is set (for JWT)

**Problem**: CORS errors in dashboard
- Check `CORS_ORIGINS` environment variable
- Ensure your frontend URL is included

**Problem**: Forecast data not updating
- Check `/feeds/status` endpoint
- Manually trigger with `POST /feeds/fetch-now`
- Verify database has `external_signals` table

---

## Development Commands

### Python & Testing
```bash
# Check syntax
python -m py_compile rosteriq/auth.py rosteriq/feed_runner.py

# Run linter
ruff check rosteriq/

# Run formatter
ruff format rosteriq/

# Type checking (if using mypy)
mypy rosteriq/ --ignore-missing-imports
```

### Docker
```bash
# View logs (last 100 lines)
docker-compose logs --tail 100 api

# Follow logs in real-time
docker-compose logs -f api

# Execute command in running container
docker-compose exec api python scripts/seed_demo.py

# Rebuild single service
docker-compose up --build api
```

### Database
```bash
# Connect to database
psql $DATABASE_URL

# List tables
\dt

# View schema
\d venues

# Run SQL query
psql $DATABASE_URL -c "SELECT COUNT(*) FROM venues;"

# Backup database
pg_dump $DATABASE_URL > backup.sql

# Restore database
psql $DATABASE_URL < backup.sql
```

### Railway CLI
```bash
# View project status
railway status

# View environment variables
railway variables

# Set a variable
railway variables set KEY=value

# Run command in Railway environment
railway run python scripts/seed_demo.py

# Open dashboard
railway open

# SSH into container (advanced)
railway shell
```

---

## Security Checklist

Before going to production:

- [ ] Change `RIQ_JWT_SECRET` to a strong random value (not the default)
- [ ] Set `CORS_ORIGINS` to your actual frontend domain only
- [ ] Rotate PostgreSQL password from defaults
- [ ] Enable PostgreSQL backups (Railway handles this)
- [ ] Set `ENVIRONMENT=production` to disable debug mode
- [ ] Add `.env` to `.gitignore` (never commit secrets)
- [ ] Enable HTTPS (Railway provides free HTTPS)
- [ ] Set up error monitoring (Sentry recommended)
- [ ] Enable database encryption at rest (Railway provides)
- [ ] Implement rate limiting on API endpoints
- [ ] Set up audit logging for staff access
- [ ] Rotate API keys regularly

---

## Next Steps

1. **Local Testing**: `docker-compose up` and access http://localhost:8000
2. **Try the API**: Visit http://localhost:8000/docs for interactive documentation
3. **Deploy**: Push to GitHub and Railway auto-deploys
4. **Monitor**: Check Railway dashboard for logs and metrics
5. **Add API Keys**: Configure third-party integrations for full features
6. **Invite Users**: Create additional user accounts for your team

For more details, see:
- [Full Deployment Guide](DEPLOYMENT.md)
- [API Documentation](http://localhost:8000/docs)
- [Railway Documentation](https://docs.railway.app/)

---

**Support**: For issues or questions, check the RosterIQ documentation or open an issue on GitHub.
