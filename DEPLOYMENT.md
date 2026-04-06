# RosterIQ Deployment Guide

Complete deployment setup for RosterIQ from local development to production.

## Quick Start - Local Development

### Prerequisites
- Docker and Docker Compose installed
- Python 3.11+ (optional, for running scripts directly)

### 1. Set Up Environment Variables

```bash
cp .env.example .env
# Edit .env and add your API keys for:
# - TANDA_CLIENT_ID/SECRET
# - GOOGLE_PLACES_API_KEY
# - etc.
```

### 2. Start Local Stack

```bash
docker-compose up -d
```

This starts:
- PostgreSQL 16 on localhost:5432
- RosterIQ API on localhost:8000 (with hot-reload)

### 3. Seed Demo Data

The demo data seeds automatically on first run, creating:
- Venue: "The Royal Oak" (Fitzroy, VIC)
- User: demo@rosteriq.com / demo123
- 12 staff members
- 2 weeks forecast data

To manually seed:
```bash
docker-compose exec api python scripts/seed_demo.py
```

### 4. Access the Application

- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health
- Dashboard: http://localhost:3000 (if running separate frontend)

### Stopping the Stack

```bash
docker-compose down
```

To also remove the database volume:
```bash
docker-compose down -v
```

---

## Production Deployment

### Option 1: Railway.app (Recommended for Fast Deployment)

Railway is a modern platform perfect for quick deployments with minimal configuration.

**Setup:**

1. Push your code to GitHub
2. Go to [railway.app](https://railway.app)
3. Create new project and connect your GitHub repo
4. Railway auto-detects `railway.toml` and deploys
5. Add environment variables in Railway dashboard:
   - DATABASE_URL (auto-created if you add PostgreSQL plugin)
   - RIQ_JWT_SECRET (generate with: `python -c "import secrets; print(secrets.token_urlsafe(32))"`)
   - Third-party API keys (TANDA, Google Places, etc.)

**Cost:** ~$7/month + database costs
**Deployment Time:** 2-3 minutes

---

### Option 2: Render.com (Alternative with Blueprints)

Render offers infrastructure-as-code with `render.yaml`.

**Setup:**

1. Commit `render.yaml` to your repo
2. Go to [render.com](https://render.com)
3. Select "Infrastructure" > "New"
4. Connect your GitHub repo and select `render.yaml`
5. Deploy via git push to main branch

**Cost:** ~$15/month (database) + ~$7/month (API)
**Deployment Time:** 2-3 minutes

---

### Option 3: Docker to Any Cloud (AWS/GCP/Azure/DigitalOcean)

Use the Dockerfile for maximum flexibility.

**Steps:**

1. Build image:
   ```bash
   docker build -t rosteriq:latest .
   ```

2. Push to container registry:
   ```bash
   # AWS ECR
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789.dkr.ecr.us-east-1.amazonaws.com
   docker tag rosteriq:latest 123456789.dkr.ecr.us-east-1.amazonaws.com/rosteriq:latest
   docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/rosteriq:latest
   ```

3. Deploy using cloud provider tools:
   - **AWS:** ECS / App Runner
   - **GCP:** Cloud Run / GKE
   - **Azure:** Container Instances / App Service
   - **DigitalOcean:** App Platform / Kubernetes

---

## Database Migrations

The `deploy.sh` script runs migrations from `schema.sql`:

```bash
./scripts/deploy.sh
```

To run migrations manually:
```bash
psql $DATABASE_URL -f schema.sql
```

---

## Environment Variables

All required variables (from `.env.example`):

### Core Configuration
- `DATABASE_URL`: PostgreSQL connection string
- `RIQ_JWT_SECRET`: JWT signing secret (generate a random strong value)
- `CORS_ORIGINS`: Comma-separated allowed origins (frontend URLs)

### Third-Party Integrations
- `TANDA_CLIENT_ID/SECRET`: POS system integration
- `GOOGLE_PLACES_API_KEY`: Venue lookups, competitor analysis
- `OPENWEATHERMAP_API_KEY`: Weather-based demand prediction
- `TICKETMASTER_API_KEY`: Local events
- `PREDICTHQ_TOKEN`: Global events and holidays
- `RESDIARY_API_KEY`: Restaurant reservations
- `NOWBOOKIT_API_KEY`: Booking system
- `UBEREATS_CLIENT_ID/SECRET`: Food delivery volumes
- `SPORTRADAR_API_KEY`: Sports events

---

## Monitoring & Health Checks

### Health Endpoint

All deployment configs include health checks:

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "database": "connected",
  "timestamp": "2026-04-05T10:30:00Z"
}
```

### Logs

**Docker Compose:**
```bash
docker-compose logs -f api
docker-compose logs -f postgres
```

**Production (Railway/Render):**
- Dashboard automatically shows real-time logs
- Typically integrated with error tracking (Sentry, etc.)

---

## Scaling Considerations

### Local Development
- Single-container setup with hot-reload
- Suitable for 1-5 venues

### Production at Scale

1. **Horizontal Scaling:**
   - Run multiple API instances behind a load balancer
   - Railway/Render handle this automatically

2. **Database:**
   - As data grows, consider read replicas
   - Implement query optimization for forecast tables
   - Add caching layer (Redis) for frequently accessed data

3. **Async Tasks:**
   - Forecast generation should run as background jobs
   - Consider Celery + Redis for task queue

4. **CDN:**
   - Serve static assets (dashboard) from CDN
   - Reduces API server load

---

## Troubleshooting

### Database Connection Issues

```bash
# Test connection
psql $DATABASE_URL -c "SELECT 1;"

# Check for active connections
psql $DATABASE_URL -c "SELECT * FROM pg_stat_activity;"
```

### API Not Starting

```bash
# Check logs
docker-compose logs api

# Verify health check
curl -v http://localhost:8000/health
```

### Demo Data Not Seeding

```bash
# Run seed script with verbose output
docker-compose exec api python scripts/seed_demo.py

# Check if tables exist
psql $DATABASE_URL -c "\dt"
```

---

## Security Checklist

Before deploying to production:

- [ ] Change RIQ_JWT_SECRET to a strong random value
- [ ] Enable HTTPS on your domain
- [ ] Set CORS_ORIGINS to only your frontend domain(s)
- [ ] Rotate database password from default
- [ ] Enable database backups
- [ ] Set up error monitoring (Sentry, etc.)
- [ ] Enable rate limiting on API endpoints
- [ ] Set up WAF (Web Application Firewall) rules
- [ ] Enable audit logging for staff access

---

## File Structure

```
rosteriq-deploy/
├── Dockerfile                 # Production container image
├── requirements.txt           # Python dependencies
├── docker-compose.yml        # Local dev stack
├── .env.example              # Environment template
├── railway.toml              # Railway.app config
├── render.yaml               # Render.com infrastructure
├── DEPLOYMENT.md             # This file
└── scripts/
    ├── deploy.sh             # Production startup script
    └── seed_demo.py          # Demo data generator
```

---

## Support & Next Steps

1. **Local Testing:** Start with `docker-compose up`
2. **Deploy Preview:** Use Railway/Render for staging
3. **Production:** Configure production secrets and deploy
4. **Monitoring:** Set up error tracking and alerting

For questions, check the RosterIQ documentation or open an issue.
