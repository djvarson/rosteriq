# RosterIQ Deployment Guide

## Option 1: Railway (Recommended — simplest)

1. **Create account**: https://railway.app (free tier includes PostgreSQL)

2. **Deploy from GitHub**:
   ```bash
   # Push your repo to GitHub first
   git init && git add -A && git commit -m "Initial commit"
   gh repo create rosteriq --private --push
   ```

3. **In Railway dashboard**:
   - Click "New Project" → "Deploy from GitHub repo"
   - Select your RosterIQ repo
   - Railway auto-detects the Dockerfile

4. **Add PostgreSQL**:
   - Click "+ New" → "Database" → "PostgreSQL"
   - Railway auto-sets `DATABASE_URL` in your service

5. **Run schema migration**:
   - Go to your PostgreSQL service → "Data" tab → "Query"
   - Paste contents of `schema.sql` and run

6. **Set environment variables** (Settings → Variables):
   ```
   TANDA_CLIENT_ID=your_client_id
   TANDA_CLIENT_SECRET=your_client_secret
   TANDA_REDIRECT_URI=https://your-app.up.railway.app/tanda/callback
   ```

7. **Custom domain**:
   - Settings → Networking → Custom Domain
   - Add `api.rosteriq.com.au`
   - Railway gives you a CNAME target

## Option 2: Render

1. Push to GitHub
2. Go to https://render.com → "New" → "Blueprint"
3. Connect your repo — Render reads `render.yaml` automatically
4. PostgreSQL and API service spin up together
5. Set Tanda env vars in the dashboard

## Option 3: Docker Compose (self-hosted / VPS)

```bash
# On a $5/mo DigitalOcean droplet (Ubuntu 22.04)
ssh root@your-server
git clone your-repo /opt/rosteriq
cd /opt/rosteriq
cp .env.example .env
nano .env  # Set your DB_PASSWORD and Tanda creds
./start.sh
```

Add nginx reverse proxy + Let's Encrypt for HTTPS:
```bash
apt install certbot python3-certbot-nginx
certbot --nginx -d api.rosteriq.com.au -d app.rosteriq.com.au
```

## Domain Setup: rosteriq.com.au

1. **Register** at any AU registrar (VentraIP, Crazy Domains, Namecheap):
   - Search for `rosteriq.com.au`
   - ABN required for .com.au (use your existing business ABN)

2. **DNS records** (add in your registrar's DNS panel):

   | Type  | Name  | Value                            | TTL  |
   |-------|-------|----------------------------------|------|
   | A     | @     | Your server IP                   | 300  |
   | CNAME | api   | your-app.up.railway.app          | 300  |
   | CNAME | app   | your-app.up.railway.app          | 300  |
   | CNAME | www   | rosteriq.com.au                  | 300  |
   | MX    | @     | Your email provider              | 3600 |
   | TXT   | @     | v=spf1 include:_spf.google.com ~all | 3600 |

3. **Update Tanda redirect URI** to `https://api.rosteriq.com.au/tanda/callback`

## Environment Variables Reference

| Variable             | Required | Description                          |
|----------------------|----------|--------------------------------------|
| DATABASE_URL         | Yes*     | PostgreSQL connection string         |
| TANDA_CLIENT_ID      | No       | From Tanda partner portal            |
| TANDA_CLIENT_SECRET  | No       | From Tanda partner portal            |
| TANDA_REDIRECT_URI   | No       | OAuth callback URL                   |
| TANDA_WEBHOOK_SECRET | No       | For verifying webhook signatures     |
| CORS_ORIGINS         | No       | Comma-separated allowed origins      |
| ROSTERIQ_ENV         | No       | "production" or "development"        |

*If DATABASE_URL is not set, the API uses in-memory storage (fine for demos).

## Post-Deploy Checklist

- [ ] API responds at /health
- [ ] Dashboard loads at root URL
- [ ] /demo/load seeds test data successfully
- [ ] /docs shows Swagger UI
- [ ] PostgreSQL schema applied (check /venues returns [])
- [ ] HTTPS working with valid certificate
- [ ] Tanda OAuth flow tested (if credentials set)
- [ ] Webhook URL registered in Tanda dashboard
