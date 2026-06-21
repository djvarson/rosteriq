# Deputy OAuth Setup Runbook

This sets up **one-click "Connect with Deputy login"** for venues. It's optional —
the **paste-a-permanent-token** path needs none of this and is the recommended
path for the pilot. Use OAuth when you want venues to connect without generating a
token themselves.

I can't perform these steps for you: registering the app happens inside Deputy's
developer console (your login), and the redirect URI depends on your deploy domain.
Everything below is exact except the one value only you know — your domain.

---

## Step 0 — Your deploy domain  →  Railway

You're deploying on Railway, and the app already defaults to:

```
https://rosteriq-production-6aaf.up.railway.app
```

So everywhere below, `<YOUR_DOMAIN>` = `rosteriq-production-6aaf.up.railway.app`, and
the Deputy redirect URI is:

```
https://rosteriq-production-6aaf.up.railway.app/deputy/callback
```

This is exactly the code's `DEPUTY_REDIRECT_URI` default — so you do **not** need to
set `DEPUTY_REDIRECT_URI` at all; just register that URL in Deputy (Step 1) and set
the Client ID/Secret (Step 3).

> ⚠️ Confirm that's your live Railway domain (Railway → your service → Settings →
> Domains). If you've attached a custom domain or Railway generated a different
> subdomain, use that instead and set `DEPUTY_REDIRECT_URI` to match.

---

## Step 1 — Register the OAuth app in Deputy

1. Sign in to the **Deputy Developer Portal** (developer.deputy.com) with an account
   that can create apps. (Deputy OAuth uses the `once.deputy.com` identity service —
   our code talks to `https://once.deputy.com/my/oauth/login` and
   `.../my/oauth/access_token`.)
2. Create a new **OAuth 2.0** app / integration.
3. Set the **Redirect URI** to EXACTLY (must match byte-for-byte, no trailing slash):

   ```
   https://<YOUR_DOMAIN>/deputy/callback
   ```

   (The callback is also mounted at `/api/deputy/callback`; use `/deputy/callback`.)
4. Request the scope **`longlife_refresh_token`** (long-lived tokens, so refresh
   keeps working — this is the scope our connect flow requests).
5. Save, and copy the **Client ID** and **Client Secret**.

---

## Step 2 — (one-time) confirm the OAuth values our app uses

These are already in the code (`deputy_adapter.py` / `routes/deputy.py`) — listed so
you can verify they match what you registered:

| Thing | Value |
|-------|-------|
| Authorize URL | `https://once.deputy.com/my/oauth/login` |
| Token URL | `https://once.deputy.com/my/oauth/access_token` |
| Scope | `longlife_refresh_token` |
| Redirect path | `/deputy/callback` |
| Grant type | authorization_code (+ refresh_token) |

---

## Step 3 — Set the environment variables

On the RosterIQ server/deployment, set:

```bash
DEPUTY_CLIENT_ID=<the Client ID from Step 1>
DEPUTY_CLIENT_SECRET=<the Client Secret from Step 1>
DEPUTY_REDIRECT_URI=https://<YOUR_DOMAIN>/deputy/callback
```

(`.env.example` documents these. The paste-a-token path needs none of them.)

Restart so the app picks them up.

---

## Step 4 — Verify

1. Open `/connections` in RosterIQ. The **Deputy → "Connect with Deputy login (OAuth)"**
   button should now be **enabled** (it greys out with "Needs server setup" until
   `DEPUTY_CLIENT_ID/SECRET/REDIRECT_URI` are all set).
2. Click **Connect**. You're redirected to Deputy to log in and authorise.
3. Deputy redirects back to `https://<YOUR_DOMAIN>/deputy/callback`. The app:
   - exchanges the code for tokens (and persists them, with refresh),
   - auto-detects your Deputy subdomain,
   - **imports staff + the next 14 days of rosters**,
   - flips the Deputy card to **Connected**.

If the button stays greyed: an env var is missing/typo'd. If Deputy shows a
redirect-mismatch error: the registered Redirect URI ≠ `DEPUTY_REDIRECT_URI`
(they must be identical).

---

## Fallback — no registration needed (recommended for the pilot)

A venue can connect Deputy with a permanent token instead, no OAuth app:

1. In Deputy: **Integrations → Install new → "Deputy API" → generate a Permanent Token**.
2. In RosterIQ `/connections` → Deputy → **"Paste a permanent token"** → enter the
   Deputy **subdomain** + the token → Connect.

Same result (staff + 14 days of rosters imported), zero server-side setup.

---

## Repeating this for Humanforce / MYOB / Xero

Same shape, different values (all in `.env.example`):

| Provider | Redirect URI to register | Env vars |
|----------|--------------------------|----------|
| Deputy | `https://<YOUR_DOMAIN>/deputy/callback` | `DEPUTY_CLIENT_ID/SECRET/REDIRECT_URI` |
| Humanforce | `https://<YOUR_DOMAIN>/api/humanforce/callback` | `HUMANFORCE_CLIENT_ID/SECRET/REDIRECT_URI` |
| MYOB | `https://<YOUR_DOMAIN>/api/myob/callback` | `MYOB_CLIENT_ID/SECRET/REDIRECT_URI` |
| Xero | `https://<YOUR_DOMAIN>/api/xero/callback` | `XERO_CLIENT_ID/SECRET/REDIRECT_URI` |

MYOB and Xero also support a paste-credentials path; Humanforce is OAuth-only.
