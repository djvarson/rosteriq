# Pilot Connections Guide

How a pilot venue connects its systems to RosterIQ. The app has a **Connections
hub** at **`/connections`** that lists every integration, shows its live status,
and walks the venue through connecting each one with on-screen directions. This
document is the operator's pre-work checklist: what to gather before the venue
sits down at the hub.

> Two ways to connect (you can mix):
> - **Paste a key/token** — simplest, no setup on our side. The venue gets a
>   key/token from the provider and pastes it into the hub.
> - **OAuth (one-click login)** — nicer UX, but requires us to register RosterIQ
>   as an app with that provider first and set the redirect URI (env vars below).
>   The hub greys out an OAuth button until its server env is configured, so it
>   never shows a button that will fail.

The hub reads connection status live; after a successful connect the badge flips
to **Connected**.

---

## Workforce & rostering

### Deputy  *(imports staff + next 14 days of rosters on connect)*
- **Paste a token (recommended for pilot):** in Deputy → Integrations → Install
  new → "Deputy API" → generate a **Permanent Token**. The venue also needs its
  **subdomain** (the part before `.deputy.com`). Paste both in the hub.
- **OAuth:** set `DEPUTY_CLIENT_ID`, `DEPUTY_CLIENT_SECRET`, `DEPUTY_REDIRECT_URI`
  (register the redirect URI in your Deputy OAuth app).

### Tanda  *(staff + rosters)*
- Installed from the **Tanda Marketplace** (find RosterIQ, click Install). No
  paste flow — Tanda connects it automatically.

### Humanforce  *(staff + rosters)*
- **OAuth only:** set `HUMANFORCE_CLIENT_ID`, `HUMANFORCE_CLIENT_SECRET`,
  `HUMANFORCE_REDIRECT_URI`.

---

## Point of sale  *(streams hourly sales → grades forecasts against real takings)*

### SwiftPOS
- Ask SwiftPOS to enable API access and issue **Client ID + Client Secret**.
  Paste both. (We validate before saving.)

### Lightspeed Restaurant
- From the Lightspeed developer portal: **Client ID + Client Secret + Refresh
  Token** for the account.

### Kounta (Lightspeed K-Series)
- In Kounta → Add-ons / API → generate an **API key**. Paste it.

> POS sales are persisted as observed actuals, so once connected the forecast
> accuracy view starts grading predictions against real covers/transactions.

---

## Bookings & reservations  *(bookings/covers become a demand signal)*

### ResDiary · NowBookIt · OpenTable · SevenRooms · bookitLive
- Each: create an **API key** in the provider's integration/API settings and
  paste it (plus the provider's venue ID if it differs from RosterIQ's).

### Other / theatre booking system  *(no native adapter)*
- Use **"Other booking system (file import)"** in the hub: upload a **CSV** with
  columns `date` (YYYY-MM-DD), `party_size` (or `covers`), and optional `time`
  (HH:MM). Re-upload nightly, or POST JSON to
  `/api/reservations/direct/ingest` if the booking system can fire webhooks.

---

## Accounting & payroll

### Xero  *(pulls revenue, pushes labour-cost journals; P&L / labour %)*
- **OAuth:** set `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET`, `XERO_REDIRECT_URI`.
- For payroll **export** (timesheets → Xero), the venue's Xero earning rates must
  be mapped to RosterIQ earning types. Either let RosterIQ read them live from the
  tenant's PayItems, or pin them via an `earnings_rate_ids` map on the stored Xero
  credentials. Export fails loud (pushes nothing) until earning rates resolve.

### MYOB  *(imports staff; exports timesheets)*
- **Paste:** register an app at developer.myob.com for an **API key + secret**,
  authorise it for an **access token**, and note the **company file URI**. Paste
  all four.
- **OAuth:** set `MYOB_CLIENT_ID`, `MYOB_CLIENT_SECRET`, `MYOB_REDIRECT_URI`.

### KeyPay (Employment Hero Payroll)  *(exports timesheets)*
- In KeyPay → profile → Manage → generate an **API key**; note the **Business
  ID**. Paste both. (We reject an invalid key on connect.)

---

## Server env for OAuth (only if you use OAuth instead of paste-a-token)

Set these for each OAuth provider you enable, then the hub un-greys its Connect
button:

| Provider   | Env vars |
|------------|----------|
| Deputy     | `DEPUTY_CLIENT_ID`, `DEPUTY_CLIENT_SECRET`, `DEPUTY_REDIRECT_URI` |
| Humanforce | `HUMANFORCE_CLIENT_ID`, `HUMANFORCE_CLIENT_SECRET`, `HUMANFORCE_REDIRECT_URI` |
| Xero       | `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET`, `XERO_REDIRECT_URI` |
| MYOB       | `MYOB_CLIENT_ID`, `MYOB_CLIENT_SECRET`, `MYOB_REDIRECT_URI` |

Also set `FEED_CONFIG_ENCRYPTION_KEY` in production — it encrypts stored
credentials (and connector tokens) at rest. Without it, encryption is disabled
(dev only).

---

## API reference (the hub uses these)

- `GET  /api/connections/catalog` — all connectors + directions + form specs
- `GET  /api/connections/venue/{venue_id}` — live status per connector
- `GET  /api/connections/venue/{venue_id}/summary` — connected/total roll-up

Connect/disconnect happen on each provider's own routes (the catalog tells the
hub which endpoint + fields to use), e.g. `POST /api/deputy/install-token`,
`POST /api/pos/swiftpos/install`, `POST /api/keypay/install`,
`POST /api/reservations/{provider}/install`,
`POST /api/reservations/direct/upload`.
