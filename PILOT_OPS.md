# Pilot operations — the rules that start the day a real venue plugs in

RosterIQ has one production environment. That is fine while it is a demo;
it is not fine once a venue's roster and timesheets live in it. These are
the operating rules from the first real venue onward.

## 1. Deploy discipline (the freeze)

Until the pilot has its own Railway service, production IS the pilot:

- **No direct pushes to `main`.** Work lands on a branch; `main` only moves
  by fast-forward after the full suite is green AND the change was exercised
  on a local server (or a throwaway prod tenant for DB-touching changes).
- **Deploy windows:** ship Mon–Thu before 3pm Perth. Never Friday, never
  during a venue's service hours (check their roster — we can see it).
- **Every deploy is watched:** after Railway promotes, verify `/ready`
  returns 200 and the behaviour marker of the change on prod. Railway's
  healthcheck now gates on `/ready` (deep check: DB connectivity, durable
  Postgres store, JWT secret) — a broken build never replaces a good one,
  but watch anyway.
- **Rollback:** `git revert` the commit and push; Railway redeploys the
  revert. Never force-push `main`.
- Better (when there are two venues or one fussy one): a second Railway
  service `rosteriq-pilot` pinned to a `pilot` branch; `main` keeps moving,
  `pilot` fast-forwards only after soak on the main service.

## 2. Backups — drilled, not hoped

- Railway → Postgres service → Backups: confirm daily backups are ON and
  note the retention window. (Dashboard-side; needs the Railway login.)
- **Restore drill:** run `scripts/restore_drill.sh` with the Railway
  `DATABASE_URL` — it dumps the live DB (read-only), restores into a
  throwaway database on the same server, compares critical-table row
  counts, and drops the throwaway. Run it monthly and before every pilot
  onboarding. Log each run here:

  | Date | Result | Run by |
  |------|--------|--------|
  | _pending first drill_ | | |

- Per-venue export (`GET /api/v1/venues/{id}/export`, venue-manager gated)
  is the venue's data-portability path, not our disaster recovery.

## 3. Monitoring — someone finds out before the venue does

- **Uptime:** point a free UptimeRobot (or similar) monitor at
  `https://<prod-domain>/ready`, 5-minute interval, alerting Dale's phone
  and email. `/ready` is a deep check, so this catches DB loss and config
  regressions, not just process death. (External account; needs Dale.)
- **In-app:** the System health card (Comms → Activity) surfaces repeat
  errors, slow routes and integration failures from `/api/events/insights`.
  Check it whenever you deploy and each Monday.
- **Railway:** enable deploy + crash notifications to email in the Railway
  dashboard.

## 4. Pilot data honesty

- **Payroll runs parallel.** RosterIQ plans, costs and records time; the
  venue's existing payroll remains the source of truth for actual pay until
  award rate values have payroll/IR advisor sign-off. Say this to the venue
  in exactly those words.
- Work-rights (visa) data is recorded from the venue's own VEVO checks;
  RosterIQ enforces what was recorded and never asserts what the law is.
- The audit trail (Comms → Activity) is append-only; nothing is edited or
  deleted. That is a selling point — show it.

## 5. Access + secrets

- Secrets live ONLY in Railway variables. Adding a credential = paste in
  Railway, redeploy happens automatically, `/api/.../status` endpoints and
  `/api/tanda/plugin/health` confirm what lit up. Nothing sensitive ever
  enters the repo.
- Current credential states are visible without guesswork:
  SMS `/api/sms/status` · Xero `/api/xero/status/{venue}` · Tanda
  marketplace `/api/tanda/plugin/health` (`marketplace_signature`).
- Railway variables that change how the pilot behaves:
  - `FORWARDED_ALLOW_IPS=*` — without it every user shares the proxy's IP,
    so rate limits and the failed-login lockout apply across the whole
    platform at once (a pub on one Wi-Fi gets locked out together).
  - `SMTP_HOST` / `SMTP_USER` / `SMTP_PASS` / `SMTP_FROM` (a domain you own,
    with SPF/DKIM) — until these are set, "Forgot password" says so plainly
    and managers issue reset links from the Staff page.
  - `JWT_SECRET` — never rotate it mid-pilot: join codes are derived from it,
    so every printed or texted code would stop working and everyone is
    signed out.
  - `LLM_PROVIDER` / model — the AI copilot sends the venue's data (staff
    names, shifts, costs) to this provider. Name it, and where it processes
    data, in the privacy statement.

## 6. Before each new venue (preflight)

1. Full suite green locally; `/ready` 200 on prod.
2. Restore drill within the last 30 days (see table above).
3. Walk the first-venue journey on a throwaway prod tenant: register →
   create venue → import staff CSV → join code → import ingredients →
   generate + publish first roster. Fix anything that snags BEFORE the
   venue sees it.
4. Uptime monitor green for 7 straight days.
5. Pilot agreement + privacy statement signed (venue stores staff PII,
   including work-rights records, in the system).
6. Run the venue's REAL exports (Deputy people + schedule, filtered to the
   venue's location) through the importers on a throwaway tenant first, and
   read the import report: defaulted rates, salaried rows, skipped rows.
7. On the venue's actual devices: set up the kiosk iPad as the time clock
   and leave it over an hour, then punch; add /my to the home screen on one
   iPhone and one Android and open it.
8. Publish a throwaway roster with a 23:30 close while watching `/health`.

## 7. The Wembley go-live, in order

1. Ben signs up at `/register` ("Setting up a venue") and runs the wizard:
   venue, staff import, **Areas & minimums** (per weekday, duty manager
   marked "needs the skill") and trading hours (Fri/Sat past midnight).
2. Staff page → check the import report; fix rates, areas and mobiles with
   **Edit**. Managers paid a salary come in as salaried; confirm the weekly
   figures with the payroll advisor.
3. Roster → import the current Deputy week (it shows on staff phones at
   once), or generate next week and publish.
4. Settings → Time clock → set up the bar iPad as the device; Staff →
   **Generate PINs** and hand out the printed sheet.
5. Staff → **Join codes** sheet; staff sign up at `/register?join=1` with the
   email on their record and their code.
6. Apothecary: venue selector → **Add another venue**, same steps. Shared
   managers get both venues from the Access dialog by a manager who runs
   both; shared staff enter the second venue's code in /my.
7. Payroll stays parallel: compare RosterIQ's prep with the real pay run
   each fortnight and log differences for the advisor.

