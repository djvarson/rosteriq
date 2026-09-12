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
