#!/usr/bin/env bash
# RosterIQ backup restore drill.
#
# "Until we've done one restore drill, we don't have backups — we have hope."
#
# What it does, READ-ONLY against the source database:
#   1. pg_dump the database named by DATABASE_URL (custom format)
#   2. create a THROWAWAY database on the same server (restore_drill_<ts>)
#   3. pg_restore the dump into it
#   4. compare row counts of the critical tables source-vs-restored
#   5. drop the throwaway database
#
# Usage:
#   DATABASE_URL='postgres://user:pass@host:port/railway' ./scripts/restore_drill.sh
#
# Get DATABASE_URL from Railway -> Postgres service -> Connect -> Postgres
# Connection URL. Needs pg client tools: `brew install libpq` then
# `brew link --force libpq` (or use the full /opt/homebrew/opt/libpq/bin path).
#
# The source database is only ever read (pg_dump + SELECT COUNTs). The only
# writes are CREATE/DROP of the uniquely named throwaway database.
set -euo pipefail

if [ -z "${DATABASE_URL:-}" ]; then
  echo "ERROR: set DATABASE_URL (Railway -> Postgres -> Connect -> Connection URL)" >&2
  exit 2
fi
for tool in pg_dump pg_restore psql; do
  command -v "$tool" >/dev/null || { echo "ERROR: $tool not found — brew install libpq && brew link --force libpq" >&2; exit 2; }
done

TS=$(date +%Y%m%d_%H%M%S)
DRILL_DB="restore_drill_${TS}"
DUMP="/tmp/rosteriq_drill_${TS}.dump"
# Server-level URL (no database path) for CREATE/DROP, and the drill DB URL
BASE_URL=$(echo "$DATABASE_URL" | sed -E 's#/[^/]+(\?.*)?$#/postgres#')
DRILL_URL=$(echo "$DATABASE_URL" | sed -E "s#/[^/]+(\?.*)?\$#/${DRILL_DB}#")

TABLES="venues employees rosters shifts ingredients stocktakes announcements sop_documents timesheets kv_store"

echo "== 1/5 Dumping source database (read-only)…"
pg_dump --format=custom --no-owner --no-privileges --file="$DUMP" "$DATABASE_URL"
echo "   dump: $DUMP ($(du -h "$DUMP" | cut -f1))"

echo "== 2/5 Creating throwaway database ${DRILL_DB}…"
psql "$BASE_URL" -q -c "CREATE DATABASE ${DRILL_DB};"
trap 'echo "== cleanup: dropping ${DRILL_DB}"; psql "$BASE_URL" -q -c "DROP DATABASE IF EXISTS ${DRILL_DB};" || true; rm -f "$DUMP"' EXIT

echo "== 3/5 Restoring into ${DRILL_DB}…"
pg_restore --no-owner --no-privileges --dbname="$DRILL_URL" "$DUMP"

echo "== 4/5 Comparing critical-table row counts…"
FAIL=0
for t in $TABLES; do
  SRC=$(psql "$DATABASE_URL" -tA -c "SELECT COALESCE((SELECT count(*) FROM ${t}), 0);" 2>/dev/null || echo "absent")
  DST=$(psql "$DRILL_URL"    -tA -c "SELECT COALESCE((SELECT count(*) FROM ${t}), 0);" 2>/dev/null || echo "absent")
  if [ "$SRC" = "$DST" ]; then
    printf "   PASS %-18s %s rows\n" "$t" "$SRC"
  else
    printf "   FAIL %-18s source=%s restored=%s\n" "$t" "$SRC" "$DST"; FAIL=1
  fi
done

echo "== 5/5 Result:"
if [ "$FAIL" = "0" ]; then
  echo "   RESTORE DRILL PASSED — a dump taken now restores completely."
  echo "   Log this date in PILOT_OPS.md. Re-run monthly and before every pilot."
else
  echo "   RESTORE DRILL FAILED — do NOT rely on backups until this passes." >&2
  exit 1
fi
