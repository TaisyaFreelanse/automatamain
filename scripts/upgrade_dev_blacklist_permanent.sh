#!/usr/bin/env bash
# Apply 0014: SL CRASH dev_blacklist rows → permanent (expires_at=0).
set -euo pipefail
DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' /home/automata/.env | tr -d '"' | tr -d "'" | head -1)"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== before ==="
psql "$DBURL" -X -P pager=off -c "
SELECT
  count(*) FILTER (WHERE expires_at = 0) AS permanent,
  count(*) FILTER (WHERE expires_at > 0) AS timed,
  count(*) AS total
FROM dev_blacklist;"

psql "$DBURL" -X -v ON_ERROR_STOP=1 -f "$DIR/migrations/0014_dev_blacklist_permanent.sql"

echo "=== after ==="
psql "$DBURL" -X -P pager=off -c "
SELECT dev_wallet, reason, mint, expires_at,
       to_timestamp(created_at) AT TIME ZONE 'Europe/Moscow' AS created_msk
FROM dev_blacklist
ORDER BY created_at DESC
LIMIT 15;"
