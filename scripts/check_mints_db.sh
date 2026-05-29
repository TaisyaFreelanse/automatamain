#!/usr/bin/env bash
# Check whether specific mints were ingested at all (coins/trades/tape). Read-only.
# Usage: check_mints_db.sh <mint> [mint...]
set -u
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
DBURL=""
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
           | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }
PSQL="psql $DBURL -X -q -P pager=off"

for M in "$@"; do
  echo "===== $M ====="
  $PSQL -c "SELECT
    (SELECT count(*) FROM coins WHERE coin_address='$M') AS in_coins,
    (SELECT developer FROM coins WHERE coin_address='$M') AS dev,
    (SELECT count(*) FROM trades WHERE coin_address='$M') AS trades_rows,
    (SELECT count(*) FROM coin_mcap_tape WHERE coin_address='$M') AS tape_rows,
    (SELECT round(max(mcap_sol)::numeric,1) FROM coin_mcap_tape WHERE coin_address='$M') AS peak_mcap_sol,
    (SELECT count(*) FROM learning_skipped WHERE mint='$M') AS skip_rows;"
  echo "  -- dev's coin count (capped 120) --"
  $PSQL -t -A -c "SELECT count(*) FROM (SELECT 1 FROM coins WHERE developer=(SELECT developer FROM coins WHERE coin_address='$M') LIMIT 120) t;"
done
