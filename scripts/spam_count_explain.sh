#!/usr/bin/env bash
set -u
DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' /home/automata/.env | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
[ -z "$DBURL" ] && { echo "no DATABASE_URL"; exit 1; }
PSQL="psql $DBURL -X -q -P pager=off"
DEV="bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa"   # the 8656-coin dev
echo "=== capped count plan for prolific dev (cap 100 -> LIMIT 101) ==="
$PSQL -c "EXPLAIN (ANALYZE, BUFFERS, TIMING)
          SELECT COUNT(*) FROM (SELECT 1 FROM coins WHERE developer = '$DEV' LIMIT 101) t;"
echo "=== actual capped value ==="
$PSQL -t -A -c "SELECT COUNT(*) FROM (SELECT 1 FROM coins WHERE developer = '$DEV' LIMIT 101) t;"
