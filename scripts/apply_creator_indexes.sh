#!/usr/bin/env bash
# Create the creator/dev-stat index CONCURRENTLY (online, no table lock) and
# re-run EXPLAIN ANALYZE to confirm the planner uses it. Reads DATABASE_URL
# from loggaper .env. Safe to re-run (IF NOT EXISTS).
set -u

WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
DBURL=""
for f in "$WD/.env" /home/automata/.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
           | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }
PSQL="psql $DBURL -X -q -P pager=off"

echo "=== creating idx_trades_coin_sol_regular CONCURRENTLY (this can take minutes on 12GB) ==="
$PSQL -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_coin_sol_regular
          ON trades (coin_address, trader_address, slot_time DESC, id DESC)
          WHERE currency = 'sol' AND role = 'regular';"
echo "create rc=$?"

echo "=== ANALYZE trades (refresh planner stats) ==="
$PSQL -c "ANALYZE trades;"

echo "=== index size ==="
$PSQL -c "SELECT indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
          FROM pg_indexes WHERE tablename='trades' AND indexname='idx_trades_coin_sol_regular';"

echo "=== verify it is valid (not left INVALID by a failed concurrent build) ==="
$PSQL -c "SELECT c.relname, i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
          WHERE c.relname='idx_trades_coin_sol_regular';"
echo "=== DONE creating ==="
