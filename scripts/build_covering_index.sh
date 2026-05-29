#!/usr/bin/env bash
# Build a COVERING replacement for idx_trades_coin_sol_regular so the creator-stats
# query becomes index-only (no heap fetch for market_cap/size/is_buy/pnl) and
# token_stats can use the index order (no big sort). CONCURRENTLY = no table lock.
# Read-mostly: only adds an index. Old index dropped in a separate step after verify.
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
PSQL="psql $DBURL -X -q -P pager=off -v ON_ERROR_STOP=1"

echo "=== building covering index (CONCURRENTLY) $(date -u '+%T')Z ==="
$PSQL -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_coin_sol_regular_cov
ON trades (coin_address, trader_address, slot_time DESC, id DESC)
INCLUDE (is_buy, market_cap, size, pnl)
WHERE currency = 'sol' AND role = 'regular';"
echo "=== build done $(date -u '+%T')Z ==="

echo "=== ANALYZE trades ==="
$PSQL -c "ANALYZE trades;"

echo "=== validity + size ==="
$PSQL -c "SELECT c.relname, i.indisvalid,
  pg_size_pretty(pg_relation_size(c.oid)) AS size
  FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
  WHERE c.relname IN ('idx_trades_coin_sol_regular','idx_trades_coin_sol_regular_cov');"
echo "=== DONE ==="
