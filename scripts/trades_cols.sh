#!/usr/bin/env bash
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

echo "=== trades column types ==="
$PSQL -c "SELECT column_name, data_type FROM information_schema.columns
  WHERE table_name='trades'
    AND column_name IN ('market_cap','size','is_buy','pnl','trader_address','slot_time','currency','role','coin_address')
  ORDER BY column_name;"

echo
echo "=== sizes ==="
$PSQL -c "SELECT pg_size_pretty(pg_relation_size('idx_trades_coin_sol_regular')) AS idx_size,
  pg_size_pretty(pg_relation_size('trades')) AS trades_size,
  (SELECT reltuples::bigint FROM pg_class WHERE relname='trades') AS est_rows;"

echo
echo "=== sol/regular row count ==="
$PSQL -c "SELECT count(*) AS sol_regular_rows FROM trades WHERE currency='sol' AND role='regular';"
