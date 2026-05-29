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
psql "$DBURL" -X -P pager=off -c "DROP INDEX CONCURRENTLY IF EXISTS idx_trades_coin_sol_regular;"
psql "$DBURL" -X -P pager=off -c "SELECT indexname FROM pg_indexes WHERE tablename='trades' AND indexname LIKE 'idx_trades_coin_sol_regular%';"
