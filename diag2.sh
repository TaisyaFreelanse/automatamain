#!/bin/bash
set -e

# 1) Find DATABASE_URL
ENV_FILE=""
for f in /home/automata/.env /root/automata-build/.env /root/.env; do
  if [ -f "$f" ]; then ENV_FILE="$f"; break; fi
done
echo "env file: $ENV_FILE"
if [ -n "$ENV_FILE" ]; then
  DB_URL=$(grep -E '^DATABASE_URL=' "$ENV_FILE" | head -1 | sed 's/^DATABASE_URL=//;s/^"//;s/"$//')
fi
echo "DATABASE_URL set: $([ -n "$DB_URL" ] && echo yes || echo no)"

# 2) Try psql via the URL
if [ -z "$DB_URL" ]; then exit 0; fi

echo
echo "=== tables ==="
psql "$DB_URL" -c '\dt' 2>&1 | head -30 || true

echo
echo "=== schema preview: coins, creator_stats, traders, trades ==="
for t in coins creator_stats traders trades; do
  echo "--- $t ---"
  psql "$DB_URL" -c "\d $t" 2>&1 | head -25 || true
done
