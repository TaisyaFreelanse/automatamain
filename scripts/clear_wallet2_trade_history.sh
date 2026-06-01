#!/usr/bin/env bash
# Delete bot_trades HISTORY rows for wallet_2 only (wallet_1 untouched).
set -euo pipefail
WALLET="${1:-wallet_2}"
DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' /home/automata/.env | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }

echo "=== bot_trades before ==="
psql "$DBURL" -X -P pager=off -c "SELECT wallet_id, count(*) FROM bot_trades GROUP BY wallet_id ORDER BY wallet_id;"

N="$(psql "$DBURL" -X -t -A -c "SELECT count(*) FROM bot_trades WHERE wallet_id = '$WALLET';")"
echo "Deleting $N rows for wallet_id=$WALLET ..."
psql "$DBURL" -X -P pager=off -v ON_ERROR_STOP=1 -c "DELETE FROM bot_trades WHERE wallet_id = '$WALLET';"

echo "=== bot_trades after ==="
psql "$DBURL" -X -P pager=off -c "SELECT wallet_id, count(*) FROM bot_trades GROUP BY wallet_id ORDER BY wallet_id;"
