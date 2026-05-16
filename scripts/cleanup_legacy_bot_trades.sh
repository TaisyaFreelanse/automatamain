#!/usr/bin/env bash
# Remove bot_trades rows corrupted by the pre-fix bug:
#   invested_sol was written as token float (huge) while realized_pnl_pct
#   collapsed to -100% when total_returned was 0.
#
# Safe pattern: realized_pnl_pct <= -99.5 AND invested_sol is absurdly large
# relative to entry_mcap_sol (SOL), OR invested_sol alone is impossible for
# this bot (single position > 500 SOL).

set -euo pipefail

# Prefer DATABASE_URL from loggaper .env (same as the running service).
for envfile in /home/automata/.env /root/.env; do
  if [[ -f "$envfile" ]] && grep -Eq '^[[:space:]]*DATABASE_URL[[:space:]]*=' "$envfile"; then
    DATABASE_URL="$(
      grep -E '^[[:space:]]*DATABASE_URL[[:space:]]*=' "$envfile" \
        | head -1 \
        | sed -E 's/^[[:space:]]*DATABASE_URL[[:space:]]*=[[:space:]]*//; s/^"//; s/"$//; s/^'"'"'//; s/'"'"'$//'
    )"
    export DATABASE_URL
    break
  fi
done

if [[ -n "${DATABASE_URL:-}" ]]; then
  PSQL=(psql "$DATABASE_URL" -v ON_ERROR_STOP=1)
else
  export PGPASSWORD="${PGPASSWORD:-postgres}"
  export PGHOST="${PGHOST:-127.0.0.1}"
  export PGPORT="${PGPORT:-5432}"
  DBNAME="${PGDATABASE:-postgres}"
  DBUSER="${PGUSER:-postgres}"
  PSQL=(psql -U "$DBUSER" -d "$DBNAME" -v ON_ERROR_STOP=1)
fi

PREVIEW_SQL="
SELECT id, mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, closed_at
FROM bot_trades
WHERE
  (realized_pnl_pct <= -99.5 AND invested_sol > GREATEST(5.0, COALESCE(NULLIF(entry_mcap_sol, 0), 1) * 3.0))
  OR invested_sol > 500.0
ORDER BY closed_at DESC;
"

COUNT_SQL="
SELECT COUNT(*)::bigint AS n
FROM bot_trades
WHERE
  (realized_pnl_pct <= -99.5 AND invested_sol > GREATEST(5.0, COALESCE(NULLIF(entry_mcap_sol, 0), 1) * 3.0))
  OR invested_sol > 500.0;
"

DELETE_SQL="
DELETE FROM bot_trades
WHERE
  (realized_pnl_pct <= -99.5 AND invested_sol > GREATEST(5.0, COALESCE(NULLIF(entry_mcap_sol, 0), 1) * 3.0))
  OR invested_sol > 500.0;
"

echo "=== preview (rows to delete) ==="
"${PSQL[@]}" -c "$PREVIEW_SQL"

echo "=== count ==="
"${PSQL[@]}" -t -A -c "$COUNT_SQL"

echo "=== deleting ==="
"${PSQL[@]}" -c "$DELETE_SQL"

echo "=== remaining count ==="
"${PSQL[@]}" -t -A -c "SELECT COUNT(*) FROM bot_trades;"

echo "Done."
