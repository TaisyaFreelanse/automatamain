#!/usr/bin/env bash
# Delete all rows from bot_trades (dashboard HISTORY + dev_ranker past trades source).
# Uses DATABASE_URL from /home/automata/.env or /root/.env (same pattern as cleanup_legacy_bot_trades.sh).
set -euo pipefail

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

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL not found in .env" >&2
  exit 1
fi

PSQL=(psql "$DATABASE_URL" -v ON_ERROR_STOP=1)

echo "=== count before ==="
"${PSQL[@]}" -t -A -c "SELECT COUNT(*) FROM bot_trades;"

echo "=== deleting all bot_trades ==="
"${PSQL[@]}" -c "DELETE FROM bot_trades;"

echo "=== count after ==="
"${PSQL[@]}" -t -A -c "SELECT COUNT(*) FROM bot_trades;"

echo "Done. Refresh the dashboard (or reopen HISTORY) to see empty list."
