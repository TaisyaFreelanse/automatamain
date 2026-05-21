#!/usr/bin/env bash
set -euo pipefail
MINT="${1:-5hu1Vv7D2yXcQ8bhQPh7ssrZjnKix82nDZ7KYe4Cpump}"
SHORT="${MINT:0:6}"

echo "=== bot_trades ==="
sudo -u postgres psql -d automata <<SQL
SELECT mint,
       invested_sol,
       entry_mcap_sol,
       exit_mcap_sol,
       realized_pnl_pct,
       close_reason,
       to_timestamp(entry_at) AT TIME ZONE 'UTC' AS entry_utc,
       to_timestamp(closed_at) AT TIME ZONE 'UTC' AS closed_utc
FROM bot_trades
WHERE mint = '${MINT}';
SQL

echo ""
echo "=== SOL PnL from DB fields ==="
sudo -u postgres psql -d automata -t -A <<SQL
SELECT
  invested_sol,
  (exit_mcap_sol / NULLIF(entry_mcap_sol, 0) - 1) * 100 AS mcap_pct_from_db
FROM bot_trades WHERE mint = '${MINT}';
SQL

echo ""
echo "=== journal (full) trade lines ==="
journalctl -u loggaper --no-pager -o cat 2>/dev/null | grep "${MINT}" | grep -E 'BUY|SELL|BROKER|Opened|GATE|SCORE' || true

echo ""
echo "=== journal short grep count ==="
journalctl -u loggaper --no-pager -o cat 2>/dev/null | grep -c "${SHORT}" || true
