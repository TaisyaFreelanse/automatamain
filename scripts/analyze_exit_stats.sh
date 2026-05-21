#!/usr/bin/env bash
set -euo pipefail

echo "=== bot_trades by close_reason ==="
sudo -u postgres psql -d automata <<'SQL'
SELECT close_reason,
       COUNT(*) AS n,
       ROUND(AVG(realized_pnl_pct)::numeric, 2) AS avg_pnl,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY realized_pnl_pct)::numeric, 2) AS med_pnl
FROM bot_trades
GROUP BY close_reason
ORDER BY n DESC;
SQL

echo ""
echo "=== last 20 closes ==="
sudo -u postgres psql -d automata <<'SQL'
SELECT mint,
       ROUND(entry_mcap_sol::numeric, 1) AS entry_mcap,
       ROUND(realized_pnl_pct::numeric, 2) AS pnl,
       LEFT(close_reason, 72) AS reason,
       to_timestamp(closed_at) AT TIME ZONE 'UTC' AS closed_utc
FROM bot_trades
ORDER BY closed_at DESC
LIMIT 20;
SQL

echo ""
echo "=== journal exits last 7d (sample) ==="
journalctl -u loggaper --no-pager -S "7 days ago" -o cat 2>/dev/null \
  | grep -E 'TP1|TP2|TP3|TP4|TP5|TRAILING|TIME KILL|SL \(floor|MCAP CEILING|FILL MCAP' \
  | tail -40 || true
