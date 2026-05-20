#!/usr/bin/env bash
# Stats for FILL MCAP SPIKE ABORT — run on server.
set -euo pipefail

HOURS="${1:-12}"
SINCE="$(date -u -d "${HOURS} hours ago" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -u -v-${HOURS}H '+%Y-%m-%d %H:%M:%S')"

echo "=== journal (since ~${HOURS}h, BUY / ABORT) ==="
journalctl -u loggaper --no-pager -S "$SINCE" 2>/dev/null | grep -E '\[BUY ABORT\]|FILL MCAP SPIKE' || echo "(none)"
echo "BUY Opened count:"
journalctl -u loggaper --no-pager -S "$SINCE" 2>/dev/null | grep -c '\[BUY\] Opened' || echo 0
echo "BUY ABORT count:"
journalctl -u loggaper --no-pager -S "$SINCE" 2>/dev/null | grep -c '\[BUY ABORT\]' || echo 0

echo ""
echo "=== Postgres learning_skipped (fill_mcap_spike) ==="
sudo -u postgres psql -d automata -t -A <<'SQL'
SELECT COUNT(*) AS fill_mcap_skipped FROM learning_skipped WHERE reason = 'fill_mcap_spike';
SQL

sudo -u postgres psql -d automata <<'SQL'
SELECT mint,
       (payload->>'ratio')::float8 AS ratio,
       (payload->>'score_mcap_sol')::float8 AS score_mcap,
       (payload->>'fill_mcap_sol')::float8 AS fill_mcap,
       to_timestamp(created_at) AS ts_utc
FROM learning_skipped
WHERE reason = 'fill_mcap_spike'
ORDER BY created_at DESC
LIMIT 25;
SQL

echo ""
echo "=== Postgres bot_trades (FILL MCAP close) ==="
sudo -u postgres psql -d automata <<'SQL'
SELECT close_reason, COUNT(*) AS n
FROM bot_trades
WHERE close_reason ILIKE '%FILL MCAP%'
GROUP BY close_reason
ORDER BY n DESC;

SELECT mint, realized_pnl_pct, close_reason,
       to_timestamp(closed_at) AS ts_utc
FROM bot_trades
WHERE close_reason ILIKE '%FILL MCAP%'
ORDER BY closed_at DESC
LIMIT 25;
SQL

echo ""
echo "=== bot_trades last ${HOURS}h (all closes) ==="
CUTOFF=$(date -u -d "${HOURS} hours ago" +%s 2>/dev/null || echo 0)
sudo -u postgres psql -d automata -t -A -v cutoff="$CUTOFF" <<'SQL'
SELECT COUNT(*) FROM bot_trades WHERE closed_at >= :cutoff;
SQL
