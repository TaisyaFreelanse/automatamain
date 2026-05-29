#!/usr/bin/env bash
# FILL-MCAP-ABORT AUDIT
# For buys aborted by fill_mcap_abort (we bought, the on-chain fill landed far
# above score-time mcap, so we emergency-sold), measure what the token did AFTER
# the abort via coin_mcap_tape. Tells us how often the abort dodged a real runner
# vs correctly avoided a pump-and-dump — needed before raising fill_mcap_abort_max_ratio.
#
# "runner" = peak after abort >= MULT x the fill mcap (the price we WOULD have
# entered at if we hadn't aborted), or >= GRAD high-watermark.
# "died"   = last mcap fell back to <= DEAD x the fill mcap.
#
# Usage: abort_audit.sh [HOURS] [MULT] [GRAD] [DEAD]
#   HOURS lookback (default 24)   MULT runner mult vs fill (default 2.0)
#   GRAD  graduation SOL (250)    DEAD round-trip ratio vs fill (default 0.6)
# Read-only.
set -u
HOURS="${1:-24}"; MULT="${2:-2.0}"; GRAD="${3:-250}"; DEAD="${4:-0.6}"

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
WINDOW_SECS=$(( HOURS * 3600 ))

read -r -d '' CTE <<SQL
WITH aborts AS (
  SELECT mint, closed_at,
    entry_mcap_sol AS fill_mcap,
    -- score-time mcap parsed from "(...: 42.5->95.1 SOL)" close_reason
    NULLIF((regexp_match(close_reason, '([0-9.]+)[^0-9.]+([0-9.]+) SOL'))[1], '')::float8 AS score_mcap,
    realized_pnl_pct
  FROM bot_trades
  WHERE close_reason LIKE 'FILL MCAP SPIKE ABORT%'
    AND closed_at >= EXTRACT(EPOCH FROM now())::bigint - ${WINDOW_SECS}
),
peaks AS (
  SELECT a.*,
    (SELECT max(t.mcap_sol) FROM coin_mcap_tape t
       WHERE t.coin_address = a.mint AND t.ts_unix >= a.closed_at) AS peak_after,
    (SELECT (array_agg(t.mcap_sol ORDER BY t.ts_unix DESC))[1] FROM coin_mcap_tape t
       WHERE t.coin_address = a.mint AND t.ts_unix >= a.closed_at) AS last_mcap
  FROM aborts a
),
classed AS (
  SELECT *,
    peak_after / NULLIF(fill_mcap,0) AS mult_vs_fill,
    CASE
      WHEN peak_after IS NULL THEN 'no_tape'
      WHEN peak_after >= GREATEST(fill_mcap * ${MULT}, 1) OR peak_after >= ${GRAD} THEN 'RUNNER'
      WHEN last_mcap <= fill_mcap * ${DEAD} THEN 'died'
      ELSE 'faded'
    END AS verdict
  FROM peaks
)
SQL

echo "=== FILL-MCAP-ABORT AUDIT | window=${HOURS}h runner>=${MULT}x fill | $(date -u '+%F %T')Z ==="

echo
echo "--- summary ---"
$PSQL -c "${CTE}
SELECT count(*) aborts,
  count(*) FILTER (WHERE verdict='RUNNER') runners,
  count(*) FILTER (WHERE verdict='died')   died,
  count(*) FILTER (WHERE verdict='faded')  faded,
  count(*) FILTER (WHERE verdict='no_tape') no_tape,
  round(100.0*count(*) FILTER (WHERE verdict='RUNNER')
        / NULLIF(count(*) FILTER (WHERE verdict<>'no_tape'),0),1) runner_pct,
  round(avg(realized_pnl_pct)::numeric,2) avg_abort_pnl_pct
FROM classed;"

echo
echo "--- detail (recent first) ---"
$PSQL -c "${CTE}
SELECT to_timestamp(closed_at) AT TIME ZONE 'UTC' abort_utc, mint,
  round(score_mcap::numeric,1) score, round(fill_mcap::numeric,1) fill,
  round(peak_after::numeric,1) peak, round(mult_vs_fill::numeric,2) x_fill,
  round(last_mcap::numeric,1) last, round(realized_pnl_pct::numeric,1) abort_pnl, verdict
FROM classed ORDER BY closed_at DESC LIMIT 40;"

echo
echo "=== DONE ==="
