#!/usr/bin/env bash
# MISSED-RUNNER AUDIT
# For tokens skipped by the continuation / parabolic_peak_entry gates, look at
# coin_mcap_tape and measure the realized peak AFTER the skip. Flag as
# MISSED_RUNNER when the token did >= MULT x its skip-time mcap, or reached the
# graduation-ish high-watermark (GRAD SOL). This shows how many *good* runners
# the gates cut, not just how much trash they filter -- needed to calibrate
# continuation / parabolic after the confirm_slices=4 fix.
#
# Usage: post_skip_audit.sh [HOURS] [MULT] [GRAD]
#   HOURS  lookback window in hours          (default 6)
#   MULT   peak/skip multiple => MISSED      (default 2.0)
#   GRAD   peak SOL high-watermark => MISSED (default 250)
# Read-only.
set -u

HOURS="${1:-6}"
MULT="${2:-2.0}"
GRAD="${3:-250}"

PID="$(systemctl show loggaper -p MainPID --value 2>/dev/null)"
DBURL=""
if [ -n "$PID" ] && [ -r "/proc/$PID/environ" ]; then
  DBURL="$(tr '\0' '\n' < /proc/$PID/environ | sed -n 's/^DATABASE_URL=//p')"
fi
if [ -z "$DBURL" ]; then
  WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
  for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
    [ -f "$f" ] || continue
    DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
             | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
    [ -n "$DBURL" ] && break
  done
fi
if [ -z "$DBURL" ]; then echo "[FAIL] DATABASE_URL not found"; exit 1; fi
PSQL="psql $DBURL -X -q -P pager=off"

WINDOW_SECS=$(( HOURS * 3600 ))

# Shared CTE: skips in window + post-skip peak/last from the mcap tape.
read -r -d '' CTE <<SQL
WITH skips AS (
  SELECT mint, stage, reason, created_at,
         (payload->>'entry_mcap_sol')::float8 AS mcap_skip
  FROM learning_skipped
  WHERE stage IN ('continuation','parabolic_peak_entry')
    AND created_at >= EXTRACT(EPOCH FROM now())::bigint - ${WINDOW_SECS}
),
peaks AS (
  SELECT s.*,
    (SELECT max(t.mcap_sol) FROM coin_mcap_tape t
       WHERE t.coin_address = s.mint AND t.ts_unix >= s.created_at) AS peak_after,
    (SELECT (array_agg(t.mcap_sol ORDER BY t.ts_unix DESC))[1] FROM coin_mcap_tape t
       WHERE t.coin_address = s.mint AND t.ts_unix >= s.created_at) AS last_mcap
  FROM skips s
),
classed AS (
  SELECT *,
    (peak_after / NULLIF(mcap_skip,0)) AS mult,
    CASE
      WHEN peak_after IS NULL THEN 'no_tape'
      WHEN peak_after >= GREATEST(mcap_skip * ${MULT}, 1) THEN 'MISSED_RUNNER'
      WHEN peak_after >= ${GRAD} THEN 'MISSED_RUNNER'
      ELSE 'cut_ok'
    END AS verdict
  FROM peaks
)
SQL

echo "=== MISSED-RUNNER AUDIT | window=${HOURS}h mult>=${MULT}x grad>=${GRAD} SOL | $(date -u '+%F %T')Z ==="

echo
echo "--- summary by gate ---"
$PSQL -c "${CTE}
SELECT stage,
  count(*)                                              AS skipped,
  count(*) FILTER (WHERE verdict='MISSED_RUNNER')       AS missed_runners,
  count(*) FILTER (WHERE verdict='cut_ok')              AS cut_ok,
  count(*) FILTER (WHERE verdict='no_tape')             AS no_tape,
  round(100.0 * count(*) FILTER (WHERE verdict='MISSED_RUNNER')
        / NULLIF(count(*) FILTER (WHERE verdict<>'no_tape'),0), 1) AS missed_pct
FROM classed GROUP BY stage ORDER BY stage;"

echo
echo "--- MISSED_RUNNER detail (worst first) ---"
$PSQL -c "${CTE}
SELECT to_timestamp(created_at) AT TIME ZONE 'UTC' AS skip_utc,
  stage, reason, mint,
  round(mcap_skip::numeric,1)  AS mcap_skip,
  round(peak_after::numeric,1) AS peak_after,
  round(mult::numeric,2)       AS mult,
  round(last_mcap::numeric,1)  AS last_mcap
FROM classed WHERE verdict='MISSED_RUNNER'
ORDER BY mult DESC NULLS LAST LIMIT 30;"

echo
echo "--- all skips (recent first) ---"
$PSQL -c "${CTE}
SELECT to_timestamp(created_at) AT TIME ZONE 'UTC' AS skip_utc,
  stage, mint,
  round(mcap_skip::numeric,1)  AS mcap_skip,
  round(peak_after::numeric,1) AS peak_after,
  round(mult::numeric,2)       AS mult,
  verdict
FROM classed ORDER BY created_at DESC LIMIT 40;"

echo
echo "=== DONE ==="
