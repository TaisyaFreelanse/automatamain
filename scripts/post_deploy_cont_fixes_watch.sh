#!/usr/bin/env bash
# Log markers after A+ cont/peak-guard deploy. Usage: post_deploy_cont_fixes_watch.sh [SINCE]
set -u
SINCE="${1:-$(systemctl show loggaper -p ActiveEnterTimestamp --value 2>/dev/null)}"
[[ -z "$SINCE" || "$SINCE" == "n/a" ]] && SINCE="-30 min"

J() { journalctl -u loggaper --since "$SINCE" --no-pager 2>/dev/null; }
c() { J | grep -cE "$1" || true; }

echo "=== CONT/PEAK POST-DEPLOY | since=$SINCE | now=$(date '+%F %T %Z') ==="
printf "%-40s %s\n" "second-look cont_no_uptick defer" "$(c 'second-look: deferring cont_no_uptick')"
printf "%-40s %s\n" "second-look cont_no_uptick pass" "$(c 'second-look passed \(was cont_no_uptick\)')"
printf "%-40s %s\n" "aplus_peak_recheck_mcap_drop skip" "$(c 'aplus_peak_recheck_mcap_drop|recheck rejected.*mcap')"
printf "%-40s %s\n" "peak guard recheck pass" "$(c 'peak guard recheck passed')"
printf "%-40s %s\n" "BUY Opened after second-look" "$(J | grep -c 'second-look passed' || true) (grep Opened manually below)"
printf "%-40s %s\n" "[BUY] Opened total" "$(c '\[BUY\] Opened ')"

echo "--- second-look cont_no_uptick (all) ---"
J | grep -E 'cont_no_uptick|second-look' | grep -E 'deferring|passed|skipped' | tail -20 || echo "(none yet)"

echo "--- aplus peak recheck reject ---"
J | grep -E 'recheck rejected|aplus_peak_recheck_mcap_drop' | tail -15 || echo "(none yet)"

echo "--- Opened + prior second-look on same mint ---"
J | grep -E '\[BUY\] Opened|second-look passed' | tail -15 || echo "(none)"

DEPLOY_EPOCH=$(date -d "$SINCE" +%s 2>/dev/null || echo 0)
DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' /home/automata/.env | tr -d '"' | tr -d "'" | head -1)"
if [[ "$DEPLOY_EPOCH" -gt 0 && -n "$DBURL" ]]; then
  echo "--- missed_runner (continuation skips since deploy) ---"
  psql "$DBURL" -X -P pager=off -c "
WITH skips AS (
  SELECT mint, stage, created_at,
    COALESCE((payload->>'entry_mcap_sol')::float8, 0) AS mcap_skip
  FROM learning_skipped
  WHERE created_at >= $DEPLOY_EPOCH AND stage = 'continuation'
),
x AS (
  SELECT s.*,
    (SELECT max(mcap_sol) FROM coin_mcap_tape t
       WHERE t.coin_address = s.mint AND t.ts_unix >= s.created_at) peak
  FROM skips s
)
SELECT count(*) skipped,
  count(*) FILTER (WHERE peak >= GREATEST(mcap_skip*2,1) OR peak >= 250) missed,
  count(*) FILTER (WHERE peak IS NOT NULL
    AND NOT (peak >= GREATEST(mcap_skip*2,1) OR peak >= 250)) cut_ok
FROM x;"
  echo "--- recent continuation skips ---"
  psql "$DBURL" -X -P pager=off -c "
SELECT to_timestamp(created_at) AT TIME ZONE 'Europe/Moscow' t, reason, left(mint,20) mint
FROM learning_skipped WHERE created_at >= $DEPLOY_EPOCH AND stage='continuation'
ORDER BY created_at DESC LIMIT 10;"
fi

echo "=== DONE ==="
