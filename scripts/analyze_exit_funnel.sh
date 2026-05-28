#!/usr/bin/env bash
# Read-only prod analysis: exit-reason distribution + early MOMENTUM DECAY exits.
set -uo pipefail
SINCE="${1:-48 hours ago}"
LOG=/tmp/exit_funnel.log
journalctl -u loggaper --since "$SINCE" --no-pager -o cat >"$LOG" 2>/dev/null || true
echo "window: since $SINCE"
wc -l "$LOG"

echo
echo "=== EXIT REASON DISTRIBUTION ==="
grep -oE 'MOMENTUM DECAY( \[[a-z]+\])?|TIME KILL|TRAILING EXIT|SL CRASH|SL trigger|MCAP CEILING' "$LOG" \
  | sort | uniq -c | sort -rn || true

echo
echo "=== FUNNEL ==="
printf 'BUY GATE      : %s\n' "$(grep -cF '[BUY GATE]' "$LOG")"
printf 'anti_rug skip : %s\n' "$(grep -cF 'skipped (anti_rug)' "$LOG")"
printf 'score lines   : %s\n' "$(grep -cF '[SCORE]' "$LOG")"

echo
echo "=== MOMENTUM DECAY exits with held<=12s (premature single-tick) ==="
# [EXIT] lines do not always carry held; approximate by sampling decay reasons
grep -F 'MOMENTUM DECAY' "$LOG" | head -20 || true

echo
echo "=== last 20 EXIT lines ==="
grep -F '[EXIT]' "$LOG" | tail -20 || true
