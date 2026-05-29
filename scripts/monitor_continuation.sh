#!/usr/bin/env bash
set -euo pipefail

SINCE="${1:--3 hours}"

echo "=== window: ${SINCE} | now: $(date -u '+%Y-%m-%d %H:%M:%SZ') ==="

echo
echo "=== latest metrics:bot ==="
journalctl -u loggaper --since "${SINCE}" --no-pager | grep 'metrics:bot' | tail -1

echo
echo "=== late-stage BUY-skip reasons ==="
journalctl -u loggaper --since "${SINCE}" --no-pager \
  | grep -oE '\[BUY\] \S+ skipped \([a-z_]+\)' \
  | grep -oE 'skipped \([a-z_]+\)' | sort | uniq -c | sort -rn || true

echo
echo "=== continuation skip detail ==="
journalctl -u loggaper --since "${SINCE}" --no-pager \
  | grep -E '\[BUY\] .* skipped \(continuation\)' | tail -20 || true

echo
echo "=== parabolic skip detail ==="
journalctl -u loggaper --since "${SINCE}" --no-pager \
  | grep -E '\[BUY\] .* skipped \(parabolic_peak_entry\)' | tail -20 || true

echo
echo "=== actual BUY / position-open events ==="
journalctl -u loggaper --since "${SINCE}" --no-pager \
  | grep -iE '\[BUY GATE\]|position.?initiated|opened position|SELL-JUPITER|BUY-JUPITER|sent buy|buy tx' | tail -20 || true

# Missed-runner audit (post-skip peak vs skip mcap). Same lookback as SINCE when
# expressed in hours (e.g. '-3 hours'); falls back to 6h otherwise.
if [ -x /root/post_skip_audit.sh ] || [ -f /root/post_skip_audit.sh ]; then
  H="$(echo "$SINCE" | grep -oE '[0-9]+' | head -1)"
  [ -z "$H" ] && H=6
  echo
  bash /root/post_skip_audit.sh "$H" 2>/dev/null || true
fi
