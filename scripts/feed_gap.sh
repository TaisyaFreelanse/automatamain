#!/usr/bin/env bash
# Show pump-feed connection events and per-window created counts (read-only).
# Usage: feed_gap.sh <since>   e.g. feed_gap.sh '2026-05-29 23:12:00'
set -u
SINCE="${1:-2026-05-29 23:12:00}"
echo "=== [pump] connection events since $SINCE ==="
journalctl -u loggaper --since "$SINCE" --no-pager \
  | grep -E '\[pump\] (connected|stream|reconnect|disconnect|error)'

echo
echo "=== created count per 3-min window ==="
start_epoch=$(date -d "$SINCE" +%s)
for i in $(seq 0 0 0); do :; done
for i in $(seq 0 14); do
  a=$(( start_epoch + i*180 ))
  b=$(( a + 180 ))
  ca=$(date -d "@$a" '+%Y-%m-%d %H:%M:%S')
  cb=$(date -d "@$b" '+%Y-%m-%d %H:%M:%S')
  c=$(journalctl -u loggaper --since "$ca" --until "$cb" --no-pager | grep -c 'created ')
  echo "$(date -d "@$a" '+%H:%M')-$(date -d "@$b" '+%H:%M')  created=$c"
done
