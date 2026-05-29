#!/usr/bin/env bash
# Trace specific mints through the loggaper logs (read-only).
# Usage: trace_mints.sh <since> <mint> [mint...]
set -u
SINCE="${1:-4 hours ago}"; shift || true
for M in "$@"; do
  echo "========================= $M ========================="
  journalctl -u loggaper --since "$SINCE" --no-pager | grep "$M" | head -60
  echo
done
