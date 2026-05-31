#!/usr/bin/env bash
# Block deploy/restart while the bot has open positions (live manager state).
# Usage:
#   wait_no_open_positions.sh              # wait until flat (default 30 min)
#   wait_no_open_positions.sh --check-only # exit 1 if any open, 0 if flat
#   WAIT_MAX_SEC=600 wait_no_open_positions.sh
set -euo pipefail

API_URL="${POSITIONS_URL:-http://127.0.0.1:1662/positions}"
POLL_SEC="${WAIT_POLL_SEC:-5}"
MAX_SEC="${WAIT_MAX_SEC:-1800}"
CHECK_ONLY=false

for arg in "$@"; do
  case "$arg" in
    --check-only) CHECK_ONLY=true ;;
    -h|--help)
      echo "Usage: $0 [--check-only]"
      echo "  POSITIONS_URL  (default http://127.0.0.1:1662/positions)"
      echo "  WAIT_MAX_SEC   max wait before abort (default 1800)"
      echo "  WAIT_POLL_SEC  poll interval (default 5)"
      exit 0
      ;;
  esac
done

count_open() {
  local body
  body="$(curl -sf --max-time 5 "$API_URL" 2>/dev/null)" || {
    echo "[preflight] WARN: cannot reach $API_URL (service down?)" >&2
    echo 0
    return
  }
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else 0)' <<<"$body"
}

list_open() {
  curl -sf --max-time 5 "$API_URL" 2>/dev/null \
    | python3 -c "import json,sys
d=json.load(sys.stdin)
for p in (d if isinstance(d,list) else []):
    a=p.get('address','?')
    m=p.get('market_cap',0)
    pn=p.get('pnl',0)
    print('  - %s mcap=%.1f pnl=%.1f%%' % (a,m,pn))" 2>/dev/null || true
}

n="$(count_open)"
if [[ "$n" -eq 0 ]]; then
  echo "[preflight] no open positions — safe to restart"
  exit 0
fi

if $CHECK_ONLY; then
  echo "[preflight] ABORT: $n open position(s):" >&2
  list_open >&2
  exit 1
fi

echo "[preflight] waiting for $n open position(s) to close (max ${MAX_SEC}s, poll ${POLL_SEC}s)..."
list_open
start=$(date +%s)
while true; do
  sleep "$POLL_SEC"
  n="$(count_open)"
  if [[ "$n" -eq 0 ]]; then
    echo "[preflight] all positions closed — safe to restart"
    exit 0
  fi
  now=$(date +%s)
  elapsed=$((now - start))
  if [[ "$elapsed" -ge "$MAX_SEC" ]]; then
    echo "[preflight] TIMEOUT after ${elapsed}s: still $n open position(s):" >&2
    list_open >&2
    exit 1
  fi
  echo "[preflight] still $n open (${elapsed}s / ${MAX_SEC}s)..."
  list_open
done
