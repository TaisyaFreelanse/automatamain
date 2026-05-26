#!/usr/bin/env bash
# Run on prod: compare Jupiter vs bonding mcap for a mint (validates exit-mcap patch).
set -euo pipefail
MINT="${1:-9U85nJVNNDeibnqj2byqwEQnkNiajTiJBJ1hqE6Kpump}"
WSOL="So11111111111111111111111111111111111111112"
POOL_FROZEN="${2:-410.88}"

pid="$(pgrep -f '/home/automata/loggaper' | head -1)"
if [[ -z "$pid" ]]; then
  echo "loggaper not running"
  exit 1
fi
# shellcheck disable=SC2155
export JUPITER_API_KEY="$(tr '\0' '\n' < "/proc/$pid/environ" | sed -n 's/^JUPITER_API_KEY=//p')"
export SOLANA_HTTP="$(tr '\0' '\n' < "/proc/$pid/environ" | sed -n 's/^SOLANA_HTTP=//p')"

echo "mint=$MINT"
body="$(curl -sf -H "x-api-key: $JUPITER_API_KEY" \
  "https://api.jup.ag/price/v3?ids=${MINT},${WSOL}")"
jup="$(python3 - "$body" "$MINT" "$WSOL" <<'PY'
import json, sys
b = json.loads(sys.argv[1])
m, w = sys.argv[2], sys.argv[3]
tu = float(b[m]["usdPrice"])
su = float(b[w]["usdPrice"])
print((tu / su) * 1_000_000_000)
PY
)"
echo "jupiter_implied_mcap_sol=$jup"

if [[ -n "${SOLANA_HTTP:-}" ]]; then
  python3 /tmp/verify_graduated_exit_mcap.py "$MINT" 2>/dev/null || true
fi

echo "historical_frozen_pool_mcap=$POOL_FROZEN"
echo "patch_force_after_ceiling: use_jupiter=true effective=$jup"
if python3 - "$jup" "$POOL_FROZEN" <<'PY'
import sys
j, p = float(sys.argv[1]), float(sys.argv[2])
if j < p * 0.5:
    print(f"OK: Jupiter ({j:.1f}) << frozen pool ({p:.1f}) — patch would update dashboard/exit")
    sys.exit(0)
print(f"NOTE: Jupiter ({j:.1f}) vs frozen ({p:.1f}) — ratio {j/p:.2f}")
PY
then
  :
fi
