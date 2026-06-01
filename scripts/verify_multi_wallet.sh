#!/usr/bin/env bash
# Smoke-check multi-wallet copy-trade HTTP API (run against a live loggaper instance).
set -euo pipefail

HTTP="${HTTP_URL:-http://127.0.0.1:1662}"

echo "=== GET /wallets ==="
curl -sf "${HTTP}/wallets" | head -c 2000
echo ""

echo "=== GET /status (wallets + total balance) ==="
curl -sf "${HTTP}/status" | head -c 2000
echo ""

echo "=== GET /positions (expect wallet_id per row) ==="
curl -sf "${HTTP}/positions" | head -c 2000
echo ""

echo "=== GET /bot-trades (wallet_id column) ==="
curl -sf "${HTTP}/bot-trades" | head -c 1500
echo ""

echo "=== GET /pnl ==="
curl -sf "${HTTP}/pnl" | head -c 800
echo ""

W1=$(curl -sf "${HTTP}/wallets" | python3 -c "import sys,json; w=json.load(sys.stdin); print(next((x['pubkey'] for x in w if x['id']=='wallet_1'), ''))" 2>/dev/null || true)
W2=$(curl -sf "${HTTP}/wallets" | python3 -c "import sys,json; w=json.load(sys.stdin); print(next((x['pubkey'] for x in w if x['id']=='wallet_2'), ''))" 2>/dev/null || true)
if [[ -n "${W1}" && -n "${W2}" && "${W1}" != "${W2}" ]]; then
  echo "OK: wallet_1 and wallet_2 have distinct pubkeys"
else
  echo "NOTE: wallet_2 may be disabled or same env key — check PRIVATE_KEY_WALLET_2"
fi

echo "Done."
