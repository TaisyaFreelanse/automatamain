#!/usr/bin/env bash
# Quick read-only JSON-RPC latency compare: Gatekeeper beta HTTP vs mainnet.helius HTTP.
# Usage: bench_gatekeeper_rpc.sh [/path/to/.env] [pubkey_optional]
set -euo pipefail
ENV="${1:-/home/automata/.env}"
PUB_DEFAULT="2X9SYhDEXmgiXxQRcTa4dvi6TzshG1zgXhaTyvvYT8Ei"
PUB="${2:-$PUB_DEFAULT}"

# tolerate "KEY = value" spacing in .env
line_http="$(grep -E '^[[:space:]]*SOLANA_HTTP[[:space:]]*=' "$ENV" | head -1 | tr -d '\r')"
BETA="${line_http#*=}"
BETA="${BETA#"${BETA%%[![:space:]]*}"}"
BETA="${BETA%${BETA##*[![:space:]]}}"
BETA="${BETA#\"}"
BETA="${BETA%\"}"

KEY="$(printf '%s' "$BETA" | sed -n 's/.*api-key=\([^&"[:space:]]*\).*/\1/p')"
MAIN="https://mainnet.helius-rpc.com/?api-key=${KEY}"

req_time() {
  local url="$1" body="$2"
  curl -sS -X POST "$url" -H "Content-Type: application/json" -d "$body" -o /tmp/rpc.body.$$ -w "%{http_code} %{time_total}\n"
  if grep -q '"error"' /tmp/rpc.body.$$ 2>/dev/null; then
    echo "  RPC error body:" >&2
    head -c 400 /tmp/rpc.body.$$ >&2
    echo >&2
  fi
  rm -f /tmp/rpc.body.$$
}

stats() {
  sort -n | awk '
    { v[NR]=$1+0; s+=$1+0 }
    END {
      n=NR
      if (n<1) { print "no samples"; exit 1 }
      p50=v[int((n+1)/2)]
      ix=int(n*0.95); if (ix<1) ix=1; if (ix>n) ix=n
      p95=v[ix]
      printf "n=%d min=%.3fs p50=%.3fs avg=%.3fs p95=%.3fs max=%.3fs\n", n, v[1], p50, s/n, p95, v[n]
    }'
}

run_bench() {
  local label="$1" url="$2" method="$3" body="$4" rounds="$5"
  echo "======== $label $method (${rounds}x) ========"
  : > /tmp/rpc.times.$$
  local i
  for i in $(seq 1 "$rounds"); do
    req_time "$url" "$body" | awk '{print $2}' >> /tmp/rpc.times.$$
  done
  stats < /tmp/rpc.times.$$
  rm -f /tmp/rpc.times.$$
  echo
}

get_slot() {
  curl -sS -X POST "$1" -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' | sed -n 's/.*"result":[[:space:]]*\([0-9]*\).*/\1/p'
}

echo "Pubkey: $PUB"
echo "BETA: ${BETA:0:56}..."
echo "MAIN: ${MAIN:0:56}..."
echo

B_BAL='{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["'"$PUB"'"]}'
B_ACC='{"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["'"$PUB"'",{"encoding":"base64"}]}'
B_HASH='{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"}'

for pair in "BETA|$BETA" "MAIN|$MAIN"; do
  name="${pair%%|*}"
  url="${pair#*|}"
  run_bench "$name" "$url" "getBalance" "$B_BAL" 20
done
for pair in "BETA|$BETA" "MAIN|$MAIN"; do
  name="${pair%%|*}"
  url="${pair#*|}"
  run_bench "$name" "$url" "getAccountInfo" "$B_ACC" 15
done
for pair in "BETA|$BETA" "MAIN|$MAIN"; do
  name="${pair%%|*}"
  url="${pair#*|}"
  run_bench "$name" "$url" "getLatestBlockhash" "$B_HASH" 15
done

echo "======== Slot progression (same endpoint, ~400ms apart) ========"
for pair in "BETA|$BETA" "MAIN|$MAIN"; do
  name="${pair%%|*}"
  url="${pair#*|}"
  s1="$(get_slot "$url")"
  sleep 0.4
  s2="$(get_slot "$url")"
  echo "$name slot $s1 -> $s2 (delta $((s2 - s1)))"
done

echo "======== Cross-RPC slot skew (same moment) ========"
sb="$(get_slot "$BETA")"
sm="$(get_slot "$MAIN")"
echo "BETA=$sb MAIN=$sm skew=$((sb - sm)) slots"

echo "======== Mint visibility probe (random recent pump mint pattern N/A) ========"
echo "(Skipped: need live mint; check bot logs for getAccount nulls after BUY.)"
