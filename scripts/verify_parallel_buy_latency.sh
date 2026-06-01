#!/usr/bin/env bash
# After a copy-trade BUY, verify parallel multi-wallet timing from [LATENCY] logs.
# Usage: verify_parallel_buy_latency.sh [mint_substring]
set -euo pipefail

MINT_FILTER="${1:-}"
UNIT="${LOGGAPER_UNIT:-loggaper}"
MAX_GATE_TO_SENT_MS="${MAX_GATE_TO_SENT_MS:-500}"

echo "=== verify_parallel_buy_latency (unit=$UNIT max_gate_to_sent_ms=$MAX_GATE_TO_SENT_MS) ==="

lines=$(journalctl -u "$UNIT" --since '24 hours ago' -g '\[LATENCY\]' --no-pager 2>/dev/null || true)
if [[ -z "$lines" ]]; then
  echo "[FAIL] no [LATENCY] lines in last 24h"
  exit 1
fi

if [[ -n "$MINT_FILTER" ]]; then
  lines=$(echo "$lines" | grep "$MINT_FILTER" || true)
fi

last=$(echo "$lines" | tail -1)
if [[ -z "$last" ]]; then
  echo "[FAIL] no [LATENCY] matching mint filter: $MINT_FILTER"
  exit 1
fi

echo "$last"

if ! echo "$last" | grep -q 'wallet_1:'; then
  echo "[FAIL] missing wallet_1 timing"
  exit 1
fi
if ! echo "$last" | grep -q 'wallet_2:'; then
  echo "[WARN] missing wallet_2 (single-wallet buy?)"
fi

# gate_to_sent_ms per wallet should be small when buys run in parallel (not +1–3s stagger).
for w in wallet_1 wallet_2; do
  gts=$(echo "$last" | sed -n "s/.*${w}:gate_to_sent_ms=\([0-9]*\).*/\1/p" | head -1)
  if [[ -n "$gts" && "$gts" -le "$MAX_GATE_TO_SENT_MS" ]]; then
    echo "[PASS] ${w} gate_to_sent_ms=${gts}"
  elif [[ -n "$gts" ]]; then
    echo "[FAIL] ${w} gate_to_sent_ms=${gts} > ${MAX_GATE_TO_SENT_MS} (sequential buy suspected)"
    exit 1
  fi
done

echo "[PASS] parallel buy latency OK"
exit 0
