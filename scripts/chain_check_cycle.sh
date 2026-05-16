#!/usr/bin/env bash
# One-off chain verification for a mint cycle (reads SOLANA_HTTP from /home/automata/.env).
set -euo pipefail
# Do not `source` the whole .env (PRIVATE_KEY lines can break bash); only
# extract SOLANA_HTTP.
ENV_FILE="${ENV_FILE:-/home/automata/.env}"
RPC="$(grep -E '^[[:space:]]*SOLANA_HTTP[[:space:]]*=' "$ENV_FILE" | head -1 | sed -E 's/^[^=]*=[[:space:]]*"?([^"]*)"?/\1/' | tr -d '\r')"
[ -n "$RPC" ] || { echo "Could not read SOLANA_HTTP from $ENV_FILE"; exit 1; }
WALLET="${1:?usage: $0 <wallet> <mint> <ata> <sell_sig> [buy_sig]}"
MINT="${2:?}"
ATA="${3:?}"
SELL_SIG="${4:?}"
BUY_SIG="${5:-}"

rpc() { curl -sS "$RPC" -H 'Content-Type: application/json' -d "$1"; }

echo "=== /status ==="
curl -sS http://127.0.0.1:1662/status || true
echo

echo "=== getBalance (confirmed) lamports ==="
rpc "$(jq -nc --arg w "$WALLET" '{jsonrpc:"2.0",id:1,method:"getBalance",params:[$w,{commitment:"confirmed"}]}')" \
  | jq -c '{lamports: .result.value, sol: (.result.value / 1e9)}'

echo "=== ATA getAccountInfo (null = closed) ==="
rpc "$(jq -nc --arg a "$ATA" '{jsonrpc:"2.0",id:1,method:"getAccountInfo",params:[$a,{encoding:"jsonParsed",commitment:"confirmed"}]}')" \
  | jq -c '{exists: (.result.value != null), lamports: .result.value.lamports, owner: .result.value.owner}'

echo "=== Token-2022 accounts for wallet with this mint ==="
rpc "$(jq -nc --arg w "$WALLET" '{jsonrpc:"2.0",id:1,method:"getTokenAccountsByOwner",params:[$w,{programId:"TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"},{encoding:"jsonParsed",commitment:"confirmed"}]}')" \
  | jq --arg m "$MINT" '[.result.value[]? | select(.account.data.parsed.info.mint == $m) | {pubkey: .pubkey, ui: .account.data.parsed.info.tokenAmount.uiAmountString}]'

echo "=== SELL transaction (fee, wallet balance delta index 0) ==="
rpc "$(jq -nc --arg s "$SELL_SIG" '{jsonrpc:"2.0",id:1,method:"getTransaction",params:[$s,{encoding:"json",commitment:"confirmed",maxSupportedTransactionVersion:0}]}')" \
  | jq -c 'if .result == null then {error:"no result"} else {err: .result.meta.err, fee_lamports: .result.meta.fee, pre0: .result.meta.preBalances[0], post0: .result.meta.postBalances[0], delta0_lamports: (.result.meta.postBalances[0] - .result.meta.preBalances[0])} end'

echo "=== SELL: log lines mentioning Close / Sell ==="
rpc "$(jq -nc --arg s "$SELL_SIG" '{jsonrpc:"2.0",id:1,method:"getTransaction",params:[$s,{encoding:"json",commitment:"confirmed",maxSupportedTransactionVersion:0}]}')" \
  | jq -r '.result.meta.logMessages[]? | select(test("Close|close|Instruction: Sell"; "i"))' | head -20

if [[ -n "$BUY_SIG" ]]; then
  echo "=== BUY transaction (fee, wallet balance delta index 0) ==="
  rpc "$(jq -nc --arg s "$BUY_SIG" '{jsonrpc:"2.0",id:1,method:"getTransaction",params:[$s,{encoding:"json",commitment:"confirmed",maxSupportedTransactionVersion:0}]}')" \
    | jq -c 'if .result == null then {error:"no result"} else {err: .result.meta.err, fee_lamports: .result.meta.fee, pre0: .result.meta.preBalances[0], post0: .result.meta.postBalances[0], delta0_lamports: (.result.meta.postBalances[0] - .result.meta.preBalances[0])} end'
fi