#!/usr/bin/env bash
# Prod: fund wallet_2 from wallet_1 via solana CLI (run as root on server).
set -euo pipefail
AMOUNT_SOL="${1:-0.15}"
ENV_FILE="${ENV_FILE:-/home/automata/.env}"
PUB_FILE="${PUB_FILE:-/home/automata/wallet_2_pubkey.txt}"

get_env() {
  local key="$1"
  sed -nE "s/^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=[[:space:]]*//p" "$ENV_FILE" \
    | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1
}

RPC="$(get_env SOLANA_HTTP)"
[[ -z "$RPC" ]] && RPC="$(get_env SOLANA_RPC_URL)"
PK1="$(get_env PRIVATE_KEY)"
PK2="$(get_env PRIVATE_KEY_WALLET_2)"
DEST="$(cat "$PUB_FILE" 2>/dev/null || true)"
[[ -z "$DEST" ]] && { echo "[FAIL] no $PUB_FILE"; exit 1; }
[[ -z "$PK1" || -z "$PK2" ]] && { echo "[FAIL] missing PRIVATE_KEY(s)"; exit 1; }

SOLANA_BIN="${SOLANA_BIN:-/root/.local/share/solana/install/active_release/bin/solana}"
if [[ ! -x "$SOLANA_BIN" ]]; then
  echo "=== installing solana CLI ==="
  sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"
  SOLANA_BIN="/root/.local/share/solana/install/active_release/bin/solana"
fi

TMPDIR="${TMPDIR:-/tmp/loggaper_fund}"
mkdir -p "$TMPDIR"
chmod 700 "$TMPDIR"

keypair_json() {
  python3 - "$1" "$2" <<'PY'
import json, sys, base58
from nacl.signing import SigningKey
secret_b58 = sys.argv[1]
out = sys.argv[2]
raw = base58.b58decode(secret_b58)
if len(raw) == 64:
    seed = raw[:32]
elif len(raw) == 32:
    seed = raw
else:
    raise SystemExit(f"bad key len {len(raw)}")
sk = SigningKey(seed)
arr = list(bytes(sk)) + list(bytes(sk.verify_key))
with open(out, "w") as f:
    json.dump(arr, f)
PY
}

KP1="$TMPDIR/wallet_1.json"
keypair_json "$PK1" "$KP1"
SRC="$("$SOLANA_BIN" address -k "$KP1")"

"$SOLANA_BIN" config set --url "$RPC" >/dev/null
echo "=== transfer ${AMOUNT_SOL} SOL from $SRC -> wallet_2 ($DEST) ==="
"$SOLANA_BIN" transfer --from "$KP1" "$DEST" "$AMOUNT_SOL" --allow-unfunded-recipient --fee-payer "$KP1"
rm -f "$KP1"

echo "=== balances ==="
"$SOLANA_BIN" balance "$SRC"
"$SOLANA_BIN" balance "$DEST"
