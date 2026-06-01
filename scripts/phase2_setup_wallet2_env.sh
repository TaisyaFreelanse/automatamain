#!/usr/bin/env bash
# Run on prod as root: create PRIVATE_KEY_WALLET_2 if missing, print pubkey only.
set -euo pipefail
ENV_FILE="${ENV_FILE:-/home/automata/.env}"
PUB_FILE="${PUB_FILE:-/home/automata/wallet_2_pubkey.txt}"

if grep -qE 'PRIVATE_KEY_WALLET_2[[:space:]]*=' "$ENV_FILE" 2>/dev/null; then
  echo "[setup] PRIVATE_KEY_WALLET_2 already in $ENV_FILE"
else
  read -r KEY PUB < <(python3 -c "
import nacl.signing, base58
sk = nacl.signing.SigningKey.generate()
kp = bytes(sk) + bytes(sk.verify_key)
print(base58.b58encode(kp).decode(), base58.b58encode(bytes(sk.verify_key)).decode())
")
  printf '\nPRIVATE_KEY_WALLET_2 = %s\n' "$KEY" >>"$ENV_FILE"
  echo "$PUB" >"$PUB_FILE"
  chmod 600 "$ENV_FILE"
  echo "[setup] PRIVATE_KEY_WALLET_2 added"
fi

echo "[setup] wallet_2 pubkey: $(cat "$PUB_FILE")"
