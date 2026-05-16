#!/usr/bin/env bash
# On-server: compare /status vs RPC getBalance + Token-2022 ATAs for hot wallet.
set -euo pipefail

WALLET="${1:-2X9SYhDEXmgiXxQRcTa4dvi6TzshG1zgXhaTyvvYT8Ei}"
HTTP_PORT="${2:-1662}"

for envfile in /home/automata/.env /root/.env; do
  if [[ -f "$envfile" ]] && grep -Eq '^[[:space:]]*SOLANA_HTTP[[:space:]]*=' "$envfile"; then
    RPC="$(
      grep -E '^[[:space:]]*SOLANA_HTTP[[:space:]]*=' "$envfile" \
        | head -1 \
        | sed -E 's/^[[:space:]]*SOLANA_HTTP[[:space:]]*=[[:space:]]*//; s/^"//; s/"$//; s/^'"'"'//; s/'"'"'$//'
    )"
    export RPC
    break
  fi
done

if [[ -z "${RPC:-}" ]]; then
  echo "No SOLANA_HTTP in .env" >&2
  exit 1
fi

echo "=== GET http://127.0.0.1:${HTTP_PORT}/status ==="
curl -s "http://127.0.0.1:${HTTP_PORT}/status" || true
echo

echo "=== RPC getBalance (confirmed) ==="
curl -s "$RPC" -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"$WALLET\",{\"commitment\":\"confirmed\"}]}" \
  > /tmp/bal.json
python3 -c "import json; j=json.load(open('/tmp/bal.json')); v=j['result']['value']; print('lamports', v); print('SOL', v/1e9)"

echo "=== Token-2022 ATAs (program TokenzQd...) ==="
curl -s "$RPC" -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTokenAccountsByOwner\",\"params\":[\"$WALLET\",{\"programId\":\"TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb\"},{\"encoding\":\"jsonParsed\"}]}" \
  > /tmp/ta2022.json
python3 - <<'PY'
import json
with open("/tmp/ta2022.json") as f:
    j = json.load(f)
if "error" in j:
    print("RPC error:", j["error"])
    raise SystemExit(0)
vals = j.get("result", {}).get("value", [])
nonz, zeros = [], []
for a in vals:
    info = a.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
    tok = info.get("tokenAmount", {})
    amt_s = tok.get("uiAmountString") or "0"
    try:
        amt = float(amt_s)
    except ValueError:
        amt = 0.0
    mint = info.get("mint", "")
    pk = a.get("pubkey", "")
    if amt > 1e-9:
        nonz.append((mint, amt, pk))
    else:
        zeros.append((mint, pk))
print("Token-2022 ATA count:", len(vals))
print("Non-zero:", len(nonz))
for mint, amt, pk in sorted(nonz, key=lambda x: -x[1])[:30]:
    print(f"  {mint}  ui={amt}  ata={pk}")
print("Zero-balance ATAs:", len(zeros))
for mint, pk in zeros[:40]:
    print(f"  mint={mint}  ata={pk}")
if len(zeros) > 40:
    print(f"  ... and {len(zeros)-40} more zero ATAs")
PY

echo "=== Legacy Tokenkeg ATAs (optional) ==="
curl -s "$RPC" -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTokenAccountsByOwner\",\"params\":[\"$WALLET\",{\"programId\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\"},{\"encoding\":\"jsonParsed\"}]}" \
  | python3 -c "import sys,json; j=json.load(sys.stdin); v=j.get('result',{}).get('value',[]); print('count',len(v))"

echo "Done."
