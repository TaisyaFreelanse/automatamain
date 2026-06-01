#!/usr/bin/env bash
# After first multi-wallet BUY: confirm two positions / history rows per mint.
# Usage: verify_first_copy_trade.sh [MINT_BASE58]
set -euo pipefail

HTTP="${HTTP_URL:-http://127.0.0.1:1662}"
MINT="${1:-}"

echo "=== GET /wallets ==="
curl -sf "${HTTP}/wallets" | python3 -m json.tool

echo ""
echo "=== GET /status (balance per wallet + total) ==="
curl -sf "${HTTP}/status" | python3 -c "
import json,sys
s=json.load(sys.stdin)
print('mode:', s.get('mode'))
print('balance_sol:', s.get('balance_sol'))
for w in s.get('wallets') or []:
    print(' ', w.get('id'), 'enabled=', w.get('enabled'), 'bal=', w.get('balance_sol'), 'size=', w.get('size_sol'), 'pubkey=', (w.get('pubkey') or '')[:12]+'…')
"

echo ""
echo "=== GET /pnl ==="
curl -sf "${HTTP}/pnl" | python3 -m json.tool

echo ""
echo "=== GET /positions ==="
POSITIONS="$(curl -sf "${HTTP}/positions")"
echo "$POSITIONS" | python3 -m json.tool

export POSITIONS_JSON="$POSITIONS"
python3 - "$MINT" <<'PY'
import json, os, sys

mint_filter = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
raw = os.environ.get("POSITIONS_JSON", "")
positions = json.loads(raw) if raw.strip() else []
if mint_filter:
    positions = [p for p in positions if p.get("address") == mint_filter or p.get("mint") == mint_filter]

by_mint = {}
for p in positions:
    m = p.get("address") or p.get("mint") or "?"
    by_mint.setdefault(m, []).append(p.get("wallet_id", "?"))

print("\n--- open positions by mint (wallet_id list) ---")
for m, wids in sorted(by_mint.items()):
    print(f"  {m}: {wids} (count={len(wids)})")
    if len(wids) >= 2 and len(set(wids)) >= 2:
        print("    OK: multiple wallets on same mint")

if mint_filter and mint_filter not in by_mint:
    print(f"NOTE: no open position for mint {mint_filter} (maybe already closed)")
PY

echo ""
echo "=== GET /bot-trades (last 20) ==="
TRADES="$(curl -sf "${HTTP}/bot-trades?limit=20")"
echo "$TRADES" | python3 -m json.tool 2>/dev/null || echo "$TRADES" | head -c 3000

export TRADES_JSON="$TRADES"
python3 - "$MINT" <<'PY'
import json, os, sys
from collections import defaultdict

mint_filter = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
raw = os.environ.get("TRADES_JSON", "")
try:
    trades = json.loads(raw)
except json.JSONDecodeError:
    print("WARN: could not parse bot-trades JSON")
    sys.exit(0)
if isinstance(trades, dict):
    trades = trades.get("trades") or trades.get("rows") or []

if mint_filter:
    trades = [t for t in trades if t.get("mint") == mint_filter]

by_mint = defaultdict(list)
for t in trades:
    by_mint[t.get("mint", "?")].append(t.get("wallet_id", "wallet_1"))

print("\n--- history by mint (wallet_id list, recent window) ---")
for m, wids in sorted(by_mint.items(), key=lambda x: -len(x[1]))[:10]:
    uniq = sorted(set(wids))
    print(f"  {m}: {wids} unique={uniq}")
    if len(uniq) >= 2:
        print("    OK: two wallet_id rows for same mint in history sample")
PY

echo ""
echo "Done. Manual checks on first live BUY:"
echo "  - logs: two ExecuteBuy / PositionOpen with wallet_1 and wallet_2"
echo "  - SELL/TP/SL: each exit uses the same wallet_id as its position"
echo "  - dashboard HISTORY filter: both wallets; combined PnL in header"
