#!/usr/bin/env bash
# Maximum prod battle-readiness checks for multi-wallet copy-trade (run on server as root).
set -euo pipefail

HTTP="${HTTP_URL:-http://127.0.0.1:1662}"
ENV_FILE="${ENV_FILE:-/home/automata/.env}"
CFG="${ENV_YAML:-/home/automata/filter_config.yaml}"
PASS=0
FAIL=0
WARN=0

ok() { echo "[PASS] $*"; PASS=$((PASS + 1)); }
bad() { echo "[FAIL] $*"; FAIL=$((FAIL + 1)); }
warn() { echo "[WARN] $*"; WARN=$((WARN + 1)); }

dburl() {
  sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$ENV_FILE" \
    | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1
}

get_env() {
  local key="$1"
  sed -nE "s/^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=[[:space:]]*//p" "$ENV_FILE" \
    | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1
}

echo "========== PROD BATTLE TEST: multi-wallet live =========="
echo "time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

if systemctl is-active --quiet loggaper; then ok "loggaper systemd active"; else bad "loggaper not active"; fi

[[ -n "$(get_env PRIVATE_KEY)" ]] && ok "PRIVATE_KEY set" || bad "PRIVATE_KEY missing"
[[ -n "$(get_env PRIVATE_KEY_WALLET_2)" ]] && ok "PRIVATE_KEY_WALLET_2 set" || bad "PRIVATE_KEY_WALLET_2 missing"
[[ -n "$(get_env DATABASE_URL)" ]] && ok "DATABASE_URL set" || bad "DATABASE_URL missing"
RPC="$(get_env SOLANA_HTTP)"; [[ -z "$RPC" ]] && RPC="$(get_env SOLANA_RPC_URL)"
[[ -n "$RPC" ]] && ok "SOLANA RPC set" || bad "SOLANA RPC missing"
[[ -n "$(get_env SOLANA_WEBSOCKET)" ]] && ok "SOLANA_WEBSOCKET set" || bad "SOLANA_WEBSOCKET missing"

if grep -q '^wallets:' "$CFG" && grep -q 'id: wallet_1' "$CFG" && grep -q 'id: wallet_2' "$CFG"; then
  ok "filter_config wallets block"
else
  bad "filter_config wallets incomplete"
fi
grep -A6 'id: wallet_2' "$CFG" | grep -q 'enabled: true' && ok "wallet_2 enabled in yaml" || bad "wallet_2 disabled in yaml"
grep -A8 'id: wallet_2' "$CFG" | grep -q 'size_sol: 0.05' && ok "wallet_2 size_sol 0.05 yaml" || warn "wallet_2 size_sol not 0.05 in yaml"
grep -E '^[[:space:]]*mode:[[:space:]]*live' "$CFG" >/dev/null && ok "mode live in yaml" || bad "yaml not live"

DBURL="$(dburl)"
if [[ -n "$DBURL" ]]; then
  psql "$DBURL" -X -tAc "SELECT 1 FROM _sqlx_migrations WHERE version=15 AND success" 2>/dev/null | grep -q 1 \
    && ok "migration 0015" || bad "migration 0015"
  psql "$DBURL" -X -tAc "SELECT column_name FROM information_schema.columns WHERE table_name='bot_trades' AND column_name='wallet_id'" 2>/dev/null | grep -q wallet_id \
    && ok "bot_trades.wallet_id column" || bad "wallet_id column missing"
fi

for path in /status /wallets /positions /pnl /bot-trades /mode; do
  code=$(curl -sf -o /dev/null -w "%{http_code}" "${HTTP}${path}" 2>/dev/null || echo 000)
  [[ "$code" == "200" ]] && ok "GET ${path} 200" || bad "GET ${path} ${code}"
done

PYOUT=$(HTTP="$HTTP" python3 <<'PY'
import json, os, urllib.request
base = os.environ["HTTP"]
def get(path):
    with urllib.request.urlopen(base + path, timeout=8) as r:
        return json.load(r)
status, wallets, positions = get("/status"), get("/wallets"), get("/positions")
trades = get("/bot-trades")
if isinstance(trades, dict):
    trades = trades.get("trades") or trades.get("rows") or []
def r(k, m):
    print(f"{k}|{m}")
if status.get("paused") is False:
    r("pass", "status not paused")
else:
    r("fail", f"paused={status.get('paused')}")
if status.get("mode") == "live":
    r("pass", "status mode live")
else:
    r("fail", f"mode={status.get('mode')}")
ids = {w["id"] for w in wallets}
if ids >= {"wallet_1", "wallet_2"}:
    r("pass", "both wallets in API")
else:
    r("fail", f"wallet ids={ids}")
w1 = next((w for w in wallets if w["id"] == "wallet_1"), None)
w2 = next((w for w in wallets if w["id"] == "wallet_2"), None)
if w1 and w2 and w1["pubkey"] != w2["pubkey"]:
    r("pass", "distinct pubkeys")
else:
    r("fail", "pubkey check")
if w1 and w1.get("enabled") and w2 and w2.get("enabled"):
    r("pass", "both enabled in API")
else:
    r("fail", "enable flags")
if w2 and w2.get("size_sol") == 0.05:
    r("pass", "wallet_2 size_sol 0.05 API")
else:
    r("warn", f"wallet_2 size_sol={w2.get('size_sol') if w2 else None}")
sum_bal = sum(w.get("balance_sol", 0) for w in wallets)
total = float(status.get("total_balance_sol") or 0)
if abs(sum_bal - total) < 0.02:
    r("pass", f"total_balance ok ({total:.4f} ~ {sum_bal:.4f})")
else:
    r("warn", f"total={total} sum={sum_bal}")
bal2 = float(w2.get("balance_sol", 0)) if w2 else 0
if bal2 >= 0.06:
    r("pass", f"wallet_2 balance {bal2:.4f} SOL")
elif bal2 >= 0.03:
    r("warn", f"wallet_2 balance tight {bal2:.4f}")
else:
    r("fail", f"wallet_2 underfunded {bal2:.4f}")
for p in positions:
    if "wallet_id" not in p:
        r("fail", "open position missing wallet_id")
        break
else:
    r("pass", f"positions ok ({len(positions)} open)")
if trades and not all("wallet_id" in t for t in trades[:30]):
    r("fail", "bot-trades missing wallet_id")
elif trades:
    r("pass", "bot-trades wallet_id present")
else:
    r("pass", "bot-trades empty")
if w2:
    print(f"pub|{w2['pubkey']}")
PY
)
W2PUB=""
while IFS='|' read -r k m; do
  [[ -z "$k" ]] && continue
  if [[ "$k" == "pub" ]]; then W2PUB="$m"; continue; fi
  case "$k" in
    pass) ok "$m" ;;
    fail) bad "$m" ;;
    warn) warn "$m" ;;
  esac
done <<< "$PYOUT"

SOLANA_BIN="/root/.local/share/solana/install/active_release/bin/solana"
if [[ -x "$SOLANA_BIN" && -n "$RPC" && -n "$W2PUB" ]]; then
  "$SOLANA_BIN" config set --url "$RPC" >/dev/null 2>&1 || true
  chain=$("$SOLANA_BIN" balance "$W2PUB" 2>/dev/null | awk '{print $1}' || echo "")
  api=$(curl -sf "${HTTP}/wallets" | python3 -c "import json,sys; print(next(w['balance_sol'] for w in json.load(sys.stdin) if w['id']=='wallet_2'))")
  if [[ -n "$chain" ]] && python3 -c "exit(0 if abs(float('$chain')-float('$api'))<0.01 else 1)" 2>/dev/null; then
    ok "wallet_2 chain balance matches API"
  else
    warn "wallet_2 chain=${chain:-?} api=${api:-?}"
  fi
fi

if journalctl -u loggaper -g 'wallet=wallet_2 label=Copy mode=live' --no-pager -n 1 2>/dev/null | grep -q 'wallet=wallet_2'; then
  ok "log wallet_2 broker initialized"
else
  bad "no wallet_2 EXEC in recent logs"
fi
if journalctl -u loggaper -g 'wallet=wallet_1 label=Main mode=live' --no-pager -n 1 2>/dev/null | grep -q 'wallet=wallet_1'; then
  ok "log wallet_1 broker initialized"
else
  bad "no wallet_1 EXEC in recent logs"
fi

PUMP_LINE=$(journalctl -u loggaper --since '5 min ago' --no-pager 2>/dev/null | grep '\[metrics:pump\]' | tail -1 || true)
if [[ -n "$PUMP_LINE" ]] && echo "$PUMP_LINE" | grep -qE 'msgs/s=[1-9][0-9]*'; then
  ok "pump stream active"
  echo "       $PUMP_LINE"
else
  bad "no recent pump metrics"
fi

if journalctl -u loggaper --since '30 min ago' --no-pager 2>/dev/null | grep -iE 'panic|panicked' | grep -q .; then
  bad "panic in last 30m"
else
  ok "no panic 30m"
fi

BOT=$(journalctl -u loggaper --since '3 min ago' --no-pager 2>/dev/null | grep 'metrics:bot' | tail -1 || true)
if [[ -n "$BOT" ]]; then
  ok "bot metrics ticking"
  echo "       $BOT"
else
  warn "no recent metrics:bot"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if bash "$SCRIPT_DIR/deploy_preflight.sh" --check-only 2>/dev/null; then
  ok "flat book (preflight)"
else
  warn "open positions on book"
fi

if systemctl cat loggaper 2>/dev/null | grep -q JUPITER_API_KEY; then
  ok "JUPITER_API_KEY in systemd"
else
  warn "JUPITER_API_KEY not in systemd drop-in"
fi

if journalctl -u loggaper --since '7 days ago' -g '\[LATENCY\]' -n 1 --no-pager 2>/dev/null | grep -q '\[LATENCY\]'; then
  ok "[LATENCY] buy-path timing in journal (7d)"
  LAT=$(journalctl -u loggaper --since '7 days ago' -g '\[LATENCY\]' -n 1 --no-pager 2>/dev/null | tail -1)
  if echo "$LAT" | grep -q 'wallet_1:.*wallet_2:'; then
    ok "[LATENCY] multi-wallet line (w1+w2)"
  else
    warn "[LATENCY] latest line may be single-wallet only"
  fi
else
  warn "no [LATENCY] in journal yet (expected after first A/A+ BUY post P0 deploy)"
fi

echo ""
echo "========== SUMMARY =========="
echo "PASS=$PASS  FAIL=$FAIL  WARN=$WARN"
if [[ "$FAIL" -eq 0 ]]; then
  echo "RESULT: READY — live copy-trade infra OK; first BUY -> verify_first_copy_trade.sh <mint>"
  exit 0
fi
echo "RESULT: FIX FAIL items above"
exit 1
