#!/usr/bin/env bash
# Careful prod deploy: P0–P2 latency optimizations (no trading-parameter changes).
#
# Policy:
#   - Do NOT lower buy_fanout_delay_ms on prod (keep 800 in filter_config.yaml).
#   - Do NOT change scoring_window_ms or strategy thresholds.
#   - Deploy only when flat: deploy_preflight / wait_no_open_positions.
#   - Roll back binary on write_queue drops storm or confirm/tx regressions.
#
# Run on server as root from /root/automata-build after git pull/merge.
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-/root/automata-build}"
INSTALL_BIN="${INSTALL_BIN:-/home/automata/loggaper}"
CFG="${ENV_YAML:-/home/automata/filter_config.yaml}"
BACKUP_BIN="${BACKUP_BIN:-/home/automata/loggaper.pre-latency-opt}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HTTP="${HTTP_URL:-http://127.0.0.1:1662}"

step() { echo ""; echo "========== $* =========="; }

fail() { echo "[FAIL] $*"; exit 1; }
ok() { echo "[OK] $*"; }

step "0) Config guard (trading params unchanged)"
grep -q 'buy_fanout_delay_ms: 800' "$CFG" \
  || fail "prod yaml must keep buy_fanout_delay_ms: 800 (do not tune down yet)"
ok "buy_fanout_delay_ms: 800 in $CFG"

if grep -qE '^[[:space:]]*scoring_window_ms:[[:space:]]*[0-9]+' "$CFG"; then
  SW=$(grep -E '^[[:space:]]*scoring_window_ms:' "$CFG" | head -1)
  echo "NOTE: scoring_window unchanged by this deploy — $SW"
fi

step "1) Preflight — no open positions"
bash "$SCRIPT_DIR/deploy_preflight.sh" --check-only \
  || fail "open positions on book — wait flat or close manually"

step "2) Build + unit tests"
cd "$BUILD_DIR"
if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck source=/dev/null
  . "$HOME/.cargo/env"
fi
export PATH="${HOME}/.cargo/bin:${PATH}"
cargo test
cargo build --release
ok "cargo test + release build"

step "3) Pre-stop preflight (re-check flat)"
bash "$SCRIPT_DIR/deploy_preflight.sh" --check-only

step "4) Backup + install binary"
[[ -f "$INSTALL_BIN" ]] && cp -a "$INSTALL_BIN" "$BACKUP_BIN" && ok "backed up to $BACKUP_BIN"
systemctl stop loggaper
install -m 755 "$BUILD_DIR/target/release/loggaper" "$INSTALL_BIN"
systemctl start loggaper
sleep 3
systemctl is-active --quiet loggaper || fail "loggaper not active after start"
ok "loggaper restarted"

step "5) Smoke HTTP"
bash "$SCRIPT_DIR/verify_multi_wallet.sh"
bash "$SCRIPT_DIR/prod_battle_multi_wallet_test.sh"

step "6) Post-deploy watch (first 5 min) — manual review"
echo "--- Recent boot / errors ---"
journalctl -u loggaper -n 40 --no-pager | tail -40
echo ""
echo "--- Write queue drops (should stay 0 or rare) ---"
journalctl -u loggaper --since '5 min ago' -g 'WRITE_QUEUE' --no-pager 2>/dev/null | tail -20 || true
echo ""
echo "--- DB pool / acquire (should be absent) ---"
journalctl -u loggaper --since '5 min ago' -g 'acquire' --no-pager 2>/dev/null | tail -10 || true
journalctl -u loggaper --since '5 min ago' -g 'pool' --no-pager 2>/dev/null | tail -10 || true
echo ""
echo "--- LATENCY (after first A/A+ BUY) ---"
journalctl -u loggaper --since '1 hour ago' -g 'LATENCY' --no-pager 2>/dev/null | tail -5 \
  || echo "(none yet — run after first copy-trade)"
echo ""
echo "After first copy-trade:"
echo "  bash $SCRIPT_DIR/verify_parallel_buy_latency.sh <mint_substring>"
echo ""
echo "Rollback if write_queue drops spike or confirm regressions:"
echo "  systemctl stop loggaper"
echo "  cp -a $BACKUP_BIN $INSTALL_BIN"
echo "  systemctl start loggaper"
echo ""
ok "deploy_latency_optimization_prod finished — monitor live; do not lower buy_fanout_delay_ms yet"
