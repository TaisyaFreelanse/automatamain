#!/usr/bin/env bash
# Multi-wallet copy-trade prod deploy (run on server as root).
#
# Phase 1 (default): build, preflight (flat book), deploy binary + wallets yaml,
#   migration 0015 on startup (sqlx). wallet_2 may be disabled / env absent — skipped.
#
# Phase 2 (--enable-wallet-2): after PRIVATE_KEY_WALLET_2 is in /home/automata/.env,
#   flip wallet_2 enabled in yaml and restart (preflight again).
#
# Usage:
#   bash scripts/deploy_multi_wallet_prod.sh
#   bash scripts/deploy_multi_wallet_prod.sh --enable-wallet-2
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-/root/automata-build}"
INSTALL_BIN="${INSTALL_BIN:-/home/automata/loggaper}"
ENV_YAML="${ENV_YAML:-/home/automata/filter_config.yaml}"
ENV_FILE="${ENV_FILE:-/home/automata/.env}"
ENABLE_W2=false

for arg in "$@"; do
  case "$arg" in
    --enable-wallet-2) ENABLE_W2=true ;;
    -h|--help)
      echo "Usage: $0 [--enable-wallet-2]"
      exit 0
      ;;
  esac
done

dburl() {
  sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$ENV_FILE" \
    | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1
}

ensure_wallets_block() {
  if grep -q '^wallets:' "$ENV_YAML" 2>/dev/null; then
    echo "=== $ENV_YAML already has wallets: ==="
    grep -A20 '^wallets:' "$ENV_YAML" | head -22
    return 0
  fi
  echo "=== patching $ENV_YAML: wallets block (wallet_2 disabled until key) ==="
  cat >>"$ENV_YAML" <<'EOF'

# Copy-trade wallets (keys in env only — never put secrets here).
wallets:
  - id: wallet_1
    label: Main
    enabled: true
    private_key_env: PRIVATE_KEY
    size_sol: null
  - id: wallet_2
    label: Copy
    enabled: false
    private_key_env: PRIVATE_KEY_WALLET_2
    size_sol: 0.05
EOF
}

patch_wallet_2_enabled() {
  local on="$1"
  if ! grep -q 'id: wallet_2' "$ENV_YAML"; then
    echo "[FAIL] no wallet_2 in $ENV_YAML — add wallets block first" >&2
    exit 1
  fi
  if ! grep -qE '^[[:space:]]*PRIVATE_KEY_WALLET_2[[:space:]]*=' "$ENV_FILE" 2>/dev/null \
    && ! grep -qE '^export[[:space:]]+PRIVATE_KEY_WALLET_2=' "$ENV_FILE" 2>/dev/null; then
    echo "[FAIL] PRIVATE_KEY_WALLET_2 not found in $ENV_FILE" >&2
    exit 1
  fi
  perl -i -0pe "
    s/(^[[:space:]]*- id: wallet_2\n(?:^[[:space:]].*\n)*?^[[:space:]]*enabled:)[[:space:]]*(true|false)/\${1} $on/m
  " "$ENV_YAML"
  echo "=== wallet_2 enabled=$on in $ENV_YAML ==="
  grep -A6 'id: wallet_2' "$ENV_YAML"
}

if $ENABLE_W2; then
  patch_wallet_2_enabled true
fi

cd "$BUILD_DIR"
git fetch origin
git reset --hard origin/main
git log -1 --oneline

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck source=/dev/null
  . "$HOME/.cargo/env"
fi
export PATH="${HOME}/.cargo/bin:${PATH}"

cargo build --release --bin loggaper

bash "$SCRIPT_DIR/deploy_preflight.sh"

systemctl stop loggaper || true
sleep 2
install -m 755 "$BUILD_DIR/target/release/loggaper" "$INSTALL_BIN"

cp "$BUILD_DIR/filter_config.yaml" "$ENV_YAML"
ensure_wallets_block

if $ENABLE_W2; then
  patch_wallet_2_enabled true
else
  # Keep wallet_2 off on fresh copy from repo unless explicitly enabling.
  if grep -q 'id: wallet_2' "$ENV_YAML"; then
    perl -i -0pe 's/(^[[:space:]]*- id: wallet_2\n(?:^[[:space:]].*\n)*?^[[:space:]]*enabled:)[[:space:]]*true/\1 false/m' "$ENV_YAML" || true
  fi
fi

perl -pi -e 's/^\s+mode:\s+demo/  mode: live/' "$ENV_YAML"
grep -E '^[[:space:]]*mode:' "$ENV_YAML" | head -1

DBURL="$(dburl)"
if [[ -n "$DBURL" ]]; then
  echo "=== migration 0015 pre-check (optional; sqlx runs on start) ==="
  psql "$DBURL" -X -P pager=off -c \
    "SELECT column_name FROM information_schema.columns WHERE table_name='bot_trades' AND column_name='wallet_id';" \
    || true
fi

systemctl start loggaper
sleep 3
systemctl is-active loggaper

echo "=== post-start migration row ==="
if [[ -n "$DBURL" ]]; then
  psql "$DBURL" -X -P pager=off -c \
    "SELECT version, description, success FROM _sqlx_migrations WHERE version >= 14 ORDER BY version;" \
    || true
fi

bash "$SCRIPT_DIR/verify_multi_wallet.sh"

echo ""
echo "=== next steps ==="
if ! $ENABLE_W2; then
  echo "1. Add PRIVATE_KEY_WALLET_2 to $ENV_FILE (base58, same format as PRIVATE_KEY)."
  echo "2. When flat: bash $SCRIPT_DIR/deploy_multi_wallet_prod.sh --enable-wallet-2"
  echo "3. After first live BUY: bash $SCRIPT_DIR/verify_first_copy_trade.sh [MINT]"
else
  echo "wallet_2 enabled — on first copy BUY run: bash $SCRIPT_DIR/verify_first_copy_trade.sh <mint>"
fi
