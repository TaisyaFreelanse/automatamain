#!/usr/bin/env bash
# Prod: permanent dev blacklist — build, preflight, restart, migration, config patch.
set -euo pipefail
BUILD_DIR="${BUILD_DIR:-/root/automata-build}"
CFG="${ENV_YAML:-/home/automata/filter_config.yaml}"
INSTALL_BIN="${INSTALL_BIN:-/home/automata/loggaper}"

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
cargo test dev_blacklist --release --quiet

bash "$(dirname "$0")/deploy_preflight.sh"

systemctl stop loggaper || true
sleep 2
install -m 755 "$BUILD_DIR/target/release/loggaper" "$INSTALL_BIN"

if ! grep -q '^dev_blacklist:' "$CFG"; then
  echo "=== patching $CFG: dev_blacklist block ==="
  perl -i -pe 'BEGIN{undef $/;} s/(# Strategy controller)/dev_blacklist:\n  enabled: true\n  cooldown_secs: 604800\n  min_pnl_pct_for_sl: -30.0\n  min_tick_drop_pct: 40.0\n  permanent_min_tick_drop_pct: 55.0\n  permanent_min_mcap_drop_pct: 60.0\n\n$1/s' "$CFG"
else
  echo "=== $CFG already has dev_blacklist ==="
  grep -q 'permanent_min_tick_drop_pct' "$CFG" || {
    perl -i -pe 'if (/^  min_tick_drop_pct:/){ $_ .= "  permanent_min_tick_drop_pct: 55.0\n  permanent_min_mcap_drop_pct: 60.0\n" }' "$CFG"
  }
fi
echo "--- dev_blacklist in $CFG ---"
grep -A8 '^dev_blacklist:' "$CFG" | head -10

systemctl start loggaper
sleep 2
systemctl is-active loggaper
curl -sf --max-time 5 http://127.0.0.1:1662/status | head -c 120 || true
echo

bash "$(dirname "$0")/upgrade_dev_blacklist_permanent.sh"

echo "=== permanent devs (expires_at=0) ==="
DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' /home/automata/.env | tr -d '"' | tr -d "'" | head -1)"
psql "$DBURL" -X -P pager=off -c "
SELECT dev_wallet, reason, mint, expires_at,
       to_timestamp(created_at) AT TIME ZONE 'Europe/Moscow' AS created_msk
FROM dev_blacklist
WHERE expires_at = 0
ORDER BY created_at DESC;"

echo "=== SQL active-ban check (expires_at=0 must match) ==="
psql "$DBURL" -X -P pager=off -c "
SELECT dev_wallet,
       count(*) FILTER (WHERE expires_at = 0 OR expires_at > extract(epoch FROM now())::bigint) AS active_rows
FROM dev_blacklist
GROUP BY dev_wallet
HAVING count(*) FILTER (WHERE expires_at = 0 OR expires_at > extract(epoch FROM now())::bigint) > 0
ORDER BY active_rows DESC
LIMIT 20;"
