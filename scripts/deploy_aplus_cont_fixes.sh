#!/usr/bin/env bash
# Deploy: A+ second-look on cont_no_uptick + peak guard recheck mcap floor.
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
cargo test continuation --release --quiet

bash "$(dirname "$0")/deploy_preflight.sh"

systemctl stop loggaper || true
sleep 2
install -m 755 "$BUILD_DIR/target/release/loggaper" "$INSTALL_BIN"

if grep -q '^    aplus_peak_guard:' "$CFG"; then
  if ! grep -q 'recheck_min_vs_peak_ratio' "$CFG"; then
    perl -i -pe 'if (/^\s+strong_new_buyers:/) { $_ .= "      recheck_min_vs_peak_ratio: 0.78\n" }' "$CFG"
    echo "=== patched recheck_min_vs_peak_ratio ==="
  fi
fi
grep -A10 'aplus_peak_guard:' "$CFG" | head -12 || true

systemctl start loggaper
sleep 3
systemctl is-active loggaper
curl -sf --max-time 5 http://127.0.0.1:1662/status | head -c 120 || true
echo

SINCE="$(date '+%F %T')"
echo "=== post-deploy watch markers (since deploy) ==="
echo "ActiveSince: $(systemctl show loggaper -p ActiveEnterTimestamp --value)"
journalctl -u loggaper --since "$SINCE" --no-pager 2>/dev/null | head -5 || true
