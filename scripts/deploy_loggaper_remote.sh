#!/usr/bin/env bash
# Run on server as root: pull automata-build, release-build loggaper, install, set live mode in yaml, restart.
set -euo pipefail
BUILD_DIR="${BUILD_DIR:-/root/automata-build}"
INSTALL_BIN="${INSTALL_BIN:-/home/automata/loggaper}"
ENV_YAML="${ENV_YAML:-/home/automata/filter_config.yaml}"

cd "$BUILD_DIR"
git fetch origin
git reset --hard origin/main
git log -1 --oneline
cargo build --release --bin loggaper
bash "$(dirname "$0")/deploy_preflight.sh"
systemctl stop loggaper || true
sleep 2
cp target/release/loggaper "$INSTALL_BIN"
chmod +x "$INSTALL_BIN"
cp filter_config.yaml "$ENV_YAML"
perl -pi -e 's/^\s+mode:\s+demo/  mode: live/' "$ENV_YAML"
grep -E '^[[:space:]]*mode:' "$ENV_YAML" | head -1
systemctl start loggaper
sleep 2
systemctl is-active loggaper
curl -sS "http://127.0.0.1:1662/status" || true
