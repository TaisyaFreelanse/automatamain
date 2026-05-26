#!/usr/bin/env bash
set -euo pipefail
cd /root/automata-build
git fetch origin
git merge origin/main --no-edit
if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck source=/dev/null
  . "$HOME/.cargo/env"
fi
export PATH="${HOME}/.cargo/bin:${PATH}"
cargo build --release
systemctl stop loggaper
install -m 755 /root/automata-build/target/release/loggaper /home/automata/loggaper
systemctl start loggaper
sleep 2
systemctl is-active loggaper
stat -c '%y %s' /home/automata/loggaper
journalctl -u loggaper -n 20 --no-pager
