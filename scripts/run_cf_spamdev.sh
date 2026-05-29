#!/usr/bin/env bash
# Build + run the spam_dev counterfactual on the prod box. Read-only analysis.
set -euo pipefail
cd /root/automata-build

if [[ -f "$HOME/.cargo/env" ]]; then . "$HOME/.cargo/env"; fi
export PATH="${HOME}/.cargo/bin:${PATH}"

echo "[build] cargo build --release --bin cf_spamdev"
cargo build --release --bin cf_spamdev 2>&1 | tail -5

# Discover DATABASE_URL from the runtime .env files.
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null || echo /home/automata)"
DBURL=""
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }

CFG=/home/automata/filter_config.yaml
SM=/home/automata/state/smart_money.json
CSV=/root/spam_dev_mints.csv
WIN="${1:-14}"

echo "[run] win_slots=$WIN"
DATABASE_URL="$DBURL" ./target/release/cf_spamdev "$CFG" "$SM" "$CSV" "$WIN"
