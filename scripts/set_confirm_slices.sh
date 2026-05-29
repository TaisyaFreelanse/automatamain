#!/usr/bin/env bash
set -euo pipefail

F=/home/automata/filter_config.yaml
cp "$F" "$F.bak.preslices4"

echo '--- before ---'
grep -n 'confirm_slices' "$F" || true

# Bump confirm_slices 2 -> 4 (continuation block); keep min_upticks: 2.
sed -i -E 's/^([[:space:]]*)confirm_slices:[[:space:]]*2[[:space:]]*$/\1confirm_slices: 4/' "$F"

echo '--- after ---'
grep -n 'confirm_slices' "$F" || true

echo '--- continuation block ---'
sed -n '/continuation:/,/anti_parabolic:/p' "$F"

echo '--- restart service ---'
systemctl restart loggaper
sleep 2
systemctl is-active loggaper
