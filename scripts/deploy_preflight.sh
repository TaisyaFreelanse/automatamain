#!/usr/bin/env bash
# Shared pre-deploy gate: do not stop loggaper while positions are open.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAIT_SCRIPT="${WAIT_SCRIPT:-$DIR/wait_no_open_positions.sh}"
if [[ ! -x "$WAIT_SCRIPT" ]]; then
  chmod +x "$WAIT_SCRIPT" 2>/dev/null || true
fi
echo "=== deploy preflight: open positions ==="
bash "$WAIT_SCRIPT" "$@"
