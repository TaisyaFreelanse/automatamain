#!/usr/bin/env bash
set -euo pipefail
SINCE="${1:-2026-05-21 14:22:41}"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
journalctl -u loggaper --since "$SINCE" --no-pager -o cat >"$tmp"

echo "=== Since: $SINCE (server $(date)) ==="
echo "log lines: $(wc -l <"$tmp")"
echo "created: $(grep -c '^created ' "$tmp" || true)"
echo "FILTER creator_config: $(grep -c 'rejected by creator_config' "$tmp" || true)"
echo "FILTER no_history: $(grep -c 'skipped: no creator history' "$tmp" || true)"
echo "SCORE lines: $(grep -cF '[SCORE]' "$tmp" || true)"
echo "SCORE APlus: $(grep -F '[SCORE]' "$tmp" | grep -cF 'tier=APlus' || true)"
echo "SCORE Skip: $(grep -F '[SCORE]' "$tmp" | grep -cF 'tier=Skip' || true)"
echo "BUY GATE: $(grep -cF '[BUY GATE]' "$tmp" || true)"
echo "BUY Opened: $(grep -cF '[BUY] Opened' "$tmp" || true)"
echo "SELL: $(grep -cF '[SELL]' "$tmp" || true)"
echo "live skips:"
grep -oE 'skipped \(live\): [^|]+' "$tmp" 2>/dev/null | sort | uniq -c | sort -rn | head -8 || true
echo "last metrics:bot:"
grep -F '[metrics:bot]' "$tmp" | tail -1 || true
