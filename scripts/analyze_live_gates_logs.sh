#!/usr/bin/env bash
# Parse loggaper journal for scoring / live-gate / filter funnel.
# Usage: analyze_live_gates_logs.sh [since]   e.g. "2 hours ago" (default)
set -euo pipefail
SINCE="${1:-2 hours ago}"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
journalctl -u loggaper --since "$SINCE" --no-pager -o cat >"$tmp"

echo "=== Window: since $SINCE ==="
echo "=== loggaper active timestamp ==="
systemctl show loggaper -p ActiveEnterTimestamp --value 2>/dev/null || true
echo

echo "=== [metrics:bot] FIRST in window ==="
grep -F '[metrics:bot]' "$tmp" | head -1 || echo "(none)"
echo "=== [metrics:bot] LAST in window ==="
grep -F '[metrics:bot]' "$tmp" | tail -1 || echo "(none)"
echo

parse_metrics() {
  # creates=.. no_history=.. filter_rejected=.. passed_filter=.. score_skip=.. score_a=.. score_a_plus=.. strategy_blocked=.. positions_initiated=..
  echo "$1" | sed -n 's/.*creates=\([0-9]*\).*no_history=\([0-9]*\).*filter_rejected=\([0-9]*\).*passed_filter=\([0-9]*\).*score_skip=\([0-9]*\).*score_a=\([0-9]*\).*score_a_plus=\([0-9]*\).*strategy_blocked=\([0-9]*\).*positions_initiated=\([0-9]*\).*/\1 \2 \3 \4 \5 \6 \7 \8 \9/p'
}

first="$(grep -F '[metrics:bot]' "$tmp" | head -1 || true)"
last="$(grep -F '[metrics:bot]' "$tmp" | tail -1 || true)"
if [[ -n "$first" && -n "$last" ]]; then
  read -r c0 nh0 fr0 pf0 ss0 sa0 sap0 sb0 pi0 <<<"$(parse_metrics "$first")"
  read -r c1 nh1 fr1 pf1 ss1 sa1 sap1 sb1 pi1 <<<"$(parse_metrics "$last")"
  echo "=== Delta (last minus first [metrics:bot] in window) ==="
  echo "creates:        $((c1 - c0))"
  echo "no_history:     $((nh1 - nh0))"
  echo "filter_rejected:$((fr1 - fr0))"
  echo "passed_filter:  $((pf1 - pf0))"
  echo "score_skip:     $((ss1 - ss0))"
  echo "score_a:        $((sa1 - sa0))"
  echo "score_a_plus:   $((sap1 - sap0))"
  echo "strategy_blocked:$((sb1 - sb0))"
  echo "positions_initiated:$((pi1 - pi0))"
  echo "(score_a / score_a_plus here = passed engine tier AND live gates; see code.)"
fi
echo

echo "=== Log-line counts (window) ==="
score_n="$(grep -cF '[SCORE]' "$tmp" || true)"
echo "SCORE lines:              $score_n"
echo "SCORE tier=APlus:         $(grep -F '[SCORE]' "$tmp" | grep -cF 'tier=APlus' || true)"
echo "SCORE tier=A (not A+):    $(grep -F '[SCORE]' "$tmp" | grep -F 'tier=A' | grep -vcF 'tier=APlus' || true)"
echo "SCORE tier=Skip:          $(grep -F '[SCORE]' "$tmp" | grep -cF 'tier=Skip' || true)"
echo "[SCORE] with momentum_good substring: $(grep -F '[SCORE]' "$tmp" | grep -cF 'momentum_good' || true)"
echo "live skip require_momentum: $(grep -cF 'skipped (live): require_momentum_good' "$tmp" || true)"
echo "live skip min tier APlus:   $(grep -cF 'skipped (live): minimum_tier_for_buy' "$tmp" || true)"
echo "FILTER creator_config:     $(grep -cF 'rejected by creator_config' "$tmp" || true)"
echo "FILTER no_history:         $(grep -cF 'skipped: no creator history' "$tmp" || true)"
echo "[BUY GATE] (passed to mgr): $(grep -cF '[BUY GATE]' "$tmp" || true)"
echo "BUY skip operator_cap:     $(grep -cF 'operator_cap' "$tmp" || true)"
echo

echo "=== Last 10 live require_momentum_good skips ==="
grep -F 'skipped (live): require_momentum_good' "$tmp" | tail -10 || true
echo

echo "=== Last 5 [BUY GATE] (actually dispatched toward InitiateBuy) ==="
grep -F '[BUY GATE]' "$tmp" | tail -5 || true
echo

echo "=== Near-miss SCORE: A or APlus but no momentum_good in same line ==="
grep -F '[SCORE]' "$tmp" | grep -F 'tier=A' | grep -vF 'tier=APlus' | grep -vF 'momentum_good' | tail -6 || true
echo

echo "=== Random high-score Skip examples (last 4) ==="
grep -F '[SCORE]' "$tmp" | grep -F 'tier=Skip' | tail -4 || true
