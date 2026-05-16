#!/usr/bin/env bash
# Post-deploy scoring / momentum analysis from loggaper journal.
# Usage: analyze_momentum_window.sh [journalctl --since arg]
# Default: start time of current loggaper unit (field 2+3 of ActiveEnterTimestamp).
set -euo pipefail

if [[ -n "${1:-}" ]]; then
  SINCE="$1"
else
  ts="$(systemctl show loggaper -p ActiveEnterTimestamp --value 2>/dev/null || true)"
  SINCE="$(echo "$ts" | awk '{print $2" "$3}')"
  [[ -n "${SINCE// }" ]] || SINCE="6 hours ago"
fi

tmp="$(mktemp)"
mf="$(mktemp)"
trap 'rm -f "$tmp" "$mf"' EXIT
journalctl -u loggaper --since "$SINCE" --no-pager -o cat >"$tmp"

echo "=== Window: since $SINCE ==="
systemctl show loggaper -p ActiveEnterTimestamp --value 2>/dev/null || true
echo

echo "=== [metrics:bot] FIRST ==="
grep -F '[metrics:bot]' "$tmp" | head -1 || echo "(none)"
echo "=== [metrics:bot] LAST ==="
grep -F '[metrics:bot]' "$tmp" | tail -1 || echo "(none)"
echo

parse_metrics() {
  echo "$1" | sed -n 's/.*creates=\([0-9]*\).*no_history=\([0-9]*\).*filter_rejected=\([0-9]*\).*passed_filter=\([0-9]*\).*score_skip=\([0-9]*\).*score_a=\([0-9]*\).*score_a_plus=\([0-9]*\).*strategy_blocked=\([0-9]*\).*positions_initiated=\([0-9]*\).*/\1 \2 \3 \4 \5 \6 \7 \8 \9/p'
}
first="$(grep -F '[metrics:bot]' "$tmp" | head -1 || true)"
last="$(grep -F '[metrics:bot]' "$tmp" | tail -1 || true)"
if [[ -n "$first" && -n "$last" ]]; then
  read -r c0 nh0 fr0 pf0 ss0 sa0 sap0 sb0 pi0 <<<"$(parse_metrics "$first")"
  read -r c1 nh1 fr1 pf1 ss1 sa1 sap1 sb1 pi1 <<<"$(parse_metrics "$last")"
  echo "=== Delta metrics (last - first) ==="
  echo "creates=$(((c1 - c0))) no_history=$(((nh1 - nh0))) filter_rejected=$(((fr1 - fr0))) passed_filter=$(((pf1 - pf0)))"
  echo "score_skip=$(((ss1 - ss0))) score_a=$(((sa1 - sa0))) score_a_plus=$(((sap1 - sap0))) strategy_blocked=$(((sb1 - sb0))) positions_initiated=$(((pi1 - pi0)))"
fi
echo

score_lines="$(grep -F '[SCORE]' "$tmp" | grep -c . || true)"
sc_mf="$(grep -F '[SCORE]' "$tmp" | grep -cF 'momentum_good' || true)"
echo "=== [SCORE] lines total: $score_lines (with substring momentum_good: $sc_mf) ==="
echo "SCORE tier=APlus: $(grep -F '[SCORE]' "$tmp" | grep -cF 'tier=APlus' || true)"
echo "SCORE tier=A (not A+): $(grep -F '[SCORE]' "$tmp" | grep -F 'tier=A' | grep -vcF 'tier=APlus' || true)"
echo "SCORE tier=Skip: $(grep -F '[SCORE]' "$tmp" | grep -cF 'tier=Skip' || true)"
echo "live skip require_momentum_good: $(grep -cF 'skipped (live): require_momentum_good' "$tmp" || true)"
echo "live skip minimum_tier: $(grep -cF 'skipped (live): minimum_tier_for_buy' "$tmp" || true)"
echo "[BUY GATE]: $(grep -cF '[BUY GATE]' "$tmp" || true)"
echo "BUY skip operator_cap line: $(grep -cF 'operator_cap' "$tmp" || true)"
echo

# momentum % from mcap_init / mcap_now on [SCORE] lines
: >"$mf"
while IFS= read -r line; do
  [[ "$line" != *"[SCORE]"* ]] && continue
  init="$(echo "$line" | sed -n 's/.*mcap_init=\([0-9.]*\).*/\1/p')"
  now="$(echo "$line" | sed -n 's/.*mcap_now=\([0-9.]*\).*/\1/p')"
  mint="$(echo "$line" | sed -n 's/.*\[SCORE\] \([^ ]*\) .*/\1/p')"
  tier="$(echo "$line" | sed -n 's/.*tier=\([^ ]*\) .*/\1/p')"
  score="$(echo "$line" | sed -n 's/.*score=\([^ ]*\) .*/\1/p')"
  if [[ -z "$init" || -z "$now" ]]; then
    echo "??|$mint|$tier|$score|init=$init|now=$now|" >>"$mf"
    continue
  fi
  pct="$(awk -v a="$init" -v b="$now" 'BEGIN{if (a>0) printf "%.6f", (b/a-1)*100; else print "nan"}')"
  has_mf=0
  [[ "$line" == *"momentum_good"* ]] && has_mf=1
  echo "${pct}|${has_mf}|${mint}|${tier}|${score}|${init}|${now}|${line:0:220}" >>"$mf"
done < "$tmp"

echo "=== Momentum % on [SCORE] rows (mcap growth over scoring window) ==="
echo "(pct|has_mf_flag|mint|tier|score|init|now|trunc)"
sort -t'|' -k1,1n "$mf" | head -20
echo "..."
sort -t'|' -k1,1n "$mf" | tail -15
echo

echo "=== Counts by momentum bucket (SCORE rows with numeric pct) ==="
grep -v '^??|' "$mf" | awk -F'|' 'BEGIN{neg=b0=b2=b4=b12=high=withmf=almost=0}
$1!="nan"{
  p=$1+0
  if (p < 0) neg++; else if (p < 2) b0++; else if (p < 4) b2++; else if (p < 12) b4++; else if (p < 30) b12++; else high++
  if (p>=2 && p<4 && $2==0) almost++
  if ($2==1) withmf++
}
END{
  print "negative:", neg+0
  print "[0,2)%:", b0+0
  print "[2,4)% (no momentum_good flag):", b2+0, "  <- old 12%% floor would reject; 4%% may still reject if below 4"
  print "[4,12)%:", b4+0
  print "[12,30)%:", b12+0
  print ">=30 or overheated band edge:", high+0
  print "rows with momentum_good in line:", withmf+0
  print "[2,4)% AND no momentum_good substring:", almost+0
}'
grep -v '^??|' "$mf" | awk -F'|' 'BEGIN{n=0} $1!="nan"{n++; if($1+0>=2 && $1+0<4 && $2==0) c++} END{print "SCORE rows parsed:", n, "  in [2,4)% without mf:", c+0}'

echo
echo "=== Examples: [2,4)% mcap growth, no momentum_good in line (borderline vs 4% gate) ==="
grep -v '^??|' "$mf" | awk -F'|' '$1!="nan" && $1+0>=2 && $1+0<4 && $2==0 {print}' | tail -8

echo
echo "=== Examples: last [SCORE] with momentum_good ==="
grep -F '[SCORE]' "$tmp" | grep -F 'momentum_good' | tail -5

echo
echo "=== Examples: last require_momentum_good skips ==="
grep -F 'skipped (live): require_momentum_good' "$tmp" | tail -6

echo
echo "=== Examples: last tier=Skip [SCORE] (truncated) ==="
grep -F '[SCORE]' "$tmp" | grep -F 'tier=Skip' | tail -4 | while IFS= read -r L; do echo "${L:0:280}"; done
