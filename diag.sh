#!/bin/bash
set -e

SINCE='2026-05-13 05:14:46'
LOG=/tmp/diag.log
journalctl -u loggaper --since "$SINCE" --no-pager -o cat > "$LOG"

echo "=== aggregate counts ==="
total_creates=$(grep -c '^created ' "$LOG" || true)
no_hist=$(grep -c 'no creator history' "$LOG" || true)
rejected=$(grep -c 'rejected by creator_config' "$LOG" || true)
score_lines=$(grep -c '^\[SCORE\]' "$LOG" || true)
buy_lines=$(grep -c '^\[BUY\]' "$LOG" || true)
sell_lines=$(grep -c '^\[SELL\]' "$LOG" || true)
strat_lines=$(grep -c '^\[STRATEGY\]' "$LOG" || true)
echo "creates=$total_creates  no_history=$no_hist  creator_config_rejected=$rejected  score_engine_runs=$score_lines  buys=$buy_lines  sells=$sell_lines  strategy_blocks=$strat_lines"

echo
echo "=== SCORE tier distribution ==="
grep '^\[SCORE\]' "$LOG" | sed -E 's/.*tier=([A-Za-z]+).*/\1/' | sort | uniq -c

echo
echo "=== SCORE score distribution ==="
grep '^\[SCORE\]' "$LOG" | sed -E 's/.*score=(-?[0-9]+).*/\1/' | sort -n | uniq -c

echo
echo "=== All SCORE events (oldest -> newest) ==="
grep '^\[SCORE\]' "$LOG" | tail -50

echo
echo "=== Rejected dev stats distribution ==="
grep 'FILTER.* stats:' "$LOG" | sed -E 's/.*coins=([0-9]+) pnl=(-?[0-9.]+)% holders=([0-9]+).*/\1 \2 \3/' > /tmp/devstats.tsv
n=$(wc -l /tmp/devstats.tsv | awk '{print $1}')
echo "n_with_history=$n"
if [ "$n" -gt 0 ]; then
  echo "coins:"
  awk '{print $1}' /tmp/devstats.tsv | sort -n | awk -v c=0 'BEGIN{c=0;sum=0}{c++;sum+=$1;v[c]=$1}END{if(c>0)print "  n="c" mean="(sum/c)" min="v[1]" p25="v[int(c*0.25)]" p50="v[int(c*0.50)]" p75="v[int(c*0.75)]" p90="v[int(c*0.90)]" max="v[c]}'
  echo "pnl%:"
  awk '{print $2}' /tmp/devstats.tsv | sort -n | awk 'BEGIN{c=0;sum=0}{c++;sum+=$1;v[c]=$1}END{if(c>0)print "  n="c" mean="(sum/c)" min="v[1]" p25="v[int(c*0.25)]" p50="v[int(c*0.50)]" p75="v[int(c*0.75)]" p90="v[int(c*0.90)]" max="v[c]}'
  echo "holders:"
  awk '{print $3}' /tmp/devstats.tsv | sort -n | awk 'BEGIN{c=0;sum=0}{c++;sum+=$1;v[c]=$1}END{if(c>0)print "  n="c" mean="(sum/c)" min="v[1]" p25="v[int(c*0.25)]" p50="v[int(c*0.50)]" p75="v[int(c*0.75)]" p90="v[int(c*0.90)]" max="v[c]}'
fi

echo
echo "=== How many rejected devs were close to passing creator_config ==="
echo "creator_config requires (from yaml):  median_mcap>=90, pnl>=10, holders>=50, volume>=90, trades>=50"
echo "Note: dev_stats log only shows coins/pnl/holders — using those three."
if [ "$n" -gt 0 ]; then
  c_pnl10=$(awk '$2>=10' /tmp/devstats.tsv | wc -l)
  c_h50=$(awk '$3>=50' /tmp/devstats.tsv | wc -l)
  c_both_strict=$(awk '$2>=10 && $3>=50' /tmp/devstats.tsv | wc -l)
  c_relaxed1=$(awk '$2>=10 && $3>=40' /tmp/devstats.tsv | wc -l)
  c_relaxed2=$(awk '$2>=5  && $3>=30' /tmp/devstats.tsv | wc -l)
  c_relaxed3=$(awk '$2>=0  && $3>=30' /tmp/devstats.tsv | wc -l)
  c_relaxed4=$(awk '$2>=10 && $3>=20' /tmp/devstats.tsv | wc -l)
  echo "  pnl>=10            : $c_pnl10 / $n"
  echo "  holders>=50        : $c_h50 / $n"
  echo "  pnl>=10 & h>=50    : $c_both_strict / $n  (current pre-gate)"
  echo "  pnl>=10 & h>=40    : $c_relaxed1 / $n"
  echo "  pnl>=10 & h>=20    : $c_relaxed4 / $n"
  echo "  pnl>=5  & h>=30    : $c_relaxed2 / $n"
  echo "  pnl>=0  & h>=30    : $c_relaxed3 / $n"
fi

echo
echo "=== Top dev_stats lines closest to threshold (pnl>=8, h>=40) ==="
grep 'FILTER.* stats:' "$LOG" | awk -F'pnl=|% holders=| stats:' '{print $0}' | head -1 >/dev/null
grep 'FILTER.* stats:' "$LOG" | awk -F'pnl=' '{print $2}' | sed -E 's/% holders=/ /;s/ .*$//' >/dev/null
grep 'FILTER.* stats:' "$LOG" | grep -E 'pnl=([1-9][0-9]?|[8-9])\.[0-9]+%' | head -20

echo
echo "=== bot-level metrics history (per 30s, last 20) ==="
grep 'metrics:bot' "$LOG" | tail -20
