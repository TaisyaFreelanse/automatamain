#!/usr/bin/env bash
# Counterfactual: fill_mcap / score_mcap from journal SCORE + BUY Opened lines.
set -euo pipefail
MAX_RATIO="${1:-1.5}"
SINCE="${2:-2026-05-19 00:00:00}"

tmp=$(mktemp)
journalctl -u loggaper --no-pager -S "$SINCE" -o cat 2>/dev/null >"$tmp"

python3 <<PY
import re
from pathlib import Path

text = Path("$tmp").read_text(errors="replace")
score_re = re.compile(
    r"\[SCORE\] (\S+) tier=(\S+) score=\d+ .* mcap_init=([\d.]+) "
)
buy_re = re.compile(
    r"\[BUY\] Opened (\S+) \| mcap=([\d.]+) SOL(?: \(on-chain fill\))? \|"
)

scores = {}
for m in score_re.finditer(text):
    scores[m.group(1)] = float(m.group(3))

rows = []
for m in buy_re.finditer(text):
    mint, fill = m.group(1), float(m.group(2))
    on_chain = "(on-chain fill)" in m.group(0)
    score = scores.get(mint)
    if score is None or score <= 0:
        continue
    ratio = fill / score
    rows.append((mint, score, fill, ratio, on_chain))

max_ratio = float("$MAX_RATIO")
would_abort = [r for r in rows if r[3] > max_ratio and r[4]]
on_chain_buys = [r for r in rows if r[4]]

print(f"Since: $SINCE")
print(f"Threshold: fill/score > {max_ratio}")
print(f"BUY with score snapshot: {len(rows)}")
print(f"BUY with on-chain fill mcap: {len(on_chain_buys)}")
print(f"Would abort (on-chain only): {len(would_abort)}")
if on_chain_buys:
    pct = 100.0 * len(would_abort) / len(on_chain_buys)
    print(f"Abort rate (on-chain buys): {pct:.1f}%")
print()
if would_abort:
    print("Would abort (ratio > {:.2f}):".format(max_ratio))
    for mint, score, fill, ratio, _ in sorted(would_abort, key=lambda x: -x[3]):
        print(f"  {ratio:.2f}x  score={score:.1f} fill={fill:.1f}  {mint}")
print()
passed = [r for r in on_chain_buys if r[3] <= max_ratio]
if passed:
    print("Would pass guard:")
    for mint, score, fill, ratio, _ in sorted(passed, key=lambda x: x[3]):
        print(f"  {ratio:.2f}x  score={score:.1f} fill={fill:.1f}  {mint}")
PY
rm -f "$tmp"
