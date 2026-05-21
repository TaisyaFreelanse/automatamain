#!/usr/bin/env python3
"""Cross-check [SCORE] mcap_init/peak vs momentum_good in items (legacy engine)."""
import re
import subprocess
import sys

LOW, HIGH, OH = 3.0, 30.0, 55.0
since = sys.argv[1] if len(sys.argv) > 1 else "24 hours ago"

raw = subprocess.check_output(
    ["journalctl", "-u", "loggaper", "--since", since, "--no-pager", "-o", "cat"],
    text=True,
    errors="replace",
)
lines = [l for l in raw.splitlines() if "[SCORE]" in l]
pat = re.compile(
    r"^.*\[SCORE\] (\S+) tier=(\w+) .*mcap_init=([0-9.]+) mcap_peak=([0-9.]+).*items=\[(.*)\]"
)

print(f"=== momentum validation (since {since}) ===")
print(f"band [{LOW}, {HIGH}]%, overheated >= {OH}% (legacy_scoring order)\n")
mismatches = 0
for l in lines:
    m = pat.match(l)
    if not m:
        print("PARSE_FAIL:", l[:100])
        mismatches += 1
        continue
    mint, tier, ini_s, peak_s, items = m.groups()
    ini, peak = float(ini_s), float(peak_s)
    mom = (peak / ini - 1.0) * 100.0 if ini > 0 else 0.0
    has_mg = '("momentum_good"' in items
    has_oh = "momentum_overheated" in items

    if mom >= OH:
        expect_mg, expect_oh = False, True
    elif LOW <= mom <= HIGH:
        expect_mg, expect_oh = True, False
    else:
        expect_mg, expect_oh = False, False

    ok = (has_mg == expect_mg) and (has_oh == expect_oh)
    if not ok:
        mismatches += 1
        flag = "MISMATCH"
    else:
        flag = "ok"
    print(
        f"{flag} {mint[:16]:16} tier={tier:5} mom={mom:6.2f}% "
        f"mg={has_mg} oh={has_oh} exp_mg={expect_mg} exp_oh={expect_oh}"
    )

print(f"\nSCORE lines: {len(lines)}, mismatches: {mismatches}")

skips = [l for l in raw.splitlines() if "require_momentum_good" in l]
gates = [l for l in raw.splitlines() if "[BUY GATE]" in l]
print(f"\nrequire_momentum skips: {len(skips)}")
print(f"BUY GATE: {len(gates)}")

# Gate consistency: every skip should be tier A/APlus without momentum_good in prior SCORE
score_by_mint = {}
for l in lines:
    m = pat.match(l)
    if m:
        score_by_mint[m.group(1)] = m.group(5)

gate_ok = 0
gate_bad = 0
for l in skips:
    mint = l.split("[BUY]")[1].split()[0]
    items = score_by_mint.get(mint, "")
    if "momentum_good" in items:
        print(f"GATE_BUG skip but SCORE had momentum_good: {mint[:20]}")
        gate_bad += 1
    else:
        gate_ok += 1

for l in gates:
    mint = l.split("[BUY GATE]")[1].split()[0]
    items = score_by_mint.get(mint, "")
    if "momentum_good" not in items:
        print(f"GATE_BUG BUY without momentum_good: {mint[:20]}")
        gate_bad += 1
    else:
        gate_ok += 1

print(f"gate checks ok={gate_ok} bad={gate_bad}")
