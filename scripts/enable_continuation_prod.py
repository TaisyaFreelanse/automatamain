#!/usr/bin/env python3
"""Idempotently inject/enable the scoring.continuation block in prod filter_config.yaml.

Text insertion only (no YAML round-trip) so existing formatting/comments are preserved.
Inserts the block right before the first top-level-of-scoring `  weights:` line.
"""
import shutil
import sys
import time

PATH = "/home/automata/filter_config.yaml"
BLOCK = """  continuation:
    enabled: true
    confirm_window_ms: 1500
    confirm_slices: 2
    min_upticks: 1
    min_new_unique_buyers: 1
    max_b2s_drop_ratio: 0.6
    max_sell_absorption_ratio: 1.5
    min_buys_per_sec: 0.0
"""

with open(PATH, "r") as f:
    lines = f.readlines()

if any(ln.strip() == "continuation:" for ln in lines):
    print("continuation block already present; leaving as-is")
    sys.exit(0)

# find the scoring section, then the first `  weights:` after it
insert_at = None
in_scoring = False
for i, ln in enumerate(lines):
    if ln.rstrip() == "scoring:":
        in_scoring = True
        continue
    if in_scoring and ln.rstrip() == "  weights:":
        insert_at = i
        break

if insert_at is None:
    print("ERROR: could not locate scoring -> weights anchor; no change made")
    sys.exit(1)

backup = f"{PATH}.bak.continuation.{int(time.time())}"
shutil.copy2(PATH, backup)

new_lines = lines[:insert_at] + [BLOCK] + lines[insert_at:]
with open(PATH, "w") as f:
    f.writelines(new_lines)

print(f"inserted continuation block before line {insert_at + 1}; backup: {backup}")
