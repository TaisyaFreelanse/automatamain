#!/usr/bin/env python3
"""Parse journalctl for Create → filter → SCORE → BUY funnel (post-V2 sanity)."""
from __future__ import annotations

import re
import subprocess
import sys
from collections import Counter

# journal: ... loggaper[PID]: created MINT
RE_CREATED = re.compile(r"loggaper\[\d+\]:\s+created\s+(\S+)")
RE_SCORE = re.compile(
    r"\[SCORE\]\s+(\S+)\s+tier=(\w+)\s+score=(\d+)"
)
RE_BUY_GATE = re.compile(r"\[BUY GATE\]\s+(\S+)\s+tier=(\w+)")
RE_BUY_OPENED = re.compile(r"\[BUY\]\s+Opened\s+(\S+)")
RE_FILTER_NH = re.compile(
    r"\[FILTER\]\s+(\S+)\s+skipped:\s+no creator history"
)
RE_FILTER_CR = re.compile(
    r"\[FILTER\]\s+(\S+)\s+rejected by creator_config"
)
RE_STRATEGY = re.compile(r"\[STRATEGY\]\s+(\S+)\s+blocked:")
RE_BUY_SKIP = re.compile(r"\[BUY\]\s+(\S+)\s+skipped")


def main() -> None:
    since = sys.argv[1] if len(sys.argv) > 1 else "2 hours ago"
    proc = subprocess.run(
        ["journalctl", "-u", "loggaper", "--since", since, "--no-pager"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        sys.exit(proc.returncode)
    lines = proc.stdout.splitlines()

    mints_created: list[str] = []
    created_set: set[str] = set()
    nh_set: set[str] = set()
    cr_set: set[str] = set()
    mints_nh: list[str] = []
    mints_cr: list[str] = []
    score_rows: list[tuple[str, str, int]] = []
    buy_gate_mints: list[str] = []
    opened: list[str] = []
    strategy_blocked: list[str] = []
    buy_skipped: list[str] = []

    for ln in lines:
        if m := RE_CREATED.search(ln):
            mm = m.group(1)
            mints_created.append(mm)
            created_set.add(mm)
        if m := RE_FILTER_NH.search(ln):
            x = m.group(1)
            mints_nh.append(x)
            nh_set.add(x)
        if m := RE_FILTER_CR.search(ln):
            x = m.group(1)
            mints_cr.append(x)
            cr_set.add(x)
        if m := RE_SCORE.search(ln):
            score_rows.append((m.group(1), m.group(2), int(m.group(3))))
        if m := RE_BUY_GATE.search(ln):
            buy_gate_mints.append(m.group(1))
        if m := RE_BUY_OPENED.search(ln):
            opened.append(m.group(1))
        if m := RE_STRATEGY.search(ln):
            strategy_blocked.append(m.group(1))
        if m := RE_BUY_SKIP.search(ln):
            buy_skipped.append(m.group(1))

    tier_c = Counter(t for _, t, _ in score_rows)
    score_mints = {m for m, _, _ in score_rows}

    print(f"=== journal window: --since {since!r} ===")
    print(f"log lines: {len(lines)}")
    print()
    print(f"creates (log lines 'created <mint>'): {len(mints_created)}")
    print(f"  unique mints created: {len(created_set)}")
    passed_set = created_set - nh_set - cr_set
    print(f"  unique mints not in nh/cr filter lines: {len(passed_set)}  (proxy: passed creator stage)")
    print()
    print(f"creator filter FAIL no_creator_history lines: {len(mints_nh)} (unique {len(nh_set)})")
    print(f"creator filter FAIL creator_config lines: {len(mints_cr)} (unique {len(cr_set)})")
    passed_est = len(mints_created) - len(mints_nh) - len(mints_cr)
    print(
        f"creator filter PASS (naive line diff): {passed_est}  "
        f"(use unique proxy above — preferred)"
    )
    print()
    print(f"DB errors (creator lookup): {sum(1 for ln in lines if '[FILTER] DB error' in ln)}")
    print(f"reached [SCORE] lines: {len(score_rows)}  (unique mints: {len(score_mints)})")
    print(f"  tier breakdown on [SCORE] lines: {dict(tier_c)}")
    print(f"  APlus: {tier_c.get('APlus', 0)}   A: {tier_c.get('A', 0)}   Skip: {tier_c.get('Skip', 0)}")
    print()
    print(f"[BUY GATE] lines: {len(buy_gate_mints)}")
    print(f"[BUY] Opened lines: {len(opened)}")
    print(f"[STRATEGY] blocked lines: {len(strategy_blocked)}")
    print(f"[BUY] ... skipped lines: {len(buy_skipped)}")
    print()

    # Near-miss examples: best Skip scores (last window)
    skips = [(m, t, s) for m, t, s in score_rows if t == "Skip"]
    skips.sort(key=lambda x: -x[2])
    print("=== near-miss: top Skip by score (mint, tier, score) ===")
    for row in skips[:8]:
        print(f"  {row[0]}  {row[1]}  score={row[2]}")

    # Tier A but no BUY GATE for same mint (in this window) — rough near-miss
    gated = set(buy_gate_mints)
    a_no_gate = [(m, s) for m, t, s in score_rows if t == "A" and m not in gated]
    a_no_gate.sort(key=lambda x: -x[1])
    print()
    print("=== near-miss: tier=A mints with no [BUY GATE] line in window ===")
    for m, s in a_no_gate[:8]:
        print(f"  {m}  score={s}")

    # Highest score Skip (already above)

    print()
    print("=== learning_* (psql, same wall window ~2h via created_at/closed_at) ===")
    try:
        import pathlib
        import time

        env_path = pathlib.Path("/home/automata/.env")
        if env_path.is_file():
            db = None
            for line in env_path.read_text(errors="ignore").splitlines():
                line = line.split("#")[0].strip()
                if not line or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() == "DATABASE_URL":
                    db = v.strip().strip('"').strip("'")
                    break
            if db:
                cut = int(time.time()) - 7200
                sql = f"""
SELECT stage, reason, COUNT(*) AS n
FROM learning_skipped WHERE created_at >= {cut}
GROUP BY stage, reason ORDER BY n DESC LIMIT 15;

SELECT tier, close_reason, COUNT(*) AS n
FROM learning_trades WHERE closed_at >= {cut}
GROUP BY tier, close_reason;

SELECT mint, stage, reason, score_total
FROM (
  SELECT mint, stage, reason,
         COALESCE((payload->>'score_total')::int, -1) AS score_total, id
  FROM learning_skipped
  WHERE created_at >= {cut} AND stage = 'score_skip'
) t
ORDER BY score_total DESC NULLS LAST, id DESC
LIMIT 6;
"""
                r = subprocess.run(
                    ["psql", db, "-c", sql],
                    capture_output=True,
                    text=True,
                )
                print(r.stdout)
                if r.stderr:
                    print(r.stderr, file=sys.stderr)
    except Exception as e:
        print(f"(skip DB) {e}")
    print()
    print("=== last [SCORE] lines (up to 6) ===")
    score_lines = [ln for ln in lines if "[SCORE]" in ln]
    for ln in score_lines[-6:]:
        # strip syslog prefix for readability
        idx = ln.find("loggaper[")
        short = ln[idx:] if idx >= 0 else ln
        print(short[:220])


if __name__ == "__main__":
    main()
