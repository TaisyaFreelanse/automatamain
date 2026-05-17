#!/usr/bin/env python3
"""Throughput report for one loggaper PID (fair A/B vs mixing journal PIDs).

Uses journalctl, then keeps only lines emitted by loggaper[PID] (default: current
MainPID from systemd). Parses funnel lines + first/last [metrics:bot] deltas.

Examples:
  sudo python3 scripts/report_throughput_ab.py
  sudo python3 scripts/report_throughput_ab.py --since '2026-05-17 16:39:40'
  sudo python3 scripts/report_throughput_ab.py --pid 3642449 --since '24 hours ago'
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import Counter

RE_CREATED = re.compile(r"loggaper\[\d+\]:\s+created\s+(\S+)")
RE_SCORE = re.compile(
    r"loggaper\[\d+\]:\s+\[SCORE\]\s+(\S+)\s+tier=(\w+)\s+score=(\d+)"
)
RE_BUY_GATE = re.compile(r"loggaper\[\d+\]:\s+\[BUY GATE\]\s+(\S+)\s+tier=(\w+)")
RE_BUY_OPENED = re.compile(r"loggaper\[\d+\]:\s+\[BUY\]\s+Opened\s+(\S+)")
RE_BROKER_BUY = re.compile(r"loggaper\[\d+\]:\s+\[BROKER BUY\]\s+(\S+):")
RE_FILTER_NH = re.compile(
    r"loggaper\[\d+\]:\s+\[FILTER\]\s+(\S+)\s+skipped:\s+no creator history"
)
RE_FILTER_CR = re.compile(
    r"loggaper\[\d+\]:\s+\[FILTER\]\s+(\S+)\s+rejected by creator_config"
)
RE_FILTER_DB = re.compile(r"loggaper\[\d+\]:\s+\[FILTER\]\s+DB error")
RE_METRICS = re.compile(
    r"\[metrics:bot\]\s+creates=(\d+)\s+no_history=(\d+)\s+filter_rejected=(\d+)\s+"
    r"passed_filter=(\d+)\s+score_skip=(\d+)\s+score_a=(\d+)\s+score_a_plus=(\d+)\s+"
    r"strategy_blocked=(\d+)\s+positions_initiated=(\d+)"
)
RE_BOOT_SCORING = re.compile(r"\[BOOT\]\s+scoring=(\S+)")


def systemd_main_pid(unit: str) -> int:
    r = subprocess.run(
        ["systemctl", "show", unit, "-p", "MainPID", "--value"],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        print(r.stderr, file=sys.stderr)
        sys.exit(r.returncode or 1)
    s = (r.stdout or "").strip()
    if not s or s == "0":
        print(f"error: {unit} MainPID is empty or 0 — is the service running?", file=sys.stderr)
        sys.exit(1)
    return int(s)


def journal_lines(unit: str, since: str) -> list[str]:
    proc = subprocess.run(
        ["journalctl", "-u", unit, "--since", since, "--no-pager"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        sys.exit(proc.returncode or 1)
    return proc.stdout.splitlines()


def filter_pid(lines: list[str], pid: int) -> list[str]:
    needle = f"loggaper[{pid}]:"
    return [ln for ln in lines if needle in ln]


def parse_metrics_line(line: str) -> tuple[int, ...] | None:
    m = RE_METRICS.search(line)
    if not m:
        return None
    return tuple(int(m.group(i)) for i in range(1, 10))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--unit", default="loggaper", help="systemd unit (default: loggaper)")
    ap.add_argument(
        "--pid",
        default="auto",
        help='process PID in log prefix loggaper[PID], or "auto" = systemctl MainPID (default)',
    )
    ap.add_argument(
        "--since",
        default="48 hours ago",
        help='journalctl --since (default: "48 hours ago"). Widen if this PID has no lines.',
    )
    args = ap.parse_args()

    pid = systemd_main_pid(args.unit) if args.pid == "auto" else int(args.pid)
    raw = journal_lines(args.unit, args.since)
    lines = filter_pid(raw, pid)

    print(f"=== throughput A/B report ===")
    print(f"unit={args.unit!r}  pid={pid}  journal --since {args.since!r}")
    print(f"raw journal lines: {len(raw)}  lines for loggaper[{pid}]: {len(lines)}")
    if not lines:
        print("\n(no lines for this PID in window — widen --since or check unit name)")
        sys.exit(2)

    boot_scoring = None
    for ln in lines:
        if m := RE_BOOT_SCORING.search(ln):
            boot_scoring = m.group(1)
            break
    if boot_scoring:
        print(f"boot scoring mode (first [BOOT] in window): {boot_scoring}")
    else:
        print("boot scoring mode: (no [BOOT] scoring= line in this PID slice)")

    mints_created: list[str] = []
    nh_set: set[str] = set()
    cr_set: set[str] = set()
    score_rows: list[tuple[str, str, int]] = []
    buy_gate: list[str] = []
    opened: list[str] = []
    broker_buy: list[str] = []

    for ln in lines:
        if m := RE_CREATED.search(ln):
            mints_created.append(m.group(1))
        if m := RE_FILTER_NH.search(ln):
            nh_set.add(m.group(1))
        if m := RE_FILTER_CR.search(ln):
            cr_set.add(m.group(1))
        if m := RE_SCORE.search(ln):
            score_rows.append((m.group(1), m.group(2), int(m.group(3))))
        if m := RE_BUY_GATE.search(ln):
            buy_gate.append(m.group(1))
        if m := RE_BUY_OPENED.search(ln):
            opened.append(m.group(1))
        if m := RE_BROKER_BUY.search(ln):
            broker_buy.append(m.group(1))

    created_set = set(mints_created)
    passed_proxy = created_set - nh_set - cr_set
    tier_c = Counter(t for _, t, _ in score_rows)
    scores = [s for _, _, s in score_rows]
    max_score = max(scores) if scores else None

    metrics_parsed: list[tuple[int, ...]] = []
    metrics_lines: list[str] = []
    for ln in lines:
        if "[metrics:bot]" in ln:
            p = parse_metrics_line(ln)
            if p:
                metrics_parsed.append(p)
                metrics_lines.append(ln)

    print()
    print("--- funnel (this PID only) ---")
    print(f"creates (log lines): {len(mints_created)}  unique mints: {len(created_set)}")
    print(f"creator FAIL no_history (unique mints): {len(nh_set)}")
    print(f"creator FAIL creator_config (unique mints): {len(cr_set)}")
    print(f"creator PASS proxy (unique created \\ nh \\ cr): {len(passed_proxy)}")
    print(f"[FILTER] DB error lines: {sum(1 for ln in lines if RE_FILTER_DB.search(ln))}")
    print(f"[SCORE] lines: {len(score_rows)}  unique mints: {len({m for m, _, _ in score_rows})}")
    print(f"  tier breakdown: {dict(tier_c)}  (APlus / A / Skip)")
    print(f"  max score: {max_score if max_score is not None else 'n/a'}")
    print(f"[BUY GATE] lines: {len(buy_gate)}")
    print(f"[BUY] Opened lines: {len(opened)}")
    print(f"[BROKER BUY] lines: {len(broker_buy)}")
    print()
    if scores:
        ge6 = sum(1 for s in scores if s >= 6)
        ge7 = sum(1 for s in scores if s >= 7)
        print(f"SCORE distribution: score>=6: {ge6}   score>=7: {ge7}")
    else:
        print("SCORE distribution: score>=6 / >=7: n/a (no SCORE lines)")

    print()
    print("--- [metrics:bot] (cumulative counters for this process) ---")
    if not metrics_parsed:
        print("(no [metrics:bot] lines in window for this PID)")
    else:
        labels = (
            "creates",
            "no_history",
            "filter_rejected",
            "passed_filter",
            "score_skip",
            "score_a",
            "score_a_plus",
            "strategy_blocked",
            "positions_initiated",
        )
        first = metrics_parsed[0]
        last = metrics_parsed[-1]
        print(f"samples: {len(metrics_parsed)}  (first → last in window)")
        for i, name in enumerate(labels):
            d = last[i] - first[i]
            print(f"  {name}: {first[i]} → {last[i]}  (Δ {d})")
        print()
        print("last raw metrics line:")
        tail = metrics_lines[-1]
        idx = tail.find("loggaper[")
        print(tail[idx:] if idx >= 0 else tail[-240:])

    print()
    print("--- last [SCORE] (up to 5, this PID) ---")
    score_lines = [ln for ln in lines if "[SCORE]" in ln]
    for ln in score_lines[-5:]:
        idx = ln.find("loggaper[")
        print((ln[idx:] if idx >= 0 else ln)[:260])


if __name__ == "__main__":
    main()
