#!/usr/bin/env python3
"""One-off ops report: bot_trades WR + learning_* tables. Run on server with cwd /home/automata."""
from __future__ import annotations

import datetime as dt
import pathlib
import subprocess
import sys


def load_database_url(env_path: pathlib.Path) -> str:
    text = env_path.read_text(errors="ignore")
    for line in text.splitlines():
        line = line.split("#")[0].strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == "DATABASE_URL":
            return v.strip().strip('"').strip("'")
    sys.exit("DATABASE_URL not found in .env")


def main() -> None:
    env_path = pathlib.Path("/home/automata/.env")
    if not env_path.is_file():
        env_path = pathlib.Path(sys.argv[1])
    db = load_database_url(env_path)
    # Deploy ~ 2026-05-17 06:30 UTC (adjust if you redeploy)
    cut = int(dt.datetime(2026, 5, 17, 6, 28, 0, tzinfo=dt.timezone.utc).timestamp())
    if len(sys.argv) > 2:
        cut = int(sys.argv[2])

    queries: list[tuple[str, str]] = [
        (
            "bot_trades before vs after cutoff",
            f"""
SELECT CASE WHEN closed_at < {cut} THEN 'before' ELSE 'after' END AS period,
       COUNT(*) AS n,
       ROUND(AVG(realized_pnl_pct)::numeric, 2) AS avg_pnl_pct,
       ROUND((SUM(CASE WHEN realized_pnl_pct > 0 THEN 1 ELSE 0 END)::float
              / NULLIF(COUNT(*), 0))::numeric, 3) AS winrate
FROM bot_trades GROUP BY 1 ORDER BY 1;
""",
        ),
        (
            "bot_trades close_reason after cutoff",
            f"""
SELECT close_reason, COUNT(*) AS n
FROM bot_trades WHERE closed_at >= {cut}
GROUP BY 1 ORDER BY n DESC;
""",
        ),
        (
            "bot_trades close_reason before cutoff (top 20)",
            f"""
SELECT close_reason, COUNT(*) AS n
FROM bot_trades WHERE closed_at < {cut}
GROUP BY 1 ORDER BY n DESC LIMIT 20;
""",
        ),
        (
            "learning_trades after cutoff",
            f"""
SELECT COUNT(*) AS n,
       MIN(closed_at) AS first_ts,
       MAX(closed_at) AS last_ts
FROM learning_trades WHERE closed_at >= {cut};
""",
        ),
        (
            "learning_trades last 12",
            """
SELECT id, mint, tier, score_total,
       ROUND(pnl_sol_pct::numeric, 2) AS pnl_pct,
       close_reason, hold_time_secs, closed_at
FROM learning_trades ORDER BY id DESC LIMIT 12;
""",
        ),
        (
            "learning_skipped by stage after cutoff",
            f"""
SELECT stage, COUNT(*) AS n
FROM learning_skipped WHERE created_at >= {cut}
GROUP BY 1 ORDER BY n DESC;
""",
        ),
        (
            "learning_skipped stage+reason after cutoff (top 20)",
            f"""
SELECT stage, reason, COUNT(*) AS n
FROM learning_skipped WHERE created_at >= {cut}
GROUP BY 1, 2 ORDER BY n DESC LIMIT 20;
""",
        ),
        (
            "learning_skipped last 12",
            """
SELECT id, mint, stage, reason, created_at
FROM learning_skipped ORDER BY id DESC LIMIT 12;
""",
        ),
    ]

    for title, q in queries:
        print(f"=== {title} ===")
        r = subprocess.run(
            ["psql", db, "-v", "ON_ERROR_STOP=1", "-c", q],
            capture_output=True,
            text=True,
        )
        print(r.stdout)
        if r.stderr:
            print(r.stderr, file=sys.stderr)
        if r.returncode != 0:
            raise SystemExit(r.returncode)

    since = "2026-05-17 06:28:00"
    print(f"=== journalctl loggaper since {since} (flow / tiers / exits) ===")
    j = subprocess.run(
        ["journalctl", "-u", "loggaper", "--since", since, "--no-pager"],
        capture_output=True,
        text=True,
    )
    if j.returncode != 0:
        print(j.stderr, file=sys.stderr)
        raise SystemExit(j.returncode)
    lines = j.stdout.splitlines()
    text = j.stdout

    def cnt(sub: str) -> int:
        return text.count(sub)

    score_lines = [ln for ln in lines if "[SCORE]" in ln]
    tier_aplus = sum(1 for ln in score_lines if "tier=APlus" in ln)
    tier_skip = sum(1 for ln in score_lines if "tier=Skip" in ln)
    tier_a = sum(
        1 for ln in score_lines if "tier=A" in ln and "tier=APlus" not in ln
    )

    print(f"raw_lines: {len(lines)}")
    print(f"[SCORE] lines: {len(score_lines)}")
    print(f"  tier=APlus (on [SCORE] lines): {tier_aplus}")
    print(f"  tier=A       (on [SCORE] lines): {tier_a}")
    print(f"  tier=Skip    (on [SCORE] lines): {tier_skip}")
    print(f"[BUY] Opened: {cnt('[BUY] Opened')}")
    print(f"[BUY GATE]: {cnt('[BUY GATE]')}")
    print(f"[BUY] skipped live momentum: {cnt('skipped (live): require_momentum_good')}")
    print(f"[BUY] skipped live APlus gate: {cnt('minimum_tier_for_buy=APlus')}")
    print(f"[BUY] skipped tier size: {cnt('skipped: tier size')}")
    print(f"[SELL] lines: {cnt('[SELL]')}")
    print(f"SELL reason=TP1: {cnt('reason=TP1')}")
    print(f"SELL reason=TP2: {cnt('reason=TP2')}")
    print(f"TIME KILL (log line): {cnt('TIME KILL')}")
    print(f"SL floor (log line): {cnt('SL (floor')}")
    print(f"TRAILING EXIT: {cnt('TRAILING EXIT')}")


if __name__ == "__main__":
    main()
