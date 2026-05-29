#!/usr/bin/env bash
# One-off VACUUM (ANALYZE) on trades to refresh the visibility map so the new
# covering index can do true index-only scans (cut the ~30k heap fetches on the
# creator-stats query). Online, no exclusive lock. Append-only table -> stays
# mostly all-visible afterwards; autovacuum maintains it.
set -u
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
DBURL=""
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
           | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }
echo "=== VACUUM (ANALYZE) trades $(date -u '+%T')Z ==="
psql "$DBURL" -X -q -P pager=off -c "VACUUM (ANALYZE, VERBOSE) trades;" 2>&1 | tail -8
echo "=== done $(date -u '+%T')Z ==="
