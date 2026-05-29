#!/usr/bin/env bash
set -u
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
[ -z "$WD" ] && WD=/root/automata-build
DBURL=""
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
           | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }

echo "=== stored checksums (v11..v13) ==="
psql "$DBURL" -X -P pager=off -c "SELECT version, encode(checksum,'hex') AS checksum, success FROM _sqlx_migrations WHERE version BETWEEN 11 AND 13 ORDER BY version;"

F="/root/automata-build/migrations/0012_creator_stats_indexes.sql"
echo
echo "=== build-dir file: $F ==="
echo "exists: $([ -f "$F" ] && echo yes || echo NO)"
echo "sha384 (file bytes): $(sha384sum "$F" 2>/dev/null | awk '{print $1}')"
echo "CRLF present: $(grep -lU $'\r' "$F" >/dev/null 2>&1 && echo YES || echo no)"
echo "byte size: $(wc -c < "$F" 2>/dev/null)"
echo "--- head -c 200 | od ---"
head -c 200 "$F" | od -c | tail -6
