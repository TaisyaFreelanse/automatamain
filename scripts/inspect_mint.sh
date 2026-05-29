#!/usr/bin/env bash
set -u
MINT="${1:?usage: inspect_mint.sh <mint>}"

PID="$(systemctl show loggaper -p MainPID --value 2>/dev/null)"
DBURL=""
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
           | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }
PSQL="psql $DBURL -X -q -P pager=off"

echo "=== bot_trades row ==="
$PSQL -c "SELECT id, round(entry_mcap_sol::numeric,1) entry_mcap, round(invested_sol::numeric,4) invested,
  round(exit_mcap_sol::numeric,1) exit_mcap, round(realized_pnl_pct::numeric,2) pnl_pct,
  close_reason, to_timestamp(entry_at) AT TIME ZONE 'UTC' entry_utc
  FROM bot_trades WHERE mint='$MINT' ORDER BY id DESC LIMIT 5;"

echo "=== mcap tape (post events) ==="
$PSQL -c "SELECT count(*) pts, round(min(mcap_sol)::numeric,1) min, round(max(mcap_sol)::numeric,1) peak,
  round((array_agg(mcap_sol ORDER BY ts_unix DESC))[1]::numeric,1) last,
  to_timestamp(min(ts_unix)) AT TIME ZONE 'UTC' first_ts,
  to_timestamp(max(ts_unix)) AT TIME ZONE 'UTC' last_ts
  FROM coin_mcap_tape WHERE coin_address='$MINT';"
