#!/usr/bin/env bash
# What happened to the 6 continuation-skipped mints AFTER the skip?
# Pulls the full mcap trajectory from trades (currency='sol') + coin_mcap_tape.
# Read-only.
set -u

PID="$(systemctl show loggaper -p MainPID --value 2>/dev/null)"
DBURL=""
if [ -n "$PID" ] && [ -r "/proc/$PID/environ" ]; then
  DBURL="$(tr '\0' '\n' < /proc/$PID/environ | sed -n 's/^DATABASE_URL=//p')"
fi
if [ -z "$DBURL" ]; then
  WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
  for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
    [ -f "$f" ] || continue
    DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
             | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
    [ -n "$DBURL" ] && break
  done
fi
if [ -z "$DBURL" ]; then echo "[FAIL] DATABASE_URL not found"; exit 1; fi
PSQL="psql $DBURL -X -q -P pager=off"

MINTS=(
  "EL1Ts6C9WeakCan8vTEfm3cP6ggf8A6S7aaoZbMPpump"
  "7Tx2GZXq9AL4XMkFiGwbBbAniE8rSLER2XJDRYN1pump"
  "8kr7cmLBfuKBNjA11kSVLdCSVfvw59pD3GFM4hgjpump"
  "fFca1EipR4BT91GdARudh49sE7BBdxBmUmV62jEpump"
  "5YuKy8J7KkbpWUu4Adh1du2wbBfQqUw5t3zgkcLpump"
  "Bred5sR4UZjzKJcfvxLKM56YcUNCg6m7HxVa1zfmpump"
)

# detect slot_time unit (sec vs ms)
echo "=== slot_time sample (unit detect) ==="
$PSQL -c "SELECT max(slot_time) AS max_slot_time FROM trades;"

for m in "${MINTS[@]}"; do
  echo
  echo "################################################################"
  echo "MINT: $m"
  echo "----- trades (currency='sol') summary -----"
  $PSQL -c "
    SELECT
      count(*)                                            AS sol_trades,
      count(*) FILTER (WHERE is_buy)                      AS buys,
      count(*) FILTER (WHERE NOT is_buy)                  AS sells,
      round(min(market_cap::numeric),1)                   AS min_mcap,
      round(max(market_cap::numeric),1)                   AS peak_mcap,
      round((array_agg(market_cap::numeric ORDER BY slot_time DESC, id DESC))[1],1) AS last_mcap,
      min(slot_time)                                      AS first_slot,
      max(slot_time)                                      AS last_slot
    FROM trades
    WHERE coin_address='$m' AND currency='sol';"

  echo "----- mcap tape (dashboard samples) -----"
  $PSQL -c "
    SELECT count(*) AS tape_pts,
           round(min(mcap_sol)::numeric,1) AS min_mcap,
           round(max(mcap_sol)::numeric,1) AS peak_mcap,
           round((array_agg(mcap_sol ORDER BY ts_unix DESC))[1]::numeric,1) AS last_mcap,
           to_timestamp(min(ts_unix)) AT TIME ZONE 'UTC' AS first_ts_utc,
           to_timestamp(max(ts_unix)) AT TIME ZONE 'UTC' AS last_ts_utc
    FROM coin_mcap_tape WHERE coin_address='$m';"
done
echo
echo "=== DONE ==="
