#!/usr/bin/env bash
# EXPLAIN ANALYZE for the creator/dev stat queries that show up as slow in logs.
# Reads DATABASE_URL from the running loggaper process env. Read-only.
set -u

PID="$(systemctl show loggaper -p MainPID --value 2>/dev/null)"
DBURL=""
if [ -n "$PID" ] && [ -r "/proc/$PID/environ" ]; then
  DBURL="$(tr '\0' '\n' < /proc/$PID/environ | sed -n 's/^DATABASE_URL=//p')"
fi
# dotenv-loaded vars are not in /proc/environ; fall back to .env files.
if [ -z "$DBURL" ]; then
  WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
  for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
    [ -f "$f" ] || continue
    # handles: DATABASE_URL=..., export DATABASE_URL = "...", with/without quotes
    DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" \
             | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
    [ -n "$DBURL" ] && echo "[ok] DATABASE_URL from $f" && break
  done
fi
if [ -z "$DBURL" ]; then
  echo "[FAIL] DATABASE_URL not found in process env or .env files"; exit 1
fi
echo "[ok] DATABASE_URL resolved; PID=$PID"
PSQL="psql $DBURL -X -q -P pager=off"

echo; echo "=== table sizes ==="
$PSQL -c "SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(relid)) AS total
          FROM pg_stat_user_tables WHERE relname IN ('trades','coins','traders','developers')
          ORDER BY n_live_tup DESC;"

echo; echo "=== existing indexes on trades/coins ==="
$PSQL -c "SELECT tablename, indexname, indexdef FROM pg_indexes
          WHERE tablename IN ('trades','coins') ORDER BY tablename, indexname;"

echo; echo "=== heaviest trader_address (most rows) ==="
HV_TRADER="$($PSQL -t -A -c "SELECT trader_address FROM trades GROUP BY trader_address ORDER BY count(*) DESC LIMIT 1;")"
echo "heaviest trader=$HV_TRADER ($($PSQL -t -A -c "SELECT count(*) FROM trades WHERE trader_address='$HV_TRADER';") rows)"

echo; echo "=== heaviest developer (most coins) ==="
HV_DEV="$($PSQL -t -A -c "SELECT developer FROM coins GROUP BY developer ORDER BY count(*) DESC LIMIT 1;")"
echo "heaviest dev=$HV_DEV ($($PSQL -t -A -c "SELECT count(*) FROM coins WHERE developer='$HV_DEV';") coins)"

echo; echo "############ QUERY A: get_trader_stats (slow-log query) ############"
$PSQL -c "EXPLAIN (ANALYZE, BUFFERS, TIMING) 
  SELECT SUM((pnl > 0)::int)::float8 / NULLIF(COUNT(pnl), 0) AS winrate,
         COUNT(pnl) AS total_trades, MAX(pnl)::float8 AS best_pnl,
         MIN(pnl)::float8 AS worst_pnl, MIN(slot_time) AS active_from
  FROM trades WHERE trader_address = '$HV_TRADER' AND pnl IS NOT NULL;"

echo; echo "############ QUERY B: get_creator_stats_in_sol (buy-path pre-gate) ############"
$PSQL -c "EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH creator_coins AS (SELECT coin_address FROM coins WHERE developer = '$HV_DEV'),
token_stats AS (
  SELECT cc.coin_address, MAX(t.market_cap::double precision) AS ath_market_cap,
         SUM(t.size::double precision) AS volume, COUNT(*) AS total_trades,
         COUNT(DISTINCT t.trader_address) FILTER (WHERE t.is_buy) AS unique_buy_wallets,
         COUNT(DISTINCT t.trader_address) FILTER (WHERE NOT t.is_buy) AS unique_sell_wallets,
         AVG(t.size::double precision) FILTER (WHERE t.is_buy) AS avg_buy_size
  FROM creator_coins cc
  LEFT JOIN trades t ON t.coin_address = cc.coin_address AND t.currency='sol' AND t.role='regular'
  GROUP BY cc.coin_address),
trader_last_trade AS (
  SELECT DISTINCT ON (t.trader_address) t.trader_address, t.pnl::double precision AS pnl
  FROM trades t JOIN creator_coins cc ON cc.coin_address = t.coin_address
  WHERE t.role='regular' AND t.currency='sol'
  ORDER BY t.trader_address, t.slot_time DESC, t.id DESC)
SELECT
  COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY ath_market_cap),0.0) AS median_market_cap,
  COALESCE((SELECT AVG(pnl) FROM trader_last_trade),0.0) AS trader_pnl_average,
  COALESCE(AVG(unique_buy_wallets::double precision),0.0) AS total_holders_average,
  COALESCE(AVG(COALESCE(volume,0.0)),0.0) AS average_volume,
  COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_trades),0.0) AS median_total_trades,
  COALESCE(AVG(unique_buy_wallets::double precision / NULLIF(unique_sell_wallets::double precision,0.0)),0.0) AS avg_b2s,
  COALESCE(AVG(COALESCE(avg_buy_size,0.0)),0.0) AS average_buy_trader_size,
  (SELECT COUNT(*) FROM creator_coins) AS total_coins
FROM token_stats;"

echo; echo "=== DONE ==="
