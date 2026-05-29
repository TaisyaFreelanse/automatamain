#!/usr/bin/env bash
# EXPLAIN ANALYZE the creator-stats query for a *typical* dev (handful of coins),
# which is the common buy-path case, to confirm the new partial index is used and
# the lookup is ms-level.
set -u
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
DBURL=""
for f in "$WD/.env" /home/automata/.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }
PSQL="psql $DBURL -X -q -P pager=off"

# Distribution of coins-per-dev (context).
echo "=== coins-per-dev distribution ==="
$PSQL -c "SELECT width_bucket(c, 1, 100, 10) AS bucket, count(*) AS devs, min(c) AS min_coins, max(c) AS max_coins
          FROM (SELECT developer, count(*) c FROM coins GROUP BY developer) s
          GROUP BY 1 ORDER BY 1;"

# Pick a representative mid dev: between 8 and 25 coins.
MID_DEV="$($PSQL -t -A -c "SELECT developer FROM coins GROUP BY developer HAVING count(*) BETWEEN 8 AND 25 ORDER BY count(*) DESC LIMIT 1;")"
NCO="$($PSQL -t -A -c "SELECT count(*) FROM coins WHERE developer='$MID_DEV';")"
echo "mid dev=$MID_DEV ($NCO coins)"

run_b(){
$PSQL -c "EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH creator_coins AS (SELECT coin_address FROM coins WHERE developer = '$MID_DEV'),
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
  COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY ath_market_cap),0.0) AS m1,
  COALESCE((SELECT AVG(pnl) FROM trader_last_trade),0.0) AS m2,
  COALESCE(AVG(unique_buy_wallets::double precision),0.0) AS m3,
  COALESCE(AVG(COALESCE(volume,0.0)),0.0) AS m4,
  COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_trades),0.0) AS m5,
  COALESCE(AVG(unique_buy_wallets::double precision / NULLIF(unique_sell_wallets::double precision,0.0)),0.0) AS m6,
  COALESCE(AVG(COALESCE(avg_buy_size,0.0)),0.0) AS m7,
  (SELECT COUNT(*) FROM creator_coins) AS total_coins
FROM token_stats;"
}

echo; echo "=== mid-dev creator query WITH new index ==="
run_b | grep -E 'Execution Time|idx_trades_coin_sol_regular|Seq Scan on trades|Index.*Scan|Nested Loop'

echo; echo "=== same query, index DISABLED (baseline, enable_indexscan off would not isolate; use SET enable_seqscan) ==="
$PSQL -c "SET LOCAL enable_bitmapscan=on;" >/dev/null 2>&1
echo "(planner free choice above)"
