#!/usr/bin/env bash
# Profile the creator-stats query: existing indexes, a representative mid-size dev
# (the ones that still hit 1.6-3.4s), and EXPLAIN ANALYZE of both heavy CTEs.
# Read-only.
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
PSQL="psql $DBURL -X -q -P pager=off"

echo "=== existing indexes on trades ==="
$PSQL -c "SELECT indexname, indexdef FROM pg_indexes WHERE tablename='trades' ORDER BY indexname;"

echo
echo "=== indexes on coins ==="
$PSQL -c "SELECT indexname, indexdef FROM pg_indexes WHERE tablename='coins' ORDER BY indexname;"

echo
echo "=== pick a representative dev: 20..100 coins, most sol/regular trades ==="
DEV="$($PSQL -t -A -c "
  WITH d AS (
    SELECT developer, count(*) AS coins
    FROM coins WHERE developer IS NOT NULL
    GROUP BY developer HAVING count(*) BETWEEN 20 AND 100
  )
  SELECT d.developer
  FROM d
  JOIN coins c ON c.developer = d.developer
  JOIN trades t ON t.coin_address = c.coin_address AND t.currency='sol' AND t.role='regular'
  GROUP BY d.developer
  ORDER BY count(*) DESC
  LIMIT 1;")"
echo "dev = $DEV"
[ -z "$DEV" ] && { echo "[WARN] no dev found in range"; exit 0; }

echo
echo "=== EXPLAIN ANALYZE: full creator-stats query ==="
$PSQL -c "EXPLAIN (ANALYZE, BUFFERS, TIMING)
WITH creator_coins AS (
    SELECT coin_address FROM coins WHERE developer = '$DEV'
),
token_stats AS (
    SELECT cc.coin_address,
        MAX(t.market_cap::double precision) AS ath_market_cap,
        SUM(t.size::double precision) AS volume,
        COUNT(*) AS total_trades,
        COUNT(DISTINCT t.trader_address) FILTER (WHERE t.is_buy) AS unique_buy_wallets,
        COUNT(DISTINCT t.trader_address) FILTER (WHERE NOT t.is_buy) AS unique_sell_wallets,
        AVG(t.size::double precision) FILTER (WHERE t.is_buy) AS avg_buy_size
    FROM creator_coins cc
    LEFT JOIN trades t ON t.coin_address = cc.coin_address AND t.currency='sol' AND t.role='regular'
    GROUP BY cc.coin_address
),
trader_last_trade AS (
    SELECT DISTINCT ON (t.trader_address) t.trader_address, t.pnl::double precision AS pnl
    FROM trades t JOIN creator_coins cc ON cc.coin_address = t.coin_address
    WHERE t.role='regular' AND t.currency='sol'
    ORDER BY t.trader_address, t.slot_time DESC, t.id DESC
)
SELECT
  COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY ath_market_cap),0.0),
  COALESCE((SELECT AVG(pnl) FROM trader_last_trade),0.0),
  COALESCE(AVG(unique_buy_wallets::double precision),0.0),
  COALESCE(AVG(COALESCE(volume,0.0)),0.0),
  COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_trades),0.0),
  COALESCE(AVG(unique_buy_wallets::double precision/NULLIF(unique_sell_wallets::double precision,0.0)),0.0),
  COALESCE(AVG(COALESCE(avg_buy_size,0.0)),0.0),
  (SELECT COUNT(*) FROM creator_coins)
FROM token_stats;"

echo
echo "=== DONE ==="
