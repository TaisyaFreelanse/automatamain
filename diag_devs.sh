#!/bin/bash
set -e

DB_URL='postgres://postgres:v3rYS3curPassw0rd@localhost:5432/automata'

# Time range — last ~80 minutes by service uptime. created_at in coins is unix slot? Let me check.
psql "$DB_URL" -c "SELECT created_at FROM coins ORDER BY created_at DESC LIMIT 3;" 2>&1 | head -10

echo
echo "=== last created in coins ==="
psql "$DB_URL" -c "SELECT coin_address, developer, created_at FROM coins ORDER BY created_at DESC LIMIT 5;"

echo
echo "=== rows ==="
psql "$DB_URL" -c "SELECT count(*) AS total_coins, count(DISTINCT developer) AS total_devs FROM coins;"

# created_at is in slot (Solana slot). At current slot rate ~2.5/s, last 80 min = ~12000 slots.
# But simpler: just take last N coins by created_at and analyze their devs.

cat <<'SQL' > /tmp/dev_dist.sql
WITH recent_raw AS (
  SELECT developer, created_at
  FROM coins
  ORDER BY created_at DESC
  LIMIT 4000
),
recent_coins AS (
  SELECT DISTINCT developer FROM recent_raw
),
per_dev AS (
  SELECT
    rc.developer,
    cc.coin_address,
    MAX(t.market_cap::double precision) AS ath_mcap,
    SUM(t.size::double precision)       AS volume,
    COUNT(*)                            AS total_trades,
    COUNT(DISTINCT t.trader_address) FILTER (WHERE t.is_buy)     AS uniq_buyers,
    COUNT(DISTINCT t.trader_address) FILTER (WHERE NOT t.is_buy) AS uniq_sellers
  FROM recent_coins rc
  JOIN coins cc ON cc.developer = rc.developer
  LEFT JOIN trades t
    ON  t.coin_address = cc.coin_address
   AND t.currency = 'sol'
   AND t.role    = 'regular'
  GROUP BY rc.developer, cc.coin_address
),
trader_last AS (
  SELECT DISTINCT ON (t.trader_address, cc.developer)
    cc.developer,
    t.trader_address,
    t.pnl::double precision AS pnl
  FROM trades t
  JOIN coins cc ON cc.coin_address = t.coin_address
  WHERE t.role='regular' AND t.currency='sol'
    AND cc.developer IN (SELECT developer FROM recent_coins)
  ORDER BY t.trader_address, cc.developer, t.slot_time DESC, t.id DESC
),
dev_agg AS (
  SELECT
    pd.developer,
    COUNT(DISTINCT pd.coin_address) AS total_coins,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.ath_mcap) AS median_mcap,
    AVG(COALESCE(pd.volume,0.0))                              AS avg_volume,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY pd.total_trades) AS median_trades,
    AVG(pd.uniq_buyers::double precision)                     AS avg_buyers
  FROM per_dev pd
  GROUP BY pd.developer
),
dev_pnl AS (
  SELECT developer, AVG(pnl) AS pnl_avg FROM trader_last GROUP BY developer
),
final AS (
  SELECT
    da.developer,
    da.total_coins,
    COALESCE(da.median_mcap,0)   AS median_mcap,
    COALESCE(dp.pnl_avg,0)       AS trader_pnl_avg,
    COALESCE(da.avg_buyers,0)    AS holders_avg,
    COALESCE(da.avg_volume,0)    AS avg_volume,
    COALESCE(da.median_trades,0) AS median_trades
  FROM dev_agg da
  LEFT JOIN dev_pnl dp ON dp.developer = da.developer
)
SELECT * FROM final;
SQL

echo
echo "=== running heavy distribution query (this may take ~10s) ==="
psql "$DB_URL" -t -A -F $'\t' -f /tmp/dev_dist.sql > /tmp/devdist.tsv
echo "rows=$(wc -l < /tmp/devdist.tsv)"
echo "first lines:"
head -3 /tmp/devdist.tsv

echo
echo "=== distributions (n devs) ==="
dist_col () {
  local name="$1" col="$2"
  awk -F'\t' -v c="$col" '{print $c}' /tmp/devdist.tsv | sort -n > /tmp/_col
  local n=$(wc -l < /tmp/_col)
  if [ "$n" -eq 0 ]; then echo "  $name: n=0"; return; fi
  local min=$(head -1 /tmp/_col)
  local max=$(tail -1 /tmp/_col)
  local p25=$(awk -v n=$n 'NR==int(n*0.25)+1{print; exit}' /tmp/_col)
  local p50=$(awk -v n=$n 'NR==int(n*0.50)+1{print; exit}' /tmp/_col)
  local p75=$(awk -v n=$n 'NR==int(n*0.75)+1{print; exit}' /tmp/_col)
  local p90=$(awk -v n=$n 'NR==int(n*0.90)+1{print; exit}' /tmp/_col)
  local p95=$(awk -v n=$n 'NR==int(n*0.95)+1{print; exit}' /tmp/_col)
  printf "  %-18s n=%d min=%s p25=%s p50=%s p75=%s p90=%s p95=%s max=%s\n" "$name" "$n" "$min" "$p25" "$p50" "$p75" "$p90" "$p95" "$max"
}
dist_col "median_mcap"    3
dist_col "trader_pnl_avg" 4
dist_col "holders_avg"    5
dist_col "avg_volume"     6
dist_col "median_trades"  7

echo
echo "=== how many of these devs pass each pre-gate threshold ==="
awk -F'\t' -v n=$(wc -l < /tmp/devdist.tsv) 'BEGIN{
  total=0; mc=0; pn=0; ho=0; vo=0; tr=0; all5=0; relax_mc60=0; relax_pn5=0; relax_h30=0; relax_v60=0; relax_t30=0; relax_all=0
}
{
  total++
  pmcap = ($3+0) >= 90
  ppnl  = ($4+0) >= 10
  phold = ($5+0) >= 50
  pvol  = ($6+0) >= 90
  ptrd  = ($7+0) >= 50
  if (pmcap) mc++
  if (ppnl)  pn++
  if (phold) ho++
  if (pvol)  vo++
  if (ptrd)  tr++
  if (pmcap && ppnl && phold && pvol && ptrd) all5++

  # relaxed
  if (($3+0)>=60) relax_mc60++
  if (($4+0)>=5)  relax_pn5++
  if (($5+0)>=30) relax_h30++
  if (($6+0)>=60) relax_v60++
  if (($7+0)>=30) relax_t30++
  if (($3+0)>=60 && ($4+0)>=5 && ($5+0)>=30 && ($6+0)>=60 && ($7+0)>=30) relax_all++
}
END{
  printf "  total_devs                    : %d\n", total
  printf "  median_mcap >= 90             : %d (%.1f%%)\n", mc, 100.0*mc/total
  printf "  trader_pnl_avg >= 10          : %d (%.1f%%)\n", pn, 100.0*pn/total
  printf "  holders_avg >= 50             : %d (%.1f%%)\n", ho, 100.0*ho/total
  printf "  avg_volume >= 90              : %d (%.1f%%)\n", vo, 100.0*vo/total
  printf "  median_trades >= 50           : %d (%.1f%%)\n", tr, 100.0*tr/total
  printf "  ALL 5 together (current gate) : %d (%.2f%%)\n", all5, 100.0*all5/total
  print ""
  printf "  relaxed (mcap>=60)            : %d (%.1f%%)\n", relax_mc60, 100.0*relax_mc60/total
  printf "  relaxed (pnl>=5)              : %d (%.1f%%)\n", relax_pn5, 100.0*relax_pn5/total
  printf "  relaxed (holders>=30)         : %d (%.1f%%)\n", relax_h30, 100.0*relax_h30/total
  printf "  relaxed (volume>=60)          : %d (%.1f%%)\n", relax_v60, 100.0*relax_v60/total
  printf "  relaxed (trades>=30)          : %d (%.1f%%)\n", relax_t30, 100.0*relax_t30/total
  printf "  ALL 5 relaxed                 : %d (%.2f%%)\n", relax_all, 100.0*relax_all/total
}
' /tmp/devdist.tsv

echo
echo "=== sample of devs CLOSEST to passing strict gate (sorted by # criteria met) ==="
awk -F'\t' 'BEGIN{OFS="\t"}{
  c=0
  c += (($3+0)>=90)
  c += (($4+0)>=10)
  c += (($5+0)>=50)
  c += (($6+0)>=90)
  c += (($7+0)>=50)
  print c, $1, $2, $3, $4, $5, $6, $7
}' /tmp/devdist.tsv | sort -t$'\t' -k1,1 -rn | head -15 | awk -F'\t' '{printf "  pass=%s  coins=%-3s  dev=%s\n    mcap=%.1f  pnl=%.1f%%  holders=%.1f  vol=%.1f  trades=%.0f\n", $1,$3,$2,$4,$5,$6,$7,$8}'
