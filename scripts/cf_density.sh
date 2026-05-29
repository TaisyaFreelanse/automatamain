#!/usr/bin/env bash
# Check whether `trades` densely captures the post-create scoring window for the
# peak>=250 spam_dev mints, and whether coin_mcap_tape is dense enough for a
# momentum/buyer reconstruction. Read-only.
set -u
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
DBURL=""
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }

psql "$DBURL" -X -q -P pager=off <<'SQL'
CREATE TEMP TABLE sd(mint text, skip_unix bigint);
\copy sd FROM '/root/spam_dev_mints.csv' WITH (FORMAT csv)

CREATE TEMP TABLE grad AS
SELECT d.mint
FROM sd d
WHERE (SELECT max(t.mcap_sol) FROM coin_mcap_tape t WHERE t.coin_address=d.mint) >= 250;

\echo === per-mint trades + tape density for peak>=250 spam_dev mints (first 40) ===
SELECT g.mint,
  c.created_at,
  (SELECT count(*) FROM trades t WHERE t.coin_address=g.mint) AS trade_rows,
  (SELECT count(DISTINCT t.slot_time) FROM trades t WHERE t.coin_address=g.mint) AS distinct_slots,
  (SELECT count(*) FROM trades t WHERE t.coin_address=g.mint AND t.is_buy) AS buys,
  (SELECT count(DISTINCT t.trader_address) FROM trades t WHERE t.coin_address=g.mint AND t.is_buy) AS distinct_buyers,
  (SELECT min(t.slot_time) FROM trades t WHERE t.coin_address=g.mint) AS min_slot,
  (SELECT max(t.slot_time) FROM trades t WHERE t.coin_address=g.mint) AS max_slot,
  (SELECT count(*) FROM coin_mcap_tape tp WHERE tp.coin_address=g.mint) AS tape_rows
FROM grad g JOIN coins c ON c.coin_address=g.mint
ORDER BY trade_rows DESC
LIMIT 40;

\echo === aggregate: how usable is trades for windowing? ===
SELECT
  count(*) AS grad_mints,
  count(*) FILTER (WHERE tr.trade_rows>0) AS have_trades,
  count(*) FILTER (WHERE tr.distinct_slots>=3) AS slots_ge3,
  count(*) FILTER (WHERE tr.distinct_buyers>=4) AS buyers_ge4,
  round(avg(tr.trade_rows),1) AS avg_trade_rows,
  round(avg(tr.distinct_buyers),1) AS avg_distinct_buyers,
  round(avg(tr.tape_rows),1) AS avg_tape_rows
FROM grad g
JOIN LATERAL (
  SELECT
    (SELECT count(*) FROM trades t WHERE t.coin_address=g.mint) AS trade_rows,
    (SELECT count(DISTINCT t.slot_time) FROM trades t WHERE t.coin_address=g.mint) AS distinct_slots,
    (SELECT count(DISTINCT t.trader_address) FROM trades t WHERE t.coin_address=g.mint AND t.is_buy) AS distinct_buyers,
    (SELECT count(*) FROM coin_mcap_tape tp WHERE tp.coin_address=g.mint) AS tape_rows
) tr ON true;
SQL
