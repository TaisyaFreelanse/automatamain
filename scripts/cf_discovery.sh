#!/usr/bin/env bash
# Discovery for the spam_dev counterfactual: prod scoring config, smart-money
# registry shape, slot_time units, and how many peak>=250 spam_dev mints exist.
# Read-only.
set -u

CFG=/home/automata/filter_config.yaml
echo "================ PROD SCORING CONFIG (scoring section) ================"
awk '/^scoring:/{f=1} f{print} /^[a-zA-Z]/{if(f && $0 !~ /^scoring:/ && $0 ~ /^[a-z]/ && NR>1){}}' "$CFG" 2>/dev/null | sed -n '1,200p'

echo
echo "================ smart_money + persistence paths ================"
grep -nE 'smart_money_path|entity_ttl_secs|flush_every_secs|legacy_scoring|scoring_window_ms|a_threshold|a_plus_threshold|spam_skip_coins|spam_dev_penalty|spam_dev_require_a_plus|require_momentum_good|momentum_good_smart_bypass|minimum_tier_for_buy' "$CFG"

echo
echo "================ smart_money JSON ================"
SM=$(grep -E 'smart_money_path' "$CFG" | sed -E 's/.*:\s*//' | tr -d '"'"'"' ' | head -1)
echo "smart_money_path=$SM"
for c in "$SM" /home/automata/$SM /home/automata/data/smart_money.json /home/automata/smart_money.json; do
  [ -f "$c" ] && { echo "FOUND: $c"; ls -la "$c"; echo "records=$(python3 -c "import json,sys;print(len(json.load(open('$c'))))" 2>/dev/null)"; break; }
done

# DB URL
WD="$(systemctl show loggaper -p WorkingDirectory --value 2>/dev/null)"
DBURL=""
for f in "$WD/.env" /home/automata/.env /root/automata-build/.env /home/automata/loggaper.env; do
  [ -f "$f" ] || continue
  DBURL="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?DATABASE_URL[[:space:]]*=[[:space:]]*//p' "$f" | tr -d '"' | tr -d "'" | sed 's/[[:space:]]*$//' | head -1)"
  [ -n "$DBURL" ] && break
done
[ -z "$DBURL" ] && { echo "[FAIL] no DATABASE_URL"; exit 1; }

echo
echo "================ slot_time units sanity (one graduated mint) ================"
psql "$DBURL" -X -q -P pager=off <<'SQL'
\echo --- coins.created_at vs trades.slot_time vs tape.ts_unix (sample) ---
WITH m AS (SELECT coin_address FROM coin_mcap_tape GROUP BY coin_address HAVING max(mcap_sol)>=250 LIMIT 1)
SELECT
  (SELECT created_at FROM coins WHERE coin_address=(SELECT coin_address FROM m)) AS coin_created_at,
  (SELECT min(slot_time) FROM trades WHERE coin_address=(SELECT coin_address FROM m)) AS trades_min_slot_time,
  (SELECT max(slot_time) FROM trades WHERE coin_address=(SELECT coin_address FROM m)) AS trades_max_slot_time,
  (SELECT min(ts_unix) FROM coin_mcap_tape WHERE coin_address=(SELECT coin_address FROM m)) AS tape_min_ts,
  (SELECT max(ts_unix) FROM coin_mcap_tape WHERE coin_address=(SELECT coin_address FROM m)) AS tape_max_ts;
SQL

echo
echo "================ peak>=250 spam_dev mints available ================"
if [ -f /root/spam_dev_mints.csv ]; then
  echo "spam_dev_mints.csv rows=$(wc -l < /root/spam_dev_mints.csv)"
  psql "$DBURL" -X -q -P pager=off <<'SQL'
CREATE TEMP TABLE sd(mint text, skip_unix bigint);
\copy sd FROM '/root/spam_dev_mints.csv' WITH (FORMAT csv)
WITH pk AS (
  SELECT d.mint,
    (SELECT max(t.mcap_sol) FROM coin_mcap_tape t WHERE t.coin_address=d.mint) AS peak
  FROM sd d)
SELECT count(*) FILTER (WHERE peak>=150) AS peak_ge150,
       count(*) FILTER (WHERE peak>=250) AS peak_ge250,
       count(*) FILTER (WHERE peak>=405) AS full_grad
FROM pk;
SQL
else
  echo "NO /root/spam_dev_mints.csv -- will need to re-extract from logs"
fi
