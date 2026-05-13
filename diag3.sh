#!/bin/bash
set -e

DB_URL='postgres://postgres:v3rYS3curPassw0rd@localhost:5432/automata'

echo "=== tables ==="
psql "$DB_URL" -c '\dt' 2>&1 | head -30

echo
echo "=== column lists ==="
for t in coins traders trades creators; do
  echo "--- $t ---"
  psql "$DB_URL" -c "\d $t" 2>&1 | head -30
done
