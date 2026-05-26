#!/usr/bin/env bash
# Prod check: Jupiter price/v3 with key from loggaper env or /home/automata/.env
set -euo pipefail

MINT="${1:-9U85nJVNNDeibnqj2byqwEQnkNiajTiJBJ1hqE6Kpump}"
WSOL="So11111111111111111111111111111111111111112"
URL="https://api.jup.ag/price/v3?ids=${MINT},${WSOL}"

load_key_from_env_file() {
  local f="/home/automata/.env"
  [[ -f "$f" ]] || return 1
  # KEY:VALUE or KEY=VALUE (no export in file)
  local line
  line="$(grep -E '^JUPITER_API_KEY[:=]' "$f" | head -1)" || return 1
  if [[ "$line" == *:* ]]; then
    echo "${line#JUPITER_API_KEY:}"
  else
    echo "${line#JUPITER_API_KEY=}"
  fi
}

load_key_from_loggaper_proc() {
  local pid
  pid="$(pgrep -f '/home/automata/loggaper' | head -1)" || return 1
  tr '\0' '\n' < "/proc/$pid/environ" | sed -n 's/^JUPITER_API_KEY=//p' | head -1
}

KEY_PROC="$(load_key_from_loggaper_proc || true)"
KEY_FILE="$(load_key_from_env_file || true)"

probe() {
  local label="$1"
  local key="$2"
  echo "=== $label ==="
  if [[ -z "$key" ]]; then
    echo "key: MISSING"
    return
  fi
  echo "key: present (${#key} chars, prefix=${key:0:8}…)"
  local out body
  out="$(mktemp)"
  body="$(mktemp)"
  local http_code
  http_code="$(curl -sS -o "$body" -w '%{http_code}' \
    -H "x-api-key: $key" \
    "$URL")" || http_code="curl_error"
  echo "HTTP status: $http_code"
  echo "body (first 500 chars):"
  head -c 500 "$body" | tr '\n' ' '
  echo
  rm -f "$out" "$body"
}

probe "loggaper process environ" "$KEY_PROC"
probe "/home/automata/.env parsed" "$KEY_FILE"

echo "=== Python verify script (repo) ==="
if [[ -f /root/automata-build/scripts/verify_graduated_exit_mcap.py ]]; then
  python3 /root/automata-build/scripts/verify_graduated_exit_mcap.py "$MINT" 2>&1 | head -20
elif [[ -f /tmp/verify_graduated_exit_mcap.py ]]; then
  python3 /tmp/verify_graduated_exit_mcap.py "$MINT" 2>&1 | head -20
else
  echo "verify_graduated_exit_mcap.py not found on server"
fi

echo "=== bash: source /home/automata/.env then curl ==="
echo "JUPITER in .env after source:"
bash -c 'set -a; source /home/automata/.env 2>/dev/null; echo "  len=\${#JUPITER_API_KEY}"'
if KEY_FILE="$(load_key_from_env_file 2>/dev/null)"; then
  : # parsed separately
fi
KEY_SRC="$(load_key_from_loggaper_proc || true)"
if [[ -n "$KEY_SRC" ]]; then
  echo "=== curl -i (loggaper key, like user example) ==="
  curl -sS -i -H "x-api-key: $KEY_SRC" "$URL" | head -25
fi

echo "=== loggaper journal: price/v3 or 403 (last 14d) ==="
journalctl -u loggaper --since '14 days ago' --no-pager 2>/dev/null \
  | grep -iE 'price/v3|403|Forbidden|JUPITER.*HTTP|error code: 1010' | tail -15 || echo "(no matches)"

echo "=== verify script: passes x-api-key when key set? ==="
grep -n 'x-api-key' /root/automata-build/scripts/verify_graduated_exit_mcap.py 2>/dev/null | head -3 || true
