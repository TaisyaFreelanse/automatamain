#!/usr/bin/env bash
set -u
echo "=== disk free ==="
df -h / /var/lib/postgresql 2>/dev/null | sort -u
echo
echo "=== pg data dir size ==="
du -sh /var/lib/postgresql/*/main 2>/dev/null || echo "(no access / different path)"
