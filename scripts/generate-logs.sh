#!/usr/bin/env bash
# Generate demo logs periodically for Barnacles Agent

set -euo pipefail

LOG_DIR="./data/demo"
APP_LOG="$LOG_DIR/app.log"
ACCESS_LOG="$LOG_DIR/access.log"

mkdir -p "$LOG_DIR"
touch "$APP_LOG" "$ACCESS_LOG"

echo "Log generator started writing to $LOG_DIR (Press Ctrl+C to stop)"

ENDPOINTS=("/api/v1/users" "/api/v1/checkout" "/healthz" "/api/v1/products" "/auth/login")
STATUSES=(200 200 200 201 204 400 401 404 500 502)
LEVELS=("INFO" "INFO" "INFO" "WARN" "ERROR")

while true; do
  TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  LVL=${LEVELS[$RANDOM % ${#LEVELS[@]}]}
  EP=${ENDPOINTS[$RANDOM % ${#ENDPOINTS[@]}]}
  STATUS=${STATUSES[$RANDOM % ${#STATUSES[@]}]}
  LATENCY=$((RANDOM % 450 + 10))

  # 1. Plain Text / Auto Log
  if [ "$LVL" = "ERROR" ]; then
    echo "$TS [$LVL] failed to process request on $EP: downstream service timed out (status=$STATUS latency=${LATENCY}ms)" >> "$APP_LOG"
  elif [ "$LVL" = "WARN" ]; then
    echo "$TS [$LVL] high memory watermark detected in worker pool: 84% utilized" >> "$APP_LOG"
  else
    echo "$TS [$LVL] processed request on $EP successfully (status=$STATUS latency=${LATENCY}ms)" >> "$APP_LOG"
  fi

  # 2. JSON Access Log
  JSON_LINE="{\"timestamp\":\"$TS\",\"level\":\"$LVL\",\"endpoint\":\"$EP\",\"status\":$STATUS,\"latency_ms\":$LATENCY,\"ip\":\"192.168.1.$((RANDOM % 254 + 1))\",\"message\":\"HTTP $STATUS $EP in ${LATENCY}ms\"}"
  echo "$JSON_LINE" >> "$ACCESS_LOG"

  sleep 0.5
done
