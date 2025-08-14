#!/usr/bin/env bash
set -euo pipefail

# Simple BLPOP race reproducer.
# - Starts your server
# - Launches 2 BLPOP clients and 1 RPUSH client
# - Captures outputs in temp files for inspection
#
# Usage:
#   scripts/run_blpop_race.sh [PORT]
# Default PORT: 6380 (keeps 6379 free if you have real redis running)

PORT="${1:-6380}"
HOST="127.0.0.1"
KEY="list1"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/.race_logs"
mkdir -p "$LOG_DIR"

SERVER_LOG="$LOG_DIR/server.log"
CLIENT1_OUT="$LOG_DIR/client1_blpop.out"
CLIENT2_OUT="$LOG_DIR/client2_blpop.out"
CLIENT3_OUT="$LOG_DIR/client3_rpush.out"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && ps -p "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[run] compiling..."
(cd "$ROOT_DIR" && mix compile) >/dev/null

echo "[run] starting server on $HOST:$PORT ..."
(cd "$ROOT_DIR" && mix run --no-halt -- --port "$PORT") >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo "[run] waiting for server to accept connections..."
python3 - "$HOST" "$PORT" <<'PY'
import socket, sys, time
host, port = sys.argv[1], int(sys.argv[2])
deadline = time.time() + 15
last_err = None
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            sys.exit(0)
    except Exception as e:
        last_err = e
        time.sleep(0.1)
print("Failed to connect to server:", last_err)
sys.exit(1)
PY

echo "[run] launching clients (2x BLPOP, 1x RPUSH) ..."

# client helper (dedicated python script)
client_py() {
  python3 "$ROOT_DIR/scripts/resp_client.py" "$HOST" "$PORT" "$@"
}

# Start two BLPOP clients (5s timeout) sequentially to enforce registration order
client_py blpop "$KEY" 5 >"$CLIENT1_OUT" 2>&1 & C1=$!
sleep 0.2
client_py blpop "$KEY" 5 >"$CLIENT2_OUT" 2>&1 & C2=$!
sleep 0.2

# Start RPUSH client (push one element)
client_py rpush "$KEY" value1 >"$CLIENT3_OUT" 2>&1 & C3=$!

wait "$C1" "$C2" "$C3" || true

echo
echo "==== Server log ($SERVER_LOG) tail ===="
tail -n 100 "$SERVER_LOG" || true

echo
echo "==== Client outputs ($LOG_DIR) ===="
echo "-- client1 (BLPOP):" && cat "$CLIENT1_OUT" && echo
echo "-- client2 (BLPOP):" && cat "$CLIENT2_OUT" && echo
echo "-- client3 (RPUSH):" && cat "$CLIENT3_OUT" && echo

echo "[done] artifacts in $LOG_DIR"
exit 0


