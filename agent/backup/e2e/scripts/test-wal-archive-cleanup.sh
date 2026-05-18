#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
source "$SCRIPT_DIR/backup-restore-helpers.sh"

MOCK_SERVER="${MOCK_SERVER_OVERRIDE:-http://e2e-mock-server:4050}"
PGDATA="/tmp/pgdata-walcleanup"
WAL_QUEUE="/tmp/wal-queue-walcleanup"
PG_PORT=5435

PG_BIN_DIR=$(find_pg_bin_dir)
echo "Using PG bin dir: $PG_BIN_DIR"

echo "=== Phase 1: Setup agent ==="
setup_agent

echo "=== Phase 2: Initialize PostgreSQL ==="
init_pg_local "$PGDATA" "$PG_PORT" "$WAL_QUEUE" "$PG_BIN_DIR"

echo "=== Phase 3: Insert test data ==="
insert_test_data "$PG_PORT" "$PG_BIN_DIR"

echo "=== Phase 4: Force checkpoint and start agent backup ==="
force_checkpoint "$PG_PORT" "$PG_BIN_DIR"
run_agent_backup "$MOCK_SERVER" "127.0.0.1" "$PG_PORT" "$WAL_QUEUE" "host"

echo "=== Phase 5: Generate WAL in background ==="
generate_wal_background "$PG_PORT" "$PG_BIN_DIR" &
WAL_GEN_PID=$!

echo "=== Phase 6: Wait for base backup to complete ==="
wait_for_backup_complete "$MOCK_SERVER" 120

echo "=== Phase 7: Stop WAL generator and PostgreSQL (agent keeps running) ==="
kill $WAL_GEN_PID 2>/dev/null || true
wait $WAL_GEN_PID 2>/dev/null || true
stop_pg "$PGDATA" "$PG_BIN_DIR"

echo "=== Phase 8: Verify agent cleaned .backup history files from WAL archive ==="
verify_wal_queue_no_backup_files "$WAL_QUEUE" 60

echo "=== Phase 9: Cleanup ==="
stop_agent

echo "wal-archive-cleanup: .backup history files cleaned from WAL queue"
