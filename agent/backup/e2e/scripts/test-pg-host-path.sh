#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
source "$SCRIPT_DIR/backup-restore-helpers.sh"

MOCK_SERVER="${MOCK_SERVER_OVERRIDE:-http://e2e-mock-server:4050}"
PGDATA="/tmp/pgdata"
RESTORE_PGDATA="/tmp/restore-pgdata"
WAL_QUEUE="/tmp/wal-queue"
PG_PORT=5433

PG_BIN_DIR=$(find_pg_bin_dir)
echo "Using PG bin dir: $PG_BIN_DIR"

# Verify pg_basebackup is in PATH
if ! which pg_basebackup > /dev/null 2>&1; then
  echo "FAIL: pg_basebackup not found in PATH (test setup issue)"
  exit 1
fi

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

echo "=== Phase 6: Wait for backup to complete ==="
wait_for_backup_complete "$MOCK_SERVER" 120

echo "=== Phase 7: Stop WAL generator, agent, and PostgreSQL ==="
kill $WAL_GEN_PID 2>/dev/null || true
wait $WAL_GEN_PID 2>/dev/null || true
stop_agent
stop_pg "$PGDATA" "$PG_BIN_DIR"

echo "=== Phase 8: Restore ==="
run_agent_restore "$MOCK_SERVER" "$RESTORE_PGDATA"

echo "=== Phase 9: Start PostgreSQL on restored data ==="
start_restored_pg "$RESTORE_PGDATA" "$PG_PORT" "$PG_BIN_DIR"

echo "=== Phase 10: Wait for recovery ==="
wait_for_recovery_complete "$PG_PORT" "$PG_BIN_DIR" 60

echo "=== Phase 11: Verify data ==="
verify_restored_data "$PG_PORT" "$PG_BIN_DIR"

echo "=== Phase 12: Cleanup ==="
stop_pg "$RESTORE_PGDATA" "$PG_BIN_DIR"

echo "pg_basebackup in PATH: full backup-restore lifecycle passed"
