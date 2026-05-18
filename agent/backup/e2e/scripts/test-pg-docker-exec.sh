#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
source "$SCRIPT_DIR/backup-restore-helpers.sh"

MOCK_SERVER="${MOCK_SERVER_OVERRIDE:-http://e2e-mock-server:4050}"
PG_CONTAINER="e2e-agent-postgres"
RESTORE_PGDATA="/tmp/restore-pgdata"
WAL_QUEUE="/wal-queue"
PG_PORT=5432

# For restore verification we need a local PG bin dir
PG_BIN_DIR=$(find_pg_bin_dir)
echo "Using local PG bin dir for restore verification: $PG_BIN_DIR"

# Verify docker CLI works and PG container is accessible
if ! docker exec "$PG_CONTAINER" pg_basebackup --version > /dev/null 2>&1; then
  echo "FAIL: Cannot reach pg_basebackup inside container $PG_CONTAINER (test setup issue)"
  exit 1
fi

echo "=== Phase 1: Setup agent ==="
setup_agent

echo "=== Phase 2: Insert test data into containerized PostgreSQL ==="
docker exec "$PG_CONTAINER" psql -U testuser -d testdb -c "
CREATE TABLE IF NOT EXISTS e2e_test_data (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
DELETE FROM e2e_test_data;
INSERT INTO e2e_test_data (name, value) VALUES
    ('row1', 100),
    ('row2', 200),
    ('row3', 300);
"
echo "Test data inserted (3 rows)"

echo "=== Phase 3: Start agent backup (docker exec mode) ==="
curl -sf -X POST "$MOCK_SERVER/mock/reset" > /dev/null

cd /tmp
cat > databasus.json <<AGENTCONF
{
  "databasusHost": "$MOCK_SERVER",
  "dbId": "test-db-id",
  "token": "test-token",
  "pgHost": "$PG_CONTAINER",
  "pgPort": $PG_PORT,
  "pgUser": "testuser",
  "pgPassword": "testpassword",
  "pgType": "docker",
  "pgDockerContainerName": "$PG_CONTAINER",
  "pgWalDir": "$WAL_QUEUE",
  "deleteWalAfterUpload": true
}
AGENTCONF

"$AGENT" _run > /tmp/agent-output.log 2>&1 &
AGENT_PID=$!
echo "Agent started with PID $AGENT_PID"

echo "=== Phase 4: Generate WAL in background ==="
generate_wal_docker_background "$PG_CONTAINER" &
WAL_GEN_PID=$!

echo "=== Phase 5: Wait for backup to complete ==="
wait_for_backup_complete "$MOCK_SERVER" 120

echo "=== Phase 6: Stop WAL generator and agent ==="
kill $WAL_GEN_PID 2>/dev/null || true
wait $WAL_GEN_PID 2>/dev/null || true
stop_agent

echo "=== Phase 7: Restore to local directory ==="
run_agent_restore "$MOCK_SERVER" "$RESTORE_PGDATA"

echo "=== Phase 8: Start local PostgreSQL on restored data ==="
# Use a different port to avoid conflict with the containerized PG
RESTORE_PORT=5433
start_restored_pg "$RESTORE_PGDATA" "$RESTORE_PORT" "$PG_BIN_DIR"

echo "=== Phase 9: Wait for recovery ==="
wait_for_recovery_complete "$RESTORE_PORT" "$PG_BIN_DIR" 60

echo "=== Phase 10: Verify data ==="
verify_restored_data "$RESTORE_PORT" "$PG_BIN_DIR"

echo "=== Phase 11: Cleanup ==="
stop_pg "$RESTORE_PGDATA" "$PG_BIN_DIR"

echo "pg_basebackup via docker exec: full backup-restore lifecycle passed"
