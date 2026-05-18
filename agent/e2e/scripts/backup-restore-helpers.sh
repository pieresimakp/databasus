#!/bin/bash
# Shared helper functions for backup-restore E2E tests.
# Source this file from test scripts: source "$(dirname "$0")/backup-restore-helpers.sh"

AGENT="/tmp/test-agent"
AGENT_PID=""

cleanup_agent() {
  if [ -n "$AGENT_PID" ]; then
    kill "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
    AGENT_PID=""
  fi

  pkill -f "test-agent" 2>/dev/null || true
  for i in $(seq 1 20); do
    pgrep -f "test-agent" > /dev/null 2>&1 || break
    sleep 0.5
  done
  pkill -9 -f "test-agent" 2>/dev/null || true
  sleep 0.5

  rm -f "$AGENT" "$AGENT.update" databasus.lock databasus.log databasus.log.old databasus.json 2>/dev/null || true
}

setup_agent() {
  local artifacts="${1:-/opt/agent/artifacts}"

  cleanup_agent
  cp "$artifacts/agent-v1" "$AGENT"
  chmod +x "$AGENT"
}

init_pg_local() {
  local pgdata="$1"
  local port="$2"
  local wal_queue="$3"
  local pg_bin_dir="$4"

  # Stop any leftover PG from previous test runs
  su postgres -c "$pg_bin_dir/pg_ctl -D $pgdata stop -m immediate" 2>/dev/null || true
  su postgres -c "$pg_bin_dir/pg_ctl -D /tmp/restore-pgdata stop -m immediate" 2>/dev/null || true

  mkdir -p "$wal_queue"
  chown postgres:postgres "$wal_queue"
  rm -rf "$pgdata"

  su postgres -c "$pg_bin_dir/initdb -D $pgdata" > /dev/null

  cat >> "$pgdata/postgresql.conf" <<PGCONF
wal_level = replica
archive_mode = on
archive_command = 'cp %p $wal_queue/%f'
max_wal_senders = 3
listen_addresses = 'localhost'
port = $port
checkpoint_timeout = 30s
PGCONF

  echo "local all all trust" > "$pgdata/pg_hba.conf"
  echo "host all all 127.0.0.1/32 trust" >> "$pgdata/pg_hba.conf"
  echo "host all all ::1/128 trust" >> "$pgdata/pg_hba.conf"
  echo "local replication all trust" >> "$pgdata/pg_hba.conf"
  echo "host replication all 127.0.0.1/32 trust" >> "$pgdata/pg_hba.conf"
  echo "host replication all ::1/128 trust" >> "$pgdata/pg_hba.conf"

  su postgres -c "$pg_bin_dir/pg_ctl -D $pgdata -l /tmp/pg.log start -w"

  su postgres -c "$pg_bin_dir/psql -p $port -c \"CREATE USER testuser WITH SUPERUSER REPLICATION;\"" > /dev/null 2>&1 || true
  su postgres -c "$pg_bin_dir/psql -p $port -c \"CREATE DATABASE testdb OWNER testuser;\"" > /dev/null 2>&1 || true

  echo "PostgreSQL initialized and started on port $port"
}

insert_test_data() {
  local port="$1"
  local pg_bin_dir="$2"

  su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb" <<SQL
CREATE TABLE e2e_test_data (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO e2e_test_data (name, value) VALUES
    ('row1', 100),
    ('row2', 200),
    ('row3', 300);
SQL

  echo "Test data inserted (3 rows)"
}

force_checkpoint() {
  local port="$1"
  local pg_bin_dir="$2"

  su postgres -c "$pg_bin_dir/psql -p $port -c 'CHECKPOINT;'" > /dev/null
  echo "Checkpoint forced"
}

run_agent_backup() {
  local mock_server="$1"
  local pg_host="$2"
  local pg_port="$3"
  local wal_queue="$4"
  local pg_type="$5"
  local pg_host_bin_dir="${6:-}"
  local pg_docker_container="${7:-}"

  # Reset mock server state and set version to match agent (prevents background upgrade loop)
  curl -sf -X POST "$mock_server/mock/reset" > /dev/null
  curl -sf -X POST "$mock_server/mock/set-version" \
    -H "Content-Type: application/json" \
    -d '{"version":"v1.0.0"}' > /dev/null

  # Build JSON config
  cd /tmp

  local extra_fields=""
  if [ -n "$pg_host_bin_dir" ]; then
    extra_fields="$extra_fields\"pgHostBinDir\": \"$pg_host_bin_dir\","
  fi
  if [ -n "$pg_docker_container" ]; then
    extra_fields="$extra_fields\"pgDockerContainerName\": \"$pg_docker_container\","
  fi

  cat > databasus.json <<AGENTCONF
{
  "databasusHost": "$mock_server",
  "dbId": "test-db-id",
  "token": "test-token",
  "pgHost": "$pg_host",
  "pgPort": $pg_port,
  "pgUser": "testuser",
  "pgPassword": "",
  ${extra_fields}
  "pgType": "$pg_type",
  "pgWalDir": "$wal_queue",
  "deleteWalAfterUpload": true
}
AGENTCONF

  # Run agent daemon in background
  "$AGENT" _run > /tmp/agent-output.log 2>&1 &
  AGENT_PID=$!

  echo "Agent started with PID $AGENT_PID"
}

generate_wal_background() {
  local port="$1"
  local pg_bin_dir="$2"

  while true; do
    su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb -c \"
      INSERT INTO e2e_test_data (name, value)
      SELECT 'bulk_' || g, g FROM generate_series(1, 1000) g;
      SELECT pg_switch_wal();
    \"" > /dev/null 2>&1 || break
    sleep 2
  done
}

generate_wal_docker_background() {
  local container="$1"

  while true; do
    docker exec "$container" psql -U testuser -d testdb -c "
      INSERT INTO e2e_test_data (name, value)
      SELECT 'bulk_' || g, g FROM generate_series(1, 1000) g;
      SELECT pg_switch_wal();
    " > /dev/null 2>&1 || break
    sleep 2
  done
}

wait_for_backup_complete() {
  local mock_server="$1"
  local timeout="${2:-120}"

  echo "Waiting for backup to complete (timeout: ${timeout}s)..."

  for i in $(seq 1 "$timeout"); do
    STATUS=$(curl -sf "$mock_server/mock/backup-status" 2>/dev/null || echo '{}')
    IS_FINALIZED=$(echo "$STATUS" | grep -o '"isFinalized":true' || true)
    WAL_COUNT=$(echo "$STATUS" | grep -o '"walSegmentCount":[0-9]*' | grep -o '[0-9]*$' || echo "0")

    if [ -n "$IS_FINALIZED" ] && [ "$WAL_COUNT" -gt 0 ]; then
      echo "Backup complete: finalized with $WAL_COUNT WAL segments"
      return 0
    fi

    sleep 1
  done

  echo "FAIL: Backup did not complete within ${timeout} seconds"
  echo "Last status: $STATUS"
  echo "Agent output:"
  cat /tmp/agent-output.log 2>/dev/null || true
  return 1
}

stop_agent() {
  if [ -n "$AGENT_PID" ]; then
    kill "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
    AGENT_PID=""
  fi

  echo "Agent stopped"
}

stop_pg() {
  local pgdata="$1"
  local pg_bin_dir="$2"

  su postgres -c "$pg_bin_dir/pg_ctl -D $pgdata stop -m fast" 2>/dev/null || true

  echo "PostgreSQL stopped"
}

run_agent_restore() {
  local mock_server="$1"
  local restore_dir="$2"

  rm -rf "$restore_dir"
  mkdir -p "$restore_dir"
  chown postgres:postgres "$restore_dir"

  cd /tmp

  "$AGENT" restore \
    --skip-update \
    --databasus-host "$mock_server" \
    --token test-token \
    --target-dir "$restore_dir"

  echo "Agent restore completed"
}

start_restored_pg() {
  local restore_dir="$1"
  local port="$2"
  local pg_bin_dir="$3"

  # Ensure port is set in restored config
  if ! grep -q "^port" "$restore_dir/postgresql.conf" 2>/dev/null; then
    echo "port = $port" >> "$restore_dir/postgresql.conf"
  fi

  # Ensure listen_addresses is set
  if ! grep -q "^listen_addresses" "$restore_dir/postgresql.conf" 2>/dev/null; then
    echo "listen_addresses = 'localhost'" >> "$restore_dir/postgresql.conf"
  fi

  chown -R postgres:postgres "$restore_dir"
  chmod 700 "$restore_dir"

  if ! su postgres -c "$pg_bin_dir/pg_ctl -D $restore_dir -l /tmp/pg-restore.log start -w"; then
    echo "FAIL: PostgreSQL failed to start on restored data"
    echo "--- pg-restore.log ---"
    cat /tmp/pg-restore.log 2>/dev/null || echo "(no log file)"
    echo "--- postgresql.auto.conf ---"
    cat "$restore_dir/postgresql.auto.conf" 2>/dev/null || echo "(no file)"
    echo "--- pg_wal/ listing ---"
    ls -la "$restore_dir/pg_wal/" 2>/dev/null || echo "(no pg_wal dir)"
    echo "--- databasus-wal-restore/ listing ---"
    ls -la "$restore_dir/databasus-wal-restore/" 2>/dev/null || echo "(no dir)"
    echo "--- end diagnostics ---"
    return 1
  fi

  echo "PostgreSQL started on restored data"
}

wait_for_recovery_complete() {
  local port="$1"
  local pg_bin_dir="$2"
  local timeout="${3:-60}"

  echo "Waiting for recovery to complete (timeout: ${timeout}s)..."

  for i in $(seq 1 "$timeout"); do
    IS_READY=$(su postgres -c "$pg_bin_dir/pg_isready -p $port" 2>&1 || true)

    if echo "$IS_READY" | grep -q "accepting connections"; then
      IN_RECOVERY=$(su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb -t -c 'SELECT pg_is_in_recovery();'" 2>/dev/null | tr -d ' \n' || echo "t")

      if [ "$IN_RECOVERY" = "f" ]; then
        echo "PostgreSQL recovered and promoted to primary"
        return 0
      fi
    fi

    sleep 1
  done

  echo "FAIL: PostgreSQL did not recover within ${timeout} seconds"
  echo "Recovery log:"
  cat /tmp/pg-restore.log 2>/dev/null || true
  return 1
}

verify_restored_data() {
  local port="$1"
  local pg_bin_dir="$2"

  ROW_COUNT=$(su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb -t -c 'SELECT COUNT(*) FROM e2e_test_data;'" | tr -d ' \n')

  if [ "$ROW_COUNT" -lt 3 ]; then
    echo "FAIL: Expected at least 3 rows, got $ROW_COUNT"
    su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb -c 'SELECT * FROM e2e_test_data;'"
    return 1
  fi

  RESULT=$(su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb -t -c \"SELECT value FROM e2e_test_data WHERE name='row1';\"" | tr -d ' \n')

  if [ "$RESULT" != "100" ]; then
    echo "FAIL: Expected row1 value=100, got $RESULT"
    return 1
  fi

  RESULT2=$(su postgres -c "$pg_bin_dir/psql -p $port -U testuser -d testdb -t -c \"SELECT value FROM e2e_test_data WHERE name='row3';\"" | tr -d ' \n')

  if [ "$RESULT2" != "300" ]; then
    echo "FAIL: Expected row3 value=300, got $RESULT2"
    return 1
  fi

  echo "PASS: Found $ROW_COUNT rows, data integrity verified"
  return 0
}

find_pg_bin_dir() {
  # Find the PG bin dir from the installed version
  local pg_config_path
  pg_config_path=$(which pg_config 2>/dev/null || true)

  if [ -n "$pg_config_path" ]; then
    pg_config --bindir
    return
  fi

  # Fallback: search common locations
  for version in 18 17 16 15; do
    if [ -d "/usr/lib/postgresql/$version/bin" ]; then
      echo "/usr/lib/postgresql/$version/bin"
      return
    fi
  done

  echo "ERROR: Cannot find PostgreSQL bin directory" >&2
  return 1
}
