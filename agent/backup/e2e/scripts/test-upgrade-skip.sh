#!/bin/bash
set -euo pipefail

ARTIFACTS="/opt/agent/artifacts"
AGENT="/tmp/test-agent"

# Cleanup from previous runs
pkill -f "test-agent" 2>/dev/null || true
for i in $(seq 1 20); do
  pgrep -f "test-agent" > /dev/null 2>&1 || break
  sleep 0.5
done
pkill -9 -f "test-agent" 2>/dev/null || true
sleep 0.5
rm -f "$AGENT" "$AGENT.update" databasus.lock databasus.log databasus.log.old databasus.json 2>/dev/null || true

# Set mock server to return v1.0.0 (same as agent)
curl -sf -X POST http://e2e-mock-server:4050/mock/set-version \
  -H "Content-Type: application/json" \
  -d '{"version":"v1.0.0"}'

# Copy v1 binary to writable location
cp "$ARTIFACTS/agent-v1" "$AGENT"
chmod +x "$AGENT"

# Verify initial version
VERSION=$("$AGENT" version)
if [ "$VERSION" != "v1.0.0" ]; then
  echo "FAIL: Expected initial version v1.0.0, got $VERSION"
  exit 1
fi

# Run start — agent should see version matches and skip upgrade
echo "Running agent start (expecting upgrade skip)..."
OUTPUT=$("$AGENT" start \
  --databasus-host http://e2e-mock-server:4050 \
  --db-id test-db-id \
  --token test-token \
  --pg-host e2e-postgres \
  --pg-port 5432 \
  --pg-user testuser \
  --pg-password testpassword \
  --pg-wal-dir /tmp/wal \
  --pg-type host 2>&1) || true

echo "$OUTPUT"

# Verify output contains "up to date"
if ! echo "$OUTPUT" | grep -qi "up to date"; then
  echo "FAIL: Expected output to contain 'up to date'"
  exit 1
fi

# Verify binary is still v1
VERSION=$("$AGENT" version)
if [ "$VERSION" != "v1.0.0" ]; then
  echo "FAIL: Expected version v1.0.0 (unchanged), got $VERSION"
  exit 1
fi

echo "Upgrade correctly skipped, version still $VERSION"

# Cleanup daemon
"$AGENT" stop || true
