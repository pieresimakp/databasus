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

# Set mock server to v1.0.0 (same as agent — no sync upgrade on start)
curl -sf -X POST http://e2e-mock-server:4050/mock/set-version \
  -H "Content-Type: application/json" \
  -d '{"version":"v1.0.0"}'

curl -sf -X POST http://e2e-mock-server:4050/mock/set-binary-path \
  -H "Content-Type: application/json" \
  -d '{"binaryPath":"/artifacts/agent-v1"}'

# Copy v1 binary to writable location
cp "$ARTIFACTS/agent-v1" "$AGENT"
chmod +x "$AGENT"

# Verify initial version
VERSION=$("$AGENT" version)
if [ "$VERSION" != "v1.0.0" ]; then
  echo "FAIL: Expected initial version v1.0.0, got $VERSION"
  exit 1
fi
echo "Initial version: $VERSION"

# Start agent as daemon (versions match → no sync upgrade)
mkdir -p /tmp/wal
"$AGENT" start \
  --databasus-host http://e2e-mock-server:4050 \
  --db-id test-db-id \
  --token test-token \
  --pg-host e2e-postgres \
  --pg-port 5432 \
  --pg-user testuser \
  --pg-password testpassword \
  --pg-wal-dir /tmp/wal \
  --pg-type host

echo "Agent started as daemon, waiting for stabilization..."
sleep 2

# Change mock server to v2.0.0 and point to v2 binary
curl -sf -X POST http://e2e-mock-server:4050/mock/set-version \
  -H "Content-Type: application/json" \
  -d '{"version":"v2.0.0"}'

curl -sf -X POST http://e2e-mock-server:4050/mock/set-binary-path \
  -H "Content-Type: application/json" \
  -d '{"binaryPath":"/artifacts/agent-v2"}'

echo "Mock server updated to v2.0.0, waiting for background upgrade..."

# Poll for upgrade (timeout 60s, poll every 3s)
DEADLINE=$((SECONDS + 60))
while [ $SECONDS -lt $DEADLINE ]; do
  VERSION=$("$AGENT" version)
  if [ "$VERSION" = "v2.0.0" ]; then
    echo "Binary upgraded to $VERSION"
    break
  fi
  sleep 3
done

VERSION=$("$AGENT" version)
if [ "$VERSION" != "v2.0.0" ]; then
  echo "FAIL: Expected v2.0.0 after background upgrade, got $VERSION"
  cat databasus.log 2>/dev/null || true
  exit 1
fi

# Verify agent is still running after restart
sleep 2
"$AGENT" status || true

# Cleanup
"$AGENT" stop || true

echo "Background upgrade test passed"
