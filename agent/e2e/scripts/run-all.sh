#!/bin/bash
set -euo pipefail

MODE="${1:-host}"
SCRIPT_DIR="$(dirname "$0")"
PASSED=0
FAILED=0
FAILED_NAMES=""

run_test() {
  local name="$1"
  local script="$2"

  echo ""
  echo "========================================"
  echo "  $name"
  echo "========================================"

  if bash "$script"; then
    echo "  PASSED: $name"
    PASSED=$((PASSED + 1))
  else
    echo "  FAILED: $name"
    FAILED=$((FAILED + 1))
    FAILED_NAMES="${FAILED_NAMES}\n    - ${name}"
  fi
}

if [ "$MODE" = "host" ]; then
  run_test "Test 1: Upgrade success (v1 -> v2)" "$SCRIPT_DIR/test-upgrade-success.sh"
  run_test "Test 2: Upgrade skip (version matches)" "$SCRIPT_DIR/test-upgrade-skip.sh"
  run_test "Test 3: Background upgrade (v1 -> v2 while running)" "$SCRIPT_DIR/test-upgrade-background.sh"
  run_test "Test 4: Backup-restore via host PATH" "$SCRIPT_DIR/test-pg-host-path.sh"
  run_test "Test 5: Backup-restore via host bindir" "$SCRIPT_DIR/test-pg-host-bindir.sh"

elif [ "$MODE" = "docker" ]; then
  run_test "Test 6: Backup-restore via docker exec" "$SCRIPT_DIR/test-pg-docker-exec.sh"

else
  echo "Unknown mode: $MODE (expected 'host' or 'docker')"
  exit 1
fi

echo ""
echo "========================================"
echo "  Results: $PASSED passed, $FAILED failed"
if [ "$FAILED" -gt 0 ]; then
  echo ""
  echo "  Failed:"
  echo -e "$FAILED_NAMES"
fi
echo "========================================"

if [ "$FAILED" -gt 0 ]; then
  exit 1
fi
