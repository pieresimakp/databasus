// Package logicaltesting holds the engine-agnostic helpers shared by the
// per-engine logical backup/restore test packages (mysql, mariadb, mongodb,
// postgresql). It is normal (non-_test) code so the sibling test packages can
// import it; nothing in production imports anything under tests/, so it never
// enters the shipped binary.
package logicaltesting

import (
	"testing"

	backuping_logical "databasus-backend/internal/features/backups/backups/backuping/logical"
	"databasus-backend/internal/features/restores/restoring"
	cache_utils "databasus-backend/internal/util/cache"
)

// SetupNodes clears this worker's Valkey logical DB, then starts the logical
// backuper and restorer nodes for the test process. It returns a teardown that
// stops both nodes. Each engine package calls it from TestMain so every parallel
// test binary runs its own isolated node pair.
func SetupNodes() func() {
	// Best-effort clean slate for this worker's Valkey logical DB; a stale key
	// here is harmless because each worker uses its own DB and namespace.
	_ = cache_utils.ClearAllCache()

	backuperNode := backuping_logical.CreateTestBackuperNode()
	cancelBackup := backuping_logical.StartBackuperNodeForTest(&testing.T{}, backuperNode)

	restorerNode := restoring.CreateTestRestorerNode()
	cancelRestore := restoring.StartRestorerNodeForTest(&testing.T{}, restorerNode)

	return func() {
		backuping_logical.StopBackuperNodeForTest(&testing.T{}, cancelBackup, backuperNode)
		restoring.StopRestorerNodeForTest(&testing.T{}, cancelRestore, restorerNode)
	}
}
