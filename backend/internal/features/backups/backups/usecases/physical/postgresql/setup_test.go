package usecases_physical_postgresql_test

import (
	"os"
	"testing"

	backuping_logical "databasus-backend/internal/features/backups/backups/backuping/logical"
	backuping_physical "databasus-backend/internal/features/backups/backups/backuping/physical"
	cache_utils "databasus-backend/internal/util/cache"
)

// TestMain boots the logical backuper for registry coverage; physical
// tests call PhysicalBackuperNode.MakeBackup directly rather than
// subscribing through the registry (the BackupNodesRegistry only allows
// one subscriber per channel, so we can't run two Run() loops side by side).
//
// SetupDependencies registers the PhysicalSlotCleanupListener on
// DatabaseService.AddDbRemoveListener so RemoveTestDatabase drops
// per-backup and WAL streamer slots when test databases are torn down.
func TestMain(m *testing.M) {
	cache_utils.ClearAllCache()
	backuping_physical.SetupDependencies()

	logicalBackuperNode := backuping_logical.CreateTestBackuperNode()
	cancelLogical := backuping_logical.StartBackuperNodeForTest(&testing.T{}, logicalBackuperNode)

	exitCode := m.Run()

	backuping_logical.StopBackuperNodeForTest(&testing.T{}, cancelLogical, logicalBackuperNode)

	os.Exit(exitCode)
}
