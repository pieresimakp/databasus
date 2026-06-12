// Package physicaltesting holds the shared control plane, helpers and version-parameterized bodies
// for the PostgreSQL physical backup→restore E2E tests. The per-version packages (pg17, pg18) are
// thin wrappers that call the exported Run* bodies, so each PostgreSQL major runs as its own test
// binary — isolated and in parallel under `go test -p`.
package physicaltesting

import (
	"testing"

	backuping_physical "databasus-backend/internal/features/backups/backups/backuping/physical"
	cache_utils "databasus-backend/internal/util/cache"
)

// SetupNodes starts the single-node production wiring the whole suite drives through the HTTP API:
// a physical backuper node, the scheduler, and the WAL stream supervisor, coordinating through the
// shared physical node registry. With all three up, a backup requested over the API (config-enable
// bootstrap for the FULL, the trigger endpoint for incrementals) is claimed on the next 1s scheduler
// tick, and enabling WAL-stream backups makes the supervisor claim the database and create its
// replication slot. The returned func tears them down after m.Run(). Each version package calls this
// in its own TestMain, so the two majors run with fully isolated control planes.
func SetupNodes() func() {
	_ = cache_utils.ClearAllCache()
	backuping_physical.SetupDependencies()

	backuperNode := backuping_physical.CreateTestPhysicalBackuper(nil)
	stopBackuper := backuping_physical.StartPhysicalBackuperForTest(&testing.T{}, backuperNode)
	stopScheduler := backuping_physical.StartPhysicalSchedulerForTest(&testing.T{})
	stopWalStreamSupervisor := backuping_physical.StartPhysicalWalStreamSupervisorForTest(&testing.T{})

	return func() {
		stopWalStreamSupervisor()
		stopScheduler()
		backuping_physical.StopPhysicalBackuperForTest(&testing.T{}, stopBackuper)
	}
}
