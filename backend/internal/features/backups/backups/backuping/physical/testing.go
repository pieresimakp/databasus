package backuping_physical

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	"databasus-backend/internal/features/backups/backups/core/physical/chain_view"
	physical_repositories "databasus-backend/internal/features/backups/backups/core/physical/repositories"
	physical_service "databasus-backend/internal/features/backups/backups/core/physical/service"
	postgresql_executor "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	backups_config_physical "databasus-backend/internal/features/backups/config/physical"
	"databasus-backend/internal/features/billing"
	"databasus-backend/internal/features/databases"
	encryption_secrets "databasus-backend/internal/features/encryption/secrets"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

// CreateTestPhysicalBackuper returns a fully wired PhysicalBackuperNode for
// tests. The notification sender is parameterized so tests that don't want
// to exercise the notifier stack can inject a counting / no-op stub.
// Pass nil to use the production notifier service.
func CreateTestPhysicalBackuper(notificationSender NotificationSender) *PhysicalBackuperNode {
	sender := notificationSender
	if sender == nil {
		sender = notifiers.GetNotifierService()
	}

	return &PhysicalBackuperNode{
		databases.GetDatabaseService(),
		encryption.GetFieldEncryptor(),
		workspaces_services.GetWorkspaceService(),
		physical_repositories.GetFullBackupRepository(),
		physical_repositories.GetIncrementalBackupRepository(),
		physical_repositories.GetInFlightBackupRepository(),
		physical_repositories.GetWalHistoryRepository(),
		backups_config_physical.GetBackupConfigService(),
		storages.GetStorageService(),
		sender,
		tasks_cancellation.GetTaskCancelManager(),
		physicalBackupNodesRegistry,
		encryption_secrets.GetSecretKeyService(),
		logger.GetLogger(),
		postgresql_executor.NewCreateFullBackupUsecase(),
		postgresql_executor.NewCreateIncrementalBackupUsecase(),
		uuid.New(),
		time.Time{},
		atomic.Bool{},
	}
}

// CreateTestPhysicalScheduler returns a scheduler wired to the production repos
// and the physical node pool, with the billing seam injected so cloud-mode
// tests can pin subscription state.
func CreateTestPhysicalScheduler(billingService BillingService) *PhysicalBackupsScheduler {
	return CreateTestPhysicalSchedulerWithCoordinator(
		billingService,
		nodes.NewNodeAssignmentCoordinator(physicalBackupNodesRegistry, logger.GetLogger()),
	)
}

// CreateTestPhysicalSchedulerWithCoordinator is CreateTestPhysicalScheduler with
// the assignment coordinator injected, so a test can hand the scheduler an
// isolated registry (its own namespace) and avoid touching the shared physical
// pool's completion subscription.
func CreateTestPhysicalSchedulerWithCoordinator(
	billingService BillingService,
	assignmentCoordinator *nodes.NodeAssignmentCoordinator,
) *PhysicalBackupsScheduler {
	return &PhysicalBackupsScheduler{
		physical_repositories.GetFullBackupRepository(),
		physical_repositories.GetIncrementalBackupRepository(),
		physical_repositories.GetInFlightBackupRepository(),
		backups_config_physical.GetBackupConfigService(),
		chain_view.GetChainViewService(),
		tasks_cancellation.GetTaskCancelManager(),
		assignmentCoordinator,
		billingService,
		atomicTime{},
		logger.GetLogger(),
		atomic.Bool{},
		atomic.Bool{},
	}
}

// CreateTestWalStreamSupervisor returns a fresh WAL stream supervisor wired to
// the production repos and services. A fresh instance (not a copy of the DI
// singleton) keeps each test's hasRun/running state isolated and avoids copying
// the embedded mutex.
func CreateTestWalStreamSupervisor() *PhysicalWalStreamSupervisor {
	return &PhysicalWalStreamSupervisor{
		databases.GetDatabaseService(),
		backups_config_physical.GetBackupConfigService(),
		storages.GetStorageService(),
		physical_repositories.GetWalSegmentRepository(),
		physical_repositories.GetWalHistoryRepository(),
		physical_repositories.GetWalStreamerRepository(),
		notifiers.GetNotifierService(),
		tasks_cancellation.GetTaskCancelManager(),
		encryption_secrets.GetSecretKeyService(),
		encryption.GetFieldEncryptor(),
		logger.GetLogger(),
		sync.Mutex{},
		make(map[uuid.UUID]*runningStreamer),
		atomicTime{},
		atomic.Bool{},
		atomic.Bool{},
	}
}

// CreateTestPhysicalCleaner returns a cleaner wired to the production service +
// repos with the billing seam injected for cloud-mode storage-cap tests.
func CreateTestPhysicalCleaner(billingService BillingService) *PhysicalBackupCleaner {
	return &PhysicalBackupCleaner{
		physical_service.GetPhysicalBackupService(),
		chain_view.GetChainViewService(),
		backups_config_physical.GetBackupConfigService(),
		physical_repositories.GetFullBackupRepository(),
		physical_repositories.GetWalSegmentRepository(),
		physical_repositories.GetInFlightBackupRepository(),
		physicalBackupNodesRegistry,
		billingService,
		logger.GetLogger(),
		atomic.Bool{},
	}
}

// StartPhysicalBackuperForTest starts the backuper's Run loop in a goroutine.
// Returns a cancel func the caller defers; the cancel waits for the
// goroutine to drain before returning so tests don't leak the subscription.
func StartPhysicalBackuperForTest(
	t *testing.T,
	backuper *PhysicalBackuperNode,
) context.CancelFunc {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		backuper.Run(ctx)
		close(done)
	}()

	deadline := time.Now().UTC().Add(5 * time.Second)
	for time.Now().UTC().Before(deadline) {
		if backuper.IsRunning() {
			return func() {
				cancel()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Log("physical backuper stop timeout")
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("physical backuper failed to start within timeout")

	return nil
}

// StopPhysicalBackuperForTest signals the backuper's Run loop to exit and
// waits for it to drain. The cancel returned from StartPhysicalBackuperForTest
// already does this; StopPhysicalBackuperForTest is the matching named
// wrapper to keep test code symmetric.
func StopPhysicalBackuperForTest(_ *testing.T, cancel context.CancelFunc) {
	cancel()
}

// StartPhysicalSchedulerForTest starts a fresh scheduler's Run loop in a
// goroutine and waits until it has subscribed to completions (IsRunning). It
// mirrors the single-node production wiring where the same process runs the
// scheduler and a backuper node, coordinating through the shared physical node
// registry — so a backup requested over HTTP is picked up on the next tick and
// handed to StartPhysicalBackuperForTest's node. The production billing seam is
// injected; outside cloud mode it is never consulted. Returns a cancel func the
// caller defers; it cancels and waits for the goroutine to drain.
func StartPhysicalSchedulerForTest(t *testing.T) context.CancelFunc {
	t.Helper()

	scheduler := CreateTestPhysicalScheduler(billing.GetBillingService())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	deadline := time.Now().UTC().Add(5 * time.Second)
	for time.Now().UTC().Before(deadline) {
		if scheduler.IsRunning() {
			return func() {
				cancel()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Log("physical scheduler stop timeout")
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("physical scheduler failed to start within timeout")

	return nil
}

// StartPhysicalWalStreamSupervisorForTest starts a fresh WAL stream supervisor's
// Run loop in a goroutine and waits until it is ready (IsRunning). It mirrors the
// processing-node wiring in cmd/main.go: with the supervisor up, enabling
// WAL-stream backups over the HTTP config API causes it to claim the database and
// create its replication slot on the next reconcile tick — so tests drive the slot
// lifecycle through the API instead of starting a streamer by hand. Returns a
// cancel func the caller defers; it cancels and waits for the goroutine to drain.
func StartPhysicalWalStreamSupervisorForTest(t *testing.T) context.CancelFunc {
	t.Helper()

	supervisor := CreateTestWalStreamSupervisor()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		supervisor.Run(ctx)
		close(done)
	}()

	deadline := time.Now().UTC().Add(5 * time.Second)
	for time.Now().UTC().Before(deadline) {
		if supervisor.IsRunning() {
			return func() {
				cancel()

				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Log("physical wal stream supervisor stop timeout")
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("physical wal stream supervisor failed to start within timeout")

	return nil
}
