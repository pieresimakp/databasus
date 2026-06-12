package backuping_logical

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	backups_core_logical "databasus-backend/internal/features/backups/backups/core/logical"
	usecases_logical "databasus-backend/internal/features/backups/backups/usecases/logical"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/storage"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

func seedBackup(
	t *testing.T,
	label string,
	backup *backups_core_logical.LogicalBackup,
) *backups_core_logical.LogicalBackup {
	t.Helper()

	if err := storage.GetDb().Create(backup).Error; err != nil {
		t.Fatalf("seed %s backup: %v", label, err)
	}

	return backup
}

func SeedTestBackup(
	t *testing.T,
	databaseID, storageID uuid.UUID,
	sizeMb float64,
) *backups_core_logical.LogicalBackup {
	t.Helper()

	return seedBackup(t, "completed", &backups_core_logical.LogicalBackup{
		ID:                uuid.New(),
		FileName:          "test-backup-" + uuid.New().String(),
		DatabaseID:        databaseID,
		StorageID:         storageID,
		Status:            backups_core_logical.BackupStatusCompleted,
		BackupSizeMb:      sizeMb,
		BackupRawDbSizeMb: sizeMb,
		CreatedAt:         time.Now().UTC(),
	})
}

// SeedInProgressTestBackup inserts an IN_PROGRESS backup row so MakeBackup can
// pick it up and drive it through to completion (mirrors backuper_test.go's
// manual setup, but reusable across packages).
func SeedInProgressTestBackup(
	t *testing.T,
	databaseID, storageID uuid.UUID,
) *backups_core_logical.LogicalBackup {
	t.Helper()

	return seedBackup(t, "in-progress", &backups_core_logical.LogicalBackup{
		ID:         uuid.New(),
		DatabaseID: databaseID,
		StorageID:  storageID,
		Status:     backups_core_logical.BackupStatusInProgress,
		CreatedAt:  time.Now().UTC(),
	})
}

func CreateTestRouter() *gin.Engine {
	router := workspaces_testing.CreateTestRouter(
		workspaces_controllers.GetWorkspaceController(),
		workspaces_controllers.GetMembershipController(),
		databases.GetDatabaseController(),
		backups_config_logical.GetBackupConfigController(),
	)

	return router
}

func CreateTestBackupCleaner(billingService BillingService) *BackupCleaner {
	return &BackupCleaner{
		backupRepository,
		storages.GetStorageService(),
		backups_config_logical.GetBackupConfigService(),
		billingService,
		encryption.GetFieldEncryptor(),
		logger.GetLogger(),
		[]backups_core_logical.BackupRemoveListener{},
		atomic.Bool{},
	}
}

func CreateTestBackuperNode() *BackuperNode {
	return &BackuperNode{
		databases.GetDatabaseService(),
		encryption.GetFieldEncryptor(),
		workspaces_services.GetWorkspaceService(),
		backupRepository,
		backups_config_logical.GetBackupConfigService(),
		storages.GetStorageService(),
		notifiers.GetNotifierService(),
		taskCancelManager,
		backupNodesRegistry,
		logger.GetLogger(),
		usecases_logical.GetCreateBackupUsecase(),
		uuid.New(),
		time.Time{},
		atomic.Bool{},
	}
}

func CreateTestBackuperNodeWithUseCase(useCase backups_core_logical.CreateBackupUsecase) *BackuperNode {
	return &BackuperNode{
		databases.GetDatabaseService(),
		encryption.GetFieldEncryptor(),
		workspaces_services.GetWorkspaceService(),
		backupRepository,
		backups_config_logical.GetBackupConfigService(),
		storages.GetStorageService(),
		notifiers.GetNotifierService(),
		taskCancelManager,
		backupNodesRegistry,
		logger.GetLogger(),
		useCase,
		uuid.New(),
		time.Time{},
		atomic.Bool{},
	}
}

func CreateTestScheduler(billingService BillingService) *BackupsScheduler {
	return &BackupsScheduler{
		backupRepository,
		backups_config_logical.GetBackupConfigService(),
		taskCancelManager,
		nodes.NewNodeAssignmentCoordinator(backupNodesRegistry, logger.GetLogger()),
		databases.GetDatabaseService(),
		billingService,
		time.Now().UTC(),
		logger.GetLogger(),
		CreateTestBackuperNode(),
		[]backups_core_logical.BackupCompletionListener{},
		atomic.Bool{},
		atomic.Bool{},
	}
}

// WaitForBackupCompletion waits for a new backup to be created and completed (or failed)
// for the given database. It checks for backups with count greater than expectedInitialCount.
func WaitForBackupCompletion(
	t *testing.T,
	databaseID uuid.UUID,
	expectedInitialCount int,
	timeout time.Duration,
) {
	deadline := time.Now().UTC().Add(timeout)

	for time.Now().UTC().Before(deadline) {
		backups, err := backupRepository.FindByDatabaseID(databaseID)
		if err != nil {
			t.Logf("WaitForBackupCompletion: error finding backups: %v", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		t.Logf(
			"WaitForBackupCompletion: found %d backups (expected > %d)",
			len(backups),
			expectedInitialCount,
		)

		if len(backups) > expectedInitialCount {
			// Check if the newest backup has completed or failed
			newestBackup := backups[0]
			t.Logf("WaitForBackupCompletion: newest backup status: %s", newestBackup.Status)

			if newestBackup.Status == backups_core_logical.BackupStatusCompleted ||
				newestBackup.Status == backups_core_logical.BackupStatusFailed ||
				newestBackup.Status == backups_core_logical.BackupStatusCanceled {
				t.Logf(
					"WaitForBackupCompletion: backup finished with status %s",
					newestBackup.Status,
				)
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("WaitForBackupCompletion: timeout waiting for backup to complete")
}

// StartBackuperNodeForTest starts a BackuperNode in a goroutine for testing.
// The node registers itself in the registry and subscribes to backup assignments.
// Returns a context cancel function that should be deferred to stop the node.
func StartBackuperNodeForTest(t *testing.T, backuperNode *BackuperNode) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		backuperNode.Run(ctx)
		close(done)
	}()

	// Poll registry for node presence instead of fixed sleep
	deadline := time.Now().UTC().Add(5 * time.Second)
	for time.Now().UTC().Before(deadline) {
		nodes, err := backupNodesRegistry.GetAvailableNodes()
		if err == nil {
			for _, node := range nodes {
				if node.ID == backuperNode.nodeID {
					t.Logf("BackuperNode registered in registry: %s", backuperNode.nodeID)

					return func() {
						cancel()
						select {
						case <-done:
							t.Log("BackuperNode stopped gracefully")
						case <-time.After(2 * time.Second):
							t.Log("BackuperNode stop timeout")
						}
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("BackuperNode failed to register in registry within timeout")
	return nil
}

// StartSchedulerForTest starts the BackupsScheduler in a goroutine for testing.
// The scheduler subscribes to task completions and manages backup lifecycle.
// Returns a context cancel function that should be deferred to stop the scheduler.
//
// PubSubManager.Subscribe handshakes with Valkey before returning, so we
// don't need to sleep here waiting for the subscription to register. Poll
// the scheduler's hasRun flag instead to be sure Run() has entered its
// loop before the caller proceeds.
func StartSchedulerForTest(t *testing.T, scheduler *BackupsScheduler) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	deadline := time.Now().UTC().Add(5 * time.Second)
	for time.Now().UTC().Before(deadline) {
		if scheduler.IsRunning() {
			t.Log("BackupsScheduler started")

			return func() {
				cancel()
				select {
				case <-done:
					t.Log("BackupsScheduler stopped gracefully")
				case <-time.After(2 * time.Second):
					t.Log("BackupsScheduler stop timeout")
				}
			}
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("BackupsScheduler failed to start within timeout")

	return nil
}

// StopBackuperNodeForTest stops the BackuperNode by canceling its context.
// It waits for the node to unregister from the registry.
func StopBackuperNodeForTest(t *testing.T, cancel context.CancelFunc, backuperNode *BackuperNode) {
	cancel()

	// Wait for node to unregister from registry
	deadline := time.Now().UTC().Add(2 * time.Second)
	for time.Now().UTC().Before(deadline) {
		nodes, err := backupNodesRegistry.GetAvailableNodes()
		if err == nil {
			found := false
			for _, node := range nodes {
				if node.ID == backuperNode.nodeID {
					found = true
					break
				}
			}
			if !found {
				t.Logf("BackuperNode unregistered from registry: %s", backuperNode.nodeID)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("BackuperNode stop completed for %s", backuperNode.nodeID)
}

func CreateMockNodeInRegistry(nodeID uuid.UUID, throughputMBs int, lastHeartbeat time.Time) error {
	backupNode := nodes.BackupNode{
		ID:            nodeID,
		ThroughputMBs: throughputMBs,
		LastHeartbeat: lastHeartbeat,
	}

	return backupNodesRegistry.HearthbeatNodeInRegistry(lastHeartbeat, backupNode)
}

func UpdateNodeHeartbeatDirectly(
	nodeID uuid.UUID,
	throughputMBs int,
	lastHeartbeat time.Time,
) error {
	backupNode := nodes.BackupNode{
		ID:            nodeID,
		ThroughputMBs: throughputMBs,
		LastHeartbeat: lastHeartbeat,
	}

	return backupNodesRegistry.HearthbeatNodeInRegistry(lastHeartbeat, backupNode)
}

func GetNodeFromRegistry(nodeID uuid.UUID) (*nodes.BackupNode, error) {
	nodes, err := backupNodesRegistry.GetAvailableNodes()
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		if node.ID == nodeID {
			return &node, nil
		}
	}

	return nil, fmt.Errorf("node not found")
}

// WaitForActiveTasksDecrease waits for the active task count to decrease below the initial count.
// It polls the registry every 500ms until the count decreases or the timeout is reached.
// Returns true if the count decreased, false if timeout was reached.
func WaitForActiveTasksDecrease(
	t *testing.T,
	nodeID uuid.UUID,
	initialCount int,
	timeout time.Duration,
) bool {
	deadline := time.Now().UTC().Add(timeout)

	for time.Now().UTC().Before(deadline) {
		stats, err := backupNodesRegistry.GetBackupNodesStats()
		if err != nil {
			t.Logf("WaitForActiveTasksDecrease: error getting node stats: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stat := range stats {
			if stat.ID == nodeID {
				t.Logf(
					"WaitForActiveTasksDecrease: current active tasks = %d (initial = %d)",
					stat.ActiveBackups,
					initialCount,
				)
				if stat.ActiveBackups < initialCount {
					t.Logf(
						"WaitForActiveTasksDecrease: active tasks decreased from %d to %d",
						initialCount,
						stat.ActiveBackups,
					)
					return true
				}
				break
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("WaitForActiveTasksDecrease: timeout waiting for active tasks to decrease")
	return false
}
