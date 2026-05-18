package backuping

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/backups/backups/usecases"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

func CreateTestRouter() *gin.Engine {
	router := workspaces_testing.CreateTestRouter(
		workspaces_controllers.GetWorkspaceController(),
		workspaces_controllers.GetMembershipController(),
		databases.GetDatabaseController(),
		backups_config.GetBackupConfigController(),
	)

	return router
}

func CreateTestBackupCleaner(billingService BillingService) *BackupCleaner {
	return &BackupCleaner{
		backupRepository,
		storages.GetStorageService(),
		backups_config.GetBackupConfigService(),
		billingService,
		encryption.GetFieldEncryptor(),
		logger.GetLogger(),
		[]backups_core.BackupRemoveListener{},
		atomic.Bool{},
	}
}

func CreateTestBackuperNode() *BackuperNode {
	return &BackuperNode{
		databases.GetDatabaseService(),
		encryption.GetFieldEncryptor(),
		workspaces_services.GetWorkspaceService(),
		backupRepository,
		backups_config.GetBackupConfigService(),
		storages.GetStorageService(),
		notifiers.GetNotifierService(),
		taskCancelManager,
		backupNodesRegistry,
		logger.GetLogger(),
		usecases.GetCreateBackupUsecase(),
		uuid.New(),
		time.Time{},
		atomic.Bool{},
	}
}

func CreateTestBackuperNodeWithUseCase(useCase backups_core.CreateBackupUsecase) *BackuperNode {
	return &BackuperNode{
		databases.GetDatabaseService(),
		encryption.GetFieldEncryptor(),
		workspaces_services.GetWorkspaceService(),
		backupRepository,
		backups_config.GetBackupConfigService(),
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
		backups_config.GetBackupConfigService(),
		taskCancelManager,
		backupNodesRegistry,
		databases.GetDatabaseService(),
		billingService,
		time.Now().UTC(),
		logger.GetLogger(),
		make(map[uuid.UUID]BackupToNodeRelation),
		CreateTestBackuperNode(),
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

			if newestBackup.Status == backups_core.BackupStatusCompleted ||
				newestBackup.Status == backups_core.BackupStatusFailed ||
				newestBackup.Status == backups_core.BackupStatusCanceled {
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
func StartSchedulerForTest(t *testing.T, scheduler *BackupsScheduler) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	// Give scheduler time to subscribe to completions
	time.Sleep(100 * time.Millisecond)
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
	backupNode := BackupNode{
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
	backupNode := BackupNode{
		ID:            nodeID,
		ThroughputMBs: throughputMBs,
		LastHeartbeat: lastHeartbeat,
	}

	return backupNodesRegistry.HearthbeatNodeInRegistry(lastHeartbeat, backupNode)
}

func GetNodeFromRegistry(nodeID uuid.UUID) (*BackupNode, error) {
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
