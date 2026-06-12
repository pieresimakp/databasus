package restoring

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"databasus-backend/internal/config"
	backups_core_logical "databasus-backend/internal/features/backups/backups/core/logical"
	backups_services "databasus-backend/internal/features/backups/backups/services"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/databases"
	postgresql_logical "databasus-backend/internal/features/databases/databases/postgresql/logical"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/restores/usecases"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

func CreateTestRouter() *gin.Engine {
	router := workspaces_testing.CreateTestRouter(
		workspaces_controllers.GetWorkspaceController(),
		workspaces_controllers.GetMembershipController(),
		databases.GetDatabaseController(),
		backups_config_logical.GetBackupConfigController(),
	)

	return router
}

func CreateTestRestorerNode() *RestorerNode {
	return &RestorerNode{
		uuid.New(),
		databases.GetDatabaseService(),
		backups_services.GetBackupService(),
		encryption.GetFieldEncryptor(),
		restoreRepository,
		backups_config_logical.GetBackupConfigService(),
		storages.GetStorageService(),
		restoreNodesRegistry,
		logger.GetLogger(),
		usecases.GetRestoreBackupUsecase(),
		restoreDatabaseCache,
		tasks_cancellation.GetTaskCancelManager(),
		time.Time{},
		atomic.Bool{},
	}
}

func CreateTestRestorerNodeWithUsecase(usecase restores_core.RestoreBackupUsecase) *RestorerNode {
	return &RestorerNode{
		uuid.New(),
		databases.GetDatabaseService(),
		backups_services.GetBackupService(),
		encryption.GetFieldEncryptor(),
		restoreRepository,
		backups_config_logical.GetBackupConfigService(),
		storages.GetStorageService(),
		restoreNodesRegistry,
		logger.GetLogger(),
		usecase,
		restoreDatabaseCache,
		tasks_cancellation.GetTaskCancelManager(),
		time.Time{},
		atomic.Bool{},
	}
}

func CreateTestRestoresScheduler() *RestoresScheduler {
	return &RestoresScheduler{
		restoreRepository,
		backups_services.GetBackupService(),
		storages.GetStorageService(),
		backups_config_logical.GetBackupConfigService(),
		restoreNodesRegistry,
		time.Now().UTC(),
		logger.GetLogger(),
		make(map[uuid.UUID]RestoreToNodeRelation),
		restorerNode,
		restoreDatabaseCache,
		uuid.Nil,
		atomic.Bool{},
	}
}

// WaitForRestoreCompletion waits for a restore to be completed (or failed)
func WaitForRestoreCompletion(
	t *testing.T,
	restoreID uuid.UUID,
	timeout time.Duration,
) {
	deadline := time.Now().UTC().Add(timeout)

	for time.Now().UTC().Before(deadline) {
		restore, err := restoreRepository.FindByID(restoreID)
		if err != nil {
			t.Logf("WaitForRestoreCompletion: error finding restore: %v", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		t.Logf("WaitForRestoreCompletion: restore status: %s", restore.Status)

		if restore.Status == restores_core.RestoreStatusCompleted ||
			restore.Status == restores_core.RestoreStatusFailed {
			t.Logf(
				"WaitForRestoreCompletion: restore finished with status %s",
				restore.Status,
			)
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("WaitForRestoreCompletion: timeout waiting for restore to complete")
}

// StartRestorerNodeForTest starts a RestorerNode in a goroutine for testing.
// The node registers itself in the registry and subscribes to restore assignments.
// Returns a context cancel function that should be deferred to stop the node.
func StartRestorerNodeForTest(t *testing.T, restorerNode *RestorerNode) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		restorerNode.Run(ctx)
		close(done)
	}()

	// Poll registry for node presence instead of fixed sleep
	deadline := time.Now().UTC().Add(5 * time.Second)
	for time.Now().UTC().Before(deadline) {
		nodes, err := restoreNodesRegistry.GetAvailableNodes()
		if err == nil {
			for _, node := range nodes {
				if node.ID == restorerNode.nodeID {
					t.Logf("RestorerNode registered in registry: %s", restorerNode.nodeID)

					return func() {
						cancel()
						select {
						case <-done:
							t.Log("RestorerNode stopped gracefully")
						case <-time.After(2 * time.Second):
							t.Log("RestorerNode stop timeout")
						}
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("RestorerNode failed to register in registry within timeout")
	return nil
}

// StartSchedulerForTest starts the RestoresScheduler in a goroutine for testing.
// The scheduler subscribes to task completions and manages restore lifecycle.
// Returns a context cancel function that should be deferred to stop the scheduler.
func StartSchedulerForTest(t *testing.T, scheduler *RestoresScheduler) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		scheduler.Run(ctx)
		close(done)
	}()

	// Give scheduler time to subscribe to completions
	time.Sleep(100 * time.Millisecond)
	t.Log("RestoresScheduler started")

	return func() {
		cancel()
		select {
		case <-done:
			t.Log("RestoresScheduler stopped gracefully")
		case <-time.After(2 * time.Second):
			t.Log("RestoresScheduler stop timeout")
		}
	}
}

// StopRestorerNodeForTest stops the RestorerNode by canceling its context.
// It waits for the node to unregister from the registry.
func StopRestorerNodeForTest(t *testing.T, cancel context.CancelFunc, restorerNode *RestorerNode) {
	cancel()

	// Wait for node to unregister from registry
	deadline := time.Now().UTC().Add(2 * time.Second)
	for time.Now().UTC().Before(deadline) {
		nodes, err := restoreNodesRegistry.GetAvailableNodes()
		if err == nil {
			found := false
			for _, node := range nodes {
				if node.ID == restorerNode.nodeID {
					found = true
					break
				}
			}
			if !found {
				t.Logf("RestorerNode unregistered from registry: %s", restorerNode.nodeID)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("RestorerNode stop completed for %s", restorerNode.nodeID)
}

func CreateMockNodeInRegistry(nodeID uuid.UUID, throughputMBs int, lastHeartbeat time.Time) error {
	restoreNode := RestoreNode{
		ID:            nodeID,
		ThroughputMBs: throughputMBs,
		LastHeartbeat: lastHeartbeat,
	}

	return restoreNodesRegistry.HearthbeatNodeInRegistry(lastHeartbeat, restoreNode)
}

func UpdateNodeHeartbeatDirectly(
	nodeID uuid.UUID,
	throughputMBs int,
	lastHeartbeat time.Time,
) error {
	restoreNode := RestoreNode{
		ID:            nodeID,
		ThroughputMBs: throughputMBs,
		LastHeartbeat: lastHeartbeat,
	}

	return restoreNodesRegistry.HearthbeatNodeInRegistry(lastHeartbeat, restoreNode)
}

func GetNodeFromRegistry(nodeID uuid.UUID) (*RestoreNode, error) {
	nodes, err := restoreNodesRegistry.GetAvailableNodes()
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
		stats, err := restoreNodesRegistry.GetRestoreNodesStats()
		if err != nil {
			t.Logf("WaitForActiveTasksDecrease: error getting node stats: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stat := range stats {
			if stat.ID == nodeID {
				t.Logf(
					"WaitForActiveTasksDecrease: current active tasks = %d (initial = %d)",
					stat.ActiveRestores,
					initialCount,
				)
				if stat.ActiveRestores < initialCount {
					t.Logf(
						"WaitForActiveTasksDecrease: active tasks decreased from %d to %d",
						initialCount,
						stat.ActiveRestores,
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

// CreateTestRestore creates a test restore with the given backup and status
func CreateTestRestore(
	t *testing.T,
	backup *backups_core_logical.LogicalBackup,
	status restores_core.RestoreStatus,
) *restores_core.Restore {
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   status,
		PostgresqlLogicalDatabase: &postgresql_logical.PostgresqlLogicalDatabase{
			Host:     config.GetEnv().TestLocalhost,
			Port:     5432,
			Username: "test",
			Password: "test",
			Database: func() *string { s := "testdb"; return &s }(),
			Version:  "16",
		},
	}

	err := restoreRepository.Save(restore)
	if err != nil {
		t.Fatalf("Failed to create test restore: %v", err)
	}

	return restore
}
