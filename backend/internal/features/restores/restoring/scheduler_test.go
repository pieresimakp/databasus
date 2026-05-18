package restoring

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"databasus-backend/internal/config"
	backups_controllers "databasus-backend/internal/features/backups/backups/controllers"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/databases/databases/postgresql"
	"databasus-backend/internal/features/notifiers"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/storages"
	users_enums "databasus-backend/internal/features/users/enums"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
)

func Test_CheckDeadNodesAndFailRestores_NodeDies_FailsRestoreAndCleansUpRegistry(t *testing.T) {
	cache_utils.ClearAllCache()

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	var mockNodeID uuid.UUID

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}
		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusInProgress)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}
		restores, _ = restoreRepo.FindByStatus(restores_core.RestoreStatusFailed)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)

		// Clean up mock node
		if mockNodeID != uuid.Nil {
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: mockNodeID})
		}
		cache_utils.ClearAllCache()
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	var err error
	// Register mock node without subscribing to restores (simulates node crash after registration)
	mockNodeID = uuid.New()
	err = CreateMockNodeInRegistry(mockNodeID, 100, time.Now().UTC())
	assert.NoError(t, err)

	// Create restore and assign to mock node
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   restores_core.RestoreStatusInProgress,
	}
	err = restoreRepository.Save(restore)
	assert.NoError(t, err)

	// Scheduler assigns restore to mock node
	err = GetRestoresScheduler().StartRestore(restore.ID, nil)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Verify Valkey counter was incremented when restore was assigned
	stats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	foundStat := false
	for _, stat := range stats {
		if stat.ID == mockNodeID {
			assert.Equal(t, 1, stat.ActiveRestores)
			foundStat = true
			break
		}
	}
	assert.True(t, foundStat, "Node stats should be present")

	// Simulate node death by setting heartbeat older than 2-minute threshold
	oldHeartbeat := time.Now().UTC().Add(-3 * time.Minute)
	err = UpdateNodeHeartbeatDirectly(mockNodeID, 100, oldHeartbeat)
	assert.NoError(t, err)

	// Trigger dead node detection
	err = GetRestoresScheduler().checkDeadNodesAndFailRestores()
	assert.NoError(t, err)

	// Verify restore was failed with appropriate error message
	failedRestore, err := restoreRepository.FindByID(restore.ID)
	assert.NoError(t, err)
	assert.Equal(t, restores_core.RestoreStatusFailed, failedRestore.Status)
	assert.NotNil(t, failedRestore.FailMessage)
	assert.Contains(t, *failedRestore.FailMessage, "node unavailability")

	// Verify Valkey counter was decremented after restore failed
	stats, err = restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	for _, stat := range stats {
		if stat.ID == mockNodeID {
			assert.Equal(t, 0, stat.ActiveRestores)
		}
	}

	time.Sleep(200 * time.Millisecond)
}

func Test_OnRestoreCompleted_TaskIsNotRestore_SkipsProcessing(t *testing.T) {
	cache_utils.ClearAllCache()

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	var mockNodeID uuid.UUID

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}
		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusInProgress)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)

		// Clean up mock node
		if mockNodeID != uuid.Nil {
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: mockNodeID})
		}
		cache_utils.ClearAllCache()
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	// Register mock node
	mockNodeID = uuid.New()
	err := CreateMockNodeInRegistry(mockNodeID, 100, time.Now().UTC())
	assert.NoError(t, err)

	// Create restore and assign to the node
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   restores_core.RestoreStatusInProgress,
	}
	err = restoreRepository.Save(restore)
	assert.NoError(t, err)

	err = GetRestoresScheduler().StartRestore(restore.ID, nil)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Get initial state of the registry
	initialStats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	var initialActiveTasks int
	for _, stat := range initialStats {
		if stat.ID == mockNodeID {
			initialActiveTasks = stat.ActiveRestores
			break
		}
	}
	assert.Equal(t, 1, initialActiveTasks, "Should have 1 active task")

	// Call onRestoreCompleted with a random UUID (not a restore ID)
	nonRestoreTaskID := uuid.New()
	GetRestoresScheduler().onRestoreCompleted(mockNodeID, nonRestoreTaskID)

	time.Sleep(100 * time.Millisecond)

	// Verify: Active tasks counter should remain the same (not decremented)
	stats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	for _, stat := range stats {
		if stat.ID == mockNodeID {
			assert.Equal(t, initialActiveTasks, stat.ActiveRestores,
				"Active tasks should not change for non-restore task")
		}
	}

	// Verify: restore should still be in progress (not modified)
	unchangedRestore, err := restoreRepository.FindByID(restore.ID)
	assert.NoError(t, err)
	assert.Equal(t, restores_core.RestoreStatusInProgress, unchangedRestore.Status,
		"Restore status should not change for non-restore task completion")

	// Verify: restoreToNodeRelations should still contain the node
	scheduler := GetRestoresScheduler()
	_, exists := scheduler.restoreToNodeRelations[mockNodeID]
	assert.True(t, exists, "Node should still be in restoreToNodeRelations")

	time.Sleep(200 * time.Millisecond)
}

func Test_CalculateLeastBusyNode_SelectsNodeWithBestScore(t *testing.T) {
	t.Run("Nodes with same throughput", func(t *testing.T) {
		cache_utils.ClearAllCache()

		node1ID := uuid.New()
		node2ID := uuid.New()
		node3ID := uuid.New()
		now := time.Now().UTC()

		defer func() {
			// Clean up all mock nodes
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: node1ID})
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: node2ID})
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: node3ID})
			cache_utils.ClearAllCache()
		}()

		err := CreateMockNodeInRegistry(node1ID, 100, now)
		assert.NoError(t, err)
		err = CreateMockNodeInRegistry(node2ID, 100, now)
		assert.NoError(t, err)
		err = CreateMockNodeInRegistry(node3ID, 100, now)
		assert.NoError(t, err)

		for range 5 {
			err = restoreNodesRegistry.IncrementRestoresInProgress(node1ID)
			assert.NoError(t, err)
		}

		for range 2 {
			err = restoreNodesRegistry.IncrementRestoresInProgress(node2ID)
			assert.NoError(t, err)
		}

		for range 8 {
			err = restoreNodesRegistry.IncrementRestoresInProgress(node3ID)
			assert.NoError(t, err)
		}

		leastBusyNodeID, err := GetRestoresScheduler().calculateLeastBusyNode()
		assert.NoError(t, err)
		assert.NotNil(t, leastBusyNodeID)
		assert.Equal(t, node2ID, *leastBusyNodeID)
	})

	t.Run("Nodes with different throughput", func(t *testing.T) {
		cache_utils.ClearAllCache()

		node100MBsID := uuid.New()
		node50MBsID := uuid.New()
		now := time.Now().UTC()

		defer func() {
			// Clean up all mock nodes
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: node100MBsID})
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: node50MBsID})
			cache_utils.ClearAllCache()
		}()

		err := CreateMockNodeInRegistry(node100MBsID, 100, now)
		assert.NoError(t, err)
		err = CreateMockNodeInRegistry(node50MBsID, 50, now)
		assert.NoError(t, err)

		for range 10 {
			err = restoreNodesRegistry.IncrementRestoresInProgress(node100MBsID)
			assert.NoError(t, err)
		}

		err = restoreNodesRegistry.IncrementRestoresInProgress(node50MBsID)
		assert.NoError(t, err)

		leastBusyNodeID, err := GetRestoresScheduler().calculateLeastBusyNode()
		assert.NoError(t, err)
		assert.NotNil(t, leastBusyNodeID)
		assert.Equal(t, node50MBsID, *leastBusyNodeID)
	})
}

func Test_FailRestoresInProgress_SchedulerStarts_UpdatesStatus(t *testing.T) {
	cache_utils.ClearAllCache()

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}

		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusInProgress)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		restores, _ = restoreRepo.FindByStatus(restores_core.RestoreStatusFailed)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		restores, _ = restoreRepo.FindByStatus(restores_core.RestoreStatusCompleted)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)

		cache_utils.ClearAllCache()
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	// Create two in-progress restores that should be failed on scheduler restart
	restore1 := &restores_core.Restore{
		BackupID:  backup.ID,
		Status:    restores_core.RestoreStatusInProgress,
		CreatedAt: time.Now().UTC().Add(-30 * time.Minute),
	}
	err := restoreRepository.Save(restore1)
	assert.NoError(t, err)

	restore2 := &restores_core.Restore{
		BackupID:  backup.ID,
		Status:    restores_core.RestoreStatusInProgress,
		CreatedAt: time.Now().UTC().Add(-15 * time.Minute),
	}
	err = restoreRepository.Save(restore2)
	assert.NoError(t, err)

	// Create a completed restore to verify it's not affected by failRestoresInProgress
	completedRestore := &restores_core.Restore{
		BackupID:  backup.ID,
		Status:    restores_core.RestoreStatusCompleted,
		CreatedAt: time.Now().UTC().Add(-1 * time.Hour),
	}
	err = restoreRepository.Save(completedRestore)
	assert.NoError(t, err)

	// Trigger the scheduler's failRestoresInProgress logic
	// This should mark in-progress restores as failed
	err = GetRestoresScheduler().failRestoresInProgress()
	assert.NoError(t, err)

	// Verify all restores exist and were processed correctly
	allRestores1, err := restoreRepository.FindByID(restore1.ID)
	assert.NoError(t, err)
	allRestores2, err := restoreRepository.FindByID(restore2.ID)
	assert.NoError(t, err)
	allRestores3, err := restoreRepository.FindByID(completedRestore.ID)
	assert.NoError(t, err)

	var failedCount int
	var completedCount int

	restoresToCheck := []*restores_core.Restore{allRestores1, allRestores2, allRestores3}
	for _, restore := range restoresToCheck {
		switch restore.Status {
		case restores_core.RestoreStatusFailed:
			failedCount++
			// Verify fail message indicates application restart
			assert.NotNil(t, restore.FailMessage)
			assert.Equal(t, "Restore failed due to application restart", *restore.FailMessage)
		case restores_core.RestoreStatusCompleted:
			completedCount++
		}
	}

	// Verify correct number of restores in each state
	assert.Equal(t, 2, failedCount, "Should have 2 failed restores (originally in progress)")
	assert.Equal(t, 1, completedCount, "Should have 1 completed restore (unchanged)")

	time.Sleep(200 * time.Millisecond)
}

func Test_StartRestore_RestoreCompletes_DecrementsActiveTaskCount(t *testing.T) {
	cache_utils.ClearAllCache()

	// Start scheduler so it can handle task completions
	scheduler := CreateTestRestoresScheduler()
	schedulerCancel := StartSchedulerForTest(t, scheduler)
	defer schedulerCancel()

	restorerNode := CreateTestRestorerNode()
	restorerNode.restoreBackupUsecase = &MockSuccessRestoreUsecase{}

	cancel := StartRestorerNodeForTest(t, restorerNode)
	defer StopRestorerNodeForTest(t, cancel, restorerNode)

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}
		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusCompleted)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	// Get initial active task count
	stats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	var initialActiveTasks int
	for _, stat := range stats {
		if stat.ID == restorerNode.nodeID {
			initialActiveTasks = stat.ActiveRestores
			break
		}
	}
	t.Logf("Initial active tasks: %d", initialActiveTasks)

	// Create and start restore
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   restores_core.RestoreStatusInProgress,
	}
	err = restoreRepository.Save(restore)
	assert.NoError(t, err)

	err = scheduler.StartRestore(restore.ID, nil)
	assert.NoError(t, err)

	// Wait for restore to complete
	WaitForRestoreCompletion(t, restore.ID, 10*time.Second)

	// Verify restore was completed
	completedRestore, err := restoreRepository.FindByID(restore.ID)
	assert.NoError(t, err)
	assert.Equal(t, restores_core.RestoreStatusCompleted, completedRestore.Status)

	// Wait for active task count to decrease
	decreased := WaitForActiveTasksDecrease(
		t,
		restorerNode.nodeID,
		initialActiveTasks+1,
		10*time.Second,
	)
	assert.True(t, decreased, "Active task count should have decreased after restore completion")

	// Verify final active task count equals initial count
	finalStats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	for _, stat := range finalStats {
		if stat.ID == restorerNode.nodeID {
			t.Logf("Final active tasks: %d", stat.ActiveRestores)
			assert.Equal(t, initialActiveTasks, stat.ActiveRestores,
				"Active task count should return to initial value after restore completion")
			break
		}
	}

	time.Sleep(200 * time.Millisecond)
}

func Test_StartRestore_RestoreFails_DecrementsActiveTaskCount(t *testing.T) {
	cache_utils.ClearAllCache()

	// Start scheduler so it can handle task completions
	scheduler := CreateTestRestoresScheduler()
	schedulerCancel := StartSchedulerForTest(t, scheduler)
	defer schedulerCancel()

	restorerNode := CreateTestRestorerNode()
	restorerNode.restoreBackupUsecase = &MockFailedRestoreUsecase{}

	cancel := StartRestorerNodeForTest(t, restorerNode)
	defer StopRestorerNodeForTest(t, cancel, restorerNode)

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}
		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusFailed)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	// Get initial active task count
	stats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	var initialActiveTasks int
	for _, stat := range stats {
		if stat.ID == restorerNode.nodeID {
			initialActiveTasks = stat.ActiveRestores
			break
		}
	}
	t.Logf("Initial active tasks: %d", initialActiveTasks)

	// Create and start restore
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   restores_core.RestoreStatusInProgress,
	}
	err = restoreRepository.Save(restore)
	assert.NoError(t, err)

	err = scheduler.StartRestore(restore.ID, nil)
	assert.NoError(t, err)

	// Wait for restore to fail
	WaitForRestoreCompletion(t, restore.ID, 10*time.Second)

	// Verify restore failed
	failedRestore, err := restoreRepository.FindByID(restore.ID)
	assert.NoError(t, err)
	assert.Equal(t, restores_core.RestoreStatusFailed, failedRestore.Status)

	// Wait for active task count to decrease
	decreased := WaitForActiveTasksDecrease(
		t,
		restorerNode.nodeID,
		initialActiveTasks+1,
		10*time.Second,
	)
	assert.True(t, decreased, "Active task count should have decreased after restore failure")

	// Verify final active task count equals initial count
	finalStats, err := restoreNodesRegistry.GetRestoreNodesStats()
	assert.NoError(t, err)
	for _, stat := range finalStats {
		if stat.ID == restorerNode.nodeID {
			t.Logf("Final active tasks: %d", stat.ActiveRestores)
			assert.Equal(t, initialActiveTasks, stat.ActiveRestores,
				"Active task count should return to initial value after restore failure")
			break
		}
	}

	time.Sleep(200 * time.Millisecond)
}

func Test_StartRestore_CredentialsStoredEncryptedInCache(t *testing.T) {
	cache_utils.ClearAllCache()

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	var mockNodeID uuid.UUID

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}
		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusInProgress)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)

		// Clean up mock node
		if mockNodeID != uuid.Nil {
			restoreNodesRegistry.UnregisterNodeFromRegistry(RestoreNode{ID: mockNodeID})
		}
		cache_utils.ClearAllCache()
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	// Register mock node so scheduler can assign restore to it
	mockNodeID = uuid.New()
	err := CreateMockNodeInRegistry(mockNodeID, 100, time.Now().UTC())
	assert.NoError(t, err)

	// Create restore with plaintext credentials
	plaintextPassword := "test_password_123"
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   restores_core.RestoreStatusInProgress,
	}
	err = restoreRepository.Save(restore)
	assert.NoError(t, err)

	// Create PostgreSQL database credentials with plaintext password
	postgresDB := &postgresql.PostgresqlDatabase{
		Host:     config.GetEnv().TestLocalhost,
		Port:     5432,
		Username: "testuser",
		Password: plaintextPassword,
		Database: new("testdb"),
		Version:  "16",
	}

	// Encrypt password using FieldEncryptor (same as production flow)
	encryptor := encryption.GetFieldEncryptor()
	err = postgresDB.EncryptSensitiveFields(database.ID, encryptor)
	assert.NoError(t, err)

	// Verify password was encrypted (different from plaintext)
	assert.NotEqual(t, plaintextPassword, postgresDB.Password,
		"Password should be encrypted, not plaintext")

	// Create cache with encrypted credentials
	dbCache := &RestoreDatabaseCache{
		PostgresqlDatabase: postgresDB,
	}

	// Call StartRestore to cache credentials (do NOT start restore node)
	err = GetRestoresScheduler().StartRestore(restore.ID, dbCache)
	assert.NoError(t, err)

	// Directly read from cache
	cachedData := restoreDatabaseCache.Get(restore.ID.String())
	assert.NotNil(t, cachedData, "Cache entry should exist")
	assert.NotNil(t, cachedData.PostgresqlDatabase, "PostgreSQL credentials should be cached")

	// Verify password in cache is encrypted (not plaintext)
	assert.NotEqual(t, plaintextPassword, cachedData.PostgresqlDatabase.Password,
		"Cached password should be encrypted, not plaintext")
	assert.Equal(t, postgresDB.Password, cachedData.PostgresqlDatabase.Password,
		"Cached password should match the encrypted version")

	// Verify other fields are present
	assert.Equal(t, config.GetEnv().TestLocalhost, cachedData.PostgresqlDatabase.Host)
	assert.Equal(t, 5432, cachedData.PostgresqlDatabase.Port)
	assert.Equal(t, "testuser", cachedData.PostgresqlDatabase.Username)
	assert.Equal(t, "testdb", *cachedData.PostgresqlDatabase.Database)

	time.Sleep(200 * time.Millisecond)
}

func Test_StartRestore_CredentialsRemovedAfterRestoreStarts(t *testing.T) {
	cache_utils.ClearAllCache()

	// Start scheduler so it can handle task assignments
	scheduler := CreateTestRestoresScheduler()
	schedulerCancel := StartSchedulerForTest(t, scheduler)
	defer schedulerCancel()

	// Create mock restorer node with credential capture usecase
	restorerNode := CreateTestRestorerNode()
	calledChan := make(chan *databases.Database, 1)
	restorerNode.restoreBackupUsecase = &MockCaptureCredentialsRestoreUsecase{
		CalledChan:    calledChan,
		ShouldSucceed: true,
	}

	cancel := StartRestorerNodeForTest(t, restorerNode)
	defer StopRestorerNodeForTest(t, cancel, restorerNode)

	user := users_testing.CreateTestUser(users_enums.UserRoleAdmin)
	router := CreateTestRouter()
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backupRepo := backups_core.BackupRepository{}
		backups, _ := backupRepo.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepo.DeleteByID(backup.ID)
		}

		restoreRepo := restores_core.RestoreRepository{}
		restores, _ := restoreRepo.FindByStatus(restores_core.RestoreStatusCompleted)
		for _, restore := range restores {
			restoreRepo.DeleteByID(restore.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		storages.RemoveTestStorage(storage.ID)
		notifiers.RemoveTestNotifier(notifier)
		workspaces_testing.RemoveTestWorkspace(workspace, router)

		cache_utils.ClearAllCache()
	}()

	backups_config.EnableBackupsForTestDatabase(database.ID, storage)

	// Create a test backup
	backup := backups_controllers.CreateTestBackup(database.ID, storage.ID)

	// Create restore with credentials
	plaintextPassword := "test_password_456"
	restore := &restores_core.Restore{
		BackupID: backup.ID,
		Status:   restores_core.RestoreStatusInProgress,
	}
	err := restoreRepository.Save(restore)
	assert.NoError(t, err)

	// Create PostgreSQL database credentials
	// Database field is nil to avoid PopulateDbData trying to connect
	postgresDB := &postgresql.PostgresqlDatabase{
		Host:     config.GetEnv().TestLocalhost,
		Port:     5432,
		Username: "testuser",
		Password: plaintextPassword,
		Database: nil,
		Version:  "16",
	}

	// Encrypt password (same as production flow)
	encryptor := encryption.GetFieldEncryptor()
	err = postgresDB.EncryptSensitiveFields(database.ID, encryptor)
	assert.NoError(t, err)

	encryptedPassword := postgresDB.Password

	// Create cache with encrypted credentials
	dbCache := &RestoreDatabaseCache{
		PostgresqlDatabase: postgresDB,
	}

	// Call StartRestore to cache credentials and trigger restore
	err = scheduler.StartRestore(restore.ID, dbCache)
	assert.NoError(t, err)

	// Wait for mock usecase to be called (with timeout)
	var capturedDB *databases.Database
	select {
	case capturedDB = <-calledChan:
		t.Log("Mock usecase was called, credentials captured")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for mock usecase to be called")
	}

	// Verify cache is empty after restore starts (credentials were deleted)
	cacheAfterExecution := restoreDatabaseCache.Get(restore.ID.String())
	assert.Nil(t, cacheAfterExecution, "Cache should be empty after restore execution starts")

	// Verify mock received valid credentials
	assert.NotNil(t, capturedDB, "Captured database should not be nil")
	assert.NotNil(t, capturedDB.Postgresql, "PostgreSQL credentials should be provided to usecase")
	assert.Equal(t, config.GetEnv().TestLocalhost, capturedDB.Postgresql.Host)
	assert.Equal(t, 5432, capturedDB.Postgresql.Port)
	assert.Equal(t, "testuser", capturedDB.Postgresql.Username)
	assert.NotEmpty(t, capturedDB.Postgresql.Password, "Password should be provided to usecase")

	// Note: Password at this point may still be encrypted because PopulateDbData
	// is called after the mock captures it. The important thing is that credentials
	// were provided to the usecase despite cache being deleted.
	t.Logf("Encrypted password in cache: %s", encryptedPassword)
	t.Logf("Password received by usecase: %s", capturedDB.Postgresql.Password)

	// Wait for restore to complete
	WaitForRestoreCompletion(t, restore.ID, 10*time.Second)

	// Verify restore was completed
	completedRestore, err := restoreRepository.FindByID(restore.ID)
	assert.NoError(t, err)
	assert.Equal(t, restores_core.RestoreStatusCompleted, completedRestore.Status)

	time.Sleep(200 * time.Millisecond)
}
