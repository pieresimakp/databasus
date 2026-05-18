package backuping

import (
	"log/slog"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"databasus-backend/internal/config"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	billing_models "databasus-backend/internal/features/billing/models"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/intervals"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	users_enums "databasus-backend/internal/features/users/enums"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/storage"
	"databasus-backend/internal/util/logger"
	"databasus-backend/internal/util/period"
)

func Test_CleanOldBackups_DeletesBackupsOlderThanRetentionTimePeriod(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodWeek,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()
	oldBackup1 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-10 * 24 * time.Hour),
	}
	oldBackup2 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-8 * 24 * time.Hour),
	}
	recentBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-3 * 24 * time.Hour),
	}

	err = backupRepository.Save(oldBackup1)
	assert.NoError(t, err)
	err = backupRepository.Save(oldBackup2)
	assert.NoError(t, err)
	err = backupRepository.Save(recentBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remainingBackups))
	assert.Equal(t, recentBackup.ID, remainingBackups[0].ID)
}

func Test_CleanOldBackups_SkipsDatabaseWithForeverRetentionPeriod(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	oldBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    time.Now().UTC().Add(-365 * 24 * time.Hour),
	}
	err = backupRepository.Save(oldBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remainingBackups))
	assert.Equal(t, oldBackup.ID, remainingBackups[0].ID)
}

func Test_CleanExceededBackups_WhenUnderStorageLimit_NoBackupsDeleted(t *testing.T) {
	enableCloud(t)
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	for i := range 3 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 100,
			CreatedAt:    time.Now().UTC().Add(-time.Duration(i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 1, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err = cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(remainingBackups))
}

func Test_CleanExceededBackups_WhenOverStorageLimit_DeletesOldestBackups(t *testing.T) {
	enableCloud(t)
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// 5 backups at 300 MB each = 1500 MB total, limit = 1 GB (1024 MB)
	// Expect 2 oldest deleted, 3 remain (900 MB < 1024 MB)
	now := time.Now().UTC()
	var backupIDs []uuid.UUID
	for i := range 5 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 300,
			CreatedAt:    now.Add(-time.Duration(4-i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
		backupIDs = append(backupIDs, backup.ID)
	}

	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 1, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err = cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(remainingBackups))

	remainingIDs := make(map[uuid.UUID]bool)
	for _, backup := range remainingBackups {
		remainingIDs[backup.ID] = true
	}
	assert.False(t, remainingIDs[backupIDs[0]])
	assert.False(t, remainingIDs[backupIDs[1]])
	assert.True(t, remainingIDs[backupIDs[2]])
	assert.True(t, remainingIDs[backupIDs[3]])
	assert.True(t, remainingIDs[backupIDs[4]])
}

func Test_CleanExceededBackups_SkipsInProgressBackups(t *testing.T) {
	enableCloud(t)
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()

	// 3 completed at 500 MB each = 1500 MB, limit = 1 GB (1024 MB)
	completedBackups := make([]*backups_core.Backup, 3)
	for i := range 3 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 500,
			CreatedAt:    now.Add(-time.Duration(3-i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
		completedBackups[i] = backup
	}

	inProgressBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusInProgress,
		BackupSizeMb: 10,
		CreatedAt:    now,
	}
	err = backupRepository.Save(inProgressBackup)
	assert.NoError(t, err)

	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 1, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err = cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(remainingBackups), 2)

	var inProgressFound bool
	for _, backup := range remainingBackups {
		if backup.ID == inProgressBackup.ID {
			inProgressFound = true
			assert.Equal(t, backups_core.BackupStatusInProgress, backup.Status)
		}
	}
	assert.True(t, inProgressFound, "In-progress backup should not be deleted")
}

func Test_CleanExceededBackups_WithZeroStorageLimit_RemovesAllBackups(t *testing.T) {
	enableCloud(t)
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	for i := range 10 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 100,
			CreatedAt:    time.Now().UTC().Add(-time.Duration(i+2) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	// StorageGB=0 means no storage allowed — non-WAL backups are deleted to zero.
	// (WAL databases keep their latest full backup via the WAL-specific guard, but
	// non-WAL retention has no such requirement.)
	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 0, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err = cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(remainingBackups))
}

func Test_GetTotalSizeByDatabase_CalculatesCorrectly(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	completedBackup1 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10.5,
		CreatedAt:    time.Now().UTC(),
	}
	completedBackup2 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 20.3,
		CreatedAt:    time.Now().UTC(),
	}
	failedBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusFailed,
		BackupSizeMb: 5.2,
		CreatedAt:    time.Now().UTC(),
	}
	inProgressBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusInProgress,
		BackupSizeMb: 100,
		CreatedAt:    time.Now().UTC(),
	}

	err := backupRepository.Save(completedBackup1)
	assert.NoError(t, err)
	err = backupRepository.Save(completedBackup2)
	assert.NoError(t, err)
	err = backupRepository.Save(failedBackup)
	assert.NoError(t, err)
	err = backupRepository.Save(inProgressBackup)
	assert.NoError(t, err)

	totalSize, err := backupRepository.GetTotalSizeByDatabase(database.ID)
	assert.NoError(t, err)
	assert.InDelta(t, 36.0, totalSize, 0.1)
}

func Test_CleanByCount_KeepsNewestNBackups_DeletesOlder(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeCount,
		RetentionCount:      3,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()
	var backupIDs []uuid.UUID
	for i := range 5 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 10,
			CreatedAt: now.Add(
				-time.Duration(4-i) * time.Hour,
			), // oldest first in loop, newest = i=4
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
		backupIDs = append(backupIDs, backup.ID)
	}

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(remainingBackups))

	remainingIDs := make(map[uuid.UUID]bool)
	for _, backup := range remainingBackups {
		remainingIDs[backup.ID] = true
	}
	assert.False(t, remainingIDs[backupIDs[0]], "Oldest backup should be deleted")
	assert.False(t, remainingIDs[backupIDs[1]], "2nd oldest backup should be deleted")
	assert.True(t, remainingIDs[backupIDs[2]], "3rd backup should remain")
	assert.True(t, remainingIDs[backupIDs[3]], "4th backup should remain")
	assert.True(t, remainingIDs[backupIDs[4]], "Newest backup should remain")
}

func Test_CleanByCount_WhenUnderLimit_NoBackupsDeleted(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeCount,
		RetentionCount:      10,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	for i := range 5 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 10,
			CreatedAt:    time.Now().UTC().Add(-time.Duration(i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(remainingBackups))
}

func Test_CleanByCount_DoesNotDeleteInProgressBackups(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeCount,
		RetentionCount:      2,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()

	for i := range 3 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 10,
			CreatedAt:    now.Add(-time.Duration(3-i) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	inProgressBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusInProgress,
		BackupSizeMb: 5,
		CreatedAt:    now,
	}
	err = backupRepository.Save(inProgressBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)

	var inProgressFound bool
	for _, backup := range remainingBackups {
		if backup.ID == inProgressBackup.ID {
			inProgressFound = true
		}
	}
	assert.True(t, inProgressFound, "In-progress backup should not be deleted by count policy")
}

// Test_DeleteBackup_WhenStorageDeleteFails_BackupStillRemovedFromDatabase verifies resilience
// when storage becomes unavailable. Even if storage.DeleteFile fails (e.g., storage is offline,
// credentials changed, or storage was deleted), the backup record should still be removed from
// the database. This prevents orphaned backup records when storage is no longer accessible.
func Test_DeleteBackup_WhenStorageDeleteFails_BackupStillRemovedFromDatabase(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	testStorage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, testStorage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(testStorage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	backup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    testStorage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    time.Now().UTC(),
	}
	err := backupRepository.Save(backup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()

	err = cleaner.DeleteBackup(backup)
	assert.NoError(t, err, "DeleteBackup should succeed even when storage file doesn't exist")

	deletedBackup, err := backupRepository.FindByID(backup.ID)
	assert.Error(t, err, "Backup should not exist in database")
	assert.Nil(t, deletedBackup)
}

func Test_CleanByTimePeriod_SkipsRecentBackup_EvenIfOlderThanRetention(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	// Retention period is 1 day — any backup older than 1 day should be deleted.
	// But the recent backup was created only 30 minutes ago and must be preserved.
	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodDay,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()

	oldBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-2 * 24 * time.Hour),
	}
	recentBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-30 * time.Minute),
	}

	err = backupRepository.Save(oldBackup)
	assert.NoError(t, err)
	err = backupRepository.Save(recentBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remainingBackups))
	assert.Equal(t, recentBackup.ID, remainingBackups[0].ID)
}

func Test_CleanByCount_SkipsRecentBackup_EvenIfOverLimit(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	// Retention count is 2 — 4 backups exist so 2 should be deleted.
	// The oldest backup in the "excess" tail was made 30 min ago — it must be preserved.
	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeCount,
		RetentionCount:      2,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()

	oldBackup1 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-5 * time.Hour),
	}
	oldBackup2 := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-3 * time.Hour),
	}
	// This backup is 3rd newest and would normally be deleted — but it is recent.
	recentExcessBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-30 * time.Minute),
	}
	newestBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 10,
		CreatedAt:    now.Add(-10 * time.Minute),
	}

	for _, b := range []*backups_core.Backup{oldBackup1, oldBackup2, recentExcessBackup, newestBackup} {
		err = backupRepository.Save(b)
		assert.NoError(t, err)
	}

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)

	remainingIDs := make(map[uuid.UUID]bool)
	for _, backup := range remainingBackups {
		remainingIDs[backup.ID] = true
	}

	assert.False(t, remainingIDs[oldBackup1.ID], "Oldest non-recent backup should be deleted")
	assert.False(t, remainingIDs[oldBackup2.ID], "2nd oldest non-recent backup should be deleted")
	assert.True(
		t,
		remainingIDs[recentExcessBackup.ID],
		"Recent backup must be preserved despite being over limit",
	)
	assert.True(t, remainingIDs[newestBackup.ID], "Newest backup should be preserved")
}

func Test_CleanExceededBackups_SkipsRecentBackup_WhenOverStorageLimit(t *testing.T) {
	enableCloud(t)
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	// Total size limit = 1 GB (1024 MB). Two backups of 600 MB each (1200 MB total).
	// The oldest backup was created 30 minutes ago — within the grace period.
	// The cleaner must stop and leave both backups intact.
	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	now := time.Now().UTC()

	olderRecentBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 600,
		CreatedAt:    now.Add(-30 * time.Minute),
	}
	newerRecentBackup := &backups_core.Backup{
		ID:           uuid.New(),
		DatabaseID:   database.ID,
		StorageID:    storage.ID,
		Status:       backups_core.BackupStatusCompleted,
		BackupSizeMb: 600,
		CreatedAt:    now.Add(-10 * time.Minute),
	}

	err = backupRepository.Save(olderRecentBackup)
	assert.NoError(t, err)
	err = backupRepository.Save(newerRecentBackup)
	assert.NoError(t, err)

	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 1, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err = cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(
		t,
		2,
		len(remainingBackups),
		"Both recent backups must be preserved even though total size exceeds limit",
	)
}

func Test_CleanExceededStorageBackups_WhenNonCloud_SkipsCleanup(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodForever,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	// 5 backups at 500 MB each = 2500 MB, would exceed 1 GB limit in cloud mode
	now := time.Now().UTC()
	for i := range 5 {
		backup := &backups_core.Backup{
			ID:           uuid.New(),
			DatabaseID:   database.ID,
			StorageID:    storage.ID,
			Status:       backups_core.BackupStatusCompleted,
			BackupSizeMb: 500,
			CreatedAt:    now.Add(-time.Duration(i+2) * time.Hour),
		}
		err = backupRepository.Save(backup)
		assert.NoError(t, err)
	}

	// IsCloud is false by default — cleaner should skip entirely
	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 1, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err = cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	remainingBackups, err := backupRepository.FindByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(remainingBackups), "All backups must remain in non-cloud mode")
}

type mockBillingService struct {
	subscription *billing_models.Subscription
	err          error
}

func (m *mockBillingService) GetSubscription(
	logger *slog.Logger,
	databaseID uuid.UUID,
) (*billing_models.Subscription, error) {
	return m.subscription, m.err
}

// Mock listener for testing
type mockBackupRemoveListener struct {
	onBeforeBackupRemove func(*backups_core.Backup) error
}

func (m *mockBackupRemoveListener) OnBeforeBackupRemove(backup *backups_core.Backup) error {
	if m.onBeforeBackupRemove != nil {
		return m.onBeforeBackupRemove(backup)
	}

	return nil
}

func Test_CleanStaleUploadedBasebackups_MarksAsFailed(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	staleTime := time.Now().UTC().Add(-15 * time.Minute)
	walBackupType := backups_core.PgWalBackupTypeFullBackup
	staleBackup := &backups_core.Backup{
		ID:                uuid.New(),
		DatabaseID:        database.ID,
		StorageID:         storage.ID,
		Status:            backups_core.BackupStatusInProgress,
		PgWalBackupType:   &walBackupType,
		UploadCompletedAt: &staleTime,
		CreatedAt:         staleTime,
	}

	err := backupRepository.Save(staleBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanStaleUploadedBasebackups(testLogger())
	assert.NoError(t, err)

	updated, err := backupRepository.FindByID(staleBackup.ID)
	assert.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusFailed, updated.Status)
	assert.NotNil(t, updated.FailMessage)
	assert.Contains(t, *updated.FailMessage, "finalization timed out")
}

func Test_CleanStaleUploadedBasebackups_SkipsRecentUploads(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	recentTime := time.Now().UTC().Add(-2 * time.Minute)
	walBackupType := backups_core.PgWalBackupTypeFullBackup
	recentBackup := &backups_core.Backup{
		ID:                uuid.New(),
		DatabaseID:        database.ID,
		StorageID:         storage.ID,
		Status:            backups_core.BackupStatusInProgress,
		PgWalBackupType:   &walBackupType,
		UploadCompletedAt: &recentTime,
		CreatedAt:         recentTime,
	}

	err := backupRepository.Save(recentBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanStaleUploadedBasebackups(testLogger())
	assert.NoError(t, err)

	updated, err := backupRepository.FindByID(recentBackup.ID)
	assert.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusInProgress, updated.Status)
}

func Test_CleanStaleUploadedBasebackups_SkipsActiveStreaming(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	walBackupType := backups_core.PgWalBackupTypeFullBackup
	activeBackup := &backups_core.Backup{
		ID:              uuid.New(),
		DatabaseID:      database.ID,
		StorageID:       storage.ID,
		Status:          backups_core.BackupStatusInProgress,
		PgWalBackupType: &walBackupType,
		CreatedAt:       time.Now().UTC().Add(-30 * time.Minute),
	}

	err := backupRepository.Save(activeBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanStaleUploadedBasebackups(testLogger())
	assert.NoError(t, err)

	updated, err := backupRepository.FindByID(activeBackup.ID)
	assert.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusInProgress, updated.Status)
	assert.Nil(t, updated.UploadCompletedAt)
}

func Test_CleanStaleUploadedBasebackups_CleansStorageFiles(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestDatabase(workspace.ID, storage, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	staleTime := time.Now().UTC().Add(-15 * time.Minute)
	walBackupType := backups_core.PgWalBackupTypeFullBackup
	staleBackup := &backups_core.Backup{
		ID:                uuid.New(),
		DatabaseID:        database.ID,
		StorageID:         storage.ID,
		Status:            backups_core.BackupStatusInProgress,
		PgWalBackupType:   &walBackupType,
		UploadCompletedAt: &staleTime,
		BackupSizeMb:      500,
		FileName:          "stale-basebackup-test-file",
		CreatedAt:         staleTime,
	}

	err := backupRepository.Save(staleBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanStaleUploadedBasebackups(testLogger())
	assert.NoError(t, err)

	updated, err := backupRepository.FindByID(staleBackup.ID)
	assert.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusFailed, updated.Status)
	assert.NotNil(t, updated.FailMessage)
	assert.Contains(t, *updated.FailMessage, "finalization timed out")
}

// Reproduces issue #533: when the cleaner deletes the only completed full WAL backup,
// the agent's chain-validity check immediately reports the chain broken and triggers a
// new pg_basebackup. That new backup ages past the grace period, the cleaner deletes it
// too, and the loop never stops. The fix is for the cleaner to keep the latest completed
// full WAL backup regardless of retention policy.
func Test_CleanByRetentionPolicy_DoesNotDeleteOnlyCompletedFullWalBackup(t *testing.T) {
	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestPostgresWalDatabase(workspace.ID, notifier)

	defer func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}()

	interval := createTestInterval()

	backupConfig := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: backups_config.RetentionPolicyTypeTimePeriod,
		RetentionTimePeriod: period.PeriodDay,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionNone,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(backupConfig)
	assert.NoError(t, err)

	walBackupType := backups_core.PgWalBackupTypeFullBackup
	stopSegment := "000000010000000000000005"
	fullBackup := &backups_core.Backup{
		ID:                             uuid.New(),
		DatabaseID:                     database.ID,
		StorageID:                      storage.ID,
		Status:                         backups_core.BackupStatusCompleted,
		PgWalBackupType:                &walBackupType,
		PgFullBackupWalStopSegmentName: &stopSegment,
		BackupSizeMb:                   100,
		CreatedAt:                      time.Now().UTC().Add(-2 * 24 * time.Hour),
	}
	err = backupRepository.Save(fullBackup)
	assert.NoError(t, err)

	cleaner := GetBackupCleaner()
	err = cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	survivor, err := backupRepository.FindLastCompletedFullWalBackupByDatabaseID(database.ID)
	assert.NoError(t, err)
	assert.NotNil(
		t,
		survivor,
		"cleaner deleted the only completed full WAL backup; agent's chain check will now report no_full_backup and trigger a new pg_basebackup (issue #533)",
	)
}

func Test_CleanByTimePeriod_WhenWalBackup_KeepsLatestFullBackup_DeletesOldFullBackupsWithSegments(t *testing.T) {
	router, database, storage, cleanup := createWalRetentionFixture(t, backups_config.RetentionPolicyTypeTimePeriod)
	defer cleanup()
	_ = router

	now := time.Now().UTC()

	oldFull, oldSegs := saveWalFullBackupWithSegments(t, database.ID, storage.ID, now.Add(-3*24*time.Hour), 3)
	newFull, newSegs := saveWalFullBackupWithSegments(t, database.ID, storage.ID, now.Add(-90*time.Minute), 2)

	cleaner := GetBackupCleaner()
	err := cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	assertBackupGone(t, oldFull.ID, "old full backup must be deleted with its set")
	for _, seg := range oldSegs {
		assertBackupGone(t, seg.ID, "WAL segments must be deleted with their full backup")
	}

	assertBackupExists(t, newFull.ID, "latest full backup must survive")
	for _, seg := range newSegs {
		assertBackupExists(t, seg.ID, "WAL segments of the latest full backup must survive")
	}
}

func Test_CleanByCount_WhenWalBackup_KeepsNMostRecentFullBackups(t *testing.T) {
	router, database, storage, cleanup := createWalRetentionFixture(t, backups_config.RetentionPolicyTypeCount)
	defer cleanup()
	setRetentionCount(t, router, database.ID, 2)

	now := time.Now().UTC()

	oldestFull, oldestSegs := saveWalFullBackupWithSegments(t, database.ID, storage.ID, now.Add(-5*24*time.Hour), 2)
	middleFull, middleSegs := saveWalFullBackupWithSegments(t, database.ID, storage.ID, now.Add(-3*24*time.Hour), 2)
	newestFull, newestSegs := saveWalFullBackupWithSegments(t, database.ID, storage.ID, now.Add(-90*time.Minute), 2)

	cleaner := GetBackupCleaner()
	err := cleaner.cleanByRetentionPolicy(testLogger())
	assert.NoError(t, err)

	assertBackupGone(t, oldestFull.ID, "oldest full backup must be deleted")
	for _, seg := range oldestSegs {
		assertBackupGone(t, seg.ID, "WAL segments of the oldest full backup must be deleted")
	}

	for _, b := range append([]*backups_core.Backup{middleFull, newestFull}, append(middleSegs, newestSegs...)...) {
		assertBackupExists(t, b.ID, "two newest full backups and their WAL segments must survive count=2 retention")
	}
}

func Test_CleanExceededStorage_WhenWalBackup_DeletesFullBackupAndItsSegmentsToFreeSpace(t *testing.T) {
	enableCloud(t)
	router, database, storage, cleanup := createWalRetentionFixture(t, backups_config.RetentionPolicyTypeTimePeriod)
	defer cleanup()
	_ = router

	now := time.Now().UTC()

	// Two full backups at ~600MB each (basebackup 500MB + 2 segments x 50MB), limit = 1GB.
	// Cleaner must delete the oldest full backup together with its WAL segments,
	// not just one row at a time.
	oldFull, oldSegs := saveSizedWalFullBackupWithSegments(
		t,
		database.ID,
		storage.ID,
		now.Add(-5*time.Hour),
		500,
		[]float64{50, 50},
	)
	newFull, newSegs := saveSizedWalFullBackupWithSegments(
		t,
		database.ID,
		storage.ID,
		now.Add(-90*time.Minute),
		500,
		[]float64{50, 50},
	)

	mockBilling := &mockBillingService{
		subscription: &billing_models.Subscription{StorageGB: 1, Status: billing_models.StatusActive},
	}
	cleaner := CreateTestBackupCleaner(mockBilling)
	err := cleaner.cleanExceededStorageBackups(testLogger())
	assert.NoError(t, err)

	assertBackupGone(t, oldFull.ID, "oldest full backup must be deleted to free space")
	for _, seg := range oldSegs {
		assertBackupGone(t, seg.ID, "WAL segments must be deleted with their full backup, not orphaned")
	}

	assertBackupExists(t, newFull.ID, "latest full backup must survive")
	for _, seg := range newSegs {
		assertBackupExists(t, seg.ID, "WAL segments of the latest full backup must survive")
	}
}

func createWalRetentionFixture(
	t *testing.T,
	policy backups_config.RetentionPolicyType,
) (*gin.Engine, *databases.Database, *storages.Storage, func()) {
	t.Helper()

	router := CreateTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Test Workspace", owner, router)
	storage := storages.CreateTestStorage(workspace.ID)
	notifier := notifiers.CreateTestNotifier(workspace.ID)
	database := databases.CreateTestPostgresWalDatabase(workspace.ID, notifier)

	interval := createTestInterval()

	cfg := &backups_config.BackupConfig{
		DatabaseID:          database.ID,
		IsBackupsEnabled:    true,
		RetentionPolicyType: policy,
		RetentionTimePeriod: period.PeriodDay,
		RetentionCount:      2,
		StorageID:           &storage.ID,
		BackupIntervalID:    interval.ID,
		BackupInterval:      interval,
		Encryption:          backups_config.BackupEncryptionEncrypted,
	}
	_, err := backups_config.GetBackupConfigService().SaveBackupConfig(cfg)
	require.NoError(t, err)

	cleanup := func() {
		backups, _ := backupRepository.FindByDatabaseID(database.ID)
		for _, backup := range backups {
			_ = backupRepository.DeleteByID(backup.ID)
		}

		databases.RemoveTestDatabase(database)
		time.Sleep(50 * time.Millisecond)
		notifiers.RemoveTestNotifier(notifier)
		storages.RemoveTestStorage(storage.ID)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	}

	return router, database, storage, cleanup
}

func saveWalFullBackupWithSegments(
	t *testing.T,
	databaseID uuid.UUID,
	storageID uuid.UUID,
	fullBackupAt time.Time,
	walSegmentCount int,
) (*backups_core.Backup, []*backups_core.Backup) {
	t.Helper()

	return saveSizedWalFullBackupWithSegments(
		t,
		databaseID,
		storageID,
		fullBackupAt,
		100,
		repeatFloat(16, walSegmentCount),
	)
}

func saveSizedWalFullBackupWithSegments(
	t *testing.T,
	databaseID uuid.UUID,
	storageID uuid.UUID,
	fullBackupAt time.Time,
	fullBackupSizeMb float64,
	walSegmentSizesMb []float64,
) (*backups_core.Backup, []*backups_core.Backup) {
	t.Helper()

	fullType := backups_core.PgWalBackupTypeFullBackup
	stopSegment := "00000001000000000000000" + uuid.New().String()[:1]

	full := &backups_core.Backup{
		ID:                             uuid.New(),
		DatabaseID:                     databaseID,
		StorageID:                      storageID,
		Status:                         backups_core.BackupStatusCompleted,
		PgWalBackupType:                &fullType,
		PgFullBackupWalStopSegmentName: &stopSegment,
		BackupSizeMb:                   fullBackupSizeMb,
		CreatedAt:                      fullBackupAt,
	}
	require.NoError(t, backupRepository.Save(full))

	segType := backups_core.PgWalBackupTypeWalSegment
	segs := make([]*backups_core.Backup, 0, len(walSegmentSizesMb))
	for i, sizeMb := range walSegmentSizesMb {
		seg := &backups_core.Backup{
			ID:              uuid.New(),
			DatabaseID:      databaseID,
			StorageID:       storageID,
			Status:          backups_core.BackupStatusCompleted,
			PgWalBackupType: &segType,
			BackupSizeMb:    sizeMb,
			CreatedAt:       fullBackupAt.Add(time.Duration(i+1) * time.Minute),
		}
		require.NoError(t, backupRepository.Save(seg))

		segs = append(segs, seg)
	}

	return full, segs
}

func setRetentionCount(t *testing.T, _ *gin.Engine, databaseID uuid.UUID, count int) {
	t.Helper()

	cfg, err := backups_config.GetBackupConfigService().GetBackupConfigByDbId(databaseID)
	require.NoError(t, err)

	cfg.RetentionCount = count
	_, err = backups_config.GetBackupConfigService().SaveBackupConfig(cfg)
	require.NoError(t, err)
}

func assertBackupExists(t *testing.T, backupID uuid.UUID, msgAndArgs ...any) {
	t.Helper()

	backup, err := backupRepository.FindByID(backupID)
	assert.NoError(t, err)
	assert.NotNil(t, backup, msgAndArgs...)
}

func assertBackupGone(t *testing.T, backupID uuid.UUID, msgAndArgs ...any) {
	t.Helper()

	backup, _ := backupRepository.FindByID(backupID)
	assert.Nil(t, backup, msgAndArgs...)
}

func repeatFloat(value float64, count int) []float64 {
	out := make([]float64, count)
	for i := range out {
		out[i] = value
	}

	return out
}

func enableCloud(t *testing.T) {
	t.Helper()
	config.GetEnv().IsCloud = true
	t.Cleanup(func() {
		config.GetEnv().IsCloud = false
	})
}

func testLogger() *slog.Logger {
	return logger.GetLogger().With("task_name", "test")
}

func createTestInterval() *intervals.Interval {
	timeOfDay := "04:00"
	interval := &intervals.Interval{
		Interval:  intervals.IntervalDaily,
		TimeOfDay: &timeOfDay,
	}

	err := storage.GetDb().Create(interval).Error
	if err != nil {
		panic(err)
	}

	return interval
}
