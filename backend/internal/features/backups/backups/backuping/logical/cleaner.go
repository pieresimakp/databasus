package backuping_logical

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/shared/gfs"
	backups_core_logical "databasus-backend/internal/features/backups/backups/core/logical"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/storages"
	util_encryption "databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/period"
)

const (
	cleanerTickerInterval   = 3 * time.Second
	recentBackupGracePeriod = 60 * time.Minute
)

type BackupCleaner struct {
	backupRepository      *backups_core_logical.BackupRepository
	storageService        *storages.StorageService
	backupConfigService   *backups_config_logical.BackupConfigService
	billingService        BillingService
	fieldEncryptor        util_encryption.FieldEncryptor
	logger                *slog.Logger
	backupRemoveListeners []backups_core_logical.BackupRemoveListener

	hasRun atomic.Bool
}

func (c *BackupCleaner) Run(ctx context.Context) {
	if c.hasRun.Swap(true) {
		panic(fmt.Sprintf("%T.Run() called multiple times", c))
	}

	if ctx.Err() != nil {
		return
	}

	retentionLog := c.logger.With("task_name", "clean_by_retention_policy")
	exceededLog := c.logger.With("task_name", "clean_exceeded_storage_backups")

	ticker := time.NewTicker(cleanerTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.cleanByRetentionPolicy(retentionLog); err != nil {
				retentionLog.Error("failed to clean backups by retention policy", "error", err)
			}

			if err := c.cleanExceededStorageBackups(exceededLog); err != nil {
				exceededLog.Error("failed to clean exceeded backups", "error", err)
			}
		}
	}
}

func (c *BackupCleaner) DeleteBackup(backup *backups_core_logical.LogicalBackup) error {
	for _, listener := range c.backupRemoveListeners {
		if err := listener.OnBeforeBackupRemove(backup); err != nil {
			return err
		}
	}

	storage, err := c.storageService.GetStorageByID(backup.StorageID)
	if err != nil {
		return err
	}

	if err := storage.DeleteFile(c.fieldEncryptor, backup.FileName); err != nil {
		// we do not return error here, because sometimes clean up performed
		// before unavailable storage removal or change - therefore we should
		// proceed even in case of error. It's possible that some S3 or
		// storage is not available yet, it should not block us
		c.logger.Error("Failed to delete backup file", "error", err)
	}

	metadataFileName := backup.FileName + ".metadata"
	if err := storage.DeleteFile(c.fieldEncryptor, metadataFileName); err != nil {
		c.logger.Error("Failed to delete backup metadata file", "error", err)
	}

	return c.backupRepository.DeleteByID(backup.ID)
}

func (c *BackupCleaner) AddBackupRemoveListener(listener backups_core_logical.BackupRemoveListener) {
	c.backupRemoveListeners = append(c.backupRemoveListeners, listener)
}

func (c *BackupCleaner) cleanByRetentionPolicy(logger *slog.Logger) error {
	enabledBackupConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledBackupConfigs {
		dbLog := logger.With("database_id", backupConfig.DatabaseID, "policy", backupConfig.RetentionPolicyType)

		var cleanErr error

		switch backupConfig.RetentionPolicyType {
		case backups_config_logical.RetentionPolicyTypeCount:
			cleanErr = c.cleanByCount(dbLog, backupConfig)
		case backups_config_logical.RetentionPolicyTypeGFS:
			cleanErr = c.cleanByGFS(dbLog, backupConfig)
		default:
			cleanErr = c.cleanByTimePeriod(dbLog, backupConfig)
		}

		if cleanErr != nil {
			dbLog.Error("failed to clean backups by retention policy", "error", cleanErr)
		}
	}

	return nil
}

func (c *BackupCleaner) cleanExceededStorageBackups(logger *slog.Logger) error {
	if !config.GetEnv().IsCloud {
		return nil
	}

	enabledBackupConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledBackupConfigs {
		dbLog := logger.With("database_id", backupConfig.DatabaseID)

		subscription, subErr := c.billingService.GetSubscription(dbLog, backupConfig.DatabaseID)
		if subErr != nil {
			dbLog.Error("failed to get subscription for exceeded backups check", "error", subErr)
			continue
		}

		storageLimitMB := int64(subscription.GetBackupsStorageGB()) * 1024

		if err := c.cleanExceededBackupsForDatabase(dbLog, backupConfig.DatabaseID, storageLimitMB); err != nil {
			dbLog.Error("failed to clean exceeded backups for database", "error", err)
			continue
		}
	}

	return nil
}

func (c *BackupCleaner) cleanByTimePeriod(
	logger *slog.Logger,
	backupConfig *backups_config_logical.LogicalBackupConfig,
) error {
	if backupConfig.RetentionTimePeriod == "" {
		return nil
	}

	if backupConfig.RetentionTimePeriod == period.PeriodForever {
		return nil
	}

	cutoff := time.Now().UTC().Add(-backupConfig.RetentionTimePeriod.ToDuration())

	oldBackups, err := c.backupRepository.FindBackupsBeforeDate(backupConfig.DatabaseID, cutoff)
	if err != nil {
		return fmt.Errorf("failed to find old backups for database %s: %w", backupConfig.DatabaseID, err)
	}

	for _, backup := range oldBackups {
		if isRecentBackup(backup) {
			continue
		}

		if err := c.DeleteBackup(backup); err != nil {
			logger.Error("failed to delete backup", "backup_id", backup.ID, "error", err)
			continue
		}

		logger.Info("deleted old backup", "backup_id", backup.ID)
	}

	return nil
}

func (c *BackupCleaner) cleanByCount(
	logger *slog.Logger,
	backupConfig *backups_config_logical.LogicalBackupConfig,
) error {
	if backupConfig.RetentionCount <= 0 {
		return nil
	}

	completedBackups, err := c.findCompletedBackups(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	if len(completedBackups) <= backupConfig.RetentionCount {
		return nil
	}

	successMsg := fmt.Sprintf("deleted backup by count policy: retention count is %d", backupConfig.RetentionCount)
	for _, backup := range completedBackups[backupConfig.RetentionCount:] {
		if isRecentBackup(backup) {
			continue
		}

		if err := c.DeleteBackup(backup); err != nil {
			logger.Error("failed to delete backup", "backup_id", backup.ID, "error", err)
			continue
		}

		logger.Info(successMsg, "backup_id", backup.ID)
	}

	return nil
}

func (c *BackupCleaner) cleanByGFS(
	logger *slog.Logger,
	backupConfig *backups_config_logical.LogicalBackupConfig,
) error {
	if backupConfig.RetentionGfsHours <= 0 && backupConfig.RetentionGfsDays <= 0 &&
		backupConfig.RetentionGfsWeeks <= 0 && backupConfig.RetentionGfsMonths <= 0 &&
		backupConfig.RetentionGfsYears <= 0 {
		return nil
	}

	completedBackups, err := c.findCompletedBackups(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	keepSet := buildGFSKeepSet(
		completedBackups,
		backupConfig.RetentionGfsHours,
		backupConfig.RetentionGfsDays,
		backupConfig.RetentionGfsWeeks,
		backupConfig.RetentionGfsMonths,
		backupConfig.RetentionGfsYears,
	)

	for _, backup := range completedBackups {
		if keepSet[backup.ID] {
			continue
		}

		if isRecentBackup(backup) {
			continue
		}

		if err := c.DeleteBackup(backup); err != nil {
			logger.Error("failed to delete backup", "backup_id", backup.ID, "error", err)
			continue
		}

		logger.Info("deleted backup by GFS policy", "backup_id", backup.ID)
	}

	return nil
}

func (c *BackupCleaner) cleanExceededBackupsForDatabase(
	logger *slog.Logger,
	databaseID uuid.UUID,
	limitPerDbMB int64,
) error {
	for {
		totalSizeMB, err := c.backupRepository.GetTotalSizeByDatabase(databaseID)
		if err != nil {
			return err
		}

		if totalSizeMB <= float64(limitPerDbMB) {
			break
		}

		completedBackups, err := c.findCompletedBackups(databaseID)
		if err != nil {
			return err
		}

		oldestDeletable := findOldestDeletable(completedBackups)
		if oldestDeletable == nil {
			logger.Warn(fmt.Sprintf(
				"no backup to delete but still over limit: total size is %.1f MB, limit is %d MB",
				totalSizeMB, limitPerDbMB,
			))
			break
		}

		successMsg := fmt.Sprintf(
			"deleted exceeded backup: backup size is %.1f MB, total size is %.1f MB, limit is %d MB",
			oldestDeletable.BackupSizeMb, totalSizeMB, limitPerDbMB,
		)

		if err := c.DeleteBackup(oldestDeletable); err != nil {
			logger.Error("failed to delete backup", "backup_id", oldestDeletable.ID, "error", err)
			break
		}

		logger.Info(successMsg, "backup_id", oldestDeletable.ID)
	}

	return nil
}

func (c *BackupCleaner) findCompletedBackups(databaseID uuid.UUID) ([]*backups_core_logical.LogicalBackup, error) {
	completed, err := c.backupRepository.FindByDatabaseIdAndStatus(
		databaseID,
		backups_core_logical.BackupStatusCompleted,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to find completed backups for database %s: %w", databaseID, err)
	}

	return completed, nil
}

func findOldestDeletable(
	completedBackupsNewestFirst []*backups_core_logical.LogicalBackup,
) *backups_core_logical.LogicalBackup {
	for i := len(completedBackupsNewestFirst) - 1; i >= 0; i-- {
		candidate := completedBackupsNewestFirst[i]

		if isRecentBackup(candidate) {
			continue
		}

		return candidate
	}

	return nil
}

func isRecentBackup(backup *backups_core_logical.LogicalBackup) bool {
	return time.Since(backup.CreatedAt) < recentBackupGracePeriod
}

// buildGFSKeepSet projects logical backups onto the shared GFS keep-set
// algorithm. Backups must be sorted newest-first.
func buildGFSKeepSet(
	backups []*backups_core_logical.LogicalBackup,
	hours, days, weeks, months, years int,
) map[uuid.UUID]bool {
	items := make([]gfs.Item, len(backups))
	for i, backup := range backups {
		items[i] = gfs.Item{ID: backup.ID, CreatedAt: backup.CreatedAt}
	}

	return gfs.GetItemsToRetain(items, hours, days, weeks, months, years)
}
