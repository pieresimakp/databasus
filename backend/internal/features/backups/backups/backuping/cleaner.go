package backuping

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/config"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/storages"
	util_encryption "databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/period"
)

const (
	cleanerTickerInterval   = 1 * time.Minute
	recentBackupGracePeriod = 60 * time.Minute
)

type BackupCleaner struct {
	backupRepository      *backups_core.BackupRepository
	storageService        *storages.StorageService
	backupConfigService   *backups_config.BackupConfigService
	billingService        BillingService
	fieldEncryptor        util_encryption.FieldEncryptor
	logger                *slog.Logger
	backupRemoveListeners []backups_core.BackupRemoveListener

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
	staleLog := c.logger.With("task_name", "clean_stale_basebackups")

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

			if err := c.cleanStaleUploadedBasebackups(staleLog); err != nil {
				staleLog.Error("failed to clean stale uploaded basebackups", "error", err)
			}
		}
	}
}

func (c *BackupCleaner) DeleteBackup(backup *backups_core.Backup) error {
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

func (c *BackupCleaner) AddBackupRemoveListener(listener backups_core.BackupRemoveListener) {
	c.backupRemoveListeners = append(c.backupRemoveListeners, listener)
}

func (c *BackupCleaner) cleanStaleUploadedBasebackups(logger *slog.Logger) error {
	staleBackups, err := c.backupRepository.FindStaleUploadedBasebackups(
		time.Now().UTC().Add(-10 * time.Minute),
	)
	if err != nil {
		return fmt.Errorf("failed to find stale uploaded basebackups: %w", err)
	}

	for _, backup := range staleBackups {
		backupLog := logger.With("database_id", backup.DatabaseID, "backup_id", backup.ID)

		staleStorage, storageErr := c.storageService.GetStorageByID(backup.StorageID)
		if storageErr != nil {
			backupLog.Error(
				"failed to get storage for stale basebackup cleanup",
				"storage_id", backup.StorageID,
				"error", storageErr,
			)
		} else {
			if err := staleStorage.DeleteFile(c.fieldEncryptor, backup.FileName); err != nil {
				backupLog.Error(
					fmt.Sprintf("failed to delete stale basebackup file: %s", backup.FileName),
					"error",
					err,
				)
			}

			metadataFileName := backup.FileName + ".metadata"
			if err := staleStorage.DeleteFile(c.fieldEncryptor, metadataFileName); err != nil {
				backupLog.Error(
					fmt.Sprintf("failed to delete stale basebackup metadata file: %s", metadataFileName),
					"error",
					err,
				)
			}
		}

		failMsg := "basebackup finalization timed out after 10 minutes"
		backup.Status = backups_core.BackupStatusFailed
		backup.FailMessage = &failMsg

		if err := c.backupRepository.Save(backup); err != nil {
			backupLog.Error("failed to mark stale uploaded basebackup as failed", "error", err)
			continue
		}

		backupLog.Info("marked stale uploaded basebackup as failed and cleaned storage")
	}

	return nil
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
		case backups_config.RetentionPolicyTypeCount:
			cleanErr = c.cleanByCount(dbLog, backupConfig)
		case backups_config.RetentionPolicyTypeGFS:
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

func (c *BackupCleaner) cleanByTimePeriod(logger *slog.Logger, backupConfig *backups_config.BackupConfig) error {
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

		c.deleteBackupAndCascadeWalSegments(logger, backup, "deleted old backup")
	}

	return nil
}

func (c *BackupCleaner) cleanByCount(logger *slog.Logger, backupConfig *backups_config.BackupConfig) error {
	if backupConfig.RetentionCount <= 0 {
		return nil
	}

	fullBackups, err := c.findCompletedFullBackups(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	if len(fullBackups) <= backupConfig.RetentionCount {
		return nil
	}

	successMsg := fmt.Sprintf("deleted backup by count policy: retention count is %d", backupConfig.RetentionCount)
	for _, backup := range fullBackups[backupConfig.RetentionCount:] {
		if isRecentBackup(backup) {
			continue
		}

		c.deleteBackupAndCascadeWalSegments(logger, backup, successMsg)
	}

	return nil
}

func (c *BackupCleaner) cleanByGFS(logger *slog.Logger, backupConfig *backups_config.BackupConfig) error {
	if backupConfig.RetentionGfsHours <= 0 && backupConfig.RetentionGfsDays <= 0 &&
		backupConfig.RetentionGfsWeeks <= 0 && backupConfig.RetentionGfsMonths <= 0 &&
		backupConfig.RetentionGfsYears <= 0 {
		return nil
	}

	fullBackups, err := c.findCompletedFullBackups(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	keepSet := buildGFSKeepSet(
		fullBackups,
		backupConfig.RetentionGfsHours,
		backupConfig.RetentionGfsDays,
		backupConfig.RetentionGfsWeeks,
		backupConfig.RetentionGfsMonths,
		backupConfig.RetentionGfsYears,
	)

	for _, backup := range fullBackups {
		if keepSet[backup.ID] {
			continue
		}

		if isRecentBackup(backup) {
			continue
		}

		c.deleteBackupAndCascadeWalSegments(logger, backup, "deleted backup by GFS policy")
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

		fullBackups, err := c.findCompletedFullBackups(databaseID)
		if err != nil {
			return err
		}

		oldestDeletable := c.findOldestDeletableFullBackup(fullBackups)
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
		if !c.deleteBackupAndCascadeWalSegments(logger, oldestDeletable, successMsg) {
			break
		}
	}

	return nil
}

// deleteBackupAndCascadeWalSegments refuses to delete the latest full WAL backup (would
// break the agent's chain check, see issue #533), skips lone WAL segments (deleted only
// alongside their parent), and otherwise deletes the backup plus any dependent WAL
// segments. Returns true when a backup row was removed.
func (c *BackupCleaner) deleteBackupAndCascadeWalSegments(
	logger *slog.Logger,
	backup *backups_core.Backup,
	successMessage string,
) bool {
	if isWalSegmentBackup(backup) {
		return false
	}

	if !isFullWalBackup(backup) {
		if err := c.DeleteBackup(backup); err != nil {
			logger.Error("failed to delete backup", "backup_id", backup.ID, "error", err)
			return false
		}

		logger.Info(successMessage, "backup_id", backup.ID)

		return true
	}

	if c.isLatestFullWalBackup(backup) {
		return false
	}

	dependentSegments, err := c.findWalSegmentsForFullBackup(backup)
	if err != nil {
		logger.Error("failed to load WAL segments for cascade delete", "backup_id", backup.ID, "error", err)
		return false
	}

	for i := len(dependentSegments) - 1; i >= 0; i-- {
		seg := dependentSegments[i]
		if err := c.DeleteBackup(seg); err != nil {
			logger.Error("failed to delete WAL segment in cascade", "backup_id", seg.ID, "error", err)
		}
	}

	if err := c.DeleteBackup(backup); err != nil {
		logger.Error("failed to delete full backup in cascade", "backup_id", backup.ID, "error", err)
		return false
	}

	logger.Info(successMessage, "backup_id", backup.ID, "wal_segments_deleted", len(dependentSegments))

	return true
}

// findCompletedFullBackups returns completed full backups (WAL or non-WAL). WAL
// segments are excluded — they ride with their parent full backup, not standalone.
// Ordered newest-first to match the source repo query.
func (c *BackupCleaner) findCompletedFullBackups(databaseID uuid.UUID) ([]*backups_core.Backup, error) {
	completed, err := c.backupRepository.FindByDatabaseIdAndStatus(databaseID, backups_core.BackupStatusCompleted)
	if err != nil {
		return nil, fmt.Errorf("failed to find completed backups for database %s: %w", databaseID, err)
	}

	fullBackups := make([]*backups_core.Backup, 0, len(completed))
	for _, b := range completed {
		if !isWalSegmentBackup(b) {
			fullBackups = append(fullBackups, b)
		}
	}

	return fullBackups, nil
}

// findOldestDeletableFullBackup walks newest-first input from the oldest end and
// returns the first backup that's both past the grace period and not the chain-anchoring
// latest full WAL backup. Returns nil when nothing is deletable.
func (c *BackupCleaner) findOldestDeletableFullBackup(
	fullBackupsNewestFirst []*backups_core.Backup,
) *backups_core.Backup {
	for i := len(fullBackupsNewestFirst) - 1; i >= 0; i-- {
		candidate := fullBackupsNewestFirst[i]

		if isRecentBackup(candidate) {
			continue
		}

		if c.isLatestFullWalBackup(candidate) {
			continue
		}

		return candidate
	}

	return nil
}

// findWalSegmentsForFullBackup returns the WAL segments uploaded after fullBackup but
// before the next completed full backup (or all later segments if fullBackup is the
// most recent). These belong to fullBackup's restore chain and are deleted with it.
func (c *BackupCleaner) findWalSegmentsForFullBackup(fullBackup *backups_core.Backup) ([]*backups_core.Backup, error) {
	completedBackups, err := c.backupRepository.FindByDatabaseIdAndStatus(
		fullBackup.DatabaseID,
		backups_core.BackupStatusCompleted,
	)
	if err != nil {
		return nil, err
	}

	var nextFullCreatedAt *time.Time
	for _, b := range completedBackups {
		if !isFullWalBackup(b) || !b.CreatedAt.After(fullBackup.CreatedAt) {
			continue
		}

		if nextFullCreatedAt == nil || b.CreatedAt.Before(*nextFullCreatedAt) {
			t := b.CreatedAt
			nextFullCreatedAt = &t
		}
	}

	var dependentSegments []*backups_core.Backup
	for _, candidate := range completedBackups {
		if !isWalSegmentBackup(candidate) {
			continue
		}

		if candidate.CreatedAt.Before(fullBackup.CreatedAt) {
			continue
		}

		if nextFullCreatedAt != nil && !candidate.CreatedAt.Before(*nextFullCreatedAt) {
			continue
		}

		dependentSegments = append(dependentSegments, candidate)
	}

	return dependentSegments, nil
}

func (c *BackupCleaner) isLatestFullWalBackup(backup *backups_core.Backup) bool {
	if !isFullWalBackup(backup) {
		return false
	}

	latest, err := c.backupRepository.FindLastCompletedFullWalBackupByDatabaseID(backup.DatabaseID)
	if err != nil || latest == nil {
		return false
	}

	return latest.ID == backup.ID
}

func isFullWalBackup(backup *backups_core.Backup) bool {
	return backup.PgWalBackupType != nil &&
		*backup.PgWalBackupType == backups_core.PgWalBackupTypeFullBackup
}

func isWalSegmentBackup(backup *backups_core.Backup) bool {
	return backup.PgWalBackupType != nil &&
		*backup.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment
}

func isRecentBackup(backup *backups_core.Backup) bool {
	return time.Since(backup.CreatedAt) < recentBackupGracePeriod
}

// buildGFSKeepSet determines which backups to retain under the GFS rotation scheme.
// Backups must be sorted newest-first. A backup can fill multiple slots simultaneously
// (e.g. the newest backup of a year also fills the monthly, weekly, daily, and hourly slot).
func buildGFSKeepSet(
	backups []*backups_core.Backup,
	hours, days, weeks, months, years int,
) map[uuid.UUID]bool {
	keep := make(map[uuid.UUID]bool)

	if len(backups) == 0 {
		return keep
	}

	hoursSeen := make(map[string]bool)
	daysSeen := make(map[string]bool)
	weeksSeen := make(map[string]bool)
	monthsSeen := make(map[string]bool)
	yearsSeen := make(map[string]bool)

	hoursKept, daysKept, weeksKept, monthsKept, yearsKept := 0, 0, 0, 0, 0

	// Compute per-level time-window cutoffs so higher-frequency slots
	// cannot absorb backups that belong to lower-frequency levels.
	ref := backups[0].CreatedAt

	rawHourlyCutoff := ref.Add(-time.Duration(hours) * time.Hour)
	rawDailyCutoff := ref.Add(-time.Duration(days) * 24 * time.Hour)
	rawWeeklyCutoff := ref.Add(-time.Duration(weeks) * 7 * 24 * time.Hour)
	rawMonthlyCutoff := ref.AddDate(0, -months, 0)
	rawYearlyCutoff := ref.AddDate(-years, 0, 0)

	// Hierarchical capping: each level's window cannot extend further back
	// than the nearest active lower-frequency level's window.
	yearlyCutoff := rawYearlyCutoff

	monthlyCutoff := rawMonthlyCutoff
	if years > 0 {
		monthlyCutoff = laterOf(monthlyCutoff, yearlyCutoff)
	}

	weeklyCutoff := rawWeeklyCutoff
	if months > 0 {
		weeklyCutoff = laterOf(weeklyCutoff, monthlyCutoff)
	} else if years > 0 {
		weeklyCutoff = laterOf(weeklyCutoff, yearlyCutoff)
	}

	dailyCutoff := rawDailyCutoff
	switch {
	case weeks > 0:
		dailyCutoff = laterOf(dailyCutoff, weeklyCutoff)
	case months > 0:
		dailyCutoff = laterOf(dailyCutoff, monthlyCutoff)
	case years > 0:
		dailyCutoff = laterOf(dailyCutoff, yearlyCutoff)
	}

	hourlyCutoff := rawHourlyCutoff
	switch {
	case days > 0:
		hourlyCutoff = laterOf(hourlyCutoff, dailyCutoff)
	case weeks > 0:
		hourlyCutoff = laterOf(hourlyCutoff, weeklyCutoff)
	case months > 0:
		hourlyCutoff = laterOf(hourlyCutoff, monthlyCutoff)
	case years > 0:
		hourlyCutoff = laterOf(hourlyCutoff, yearlyCutoff)
	}

	for _, backup := range backups {
		t := backup.CreatedAt

		hourKey := t.Format("2006-01-02-15")
		dayKey := t.Format("2006-01-02")
		weekYear, week := t.ISOWeek()
		weekKey := fmt.Sprintf("%d-%02d", weekYear, week)
		monthKey := t.Format("2006-01")
		yearKey := t.Format("2006")

		if hours > 0 && hoursKept < hours && !hoursSeen[hourKey] && t.After(hourlyCutoff) {
			keep[backup.ID] = true
			hoursSeen[hourKey] = true
			hoursKept++
		}

		if days > 0 && daysKept < days && !daysSeen[dayKey] && t.After(dailyCutoff) {
			keep[backup.ID] = true
			daysSeen[dayKey] = true
			daysKept++
		}

		if weeks > 0 && weeksKept < weeks && !weeksSeen[weekKey] && t.After(weeklyCutoff) {
			keep[backup.ID] = true
			weeksSeen[weekKey] = true
			weeksKept++
		}

		if months > 0 && monthsKept < months && !monthsSeen[monthKey] && t.After(monthlyCutoff) {
			keep[backup.ID] = true
			monthsSeen[monthKey] = true
			monthsKept++
		}

		if years > 0 && yearsKept < years && !yearsSeen[yearKey] && t.After(yearlyCutoff) {
			keep[backup.ID] = true
			yearsSeen[yearKey] = true
			yearsKept++
		}
	}

	return keep
}

func laterOf(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}

	return b
}
