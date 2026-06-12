package backuping_logical

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	backups_core_logical "databasus-backend/internal/features/backups/backups/core/logical"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/databases"
	task_cancellation "databasus-backend/internal/features/tasks/cancellation"
)

const (
	schedulerStartupDelay         = 1 * time.Minute
	schedulerTickerInterval       = 15 * time.Second
	schedulerHealthcheckThreshold = 5 * time.Minute
)

type BackupsScheduler struct {
	backupRepository      *backups_core_logical.BackupRepository
	backupConfigService   *backups_config_logical.BackupConfigService
	taskCancelManager     *task_cancellation.TaskCancelManager
	assignmentCoordinator *nodes.NodeAssignmentCoordinator
	databaseService       *databases.DatabaseService
	billingService        BillingService

	lastBackupTime time.Time
	logger         *slog.Logger

	backuperNode *BackuperNode

	backupCompletionListeners []backups_core_logical.BackupCompletionListener

	hasRun  atomic.Bool
	isReady atomic.Bool
}

// IsRunning reports whether the scheduler has subscribed to backup
// completions and is ready to receive them. Tests use it to gate the
// start of work
func (s *BackupsScheduler) IsRunning() bool {
	return s.isReady.Load()
}

func (s *BackupsScheduler) AddBackupCompletionListener(
	listener backups_core_logical.BackupCompletionListener,
) {
	s.backupCompletionListeners = append(s.backupCompletionListeners, listener)
}

func (s *BackupsScheduler) Run(ctx context.Context) {
	if s.hasRun.Swap(true) {
		panic(fmt.Sprintf("%T.Run() called multiple times", s))
	}

	s.lastBackupTime = time.Now().UTC()

	if config.GetEnv().IsManyNodesMode {
		// wait other nodes to start
		time.Sleep(schedulerStartupDelay)
	}

	if err := s.failBackupsInProgress(); err != nil {
		s.logger.Error("Failed to fail backups in progress", "error", err)
		panic(err)
	}

	err := s.assignmentCoordinator.SubscribeForBackupsCompletions(s.onBackupCompleted)
	if err != nil {
		s.logger.Error("Failed to subscribe to backup completions", "error", err)
		panic(err)
	}

	s.isReady.Store(true)

	defer func() {
		s.isReady.Store(false)

		if err := s.assignmentCoordinator.UnsubscribeForBackupsCompletions(); err != nil {
			s.logger.Error("Failed to unsubscribe from backup completions", "error", err)
		}
	}()

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(schedulerTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.checkDeadNodesAndFailBackups(); err != nil {
				s.logger.Error("Failed to check dead nodes and fail backups", "error", err)
			}

			if err := s.runPendingBackups(); err != nil {
				s.logger.Error("Failed to run pending backups", "error", err)
			}

			s.lastBackupTime = time.Now().UTC()
		}
	}
}

func (s *BackupsScheduler) IsSchedulerRunning() bool {
	// if last backup time is more than 5 minutes ago, return false
	return s.lastBackupTime.After(time.Now().UTC().Add(-schedulerHealthcheckThreshold))
}

func (s *BackupsScheduler) IsBackupNodesAvailable() bool {
	hasNodes, err := s.assignmentCoordinator.HasAvailableNodes()
	if err != nil {
		s.logger.Error("Failed to get available nodes for health check", "error", err)
		return false
	}

	return hasNodes
}

func (s *BackupsScheduler) StartBackup(database *databases.Database, isCallNotifier bool) {
	backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(database.ID)
	if err != nil {
		s.logger.Error("Failed to get backup config by database ID", "error", err)
		return
	}

	if backupConfig.StorageID == nil {
		s.logger.Error("Backup config storage ID is nil", "databaseId", database.ID)
		return
	}

	if config.GetEnv().IsCloud {
		subscription, subErr := s.billingService.GetSubscription(s.logger, database.ID)
		if subErr != nil || !subscription.CanCreateNewBackups() {
			failMessage := "subscription has expired, please renew"
			backup := &backups_core_logical.LogicalBackup{
				ID:          uuid.New(),
				DatabaseID:  database.ID,
				StorageID:   *backupConfig.StorageID,
				Status:      backups_core_logical.BackupStatusFailed,
				FailMessage: &failMessage,
				IsSkipRetry: true,
				CreatedAt:   time.Now().UTC(),
			}

			backup.GenerateFilename(database.Name)

			if err := s.backupRepository.Save(backup); err != nil {
				s.logger.Error(
					"failed to save failed backup for expired subscription",
					"database_id", database.ID,
					"error", err,
				)
			}

			return
		}
	}

	// Check for existing in-progress backups
	inProgressBackups, err := s.backupRepository.FindByDatabaseIdAndStatus(
		database.ID,
		backups_core_logical.BackupStatusInProgress,
	)
	if err != nil {
		s.logger.Error(
			"Failed to check for in-progress backups",
			"databaseId",
			database.ID,
			"error",
			err,
		)
		return
	}

	if len(inProgressBackups) > 0 {
		s.logger.Warn(
			"Backup already in progress for database, skipping new backup",
			"databaseId",
			database.ID,
			"existingBackupId",
			inProgressBackups[0].ID,
		)
		return
	}

	// Pick the node BEFORE persisting the row so a "no nodes available" outcome
	// leaves no orphaned IN_PROGRESS backup behind.
	leastBusyNodeID, err := s.assignmentCoordinator.PickLeastBusyNode()
	if err != nil {
		s.logger.Error(
			"Failed to calculate least busy node",
			"databaseId",
			backupConfig.DatabaseID,
			"error",
			err,
		)
		return
	}

	backupID := uuid.New()
	timestamp := time.Now().UTC()

	backup := &backups_core_logical.LogicalBackup{
		ID:           backupID,
		DatabaseID:   backupConfig.DatabaseID,
		StorageID:    *backupConfig.StorageID,
		Status:       backups_core_logical.BackupStatusInProgress,
		BackupSizeMb: 0,
		CreatedAt:    timestamp,
	}

	backup.GenerateFilename(database.Name)

	if err := s.backupRepository.Save(backup); err != nil {
		s.logger.Error(
			"Failed to save backup",
			"databaseId",
			backupConfig.DatabaseID,
			"error",
			err,
		)
		return
	}

	if err := s.assignmentCoordinator.Assign(leastBusyNodeID, backup.ID, isCallNotifier); err != nil {
		s.logger.Error(
			"Failed to assign backup to node",
			"databaseId",
			backupConfig.DatabaseID,
			"backupId",
			backup.ID,
			"error",
			err,
		)
		return
	}

	s.logger.Info(
		"Successfully triggered scheduled backup",
		"databaseId",
		backupConfig.DatabaseID,
		"backupId",
		backup.ID,
		"nodeId",
		leastBusyNodeID,
	)
}

// GetRemainedBackupTryCount returns the number of remaining backup tries for a given backup.
// If the backup is not failed or the backup config does not allow retries, it returns 0.
// If the backup is failed and the backup config allows retries, it returns the number of remaining tries.
// If the backup is failed and the backup config does not allow retries, it returns 0.
func (s *BackupsScheduler) GetRemainedBackupTryCount(lastBackup *backups_core_logical.LogicalBackup) int {
	if lastBackup == nil {
		return 0
	}

	if lastBackup.Status != backups_core_logical.BackupStatusFailed {
		return 0
	}

	if lastBackup.IsSkipRetry {
		return 0
	}

	backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(lastBackup.DatabaseID)
	if err != nil {
		s.logger.Error("Failed to get backup config by database ID", "error", err)
		return 0
	}

	if !backupConfig.IsRetryIfFailed {
		return 0
	}

	maxFailedTriesCount := backupConfig.MaxFailedTriesCount

	lastBackups, err := s.backupRepository.FindByDatabaseIDWithLimit(
		lastBackup.DatabaseID,
		maxFailedTriesCount,
	)
	if err != nil {
		s.logger.Error("Failed to find last backups by database ID", "error", err)
		return 0
	}

	lastFailedBackups := make([]*backups_core_logical.LogicalBackup, 0)

	for _, backup := range lastBackups {
		if backup.Status == backups_core_logical.BackupStatusFailed {
			lastFailedBackups = append(lastFailedBackups, backup)
		}
	}

	return maxFailedTriesCount - len(lastFailedBackups)
}

func (s *BackupsScheduler) runPendingBackups() error {
	enabledBackupConfigs, err := s.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledBackupConfigs {
		lastBackup, err := s.backupRepository.FindLastByDatabaseID(backupConfig.DatabaseID)
		if err != nil {
			s.logger.Error(
				"Failed to get last backup for database",
				"databaseId",
				backupConfig.DatabaseID,
				"error",
				err,
			)
			continue
		}

		var lastBackupTime *time.Time
		if lastBackup != nil {
			lastBackupTime = &lastBackup.CreatedAt
		}

		remainedBackupTryCount := s.GetRemainedBackupTryCount(lastBackup)

		if backupConfig.BackupInterval.ShouldTriggerBackup(time.Now().UTC(), lastBackupTime) ||
			remainedBackupTryCount > 0 {
			s.logger.Info(
				"Triggering scheduled backup",
				"databaseId",
				backupConfig.DatabaseID,
				"intervalType",
				backupConfig.BackupInterval.Type,
			)

			database, err := s.databaseService.GetDatabaseByID(backupConfig.DatabaseID)
			if err != nil {
				s.logger.Error("Failed to get database by ID", "error", err)
				continue
			}

			if config.GetEnv().IsCloud {
				subscription, subErr := s.billingService.GetSubscription(s.logger, backupConfig.DatabaseID)
				if subErr != nil {
					s.logger.Warn(
						"failed to get subscription, skipping backup",
						"database_id", backupConfig.DatabaseID,
						"error", subErr,
					)
					continue
				}

				if !subscription.CanCreateNewBackups() {
					s.logger.Debug(
						"subscription is not active, skipping scheduled backup",
						"database_id", backupConfig.DatabaseID,
						"subscription_status", subscription.Status,
					)
					continue
				}
			}

			s.StartBackup(database, remainedBackupTryCount == 1)
			continue
		}
	}

	return nil
}

func (s *BackupsScheduler) failBackupsInProgress() error {
	backupsInProgress, err := s.backupRepository.FindByStatus(backups_core_logical.BackupStatusInProgress)
	if err != nil {
		return err
	}

	for _, backup := range backupsInProgress {
		if err := s.taskCancelManager.CancelTask(backup.ID); err != nil {
			s.logger.Error(
				"Failed to cancel backup via task cancel manager",
				"backupId",
				backup.ID,
				"error",
				err,
			)
		}

		backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(backup.DatabaseID)
		if err != nil {
			s.logger.Error("Failed to get backup config by database ID", "error", err)
			continue
		}

		failMessage := "Backup failed due to application restart"
		backup.FailMessage = &failMessage
		backup.Status = backups_core_logical.BackupStatusFailed
		backup.BackupSizeMb = 0

		s.backuperNode.SendBackupNotification(
			backupConfig,
			backup,
			backups_config_logical.NotificationBackupFailed,
			&failMessage,
		)

		if err := s.backupRepository.Save(backup); err != nil {
			return err
		}
	}

	return nil
}

func (s *BackupsScheduler) onBackupCompleted(nodeID, backupID uuid.UUID) {
	// Verify this task is actually a backup (registry contains multiple task types)
	_, err := s.backupRepository.FindByID(backupID)
	if err != nil {
		// Not a backup task, ignore it
		return
	}

	for _, listener := range s.backupCompletionListeners {
		go listener.OnBackupCompleted(backupID)
	}

	s.assignmentCoordinator.Release(nodeID, backupID)
}

func (s *BackupsScheduler) checkDeadNodesAndFailBackups() error {
	return s.assignmentCoordinator.HandleDeadNodes(s.failDeadNodeBackup)
}

func (s *BackupsScheduler) failDeadNodeBackup(nodeID, backupID uuid.UUID) bool {
	backup, err := s.backupRepository.FindByID(backupID)
	if err != nil {
		s.logger.Error(
			"Failed to find backup for dead node",
			"nodeId", nodeID,
			"backupId", backupID,
			"error", err,
		)

		return false
	}

	failMessage := "Backup failed due to node unavailability"
	backup.FailMessage = &failMessage
	backup.Status = backups_core_logical.BackupStatusFailed
	backup.BackupSizeMb = 0

	if err := s.backupRepository.Save(backup); err != nil {
		s.logger.Error(
			"Failed to save failed backup for dead node",
			"nodeId", nodeID,
			"backupId", backupID,
			"error", err,
		)

		return false
	}

	s.logger.Info("Failed backup due to dead node", "nodeId", nodeID, "backupId", backupID)

	return true
}
