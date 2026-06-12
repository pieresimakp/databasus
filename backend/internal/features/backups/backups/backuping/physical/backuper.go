package backuping_physical

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	backups_core_enums "databasus-backend/internal/features/backups/backups/core/enums"
	physical_enums "databasus-backend/internal/features/backups/backups/core/physical/enums"
	physical_models "databasus-backend/internal/features/backups/backups/core/physical/models"
	physical_repositories "databasus-backend/internal/features/backups/backups/core/physical/repositories"
	postgresql_executor "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	backups_config_physical "databasus-backend/internal/features/backups/config/physical"
	"databasus-backend/internal/features/databases"
	encryption_secrets "databasus-backend/internal/features/encryption/secrets"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	"databasus-backend/internal/storage"
	util_encryption "databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/walmath"
)

const (
	heartbeatTickerInterval = 15 * time.Second
	heartbeatStaleThreshold = 5 * time.Minute
)

// PhysicalBackuperNode is the per-node worker that consumes backup:submit
// assignments and drives a FULL or INCR through the postgresql executor.
type PhysicalBackuperNode struct {
	databaseService     *databases.DatabaseService
	fieldEncryptor      util_encryption.FieldEncryptor
	workspaceService    *workspaces_services.WorkspaceService
	fullRepo            *physical_repositories.PhysicalFullBackupRepository
	incrRepo            *physical_repositories.PhysicalIncrementalBackupRepository
	inFlightRepo        *physical_repositories.PhysicalInFlightBackupRepository
	historyRepo         *physical_repositories.PhysicalWalHistoryRepository
	backupConfigService *backups_config_physical.BackupConfigService
	storageService      *storages.StorageService
	notificationSender  NotificationSender
	taskCancelManager   *tasks_cancellation.TaskCancelManager
	backupNodesRegistry *nodes.BackupNodesRegistry
	secretKeyService    *encryption_secrets.SecretKeyService
	logger              *slog.Logger
	fullExecutor        FullBackupExecutor
	incrExecutor        IncrementalBackupExecutor
	nodeID              uuid.UUID

	lastHeartbeat time.Time

	hasRun atomic.Bool
}

func (n *PhysicalBackuperNode) Run(ctx context.Context) {
	if n.hasRun.Swap(true) {
		panic(fmt.Sprintf("%T.Run() called multiple times", n))
	}

	n.lastHeartbeat = time.Now().UTC()

	throughputMBs := config.GetEnv().NodeNetworkThroughputMBs

	backupNode := nodes.BackupNode{
		ID:            n.nodeID,
		ThroughputMBs: throughputMBs,
		LastHeartbeat: time.Now().UTC(),
	}

	if err := n.backupNodesRegistry.HearthbeatNodeInRegistry(time.Now().UTC(), backupNode); err != nil {
		n.logger.Error("failed to register physical backuper node", "error", err)

		panic(err)
	}

	backupAssignmentListener := func(backupID uuid.UUID, isCallNotifier bool) {
		go func() {
			n.MakeBackup(backupID, isCallNotifier)

			if err := n.backupNodesRegistry.PublishBackupCompletion(n.nodeID, backupID); err != nil {
				n.logger.Error("failed to publish backup completion",
					"backup_id", backupID,
					"error", err)
			}
		}()
	}

	if err := n.backupNodesRegistry.SubscribeNodeForBackupsAssignment(n.nodeID, backupAssignmentListener); err != nil {
		n.logger.Error("failed to subscribe physical backuper", "error", err)

		panic(err)
	}

	defer func() {
		if err := n.backupNodesRegistry.UnsubscribeNodeForBackupsAssignments(); err != nil {
			n.logger.Error("failed to unsubscribe physical backuper", "error", err)
		}
	}()

	ticker := time.NewTicker(heartbeatTickerInterval)
	defer ticker.Stop()

	n.logger.Info("physical backuper node started", "node_id", n.nodeID, "throughput_mbs", throughputMBs)

	for {
		select {
		case <-ctx.Done():
			n.logger.Info("shutdown signal received, unregistering physical backuper", "node_id", n.nodeID)

			if err := n.backupNodesRegistry.UnregisterNodeFromRegistry(backupNode); err != nil {
				n.logger.Error("failed to unregister physical backuper", "error", err)
			}

			return

		case <-ticker.C:
			n.sendHeartbeat(&backupNode)
		}
	}
}

func (n *PhysicalBackuperNode) IsRunning() bool {
	return n.lastHeartbeat.After(time.Now().UTC().Add(-heartbeatStaleThreshold))
}

func (n *PhysicalBackuperNode) MakeBackup(backupID uuid.UUID, isCallNotifier bool) {
	logger := n.logger.With("backup_id", backupID)

	fullBackup, err := n.fullRepo.FindByID(backupID)
	if err != nil {
		logger.Error("failed to look up full backup row", "error", err)
		return
	}

	if fullBackup != nil {
		n.runFullBackup(logger, fullBackup, isCallNotifier)
		return
	}

	incrBackup, err := n.incrRepo.FindByID(backupID)
	if err != nil {
		logger.Error("failed to look up incremental backup row", "error", err)

		return
	}

	if incrBackup != nil {
		n.runIncrementalBackup(logger, incrBackup, isCallNotifier)
		return
	}

	logger.Warn("backup not found in either typed table; ignoring assignment")
}

func (n *PhysicalBackuperNode) runFullBackup(
	logger *slog.Logger,
	fullBackup *physical_models.PhysicalFullBackup,
	isCallNotifier bool,
) {
	backupCtx, ok := n.loadBackupContext(logger, fullBackup.DatabaseID)
	if !ok {
		n.finalizeFullAsError(fullBackup, physical_enums.PhysicalBackupErrorPgBasebackupFailed,
			"failed to load backup context")

		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.taskCancelManager.RegisterTask(fullBackup.ID, cancel)
	defer n.taskCancelManager.UnregisterTask(fullBackup.ID)

	fullBackupSpec := postgresql_executor.FullBackupSpec{
		CommonBackupSpec: postgresql_executor.CommonBackupSpec{
			SourceDB:       backupCtx.Database.PostgresqlPhysical,
			DatabaseName:   backupCtx.Database.Name,
			StorageID:      backupCtx.Storage.ID,
			Storage:        backupCtx.Storage,
			Encryption:     backupCtx.Config.Encryption,
			MasterKey:      backupCtx.MasterKey,
			FieldEncryptor: n.fieldEncryptor,
			FullRepo:       n.fullRepo,
			HistoryRepo:    n.historyRepo,
			Logger:         logger,
		},
		Backup: fullBackup,
	}

	backupResult, err := n.fullExecutor.Execute(ctx, fullBackupSpec)
	if err != nil {
		logger.Error("full executor returned error", "error", err)

		n.finalizeFullAsError(fullBackup, physical_enums.PhysicalBackupErrorPgBasebackupFailed, err.Error())

		return
	}

	if backupResult.Status != physical_enums.PhysicalBackupStatusCompleted {
		logger.Warn("full executor returned non-COMPLETED result",
			"status", backupResult.Status,
			"reason", reasonOrEmpty(backupResult.ErrorReason),
			"message", backupResult.ErrorMessage)
	}

	if err := n.persistFullResult(fullBackup, backupResult); err != nil {
		logger.Error("failed to persist full result", "error", err)

		return
	}

	if isCallNotifier {
		n.sendFullBackupNotification(backupCtx.Config, backupCtx.Database, fullBackup, backupResult)
	}
}

func (n *PhysicalBackuperNode) runIncrementalBackup(
	logger *slog.Logger,
	incrBackup *physical_models.PhysicalIncrementalBackup,
	isCallNotifier bool,
) {
	backupCtx, ok := n.loadBackupContext(logger, incrBackup.DatabaseID)
	if !ok {
		n.finalizeIncrAsError(incrBackup, physical_enums.PhysicalBackupErrorPgBasebackupFailed,
			"failed to load backup context")

		return
	}

	parentRef, err := n.resolveParentManifest(incrBackup)
	if err != nil {
		logger.Error("failed to resolve parent manifest", "error", err)

		n.finalizeIncrAsChainBroken(incrBackup,
			physical_enums.PhysicalBackupErrorParentManifestMissing, err.Error())

		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.taskCancelManager.RegisterTask(incrBackup.ID, cancel)
	defer n.taskCancelManager.UnregisterTask(incrBackup.ID)

	incrBackupSpec := postgresql_executor.IncrementalBackupSpec{
		CommonBackupSpec: postgresql_executor.CommonBackupSpec{
			SourceDB:       backupCtx.Database.PostgresqlPhysical,
			DatabaseName:   backupCtx.Database.Name,
			StorageID:      backupCtx.Storage.ID,
			Storage:        backupCtx.Storage,
			Encryption:     backupCtx.Config.Encryption,
			MasterKey:      backupCtx.MasterKey,
			FieldEncryptor: n.fieldEncryptor,
			FullRepo:       n.fullRepo,
			HistoryRepo:    n.historyRepo,
			Logger:         logger,
		},
		Backup:             incrBackup,
		ParentManifest:     parentRef,
		IncrRepo:           n.incrRepo,
		IncrementalCadence: backupCtx.Config.IncrementalBackupInterval.ApproxPeriod(),
	}

	backupResult, err := n.incrExecutor.Execute(ctx, incrBackupSpec)
	if err != nil {
		logger.Error("incremental executor returned error", "error", err)

		n.finalizeIncrAsError(incrBackup, physical_enums.PhysicalBackupErrorPgBasebackupFailed, err.Error())

		return
	}

	if backupResult.Status != physical_enums.PhysicalBackupStatusCompleted {
		logger.Warn("incremental executor returned non-COMPLETED result",
			"status", backupResult.Status,
			"reason", reasonOrEmpty(backupResult.ErrorReason),
			"message", backupResult.ErrorMessage)
	}

	if err := n.persistIncrResult(incrBackup, backupResult); err != nil {
		logger.Error("failed to persist incremental result", "error", err)

		return
	}

	if isCallNotifier {
		n.sendIncrBackupNotification(backupCtx.Config, backupCtx.Database, incrBackup, backupResult)
	}
}

func (n *PhysicalBackuperNode) loadBackupContext(
	logger *slog.Logger,
	databaseID uuid.UUID,
) (*backupContext, bool) {
	cfg, err := n.backupConfigService.GetBackupConfigByDbId(databaseID)
	if err != nil {
		logger.Error("failed to fetch physical backup config", "error", err)

		return nil, false
	}

	if cfg.StorageID == nil {
		logger.Error("physical backup config has no storage id")

		return nil, false
	}

	db, err := n.databaseService.GetDatabaseByID(databaseID)
	if err != nil {
		logger.Error("failed to fetch database by id", "error", err)

		return nil, false
	}

	if db.PostgresqlPhysical == nil {
		logger.Error("database is not a physical postgres database")

		return nil, false
	}

	storage, err := n.storageService.GetStorageByID(*cfg.StorageID)
	if err != nil {
		logger.Error("failed to fetch storage", "error", err)

		return nil, false
	}

	masterKey := ""
	if cfg.Encryption == backups_core_enums.BackupEncryptionEncrypted {
		key, secretErr := n.secretKeyService.GetSecretKey()
		if secretErr != nil {
			logger.Error("failed to fetch master key", "error", secretErr)

			return nil, false
		}

		masterKey = key
	}

	return &backupContext{cfg, db, storage, masterKey}, true
}

func (n *PhysicalBackuperNode) resolveParentManifest(
	incrBackup *physical_models.PhysicalIncrementalBackup,
) (postgresql_executor.ParentManifestRef, error) {
	if incrBackup.ParentIncrementalBackupID != nil {
		parent, lookupErr := n.incrRepo.FindByID(*incrBackup.ParentIncrementalBackupID)
		if lookupErr != nil {
			return postgresql_executor.ParentManifestRef{}, fmt.Errorf("look up parent incr: %w", lookupErr)
		}

		if parent == nil || parent.ManifestFileName == nil || parent.StopLSN == nil {
			return postgresql_executor.ParentManifestRef{}, errors.New(
				"parent incremental row missing or has no manifest_file_name / stop_lsn",
			)
		}

		return postgresql_executor.ParentManifestRef{
			BackupID:   parent.ID,
			FileName:   *parent.ManifestFileName,
			Encryption: parent.Encryption,
			Salt:       derefString(parent.ManifestEncryptionSalt),
			IV:         derefString(parent.ManifestEncryptionIV),
			StopLSN:    *parent.StopLSN,
		}, nil
	}

	parent, lookupErr := n.fullRepo.FindByID(incrBackup.RootFullBackupID)
	if lookupErr != nil {
		return postgresql_executor.ParentManifestRef{}, fmt.Errorf("look up root full: %w", lookupErr)
	}

	if parent == nil || parent.ManifestFileName == nil || parent.StopLSN == nil {
		return postgresql_executor.ParentManifestRef{}, errors.New(
			"root full row missing or has no manifest_file_name / stop_lsn")
	}

	return postgresql_executor.ParentManifestRef{
		BackupID:   parent.ID,
		FileName:   *parent.ManifestFileName,
		Encryption: parent.Encryption,
		Salt:       derefString(parent.ManifestEncryptionSalt),
		IV:         derefString(parent.ManifestEncryptionIV),
		StopLSN:    *parent.StopLSN,
	}, nil
}

func (n *PhysicalBackuperNode) persistFullResult(
	fullBackup *physical_models.PhysicalFullBackup,
	backupResult postgresql_executor.PhysicalBackupResult,
) error {
	fullBackup.Status = backupResult.Status
	fullBackup.ErrorReason = backupResult.ErrorReason

	if backupResult.Status == physical_enums.PhysicalBackupStatusCompleted {
		fullBackup.TimelineID = backupResult.TimelineID
		fullBackup.StartLSN = lsnPtr(backupResult.StartLSN)
		fullBackup.StopLSN = lsnPtr(backupResult.StopLSN)
		fullBackup.BackupSizeMb = &backupResult.BackupSizeMb
		fullBackup.BackupDurationMs = &backupResult.BackupDurationMs
		fullBackup.Encryption = backupResult.EncryptionAlgo
		fullBackup.EncryptionSalt = nilOrPtr(backupResult.EncryptionSalt)
		fullBackup.EncryptionIV = nilOrPtr(backupResult.EncryptionIV)

		fullBackup.Compression = backupResult.Compression
		fullBackup.ManifestFileName = nilOrPtr(backupResult.ManifestFileName)
		fullBackup.ManifestEncryptionSalt = nilOrPtr(backupResult.ManifestEncryptionSalt)
		fullBackup.ManifestEncryptionIV = nilOrPtr(backupResult.ManifestEncryptionIV)

		completed := backupResult.CompletedAt
		if completed.IsZero() {
			completed = time.Now().UTC()
		}

		fullBackup.CompletedAt = &completed
	}

	return n.saveTerminalResultIfInProgress(
		fullBackup.DatabaseID,
		fullBackup.ID,
		func(tx *gorm.DB) (physical_enums.PhysicalBackupStatus, error) {
			var current physical_models.PhysicalFullBackup

			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("status").
				Where("id = ?", fullBackup.ID).
				First(&current).Error; err != nil {
				return "", err
			}

			return current.Status, nil
		},
		func(tx *gorm.DB) error { return tx.Save(fullBackup).Error },
	)
}

func (n *PhysicalBackuperNode) persistIncrResult(
	incrBackup *physical_models.PhysicalIncrementalBackup,
	backupResult postgresql_executor.PhysicalBackupResult,
) error {
	incrBackup.Status = backupResult.Status
	incrBackup.ErrorReason = backupResult.ErrorReason

	if backupResult.Status == physical_enums.PhysicalBackupStatusCompleted {
		incrBackup.TimelineID = backupResult.TimelineID
		incrBackup.StartLSN = lsnPtr(backupResult.StartLSN)
		incrBackup.StopLSN = lsnPtr(backupResult.StopLSN)
		incrBackup.BackupSizeMb = &backupResult.BackupSizeMb
		incrBackup.BackupDurationMs = &backupResult.BackupDurationMs
		incrBackup.Encryption = backupResult.EncryptionAlgo
		incrBackup.EncryptionSalt = nilOrPtr(backupResult.EncryptionSalt)
		incrBackup.EncryptionIV = nilOrPtr(backupResult.EncryptionIV)

		incrBackup.Compression = backupResult.Compression
		incrBackup.ManifestFileName = nilOrPtr(backupResult.ManifestFileName)
		incrBackup.ManifestEncryptionSalt = nilOrPtr(backupResult.ManifestEncryptionSalt)
		incrBackup.ManifestEncryptionIV = nilOrPtr(backupResult.ManifestEncryptionIV)

		completed := backupResult.CompletedAt
		if completed.IsZero() {
			completed = time.Now().UTC()
		}

		incrBackup.CompletedAt = &completed
	}

	return n.saveTerminalResultIfInProgress(
		incrBackup.DatabaseID,
		incrBackup.ID,
		func(tx *gorm.DB) (physical_enums.PhysicalBackupStatus, error) {
			var current physical_models.PhysicalIncrementalBackup

			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("status").
				Where("id = ?", incrBackup.ID).
				First(&current).Error; err != nil {
				return "", err
			}

			return current.Status, nil
		},
		func(tx *gorm.DB) error { return tx.Save(incrBackup).Error },
	)
}

// saveTerminalResultIfInProgress writes the mutated backup row and releases its
// in-flight claim in one transaction, but only while the row is still
// IN_PROGRESS. A backuper can return after a restart recovery or a dead-node
// sweep already moved the row to a terminal status — and possibly handed the
// database to a fresh backup. Persisting then would resurrect a superseded
// backup, so the guarded read (locked FOR UPDATE to serialize against the
// sweep's conditional update) skips the write instead. The claim delete is
// scoped to backupID so it can never remove the newer backup's claim.
func (n *PhysicalBackuperNode) saveTerminalResultIfInProgress(
	databaseID, backupID uuid.UUID,
	loadStatus func(tx *gorm.DB) (physical_enums.PhysicalBackupStatus, error),
	save func(tx *gorm.DB) error,
) error {
	superseded := false

	err := storage.GetDb().Transaction(func(tx *gorm.DB) error {
		status, err := loadStatus(tx)
		if err != nil {
			return err
		}

		if status != physical_enums.PhysicalBackupStatusInProgress {
			superseded = true

			return nil
		}

		if err := save(tx); err != nil {
			return err
		}

		return tx.Delete(
			&physical_models.PhysicalInFlightBackup{},
			"database_id = ? AND backup_id = ?",
			databaseID,
			backupID,
		).Error
	})
	if err != nil {
		return err
	}

	if superseded {
		n.logger.Warn("backup row no longer in progress; skipping terminal persist", "backup_id", backupID)
	}

	return nil
}

func (n *PhysicalBackuperNode) finalizeFullAsError(
	fullBackup *physical_models.PhysicalFullBackup,
	reason physical_enums.PhysicalBackupErrorReason,
	_ string,
) {
	r := reason

	fullBackup.Status = physical_enums.PhysicalBackupStatusError
	fullBackup.ErrorReason = &r

	if err := n.fullRepo.Save(fullBackup); err != nil {
		n.logger.Error("failed to flip full row to ERROR", "backup_id", fullBackup.ID, "error", err)
	}

	_ = n.inFlightRepo.ReleaseOwned(fullBackup.DatabaseID, fullBackup.ID)
}

func (n *PhysicalBackuperNode) finalizeIncrAsError(
	incrBackup *physical_models.PhysicalIncrementalBackup,
	reason physical_enums.PhysicalBackupErrorReason,
	_ string,
) {
	n.finalizeIncrWithStatus(incrBackup, physical_enums.PhysicalBackupStatusError, reason)
}

// finalizeIncrAsChainBroken closes the chain instead of marking a transient
// failure. Use it only for irreversible conditions (a missing parent manifest,
// expired summaries) where retrying the same INCR is futile: CHAIN_BROKEN forces
// the next scheduler tick to open a fresh FULL, whereas ERROR would keep the
// chain extendable and retry the doomed INCR forever.
func (n *PhysicalBackuperNode) finalizeIncrAsChainBroken(
	incrBackup *physical_models.PhysicalIncrementalBackup,
	reason physical_enums.PhysicalBackupErrorReason,
	_ string,
) {
	n.finalizeIncrWithStatus(incrBackup, physical_enums.PhysicalBackupStatusChainBroken, reason)
}

func (n *PhysicalBackuperNode) finalizeIncrWithStatus(
	incrBackup *physical_models.PhysicalIncrementalBackup,
	status physical_enums.PhysicalBackupStatus,
	reason physical_enums.PhysicalBackupErrorReason,
) {
	r := reason

	incrBackup.Status = status
	incrBackup.ErrorReason = &r

	if err := n.incrRepo.Save(incrBackup); err != nil {
		n.logger.Error("failed to flip incr row to terminal status",
			"status", status, "backup_id", incrBackup.ID, "error", err)
	}

	_ = n.inFlightRepo.ReleaseOwned(incrBackup.DatabaseID, incrBackup.ID)
}

func (n *PhysicalBackuperNode) sendFullBackupNotification(
	cfg *backups_config_physical.PhysicalBackupConfig,
	db *databases.Database,
	fullBackup *physical_models.PhysicalFullBackup,
	backupResult postgresql_executor.PhysicalBackupResult,
) {
	notificationType, title, message := classifyFullBackupNotification(db, fullBackup, backupResult, n.workspaceService)
	if notificationType == "" {
		return
	}

	if !slices.Contains(cfg.SendNotificationsOn, notificationType) {
		return
	}

	for _, notifier := range db.Notifiers {
		n.notificationSender.SendNotification(&notifier, title, message)
	}
}

func (n *PhysicalBackuperNode) sendIncrBackupNotification(
	cfg *backups_config_physical.PhysicalBackupConfig,
	db *databases.Database,
	incrBackup *physical_models.PhysicalIncrementalBackup,
	backupResult postgresql_executor.PhysicalBackupResult,
) {
	notificationType, title, message := classifyIncrBackupNotification(db, incrBackup, backupResult, n.workspaceService)
	if notificationType == "" {
		return
	}

	if !slices.Contains(cfg.SendNotificationsOn, notificationType) {
		return
	}

	for _, notifier := range db.Notifiers {
		n.notificationSender.SendNotification(&notifier, title, message)
	}
}

func classifyFullBackupNotification(
	db *databases.Database,
	fullBackup *physical_models.PhysicalFullBackup,
	backupResult postgresql_executor.PhysicalBackupResult,
	workspaceService *workspaces_services.WorkspaceService,
) (backups_config_physical.BackupNotificationType, string, string) {
	workspaceName := "unknown"
	if db.WorkspaceID != nil {
		if ws, err := workspaceService.GetWorkspaceByID(*db.WorkspaceID); err == nil {
			workspaceName = ws.Name
		}
	}

	switch fullBackup.Status {
	case physical_enums.PhysicalBackupStatusCompleted:
		return backups_config_physical.NotificationBackupSuccess,
			fmt.Sprintf("Physical FULL completed for %q (workspace %q)", db.Name, workspaceName),
			fmt.Sprintf("backup_id=%s size=%.2f MB duration=%dms",
				fullBackup.ID, backupResult.BackupSizeMb, backupResult.BackupDurationMs)

	case physical_enums.PhysicalBackupStatusError:
		return backups_config_physical.NotificationBackupFailed,
			fmt.Sprintf("Physical FULL failed for %q (workspace %q)", db.Name, workspaceName),
			fmt.Sprintf("backup_id=%s reason=%s message=%s",
				fullBackup.ID, reasonOrEmpty(fullBackup.ErrorReason), backupResult.ErrorMessage)

	case physical_enums.PhysicalBackupStatusChainBroken:
		return backups_config_physical.NotificationChainBroken,
			fmt.Sprintf("Physical FULL chain-broken for %q (workspace %q)", db.Name, workspaceName),
			fmt.Sprintf("backup_id=%s reason=%s message=%s",
				fullBackup.ID, reasonOrEmpty(fullBackup.ErrorReason), backupResult.ErrorMessage)
	}

	return "", "", ""
}

func classifyIncrBackupNotification(
	db *databases.Database,
	incrBackup *physical_models.PhysicalIncrementalBackup,
	backupResult postgresql_executor.PhysicalBackupResult,
	workspaceService *workspaces_services.WorkspaceService,
) (backups_config_physical.BackupNotificationType, string, string) {
	workspaceName := "unknown"
	if db.WorkspaceID != nil {
		if ws, err := workspaceService.GetWorkspaceByID(*db.WorkspaceID); err == nil {
			workspaceName = ws.Name
		}
	}

	switch incrBackup.Status {
	case physical_enums.PhysicalBackupStatusCompleted:
		return backups_config_physical.NotificationBackupSuccess,
			fmt.Sprintf("Physical INCR completed for %q (workspace %q)", db.Name, workspaceName),
			fmt.Sprintf("backup_id=%s size=%.2f MB duration=%dms",
				incrBackup.ID, backupResult.BackupSizeMb, backupResult.BackupDurationMs)

	case physical_enums.PhysicalBackupStatusError:
		return backups_config_physical.NotificationBackupFailed,
			fmt.Sprintf("Physical INCR failed for %q (workspace %q)", db.Name, workspaceName),
			fmt.Sprintf("backup_id=%s reason=%s message=%s",
				incrBackup.ID, reasonOrEmpty(incrBackup.ErrorReason), backupResult.ErrorMessage)

	case physical_enums.PhysicalBackupStatusChainBroken:
		return backups_config_physical.NotificationChainBroken,
			fmt.Sprintf("Physical INCR chain-broken for %q (workspace %q)", db.Name, workspaceName),
			fmt.Sprintf("backup_id=%s reason=%s message=%s",
				incrBackup.ID, reasonOrEmpty(incrBackup.ErrorReason), backupResult.ErrorMessage)
	}

	return "", "", ""
}

func reasonOrEmpty(r *physical_enums.PhysicalBackupErrorReason) string {
	if r == nil {
		return ""
	}

	return string(*r)
}

func (n *PhysicalBackuperNode) sendHeartbeat(backupNode *nodes.BackupNode) {
	n.lastHeartbeat = time.Now().UTC()

	if err := n.backupNodesRegistry.HearthbeatNodeInRegistry(time.Now().UTC(), *backupNode); err != nil {
		n.logger.Error("physical backuper heartbeat failed", "error", err)
	}
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func nilOrPtr(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func lsnPtr(v walmath.LSN) *walmath.LSN {
	out := v

	return &out
}
