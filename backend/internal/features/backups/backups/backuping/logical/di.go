package backuping_logical

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	backups_core_logical "databasus-backend/internal/features/backups/backups/core/logical"
	usecases_logical "databasus-backend/internal/features/backups/backups/usecases/logical"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/billing"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var backupRepository = &backups_core_logical.BackupRepository{}

var taskCancelManager = tasks_cancellation.GetTaskCancelManager()

var backupCleaner = &BackupCleaner{
	backupRepository,
	storages.GetStorageService(),
	backups_config_logical.GetBackupConfigService(),
	billing.GetBillingService(),
	encryption.GetFieldEncryptor(),
	logger.GetLogger(),
	[]backups_core_logical.BackupRemoveListener{},
	atomic.Bool{},
}

var backupNodesRegistry = nodes.NewDefaultBackupNodesRegistry(config.GetEnv().CacheNamespace)

func getNodeID() uuid.UUID {
	return uuid.New()
}

var backuperNode = &BackuperNode{
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
	getNodeID(),
	time.Time{},
	atomic.Bool{},
}

var backupsScheduler = &BackupsScheduler{
	backupRepository,
	backups_config_logical.GetBackupConfigService(),
	taskCancelManager,
	nodes.NewNodeAssignmentCoordinator(backupNodesRegistry, logger.GetLogger()),
	databases.GetDatabaseService(),
	billing.GetBillingService(),
	time.Now().UTC(),
	logger.GetLogger(),
	backuperNode,
	[]backups_core_logical.BackupCompletionListener{},
	atomic.Bool{},
	atomic.Bool{},
}

func GetBackupsScheduler() *BackupsScheduler {
	return backupsScheduler
}

func GetBackuperNode() *BackuperNode {
	return backuperNode
}

func GetBackupNodesRegistry() *nodes.BackupNodesRegistry {
	return backupNodesRegistry
}

func GetBackupCleaner() *BackupCleaner {
	return backupCleaner
}
