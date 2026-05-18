package backuping

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/backups/backups/usecases"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/billing"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var backupRepository = &backups_core.BackupRepository{}

var taskCancelManager = tasks_cancellation.GetTaskCancelManager()

var backupCleaner = &BackupCleaner{
	backupRepository,
	storages.GetStorageService(),
	backups_config.GetBackupConfigService(),
	billing.GetBillingService(),
	encryption.GetFieldEncryptor(),
	logger.GetLogger(),
	[]backups_core.BackupRemoveListener{},
	atomic.Bool{},
}

var backupNodesRegistry = &BackupNodesRegistry{
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
	atomic.Bool{},
}

func getNodeID() uuid.UUID {
	return uuid.New()
}

var backuperNode = &BackuperNode{
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
	getNodeID(),
	time.Time{},
	atomic.Bool{},
}

var backupsScheduler = &BackupsScheduler{
	backupRepository,
	backups_config.GetBackupConfigService(),
	taskCancelManager,
	backupNodesRegistry,
	databases.GetDatabaseService(),
	billing.GetBillingService(),
	time.Now().UTC(),
	logger.GetLogger(),
	make(map[uuid.UUID]BackupToNodeRelation),
	backuperNode,
	atomic.Bool{},
}

func GetBackupsScheduler() *BackupsScheduler {
	return backupsScheduler
}

func GetBackuperNode() *BackuperNode {
	return backuperNode
}

func GetBackupNodesRegistry() *BackupNodesRegistry {
	return backupNodesRegistry
}

func GetBackupCleaner() *BackupCleaner {
	return backupCleaner
}
