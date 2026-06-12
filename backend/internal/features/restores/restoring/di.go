package restoring

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/config"
	backups_services "databasus-backend/internal/features/backups/backups/services"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/databases"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/restores/usecases"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var restoreRepository = &restores_core.RestoreRepository{}

var restoreNodesRegistry = NewRestoreNodesRegistry(
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
	config.GetEnv().CacheNamespace,
)

var restoreDatabaseCache = cache_utils.NewCacheUtil[RestoreDatabaseCache](
	cache_utils.GetValkeyClient(),
	"restore_db:",
)

var restoreCancelManager = tasks_cancellation.GetTaskCancelManager()

var restorerNode = &RestorerNode{
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
	restoreCancelManager,
	time.Time{},
	atomic.Bool{},
}

var restoresScheduler = &RestoresScheduler{
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

func GetRestoresScheduler() *RestoresScheduler {
	return restoresScheduler
}

func GetRestorerNode() *RestorerNode {
	return restorerNode
}

func GetRestoreNodesRegistry() *RestoreNodesRegistry {
	return restoreNodesRegistry
}
