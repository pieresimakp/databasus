package restores

import (
	"context"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	backups_controllers_logical "databasus-backend/internal/features/backups/backups/controllers/logical"
	backups_config_logical "databasus-backend/internal/features/backups/config/logical"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/restores/restoring"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
)

func CreateTestRouter() *gin.Engine {
	router := workspaces_testing.CreateTestRouter(
		workspaces_controllers.GetWorkspaceController(),
		workspaces_controllers.GetMembershipController(),
		databases.GetDatabaseController(),
		backups_config_logical.GetBackupConfigController(),
		backups_controllers_logical.GetBackupController(),
		GetRestoreController(),
	)

	v1 := router.Group("/api/v1")
	backups_controllers_logical.GetBackupController().RegisterPublicRoutes(v1)

	return router
}

func SetupMockRestoreNode(t *testing.T) (uuid.UUID, context.CancelFunc) {
	nodeID := uuid.New()
	err := restoring.CreateMockNodeInRegistry(
		nodeID,
		100,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("Failed to create mock node: %v", err)
	}

	cleanup := func() {
		// Node will expire naturally from registry
	}

	return nodeID, cleanup
}
