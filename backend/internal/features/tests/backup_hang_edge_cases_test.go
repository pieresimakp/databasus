package tests

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"databasus-backend/internal/config"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_dto "databasus-backend/internal/features/backups/backups/dto"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/storages"
	users_enums "databasus-backend/internal/features/users/enums"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	test_utils "databasus-backend/internal/util/testing"
	"databasus-backend/internal/util/tools"
)

func Test_BackupShouldFailNotHang_WhenSaveFileFails_RegressionForIssue582(t *testing.T) {
	env := config.GetEnv()

	if env.TestMinioPort == "" {
		t.Skip("TEST_MINIO_PORT not set; skipping flaky-S3 regression test")
		return
	}

	router := createTestRouter()
	user := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace(
		"Issue 582 Workspace",
		user,
		router,
	)
	defer workspaces_testing.RemoveTestWorkspace(workspace, router)

	minioEndpoint := fmt.Sprintf("http://%s:%s", env.TestLocalhost, env.TestMinioPort)
	storage := storages.CreateTestFlakyS3Storage(workspace.ID, minioEndpoint)
	defer storages.RemoveTestStorage(storage.ID)

	t.Run("MariaDB", func(t *testing.T) {
		container, err := connectToMariadbContainer(
			tools.MariadbVersion1011,
			env.TestMariadb1011Port,
		)
		if err != nil {
			t.Skipf("Skipping: failed to connect to MariaDB test container: %v", err)
			return
		}
		defer container.DB.Close()

		setupMariadbTestData(t, container.DB)

		database := createMariadbDatabaseViaAPI(
			t, router, "Issue 582 MariaDB DB", workspace.ID,
			container.Host, container.Port,
			container.Username, container.Password, container.Database,
			container.Version,
			user.Token,
		)

		assertBackupFailsWithoutHanging(t, router, user.Token, database.ID, storage.ID)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		container, err := connectToPostgresContainer("16", env.TestPostgres16Port)
		if err != nil {
			t.Skipf("Skipping: failed to connect to PostgreSQL test container: %v", err)
			return
		}
		defer container.DB.Close()

		_, err = container.DB.Exec(createAndFillTableQuery("test_data"))
		require.NoError(t, err)

		database := createDatabaseViaAPI(
			t, router, "Issue 582 PostgreSQL DB", workspace.ID,
			container.Host, container.Port,
			container.Username, container.Password, container.Database,
			user.Token,
		)

		assertBackupFailsWithoutHanging(t, router, user.Token, database.ID, storage.ID)
	})

	t.Run("MySQL", func(t *testing.T) {
		container, err := connectToMysqlContainer(
			tools.MysqlVersion80,
			env.TestMysql80Port,
		)
		if err != nil {
			t.Skipf("Skipping: failed to connect to MySQL test container: %v", err)
			return
		}
		defer container.DB.Close()

		setupMysqlTestData(t, container.DB)

		database := createMysqlDatabaseViaAPI(
			t, router, "Issue 582 MySQL DB", workspace.ID,
			container.Host, container.Port,
			container.Username, container.Password, container.Database,
			container.Version,
			user.Token,
		)

		assertBackupFailsWithoutHanging(t, router, user.Token, database.ID, storage.ID)
	})

	t.Run("MongoDB", func(t *testing.T) {
		container, err := connectToMongodbContainer(
			tools.MongodbVersion7,
			env.TestMongodb70Port,
		)
		if err != nil {
			t.Skipf("Skipping: failed to connect to MongoDB test container: %v", err)
			return
		}
		defer func() { _ = container.Client.Disconnect(t.Context()) }()

		setupMongodbTestData(t, container)

		database := createMongodbDatabaseViaAPI(
			t, router, "Issue 582 MongoDB DB", workspace.ID,
			container.Host, container.Port,
			container.Username, container.Password, container.Database,
			container.AuthDatabase,
			container.Version,
			user.Token,
		)

		assertBackupFailsWithoutHanging(t, router, user.Token, database.ID, storage.ID)
	})
}

func assertBackupFailsWithoutHanging(
	t *testing.T,
	router *gin.Engine,
	token string,
	databaseID uuid.UUID,
	storageID uuid.UUID,
) {
	t.Helper()

	defer test_utils.MakeDeleteRequest(
		t,
		router,
		"/api/v1/databases/"+databaseID.String(),
		"Bearer "+token,
		http.StatusNoContent,
	)

	enableBackupsViaAPI(
		t, router, databaseID, storageID,
		backups_config.BackupEncryptionNone, token,
	)

	createBackupViaAPI(t, router, databaseID, token)

	backup := waitForBackupTerminalStatus(t, router, databaseID, token, 2*time.Minute)

	require.Equalf(
		t,
		backups_core.BackupStatusFailed,
		backup.Status,
		"issue #582: backup should be marked Failed when SaveFile fails; got status=%s",
		backup.Status,
	)
	require.NotNil(
		t,
		backup.FailMessage,
		"issue #582: failed backup must carry a fail message describing the SaveFile error",
	)
}

func waitForBackupTerminalStatus(
	t *testing.T,
	router *gin.Engine,
	databaseID uuid.UUID,
	token string,
	timeout time.Duration,
) *backups_core.Backup {
	deadline := time.Now().UTC().Add(timeout)
	pollInterval := 500 * time.Millisecond

	for time.Now().UTC().Before(deadline) {
		var response backups_dto.GetBackupsResponse
		test_utils.MakeGetRequestAndUnmarshal(
			t,
			router,
			fmt.Sprintf("/api/v1/backups?database_id=%s&limit=1", databaseID.String()),
			"Bearer "+token,
			http.StatusOK,
			&response,
		)

		if len(response.Backups) > 0 {
			b := response.Backups[0]
			if b.Status == backups_core.BackupStatusCompleted ||
				b.Status == backups_core.BackupStatusFailed ||
				b.Status == backups_core.BackupStatusCanceled {
				return b
			}
		}

		time.Sleep(pollInterval)
	}

	t.Fatalf(
		"backup for database %s did not reach a terminal status within %v "+
			"(issue #582: backup hangs forever when SaveFile fails)",
		databaseID,
		timeout,
	)

	return nil
}
