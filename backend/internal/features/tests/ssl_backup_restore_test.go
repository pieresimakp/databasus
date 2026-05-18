package tests

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"databasus-backend/internal/config"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	mariadbtypes "databasus-backend/internal/features/databases/databases/mariadb"
	mongodbtypes "databasus-backend/internal/features/databases/databases/mongodb"
	mysqltypes "databasus-backend/internal/features/databases/databases/mysql"
	pgtypes "databasus-backend/internal/features/databases/databases/postgresql"
	restores_core "databasus-backend/internal/features/restores/core"
	"databasus-backend/internal/features/storages"
	users_enums "databasus-backend/internal/features/users/enums"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	test_utils "databasus-backend/internal/util/testing"
	"databasus-backend/internal/util/tools"
)

// The MySQL driver panics on duplicate RegisterTLSConfig calls, so guard with sync.Once.
var sslMysqlTLSConfigOnce sync.Once

const sslMysqlTLSConfigName = "ssl-test-skip-verify"

func registerSSLMysqlTLSConfig(t *testing.T) {
	t.Helper()
	sslMysqlTLSConfigOnce.Do(func() {
		err := mysqldriver.RegisterTLSConfig(sslMysqlTLSConfigName, &tls.Config{
			InsecureSkipVerify: true,
		})
		if err != nil {
			t.Fatalf("failed to register MySQL TLS config: %v", err)
		}
	})
}

func Test_BackupAndRestorePostgresqlSSL_Succeeds(t *testing.T) {
	port := config.GetEnv().TestPostgresSslPort
	if port == "" {
		t.Skip("TEST_POSTGRES_SSL_PORT not configured")
	}

	host := config.GetEnv().TestLocalhost
	portInt, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("failed to parse SSL port: %v", err)
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d user=testuser password=testpassword dbname=testdb sslmode=require",
		host, portInt,
	)
	originalDB, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to SSL Postgres: %v", err)
	}
	defer originalDB.Close()

	tableName := "test_data_ssl"
	_, err = originalDB.Exec(createAndFillTableQuery(tableName))
	assert.NoError(t, err)

	router := createTestRouter()
	user := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("Postgres SSL Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)

	dbName := "testdb"
	database := createPostgresqlSSLDatabaseViaAPI(
		t, router, "Postgres SSL DB", workspace.ID,
		host, portInt, "testuser", "testpassword", dbName, user.Token,
	)

	enableBackupsViaAPI(
		t, router, database.ID, storage.ID,
		backups_config.BackupEncryptionNone, user.Token,
	)
	createBackupViaAPI(t, router, database.ID, user.Token)

	backup := waitForBackupCompletion(t, router, database.ID, user.Token, 5*time.Minute)
	assert.Equal(t, backups_core.BackupStatusCompleted, backup.Status)

	newDBName := "restoreddb_pg_ssl"
	_, err = originalDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", newDBName))
	assert.NoError(t, err)
	_, err = originalDB.Exec(fmt.Sprintf("CREATE DATABASE %s;", newDBName))
	assert.NoError(t, err)

	newDSN := fmt.Sprintf(
		"host=%s port=%d user=testuser password=testpassword dbname=%s sslmode=require",
		host, portInt, newDBName,
	)
	newDB, err := sqlx.Connect("postgres", newDSN)
	assert.NoError(t, err)
	defer newDB.Close()

	createPostgresqlSSLRestoreViaAPI(
		t, router, backup.ID,
		host, portInt, "testuser", "testpassword", newDBName, user.Token,
	)

	restore := waitForRestoreCompletion(t, router, backup.ID, user.Token, 5*time.Minute)
	assert.Equal(t, restores_core.RestoreStatusCompleted, restore.Status)

	verifyDataIntegrity(t, originalDB, newDB, tableName)

	_ = os.Remove(filepath.Join(config.GetEnv().DataFolder, backup.ID.String()))
	test_utils.MakeDeleteRequest(
		t, router, "/api/v1/databases/"+database.ID.String(),
		"Bearer "+user.Token, http.StatusNoContent,
	)
	storages.RemoveTestStorage(storage.ID)
	workspaces_testing.RemoveTestWorkspace(workspace, router)
}

func Test_BackupAndRestoreMariadbSSL_Succeeds(t *testing.T) {
	registerSSLMysqlTLSConfig(t)

	port := config.GetEnv().TestMariadbSslPort
	if port == "" {
		t.Skip("TEST_MARIADB_SSL_PORT not configured")
	}

	host := config.GetEnv().TestLocalhost
	portInt, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("failed to parse SSL port: %v", err)
	}

	dsn := fmt.Sprintf("root:rootpassword@tcp(%s:%d)/testdb?parseTime=true&tls=%s",
		host, portInt, sslMysqlTLSConfigName)
	originalDB, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to connect to SSL MariaDB: %v", err)
	}
	defer originalDB.Close()

	setupMariadbTestData(t, originalDB)

	router := createTestRouter()
	user := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("MariaDB SSL Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)

	dbName := "testdb"
	database := createMariadbSSLDatabaseViaAPI(
		t, router, "MariaDB SSL DB", workspace.ID,
		host, portInt, "root", "rootpassword", dbName,
		tools.MariadbVersion118, user.Token,
	)

	enableBackupsViaAPI(
		t, router, database.ID, storage.ID,
		backups_config.BackupEncryptionNone, user.Token,
	)
	createBackupViaAPI(t, router, database.ID, user.Token)

	backup := waitForBackupCompletion(t, router, database.ID, user.Token, 5*time.Minute)
	assert.Equal(t, backups_core.BackupStatusCompleted, backup.Status)

	newDBName := "restoreddb_mariadb_ssl"
	_, err = originalDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", newDBName))
	assert.NoError(t, err)
	_, err = originalDB.Exec(fmt.Sprintf("CREATE DATABASE %s;", newDBName))
	assert.NoError(t, err)

	newDSN := fmt.Sprintf("root:rootpassword@tcp(%s:%d)/%s?parseTime=true&tls=%s",
		host, portInt, newDBName, sslMysqlTLSConfigName)
	newDB, err := sqlx.Connect("mysql", newDSN)
	assert.NoError(t, err)
	defer newDB.Close()

	createMariadbSSLRestoreViaAPI(
		t, router, backup.ID,
		host, portInt, "root", "rootpassword", newDBName,
		tools.MariadbVersion118, user.Token,
	)

	restore := waitForMariadbRestoreCompletion(t, router, backup.ID, user.Token, 5*time.Minute)
	assert.Equal(t, restores_core.RestoreStatusCompleted, restore.Status)

	verifyMariadbDataIntegrity(t, originalDB, newDB)

	_ = os.Remove(filepath.Join(config.GetEnv().DataFolder, backup.ID.String()))
	test_utils.MakeDeleteRequest(
		t, router, "/api/v1/databases/"+database.ID.String(),
		"Bearer "+user.Token, http.StatusNoContent,
	)
	storages.RemoveTestStorage(storage.ID)
	workspaces_testing.RemoveTestWorkspace(workspace, router)
}

func Test_BackupAndRestoreMysqlSSL_Succeeds(t *testing.T) {
	registerSSLMysqlTLSConfig(t)

	port := config.GetEnv().TestMysqlSslPort
	if port == "" {
		t.Skip("TEST_MYSQL_SSL_PORT not configured")
	}

	host := config.GetEnv().TestLocalhost
	portInt, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("failed to parse SSL port: %v", err)
	}

	dsn := fmt.Sprintf("root:rootpassword@tcp(%s:%d)/testdb?parseTime=true&tls=%s",
		host, portInt, sslMysqlTLSConfigName)
	originalDB, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to connect to SSL MySQL: %v", err)
	}
	defer originalDB.Close()

	setupMysqlTestData(t, originalDB)

	router := createTestRouter()
	user := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("MySQL SSL Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)

	dbName := "testdb"
	database := createMysqlSSLDatabaseViaAPI(
		t, router, "MySQL SSL DB", workspace.ID,
		host, portInt, "root", "rootpassword", dbName,
		tools.MysqlVersion84, user.Token,
	)

	enableBackupsViaAPI(
		t, router, database.ID, storage.ID,
		backups_config.BackupEncryptionNone, user.Token,
	)
	createBackupViaAPI(t, router, database.ID, user.Token)

	backup := waitForBackupCompletion(t, router, database.ID, user.Token, 5*time.Minute)
	assert.Equal(t, backups_core.BackupStatusCompleted, backup.Status)

	newDBName := "restoreddb_mysql_ssl"
	_, err = originalDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", newDBName))
	assert.NoError(t, err)
	_, err = originalDB.Exec(fmt.Sprintf("CREATE DATABASE %s;", newDBName))
	assert.NoError(t, err)

	newDSN := fmt.Sprintf("root:rootpassword@tcp(%s:%d)/%s?parseTime=true&tls=%s",
		host, portInt, newDBName, sslMysqlTLSConfigName)
	newDB, err := sqlx.Connect("mysql", newDSN)
	assert.NoError(t, err)
	defer newDB.Close()

	createMysqlSSLRestoreViaAPI(
		t, router, backup.ID,
		host, portInt, "root", "rootpassword", newDBName,
		tools.MysqlVersion84, user.Token,
	)

	restore := waitForMysqlRestoreCompletion(t, router, backup.ID, user.Token, 5*time.Minute)
	assert.Equal(t, restores_core.RestoreStatusCompleted, restore.Status)

	verifyMysqlDataIntegrity(t, originalDB, newDB)

	_ = os.Remove(filepath.Join(config.GetEnv().DataFolder, backup.ID.String()))
	test_utils.MakeDeleteRequest(
		t, router, "/api/v1/databases/"+database.ID.String(),
		"Bearer "+user.Token, http.StatusNoContent,
	)
	storages.RemoveTestStorage(storage.ID)
	workspaces_testing.RemoveTestWorkspace(workspace, router)
}

func Test_BackupAndRestoreMongodbSSL_Succeeds(t *testing.T) {
	port := config.GetEnv().TestMongodbSslPort
	if port == "" {
		t.Skip("TEST_MONGODB_SSL_PORT not configured")
	}

	host := config.GetEnv().TestLocalhost
	portInt, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("failed to parse SSL port: %v", err)
	}

	uri := fmt.Sprintf(
		"mongodb://root:rootpassword@%s:%d/testdb?authSource=admin&tls=true&tlsInsecure=true&serverSelectionTimeoutMS=5000",
		host,
		portInt,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("failed to connect to SSL MongoDB: %v", err)
	}
	defer client.Disconnect(t.Context())

	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("failed to ping SSL MongoDB: %v", err)
	}

	container := &MongodbContainer{
		Host:         host,
		Port:         portInt,
		Username:     "root",
		Password:     "rootpassword",
		Database:     "testdb",
		AuthDatabase: "admin",
		Version:      tools.MongodbVersion8,
		Client:       client,
	}
	setupMongodbTestData(t, container)

	router := createTestRouter()
	user := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("MongoDB SSL Workspace", user, router)
	storage := storages.CreateTestStorage(workspace.ID)

	database := createMongodbSSLDatabaseViaAPI(
		t, router, "MongoDB SSL DB", workspace.ID,
		host, portInt, "root", "rootpassword",
		"testdb", "admin", tools.MongodbVersion8, user.Token,
	)

	enableBackupsViaAPI(
		t, router, database.ID, storage.ID,
		backups_config.BackupEncryptionNone, user.Token,
	)
	createBackupViaAPI(t, router, database.ID, user.Token)

	backup := waitForBackupCompletion(t, router, database.ID, user.Token, 5*time.Minute)
	assert.Equal(t, backups_core.BackupStatusCompleted, backup.Status)

	newDBName := "restoreddb_mongo_ssl_" + uuid.New().String()[:8]
	createMongodbSSLRestoreViaAPI(
		t, router, backup.ID,
		host, portInt, "root", "rootpassword",
		newDBName, "admin", tools.MongodbVersion8, user.Token,
	)

	restore := waitForMongodbRestoreCompletion(t, router, backup.ID, user.Token, 5*time.Minute)
	assert.Equal(t, restores_core.RestoreStatusCompleted, restore.Status)

	verifyMongodbDataIntegrity(t, container, newDBName)

	_ = container.Client.Database(newDBName).Drop(t.Context())
	_ = os.Remove(filepath.Join(config.GetEnv().DataFolder, backup.ID.String()))
	test_utils.MakeDeleteRequest(
		t, router, "/api/v1/databases/"+database.ID.String(),
		"Bearer "+user.Token, http.StatusNoContent,
	)
	storages.RemoveTestStorage(storage.ID)
	workspaces_testing.RemoveTestWorkspace(workspace, router)
}

func createPostgresqlSSLDatabaseViaAPI(
	t *testing.T,
	router *gin.Engine,
	name string,
	workspaceID uuid.UUID,
	host string,
	port int,
	username, password, database, token string,
) *databases.Database {
	request := databases.Database{
		Name:        name,
		WorkspaceID: &workspaceID,
		Type:        databases.DatabaseTypePostgres,
		Postgresql: &pgtypes.PostgresqlDatabase{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: &database,
			IsHttps:  true,
			CpuCount: 1,
		},
	}

	return submitCreateDatabase(t, router, "Postgres SSL", request, token)
}

func createPostgresqlSSLRestoreViaAPI(
	t *testing.T,
	router *gin.Engine,
	backupID uuid.UUID,
	host string,
	port int,
	username, password, database, token string,
) {
	request := restores_core.RestoreBackupRequest{
		PostgresqlDatabase: &pgtypes.PostgresqlDatabase{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: &database,
			IsHttps:  true,
			CpuCount: 1,
		},
	}
	submitRestore(t, router, backupID, request, token)
}

func createMariadbSSLDatabaseViaAPI(
	t *testing.T,
	router *gin.Engine,
	name string,
	workspaceID uuid.UUID,
	host string,
	port int,
	username, password, database string,
	version tools.MariadbVersion,
	token string,
) *databases.Database {
	request := databases.Database{
		Name:        name,
		WorkspaceID: &workspaceID,
		Type:        databases.DatabaseTypeMariadb,
		Mariadb: &mariadbtypes.MariadbDatabase{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: &database,
			Version:  version,
			IsHttps:  true,
		},
	}

	return submitCreateDatabase(t, router, "MariaDB SSL", request, token)
}

func createMariadbSSLRestoreViaAPI(
	t *testing.T,
	router *gin.Engine,
	backupID uuid.UUID,
	host string,
	port int,
	username, password, database string,
	version tools.MariadbVersion,
	token string,
) {
	request := restores_core.RestoreBackupRequest{
		MariadbDatabase: &mariadbtypes.MariadbDatabase{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: &database,
			Version:  version,
			IsHttps:  true,
		},
	}
	submitRestore(t, router, backupID, request, token)
}

func createMysqlSSLDatabaseViaAPI(
	t *testing.T,
	router *gin.Engine,
	name string,
	workspaceID uuid.UUID,
	host string,
	port int,
	username, password, database string,
	version tools.MysqlVersion,
	token string,
) *databases.Database {
	request := databases.Database{
		Name:        name,
		WorkspaceID: &workspaceID,
		Type:        databases.DatabaseTypeMysql,
		Mysql: &mysqltypes.MysqlDatabase{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: &database,
			Version:  version,
			IsHttps:  true,
		},
	}

	return submitCreateDatabase(t, router, "MySQL SSL", request, token)
}

func createMysqlSSLRestoreViaAPI(
	t *testing.T,
	router *gin.Engine,
	backupID uuid.UUID,
	host string,
	port int,
	username, password, database string,
	version tools.MysqlVersion,
	token string,
) {
	request := restores_core.RestoreBackupRequest{
		MysqlDatabase: &mysqltypes.MysqlDatabase{
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			Database: &database,
			Version:  version,
			IsHttps:  true,
		},
	}
	submitRestore(t, router, backupID, request, token)
}

func createMongodbSSLDatabaseViaAPI(
	t *testing.T,
	router *gin.Engine,
	name string,
	workspaceID uuid.UUID,
	host string,
	port int,
	username, password, database, authDatabase string,
	version tools.MongodbVersion,
	token string,
) *databases.Database {
	request := databases.Database{
		Name:        name,
		WorkspaceID: &workspaceID,
		Type:        databases.DatabaseTypeMongodb,
		Mongodb: &mongodbtypes.MongodbDatabase{
			Host:         host,
			Port:         &port,
			Username:     username,
			Password:     password,
			Database:     database,
			AuthDatabase: authDatabase,
			Version:      version,
			IsHttps:      true,
			IsSrv:        false,
			CpuCount:     1,
		},
	}

	return submitCreateDatabase(t, router, "MongoDB SSL", request, token)
}

func createMongodbSSLRestoreViaAPI(
	t *testing.T,
	router *gin.Engine,
	backupID uuid.UUID,
	host string,
	port int,
	username, password, database, authDatabase string,
	version tools.MongodbVersion,
	token string,
) {
	request := restores_core.RestoreBackupRequest{
		MongodbDatabase: &mongodbtypes.MongodbDatabase{
			Host:         host,
			Port:         &port,
			Username:     username,
			Password:     password,
			Database:     database,
			AuthDatabase: authDatabase,
			Version:      version,
			IsHttps:      true,
			IsSrv:        false,
			CpuCount:     1,
		},
	}
	submitRestore(t, router, backupID, request, token)
}

func submitCreateDatabase(
	t *testing.T,
	router *gin.Engine,
	label string,
	request databases.Database,
	token string,
) *databases.Database {
	w := workspaces_testing.MakeAPIRequest(
		router, "POST", "/api/v1/databases/create", "Bearer "+token, request,
	)
	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create %s database. Status: %d, Body: %s",
			label, w.Code, w.Body.String())
	}

	var created databases.Database
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("Failed to unmarshal %s response: %v", label, err)
	}
	return &created
}

func submitRestore(
	t *testing.T,
	router *gin.Engine,
	backupID uuid.UUID,
	request restores_core.RestoreBackupRequest,
	token string,
) {
	test_utils.MakePostRequest(
		t, router,
		fmt.Sprintf("/api/v1/restores/%s/restore", backupID.String()),
		"Bearer "+token, request, http.StatusOK,
	)
}
