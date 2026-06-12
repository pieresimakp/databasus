package physicaltesting

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	backuping_physical "databasus-backend/internal/features/backups/backups/backuping/physical"
	backups_controllers_physical "databasus-backend/internal/features/backups/backups/controllers/physical"
	backups_core_enums "databasus-backend/internal/features/backups/backups/core/enums"
	"databasus-backend/internal/features/backups/backups/core/physical/chain_view"
	physical_enums "databasus-backend/internal/features/backups/backups/core/physical/enums"
	backups_dto_physical "databasus-backend/internal/features/backups/backups/dto/physical"
	postgresql_executor "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	backups_config_physical "databasus-backend/internal/features/backups/config/physical"
	"databasus-backend/internal/features/databases"
	postgresql_physical "databasus-backend/internal/features/databases/databases/postgresql/physical"
	"databasus-backend/internal/features/intervals"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	users_middleware "databasus-backend/internal/features/users/middleware"
	users_services "databasus-backend/internal/features/users/services"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
	test_utils "databasus-backend/internal/util/testing"
	"databasus-backend/internal/util/testing/containers"
	"databasus-backend/internal/util/walmath"
)

const (
	restoreWorkDir     = "/restore"
	restoredPgUser     = "testuser"
	restoredPgPassword = "testpassword"
	restoredPgDatabase = "testdb"
)

// setupReplicationOnlyFixture boots a throwaway replication-capable source for one PostgreSQL major,
// wires a scheduler-driven physical fixture against it, and switches its backup credentials to a
// replication-only user over the API — the common preamble every physical E2E shares. The returned
// router is the one all subsequent API calls go through.
func setupReplicationOnlyFixture(
	t *testing.T,
	version string,
	image string,
	backupType postgresql_physical.BackupType,
) (*gin.Engine, *postgresql_executor.PhysicalDBFixture) {
	t.Helper()

	source := containers.StartPhysicalPostgres(t, image)

	router := newPhysicalTestRouter()
	fixture := postgresql_executor.SetupPhysicalDBForScheduledBackupVersion(
		t, source.Host, source.Port, version, backupType)
	useReplicationOnlyUserViaAPI(t, router, fixture)

	return router, fixture
}

// prepareRestoreTarget boots a throwaway idle restore-target container whose major matches the
// source backup's (a PG 18 PGDATA cannot start under PG 17). It is terminated with its tmpfs /restore
// when the test ends, so no pre/post wipe is needed.
func prepareRestoreTarget(t *testing.T, image string) containers.RestoreTarget {
	t.Helper()

	return containers.StartPhysicalRestoreTarget(t, image)
}

// newPhysicalTestRouter wires the physical controller's public (restore-stream)
// and protected routes plus the supporting controllers, mirroring production.
func newPhysicalTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	v1 := router.Group("/api/v1")

	backups_controllers_physical.GetPhysicalBackupController().RegisterPublicRoutes(v1)

	protected := v1.Group("").Use(users_middleware.AuthMiddleware(users_services.GetUserService()))
	if routerGroup, ok := protected.(*gin.RouterGroup); ok {
		workspaces_controllers.GetWorkspaceController().RegisterRoutes(routerGroup)
		workspaces_controllers.GetMembershipController().RegisterRoutes(routerGroup)
		databases.GetDatabaseController().RegisterRoutes(routerGroup)
		backups_config_physical.GetBackupConfigController().RegisterRoutes(routerGroup)
		backups_controllers_physical.GetPhysicalBackupController().RegisterRoutes(routerGroup)
	}

	storages.SetupDependencies()
	databases.SetupDependencies()
	notifiers.SetupDependencies()
	backups_config_physical.SetupDependencies()
	backuping_physical.SetupDependencies()

	return router
}

// cronInterval is a once-a-year cron used for both cadences so the scheduler
// never auto-fires on its own clock: the only FULL is the bootstrap one (no prior
// full ⇒ due immediately), and incrementals come solely from the trigger
// endpoint. Cron also satisfies the "incremental strictly more frequent than
// full" config rule trivially.
func cronInterval() intervals.Interval {
	expr := "0 0 1 1 *"

	return intervals.Interval{Type: intervals.IntervalCron, CronExpression: &expr}
}

// enablePhysicalBackupsViaAPI turns on backups for the fixture's database through
// the config endpoint. isWalStream must match the DB's BackupType: the WAL-stream
// config requires a positive lag threshold, the plain incremental config requires
// zero.
func enablePhysicalBackupsViaAPI(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
	isWalStream bool,
) {
	t.Helper()

	cfg := backups_config_physical.PhysicalBackupConfig{
		DatabaseID:                fixture.DB.ID,
		IsBackupsEnabled:          true,
		FullBackupInterval:        cronInterval(),
		IncrementalBackupInterval: cronInterval(),
		Retention:                 backups_config_physical.RetentionChains,
		ChainsRetention:           backups_config_physical.ChainsRetention{Count: 50},
		Encryption:                backups_core_enums.BackupEncryptionNone,
		StorageID:                 &fixture.Storage.ID,
		Storage:                   fixture.Storage,
	}

	if isWalStream {
		cfg.WalLagThresholdBytes = 64 * 1024 * 1024
	}

	test_utils.MakePostRequest(t, router, "/api/v1/backup-configs/physical/save",
		"Bearer "+fixture.Owner.Token, cfg, http.StatusOK)
}

func triggerIncrementalViaAPI(t *testing.T, router *gin.Engine, fixture *postgresql_executor.PhysicalDBFixture) {
	t.Helper()

	test_utils.MakePostRequest(t, router,
		"/api/v1/backups/physical/database/"+fixture.DB.ID.String()+"/trigger",
		"Bearer "+fixture.Owner.Token,
		backups_dto_physical.TriggerBackupRequest{Type: backups_dto_physical.TriggerBackupTypeIncremental},
		http.StatusAccepted)
}

// useReplicationOnlyUserViaAPI provisions a fresh LOGIN+REPLICATION role on the
// source through the public API and switches the database's stored backup
// credentials to it, so every subsequent FULL / incremental / WAL operation runs
// through a least-privilege user instead of the superuser the fixture is seeded
// with. The backuper re-fetches the database at execution time, so the switch is
// persisted via the update endpoint — mutating the in-memory fixture alone would
// not reach the running backup.
func useReplicationOnlyUserViaAPI(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
) {
	t.Helper()

	physical := fixture.DB.PostgresqlPhysical

	// The provisioning call connects to the source as the current admin user to run
	// CREATE ROLE; the database is identified by ID, the connection block carries the
	// superuser the source container was started with.
	provisionRequest := databases.Database{
		ID:          fixture.DB.ID,
		WorkspaceID: fixture.DB.WorkspaceID,
		Name:        fixture.DB.Name,
		Type:        databases.DatabaseTypePostgresPhysical,
		Notifiers:   []notifiers.Notifier{*fixture.Notifier},
		PostgresqlPhysical: &postgresql_physical.PostgresqlPhysicalDatabase{
			Version:    physical.Version,
			Host:       physical.Host,
			Port:       physical.Port,
			Username:   restoredPgUser,
			Password:   restoredPgPassword,
			BackupType: physical.BackupType,
		},
	}

	var provisioned databases.CreateReadOnlyUserResponse
	test_utils.MakePostRequestAndUnmarshal(t, router,
		"/api/v1/databases/create-replication-only-user",
		"Bearer "+fixture.Owner.Token, provisionRequest, http.StatusOK, &provisioned)
	require.NotEmpty(t, provisioned.Username)
	require.NotEmpty(t, provisioned.Password)

	switchRequest := provisionRequest
	switchRequest.PostgresqlPhysical = &postgresql_physical.PostgresqlPhysicalDatabase{
		Version:    physical.Version,
		Host:       physical.Host,
		Port:       physical.Port,
		Username:   provisioned.Username,
		Password:   provisioned.Password,
		BackupType: physical.BackupType,
	}
	test_utils.MakePostRequest(t, router, "/api/v1/databases/update",
		"Bearer "+fixture.Owner.Token, switchRequest, http.StatusOK)

	// Mirror the switch onto the in-memory fixture so direct source connections in
	// the test (slot inspection) use the same identity the backups now run as.
	physical.Username = provisioned.Username
	physical.Password = provisioned.Password

	isMinimal, excessivePrivileges, err := physical.IsUserReplicationOnly(
		t.Context(), logger.GetLogger(), encryption.GetFieldEncryptor())
	require.NoError(t, err)
	require.True(t, isMinimal,
		"backups must run through a replication-only user; excessive privileges: %v", excessivePrivileges)
	require.Empty(t, excessivePrivileges)
}

// deleteDatabaseViaAPI removes the fixture's database through the public DELETE
// endpoint, which fires the OnBeforeDatabaseRemove listeners (slot + streamer
// cleanup). The fixture's own t.Cleanup re-deletes at test end, but DeleteForTest
// is idempotent (a missing row is a no-op and the listeners tolerate it), so no
// extra guard is needed here.
func deleteDatabaseViaAPI(t *testing.T, router *gin.Engine, fixture *postgresql_executor.PhysicalDBFixture) {
	t.Helper()

	test_utils.MakeDeleteRequest(t, router,
		"/api/v1/databases/"+fixture.DB.ID.String(),
		"Bearer "+fixture.Owner.Token, http.StatusNoContent)
}

// waitForSlotPresent polls pg_replication_slots until slotName appears — the WAL
// stream supervisor creates the persistent slot a tick or two after WAL-stream
// backups are enabled — failing the test if it never does.
func waitForSlotPresent(t *testing.T, conn *pgx.Conn, slotName string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		if postgresql_executor.SlotExists(t, conn, slotName) {
			return
		}

		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("replication slot %q never appeared within %s", slotName, timeout)
}

// requireDatabaseSlotsGone polls until neither of the database's source-side slots
// remains — the persistent WAL streamer slot (databasus_slot_*) and any transient
// per-backup slot (databasus_basebackup_*) — proving the deleted database left
// nothing pinning WAL. Scoped to this database's own slot names. It polls because a
// bootstrap FULL can briefly hold its per-backup slot and the streamer slot drops
// only once its receiver has fully detached.
func requireDatabaseSlotsGone(
	t *testing.T,
	conn *pgx.Conn,
	fixture *postgresql_executor.PhysicalDBFixture,
	timeout time.Duration,
) {
	t.Helper()

	streamerSlot := fixture.DB.PostgresqlPhysical.ReplicationSlotName
	backupSlot := postgresql_executor.SlotName(fixture.DB.PostgresqlPhysical.ID)

	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		if !postgresql_executor.SlotExists(t, conn, streamerSlot) &&
			!postgresql_executor.SlotExists(t, conn, backupSlot) {
			return
		}

		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf(
		"database %s still has a replication slot pinning WAL %s after deletion "+
			"(streamer %q present=%v, per-backup %q present=%v)",
		fixture.DB.ID, timeout,
		streamerSlot, postgresql_executor.SlotExists(t, conn, streamerSlot),
		backupSlot, postgresql_executor.SlotExists(t, conn, backupSlot),
	)
}

// listChainBackups returns the database's FULL and incremental rows (the chain
// backups, excluding committed WAL) in chronological order, the oldest — the
// bootstrap FULL — first and the newest at the tip. The flat list endpoint
// interleaves WAL rows newest-first and paginates, so a single page can be all
// WAL and bury the chain; the type filter the API now exposes fetches FULL and
// INCREMENTAL directly, immune to WAL volume. These tests build exactly one
// chain (backups enabled once ⇒ one bootstrap FULL; the once-a-year cron never
// fires a second), so every non-WAL row belongs to that single chain.
func listChainBackups(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
) []backups_dto_physical.PhysicalBackupListItem {
	t.Helper()

	fullRows := listBackupsByType(t, router, fixture, physical_enums.PhysicalBackupTypeFull)
	incrRows := listBackupsByType(t, router, fixture, physical_enums.PhysicalBackupTypeIncremental)

	chain := slices.Concat(fullRows, incrRows)
	slices.SortFunc(chain, func(a, b backups_dto_physical.PhysicalBackupListItem) int {
		if byTime := a.CreatedAt.Compare(b.CreatedAt); byTime != 0 {
			return byTime
		}

		return cmp.Compare(a.ID.String(), b.ID.String())
	})

	return chain
}

// listBackupsByType pulls every backup of one type for the database. The page
// limit is set well above any chain length these tests produce so a single call
// returns them all.
func listBackupsByType(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
	backupType physical_enums.PhysicalBackupType,
) []backups_dto_physical.PhysicalBackupListItem {
	t.Helper()

	var response backups_dto_physical.GetPhysicalBackupsResponse
	test_utils.MakeGetRequestAndUnmarshal(t, router,
		"/api/v1/backups/physical/database/"+fixture.DB.ID.String()+"/backups?limit=1000&type="+string(backupType),
		"Bearer "+fixture.Owner.Token, http.StatusOK, &response)

	return response.Backups
}

// waitForChainBackups polls the flat backup list until the database's chain holds
// a COMPLETED FULL plus wantIncrementals COMPLETED incrementals, failing fast if
// any backup reaches ERROR or CHAIN_BROKEN. Returns the matched chain (oldest
// first) so the caller can read LSNs from the tip.
func waitForChainBackups(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
	wantIncrementals int,
	timeout time.Duration,
) []backups_dto_physical.PhysicalBackupListItem {
	t.Helper()

	completed := string(physical_enums.PhysicalBackupStatusCompleted)
	deadline := time.Now().UTC().Add(timeout)

	for time.Now().UTC().Before(deadline) {
		chain := listChainBackups(t, router, fixture)

		failFastOnTerminalBackup(t, chain)

		fullCompleted := false
		completedIncrementals := 0

		for _, backup := range chain {
			if backup.Status != completed {
				continue
			}

			if backup.Type == physical_enums.PhysicalBackupTypeFull {
				fullCompleted = true
			} else {
				completedIncrementals++
			}
		}

		if fullCompleted && completedIncrementals == wantIncrementals {
			return chain
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("chain never reached 1 COMPLETED full + %d COMPLETED incrementals within %s",
		wantIncrementals, timeout)

	return nil
}

// failFastOnTerminalBackup aborts the wait the moment any chain row is ERROR or
// CHAIN_BROKEN, turning a 3-minute timeout into an immediate, labelled failure.
// A CHAIN_BROKEN incremental still appears as an INCREMENTAL row in the flat
// list, so it is caught here.
func failFastOnTerminalBackup(t *testing.T, chain []backups_dto_physical.PhysicalBackupListItem) {
	t.Helper()

	for _, backup := range chain {
		if backup.Status == string(physical_enums.PhysicalBackupStatusError) ||
			backup.Status == string(physical_enums.PhysicalBackupStatusChainBroken) {
			t.Fatalf("backup %s (%s) reached terminal failure status %s",
				backup.ID, backup.Type, backup.Status)
		}
	}
}

// rootFullBackupID returns the id of the chain's FULL backup — the key the
// WAL-gap and restore-set resolvers identify a chain by. The FULL's own id is
// the chain's root full id.
func rootFullBackupID(t *testing.T, chain []backups_dto_physical.PhysicalBackupListItem) uuid.UUID {
	t.Helper()

	for _, backup := range chain {
		if backup.Type == physical_enums.PhysicalBackupTypeFull {
			return backup.ID
		}
	}

	t.Fatalf("chain has no FULL backup to derive the root full id from")

	return uuid.Nil
}

// chainTipStopLSN is the stop_lsn of the chain's newest backup — the point the
// next incremental's WAL summaries must cover before it can be built.
func chainTipStopLSN(t *testing.T, chain []backups_dto_physical.PhysicalBackupListItem) walmath.LSN {
	t.Helper()

	require.NotEmpty(t, chain, "chain must hold at least the FULL")

	tip := chain[len(chain)-1]

	return parseLSN(t, tip.StopLSN)
}

// parseLSN parses a textual LSN carried by a backup list item, failing the test
// on a malformed value.
func parseLSN(t *testing.T, text string) walmath.LSN {
	t.Helper()

	lsn, err := walmath.ParseLSN(text)
	require.NoError(t, err)

	return lsn
}

// buildIncrementalViaAPI drives one incremental end to end through the HTTP API:
// it crosses a WAL segment boundary and waits for summaries past parentStopLSN
// (pg_basebackup --incremental needs them), triggers the incremental, and waits
// for the chain to show wantIncrementalsAfter completed incrementals. Returns the
// updated chain.
func buildIncrementalViaAPI(
	t *testing.T,
	ctx context.Context,
	router *gin.Engine,
	conn *pgx.Conn,
	fixture *postgresql_executor.PhysicalDBFixture,
	parentStopLSN walmath.LSN,
	wantIncrementalsAfter int,
) []backups_dto_physical.PhysicalBackupListItem {
	t.Helper()

	_, err := postgresql_executor.GenerateWalActivity(ctx, conn, 32*1024*1024)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "CHECKPOINT")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "SELECT pg_switch_wal()")
	require.NoError(t, err)

	require.NoError(t, postgresql_executor.WaitForWalSummaries(ctx, conn, parentStopLSN, 2*time.Minute))

	triggerIncrementalViaAPI(t, router, fixture)

	return waitForChainBackups(t, router, fixture, wantIncrementalsAfter, 3*time.Minute)
}

// requestRestoreTokenViaAPI issues a restore-stream token for the given PITR
// target (nil ⇒ latest) and returns it.
func requestRestoreTokenViaAPI(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
	targetTime *time.Time,
) string {
	t.Helper()

	var response backups_dto_physical.GenerateRestoreTokenResponse
	test_utils.MakePostRequestAndUnmarshal(t, router,
		"/api/v1/backups/physical/database/"+fixture.DB.ID.String()+"/restore-token",
		"Bearer "+fixture.Owner.Token,
		backups_dto_physical.GenerateRestoreTokenRequest{TargetTime: targetTime},
		http.StatusOK, &response)

	require.NotEmpty(t, response.Token)

	return response.Token
}

// requestRestoreTokenExpectingStatus posts a restore-token request expecting a
// specific non-200 status (used by the WAL-gap test, which expects 422) and
// returns the response for body assertions.
func requestRestoreTokenExpectingStatus(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
	targetTime *time.Time,
	expectedStatus int,
) *test_utils.TestResponse {
	t.Helper()

	return test_utils.MakePostRequest(t, router,
		"/api/v1/backups/physical/database/"+fixture.DB.ID.String()+"/restore-token",
		"Bearer "+fixture.Owner.Token,
		backups_dto_physical.GenerateRestoreTokenRequest{TargetTime: targetTime},
		expectedStatus)
}

// downloadRestoreBundleViaAPI requests a restore token then streams the bundle tar
// from the public restore-stream endpoint to a host temp file, returning its path.
func downloadRestoreBundleViaAPI(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
	targetTime *time.Time,
) string {
	t.Helper()

	token := requestRestoreTokenViaAPI(t, router, fixture, targetTime)

	recorder := workspaces_testing.MakeAPIRequest(router, "GET",
		"/api/v1/backups/physical/restore-stream?token="+token, "", nil)
	require.Equal(t, http.StatusOK, recorder.Code,
		"restore-stream must return 200; body: %s", recorder.Body.String())

	hostPath := filepath.Join(t.TempDir(), "restore.tar")
	require.NoError(t, os.WriteFile(hostPath, recorder.Body.Bytes(), 0o600))

	return hostPath
}

// committedWalSegmentsInOrder lists the database's committed (uploaded) WAL
// segments through the flat backup API (type=WAL) and returns them ordered by
// start_lsn ascending. The WAL union only emits rows whose file is uploaded, so
// every listed row is committed — no extra filtering needed.
func committedWalSegmentsInOrder(
	t *testing.T,
	router *gin.Engine,
	fixture *postgresql_executor.PhysicalDBFixture,
) []backups_dto_physical.PhysicalBackupListItem {
	t.Helper()

	segments := listBackupsByType(t, router, fixture, physical_enums.PhysicalBackupTypeWal)

	slices.SortFunc(segments, func(a, b backups_dto_physical.PhysicalBackupListItem) int {
		return cmp.Compare(parseLSN(t, a.StartLSN), parseLSN(t, b.StartLSN))
	})

	return segments
}

// streamPostFullSegments forces WAL rotations until at least minCount committed
// segments lie past fullStopLSN (i.e. in the replayable range after the FULL),
// returning them ordered by start_lsn. pg_receivewal writes full, contiguous
// segments, so the run has no gap until a caller deletes one.
func streamPostFullSegments(
	t *testing.T,
	ctx context.Context,
	router *gin.Engine,
	conn *pgx.Conn,
	fixture *postgresql_executor.PhysicalDBFixture,
	fullStopLSN walmath.LSN,
	minCount int,
	timeout time.Duration,
) []backups_dto_physical.PhysicalBackupListItem {
	t.Helper()

	deadline := time.Now().UTC().Add(timeout)

	for time.Now().UTC().Before(deadline) {
		_, err := postgresql_executor.ForceWalRotation(ctx, conn)
		require.NoError(t, err)

		var postFull []backups_dto_physical.PhysicalBackupListItem
		for _, segment := range committedWalSegmentsInOrder(t, router, fixture) {
			if parseLSN(t, segment.StopLSN) > fullStopLSN {
				postFull = append(postFull, segment)
			}
		}

		if len(postFull) >= minCount {
			return postFull
		}

		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("fewer than %d committed post-FULL WAL segments archived within %s", minCount, timeout)

	return nil
}

// createMarkerTable (re)creates the restore_marker table on the source DB, with
// cleanup. Each restore test seeds phase rows here and asserts which survived on
// the restored cluster.
func createMarkerTable(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()

	_, err := conn.Exec(ctx, `DROP TABLE IF EXISTS restore_marker`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), `DROP TABLE IF EXISTS restore_marker`)
	})

	_, err = conn.Exec(ctx,
		`CREATE TABLE restore_marker (phase TEXT PRIMARY KEY, payload TEXT NOT NULL)`)
	require.NoError(t, err)
}

func insertMarker(t *testing.T, ctx context.Context, conn *pgx.Conn, phase, payload string) {
	t.Helper()

	_, err := conn.Exec(ctx,
		`INSERT INTO restore_marker (phase, payload) VALUES ($1, $2)`, phase, payload)
	require.NoError(t, err)
}

// waitForReplayableThroughLSN blocks until the resolver's latest restore set has a
// contiguous WAL run reaching throughLSN, i.e. the streamed WAL covering the PITR
// target is gap-free and shippable. It drives the same resolver the restore stream
// uses, so it waits on exactly the condition the stream needs.
func waitForReplayableThroughLSN(t *testing.T, databaseID uuid.UUID, throughLSN walmath.LSN, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().UTC().Add(timeout)

	var lastReachable walmath.LSN
	for time.Now().UTC().Before(deadline) {
		set, err := chain_view.GetChainViewService().ResolveRestoreSet(databaseID, nil)
		require.NoError(t, err)

		lastReachable = set.MaxReplayableLSN
		if set.MaxReplayableLSN >= throughLSN {
			return
		}

		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("contiguous replayable WAL never reached LSN %s within %s (latest reachable %s)",
		throughLSN.String(), timeout, lastReachable.String())
}

// reconstructCluster rebuilds PGDATA the way a user does: it runs the
// server-shipped recovery script (fetched from the public endpoint) inside the
// restore target. The script extracts the bundle, folds the incremental chain with
// pg_combinebackup, decompresses WAL on demand and arms PITR from the --target-time
// argument (nil ⇒ latest) — so the test drives the real restore path end to end,
// not a reimplementation.
func reconstructCluster(
	t *testing.T,
	target containers.RestoreTarget,
	router *gin.Engine,
	hostBundle string,
	targetTime *time.Time,
) {
	t.Helper()

	hostScript := fetchRecoveryScript(t, router)

	// docker cp (testcontainers CopyFileToContainer) cannot write into a tmpfs mount,
	// so stage the bundle and script under /tmp; the script runs in-container and
	// writes its output into the /restore tmpfs itself, which is fine.
	stagedBundle := "/tmp/restore.tar"
	stagedScript := "/tmp/databasus-recovery.sh"

	target.CopyFileToContainer(t, hostBundle, stagedBundle, 0o644)
	target.CopyFileToContainer(t, hostScript, stagedScript, 0o755)

	if targetTime != nil {
		formattedTarget := targetTime.UTC().Format("2006-01-02 15:04:05-07:00")
		target.Exec(t, "sh", stagedScript, "--target-time", formattedTarget, stagedBundle, restoreWorkDir)
	} else {
		target.Exec(t, "sh", stagedScript, stagedBundle, restoreWorkDir)
	}

	// pg_ctl runs as the postgres user, so the reconstructed data dir and the WAL
	// the restore_command reads must be owned by it.
	target.Exec(t, "chown", "-R", "postgres:postgres", restoreWorkDir)
	target.Exec(t, "chmod", "0700", restoreWorkDir+"/data")
}

// fetchRecoveryScript pulls the restore helper from the public endpoint — the same
// bytes the UI tells the user to curl — and writes it to a host temp file.
func fetchRecoveryScript(t *testing.T, router *gin.Engine) string {
	t.Helper()

	recorder := workspaces_testing.MakeAPIRequest(router, "GET",
		"/api/v1/backups/physical/recovery-script", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code,
		"recovery-script must return 200; body: %s", recorder.Body.String())
	require.NotEmpty(t, recorder.Body.Bytes(), "recovery-script must not be empty")

	hostPath := filepath.Join(t.TempDir(), "databasus-recovery.sh")
	require.NoError(t, os.WriteFile(hostPath, recorder.Body.Bytes(), 0o700))

	return hostPath
}

func startRestoredCluster(t *testing.T, target containers.RestoreTarget) {
	t.Helper()

	target.Exec(t, "sh", "-c",
		"touch "+restoreWorkDir+"/pg.log && chown postgres:postgres "+restoreWorkDir+"/pg.log")

	// Surface the server log on failure — recovery errors are otherwise invisible
	// behind pg_ctl's generic "could not start server".
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}

		t.Logf("=== restored pg.log ===\n%s", target.ExecBestEffort("", "cat", restoreWorkDir+"/pg.log"))
	})

	target.ExecAs(t, "postgres",
		"pg_ctl", "-D", restoreWorkDir+"/data", "-l", restoreWorkDir+"/pg.log", "-w", "start")

	t.Cleanup(func() {
		target.ExecBestEffort("postgres",
			"pg_ctl", "-D", restoreWorkDir+"/data", "-m", "immediate", "stop")
	})
}

func queryRestoredMarkerRows(t *testing.T, target containers.RestoreTarget) []string {
	t.Helper()

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		target.Host(), target.MappedPort(), restoredPgUser, restoredPgPassword, restoredPgDatabase)

	conn := connectWithRetry(t, dsn, 30*time.Second)
	defer func() { _ = conn.Close(t.Context()) }()

	rows, err := conn.Query(t.Context(), `SELECT phase FROM restore_marker ORDER BY phase`)
	require.NoError(t, err)
	defer rows.Close()

	var phases []string
	for rows.Next() {
		var phase string
		require.NoError(t, rows.Scan(&phase))
		phases = append(phases, phase)
	}
	require.NoError(t, rows.Err())

	return phases
}

func openSourceTestDBConn(t *testing.T, fixture *postgresql_executor.PhysicalDBFixture) *pgx.Conn {
	t.Helper()

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		fixture.DB.PostgresqlPhysical.Host,
		fixture.DB.PostgresqlPhysical.Port,
		restoredPgUser,
		restoredPgPassword,
		restoredPgDatabase,
	)

	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	return conn
}

func connectWithRetry(t *testing.T, dsn string, timeout time.Duration) *pgx.Conn {
	t.Helper()

	deadline := time.Now().UTC().Add(timeout)
	var lastErr error

	for time.Now().UTC().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)

		conn, err := pgx.Connect(ctx, dsn)
		cancel()

		if err == nil {
			return conn
		}

		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("could not connect to restored PG within %s: %v", timeout, lastErr)

	return nil
}
