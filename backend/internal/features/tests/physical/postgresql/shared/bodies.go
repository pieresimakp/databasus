package physicaltesting

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	physical_repositories "databasus-backend/internal/features/backups/backups/core/physical/repositories"
	postgresql_executor "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	postgresql_physical "databasus-backend/internal/features/databases/databases/postgresql/physical"
	"databasus-backend/internal/util/walmath"
)

// RunFullOnlyRecoversBaseRows drives the whole happy path through the HTTP control plane: seed a
// row, enable backups (the scheduler bootstraps the FULL), then pull the restore bundle through the
// restore-token + restore-stream endpoints, reconstruct the cluster (pg_combinebackup), and start it.
// The restored cluster must hold the row written before the FULL. Backups run through a
// replication-only user provisioned over the API.
func RunFullOnlyRecoversBaseRows(t *testing.T, version, image string) {
	router, fixture := setupReplicationOnlyFixture(t, version, image, postgresql_physical.BackupTypeFullAndIncremental)
	target := prepareRestoreTarget(t, image)

	sourceConn := openSourceTestDBConn(t, fixture)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	createMarkerTable(t, ctx, sourceConn)
	insertMarker(t, ctx, sourceConn, "before-full", "row-before-full")

	enablePhysicalBackupsViaAPI(t, router, fixture, false)
	waitForChainBackups(t, router, fixture, 0, 3*time.Minute)

	bundle := downloadRestoreBundleViaAPI(t, router, fixture, nil)
	reconstructCluster(t, target, router, bundle, nil)
	startRestoredCluster(t, target)

	restoredPhases := queryRestoredMarkerRows(t, target)
	assert.ElementsMatch(t, []string{"before-full"}, restoredPhases,
		"a full-only restore must recover the row written before the FULL")
}

// RunFullPlusTwoIncrementalsRecoversAllRows builds a FULL → INCR → INCR chain entirely over HTTP
// (config-enable for the FULL, the trigger endpoint for each incremental), restores the latest point,
// and asserts every row written before each backup survives — proving the incrementals chain and
// combine correctly.
func RunFullPlusTwoIncrementalsRecoversAllRows(t *testing.T, version, image string) {
	router, fixture := setupReplicationOnlyFixture(t, version, image, postgresql_physical.BackupTypeFullAndIncremental)
	target := prepareRestoreTarget(t, image)

	sourceConn := openSourceTestDBConn(t, fixture)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	createMarkerTable(t, ctx, sourceConn)
	insertMarker(t, ctx, sourceConn, "before-full", "row-before-full")

	enablePhysicalBackupsViaAPI(t, router, fixture, false)
	chain := waitForChainBackups(t, router, fixture, 0, 3*time.Minute)

	insertMarker(t, ctx, sourceConn, "after-full", "row-between-full-and-incr1")
	chain = buildIncrementalViaAPI(t, ctx, router, sourceConn, fixture, chainTipStopLSN(t, chain), 1)

	insertMarker(t, ctx, sourceConn, "after-incr1", "row-between-incr1-and-incr2")
	buildIncrementalViaAPI(t, ctx, router, sourceConn, fixture, chainTipStopLSN(t, chain), 2)

	bundle := downloadRestoreBundleViaAPI(t, router, fixture, nil)
	reconstructCluster(t, target, router, bundle, nil)
	startRestoredCluster(t, target)

	restoredPhases := queryRestoredMarkerRows(t, target)
	assert.ElementsMatch(t,
		[]string{"before-full", "after-full", "after-incr1"},
		restoredPhases,
		"restoring the latest point must recover rows from the FULL and both incrementals")
}

// RunFullTwoIncrementalsPlusWalRecoversToTarget extends the chain test with point-in-time recovery:
// after FULL → INCR → INCR it streams WAL past a captured target, restores the bundle WITH that
// target time, and asserts the chain rows plus the pre-target row replay while the post-target row is
// dropped — proving full + incrementals + streamed WAL combine and stop at the target. WAL streaming
// is driven purely by the config API: enabling the WAL-stream backup type makes the supervisor claim
// the database and start pg_receivewal.
func RunFullTwoIncrementalsPlusWalRecoversToTarget(t *testing.T, version, image string) {
	if testing.Short() {
		t.Skip("streams WAL and runs a real PITR recovery; skipped in -short")
	}

	router, fixture := setupReplicationOnlyFixture(
		t, version, image, postgresql_physical.BackupTypeFullIncrementalAndWalStream)
	target := prepareRestoreTarget(t, image)

	sourceConn := openSourceTestDBConn(t, fixture)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	createMarkerTable(t, ctx, sourceConn)
	insertMarker(t, ctx, sourceConn, "before-full", "row-in-base-backup")

	enablePhysicalBackupsViaAPI(t, router, fixture, true)
	chain := waitForChainBackups(t, router, fixture, 0, 3*time.Minute)

	insertMarker(t, ctx, sourceConn, "after-full", "row-between-full-and-incr1")
	chain = buildIncrementalViaAPI(t, ctx, router, sourceConn, fixture, chainTipStopLSN(t, chain), 1)

	insertMarker(t, ctx, sourceConn, "after-incr1", "row-between-incr1-and-incr2")
	buildIncrementalViaAPI(t, ctx, router, sourceConn, fixture, chainTipStopLSN(t, chain), 2)

	// 'before-target' is committed after the last INCR; PITR must replay it from
	// streamed WAL. Fill the segment with natural WAL so it rotates and archives;
	// pg_switch_wal is deliberately avoided here (it leaves a partial segment the
	// resolver treats as a gap).
	insertMarker(t, ctx, sourceConn, "before-target", "row-replayed-up-to-target")

	_, err := postgresql_executor.GenerateWalActivity(ctx, sourceConn, 64*1024*1024)
	require.NoError(t, err)

	// A margin wider than the whole-second recovery_target_time truncation keeps
	// the cut point unambiguous.
	time.Sleep(2 * time.Second)
	targetTime := time.Now().UTC()
	time.Sleep(2 * time.Second)

	insertMarker(t, ctx, sourceConn, "after-target", "row-after-target-must-be-absent")

	var afterTargetLSN walmath.LSN
	require.NoError(t, sourceConn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&afterTargetLSN))

	_, err = postgresql_executor.GenerateWalActivity(ctx, sourceConn, 64*1024*1024)
	require.NoError(t, err)

	waitForReplayableThroughLSN(t, fixture.DB.ID, afterTargetLSN, 90*time.Second)

	bundle := downloadRestoreBundleViaAPI(t, router, fixture, &targetTime)
	reconstructCluster(t, target, router, bundle, &targetTime)
	startRestoredCluster(t, target)

	restoredPhases := queryRestoredMarkerRows(t, target)
	assert.ElementsMatch(t,
		[]string{"before-full", "after-full", "after-incr1", "before-target"},
		restoredPhases,
		"PITR must replay the chain plus rows committed at/before the target and drop the row after it")
}

// RunWhenWalGapBeforeTargetTokenRequestReturns422 proves a WAL gap is refused at token-issue time:
// it streams a contiguous run of segments, punches a hole by deleting a committed middle segment, then
// asks for a restore token targeting a time past the gap and expects HTTP 422. No reconstruction —
// the contract is that an unreachable target never mints a token.
func RunWhenWalGapBeforeTargetTokenRequestReturns422(t *testing.T, version, image string) {
	if testing.Short() {
		t.Skip("streams WAL to build and then break a chain; skipped in -short")
	}

	router, fixture := setupReplicationOnlyFixture(
		t, version, image, postgresql_physical.BackupTypeFullIncrementalAndWalStream)

	sourceConn := openSourceTestDBConn(t, fixture)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	enablePhysicalBackupsViaAPI(t, router, fixture, true)
	chain := waitForChainBackups(t, router, fixture, 0, 3*time.Minute)
	fullStopLSN := chainTipStopLSN(t, chain)

	// Stream a run of post-FULL segments, then drop a committed middle one so a
	// real hole sits in the replayable WAL ahead of the target.
	postFull := streamPostFullSegments(t, ctx, router, sourceConn, fixture, fullStopLSN, 3, 90*time.Second)
	removed := postFull[len(postFull)/2]
	require.NoError(t, physical_repositories.GetWalSegmentRepository().DeleteByID(removed.ID))

	gaps := postgresql_executor.WaitForWalGap(t, rootFullBackupID(t, chain), 30*time.Second)
	require.NotEmpty(t, gaps, "deleting a committed middle segment must surface a gap in the chain")

	targetTime := time.Now().UTC()
	response := requestRestoreTokenExpectingStatus(
		t, router, fixture, &targetTime, http.StatusUnprocessableEntity)

	var body map[string]string
	require.NoError(t, json.Unmarshal(response.Body, &body))
	assert.Contains(t, body["error"], "wal gap",
		"the gap must be reported so the user never burns a token on an unreachable target")
}

// RunWalSlotAppearsWhenBackupingStartsRemovedWhenDatabaseDeleted proves the WAL replication-slot
// lifecycle end to end and purely over the API: no slot exists until WAL-stream backups are enabled,
// enabling them makes the supervisor create the persistent slot, and deleting the database (DELETE
// endpoint → cleanup listeners) removes it so nothing is left behind.
func RunWalSlotAppearsWhenBackupingStartsRemovedWhenDatabaseDeleted(t *testing.T, version, image string) {
	router, fixture := setupReplicationOnlyFixture(
		t, version, image, postgresql_physical.BackupTypeFullIncrementalAndWalStream)

	adminConn := postgresql_executor.OpenAdminConn(t, fixture)
	slotName := fixture.DB.PostgresqlPhysical.ReplicationSlotName
	require.NotEmpty(t, slotName, "a physical database must be assigned a slot name on creation")
	require.False(t, postgresql_executor.SlotExists(t, adminConn, slotName),
		"no WAL slot must exist before backuping is enabled")

	enablePhysicalBackupsViaAPI(t, router, fixture, true)
	waitForSlotPresent(t, adminConn, slotName, 30*time.Second)

	deleteDatabaseViaAPI(t, router, fixture)

	requireDatabaseSlotsGone(t, adminConn, fixture, 30*time.Second)
}

// RunWalSlotWhenDatabaseDeletedWithStreamedWalSlotRemovedSoNoWalStuck covers the failure-prone
// case: a database whose slot has actively streamed WAL (so it is pinning WAL on the source) is
// deleted. The cleanup must still drop the slot — otherwise WAL is stuck in the container forever.
// Generating real WAL first makes the slot's receiver active at deletion time, exercising the path a
// naive "refuse to drop an active slot" cleanup would orphan.
func RunWalSlotWhenDatabaseDeletedWithStreamedWalSlotRemovedSoNoWalStuck(t *testing.T, version, image string) {
	if testing.Short() {
		t.Skip("streams real WAL before deleting; skipped in -short")
	}

	router, fixture := setupReplicationOnlyFixture(
		t, version, image, postgresql_physical.BackupTypeFullIncrementalAndWalStream)

	adminConn := postgresql_executor.OpenAdminConn(t, fixture)
	slotName := fixture.DB.PostgresqlPhysical.ReplicationSlotName

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	enablePhysicalBackupsViaAPI(t, router, fixture, true)
	waitForChainBackups(t, router, fixture, 0, 3*time.Minute)
	waitForSlotPresent(t, adminConn, slotName, 30*time.Second)

	// Drive real WAL so the streamer's slot is actively consuming and pinning
	// WAL — the case where a cleanup that refuses an active slot would orphan it.
	sourceConn := openSourceTestDBConn(t, fixture)
	_, err := postgresql_executor.GenerateWalActivity(ctx, sourceConn, 64*1024*1024)
	require.NoError(t, err)

	deleteDatabaseViaAPI(t, router, fixture)

	requireDatabaseSlotsGone(t, adminConn, fixture, 60*time.Second)
}
