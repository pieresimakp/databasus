package backuping_physical

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	physical_enums "databasus-backend/internal/features/backups/backups/core/physical/enums"
	physical_models "databasus-backend/internal/features/backups/backups/core/physical/models"
	physical_repositories "databasus-backend/internal/features/backups/backups/core/physical/repositories"
	physical_testing "databasus-backend/internal/features/backups/backups/core/physical/testing"
	postgresql_executor "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	backups_config_physical "databasus-backend/internal/features/backups/config/physical"
	billing_models "databasus-backend/internal/features/billing/models"
	"databasus-backend/internal/features/intervals"
	"databasus-backend/internal/storage"
	"databasus-backend/internal/util/logger"
)

func Test_IsBackupSlotProtected_WhenOwnerAlive_ReturnsTrue(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	cleaner := CreateTestPhysicalCleaner(activeBilling())

	ownerNodeID := uuid.New()
	_, err := physical_repositories.GetInFlightBackupRepository().Claim(
		storage.GetDb(), physical_repositories.ClaimSpec{
			DatabaseID: prereqs.DB.ID,
			BackupType: physical_enums.PhysicalBackupTypeFull,
			BackupID:   uuid.New(),
			NodeID:     ownerNodeID,
		})
	require.NoError(t, err)

	protected, err := cleaner.isBackupSlotProtected(prereqs.DB.ID, map[uuid.UUID]bool{ownerNodeID: true})
	require.NoError(t, err)
	assert.True(t, protected, "a slot whose owner is a live node must be protected")
}

func Test_IsBackupSlotProtected_WhenOwnerDead_ReturnsFalse(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	cleaner := CreateTestPhysicalCleaner(activeBilling())

	ownerNodeID := uuid.New()
	_, err := physical_repositories.GetInFlightBackupRepository().Claim(
		storage.GetDb(), physical_repositories.ClaimSpec{
			DatabaseID: prereqs.DB.ID,
			BackupType: physical_enums.PhysicalBackupTypeFull,
			BackupID:   uuid.New(),
			NodeID:     ownerNodeID,
		})
	require.NoError(t, err)

	protected, err := cleaner.isBackupSlotProtected(prereqs.DB.ID, map[uuid.UUID]bool{})
	require.NoError(t, err)
	assert.False(t, protected, "a slot whose owner is gone is a reclaimable orphan")
}

func Test_IsBackupSlotProtected_WhenNoClaim_ReturnsFalse(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	cleaner := CreateTestPhysicalCleaner(activeBilling())

	protected, err := cleaner.isBackupSlotProtected(prereqs.DB.ID, map[uuid.UUID]bool{uuid.New(): true})
	require.NoError(t, err)
	assert.False(t, protected, "no in-flight claim means no backup is running, so the slot is droppable")
}

func Test_IsBackupSlotProtected_WhenClaimHasNoOwner_ReturnsFalse(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	cleaner := CreateTestPhysicalCleaner(activeBilling())

	_, err := physical_repositories.GetInFlightBackupRepository().Claim(
		storage.GetDb(), physical_repositories.ClaimSpec{
			DatabaseID: prereqs.DB.ID,
			BackupType: physical_enums.PhysicalBackupTypeFull,
			BackupID:   uuid.New(),
		})
	require.NoError(t, err)

	protected, err := cleaner.isBackupSlotProtected(prereqs.DB.ID, map[uuid.UUID]bool{})
	require.NoError(t, err)
	assert.False(t, protected, "a claim with no recorded owner cannot be proven live, so it is droppable")
}

// Test_RunStartupSlotCleanup_WhenClaimOwnerDead_DropsOrphanSlot is the end-to-end
// proof of the "Databasus crashed mid-backup" path: the source keeps the per-backup
// slot, the in-flight claim survives the crash, and on restart the owning node is
// gone. The startup sweep must read the claim, see the owner is not a live node, and
// drop the orphan against the real source PG — wiring the protection predicate to an
// actual pg_drop_replication_slot, which the isolated isBackupSlotProtected tests
// above do not.
func Test_RunStartupSlotCleanup_WhenClaimOwnerDead_DropsOrphanSlot(t *testing.T) {
	fixture := postgresql_executor.SetupPhysicalDBForBackup(t)
	cleaner := CreateTestPhysicalCleaner(activeBilling())

	slotName := postgresql_executor.SlotName(fixture.DB.PostgresqlPhysical.ID)
	adminConn := postgresql_executor.OpenAdminConn(t, fixture)

	_, err := adminConn.Exec(context.Background(),
		"SELECT pg_create_physical_replication_slot($1, true)", slotName)
	require.NoError(t, err, "pre-create the orphan slot a crashed backup left behind")
	t.Cleanup(func() {
		_, _ = adminConn.Exec(context.Background(),
			`SELECT pg_drop_replication_slot(slot_name)
			   FROM pg_replication_slots WHERE slot_name = $1`, slotName)
	})

	// Re-claim under a node id that was never registered, so the claim's owner is a
	// dead node (the fixture seeds a NULL-owner claim).
	require.NoError(t, physical_repositories.GetInFlightBackupRepository().Release(fixture.DB.ID))
	_, err = physical_repositories.GetInFlightBackupRepository().Claim(
		storage.GetDb(), physical_repositories.ClaimSpec{
			DatabaseID: fixture.DB.ID,
			BackupType: physical_enums.PhysicalBackupTypeFull,
			BackupID:   fixture.BackupID,
			NodeID:     uuid.New(),
		})
	require.NoError(t, err)

	cleaner.runStartupSlotCleanup(t.Context())

	assert.False(t, postgresql_executor.SlotExists(t, adminConn, slotName),
		"a per-backup orphan whose claim owner is a dead node must be dropped at startup")
}

// Test_RunStartupSlotCleanup_WhenClaimOwnerAlive_PreservesUntilClaimReleased pins the
// fast-restart convergence: if the previous owner's heartbeat is still fresh when the
// process comes back, the one-shot startup sweep must NOT drop the slot (a running
// pg_basebackup may still need the pinned WAL). Once the per-tick dead-node sweep later
// releases the stale claim, the same sweep would reclaim the now-unprotected orphan.
func Test_RunStartupSlotCleanup_WhenClaimOwnerAlive_PreservesUntilClaimReleased(t *testing.T) {
	fixture := postgresql_executor.SetupPhysicalDBForBackup(t)
	cleaner := CreateTestPhysicalCleaner(activeBilling())

	slotName := postgresql_executor.SlotName(fixture.DB.PostgresqlPhysical.ID)
	adminConn := postgresql_executor.OpenAdminConn(t, fixture)

	_, err := adminConn.Exec(context.Background(),
		"SELECT pg_create_physical_replication_slot($1, true)", slotName)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = adminConn.Exec(context.Background(),
			`SELECT pg_drop_replication_slot(slot_name)
			   FROM pg_replication_slots WHERE slot_name = $1`, slotName)
	})

	liveOwner := registerFakePhysicalNode(t)
	require.NoError(t, physical_repositories.GetInFlightBackupRepository().Release(fixture.DB.ID))
	_, err = physical_repositories.GetInFlightBackupRepository().Claim(
		storage.GetDb(), physical_repositories.ClaimSpec{
			DatabaseID: fixture.DB.ID,
			BackupType: physical_enums.PhysicalBackupTypeFull,
			BackupID:   fixture.BackupID,
			NodeID:     liveOwner,
		})
	require.NoError(t, err)

	cleaner.runStartupSlotCleanup(t.Context())
	require.True(t, postgresql_executor.SlotExists(t, adminConn, slotName),
		"slot must be preserved while its claim owner still looks alive")

	// The dead-node sweep eventually fails the backup and releases the claim.
	require.NoError(t, physical_repositories.GetInFlightBackupRepository().Release(fixture.DB.ID))

	cleaner.runStartupSlotCleanup(t.Context())
	assert.False(t, postgresql_executor.SlotExists(t, adminConn, slotName),
		"once the stale claim is released, the orphan slot must be reclaimed")
}

func cappedBilling(storageGB int) *mockBillingService {
	return &mockBillingService{
		subscription: &billing_models.Subscription{Status: billing_models.StatusActive, StorageGB: storageGB},
	}
}

// seedChainFull seeds a COMPLETED FULL at a start segment and age so tests can
// build multiple distinct chains with deterministic ordering (higher segment =
// newer = the active head).
func seedChainFull(
	t *testing.T,
	prereqs *backupPrereqs,
	startSegment, ageHours int,
) *physical_models.PhysicalFullBackup {
	t.Helper()

	full := physical_testing.NewTestCompletedFullBackup(
		prereqs.DB.ID, prereqs.Storage.ID, 1, testLSN(startSegment), testLSN(startSegment+1))

	at := time.Now().UTC().Add(-time.Duration(ageHours) * time.Hour)
	full.CreatedAt = at
	full.CompletedAt = &at
	full.BackupSizeMb = new(1000.0)

	return physical_testing.CreateTestFullBackup(t, full)
}

// shortGraceConfig sets hourly cadences so the per-chain grace is 2 h — chains
// older than 2 h become evictable, which keeps retention tests deterministic.
func shortGraceConfig(backupConfig *backups_config_physical.PhysicalBackupConfig) {
	backupConfig.FullBackupInterval = intervals.Interval{Type: intervals.IntervalHourly}
	backupConfig.IncrementalBackupInterval = intervals.Interval{Type: intervals.IntervalHourly}
}

func fullExists(t *testing.T, id uuid.UUID) bool {
	t.Helper()

	full, err := physical_repositories.GetFullBackupRepository().FindByID(id)
	require.NoError(t, err)

	return full != nil
}

func incrementalExists(t *testing.T, id uuid.UUID) bool {
	t.Helper()

	incremental, err := physical_repositories.GetIncrementalBackupRepository().FindByID(id)
	require.NoError(t, err)

	return incremental != nil
}

func walExists(t *testing.T, id uuid.UUID) bool {
	t.Helper()

	segment, err := physical_repositories.GetWalSegmentRepository().FindByID(id)
	require.NoError(t, err)

	return segment != nil
}

func Test_CleanByChains_KeepsNLatestNonExtendableChains(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	prereqs.Config.ChainsRetention = backups_config_physical.ChainsRetention{Count: 2}

	active := seedChainFull(t, prereqs, 8, 0) // newest → extendable head
	keptA := seedChainFull(t, prereqs, 6, 3)  // non-extendable, kept by count
	keptB := seedChainFull(t, prereqs, 4, 6)  // non-extendable, kept by count
	oldA := seedChainFull(t, prereqs, 2, 9)   // non-extendable, evicted
	oldB := seedChainFull(t, prereqs, 0, 12)  // non-extendable, evicted

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByChains(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID), "active chain is never a candidate")
	assert.True(t, fullExists(t, keptA.ID))
	assert.True(t, fullExists(t, keptB.ID))
	assert.False(t, fullExists(t, oldA.ID))
	assert.False(t, fullExists(t, oldB.ID))
}

func Test_CleanByChains_GracePeriodProtectsRecentChainBeyondKeepCount(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config) // 2 h grace
	prereqs.Config.ChainsRetention = backups_config_physical.ChainsRetention{Count: 1}

	active := seedChainFull(t, prereqs, 6, 0)
	recentKept := seedChainFull(t, prereqs, 4, 0)   // within grace, kept by count anyway
	recentBeyond := seedChainFull(t, prereqs, 2, 1) // within grace, beyond count → grace saves it

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByChains(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, recentKept.ID))
	assert.True(t, fullExists(t, recentBeyond.ID), "a chain younger than the grace period is never evicted")
}

func Test_CleanByFullsLastN_KeepsNewestFullsAndDropsTheirIncrAndWal(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	prereqs.Config.FullBackupsRetention = backups_config_physical.FullBackupsRetention{
		Policy: backups_config_physical.FullBackupsRetentionPolicyLastN,
		Count:  2,
	}

	active := seedChainFull(t, prereqs, 9, 0)
	keptFull := seedChainFull(t, prereqs, 6, 3) // non-extendable but in newest-2 fulls
	droppedFull := seedChainFull(t, prereqs, 3, 6)

	// keptFull's chain gets an INCR + WAL that LAST_N must shed while keeping the
	// FULL. Age their timestamps to 3 h so the chain-end stays outside the grace
	// window (a fresh INCR/WAL would make the whole chain grace-protected).
	threeHoursAgo := time.Now().UTC().Add(-3 * time.Hour)

	incrModel := physical_testing.NewTestCompletedIncrementalBackup(
		prereqs.DB.ID, prereqs.Storage.ID, keptFull.ID, nil, 1, testLSN(7), testLSN(8))
	incrModel.CreatedAt = threeHoursAgo
	incrModel.CompletedAt = &threeHoursAgo
	keptIncr := physical_testing.CreateTestIncrementalBackup(t, incrModel)

	walModel := physical_testing.NewTestWalSegment(
		prereqs.DB.ID, prereqs.Storage.ID, 1, "000000010000000000000007", testLSN(7), testLSN(8))
	walModel.ReceivedAt = threeHoursAgo
	keptWal := physical_testing.CreateTestWalSegment(t, walModel)

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByFulls(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, keptFull.ID), "newest-N full kept")
	assert.False(t, incrementalExists(t, keptIncr.ID), "kept full's incr dropped")
	assert.False(t, walExists(t, keptWal.ID), "kept full's wal dropped")
	assert.False(t, fullExists(t, droppedFull.ID), "non-kept chain deleted entirely")
}

func Test_CleanByFullsGfs_KeepsBucketRepresentativesDropsExtras(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	prereqs.Config.FullBackupsRetention = backups_config_physical.FullBackupsRetention{
		Policy:   backups_config_physical.FullBackupsRetentionPolicyGfs,
		GfsHours: 2,
	}

	// 4 distinct hourly buckets; GFS keeps the 2 newest (active + age2).
	active := seedChainFull(t, prereqs, 9, 0)
	keptHour := seedChainFull(t, prereqs, 6, 2)
	droppedA := seedChainFull(t, prereqs, 3, 3)
	droppedB := seedChainFull(t, prereqs, 0, 4)

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByFulls(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, keptHour.ID), "GFS keeps the 2nd-newest hourly bucket")
	assert.False(t, fullExists(t, droppedA.ID))
	assert.False(t, fullExists(t, droppedB.ID))
}

func Test_CleanByCombined_KeepsUnionOfChainsAndFulls(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	prereqs.Config.ChainsRetention = backups_config_physical.ChainsRetention{Count: 1}
	prereqs.Config.FullBackupsRetention = backups_config_physical.FullBackupsRetention{
		Policy: backups_config_physical.FullBackupsRetentionPolicyLastN,
		Count:  3,
	}

	active := seedChainFull(t, prereqs, 9, 0)
	keptByChains := seedChainFull(t, prereqs, 6, 3) // newest non-ext → CHAINS keeps it
	keptByFulls := seedChainFull(t, prereqs, 3, 6)  // 3rd-newest full → FULL_BACKUPS keeps it
	dropped := seedChainFull(t, prereqs, 0, 9)      // in neither keep-set

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByCombined(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, keptByChains.ID), "kept by CHAINS policy")
	assert.True(t, fullExists(t, keptByFulls.ID), "kept by FULL_BACKUPS policy even though CHAINS count=1")
	assert.False(t, fullExists(t, dropped.ID))
}

func Test_EnforceStorageCap_TrimsOldestUntilUnderCapAndKeepsActive(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	enableCloud(t)
	shortGraceConfig(prereqs.Config)

	// 4 chains × 1000 MB = 4000 MB; cap 1 GB (1024 MB) ⇒ trim down to the active.
	active := seedChainFull(t, prereqs, 9, 0)
	c1 := seedChainFull(t, prereqs, 6, 3)
	c2 := seedChainFull(t, prereqs, 4, 6)
	c3 := seedChainFull(t, prereqs, 2, 9)

	cleaner := CreateTestPhysicalCleaner(cappedBilling(1))
	cleaner.enforceStorageCapForDatabase(context.Background(), logger.GetLogger(), prereqs.Config)

	assert.True(t, fullExists(t, active.ID), "active chain is never evicted by the billing pass")
	assert.False(t, fullExists(t, c1.ID))
	assert.False(t, fullExists(t, c2.ID))
	assert.False(t, fullExists(t, c3.ID))
}

func Test_CleanOrphanWalForDatabase_WhenWalOutsideAllChains_DeletesOrphan(t *testing.T) {
	prereqs := seedBackupPrereqs(t)

	// No FULL covers this segment, so it is orphan WAL the cleaner must reclaim.
	orphan := physical_testing.CreateTestWalSegment(t, physical_testing.NewTestWalSegment(
		prereqs.DB.ID, prereqs.Storage.ID, 1, "000000010000000000000005", testLSN(5), testLSN(6)))

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	cleaner.cleanOrphanWalForDatabase(context.Background(), logger.GetLogger(), prereqs.DB.ID)

	assert.False(t, walExists(t, orphan.ID), "WAL not covered by any chain is deleted")
}

func Test_CleanOrphanWalForDatabase_WhenWalCoveredByChain_KeepsIt(t *testing.T) {
	prereqs := seedBackupPrereqs(t)

	// A COMPLETED FULL at segment 1 covers everything from its start_lsn onward on
	// the same timeline, so the segment at 2 is in-chain, not an orphan.
	seedChainFull(t, prereqs, 1, 0)
	covered := physical_testing.CreateTestWalSegment(t, physical_testing.NewTestWalSegment(
		prereqs.DB.ID, prereqs.Storage.ID, 1, "000000010000000000000002", testLSN(2), testLSN(3)))

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	cleaner.cleanOrphanWalForDatabase(context.Background(), logger.GetLogger(), prereqs.DB.ID)

	assert.True(t, walExists(t, covered.ID), "chain-covered WAL must never be caught by the orphan pass")
}

func Test_ReapAbandonedWalClaims_WhenClaimOlderThanGrace_DeletesIt(t *testing.T) {
	prereqs := seedBackupPrereqs(t)

	// An insert-first claim (file_name NULL) whose upload never finished and that
	// has aged past WAL_CLAIM_GRACE (1h). NULL file_name ⇒ no storage object exists.
	twoHoursAgo := time.Now().UTC().Add(-2 * time.Hour)
	abandoned := &physical_models.PhysicalWalSegment{
		DatabaseID:  prereqs.DB.ID,
		StorageID:   prereqs.Storage.ID,
		TimelineID:  1,
		FileName:    nil,
		WalFilename: "000000010000000000000009",
		StartLSN:    testLSN(9),
		EndLSN:      testLSN(10),
		ReceivedAt:  twoHoursAgo,
		ClaimedAt:   twoHoursAgo,
	}
	require.NoError(t, physical_repositories.GetWalSegmentRepository().Insert(abandoned))

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	cleaner.reapAbandonedWalClaims(logger.GetLogger(), prereqs.DB.ID)

	assert.False(
		t,
		walExists(t, abandoned.ID),
		"an abandoned NULL-file_name claim past grace is reaped (no storage I/O)",
	)
}

func Test_ReapAbandonedWalClaims_WhenClaimWithinGrace_KeepsIt(t *testing.T) {
	prereqs := seedBackupPrereqs(t)

	thirtyMinutesAgo := time.Now().UTC().Add(-30 * time.Minute)
	freshClaim := &physical_models.PhysicalWalSegment{
		DatabaseID:  prereqs.DB.ID,
		StorageID:   prereqs.Storage.ID,
		TimelineID:  1,
		FileName:    nil,
		WalFilename: "000000010000000000000009",
		StartLSN:    testLSN(9),
		EndLSN:      testLSN(10),
		ReceivedAt:  thirtyMinutesAgo,
		ClaimedAt:   thirtyMinutesAgo,
	}
	require.NoError(t, physical_repositories.GetWalSegmentRepository().Insert(freshClaim))

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	cleaner.reapAbandonedWalClaims(logger.GetLogger(), prereqs.DB.ID)

	assert.True(t, walExists(t, freshClaim.ID), "a live in-flight claim within grace must survive")
}

func Test_CleanByChains_WhenKeepCountZero_DeletesNothing(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	prereqs.Config.ChainsRetention = backups_config_physical.ChainsRetention{Count: 0}

	active := seedChainFull(t, prereqs, 4, 0)
	old := seedChainFull(t, prereqs, 2, 9)

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByChains(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, old.ID), "keep-count 0 is treated as keep-all, not delete-all")
}

func Test_CleanByFulls_WhenNoEffectiveConfig_DeletesNothing(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	// LAST_N with count 0 yields no keep-set; the policy must no-op rather than
	// interpret an empty keep-set as "delete every chain".
	prereqs.Config.FullBackupsRetention = backups_config_physical.FullBackupsRetention{
		Policy: backups_config_physical.FullBackupsRetentionPolicyLastN,
		Count:  0,
	}

	active := seedChainFull(t, prereqs, 4, 0)
	old := seedChainFull(t, prereqs, 2, 9)

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByFulls(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, old.ID), "an empty keep-set must never delete everything")
}

func Test_CleanByBillingStorageCap_WhenNotCloud_DoesNothing(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	// Deliberately NOT cloud: the billing pass only enforces caps in cloud mode.
	active := seedChainFull(t, prereqs, 4, 0)
	old := seedChainFull(t, prereqs, 2, 9)

	cleaner := CreateTestPhysicalCleaner(cappedBilling(1)) // would trim if cloud
	require.NoError(t, cleaner.cleanByBillingStorageCap(context.Background(), logger.GetLogger()))

	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, old.ID), "self-hosted billing never trims storage")
}

func Test_EnforceStorageCap_WhenNewestChainBrokenByIncr_EvictsEvenNewestChain(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	enableCloud(t)
	shortGraceConfig(prereqs.Config)

	// The newest COMPLETED FULL carries a downstream CHAIN_BROKEN incr, so it is
	// NOT the active chain — no chain is extendable here. At cap 0 the billing pass
	// must be free to evict even the newest, proving it isn't mis-protected.
	newest := seedChainFull(t, prereqs, 9, 3)
	seedIncrWithStatusAndAge(t, prereqs, newest.ID, physical_enums.PhysicalBackupStatusChainBroken, 10, 2)
	middle := seedChainFull(t, prereqs, 6, 6)
	oldest := seedChainFull(t, prereqs, 3, 9)

	cleaner := CreateTestPhysicalCleaner(cappedBilling(0))
	cleaner.enforceStorageCapForDatabase(context.Background(), logger.GetLogger(), prereqs.Config)

	assert.False(t, fullExists(t, newest.ID), "a broken-newest chain is not the active chain, so the cap can evict it")
	assert.False(t, fullExists(t, middle.ID))
	assert.False(t, fullExists(t, oldest.ID))
}

func Test_CleanByChains_WhenInProgressFullExists_NeverDeletesIt(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	shortGraceConfig(prereqs.Config)
	prereqs.Config.ChainsRetention = backups_config_physical.ChainsRetention{Count: 1}

	inProgress := seedFullWithStatusAndAge(t, prereqs, physical_enums.PhysicalBackupStatusInProgress, 9, 1)
	active := seedChainFull(t, prereqs, 6, 3) // newest COMPLETED → extendable head
	keptByCount := seedChainFull(t, prereqs, 4, 6)
	evicted := seedChainFull(t, prereqs, 2, 9)

	cleaner := CreateTestPhysicalCleaner(activeBilling())
	require.NoError(t, cleaner.cleanByChains(context.Background(), logger.GetLogger(), prereqs.Config))

	assert.True(t, fullExists(t, inProgress.ID), "an IN_PROGRESS full is never a retention candidate")
	assert.True(t, fullExists(t, active.ID))
	assert.True(t, fullExists(t, keptByCount.ID))
	assert.False(t, fullExists(t, evicted.ID), "completed chains beyond the keep-count are still evicted")
}

func Test_EnforceStorageCap_WhenRemainingChainsWithinGrace_WarnsAndStopsOverCap(t *testing.T) {
	prereqs := seedBackupPrereqs(t)
	enableCloud(t)
	// Default config grace is ~48h, so chains aged in hours are all grace-protected;
	// the cap pass must warn and stop rather than evict a protected (or active) chain.
	active := seedChainFull(t, prereqs, 6, 0)
	recentNonExtendable := seedChainFull(t, prereqs, 3, 1)

	cleaner := CreateTestPhysicalCleaner(cappedBilling(0)) // everything is over a 0 cap
	cleaner.enforceStorageCapForDatabase(context.Background(), logger.GetLogger(), prereqs.Config)

	assert.True(t, fullExists(t, active.ID), "the active chain is never evicted, even over cap")
	assert.True(t, fullExists(t, recentNonExtendable.ID), "a within-grace chain triggers warn-and-break, not deletion")
}
