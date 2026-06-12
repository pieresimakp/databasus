package backuping_physical

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	"databasus-backend/internal/features/backups/backups/backuping/shared/gfs"
	"databasus-backend/internal/features/backups/backups/core/physical/chain_view"
	physical_models "databasus-backend/internal/features/backups/backups/core/physical/models"
	physical_repositories "databasus-backend/internal/features/backups/backups/core/physical/repositories"
	physical_service "databasus-backend/internal/features/backups/backups/core/physical/service"
	usecases_physical_postgresql "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	backups_config_physical "databasus-backend/internal/features/backups/config/physical"
	"databasus-backend/internal/features/intervals"
)

// PhysicalBackupCleaner trims physical backups every tick through three passes:
// retention policy, cloud storage-cap billing, and orphan sweep. It never
// issues raw DELETEs — every removal goes through PhysicalBackupService — and it
// never touches the active (extendable) chain: it operates only on the
// non-extendable set that chain_view derives.
type PhysicalBackupCleaner struct {
	physicalBackupService *physical_service.PhysicalBackupService
	chainViewService      *chain_view.ChainViewService
	backupConfigService   *backups_config_physical.BackupConfigService
	fullRepo              *physical_repositories.PhysicalFullBackupRepository
	walSegmentRepo        *physical_repositories.PhysicalWalSegmentRepository
	inFlightRepo          *physical_repositories.PhysicalInFlightBackupRepository
	nodesRegistry         *nodes.BackupNodesRegistry
	billingService        BillingService
	logger                *slog.Logger

	hasRun atomic.Bool
}

func (c *PhysicalBackupCleaner) Run(ctx context.Context) {
	if c.hasRun.Swap(true) {
		panic(fmt.Sprintf("%T.Run() called multiple times", c))
	}

	if ctx.Err() != nil {
		return
	}

	if config.GetEnv().IsManyNodesMode {
		// Mirror the scheduler's startup delay so processing nodes re-register
		// (the primary wipes the registry cache on boot) before we judge which
		// slots still belong to a live backup.
		time.Sleep(schedulerStartupDelay)
	}

	c.runStartupSlotCleanup(ctx)

	ticker := time.NewTicker(cleanerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger := c.logger.With("job_id", uuid.New(), "job_name", cleanerJobName)

			if err := c.cleanByRetentionPolicy(ctx, logger); err != nil {
				logger.Error("failed to clean by retention policy", "error", err)
			}

			if err := c.cleanByBillingStorageCap(ctx, logger); err != nil {
				logger.Error("failed to clean by billing storage cap", "error", err)
			}

			if err := c.cleanOrphans(ctx, logger); err != nil {
				logger.Error("failed to clean orphans", "error", err)
			}
		}
	}
}

// runStartupSlotCleanup drops orphan per-backup replication slots once at boot,
// preserving any slot whose backup is still running on a live node. The live-node
// set is snapshotted once; if it cannot be read, cleanup is skipped entirely
// rather than risk dropping a running backup's slot (an orphan is recovered later
// by the next backup's pre-create drop, but a wrongly dropped live slot is not).
func (c *PhysicalBackupCleaner) runStartupSlotCleanup(ctx context.Context) {
	aliveNodeIDs, err := c.nodesRegistry.GetAvailableNodeIDs()
	if err != nil {
		c.logger.Error("skipping startup slot cleanup: cannot determine live nodes", "error", err)

		return
	}

	isSlotProtected := func(databaseID uuid.UUID) (bool, error) {
		return c.isBackupSlotProtected(databaseID, aliveNodeIDs)
	}

	if err := usecases_physical_postgresql.RunStartupCleanup(ctx, c.logger, isSlotProtected); err != nil {
		c.logger.Error("physical startup slot cleanup reported failures", "error", err)
	}
}

// isBackupSlotProtected reports whether a database's per-backup slot must be kept
// because a backup is still running on a live node — a claim whose owner is in
// the live-node set. No claim, or an unknown/dead owner, means the slot is a
// reclaimable orphan.
func (c *PhysicalBackupCleaner) isBackupSlotProtected(
	databaseID uuid.UUID,
	aliveNodeIDs map[uuid.UUID]bool,
) (bool, error) {
	claim, err := c.inFlightRepo.FindByDatabaseID(databaseID)
	if err != nil {
		return false, err
	}

	if claim == nil || claim.NodeID == nil {
		return false, nil
	}

	return aliveNodeIDs[*claim.NodeID], nil
}

func (c *PhysicalBackupCleaner) cleanByRetentionPolicy(ctx context.Context, logger *slog.Logger) error {
	enabledConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledConfigs {
		dbLog := logger.With("database_id", backupConfig.DatabaseID, "retention", backupConfig.Retention)

		var cleanErr error

		switch backupConfig.Retention {
		case backups_config_physical.RetentionChains:
			cleanErr = c.cleanByChains(ctx, dbLog, backupConfig)
		case backups_config_physical.RetentionFullBackups:
			cleanErr = c.cleanByFulls(ctx, dbLog, backupConfig)
		case backups_config_physical.RetentionChainsAndFullBackups:
			cleanErr = c.cleanByCombined(ctx, dbLog, backupConfig)
		}

		if cleanErr != nil {
			dbLog.Error("failed to clean database by retention policy", "error", cleanErr)
		}
	}

	return nil
}

// nonExtendableChainsNewestEndFirst returns every non-extendable chain for a DB
// sorted by chain-end timestamp, newest first. The active (extendable) head is
// already excluded by FindNonExtendableChainsByDatabase, so nothing here can
// ever delete it.
func (c *PhysicalBackupCleaner) nonExtendableChainsNewestEndFirst(
	databaseID uuid.UUID,
) ([]chainCandidate, error) {
	views, err := c.chainViewService.FindNonExtendableChainsByDatabase(databaseID)
	if err != nil {
		return nil, err
	}

	candidates := make([]chainCandidate, 0, len(views))

	for _, view := range views {
		endTs, err := c.chainViewService.GetChainEndTimestamp(view.RootFull.ID)
		if err != nil {
			c.logger.Error("failed to get chain end timestamp",
				"root_full_backup_id", view.RootFull.ID, "error", err)

			continue
		}

		candidates = append(candidates, chainCandidate{view: view, endTs: endTs})
	}

	slices.SortFunc(candidates, func(a, b chainCandidate) int {
		return b.endTs.Compare(a.endTs)
	})

	return candidates, nil
}

// deleteChainThroughService removes a whole chain via the service, bounded by
// the per-tick WAL byte budget, logging progress.
func (c *PhysicalBackupCleaner) deleteChainThroughService(
	ctx context.Context,
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	rootFullBackupID uuid.UUID,
) {
	summary, err := c.physicalBackupService.DeleteFull(
		ctx, rootFullBackupID, c.walDeleteBudgetMB(backupConfig.DatabaseID),
	)
	if err != nil {
		logger.Error("failed to delete chain", "root_full_backup_id", rootFullBackupID, "error", err)

		return
	}

	logger.Info(fmt.Sprintf(
		"chain cleanup progress: %d wal, %d incr, %d history, %.1f MB, complete=%v",
		summary.WalSegments, summary.Incrementals, summary.HistoryFiles,
		summary.BytesDeletedMB, summary.ChainFullyDeleted,
	), "root_full_backup_id", rootFullBackupID)
}

// deleteChainDependentsThroughService drops a chain's INCRs and WAL but keeps
// the FULL — used by FULL_BACKUPS policies that retain a kept FULL as a
// standalone restore point.
func (c *PhysicalBackupCleaner) deleteChainDependentsThroughService(
	ctx context.Context,
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	rootFullBackupID uuid.UUID,
) {
	summary, err := c.physicalBackupService.DeleteChainDependentsKeepFull(
		ctx, rootFullBackupID, c.walDeleteBudgetMB(backupConfig.DatabaseID),
	)
	if err != nil {
		logger.Error("failed to delete chain dependents", "root_full_backup_id", rootFullBackupID, "error", err)

		return
	}

	logger.Info(fmt.Sprintf(
		"chain dependents cleanup: %d wal, %d incr, %.1f MB",
		summary.WalSegments, summary.Incrementals, summary.BytesDeletedMB,
	), "root_full_backup_id", rootFullBackupID)
}

// walDeleteBudgetMB anchors the per-tick WAL byte budget to the latest COMPLETED
// FULL's size, floored at minWalDeleteBudgetMB.
func (c *PhysicalBackupCleaner) walDeleteBudgetMB(databaseID uuid.UUID) float64 {
	fulls, err := c.fullRepo.FindCompletedNewestFirstByDatabase(databaseID)
	if err == nil && len(fulls) > 0 && fulls[0].BackupSizeMb != nil {
		return math.Max(*fulls[0].BackupSizeMb, minWalDeleteBudgetMB)
	}

	return minWalDeleteBudgetMB
}

// isChainWithinGrace reports whether a chain is too young to evict: its end
// timestamp is within max(full, incr cadence) × 2 (floored at the 60-minute
// per-backup grace). A failure to read the timestamp is treated as within grace
// so a transient error never causes a premature delete.
func (c *PhysicalBackupCleaner) isChainWithinGrace(
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	rootFullBackupID uuid.UUID,
) bool {
	endTs, err := c.chainViewService.GetChainEndTimestamp(rootFullBackupID)
	if err != nil {
		c.logger.Error("failed to get chain end timestamp; treating as within grace",
			"root_full_backup_id", rootFullBackupID, "error", err)

		return true
	}

	grace := max(
		approxIntervalDuration(backupConfig.FullBackupInterval),
		approxIntervalDuration(backupConfig.IncrementalBackupInterval),
	) * chainGraceIntervalMultiplier

	grace = max(grace, recentBackupGracePeriod)

	return time.Since(endTs) < grace
}

// cleanByChains keeps the N newest non-extendable chains (by chain-end
// timestamp) and deletes the rest, honoring the per-chain grace period. The
// active chain is never a candidate.
func (c *PhysicalBackupCleaner) cleanByChains(
	ctx context.Context,
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) error {
	keepCount := backupConfig.ChainsRetention.Count
	if keepCount <= 0 {
		return nil
	}

	candidates, err := c.nonExtendableChainsNewestEndFirst(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	if len(candidates) <= keepCount {
		return nil
	}

	for _, candidate := range candidates[keepCount:] {
		if c.isChainWithinGrace(backupConfig, candidate.view.RootFull.ID) {
			continue
		}

		c.deleteChainThroughService(ctx, logger, backupConfig, candidate.view.RootFull.ID)
	}

	return nil
}

// cleanByFulls dispatches the FULL_BACKUPS retention policy to its LAST_N or GFS
// variant. Both keep a set of FULLs; non-extendable chains rooted at a kept FULL
// shed their INCR + WAL (the FULL stays a standalone restore point), and chains
// rooted at a non-kept FULL are deleted whole.
func (c *PhysicalBackupCleaner) cleanByFulls(
	ctx context.Context,
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) error {
	completedFulls, err := c.fullRepo.FindCompletedNewestFirstByDatabase(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	keepFullIDs := c.fullBackupsKeepSet(backupConfig, completedFulls)
	if keepFullIDs == nil {
		return nil
	}

	candidates, err := c.nonExtendableChainsNewestEndFirst(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	for _, candidate := range candidates {
		rootFullBackupID := candidate.view.RootFull.ID

		if c.isChainWithinGrace(backupConfig, rootFullBackupID) {
			continue
		}

		if keepFullIDs[rootFullBackupID] {
			c.deleteChainDependentsThroughService(ctx, logger, backupConfig, rootFullBackupID)

			continue
		}

		c.deleteChainThroughService(ctx, logger, backupConfig, rootFullBackupID)
	}

	return nil
}

// fullBackupsKeepSet computes the kept-FULL id set for the FULL_BACKUPS policy.
// Returns nil when the policy has no effective configuration (so the caller
// no-ops rather than treating an empty set as "delete everything").
func (c *PhysicalBackupCleaner) fullBackupsKeepSet(
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	completedFullsNewestFirst []*physical_models.PhysicalFullBackup,
) map[uuid.UUID]bool {
	retention := backupConfig.FullBackupsRetention

	switch retention.Policy {
	case backups_config_physical.FullBackupsRetentionPolicyLastN:
		if retention.Count <= 0 {
			return nil
		}

		keep := make(map[uuid.UUID]bool)
		for i, full := range completedFullsNewestFirst {
			if i < retention.Count {
				keep[full.ID] = true
			}
		}

		return keep

	case backups_config_physical.FullBackupsRetentionPolicyGfs:
		if retention.GfsHours <= 0 && retention.GfsDays <= 0 && retention.GfsWeeks <= 0 &&
			retention.GfsMonths <= 0 && retention.GfsYears <= 0 {
			return nil
		}

		items := make([]gfs.Item, len(completedFullsNewestFirst))
		for i, full := range completedFullsNewestFirst {
			items[i] = gfs.Item{ID: full.ID, CreatedAt: full.CreatedAt}
		}

		return gfs.GetItemsToRetain(
			items, retention.GfsHours, retention.GfsDays, retention.GfsWeeks,
			retention.GfsMonths, retention.GfsYears,
		)
	}

	return nil
}

// cleanByCombined keeps the UNION of the CHAINS keep-set (N newest
// non-extendable chains) and the FULL_BACKUPS keep-set (LAST_N or GFS over
// FULLs), deleting every other non-extendable chain whole. A chain in either
// keep-set is preserved entirely (INCR + WAL included).
func (c *PhysicalBackupCleaner) cleanByCombined(
	ctx context.Context,
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) error {
	candidates, err := c.nonExtendableChainsNewestEndFirst(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	keepRoots := make(map[uuid.UUID]bool)

	for i, candidate := range candidates {
		if i < backupConfig.ChainsRetention.Count {
			keepRoots[candidate.view.RootFull.ID] = true
		}
	}

	completedFulls, err := c.fullRepo.FindCompletedNewestFirstByDatabase(backupConfig.DatabaseID)
	if err != nil {
		return err
	}

	for fullID := range c.fullBackupsKeepSet(backupConfig, completedFulls) {
		keepRoots[fullID] = true
	}

	for _, candidate := range candidates {
		rootFullBackupID := candidate.view.RootFull.ID

		if keepRoots[rootFullBackupID] {
			continue
		}

		if c.isChainWithinGrace(backupConfig, rootFullBackupID) {
			continue
		}

		c.deleteChainThroughService(ctx, logger, backupConfig, rootFullBackupID)
	}

	return nil
}

// cleanByBillingStorageCap is the cloud-only third pass: it deletes oldest
// non-extendable chains until each DB's usage is under its subscription storage
// cap. It runs after retention so a billing cap overrides user retention
// settings, and never evicts the active chain.
func (c *PhysicalBackupCleaner) cleanByBillingStorageCap(ctx context.Context, logger *slog.Logger) error {
	if !config.GetEnv().IsCloud {
		return nil
	}

	enabledConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledConfigs {
		dbLog := logger.With("database_id", backupConfig.DatabaseID)

		c.enforceStorageCapForDatabase(ctx, dbLog, backupConfig)
	}

	return nil
}

func (c *PhysicalBackupCleaner) enforceStorageCapForDatabase(
	ctx context.Context,
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) {
	subscription, err := c.billingService.GetSubscription(logger, backupConfig.DatabaseID)
	if err != nil {
		logger.Error("failed to get subscription for exceeded backups check", "error", err)

		return
	}

	limitMB := float64(subscription.GetBackupsStorageGB()) * 1024

	for {
		usageMB, err := c.physicalBackupService.GetTotalUsageMBByDatabase(backupConfig.DatabaseID)
		if err != nil {
			logger.Error("failed to compute total usage", "error", err)

			return
		}

		if usageMB <= limitMB {
			return
		}

		candidates, err := c.nonExtendableChainsNewestEndFirst(backupConfig.DatabaseID)
		if err != nil {
			logger.Error("failed to list chains for billing cap", "error", err)

			return
		}

		target := c.oldestDeletableChain(backupConfig, candidates)
		if target == nil {
			logger.Warn(fmt.Sprintf(
				"no deletable non-extendable chain but still over limit: usage %.1f MB, limit %.1f MB",
				usageMB, limitMB,
			))

			return
		}

		summary, err := c.physicalBackupService.DeleteFull(
			ctx, target.view.RootFull.ID, c.walDeleteBudgetMB(backupConfig.DatabaseID),
		)
		if err != nil {
			logger.Error("failed to delete chain for billing cap",
				"root_full_backup_id", target.view.RootFull.ID, "error", err)

			return
		}

		logger.Info(fmt.Sprintf(
			"billing cap: trimmed chain (usage %.1f MB > limit %.1f MB)", usageMB, limitMB,
		), "root_full_backup_id", target.view.RootFull.ID)

		if !summary.ChainFullyDeleted {
			// Budget capped this chain mid-delete; resume next tick rather than
			// spinning on a chain whose usage hasn't dropped below the cap yet.
			return
		}
	}
}

// oldestDeletableChain returns the oldest non-extendable chain not protected by
// the per-chain grace period, or nil if none can be evicted.
func (c *PhysicalBackupCleaner) oldestDeletableChain(
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	candidatesNewestFirst []chainCandidate,
) *chainCandidate {
	for i := len(candidatesNewestFirst) - 1; i >= 0; i-- {
		candidate := candidatesNewestFirst[i]

		if c.isChainWithinGrace(backupConfig, candidate.view.RootFull.ID) {
			continue
		}

		return &candidate
	}

	return nil
}

// cleanOrphans removes WAL that no longer belongs to any chain and reaps
// abandoned insert-first claims, per enabled database.
func (c *PhysicalBackupCleaner) cleanOrphans(ctx context.Context, logger *slog.Logger) error {
	enabledConfigs, err := c.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	for _, backupConfig := range enabledConfigs {
		dbLog := logger.With("database_id", backupConfig.DatabaseID)

		c.cleanOrphanWalForDatabase(ctx, dbLog, backupConfig.DatabaseID)
		c.reapAbandonedWalClaims(dbLog, backupConfig.DatabaseID)
	}

	return nil
}

func (c *PhysicalBackupCleaner) cleanOrphanWalForDatabase(
	ctx context.Context,
	logger *slog.Logger,
	databaseID uuid.UUID,
) {
	orphans, err := c.chainViewService.FindWalOrphansByDatabase(databaseID)
	if err != nil {
		logger.Error("failed to find orphan wal", "error", err)

		return
	}

	budget := c.walDeleteBudgetMB(databaseID)

	for _, orphan := range orphans {
		segment := orphan.WalSegment

		// A one-segment span [start, end) uniquely matches this orphan — segments
		// never overlap, so no chain-covered WAL is ever caught.
		span := chain_view.LSNRange{Start: segment.StartLSN, End: segment.EndLSN}

		rows, mb, err := c.physicalBackupService.DeleteWalSegmentsInSpan(
			ctx, databaseID, segment.TimelineID, span, budget,
		)
		if err != nil {
			logger.Error("failed to delete orphan wal segment", "wal_filename", segment.WalFilename, "error", err)

			continue
		}

		if rows > 0 {
			logger.Info(fmt.Sprintf("deleted orphan wal segment (%.2f MB)", mb), "wal_filename", segment.WalFilename)
		}
	}
}

func (c *PhysicalBackupCleaner) reapAbandonedWalClaims(logger *slog.Logger, databaseID uuid.UUID) {
	cutoff := time.Now().UTC().Add(-walClaimGracePeriod)

	deleted, err := c.walSegmentRepo.DeleteAbandonedClaims(databaseID, cutoff)
	if err != nil {
		logger.Error("failed to reap abandoned wal claims", "error", err)

		return
	}

	if deleted > 0 {
		logger.Info(fmt.Sprintf("reaped %d abandoned wal claims", deleted))
	}
}

// approxIntervalDuration maps a schedule to an approximate period for grace
// math. Cron and unset schedules are treated as daily.
func approxIntervalDuration(interval intervals.Interval) time.Duration {
	switch interval.Type {
	case intervals.IntervalHourly:
		return time.Hour
	case intervals.IntervalDaily:
		return 24 * time.Hour
	case intervals.IntervalWeekly:
		return 7 * 24 * time.Hour
	case intervals.IntervalMonthly:
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}
