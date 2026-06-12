package backuping_physical

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	"databasus-backend/internal/features/backups/backups/core/physical/chain_view"
	physical_enums "databasus-backend/internal/features/backups/backups/core/physical/enums"
	physical_models "databasus-backend/internal/features/backups/backups/core/physical/models"
	physical_repositories "databasus-backend/internal/features/backups/backups/core/physical/repositories"
	backups_config_physical "databasus-backend/internal/features/backups/config/physical"
	postgresql_physical "databasus-backend/internal/features/databases/databases/postgresql/physical"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	"databasus-backend/internal/storage"
)

// PhysicalBackupsScheduler drives the FULL/INCR decision, the cross-table
// single-in-flight claim + typed-row INSERT, and the hand-off to the existing
// PhysicalBackuperNode via the (physical-namespaced) registry. It is the
// "front half"; the backuper writes terminal status and releases the claim.
//
// Retry policy (no IsRetryIfFailed/MaxFailedTriesCount columns exist on the
// physical config, so retry is derived from chain state, not a counter):
//   - ERROR: the chain stays extendable, so the next cadence-due tick re-attempts
//     the SAME kind. A freshly-failed attempt is the newest row of its kind, so
//     the cadence check on its created_at prevents a tight retry loop.
//   - CHAIN_BROKEN: no extendable chain remains, so the next tick opens a new FULL.
//   - CANCELED: neither COMPLETED nor CHAIN_BROKEN; the chain it belonged to stays
//     extendable and resumes on cadence — no immediate auto-retry.
type PhysicalBackupsScheduler struct {
	fullRepo              *physical_repositories.PhysicalFullBackupRepository
	incrRepo              *physical_repositories.PhysicalIncrementalBackupRepository
	inFlightRepo          *physical_repositories.PhysicalInFlightBackupRepository
	backupConfigService   *backups_config_physical.BackupConfigService
	chainViewService      *chain_view.ChainViewService
	taskCancelManager     *tasks_cancellation.TaskCancelManager
	assignmentCoordinator *nodes.NodeAssignmentCoordinator
	billingService        BillingService

	lastTickTime atomicTime
	logger       *slog.Logger

	hasRun  atomic.Bool
	isReady atomic.Bool
}

// IsRunning reports whether the scheduler has subscribed to completions and is
// ready to receive them. Tests gate on it.
func (s *PhysicalBackupsScheduler) IsRunning() bool {
	return s.isReady.Load()
}

func (s *PhysicalBackupsScheduler) IsSchedulerRunning() bool {
	return s.lastTickTime.Load().After(time.Now().UTC().Add(-schedulerHealthcheckThreshold))
}

func (s *PhysicalBackupsScheduler) Run(ctx context.Context) {
	if s.hasRun.Swap(true) {
		panic(fmt.Sprintf("%T.Run() called multiple times", s))
	}

	s.logger = s.logger.With("job_id", uuid.New(), "job_name", schedulerJobName)

	s.lastTickTime.Store(time.Now().UTC())

	if config.GetEnv().IsManyNodesMode {
		time.Sleep(schedulerStartupDelay)
	}

	if err := s.recoverInFlightBackupsOnRestart(); err != nil {
		s.logger.Error("failed to recover in-flight physical backups on restart", "error", err)

		panic(err)
	}

	if err := s.assignmentCoordinator.SubscribeForBackupsCompletions(s.onBackupCompleted); err != nil {
		s.logger.Error("failed to subscribe physical scheduler to completions", "error", err)

		panic(err)
	}

	s.isReady.Store(true)

	defer func() {
		s.isReady.Store(false)

		if err := s.assignmentCoordinator.UnsubscribeForBackupsCompletions(); err != nil {
			s.logger.Error("failed to unsubscribe physical scheduler from completions", "error", err)
		}
	}()

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(schedulerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.checkDeadNodesAndFailBackups(); err != nil {
				s.logger.Error("failed to check dead nodes", "error", err)
			}

			if err := s.runPendingBackups(); err != nil {
				s.logger.Error("failed to run pending physical backups", "error", err)
			}

			s.lastTickTime.Store(time.Now().UTC())
		}
	}
}

func (s *PhysicalBackupsScheduler) runPendingBackups() error {
	enabledConfigs, err := s.backupConfigService.GetBackupConfigsWithEnabledBackups()
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	for _, backupConfig := range enabledConfigs {
		s.evaluateConfig(now, backupConfig)
	}

	return nil
}

func (s *PhysicalBackupsScheduler) evaluateConfig(
	now time.Time,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) {
	logger := s.logger.With("database_id", backupConfig.DatabaseID, "job_name", schedulerJobName)

	if backupConfig.StorageID == nil {
		logger.Error("physical backup config has no storage id; skipping")

		return
	}

	if config.GetEnv().IsCloud && !s.canCreateBackups(logger, backupConfig.DatabaseID) {
		return
	}

	decision, ok := s.decideBackupKind(logger, now, backupConfig)
	if !ok {
		return
	}

	s.scheduleBackup(logger, backupConfig, decision)
}

func (s *PhysicalBackupsScheduler) canCreateBackups(logger *slog.Logger, databaseID uuid.UUID) bool {
	subscription, err := s.billingService.GetSubscription(logger, databaseID)
	if err != nil {
		logger.Warn("failed to get subscription, skipping tick", "error", err)

		return false
	}

	if !subscription.CanCreateNewBackups() {
		logger.Debug("subscription not active, skipping scheduled backup",
			"subscription_status", subscription.Status)

		return false
	}

	return true
}

// backupDecision is the outcome of the per-tick FULL-vs-INCR decision.
type backupDecision struct {
	kind                        physical_enums.PhysicalBackupType
	incrRootFullBackupID        uuid.UUID
	incrParentIncrID            *uuid.UUID
	forceFullRequestedAt        *time.Time
	forceIncrementalRequestedAt *time.Time
}

// decideBackupKind picks FULL or INCR purely from catalog state + cadence (no
// source-PG connection): FULL when its interval is due (covers bootstrap, chain
// rotation, and post-CHAIN_BROKEN re-anchor since no extendable chain exists);
// INCR when incrementals are enabled, an extendable chain exists, and the INCR
// interval is due. The shipped executors handle summarizer/timeline reality at
// run time, returning CHAIN_BROKEN when an INCR cannot actually proceed.
func (s *PhysicalBackupsScheduler) decideBackupKind(
	logger *slog.Logger,
	now time.Time,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) (backupDecision, bool) {
	lastFull, err := s.fullRepo.FindLastFullAnyStatusByDatabase(backupConfig.DatabaseID)
	if err != nil {
		logger.Error("failed to find last full backup", "error", err)

		return backupDecision{}, false
	}

	if backupConfig.ForceFullRequestedAt != nil {
		return backupDecision{
			kind:                 physical_enums.PhysicalBackupTypeFull,
			forceFullRequestedAt: backupConfig.ForceFullRequestedAt,
		}, true
	}

	if backupConfig.ForceIncrementalRequestedAt != nil {
		if decision, ok := s.decideForcedIncremental(logger, backupConfig); ok {
			return decision, true
		}
	}

	lastIncr, err := s.incrRepo.FindLastByDatabase(backupConfig.DatabaseID)
	if err != nil {
		logger.Error("failed to find last incremental backup", "error", err)

		return backupDecision{}, false
	}

	if backupConfig.FullBackupInterval.ShouldTriggerBackup(now, createdAtOrNil(lastFull)) {
		return backupDecision{kind: physical_enums.PhysicalBackupTypeFull}, true
	}

	if !isIncrementalEnabled(backupConfig) {
		return backupDecision{}, false
	}

	extendableChain, err := s.chainViewService.FindLastExtendableChainByDatabase(backupConfig.DatabaseID)
	if err != nil {
		logger.Error("failed to find extendable chain", "error", err)

		return backupDecision{}, false
	}

	lastBackupTime := newestCreatedAt(lastFull, lastIncr)

	if extendableChain == nil ||
		!backupConfig.IncrementalBackupInterval.ShouldTriggerBackup(now, lastBackupTime) {
		return backupDecision{}, false
	}

	parentIncrID, err := s.resolveIncrParent(extendableChain.RootFull.ID)
	if err != nil {
		logger.Error("failed to resolve incremental parent", "error", err)

		return backupDecision{}, false
	}

	return backupDecision{
		kind:                 physical_enums.PhysicalBackupTypeIncremental,
		incrRootFullBackupID: extendableChain.RootFull.ID,
		incrParentIncrID:     parentIncrID,
	}, true
}

// decideForcedIncremental honors an out-of-cadence incremental request. It can
// only be satisfied when incrementals are enabled and an extendable chain
// exists; otherwise the stale request is cleared (so it can't loop forever) and
// ok=false lets the caller fall through to normal cadence evaluation. The
// controller already rejects "incremental with no chain" up front, but the
// chain can disappear between request and tick, so the scheduler must cope.
func (s *PhysicalBackupsScheduler) decideForcedIncremental(
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) (backupDecision, bool) {
	if !isIncrementalEnabled(backupConfig) {
		s.clearIncrementalRequest(logger, backupConfig)

		return backupDecision{}, false
	}

	extendableChain, err := s.chainViewService.FindLastExtendableChainByDatabase(backupConfig.DatabaseID)
	if err != nil {
		logger.Error("failed to find extendable chain for forced incremental", "error", err)

		return backupDecision{}, false
	}

	if extendableChain == nil {
		logger.Warn("forced incremental requested but no extendable chain exists; clearing request")
		s.clearIncrementalRequest(logger, backupConfig)

		return backupDecision{}, false
	}

	parentIncrID, err := s.resolveIncrParent(extendableChain.RootFull.ID)
	if err != nil {
		logger.Error("failed to resolve incremental parent for forced incremental", "error", err)

		return backupDecision{}, false
	}

	return backupDecision{
		kind:                        physical_enums.PhysicalBackupTypeIncremental,
		incrRootFullBackupID:        extendableChain.RootFull.ID,
		incrParentIncrID:            parentIncrID,
		forceIncrementalRequestedAt: backupConfig.ForceIncrementalRequestedAt,
	}, true
}

func (s *PhysicalBackupsScheduler) clearIncrementalRequest(
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
) {
	if err := s.backupConfigService.ClearIncrementalBackupRequest(
		backupConfig.DatabaseID, backupConfig.ForceIncrementalRequestedAt,
	); err != nil {
		logger.Error("failed to clear incremental backup request", "error", err)
	}
}

// resolveIncrParent returns the latest COMPLETED INCR in the chain as the new
// INCR's parent, or nil when the chain has none yet (then the parent is the
// FULL, resolved from root_full_backup_id at read time).
func (s *PhysicalBackupsScheduler) resolveIncrParent(rootFullBackupID uuid.UUID) (*uuid.UUID, error) {
	parent, err := s.incrRepo.FindLatestCompletedByRootFull(rootFullBackupID)
	if err != nil {
		return nil, err
	}

	if parent == nil {
		return nil, nil
	}

	return &parent.ID, nil
}

func (s *PhysicalBackupsScheduler) scheduleBackup(
	logger *slog.Logger,
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	decision backupDecision,
) {
	backupID := uuid.New()
	logger = logger.With("backup_id", backupID, "backup_kind", decision.kind)

	// Pick the node BEFORE claiming so a "no nodes available" tick leaves no row
	// or claim behind, and so the claim can record its owner — restart recovery
	// reads that owner to tell a live backup from an orphaned one.
	nodeID, err := s.assignmentCoordinator.PickLeastBusyNode()
	if err != nil {
		logger.Warn("no backup node available; skipping tick", "error", err)

		return
	}

	claimed, err := s.claimAndInsert(backupConfig, backupID, decision, nodeID)
	if err != nil {
		logger.Error("failed to claim and insert backup row", "error", err)

		return
	}

	if !claimed {
		logger.Debug("in-flight slot already claimed by another instance; skipping")

		return
	}

	if err := s.assignmentCoordinator.Assign(nodeID, backupID, true); err != nil {
		logger.Error("failed to hand off backup to node; failing row", "error", err)

		if failErr := s.failBackupAndReleaseClaim(
			decision.kind, backupID, backupConfig.DatabaseID,
			physical_enums.PhysicalBackupErrorNoNodeAvailable,
		); failErr != nil {
			logger.Error("failed to roll back unassigned backup row", "error", failErr)
		}

		return
	}

	if decision.forceFullRequestedAt != nil {
		if err := s.backupConfigService.ClearFullBackupRequest(
			backupConfig.DatabaseID,
			decision.forceFullRequestedAt,
		); err != nil {
			logger.Error("failed to clear forced full request", "error", err)
		}
	}

	if decision.forceIncrementalRequestedAt != nil {
		if err := s.backupConfigService.ClearIncrementalBackupRequest(
			backupConfig.DatabaseID,
			decision.forceIncrementalRequestedAt,
		); err != nil {
			logger.Error("failed to clear forced incremental request", "error", err)
		}
	}

	logger.Info("scheduled physical backup", "node_id", nodeID)
}

// claimAndInsert reserves the cross-table in-flight slot and inserts the typed
// IN_PROGRESS row in ONE transaction. Both use the tx handle so they commit
// together; a lost claim (another instance won) commits nothing and returns
// claimed=false. The typed INSERT goes through tx.Create, NOT repo.Save (which
// uses the global DB) — that is what makes the claim and the row atomic.
func (s *PhysicalBackupsScheduler) claimAndInsert(
	backupConfig *backups_config_physical.PhysicalBackupConfig,
	backupID uuid.UUID,
	decision backupDecision,
	nodeID uuid.UUID,
) (bool, error) {
	now := time.Now().UTC()
	claimed := false

	txErr := storage.GetDb().Transaction(func(tx *gorm.DB) error {
		ok, claimErr := s.inFlightRepo.Claim(tx, physical_repositories.ClaimSpec{
			DatabaseID: backupConfig.DatabaseID,
			BackupType: decision.kind,
			BackupID:   backupID,
			NodeID:     nodeID,
		})
		if claimErr != nil {
			return claimErr
		}

		if !ok {
			return nil
		}

		claimed = true

		if decision.kind == physical_enums.PhysicalBackupTypeFull {
			return tx.Create(&physical_models.PhysicalFullBackup{
				ID:         backupID,
				DatabaseID: backupConfig.DatabaseID,
				StorageID:  *backupConfig.StorageID,
				Status:     physical_enums.PhysicalBackupStatusInProgress,
				CreatedAt:  now,
			}).Error
		}

		return tx.Create(&physical_models.PhysicalIncrementalBackup{
			ID:                        backupID,
			DatabaseID:                backupConfig.DatabaseID,
			StorageID:                 *backupConfig.StorageID,
			RootFullBackupID:          decision.incrRootFullBackupID,
			ParentIncrementalBackupID: decision.incrParentIncrID,
			Status:                    physical_enums.PhysicalBackupStatusInProgress,
			CreatedAt:                 now,
		}).Error
	})
	if txErr != nil {
		return false, txErr
	}

	return claimed, nil
}

func (s *PhysicalBackupsScheduler) onBackupCompleted(nodeID, backupID uuid.UUID) {
	if !s.isPhysicalBackup(backupID) {
		return
	}

	s.assignmentCoordinator.Release(nodeID, backupID)
}

// isPhysicalBackup guards onBackupCompleted: the shared registry carries every
// pool's completions, so ignore IDs that are not in a physical typed table.
func (s *PhysicalBackupsScheduler) isPhysicalBackup(backupID uuid.UUID) bool {
	full, err := s.fullRepo.FindByID(backupID)
	if err == nil && full != nil {
		return true
	}

	incr, err := s.incrRepo.FindByID(backupID)

	return err == nil && incr != nil
}

// recoverInFlightBackupsOnRestart reconciles the IN_PROGRESS backups the DB still
// holds when this scheduler (re)started. The in-memory owner map is empty after a
// restart, so ownership is read back from each in-flight claim's node_id:
//   - owner still alive in the registry -> rebuild the coordinator relation and
//     leave the backup running; its completion arrives once we re-subscribe. This
//     is what lets a healthy backup survive a primary restart instead of being
//     failed and redone.
//   - owner gone (dead node, or never recorded) -> fail the row and release the
//     claim, freeing the database's single-in-flight slot for a fresh attempt.
//
// Runs once at Run() entry, after the many-nodes startup delay so live nodes have
// re-registered (the primary wipes the registry cache on boot). The claim is the
// source of truth for in-flight state — claimAndInsert writes the claim and the
// typed row in one transaction — so iterating claims covers every live backup.
func (s *PhysicalBackupsScheduler) recoverInFlightBackupsOnRestart() error {
	claims, err := s.inFlightRepo.FindAll()
	if err != nil {
		return err
	}

	aliveNodeIDs, err := s.assignmentCoordinator.GetAvailableNodeIDs()
	if err != nil {
		return err
	}

	claimedBackupIDs := make(map[uuid.UUID]struct{}, len(claims))

	for _, claim := range claims {
		claimedBackupIDs[claim.BackupID] = struct{}{}

		if claim.NodeID != nil && aliveNodeIDs[*claim.NodeID] {
			s.assignmentCoordinator.RebuildAssignment(*claim.NodeID, claim.BackupID)
			s.logger.Info("kept in-flight backup owned by live node on restart",
				"backup_id", claim.BackupID, "node_id", *claim.NodeID)

			continue
		}

		s.failOrphanedBackup(claim.BackupType, claim.BackupID, claim.DatabaseID)
	}

	return s.failClaimlessInProgressBackups(claimedBackupIDs)
}

// failClaimlessInProgressBackups fails any IN_PROGRESS row that has no matching
// in-flight claim. The atomic claim+insert should never leave such a row, so this
// is a defensive sweep: without it a stray row could sit IN_PROGRESS forever after
// a restart, since claim-driven recovery would never see it.
func (s *PhysicalBackupsScheduler) failClaimlessInProgressBackups(claimedBackupIDs map[uuid.UUID]struct{}) error {
	fulls, err := s.fullRepo.FindAllInProgress()
	if err != nil {
		return err
	}

	for _, full := range fulls {
		if _, ok := claimedBackupIDs[full.ID]; ok {
			continue
		}

		s.failOrphanedBackup(physical_enums.PhysicalBackupTypeFull, full.ID, full.DatabaseID)
	}

	incrementals, err := s.incrRepo.FindAllInProgress()
	if err != nil {
		return err
	}

	for _, incremental := range incrementals {
		if _, ok := claimedBackupIDs[incremental.ID]; ok {
			continue
		}

		s.failOrphanedBackup(physical_enums.PhysicalBackupTypeIncremental, incremental.ID, incremental.DatabaseID)
	}

	return nil
}

func (s *PhysicalBackupsScheduler) failOrphanedBackup(
	kind physical_enums.PhysicalBackupType,
	backupID, databaseID uuid.UUID,
) {
	// Best-effort cancel of any locally-registered task; harmless if none
	// (a fresh process holds no registrations).
	if err := s.taskCancelManager.CancelTask(backupID); err != nil {
		s.logger.Error("failed to cancel orphaned backup task", "backup_id", backupID, "error", err)
	}

	if err := s.failBackupAndReleaseClaim(
		kind, backupID, databaseID, physical_enums.PhysicalBackupErrorApplicationRestart,
	); err != nil {
		s.logger.Error("failed to fail orphaned backup on restart", "backup_id", backupID, "error", err)
	}
}

// checkDeadNodesAndFailBackups fails the IN_PROGRESS backups owned by any node
// that has fallen out of the registry, releasing their in-flight claims so the
// DB is not stuck. The coordinator drives the dead-node sweep and the per-node
// counter decrement; failDeadNodeBackup carries the physical-specific persistence.
// Runs every tick while the scheduler is up.
func (s *PhysicalBackupsScheduler) checkDeadNodesAndFailBackups() error {
	return s.assignmentCoordinator.HandleDeadNodes(s.failDeadNodeBackup)
}

// failDeadNodeBackup flips one backup owned by a dead node to ERROR and releases
// its claim. Returns true when the row was failed so the coordinator decrements
// the node's in-flight counter; a lookup/persistence error returns false to skip
// the decrement.
func (s *PhysicalBackupsScheduler) failDeadNodeBackup(_, backupID uuid.UUID) bool {
	kind, databaseID, ok := s.lookupBackupKindAndDatabase(backupID)
	if !ok {
		return false
	}

	if err := s.failBackupAndReleaseClaim(
		kind, backupID, databaseID, physical_enums.PhysicalBackupErrorNetworkFailure,
	); err != nil {
		s.logger.Error("failed to fail dead-node backup", "backup_id", backupID, "error", err)

		return false
	}

	return true
}

func (s *PhysicalBackupsScheduler) lookupBackupKindAndDatabase(
	backupID uuid.UUID,
) (physical_enums.PhysicalBackupType, uuid.UUID, bool) {
	full, err := s.fullRepo.FindByID(backupID)
	if err != nil {
		s.logger.Error("failed to look up full for dead-node fail", "backup_id", backupID, "error", err)

		return "", uuid.Nil, false
	}
	if full != nil {
		return physical_enums.PhysicalBackupTypeFull, full.DatabaseID, true
	}

	incremental, err := s.incrRepo.FindByID(backupID)
	if err != nil {
		s.logger.Error("failed to look up incr for dead-node fail", "backup_id", backupID, "error", err)

		return "", uuid.Nil, false
	}
	if incremental != nil {
		return physical_enums.PhysicalBackupTypeIncremental, incremental.DatabaseID, true
	}

	return "", uuid.Nil, false
}

// failBackupAndReleaseClaim flips one typed row to ERROR and deletes the
// matching in-flight claim in the SAME transaction. Pairing the two is
// essential: an orphan claim left behind would block every future tick from
// acquiring the cross-table single-in-flight slot for that DB, freezing it.
//
// It deliberately does NOT touch storage. A row reaches this path only while
// IN_PROGRESS, and claimAndInsert inserts FULL/INCR rows with file_name = NULL —
// a name is written only at COMPLETED. A NULL file_name is proof no object was
// ever uploaded under any name, so there is nothing to delete (the same reasoning
// as PhysicalWalSegmentRepository.DeleteAbandonedClaims). This MUST be revisited
// if physical FULL ever starts writing file_name at upload start: at that point a
// rolled-back row could reference a partial object that needs storage cleanup
// before the status flip.
func (s *PhysicalBackupsScheduler) failBackupAndReleaseClaim(
	kind physical_enums.PhysicalBackupType,
	backupID, databaseID uuid.UUID,
	reason physical_enums.PhysicalBackupErrorReason,
) error {
	var typedModel any = &physical_models.PhysicalFullBackup{}
	if kind == physical_enums.PhysicalBackupTypeIncremental {
		typedModel = &physical_models.PhysicalIncrementalBackup{}
	}

	return storage.GetDb().Transaction(func(tx *gorm.DB) error {
		// Guard on IN_PROGRESS so a late completion that already wrote a terminal
		// status wins the race — the backuper's persist and this sweep both
		// conditionally transition the row, so at most one of them takes effect.
		if err := tx.Model(typedModel).
			Where("id = ? AND status = ?", backupID, physical_enums.PhysicalBackupStatusInProgress).
			Updates(map[string]any{
				"status":       physical_enums.PhysicalBackupStatusError,
				"error_reason": reason,
			}).Error; err != nil {
			return err
		}

		// Scope the claim delete to backup_id so failing a stale backup cannot
		// remove a newer backup's claim for the same database.
		return tx.Delete(
			&physical_models.PhysicalInFlightBackup{},
			"database_id = ? AND backup_id = ?",
			databaseID,
			backupID,
		).Error
	})
}

func createdAtOrNil(full *physical_models.PhysicalFullBackup) *time.Time {
	if full == nil {
		return nil
	}

	return &full.CreatedAt
}

func newestCreatedAt(
	full *physical_models.PhysicalFullBackup,
	incr *physical_models.PhysicalIncrementalBackup,
) *time.Time {
	switch {
	case full == nil && incr == nil:
		return nil
	case incr == nil:
		return &full.CreatedAt
	case full == nil:
		return &incr.CreatedAt
	case incr.CreatedAt.After(full.CreatedAt):
		return &incr.CreatedAt
	default:
		return &full.CreatedAt
	}
}

func isIncrementalEnabled(backupConfig *backups_config_physical.PhysicalBackupConfig) bool {
	if backupConfig.PostgresqlPhysical == nil {
		return false
	}

	backupType := backupConfig.PostgresqlPhysical.BackupType

	return backupType == postgresql_physical.BackupTypeFullAndIncremental ||
		backupType == postgresql_physical.BackupTypeFullIncrementalAndWalStream
}
