package nodes

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
)

// faultInjectingRegistry wraps a real registry and forces a chosen hand-off step
// to fail, so Assign's rollback branches can be exercised. Everything not
// overridden delegates to the embedded registry.
type faultInjectingRegistry struct {
	*BackupNodesRegistry
	failIncrement bool
	failAssign    bool
}

func (r *faultInjectingRegistry) IncrementBackupsInProgress(nodeID uuid.UUID) error {
	if r.failIncrement {
		return fmt.Errorf("injected increment failure")
	}

	return r.BackupNodesRegistry.IncrementBackupsInProgress(nodeID)
}

func (r *faultInjectingRegistry) AssignBackupToNode(nodeID, backupID uuid.UUID, isCallNotifier bool) error {
	if r.failAssign {
		return fmt.Errorf("injected publish failure")
	}

	return r.BackupNodesRegistry.AssignBackupToNode(nodeID, backupID, isCallNotifier)
}

func newTestCoordinator(registry *BackupNodesRegistry) *NodeAssignmentCoordinator {
	return NewNodeAssignmentCoordinator(registry, logger.GetLogger())
}

// registerNodeWithLoad heartbeats a node and drives its active-backups counter to
// activeBackups so PickLeastBusyNode sees a deterministic score.
func registerNodeWithLoad(
	t *testing.T,
	registry *BackupNodesRegistry,
	throughputMBs, activeBackups int,
) BackupNode {
	t.Helper()

	node := BackupNode{ID: uuid.New(), ThroughputMBs: throughputMBs, LastHeartbeat: time.Now().UTC()}
	require.NoError(t, registry.HearthbeatNodeInRegistry(time.Now().UTC(), node))
	t.Cleanup(func() { _ = registry.UnregisterNodeFromRegistry(node) })

	for range activeBackups {
		require.NoError(t, registry.IncrementBackupsInProgress(node.ID))
	}

	return node
}

func activeBackupsForNode(t *testing.T, registry *BackupNodesRegistry, nodeID uuid.UUID) int {
	t.Helper()

	stats, err := registry.GetBackupNodesStats()
	require.NoError(t, err)

	for _, stat := range stats {
		if stat.ID == nodeID {
			return stat.ActiveBackups
		}
	}

	return 0
}

func Test_PickLeastBusyNode_WhenSameThroughput_SelectsFewestActiveBackups(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	busy := registerNodeWithLoad(t, registry, 100, 3)
	idle := registerNodeWithLoad(t, registry, 100, 0)

	picked, err := newTestCoordinator(registry).PickLeastBusyNode()

	require.NoError(t, err)
	assert.Equal(t, idle.ID, picked, "with equal throughput the node with fewer active backups wins")
	assert.NotEqual(t, busy.ID, picked)
}

func Test_PickLeastBusyNode_WhenEqualLoad_PrefersHigherThroughput(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	// Both carry one active backup; score = active/throughput, so the faster node
	// (0.01) beats the slower one (0.1).
	fast := registerNodeWithLoad(t, registry, 100, 1)
	registerNodeWithLoad(t, registry, 10, 1)

	picked, err := newTestCoordinator(registry).PickLeastBusyNode()

	require.NoError(t, err)
	assert.Equal(t, fast.ID, picked, "for equal load the higher-throughput node wins")
}

func Test_PickLeastBusyNode_WhenNoNodes_ReturnsError(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	_, err := newTestCoordinator(registry).PickLeastBusyNode()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no nodes available")
}

func Test_PickLeastBusyNode_WhenZeroThroughputAndBusy_IsPenalizedBelowLoadedNormalNode(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	// A zero-throughput node with work scores active*1000 (1000), so even a heavily
	// loaded real node (50/100 = 0.5) is preferred.
	registerNodeWithLoad(t, registry, 0, 1)
	normal := registerNodeWithLoad(t, registry, 100, 50)

	picked, err := newTestCoordinator(registry).PickLeastBusyNode()

	require.NoError(t, err)
	assert.Equal(t, normal.ID, picked, "a busy zero-throughput node is deprioritized")
}

func Test_Assign_WhenSucceeds_TracksRelationAndIncrementsCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	node := registerNodeWithLoad(t, registry, 100, 0)
	backupID := uuid.New()

	require.NoError(t, coordinator.Assign(node.ID, backupID, true))

	assert.True(t, coordinator.IsNodeTrackedForTest(node.ID), "assign must track the relation")
	assert.Equal(t, 1, activeBackupsForNode(t, registry, node.ID), "assign increments the in-flight counter")
}

func Test_Assign_WhenIncrementFails_RollsBackTracking(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	faultRegistry := &faultInjectingRegistry{BackupNodesRegistry: registry, failIncrement: true}
	coordinator := NewNodeAssignmentCoordinator(faultRegistry, logger.GetLogger())

	node := registerNodeWithLoad(t, registry, 100, 0)
	backupID := uuid.New()

	err := coordinator.Assign(node.ID, backupID, true)

	require.Error(t, err)
	assert.False(
		t,
		coordinator.IsNodeTrackedForTest(node.ID),
		"a failed increment must undo the track-before-publish step",
	)
	assert.Equal(t, 0, activeBackupsForNode(t, registry, node.ID), "a failed increment leaves the counter untouched")
}

func Test_Assign_WhenPublishFails_DecrementsAndUntracks(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	faultRegistry := &faultInjectingRegistry{BackupNodesRegistry: registry, failAssign: true}
	coordinator := NewNodeAssignmentCoordinator(faultRegistry, logger.GetLogger())

	node := registerNodeWithLoad(t, registry, 100, 0)
	backupID := uuid.New()

	err := coordinator.Assign(node.ID, backupID, true)

	require.Error(t, err)
	assert.False(t, coordinator.IsNodeTrackedForTest(node.ID), "a failed publish must untrack the relation")
	assert.Equal(t, 0, activeBackupsForNode(t, registry, node.ID),
		"a failed publish must decrement the counter it just incremented")
}

func Test_Release_WhenBackupTracked_UntracksAndDecrementsCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	node := registerNodeWithLoad(t, registry, 100, 0)
	backupID := uuid.New()
	require.NoError(t, coordinator.Assign(node.ID, backupID, true))

	coordinator.Release(node.ID, backupID)

	assert.False(t, coordinator.IsNodeTrackedForTest(node.ID), "the node's last backup completing drops its entry")
	assert.Equal(t, 0, activeBackupsForNode(t, registry, node.ID), "release decrements the in-flight counter")
}

func Test_Release_WhenNodeUnknown_DoesNotPanic(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	assert.NotPanics(t, func() {
		coordinator.Release(uuid.New(), uuid.New())
	})
}

func Test_Release_WhenBackupNotTrackedForNode_LeavesOtherBackupAndCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	node := registerNodeWithLoad(t, registry, 100, 0)
	trackedBackupID := uuid.New()
	require.NoError(t, coordinator.Assign(node.ID, trackedBackupID, true))

	coordinator.Release(node.ID, uuid.New()) // a backup this node never owned

	assert.True(t, coordinator.IsNodeTrackedForTest(node.ID), "an untracked backup must not drop the node")
	assert.Equal(t, 1, activeBackupsForNode(t, registry, node.ID), "an untracked backup must not decrement the counter")
}

func Test_HandleDeadNodes_WhenNodeAlive_DoesNotFailItsBackups(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	aliveNode := registerNodeWithLoad(t, registry, 100, 1)
	coordinator.SeedAssignmentForTest(aliveNode.ID, uuid.New())

	failCalled := false
	require.NoError(t, coordinator.HandleDeadNodes(func(_, _ uuid.UUID) bool {
		failCalled = true

		return true
	}))

	assert.False(t, failCalled, "a live node's backups must never be failed")
	assert.True(t, coordinator.IsNodeTrackedForTest(aliveNode.ID))
}

func Test_HandleDeadNodes_WhenNodeDead_FailsItsBackupsAndDropsNode(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	deadNodeID := uuid.New() // never heartbeated → absent from GetAvailableNodes
	backupID := uuid.New()
	coordinator.SeedAssignmentForTest(deadNodeID, backupID)

	var failedBackupIDs []uuid.UUID
	require.NoError(t, coordinator.HandleDeadNodes(func(_, completedBackupID uuid.UUID) bool {
		failedBackupIDs = append(failedBackupIDs, completedBackupID)

		return true
	}))

	assert.Equal(t, []uuid.UUID{backupID}, failedBackupIDs, "every backup owned by a dead node is failed")
	assert.False(t, coordinator.IsNodeTrackedForTest(deadNodeID), "a dead node's relation is dropped after handling")
}

func Test_HandleDeadNodes_WhenFailBackupReturnsFalse_StillDropsNode(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	coordinator := newTestCoordinator(registry)

	deadNodeID := uuid.New()
	coordinator.SeedAssignmentForTest(deadNodeID, uuid.New())

	require.NoError(t, coordinator.HandleDeadNodes(func(_, _ uuid.UUID) bool {
		return false // persistence failed; coordinator must not decrement but still drops the node
	}))

	assert.False(
		t,
		coordinator.IsNodeTrackedForTest(deadNodeID),
		"a flapping dead node is dropped even when failBackup fails",
	)
}
