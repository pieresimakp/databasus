package nodes

import (
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"github.com/google/uuid"
)

// NodeAssignmentCoordinator owns the in-memory backup→node relation map and the
// node-selection / completion bookkeeping
type NodeAssignmentCoordinator struct {
	registry nodeRegistry
	logger   *slog.Logger

	relations    map[uuid.UUID]backupToNodeRelation
	relationsMtx sync.Mutex
}

func NewNodeAssignmentCoordinator(
	registry nodeRegistry,
	logger *slog.Logger,
) *NodeAssignmentCoordinator {
	return &NodeAssignmentCoordinator{
		registry:  registry,
		logger:    logger,
		relations: make(map[uuid.UUID]backupToNodeRelation),
	}
}

// PickLeastBusyNode returns the available node with the lowest
// active-backups-per-throughput score. Callers select the node BEFORE persisting
// a backup row so that a "no nodes available" outcome leaves no orphaned row,
// then hand the returned node to Assign.
func (c *NodeAssignmentCoordinator) PickLeastBusyNode() (uuid.UUID, error) {
	availableNodes, err := c.registry.GetAvailableNodes()
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get available nodes: %w", err)
	}

	if len(availableNodes) == 0 {
		return uuid.Nil, fmt.Errorf("no nodes available")
	}

	stats, err := c.registry.GetBackupNodesStats()
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get backup nodes stats: %w", err)
	}

	activeBackupsByNode := make(map[uuid.UUID]int)
	for _, stat := range stats {
		activeBackupsByNode[stat.ID] = stat.ActiveBackups
	}

	var bestNode *BackupNode
	bestScore := -1.0

	for i := range availableNodes {
		node := &availableNodes[i]
		activeBackups := activeBackupsByNode[node.ID]

		var score float64
		if node.ThroughputMBs > 0 {
			score = float64(activeBackups) / float64(node.ThroughputMBs)
		} else {
			score = float64(activeBackups) * 1000
		}

		if bestNode == nil || score < bestScore {
			bestNode = node
			bestScore = score
		}
	}

	if bestNode == nil {
		return uuid.Nil, fmt.Errorf("no suitable nodes available")
	}

	return bestNode.ID, nil
}

// Assign registers the backup against the node BEFORE publishing the assignment.
// The backuper can finish a tiny backup and publish its completion before the
// relation entry would otherwise exist; Release would then miss the decrement and
// leave the active counter stuck. After tracking, it increments the node's
// in-flight counter and publishes the assignment, rolling both back on failure.
func (c *NodeAssignmentCoordinator) Assign(nodeID, backupID uuid.UUID, isCallNotifier bool) error {
	c.trackAssignment(nodeID, backupID)

	if err := c.registry.IncrementBackupsInProgress(nodeID); err != nil {
		c.untrackAssignment(nodeID, backupID)

		return fmt.Errorf("failed to increment backups in progress: %w", err)
	}

	if err := c.registry.AssignBackupToNode(nodeID, backupID, isCallNotifier); err != nil {
		if decrementErr := c.registry.DecrementBackupsInProgress(nodeID); decrementErr != nil {
			c.logger.Error("failed to decrement backups in progress after assign failure",
				"node_id", nodeID, "error", decrementErr)
		}

		c.untrackAssignment(nodeID, backupID)

		return fmt.Errorf("failed to assign backup to node: %w", err)
	}

	return nil
}

// Release records a completed backup: it removes the backup from its node's
// relation and decrements the node's in-flight counter — the happy-path
// counterpart to Assign's rollback. Callers must first confirm the completion
// belongs to their pool, since the registry carries every pool's completions.
func (c *NodeAssignmentCoordinator) Release(nodeID, backupID uuid.UUID) {
	c.relationsMtx.Lock()
	relation, exists := c.relations[nodeID]
	if !exists {
		c.relationsMtx.Unlock()
		c.logger.Warn("received completion for unknown node", "node_id", nodeID, "backup_id", backupID)

		return
	}

	remaining := make([]uuid.UUID, 0, len(relation.BackupsIDs))
	found := false
	for _, id := range relation.BackupsIDs {
		if id == backupID {
			found = true

			continue
		}

		remaining = append(remaining, id)
	}

	if !found {
		c.relationsMtx.Unlock()
		c.logger.Warn("completed backup not tracked for node", "node_id", nodeID, "backup_id", backupID)

		return
	}

	if len(remaining) == 0 {
		delete(c.relations, nodeID)
	} else {
		relation.BackupsIDs = remaining
		c.relations[nodeID] = relation
	}
	c.relationsMtx.Unlock()

	if err := c.registry.DecrementBackupsInProgress(nodeID); err != nil {
		c.logger.Error("failed to decrement backups in progress",
			"node_id", nodeID, "backup_id", backupID, "error", err)
	}
}

// HandleDeadNodes fails the backups owned by any node that has dropped out of the
// registry. For each backup it calls failBackup; when failBackup reports success
// it decrements that node's in-flight counter. Each dead node's relation is
// dropped afterward so a flapping node is not re-processed. failBackup carries
// the pool-specific persistence (mark the typed row failed, release claims).
func (c *NodeAssignmentCoordinator) HandleDeadNodes(failBackup func(nodeID, backupID uuid.UUID) bool) error {
	availableNodes, err := c.registry.GetAvailableNodes()
	if err != nil {
		return fmt.Errorf("failed to get available nodes: %w", err)
	}

	aliveNodeIDs := make(map[uuid.UUID]bool, len(availableNodes))
	for _, node := range availableNodes {
		aliveNodeIDs[node.ID] = true
	}

	for nodeID, relation := range c.snapshotRelations() {
		if aliveNodeIDs[nodeID] {
			continue
		}

		c.logger.Warn("backup node is dead, failing its backups",
			"node_id", nodeID, "backup_count", len(relation.BackupsIDs))

		for _, backupID := range relation.BackupsIDs {
			if !failBackup(nodeID, backupID) {
				continue
			}

			if err := c.registry.DecrementBackupsInProgress(nodeID); err != nil {
				c.logger.Error("failed to decrement backups in progress for dead node",
					"node_id", nodeID, "backup_id", backupID, "error", err)
			}
		}

		c.dropNode(nodeID)
	}

	return nil
}

// SubscribeForBackupsCompletions registers handler for this pool's completion
// notifications.
func (c *NodeAssignmentCoordinator) SubscribeForBackupsCompletions(
	handler func(nodeID, backupID uuid.UUID),
) error {
	return c.registry.SubscribeForBackupsCompletions(handler)
}

func (c *NodeAssignmentCoordinator) UnsubscribeForBackupsCompletions() error {
	return c.registry.UnsubscribeForBackupsCompletions()
}

// HasAvailableNodes reports whether the pool currently has at least one live node.
func (c *NodeAssignmentCoordinator) HasAvailableNodes() (bool, error) {
	availableNodes, err := c.registry.GetAvailableNodes()
	if err != nil {
		return false, err
	}

	return len(availableNodes) > 0, nil
}

// GetAvailableNodeIDs returns the set of node IDs currently alive in the
// registry, for callers that need O(1) liveness checks (restart recovery deciding
// whether an in-flight backup's owner is still up).
func (c *NodeAssignmentCoordinator) GetAvailableNodeIDs() (map[uuid.UUID]bool, error) {
	return c.registry.GetAvailableNodeIDs()
}

// RebuildAssignment re-establishes an assignment the coordinator lost when the
// process restarted: it re-tracks the relation and re-increments the node's
// in-flight counter (the primary wipes the registry cache on boot, zeroing it)
// WITHOUT publishing — the backup is already running on the node, so it must not
// be handed out again. Used by restart recovery for backups whose owner is alive.
func (c *NodeAssignmentCoordinator) RebuildAssignment(nodeID, backupID uuid.UUID) {
	c.trackAssignment(nodeID, backupID)

	if err := c.registry.IncrementBackupsInProgress(nodeID); err != nil {
		c.logger.Error("failed to increment backups in progress during rebuild",
			"node_id", nodeID, "backup_id", backupID, "error", err)
	}
}

func (c *NodeAssignmentCoordinator) trackAssignment(nodeID, backupID uuid.UUID) {
	c.relationsMtx.Lock()
	defer c.relationsMtx.Unlock()

	relation := c.relations[nodeID]
	relation.NodeID = nodeID
	relation.BackupsIDs = append(relation.BackupsIDs, backupID)
	c.relations[nodeID] = relation
}

// untrackAssignment removes a single backup from a node's relation, dropping the
// node entry when it becomes empty. Rolls back a trackAssignment when a later
// hand-off step fails.
func (c *NodeAssignmentCoordinator) untrackAssignment(nodeID, backupID uuid.UUID) {
	c.relationsMtx.Lock()
	defer c.relationsMtx.Unlock()

	relation, exists := c.relations[nodeID]
	if !exists {
		return
	}

	remaining := make([]uuid.UUID, 0, len(relation.BackupsIDs))
	for _, id := range relation.BackupsIDs {
		if id != backupID {
			remaining = append(remaining, id)
		}
	}

	if len(remaining) == 0 {
		delete(c.relations, nodeID)

		return
	}

	relation.BackupsIDs = remaining
	c.relations[nodeID] = relation
}

func (c *NodeAssignmentCoordinator) snapshotRelations() map[uuid.UUID]backupToNodeRelation {
	c.relationsMtx.Lock()
	defer c.relationsMtx.Unlock()

	snapshot := make(map[uuid.UUID]backupToNodeRelation, len(c.relations))
	maps.Copy(snapshot, c.relations)

	return snapshot
}

func (c *NodeAssignmentCoordinator) dropNode(nodeID uuid.UUID) {
	c.relationsMtx.Lock()
	defer c.relationsMtx.Unlock()

	delete(c.relations, nodeID)
}
