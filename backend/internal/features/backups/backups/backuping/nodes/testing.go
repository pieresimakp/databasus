package nodes

import "github.com/google/uuid"

// SeedAssignmentForTest registers a node→backup relation directly, bypassing the
// registry round-trip. Test-only seam for the scheduler packages' dead-node and
// completion unit tests, which need a known in-memory relation without standing
// up a real backuper.
func (c *NodeAssignmentCoordinator) SeedAssignmentForTest(nodeID, backupID uuid.UUID) {
	c.trackAssignment(nodeID, backupID)
}

// IsNodeTrackedForTest reports whether a node currently has any tracked backups.
func (c *NodeAssignmentCoordinator) IsNodeTrackedForTest(nodeID uuid.UUID) bool {
	c.relationsMtx.Lock()
	defer c.relationsMtx.Unlock()

	_, exists := c.relations[nodeID]

	return exists
}
