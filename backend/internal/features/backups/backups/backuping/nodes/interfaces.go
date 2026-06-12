package nodes

import "github.com/google/uuid"

type nodeRegistry interface {
	GetAvailableNodes() ([]BackupNode, error)

	GetAvailableNodeIDs() (map[uuid.UUID]bool, error)

	GetBackupNodesStats() ([]BackupNodeStats, error)

	IncrementBackupsInProgress(nodeID uuid.UUID) error

	DecrementBackupsInProgress(nodeID uuid.UUID) error

	AssignBackupToNode(nodeID, backupID uuid.UUID, isCallNotifier bool) error

	SubscribeForBackupsCompletions(handler func(nodeID, backupID uuid.UUID)) error

	UnsubscribeForBackupsCompletions() error
}
