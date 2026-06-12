package nodes

import (
	"time"

	"github.com/google/uuid"
)

type backupToNodeRelation struct {
	NodeID     uuid.UUID   `json:"nodeId"`
	BackupsIDs []uuid.UUID `json:"backupsIds"`
}

type BackupNode struct {
	ID            uuid.UUID `json:"id"`
	ThroughputMBs int       `json:"throughputMBs"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
}

type BackupNodeStats struct {
	ID            uuid.UUID `json:"id"`
	ActiveBackups int       `json:"activeBackups"`
}

type backupSubmitMessage struct {
	NodeID         uuid.UUID `json:"nodeId"`
	BackupID       uuid.UUID `json:"backupId"`
	IsCallNotifier bool      `json:"isCallNotifier"`
}

type backupCompletionMessage struct {
	NodeID   uuid.UUID `json:"nodeId"`
	BackupID uuid.UUID `json:"backupId"`
}
