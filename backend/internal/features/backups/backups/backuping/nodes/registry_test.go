package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
)

func Test_HearthbeatNodeInRegistry_RegistersNodeWithTTL(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, node.ID, nodes[0].ID)
	assert.Equal(t, node.ThroughputMBs, nodes[0].ThroughputMBs)
}

func Test_UnregisterNodeFromRegistry_RemovesNodeAndCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	err = registry.UnregisterNodeFromRegistry(node)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Empty(t, nodes)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Empty(t, stats)
}

func Test_GetAvailableNodes_ReturnsAllRegisteredNodes(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	node3 := createTestBackupNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)

	nodeIDs := make(map[uuid.UUID]bool)
	for _, node := range nodes {
		nodeIDs[node.ID] = true
	}
	assert.True(t, nodeIDs[node1.ID])
	assert.True(t, nodeIDs[node2.ID])
	assert.True(t, nodeIDs[node3.ID])
}

func Test_GetAvailableNodes_WhenNoNodesExist_ReturnsEmptySlice(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.NotNil(t, nodes)
	assert.Empty(t, nodes)
}

func Test_IncrementBackupsInProgress_IncrementsCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, node.ID, stats[0].ID)
	assert.Equal(t, 1, stats[0].ActiveBackups)

	err = registry.IncrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	stats, err = registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, 2, stats[0].ActiveBackups)
}

func Test_DecrementBackupsInProgress_DecrementsCounter(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Equal(t, 3, stats[0].ActiveBackups)

	err = registry.DecrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	stats, err = registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Equal(t, 2, stats[0].ActiveBackups)

	err = registry.DecrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	stats, err = registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Equal(t, 1, stats[0].ActiveBackups)
}

func Test_DecrementBackupsInProgress_WhenNegative_ResetsToZero(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	err = registry.DecrementBackupsInProgress(node.ID)
	assert.NoError(t, err)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, 0, stats[0].ActiveBackups)
}

func Test_GetBackupNodesStats_ReturnsStatsForAllNodes(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	node3 := createTestBackupNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node1.ID)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node2.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node2.ID)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node3.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node3.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node3.ID)
	assert.NoError(t, err)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 3)

	statsMap := make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveBackups
	}

	assert.Equal(t, 1, statsMap[node1.ID])
	assert.Equal(t, 2, statsMap[node2.ID])
	assert.Equal(t, 3, statsMap[node3.ID])
}

func Test_GetBackupNodesStats_WhenNoStats_ReturnsEmptySlice(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Empty(t, stats)
}

func Test_MultipleNodes_RegisteredAndQueriedCorrectly(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node1.ThroughputMBs = 50
	node2 := createTestBackupNode()
	node2.ThroughputMBs = 100
	node3 := createTestBackupNode()
	node3.ThroughputMBs = 150
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)

	nodeMap := make(map[uuid.UUID]BackupNode)
	for _, node := range nodes {
		nodeMap[node.ID] = node
	}

	assert.Equal(t, 50, nodeMap[node1.ID].ThroughputMBs)
	assert.Equal(t, 100, nodeMap[node2.ID].ThroughputMBs)
	assert.Equal(t, 150, nodeMap[node3.ID].ThroughputMBs)
}

func Test_BackupCounters_TrackedSeparatelyPerNode(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node1.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node1.ID)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node2.ID)
	assert.NoError(t, err)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 2)

	statsMap := make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveBackups
	}

	assert.Equal(t, 2, statsMap[node1.ID])
	assert.Equal(t, 1, statsMap[node2.ID])

	err = registry.DecrementBackupsInProgress(node1.ID)
	assert.NoError(t, err)

	stats, err = registry.GetBackupNodesStats()
	assert.NoError(t, err)

	statsMap = make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveBackups
	}

	assert.Equal(t, 1, statsMap[node1.ID])
	assert.Equal(t, 1, statsMap[node2.ID])
}

func Test_GetAvailableNodes_SkipsInvalidJsonData(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer cleanupTestNode(registry, node)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), registry.timeout)
	defer cancel()

	invalidKey := nodeInfoKeyPrefix + uuid.New().String() + nodeInfoKeySuffix
	registry.client.Do(
		ctx,
		registry.client.B().Set().Key(invalidKey).Value("invalid json data").Build(),
	)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(t.Context(), registry.timeout)
		defer cleanupCancel()
		registry.client.Do(cleanupCtx, registry.client.B().Del().Key(invalidKey).Build())
	}()

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, node.ID, nodes[0].ID)
}

func Test_PipelineGetKeys_HandlesEmptyKeysList(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()

	keyDataMap, err := registry.pipelineGetKeys([]string{})
	assert.NoError(t, err)
	assert.NotNil(t, keyDataMap)
	assert.Empty(t, keyDataMap)
}

func Test_HearthbeatNodeInRegistry_UpdatesLastHeartbeat(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	originalHeartbeat := node.LastHeartbeat
	defer cleanupTestNode(registry, node)

	time.Sleep(10 * time.Millisecond)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node)
	assert.NoError(t, err)

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.True(t, nodes[0].LastHeartbeat.After(originalHeartbeat))
}

func Test_HearthbeatNodeInRegistry_RejectsZeroTimestamp(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()

	err := registry.HearthbeatNodeInRegistry(time.Time{}, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zero heartbeat timestamp")

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 0)
}

func Test_GetAvailableNodes_ExcludesStaleNodesFromCache(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	node3 := createTestBackupNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), registry.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	result := registry.client.Do(ctx, registry.client.B().Get().Key(key).Build())
	assert.NoError(t, result.Error())

	data, err := result.AsBytes()
	assert.NoError(t, err)

	var node BackupNode
	err = json.Unmarshal(data, &node)
	assert.NoError(t, err)

	node.LastHeartbeat = time.Now().UTC().Add(-3 * time.Minute)
	modifiedData, err := json.Marshal(node)
	assert.NoError(t, err)

	setCtx, setCancel := context.WithTimeout(t.Context(), registry.timeout)
	defer setCancel()
	setResult := registry.client.Do(
		setCtx,
		registry.client.B().Set().Key(key).Value(string(modifiedData)).Build(),
	)
	assert.NoError(t, setResult.Error())

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)

	nodeIDs := make(map[uuid.UUID]bool)
	for _, n := range nodes {
		nodeIDs[n.ID] = true
	}
	assert.True(t, nodeIDs[node1.ID])
	assert.False(t, nodeIDs[node2.ID])
	assert.True(t, nodeIDs[node3.ID])
}

func Test_GetBackupNodesStats_ExcludesStaleNodesFromCache(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	node3 := createTestBackupNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)
	defer cleanupTestNode(registry, node3)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node3)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node1.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node2.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node3.ID)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), registry.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	result := registry.client.Do(ctx, registry.client.B().Get().Key(key).Build())
	assert.NoError(t, result.Error())

	data, err := result.AsBytes()
	assert.NoError(t, err)

	var node BackupNode
	err = json.Unmarshal(data, &node)
	assert.NoError(t, err)

	node.LastHeartbeat = time.Now().UTC().Add(-3 * time.Minute)
	modifiedData, err := json.Marshal(node)
	assert.NoError(t, err)

	setCtx, setCancel := context.WithTimeout(t.Context(), registry.timeout)
	defer setCancel()
	setResult := registry.client.Do(
		setCtx,
		registry.client.B().Set().Key(key).Value(string(modifiedData)).Build(),
	)
	assert.NoError(t, setResult.Error())

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 2)

	statsMap := make(map[uuid.UUID]int)
	for _, stat := range stats {
		statsMap[stat.ID] = stat.ActiveBackups
	}

	assert.Equal(t, 1, statsMap[node1.ID])
	_, hasNode2 := statsMap[node2.ID]
	assert.False(t, hasNode2)
	assert.Equal(t, 1, statsMap[node3.ID])
}

func Test_CleanupDeadNodes_RemovesNodeInfoAndCounter(t *testing.T) {
	cache_utils.ClearAllCache()

	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	defer cleanupTestNode(registry, node1)
	defer cleanupTestNode(registry, node2)

	err := registry.HearthbeatNodeInRegistry(time.Now().UTC(), node1)
	assert.NoError(t, err)
	err = registry.HearthbeatNodeInRegistry(time.Now().UTC(), node2)
	assert.NoError(t, err)

	err = registry.IncrementBackupsInProgress(node1.ID)
	assert.NoError(t, err)
	err = registry.IncrementBackupsInProgress(node2.ID)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), registry.timeout)
	defer cancel()

	key := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	result := registry.client.Do(ctx, registry.client.B().Get().Key(key).Build())
	assert.NoError(t, result.Error())

	data, err := result.AsBytes()
	assert.NoError(t, err)

	var node BackupNode
	err = json.Unmarshal(data, &node)
	assert.NoError(t, err)

	node.LastHeartbeat = time.Now().UTC().Add(-3 * time.Minute)
	modifiedData, err := json.Marshal(node)
	assert.NoError(t, err)

	setCtx, setCancel := context.WithTimeout(t.Context(), registry.timeout)
	defer setCancel()
	setResult := registry.client.Do(
		setCtx,
		registry.client.B().Set().Key(key).Value(string(modifiedData)).Build(),
	)
	assert.NoError(t, setResult.Error())

	err = registry.cleanupDeadNodes()
	assert.NoError(t, err)

	checkCtx, checkCancel := context.WithTimeout(t.Context(), registry.timeout)
	defer checkCancel()

	infoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node2.ID.String(), nodeInfoKeySuffix)
	infoResult := registry.client.Do(checkCtx, registry.client.B().Get().Key(infoKey).Build())
	assert.Error(t, infoResult.Error())

	counterKey := fmt.Sprintf(
		"%s%s%s",
		nodeActiveBackupsPrefix,
		node2.ID.String(),
		nodeActiveBackupsSuffix,
	)
	counterCtx, counterCancel := context.WithTimeout(t.Context(), registry.timeout)
	defer counterCancel()
	counterResult := registry.client.Do(
		counterCtx,
		registry.client.B().Get().Key(counterKey).Build(),
	)
	assert.Error(t, counterResult.Error())

	activeInfoKey := fmt.Sprintf("%s%s%s", nodeInfoKeyPrefix, node1.ID.String(), nodeInfoKeySuffix)
	activeCtx, activeCancel := context.WithTimeout(t.Context(), registry.timeout)
	defer activeCancel()
	activeResult := registry.client.Do(
		activeCtx,
		registry.client.B().Get().Key(activeInfoKey).Build(),
	)
	assert.NoError(t, activeResult.Error())

	nodes, err := registry.GetAvailableNodes()
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, node1.ID, nodes[0].ID)

	stats, err := registry.GetBackupNodesStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, node1.ID, stats[0].ID)
}

func createTestRegistry() *BackupNodesRegistry {
	return &BackupNodesRegistry{
		client:            cache_utils.GetValkeyClient(),
		logger:            logger.GetLogger(),
		timeout:           cache_utils.DefaultCacheTimeout,
		pubsubBackups:     cache_utils.NewPubSubManager(),
		pubsubCompletions: cache_utils.NewPubSubManager(),
	}
}

func createTestBackupNode() BackupNode {
	return BackupNode{
		ID:            uuid.New(),
		ThroughputMBs: 100,
		LastHeartbeat: time.Now().UTC(),
	}
}

func cleanupTestNode(registry *BackupNodesRegistry, node BackupNode) {
	registry.UnregisterNodeFromRegistry(node)
}

func Test_AssignBackupTonode_PublishesJsonMessageToChannel(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID := uuid.New()

	err := registry.AssignBackupToNode(node.ID, backupID, true)
	assert.NoError(t, err)
}

func Test_SubscribeNodeForBackupsAssignment_ReceivesSubmittedBackupsForMatchingNode(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID := uuid.New()
	defer registry.UnsubscribeNodeForBackupsAssignments()

	receivedBackupID := make(chan uuid.UUID, 1)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedBackupID <- id
	}

	err := registry.SubscribeNodeForBackupsAssignment(node.ID, handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignBackupToNode(node.ID, backupID, true)
	assert.NoError(t, err)

	select {
	case received := <-receivedBackupID:
		assert.Equal(t, backupID, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for backup message")
	}
}

func Test_SubscribeNodeForBackupsAssignment_FiltersOutBackupsForDifferentNode(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	backupID := uuid.New()
	defer registry.UnsubscribeNodeForBackupsAssignments()

	receivedBackupID := make(chan uuid.UUID, 1)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedBackupID <- id
	}

	err := registry.SubscribeNodeForBackupsAssignment(node1.ID, handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignBackupToNode(node2.ID, backupID, false)
	assert.NoError(t, err)

	select {
	case <-receivedBackupID:
		t.Fatal("Should not receive backup for different node")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_SubscribeNodeForBackupsAssignment_ParsesJsonAndBackupIdCorrectly(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID1 := uuid.New()
	backupID2 := uuid.New()
	defer registry.UnsubscribeNodeForBackupsAssignments()

	receivedBackups := make(chan uuid.UUID, 2)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedBackups <- id
	}

	err := registry.SubscribeNodeForBackupsAssignment(node.ID, handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignBackupToNode(node.ID, backupID1, true)
	assert.NoError(t, err)

	err = registry.AssignBackupToNode(node.ID, backupID2, false)
	assert.NoError(t, err)

	received1 := <-receivedBackups
	received2 := <-receivedBackups

	receivedIDs := []uuid.UUID{received1, received2}
	assert.Contains(t, receivedIDs, backupID1)
	assert.Contains(t, receivedIDs, backupID2)
}

func Test_SubscribeNodeForBackupsAssignment_HandlesInvalidJson(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer registry.UnsubscribeNodeForBackupsAssignments()

	receivedBackupID := make(chan uuid.UUID, 1)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedBackupID <- id
	}

	err := registry.SubscribeNodeForBackupsAssignment(node.ID, handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx := t.Context()
	err = registry.pubsubBackups.Publish(ctx, "backup:submit", "invalid json")
	assert.NoError(t, err)

	select {
	case <-receivedBackupID:
		t.Fatal("Should not receive backup for invalid JSON")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_UnsubscribeNodeForBackupsAssignments_StopsReceivingMessages(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID1 := uuid.New()
	backupID2 := uuid.New()

	receivedBackupID := make(chan uuid.UUID, 2)
	handler := func(id uuid.UUID, isCallNotifier bool) {
		receivedBackupID <- id
	}

	err := registry.SubscribeNodeForBackupsAssignment(node.ID, handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignBackupToNode(node.ID, backupID1, true)
	assert.NoError(t, err)

	received := <-receivedBackupID
	assert.Equal(t, backupID1, received)

	err = registry.UnsubscribeNodeForBackupsAssignments()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.AssignBackupToNode(node.ID, backupID2, false)
	assert.NoError(t, err)

	select {
	case <-receivedBackupID:
		t.Fatal("Should not receive backup after unsubscribe")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_SubscribeNodeForBackupsAssignment_WhenAlreadySubscribed_ReturnsError(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	defer registry.UnsubscribeNodeForBackupsAssignments()

	handler := func(id uuid.UUID, isCallNotifier bool) {}

	err := registry.SubscribeNodeForBackupsAssignment(node.ID, handler)
	assert.NoError(t, err)

	err = registry.SubscribeNodeForBackupsAssignment(node.ID, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func Test_MultipleNodes_EachReceivesOnlyTheirBackups(t *testing.T) {
	cache_utils.ClearAllCache()
	registry1 := createTestRegistry()
	registry2 := createTestRegistry()
	registry3 := createTestRegistry()

	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	node3 := createTestBackupNode()

	backupID1 := uuid.New()
	backupID2 := uuid.New()
	backupID3 := uuid.New()

	defer registry1.UnsubscribeNodeForBackupsAssignments()
	defer registry2.UnsubscribeNodeForBackupsAssignments()
	defer registry3.UnsubscribeNodeForBackupsAssignments()

	receivedBackups1 := make(chan uuid.UUID, 3)
	receivedBackups2 := make(chan uuid.UUID, 3)
	receivedBackups3 := make(chan uuid.UUID, 3)

	handler1 := func(id uuid.UUID, isCallNotifier bool) { receivedBackups1 <- id }
	handler2 := func(id uuid.UUID, isCallNotifier bool) { receivedBackups2 <- id }
	handler3 := func(id uuid.UUID, isCallNotifier bool) { receivedBackups3 <- id }

	err := registry1.SubscribeNodeForBackupsAssignment(node1.ID, handler1)
	assert.NoError(t, err)

	err = registry2.SubscribeNodeForBackupsAssignment(node2.ID, handler2)
	assert.NoError(t, err)

	err = registry3.SubscribeNodeForBackupsAssignment(node3.ID, handler3)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	submitRegistry := createTestRegistry()
	err = submitRegistry.AssignBackupToNode(node1.ID, backupID1, true)
	assert.NoError(t, err)

	err = submitRegistry.AssignBackupToNode(node2.ID, backupID2, false)
	assert.NoError(t, err)

	err = submitRegistry.AssignBackupToNode(node3.ID, backupID3, true)
	assert.NoError(t, err)

	select {
	case received := <-receivedBackups1:
		assert.Equal(t, backupID1, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Node 1 timeout waiting for backup message")
	}

	select {
	case received := <-receivedBackups2:
		assert.Equal(t, backupID2, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Node 2 timeout waiting for backup message")
	}

	select {
	case received := <-receivedBackups3:
		assert.Equal(t, backupID3, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Node 3 timeout waiting for backup message")
	}

	select {
	case <-receivedBackups1:
		t.Fatal("Node 1 should not receive additional backups")
	case <-time.After(300 * time.Millisecond):
	}

	select {
	case <-receivedBackups2:
		t.Fatal("Node 2 should not receive additional backups")
	case <-time.After(300 * time.Millisecond):
	}

	select {
	case <-receivedBackups3:
		t.Fatal("Node 3 should not receive additional backups")
	case <-time.After(300 * time.Millisecond):
	}
}

func Test_PublishBackupCompletion_PublishesMessageToChannel(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID := uuid.New()

	err := registry.PublishBackupCompletion(node.ID, backupID)
	assert.NoError(t, err)
}

func Test_SubscribeForBackupsCompletions_ReceivesCompletedBackups(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID := uuid.New()
	defer registry.UnsubscribeForBackupsCompletions()

	receivedBackupID := make(chan uuid.UUID, 1)
	receivedNodeID := make(chan uuid.UUID, 1)
	handler := func(nodeID, backupID uuid.UUID) {
		receivedNodeID <- nodeID
		receivedBackupID <- backupID
	}

	err := registry.SubscribeForBackupsCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishBackupCompletion(node.ID, backupID)
	assert.NoError(t, err)

	select {
	case receivedNode := <-receivedNodeID:
		assert.Equal(t, node.ID, receivedNode)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for node ID")
	}

	select {
	case received := <-receivedBackupID:
		assert.Equal(t, backupID, received)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for backup completion message")
	}
}

func Test_SubscribeForBackupsCompletions_ParsesJsonCorrectly(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID1 := uuid.New()
	backupID2 := uuid.New()
	defer registry.UnsubscribeForBackupsCompletions()

	receivedBackups := make(chan uuid.UUID, 2)
	handler := func(nodeID, backupID uuid.UUID) {
		receivedBackups <- backupID
	}

	err := registry.SubscribeForBackupsCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishBackupCompletion(node.ID, backupID1)
	assert.NoError(t, err)

	err = registry.PublishBackupCompletion(node.ID, backupID2)
	assert.NoError(t, err)

	received1 := <-receivedBackups
	received2 := <-receivedBackups

	receivedIDs := []uuid.UUID{received1, received2}
	assert.Contains(t, receivedIDs, backupID1)
	assert.Contains(t, receivedIDs, backupID2)
}

func Test_SubscribeForBackupsCompletions_HandlesInvalidJson(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	defer registry.UnsubscribeForBackupsCompletions()

	receivedBackupID := make(chan uuid.UUID, 1)
	handler := func(nodeID, backupID uuid.UUID) {
		receivedBackupID <- backupID
	}

	err := registry.SubscribeForBackupsCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx := t.Context()
	err = registry.pubsubCompletions.Publish(ctx, "backup:completion", "invalid json")
	assert.NoError(t, err)

	select {
	case <-receivedBackupID:
		t.Fatal("Should not receive backup for invalid JSON")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_UnsubscribeForBackupsCompletions_StopsReceivingMessages(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	node := createTestBackupNode()
	backupID1 := uuid.New()
	backupID2 := uuid.New()

	receivedBackupID := make(chan uuid.UUID, 2)
	handler := func(nodeID, backupID uuid.UUID) {
		receivedBackupID <- backupID
	}

	err := registry.SubscribeForBackupsCompletions(handler)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishBackupCompletion(node.ID, backupID1)
	assert.NoError(t, err)

	received := <-receivedBackupID
	assert.Equal(t, backupID1, received)

	err = registry.UnsubscribeForBackupsCompletions()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = registry.PublishBackupCompletion(node.ID, backupID2)
	assert.NoError(t, err)

	select {
	case <-receivedBackupID:
		t.Fatal("Should not receive backup after unsubscribe")
	case <-time.After(500 * time.Millisecond):
	}
}

func Test_SubscribeForBackupsCompletions_WhenAlreadySubscribed_ReturnsError(t *testing.T) {
	cache_utils.ClearAllCache()
	registry := createTestRegistry()
	defer registry.UnsubscribeForBackupsCompletions()

	handler := func(nodeID, backupID uuid.UUID) {}

	err := registry.SubscribeForBackupsCompletions(handler)
	assert.NoError(t, err)

	err = registry.SubscribeForBackupsCompletions(handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func Test_MultipleSubscribers_EachReceivesCompletionMessages(t *testing.T) {
	cache_utils.ClearAllCache()
	registry1 := createTestRegistry()
	registry2 := createTestRegistry()
	registry3 := createTestRegistry()

	node1 := createTestBackupNode()
	node2 := createTestBackupNode()
	node3 := createTestBackupNode()

	backupID1 := uuid.New()
	backupID2 := uuid.New()
	backupID3 := uuid.New()

	defer registry1.UnsubscribeForBackupsCompletions()
	defer registry2.UnsubscribeForBackupsCompletions()
	defer registry3.UnsubscribeForBackupsCompletions()

	receivedBackups1 := make(chan uuid.UUID, 3)
	receivedBackups2 := make(chan uuid.UUID, 3)
	receivedBackups3 := make(chan uuid.UUID, 3)

	handler1 := func(nodeID, backupID uuid.UUID) { receivedBackups1 <- backupID }
	handler2 := func(nodeID, backupID uuid.UUID) { receivedBackups2 <- backupID }
	handler3 := func(nodeID, backupID uuid.UUID) { receivedBackups3 <- backupID }

	err := registry1.SubscribeForBackupsCompletions(handler1)
	assert.NoError(t, err)

	err = registry2.SubscribeForBackupsCompletions(handler2)
	assert.NoError(t, err)

	err = registry3.SubscribeForBackupsCompletions(handler3)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	publishRegistry := createTestRegistry()
	err = publishRegistry.PublishBackupCompletion(node1.ID, backupID1)
	assert.NoError(t, err)

	err = publishRegistry.PublishBackupCompletion(node2.ID, backupID2)
	assert.NoError(t, err)

	err = publishRegistry.PublishBackupCompletion(node3.ID, backupID3)
	assert.NoError(t, err)

	receivedAll1 := []uuid.UUID{}
	receivedAll2 := []uuid.UUID{}
	receivedAll3 := []uuid.UUID{}

	for range 3 {
		select {
		case received := <-receivedBackups1:
			receivedAll1 = append(receivedAll1, received)
		case <-time.After(2 * time.Second):
			t.Fatal("Subscriber 1 timeout waiting for completion message")
		}
	}

	for range 3 {
		select {
		case received := <-receivedBackups2:
			receivedAll2 = append(receivedAll2, received)
		case <-time.After(2 * time.Second):
			t.Fatal("Subscriber 2 timeout waiting for completion message")
		}
	}

	for range 3 {
		select {
		case received := <-receivedBackups3:
			receivedAll3 = append(receivedAll3, received)
		case <-time.After(2 * time.Second):
			t.Fatal("Subscriber 3 timeout waiting for completion message")
		}
	}

	assert.Contains(t, receivedAll1, backupID1)
	assert.Contains(t, receivedAll1, backupID2)
	assert.Contains(t, receivedAll1, backupID3)

	assert.Contains(t, receivedAll2, backupID1)
	assert.Contains(t, receivedAll2, backupID2)
	assert.Contains(t, receivedAll2, backupID3)

	assert.Contains(t, receivedAll3, backupID1)
	assert.Contains(t, receivedAll3, backupID2)
	assert.Contains(t, receivedAll3, backupID3)
}
