package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/logger"
)

const (
	nodeInfoKeyPrefix       = "backup:node:"
	nodeInfoKeySuffix       = ":info"
	nodeActiveBackupsPrefix = "backup:node:"
	nodeActiveBackupsSuffix = ":active_backups"
	backupSubmitChannel     = "backup:submit"
	backupCompletionChannel = "backup:completion"

	deadNodeThreshold     = 2 * time.Minute
	cleanupTickerInterval = 1 * time.Second
)

// BackupNodesRegistry helps to sync backups scheduler and backup nodes.
//
// Features:
// - Track node availability and load level
// - Assign from scheduler to node backups needed to be processed
// - Notify scheduler from node about backup completion
//
// Important things to remember:
//   - Nodes without heartbeat for more than 2 minutes are not included
//     in available nodes list and stats
//
// Cleanup dead nodes performed on 2 levels:
//   - List and stats functions do not return dead nodes
//   - Periodically dead nodes are cleaned up in cache (to not
//     accumulate too many dead nodes in cache)
type BackupNodesRegistry struct {
	client            valkey.Client
	logger            *slog.Logger
	timeout           time.Duration
	pubsubBackups     *cache_utils.PubSubManager
	pubsubCompletions *cache_utils.PubSubManager

	// namespace isolates one logical pool of backup nodes from another by
	// prefixing every Redis key and pub/sub channel. The logical pool uses ""
	// (the bare legacy keys); the physical pool uses its own prefix so a
	// physical backup is never assigned to a logical node and vice versa.
	namespace string

	hasRun atomic.Bool
}

func NewBackupNodesRegistry(
	client valkey.Client,
	logger *slog.Logger,
	timeout time.Duration,
	pubsubBackups *cache_utils.PubSubManager,
	pubsubCompletions *cache_utils.PubSubManager,
	namespace string,
) *BackupNodesRegistry {
	return &BackupNodesRegistry{
		client:            client,
		logger:            logger,
		timeout:           timeout,
		pubsubBackups:     pubsubBackups,
		pubsubCompletions: pubsubCompletions,
		namespace:         namespace,
	}
}

// NewDefaultBackupNodesRegistry builds a registry wired to the shared Valkey
// client, the default cache timeout, and two fresh pub/sub managers (one for
// assignments, one for completions). namespace isolates this node pool (logical
// "" vs physical) from others in the shared Redis keyspace. Both schedulers' DI
// wiring goes through here so the construction boilerplate lives in one place.
func NewDefaultBackupNodesRegistry(namespace string) *BackupNodesRegistry {
	return NewBackupNodesRegistry(
		cache_utils.GetValkeyClient(),
		logger.GetLogger(),
		cache_utils.DefaultCacheTimeout,
		cache_utils.NewPubSubManager(),
		cache_utils.NewPubSubManager(),
		namespace,
	)
}

func (r *BackupNodesRegistry) Run(ctx context.Context) {
	if r.hasRun.Swap(true) {
		panic(fmt.Sprintf("%T.Run() called multiple times", r))
	}

	if err := r.cleanupDeadNodes(); err != nil {
		r.logger.Error("Failed to cleanup dead nodes on startup", "error", err)
	}

	ticker := time.NewTicker(cleanupTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.cleanupDeadNodes(); err != nil {
				r.logger.Error("Failed to cleanup dead nodes", "error", err)
			}
		}
	}
}

func (r *BackupNodesRegistry) GetAvailableNodes() ([]BackupNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := r.nodeInfoPattern()

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(1_000).Build(),
		)

		if result.Error() != nil {
			return nil, fmt.Errorf("failed to scan node keys: %w", result.Error())
		}

		scanResult, err := result.AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to parse scan result: %w", err)
		}

		allKeys = append(allKeys, scanResult.Elements...)

		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return []BackupNode{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var nodes []BackupNode

	for key, data := range keyDataMap {
		// Skip if the key doesn't exist (data is empty)
		if len(data) == 0 {
			continue
		}

		var node BackupNode
		if err := json.Unmarshal(data, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data", "key", key, "error", err)
			continue
		}

		// Skip nodes with zero/uninitialized heartbeat
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if node.LastHeartbeat.Before(threshold) {
			continue
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetAvailableNodeIDs returns the set of live node IDs (those past the dead-node
// heartbeat threshold are excluded), for callers that need O(1) liveness checks.
func (r *BackupNodesRegistry) GetAvailableNodeIDs() (map[uuid.UUID]bool, error) {
	availableNodes, err := r.GetAvailableNodes()
	if err != nil {
		return nil, err
	}

	aliveNodeIDs := make(map[uuid.UUID]bool, len(availableNodes))
	for _, node := range availableNodes {
		aliveNodeIDs[node.ID] = true
	}

	return aliveNodeIDs, nil
}

func (r *BackupNodesRegistry) GetBackupNodesStats() ([]BackupNodeStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := r.activeBackupsPattern()

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build(),
		)

		if result.Error() != nil {
			return nil, fmt.Errorf("failed to scan active backups keys: %w", result.Error())
		}

		scanResult, err := result.AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to parse scan result: %w", err)
		}

		allKeys = append(allKeys, scanResult.Elements...)

		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return []BackupNodeStats{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get active backups keys: %w", err)
	}

	var nodeInfoKeys []string
	nodeIDToStatsKey := make(map[string]string)
	for key := range keyDataMap {
		nodeID := r.extractNodeIDFromKey(key, r.namespace+nodeActiveBackupsPrefix, nodeActiveBackupsSuffix)
		nodeIDStr := nodeID.String()
		infoKey := r.nodeInfoKey(nodeIDStr)
		nodeInfoKeys = append(nodeInfoKeys, infoKey)
		nodeIDToStatsKey[infoKey] = key
	}

	nodeInfoMap, err := r.pipelineGetKeys(nodeInfoKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node info keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var stats []BackupNodeStats
	for infoKey, nodeData := range nodeInfoMap {
		// Skip if the info key doesn't exist (nodeData is empty)
		if len(nodeData) == 0 {
			continue
		}

		var node BackupNode
		if err := json.Unmarshal(nodeData, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data", "key", infoKey, "error", err)
			continue
		}

		// Skip nodes with zero/uninitialized heartbeat
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if node.LastHeartbeat.Before(threshold) {
			continue
		}

		statsKey := nodeIDToStatsKey[infoKey]
		tasksData := keyDataMap[statsKey]
		count, err := r.parseIntFromBytes(tasksData)
		if err != nil {
			r.logger.Warn("Failed to parse active backups count", "key", statsKey, "error", err)
			continue
		}

		stat := BackupNodeStats{
			ID:            node.ID,
			ActiveBackups: int(count),
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

func (r *BackupNodesRegistry) IncrementBackupsInProgress(nodeID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := r.activeBackupsKey(nodeID.String())
	result := r.client.Do(ctx, r.client.B().Incr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to increment backups in progress for node %s: %w",
			nodeID,
			result.Error(),
		)
	}

	return nil
}

func (r *BackupNodesRegistry) DecrementBackupsInProgress(nodeID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := r.activeBackupsKey(nodeID.String())
	result := r.client.Do(ctx, r.client.B().Decr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to decrement backups in progress for node %s: %w",
			nodeID,
			result.Error(),
		)
	}

	newValue, err := result.AsInt64()
	if err != nil {
		return fmt.Errorf("failed to parse decremented value for node %s: %w", nodeID, err)
	}

	if newValue < 0 {
		setCtx, setCancel := context.WithTimeout(context.Background(), r.timeout)
		r.client.Do(setCtx, r.client.B().Set().Key(key).Value("0").Build())
		setCancel()
		r.logger.Warn("Active backups counter went below 0, reset to 0", "nodeID", nodeID)
	}

	return nil
}

func (r *BackupNodesRegistry) HearthbeatNodeInRegistry(now time.Time, backupNode BackupNode) error {
	if now.IsZero() {
		return fmt.Errorf("cannot register node with zero heartbeat timestamp")
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	backupNode.LastHeartbeat = now

	data, err := json.Marshal(backupNode)
	if err != nil {
		return fmt.Errorf("failed to marshal backup node: %w", err)
	}

	key := r.nodeInfoKey(backupNode.ID.String())
	result := r.client.Do(
		ctx,
		r.client.B().Set().Key(key).Value(string(data)).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to register node %s: %w", backupNode.ID, result.Error())
	}

	return nil
}

func (r *BackupNodesRegistry) UnregisterNodeFromRegistry(backupNode BackupNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	infoKey := r.nodeInfoKey(backupNode.ID.String())
	counterKey := r.activeBackupsKey(backupNode.ID.String())

	result := r.client.Do(
		ctx,
		r.client.B().Del().Key(infoKey, counterKey).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to unregister node %s: %w", backupNode.ID, result.Error())
	}

	r.logger.Info("Unregistered node from registry", "nodeID", backupNode.ID)
	return nil
}

func (r *BackupNodesRegistry) AssignBackupToNode(
	targetNodeID uuid.UUID,
	backupID uuid.UUID,
	isCallNotifier bool,
) error {
	ctx := context.Background()

	message := backupSubmitMessage{
		NodeID:         targetNodeID,
		BackupID:       backupID,
		IsCallNotifier: isCallNotifier,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal backup submit message: %w", err)
	}

	err = r.pubsubBackups.Publish(ctx, r.submitChannel(), string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish backup submit message: %w", err)
	}

	return nil
}

func (r *BackupNodesRegistry) SubscribeNodeForBackupsAssignment(
	nodeID uuid.UUID,
	handler func(backupID uuid.UUID, isCallNotifier bool),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg backupSubmitMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal backup submit message", "error", err)
			return
		}

		if msg.NodeID != nodeID {
			return
		}

		handler(msg.BackupID, msg.IsCallNotifier)
	}

	err := r.pubsubBackups.Subscribe(ctx, r.submitChannel(), wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to backup submit channel: %w", err)
	}

	r.logger.Info("Subscribed to backup submit channel", "nodeID", nodeID)
	return nil
}

func (r *BackupNodesRegistry) UnsubscribeNodeForBackupsAssignments() error {
	err := r.pubsubBackups.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from backup submit channel: %w", err)
	}

	r.logger.Info("Unsubscribed from backup submit channel")
	return nil
}

func (r *BackupNodesRegistry) PublishBackupCompletion(nodeID, backupID uuid.UUID) error {
	ctx := context.Background()

	message := backupCompletionMessage{
		NodeID:   nodeID,
		BackupID: backupID,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal backup completion message: %w", err)
	}

	err = r.pubsubCompletions.Publish(ctx, r.completionChannel(), string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish backup completion message: %w", err)
	}

	return nil
}

func (r *BackupNodesRegistry) SubscribeForBackupsCompletions(
	handler func(nodeID, backupID uuid.UUID),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg backupCompletionMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal backup completion message", "error", err)
			return
		}

		handler(msg.NodeID, msg.BackupID)
	}

	err := r.pubsubCompletions.Subscribe(ctx, r.completionChannel(), wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to backup completion channel: %w", err)
	}

	r.logger.Info("Subscribed to backup completion channel")
	return nil
}

func (r *BackupNodesRegistry) UnsubscribeForBackupsCompletions() error {
	err := r.pubsubCompletions.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from backup completion channel: %w", err)
	}

	r.logger.Info("Unsubscribed from backup completion channel")
	return nil
}

func (r *BackupNodesRegistry) extractNodeIDFromKey(key, prefix, suffix string) uuid.UUID {
	nodeIDStr := strings.TrimPrefix(key, prefix)
	nodeIDStr = strings.TrimSuffix(nodeIDStr, suffix)

	nodeID, err := uuid.Parse(nodeIDStr)
	if err != nil {
		r.logger.Warn("Failed to parse node ID from key", "key", key, "error", err)
		return uuid.Nil
	}

	return nodeID
}

func (r *BackupNodesRegistry) pipelineGetKeys(keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	commands := make([]valkey.Completed, 0, len(keys))
	for _, key := range keys {
		commands = append(commands, r.client.B().Get().Key(key).Build())
	}

	results := r.client.DoMulti(ctx, commands...)

	keyDataMap := make(map[string][]byte, len(keys))
	for i, result := range results {
		if result.Error() != nil {
			r.logger.Warn("Failed to get key in pipeline", "key", keys[i], "error", result.Error())
			continue
		}

		data, err := result.AsBytes()
		if err != nil {
			r.logger.Warn("Failed to parse key data in pipeline", "key", keys[i], "error", err)
			continue
		}

		keyDataMap[keys[i]] = data
	}

	return keyDataMap, nil
}

func (r *BackupNodesRegistry) parseIntFromBytes(data []byte) (int64, error) {
	str := string(data)
	var count int64
	_, err := fmt.Sscanf(str, "%d", &count)
	if err != nil {
		return 0, fmt.Errorf("failed to parse integer from bytes: %w", err)
	}
	return count, nil
}

func (r *BackupNodesRegistry) cleanupDeadNodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := r.nodeInfoPattern()

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(1_000).Build(),
		)

		if result.Error() != nil {
			return fmt.Errorf("failed to scan node keys: %w", result.Error())
		}

		scanResult, err := result.AsScanEntry()
		if err != nil {
			return fmt.Errorf("failed to parse scan result: %w", err)
		}

		allKeys = append(allKeys, scanResult.Elements...)

		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	if len(allKeys) == 0 {
		return nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return fmt.Errorf("failed to pipeline get node keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var deadNodeKeys []string

	for key, data := range keyDataMap {
		// Skip if the key doesn't exist (data is empty)
		if len(data) == 0 {
			continue
		}

		var node BackupNode
		if err := json.Unmarshal(data, &node); err != nil {
			r.logger.Warn("Failed to unmarshal node data during cleanup", "key", key, "error", err)
			continue
		}

		// Skip nodes with zero/uninitialized heartbeat
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if node.LastHeartbeat.Before(threshold) {
			nodeID := node.ID.String()
			infoKey := r.nodeInfoKey(nodeID)
			statsKey := r.activeBackupsKey(nodeID)

			deadNodeKeys = append(deadNodeKeys, infoKey, statsKey)
			r.logger.Info(
				"Marking node for cleanup",
				"nodeID", nodeID,
				"lastHeartbeat", node.LastHeartbeat,
				"threshold", threshold,
			)
		}
	}

	if len(deadNodeKeys) == 0 {
		return nil
	}

	delCtx, delCancel := context.WithTimeout(context.Background(), r.timeout)
	defer delCancel()

	result := r.client.Do(
		delCtx,
		r.client.B().Del().Key(deadNodeKeys...).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to delete dead node keys: %w", result.Error())
	}

	deletedCount, err := result.AsInt64()
	if err != nil {
		return fmt.Errorf("failed to parse deleted count: %w", err)
	}

	r.logger.Info("Cleaned up dead nodes", "deletedKeysCount", deletedCount)
	return nil
}

// Namespaced Redis key / channel helpers. The namespace prefix isolates one
// backup-node pool (logical vs physical) from another in the shared keyspace.

func (r *BackupNodesRegistry) nodeInfoKey(nodeID string) string {
	return r.namespace + nodeInfoKeyPrefix + nodeID + nodeInfoKeySuffix
}

func (r *BackupNodesRegistry) activeBackupsKey(nodeID string) string {
	return r.namespace + nodeActiveBackupsPrefix + nodeID + nodeActiveBackupsSuffix
}

func (r *BackupNodesRegistry) nodeInfoPattern() string {
	return r.namespace + nodeInfoKeyPrefix + "*" + nodeInfoKeySuffix
}

func (r *BackupNodesRegistry) activeBackupsPattern() string {
	return r.namespace + nodeActiveBackupsPrefix + "*" + nodeActiveBackupsSuffix
}

func (r *BackupNodesRegistry) submitChannel() string {
	return r.namespace + backupSubmitChannel
}

func (r *BackupNodesRegistry) completionChannel() string {
	return r.namespace + backupCompletionChannel
}
