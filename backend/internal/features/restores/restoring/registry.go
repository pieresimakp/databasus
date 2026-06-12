package restoring

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
)

const (
	nodeInfoKeyPrefix        = "restore:node:"
	nodeInfoKeySuffix        = ":info"
	nodeActiveRestoresPrefix = "restore:node:"
	nodeActiveRestoresSuffix = ":active_restores"
	restoreSubmitChannel     = "restore:submit"
	restoreCompletionChannel = "restore:completion"

	deadNodeThreshold     = 2 * time.Minute
	cleanupTickerInterval = 1 * time.Second
)

// RestoreNodesRegistry helps to sync restores scheduler and restore nodes.
//
// Features:
// - Track node availability and load level
// - Assign from scheduler to node restores needed to be processed
// - Notify scheduler from node about restore completion
//
// Important things to remember:
//   - Nodes without heartbeat for more than 2 minutes are not included
//     in available nodes list and stats
//
// Cleanup dead nodes performed on 2 levels:
//   - List and stats functions do not return dead nodes
//   - Periodically dead nodes are cleaned up in cache (to not
//     accumulate too many dead nodes in cache)
type RestoreNodesRegistry struct {
	client            valkey.Client
	logger            *slog.Logger
	timeout           time.Duration
	pubsubRestores    *cache_utils.PubSubManager
	pubsubCompletions *cache_utils.PubSubManager

	// namespace isolates one pool of restore nodes from another by prefixing
	// every Redis key and pub/sub channel. Empty in production; per-worker
	// ("w{slot}:") under parallel tests so concurrent test binaries never share
	// node state or submit/completion messages.
	namespace string

	hasRun atomic.Bool
}

func NewRestoreNodesRegistry(
	client valkey.Client,
	logger *slog.Logger,
	timeout time.Duration,
	pubsubRestores *cache_utils.PubSubManager,
	pubsubCompletions *cache_utils.PubSubManager,
	namespace string,
) *RestoreNodesRegistry {
	return &RestoreNodesRegistry{
		client:            client,
		logger:            logger,
		timeout:           timeout,
		pubsubRestores:    pubsubRestores,
		pubsubCompletions: pubsubCompletions,
		namespace:         namespace,
	}
}

func (r *RestoreNodesRegistry) Run(ctx context.Context) {
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

func (r *RestoreNodesRegistry) GetAvailableNodes() ([]RestoreNode, error) {
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
		return []RestoreNode{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var nodes []RestoreNode

	for key, data := range keyDataMap {
		// Skip if the key doesn't exist (data is empty)
		if len(data) == 0 {
			continue
		}

		var node RestoreNode
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

func (r *RestoreNodesRegistry) GetRestoreNodesStats() ([]RestoreNodeStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var allKeys []string
	cursor := uint64(0)
	pattern := r.activeRestoresPattern()

	for {
		result := r.client.Do(
			ctx,
			r.client.B().Scan().Cursor(cursor).Match(pattern).Count(100).Build(),
		)

		if result.Error() != nil {
			return nil, fmt.Errorf("failed to scan active restores keys: %w", result.Error())
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
		return []RestoreNodeStats{}, nil
	}

	keyDataMap, err := r.pipelineGetKeys(allKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get active restores keys: %w", err)
	}

	var nodeInfoKeys []string
	nodeIDToStatsKey := make(map[string]string)
	for key := range keyDataMap {
		nodeID := r.extractNodeIDFromKey(
			key,
			r.namespace+nodeActiveRestoresPrefix,
			nodeActiveRestoresSuffix,
		)
		infoKey := r.nodeInfoKey(nodeID.String())
		nodeInfoKeys = append(nodeInfoKeys, infoKey)
		nodeIDToStatsKey[infoKey] = key
	}

	nodeInfoMap, err := r.pipelineGetKeys(nodeInfoKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to pipeline get node info keys: %w", err)
	}

	threshold := time.Now().UTC().Add(-deadNodeThreshold)
	var stats []RestoreNodeStats
	for infoKey, nodeData := range nodeInfoMap {
		// Skip if the info key doesn't exist (nodeData is empty)
		if len(nodeData) == 0 {
			continue
		}

		var node RestoreNode
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
			r.logger.Warn("Failed to parse active restores count", "key", statsKey, "error", err)
			continue
		}

		stat := RestoreNodeStats{
			ID:             node.ID,
			ActiveRestores: int(count),
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

func (r *RestoreNodesRegistry) IncrementRestoresInProgress(nodeID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := r.activeRestoresKey(nodeID.String())
	result := r.client.Do(ctx, r.client.B().Incr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to increment restores in progress for node %s: %w",
			nodeID,
			result.Error(),
		)
	}

	return nil
}

func (r *RestoreNodesRegistry) DecrementRestoresInProgress(nodeID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	key := r.activeRestoresKey(nodeID.String())
	result := r.client.Do(ctx, r.client.B().Decr().Key(key).Build())

	if result.Error() != nil {
		return fmt.Errorf(
			"failed to decrement restores in progress for node %s: %w",
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
		r.logger.Warn("Active restores counter went below 0, reset to 0", "nodeID", nodeID)
	}

	return nil
}

func (r *RestoreNodesRegistry) HearthbeatNodeInRegistry(
	now time.Time,
	restoreNode RestoreNode,
) error {
	if now.IsZero() {
		return fmt.Errorf("cannot register node with zero heartbeat timestamp")
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	restoreNode.LastHeartbeat = now

	data, err := json.Marshal(restoreNode)
	if err != nil {
		return fmt.Errorf("failed to marshal restore node: %w", err)
	}

	key := r.nodeInfoKey(restoreNode.ID.String())
	result := r.client.Do(
		ctx,
		r.client.B().Set().Key(key).Value(string(data)).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to register node %s: %w", restoreNode.ID, result.Error())
	}

	return nil
}

func (r *RestoreNodesRegistry) UnregisterNodeFromRegistry(restoreNode RestoreNode) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	infoKey := r.nodeInfoKey(restoreNode.ID.String())
	counterKey := r.activeRestoresKey(restoreNode.ID.String())

	result := r.client.Do(
		ctx,
		r.client.B().Del().Key(infoKey, counterKey).Build(),
	)

	if result.Error() != nil {
		return fmt.Errorf("failed to unregister node %s: %w", restoreNode.ID, result.Error())
	}

	r.logger.Info("Unregistered node from registry", "nodeID", restoreNode.ID)
	return nil
}

func (r *RestoreNodesRegistry) AssignRestoreToNode(
	targetNodeID uuid.UUID,
	restoreID uuid.UUID,
	isCallNotifier bool,
) error {
	ctx := context.Background()

	message := RestoreSubmitMessage{
		NodeID:         targetNodeID,
		RestoreID:      restoreID,
		IsCallNotifier: isCallNotifier,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal restore submit message: %w", err)
	}

	err = r.pubsubRestores.Publish(ctx, r.submitChannel(), string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish restore submit message: %w", err)
	}

	return nil
}

func (r *RestoreNodesRegistry) SubscribeNodeForRestoresAssignment(
	nodeID uuid.UUID,
	handler func(restoreID uuid.UUID, isCallNotifier bool),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg RestoreSubmitMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal restore submit message", "error", err)
			return
		}

		if msg.NodeID != nodeID {
			return
		}

		handler(msg.RestoreID, msg.IsCallNotifier)
	}

	err := r.pubsubRestores.Subscribe(ctx, r.submitChannel(), wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to restore submit channel: %w", err)
	}

	r.logger.Info("Subscribed to restore submit channel", "nodeID", nodeID)
	return nil
}

func (r *RestoreNodesRegistry) UnsubscribeNodeForRestoresAssignments() error {
	err := r.pubsubRestores.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from restore submit channel: %w", err)
	}

	r.logger.Info("Unsubscribed from restore submit channel")
	return nil
}

func (r *RestoreNodesRegistry) PublishRestoreCompletion(
	nodeID uuid.UUID,
	restoreID uuid.UUID,
) error {
	ctx := context.Background()

	message := RestoreCompletionMessage{
		NodeID:    nodeID,
		RestoreID: restoreID,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal restore completion message: %w", err)
	}

	err = r.pubsubCompletions.Publish(ctx, r.completionChannel(), string(messageJSON))
	if err != nil {
		return fmt.Errorf("failed to publish restore completion message: %w", err)
	}

	return nil
}

func (r *RestoreNodesRegistry) SubscribeForRestoresCompletions(
	handler func(nodeID, restoreID uuid.UUID),
) error {
	ctx := context.Background()

	wrappedHandler := func(message string) {
		var msg RestoreCompletionMessage
		if err := json.Unmarshal([]byte(message), &msg); err != nil {
			r.logger.Warn("Failed to unmarshal restore completion message", "error", err)
			return
		}

		handler(msg.NodeID, msg.RestoreID)
	}

	err := r.pubsubCompletions.Subscribe(ctx, r.completionChannel(), wrappedHandler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to restore completion channel: %w", err)
	}

	r.logger.Info("Subscribed to restore completion channel")
	return nil
}

func (r *RestoreNodesRegistry) UnsubscribeForRestoresCompletions() error {
	err := r.pubsubCompletions.Close()
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from restore completion channel: %w", err)
	}

	r.logger.Info("Unsubscribed from restore completion channel")
	return nil
}

func (r *RestoreNodesRegistry) extractNodeIDFromKey(key, prefix, suffix string) uuid.UUID {
	nodeIDStr := strings.TrimPrefix(key, prefix)
	nodeIDStr = strings.TrimSuffix(nodeIDStr, suffix)

	nodeID, err := uuid.Parse(nodeIDStr)
	if err != nil {
		r.logger.Warn("Failed to parse node ID from key", "key", key, "error", err)
		return uuid.Nil
	}

	return nodeID
}

func (r *RestoreNodesRegistry) pipelineGetKeys(keys []string) (map[string][]byte, error) {
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

func (r *RestoreNodesRegistry) parseIntFromBytes(data []byte) (int64, error) {
	str := string(data)
	var count int64
	_, err := fmt.Sscanf(str, "%d", &count)
	if err != nil {
		return 0, fmt.Errorf("failed to parse integer from bytes: %w", err)
	}
	return count, nil
}

func (r *RestoreNodesRegistry) cleanupDeadNodes() error {
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

		var node RestoreNode
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
			statsKey := r.activeRestoresKey(nodeID)

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
// restore-node pool from another in the shared keyspace (empty in production,
// per-worker under parallel tests).

func (r *RestoreNodesRegistry) nodeInfoKey(nodeID string) string {
	return r.namespace + nodeInfoKeyPrefix + nodeID + nodeInfoKeySuffix
}

func (r *RestoreNodesRegistry) activeRestoresKey(nodeID string) string {
	return r.namespace + nodeActiveRestoresPrefix + nodeID + nodeActiveRestoresSuffix
}

func (r *RestoreNodesRegistry) nodeInfoPattern() string {
	return r.namespace + nodeInfoKeyPrefix + "*" + nodeInfoKeySuffix
}

func (r *RestoreNodesRegistry) activeRestoresPattern() string {
	return r.namespace + nodeActiveRestoresPrefix + "*" + nodeActiveRestoresSuffix
}

func (r *RestoreNodesRegistry) submitChannel() string {
	return r.namespace + restoreSubmitChannel
}

func (r *RestoreNodesRegistry) completionChannel() string {
	return r.namespace + restoreCompletionChannel
}
