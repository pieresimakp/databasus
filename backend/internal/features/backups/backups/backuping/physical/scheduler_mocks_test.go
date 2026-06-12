package backuping_physical

import (
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/backups/backups/backuping/nodes"
	billing_models "databasus-backend/internal/features/billing/models"
)

// mockBillingService pins subscription state for cloud-mode scheduler tests.
type mockBillingService struct {
	subscription *billing_models.Subscription
	err          error
}

func (m *mockBillingService) GetSubscription(_ *slog.Logger, _ uuid.UUID) (*billing_models.Subscription, error) {
	return m.subscription, m.err
}

func activeBilling() *mockBillingService {
	return &mockBillingService{
		subscription: &billing_models.Subscription{Status: billing_models.StatusActive},
	}
}

func expiredBilling() *mockBillingService {
	return &mockBillingService{
		subscription: &billing_models.Subscription{Status: billing_models.StatusExpired},
	}
}

func enableCloud(t *testing.T) {
	t.Helper()

	config.GetEnv().IsCloud = true
	t.Cleanup(func() { config.GetEnv().IsCloud = false })
}

// registerFakePhysicalNode heartbeats a node into the physical pool so
// PickLeastBusyNode has a candidate, without starting a real backuper that
// would pick up and run the assignment.
func registerFakePhysicalNode(t *testing.T) uuid.UUID {
	t.Helper()

	node := nodes.BackupNode{ID: uuid.New(), ThroughputMBs: 100, LastHeartbeat: time.Now().UTC()}
	require.NoError(t, physicalBackupNodesRegistry.HearthbeatNodeInRegistry(time.Now().UTC(), node))
	t.Cleanup(func() { _ = physicalBackupNodesRegistry.UnregisterNodeFromRegistry(node) })

	return node.ID
}
