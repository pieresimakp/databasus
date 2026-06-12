package billing_paddle

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/PaddleHQ/paddle-go-sdk"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/audit_logs"
	"databasus-backend/internal/features/billing"
	billing_models "databasus-backend/internal/features/billing/models"
	billing_provider "databasus-backend/internal/features/billing/provider"
	billing_repositories "databasus-backend/internal/features/billing/repositories"
	billing_webhooks "databasus-backend/internal/features/billing/webhooks"
	"databasus-backend/internal/features/databases"
	postgresql_logical "databasus-backend/internal/features/databases/databases/postgresql/logical"
	users_dto "databasus-backend/internal/features/users/dto"
	users_enums "databasus-backend/internal/features/users/enums"
	users_middleware "databasus-backend/internal/features/users/middleware"
	users_services "databasus-backend/internal/features/users/services"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/storage"
	test_utils "databasus-backend/internal/util/testing"
	"databasus-backend/internal/util/tools"
)

const (
	testWebhookSecret = "test-paddle-webhook-secret-key"
	testPriceID       = "pri_test_integration"
)

// ---- Category 1: HTTP Layer ----

func Test_PaddleWebhook_ValidSignature_Returns200(t *testing.T) {
	router, _, db := setupPaddleTest(t)

	expireTrialSubscription(t, db.ID)

	body := buildSubscriptionCreatedJSON(TestSubscriptionCreatedPayload{
		EventID:     "evt-valid-sig-" + uuid.New().String()[:8],
		SubID:       "sub-valid-" + uuid.New().String()[:8],
		CustomerID:  "cus-valid-" + uuid.New().String()[:8],
		DatabaseID:  db.ID.String(),
		QuantityGB:  50,
		PeriodStart: time.Now().UTC(),
		PeriodEnd:   time.Now().UTC().Add(30 * 24 * time.Hour),
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)

	assert.Equal(t, http.StatusOK, resp.Code)
}

func Test_PaddleWebhook_InvalidSignature_Returns401(t *testing.T) {
	router, _, _ := setupPaddleTest(t)

	body := buildSubscriptionCreatedJSON(TestSubscriptionCreatedPayload{
		EventID:     "evt-invalid-sig",
		SubID:       "sub-1",
		CustomerID:  "cus-1",
		DatabaseID:  uuid.New().String(),
		QuantityGB:  50,
		PeriodStart: time.Now().UTC(),
		PeriodEnd:   time.Now().UTC().Add(30 * 24 * time.Hour),
	})

	resp := postPaddleWebhook(router, body, "wrong-secret-key")

	assert.Equal(t, http.StatusUnauthorized, resp.Code)
}

func Test_PaddleWebhook_MalformedJSON_ValidSignature_Returns400(t *testing.T) {
	router, _, _ := setupPaddleTest(t)

	garbageBody := []byte(`{this is not valid json!!!}`)

	resp := postPaddleWebhook(router, garbageBody, testWebhookSecret)

	assert.Equal(t, http.StatusBadRequest, resp.Code)
}

// ---- Category 2: Subscription Created ----

func Test_PaddleWebhook_SubscriptionCreated_CreatesActiveSubscription(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	expireTrialSubscription(t, db.ID)

	subID := "sub-created-" + uuid.New().String()[:8]
	customerID := "cus-created-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	body := buildSubscriptionCreatedJSON(TestSubscriptionCreatedPayload{
		EventID:     "evt-created-" + uuid.New().String()[:8],
		SubID:       subID,
		CustomerID:  customerID,
		DatabaseID:  db.ID.String(),
		QuantityGB:  50,
		PeriodStart: now,
		PeriodEnd:   periodEnd,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)
	assert.Equal(t, 50, sub.StorageGB)
	assert.NotNil(t, sub.ProviderSubID)
	assert.Equal(t, subID, *sub.ProviderSubID)
	assert.NotNil(t, sub.ProviderCustomerID)
	assert.Equal(t, customerID, *sub.ProviderCustomerID)
	assert.WithinDuration(t, periodEnd, sub.CurrentPeriodEnd, 2*time.Second)
}

// ---- Category 3: Subscription Updated ----

func Test_PaddleWebhook_SubscriptionUpdated_WithScheduledChange_ConfirmsUpgrade(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	newPeriodStart := time.Now().UTC()
	newPeriodEnd := newPeriodStart.Add(30 * 24 * time.Hour)

	body := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:            "evt-upgrade-" + uuid.New().String()[:8],
		SubID:              *sub.ProviderSubID,
		CustomerID:         *sub.ProviderCustomerID,
		QuantityGB:         100,
		PeriodStart:        newPeriodStart,
		PeriodEnd:          newPeriodEnd,
		HasScheduledChange: true,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, 100, updated.StorageGB)
	assert.Nil(t, updated.PendingStorageGB)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventUpgraded)
}

func Test_PaddleWebhook_SubscriptionUpdated_WithoutScheduledChange_StartsNewBillingCycle(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	newPeriodStart := time.Now().UTC().Add(30 * 24 * time.Hour)
	newPeriodEnd := newPeriodStart.Add(30 * 24 * time.Hour)

	body := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:            "evt-renewed-" + uuid.New().String()[:8],
		SubID:              *sub.ProviderSubID,
		CustomerID:         *sub.ProviderCustomerID,
		QuantityGB:         50,
		PeriodStart:        newPeriodStart,
		PeriodEnd:          newPeriodEnd,
		HasScheduledChange: false,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, updated.Status)
	assert.WithinDuration(t, newPeriodEnd, updated.CurrentPeriodEnd, 2*time.Second)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventNewBillingCycleStarted)
}

func Test_PaddleWebhook_SubscriptionUpdated_WithoutScheduledChangeAndQuantityChanged_UpgradeConfirmed(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 20)

	newPeriodStart := time.Now().UTC()
	newPeriodEnd := newPeriodStart.Add(30 * 24 * time.Hour)

	body := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:            "evt-immediate-upgrade-" + uuid.New().String()[:8],
		SubID:              *sub.ProviderSubID,
		CustomerID:         *sub.ProviderCustomerID,
		QuantityGB:         50,
		PeriodStart:        newPeriodStart,
		PeriodEnd:          newPeriodEnd,
		HasScheduledChange: false,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, 50, updated.StorageGB)
	assert.Nil(t, updated.PendingStorageGB)
	assert.Equal(t, billing_models.StatusActive, updated.Status)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventUpgraded)
}

func Test_PaddleWebhook_SubscriptionUpdated_WithScheduledCancelChange_CancelsSubscription(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	periodStart := time.Now().UTC()
	periodEnd := periodStart.Add(30 * 24 * time.Hour)

	body := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:               "evt-sched-cancel-" + uuid.New().String()[:8],
		SubID:                 *sub.ProviderSubID,
		CustomerID:            *sub.ProviderCustomerID,
		QuantityGB:            50,
		PeriodStart:           periodStart,
		PeriodEnd:             periodEnd,
		HasScheduledChange:    true,
		ScheduledChangeAction: "cancel",
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, updated.Status)
	assert.NotNil(t, updated.CanceledAt)
	assert.NotNil(t, updated.DataRetentionGracePeriodUntil)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventCanceled)
}

// ---- Category 4: Subscription Reactivated (undo cancel) ----

func Test_PaddleWebhook_SubscriptionUpdated_AfterCancel_ReactivatesSubscription(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 20)

	periodStart := time.Now().UTC()
	periodEnd := periodStart.Add(30 * 24 * time.Hour)

	// Step 1: Cancel subscription via scheduled change
	cancelBody := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:               "evt-sched-cancel-react-" + uuid.New().String()[:8],
		SubID:                 *sub.ProviderSubID,
		CustomerID:            *sub.ProviderCustomerID,
		QuantityGB:            20,
		PeriodStart:           periodStart,
		PeriodEnd:             periodEnd,
		HasScheduledChange:    true,
		ScheduledChangeAction: "cancel",
	})

	cancelResp := postPaddleWebhook(router, cancelBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, cancelResp.Code)

	canceled := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, canceled.Status)
	assert.NotNil(t, canceled.CanceledAt)
	assert.NotNil(t, canceled.DataRetentionGracePeriodUntil)

	// Step 2: Reactivate — user undoes cancellation in Paddle portal.
	// Paddle sends subscription.updated with status=active, scheduled_change=null
	reactivateBody := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:            "evt-reactivate-" + uuid.New().String()[:8],
		SubID:              *sub.ProviderSubID,
		CustomerID:         *sub.ProviderCustomerID,
		QuantityGB:         20,
		PeriodStart:        periodStart,
		PeriodEnd:          periodEnd,
		HasScheduledChange: false,
	})

	reactivateResp := postPaddleWebhook(router, reactivateBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, reactivateResp.Code)

	// Verify subscription is active again with cancellation fields cleared
	reactivated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, reactivated.Status)
	assert.Nil(t, reactivated.CanceledAt)
	assert.Nil(t, reactivated.DataRetentionGracePeriodUntil)
	assert.Equal(t, 20, reactivated.StorageGB)

	// Verify reactivated event was created
	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventCanceled)
	assert.Contains(t, eventTypes, billing_models.EventReactivated)
}

// ---- Category 5: Subscription Canceled ----

func Test_PaddleWebhook_SubscriptionCanceled_SetsStatusCanceled(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	body := buildSubscriptionCanceledJSON(TestSubscriptionCanceledPayload{
		EventID:    "evt-cancel-" + uuid.New().String()[:8],
		SubID:      *sub.ProviderSubID,
		CustomerID: *sub.ProviderCustomerID,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, updated.Status)
	assert.NotNil(t, updated.CanceledAt)
	assert.NotNil(t, updated.DataRetentionGracePeriodUntil)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventCanceled)
}

// ---- Category 6: Payment Events ----

func Test_PaddleWebhook_TransactionCompleted_CreatesInvoice(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	txnID := "txn-completed-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	body := buildTransactionCompletedJSON(TestTransactionCompletedPayload{
		EventID:     "evt-txn-" + uuid.New().String()[:8],
		TxnID:       txnID,
		SubID:       *sub.ProviderSubID,
		CustomerID:  *sub.ProviderCustomerID,
		TotalCents:  2500,
		QuantityGB:  50,
		PeriodStart: now,
		PeriodEnd:   periodEnd,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	invoices := getInvoicesViaAPI(t, router, owner.Token, sub.ID)
	assert.Len(t, invoices, 1)
	assert.Equal(t, int64(2500), invoices[0].AmountCents)
	assert.Equal(t, billing_models.InvoiceStatusPaid, invoices[0].Status)
	assert.NotNil(t, invoices[0].PaidAt)
	assert.Equal(t, 50, invoices[0].StorageGB)
}

func Test_PaddleWebhook_SubscriptionPastDue_SetsPastDue(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	body := buildSubscriptionPastDueJSON(TestSubscriptionPastDuePayload{
		EventID:     "evt-pastdue-" + uuid.New().String()[:8],
		SubID:       *sub.ProviderSubID,
		CustomerID:  *sub.ProviderCustomerID,
		QuantityGB:  50,
		PeriodStart: now,
		PeriodEnd:   periodEnd,
	})

	resp := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp.Code)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusPastDue, updated.Status)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventPastDue)
}

// ---- Category 7: Dispute ----

func Test_PaddleWebhook_AdjustmentCreated_CancelsAndDisputesInvoice(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	sub := activateSubscriptionForPaddle(t, router, owner.Token, db.ID, 50)

	txnID := "txn-dispute-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	// First create an invoice via transaction.completed
	txnBody := buildTransactionCompletedJSON(TestTransactionCompletedPayload{
		EventID:     "evt-txn-dispute-" + uuid.New().String()[:8],
		TxnID:       txnID,
		SubID:       *sub.ProviderSubID,
		CustomerID:  *sub.ProviderCustomerID,
		TotalCents:  1000,
		QuantityGB:  50,
		PeriodStart: now,
		PeriodEnd:   periodEnd,
	})
	txnResp := postPaddleWebhook(router, txnBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, txnResp.Code)

	// Now send adjustment.created referencing the transaction
	adjBody := buildAdjustmentCreatedJSON(
		"evt-adj-"+uuid.New().String()[:8],
		txnID,
	)

	adjResp := postPaddleWebhook(router, adjBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, adjResp.Code)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, updated.Status)
	assert.NotNil(t, updated.CanceledAt)
	assert.Nil(t, updated.DataRetentionGracePeriodUntil)

	invoices := getInvoicesViaAPI(t, router, owner.Token, sub.ID)
	assert.Len(t, invoices, 1)
	assert.Equal(t, billing_models.InvoiceStatusDisputed, invoices[0].Status)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventDispute)
}

// ---- Category 8: Edge Cases ----

func Test_PaddleWebhook_UnhandledEventType_Returns200AndSkipped(t *testing.T) {
	router, _, _ := setupPaddleTest(t)

	payload := map[string]any{
		"event_id":   "evt-unknown-" + uuid.New().String()[:8],
		"event_type": "customer.created",
		"data":       map[string]any{"id": "cus-unknown"},
	}

	body, _ := json.Marshal(payload)

	resp := postPaddleWebhook(router, body, testWebhookSecret)

	assert.Equal(t, http.StatusOK, resp.Code)
}

func Test_PaddleWebhook_SubscriptionCreated_MissingDatabaseID_Returns500(t *testing.T) {
	router, _, _ := setupPaddleTest(t)

	payload := map[string]any{
		"event_id":   "evt-no-db-" + uuid.New().String()[:8],
		"event_type": "subscription.created",
		"data": map[string]any{
			"id":          "sub-no-db-" + uuid.New().String()[:8],
			"customer_id": "cus-no-db-" + uuid.New().String()[:8],
			"status":      "active",
			"items": []map[string]any{{
				"quantity": 50,
				"price":    map[string]any{"id": testPriceID},
			}},
			"current_billing_period": map[string]any{
				"starts_at": time.Now().UTC().Format(time.RFC3339),
				"ends_at":   time.Now().UTC().Add(30 * 24 * time.Hour).Format(time.RFC3339),
			},
			"custom_data": map[string]any{},
		},
	}

	body, _ := json.Marshal(payload)

	resp := postPaddleWebhook(router, body, testWebhookSecret)

	assert.Equal(t, http.StatusInternalServerError, resp.Code)
}

// ---- Category 9: Idempotency ----

func Test_PaddleWebhook_DuplicateEventID_Returns200AndProcessedOnce(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	expireTrialSubscription(t, db.ID)

	eventID := "evt-dup-" + uuid.New().String()[:8]
	subID := "sub-dup-" + uuid.New().String()[:8]

	body := buildSubscriptionCreatedJSON(TestSubscriptionCreatedPayload{
		EventID:     eventID,
		SubID:       subID,
		CustomerID:  "cus-dup-" + uuid.New().String()[:8],
		DatabaseID:  db.ID.String(),
		QuantityGB:  50,
		PeriodStart: time.Now().UTC(),
		PeriodEnd:   time.Now().UTC().Add(30 * 24 * time.Hour),
	})

	resp1 := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp1.Code)

	resp2 := postPaddleWebhook(router, body, testWebhookSecret)
	assert.Equal(t, http.StatusOK, resp2.Code)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)
}

// ---- Category 10: Full Lifecycle ----

func Test_PaddleWebhook_FullLifecycle_CreateToUpgradeToPaymentToCancelViaHTTP(t *testing.T) {
	router, owner, db := setupPaddleTest(t)

	expireTrialSubscription(t, db.ID)

	subID := "sub-lifecycle-" + uuid.New().String()[:8]
	customerID := "cus-lifecycle-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	// Step 1: Create subscription via paddle webhook
	createBody := buildSubscriptionCreatedJSON(TestSubscriptionCreatedPayload{
		EventID:     "evt-lc-create-" + uuid.New().String()[:8],
		SubID:       subID,
		CustomerID:  customerID,
		DatabaseID:  db.ID.String(),
		QuantityGB:  50,
		PeriodStart: now,
		PeriodEnd:   periodEnd,
	})
	createResp := postPaddleWebhook(router, createBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, createResp.Code)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)
	assert.Equal(t, 50, sub.StorageGB)

	// Step 2: Upgrade storage via subscription.updated webhook
	upgradeBody := buildSubscriptionUpdatedJSON(TestSubscriptionUpdatedPayload{
		EventID:            "evt-lc-upgrade-" + uuid.New().String()[:8],
		SubID:              subID,
		CustomerID:         customerID,
		QuantityGB:         100,
		PeriodStart:        now,
		PeriodEnd:          periodEnd,
		HasScheduledChange: true,
	})
	upgradeResp := postPaddleWebhook(router, upgradeBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, upgradeResp.Code)

	sub = getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, 100, sub.StorageGB)

	// Step 3: Record payment via transaction.completed webhook
	txnID := "txn-lc-" + uuid.New().String()[:8]
	payBody := buildTransactionCompletedJSON(TestTransactionCompletedPayload{
		EventID:     "evt-lc-pay-" + uuid.New().String()[:8],
		TxnID:       txnID,
		SubID:       subID,
		CustomerID:  customerID,
		TotalCents:  5000,
		QuantityGB:  100,
		PeriodStart: now,
		PeriodEnd:   periodEnd,
	})
	payResp := postPaddleWebhook(router, payBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, payResp.Code)

	invoices := getInvoicesViaAPI(t, router, owner.Token, sub.ID)
	assert.Len(t, invoices, 1)
	assert.Equal(t, int64(5000), invoices[0].AmountCents)
	assert.Equal(t, 100, invoices[0].StorageGB)

	// Step 4: Cancel subscription via webhook
	cancelBody := buildSubscriptionCanceledJSON(TestSubscriptionCanceledPayload{
		EventID:    "evt-lc-cancel-" + uuid.New().String()[:8],
		SubID:      subID,
		CustomerID: customerID,
	})
	cancelResp := postPaddleWebhook(router, cancelBody, testWebhookSecret)
	assert.Equal(t, http.StatusOK, cancelResp.Code)

	sub = refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, sub.Status)
	assert.NotNil(t, sub.CanceledAt)

	// Verify all events
	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventCreated)
	assert.Contains(t, eventTypes, billing_models.EventUpgraded)
	assert.Contains(t, eventTypes, billing_models.EventCanceled)
}

// ---- Private Helpers ----

type mockPaddleTestProvider struct{}

func (m *mockPaddleTestProvider) GetProviderName() billing_provider.ProviderName {
	return "mock-paddle-test"
}

func (m *mockPaddleTestProvider) CreateCheckoutSession(
	_ *slog.Logger,
	_ billing_provider.CheckoutRequest,
) (string, error) {
	return "https://checkout.test/mock", nil
}

func (m *mockPaddleTestProvider) UpgradeQuantityWithSurcharge(_ *slog.Logger, _ string, _ int) error {
	return nil
}

func (m *mockPaddleTestProvider) ScheduleQuantityDowngradeFromNextBillingCycle(_ *slog.Logger, _ string, _ int) error {
	return nil
}

func (m *mockPaddleTestProvider) GetSubscription(
	_ *slog.Logger,
	_ string,
) (billing_provider.ProviderSubscription, error) {
	return billing_provider.ProviderSubscription{}, nil
}

func (m *mockPaddleTestProvider) CreatePortalSession(_ *slog.Logger, _, _ string) (string, error) {
	return "https://portal.test/mock", nil
}

func setupPaddleTest(t *testing.T) (*gin.Engine, *users_dto.SignInResponseDTO, *databases.Database) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	router := gin.New()

	v1 := router.Group("/api/v1")
	protected := v1.Group("").Use(users_middleware.AuthMiddleware(users_services.GetUserService()))

	workspaces_controllers.GetWorkspaceController().RegisterRoutes(protected.(*gin.RouterGroup))
	workspaces_controllers.GetMembershipController().RegisterRoutes(protected.(*gin.RouterGroup))
	databases.GetDatabaseController().RegisterRoutes(protected.(*gin.RouterGroup))
	billing.GetBillingController().RegisterRoutes(protected.(*gin.RouterGroup))

	billing.SetupDependencies()
	audit_logs.SetupDependencies()

	// Set mock provider on the singleton BEFORE copying it for the paddle service
	billing.GetBillingService().SetBillingProvider(&mockPaddleTestProvider{})

	paddleSvc := &PaddleBillingService{
		nil,
		paddle.NewWebhookVerifier(testWebhookSecret),
		testPriceID,
		billing_webhooks.WebhookRepository{},
		billing.GetBillingService(),
	}

	paddleCtrl := &PaddleBillingController{paddleSvc}
	paddleCtrl.RegisterPublicRoutes(v1)

	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("paddle-test-"+uuid.New().String()[:8], owner, router)

	db := createTestDatabaseForPaddle(owner.Token, workspace.ID, router)

	config.GetEnv().IsCloud = true
	t.Cleanup(func() {
		config.GetEnv().IsCloud = false
		cleanupBillingTestData(db.ID)
		databases.RemoveTestDatabase(db)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	})

	return router, owner, db
}

func createTestDatabaseForPaddle(token string, workspaceID uuid.UUID, router *gin.Engine) *databases.Database {
	env := config.GetEnv()
	port, err := strconv.Atoi(env.TestLogicalPostgres16Port)
	if err != nil {
		panic(fmt.Sprintf("failed to parse TEST_LOGICAL_POSTGRES_16_PORT: %v", err))
	}

	dbName := "testdb"
	request := databases.Database{
		Name:        "paddle-test-" + uuid.New().String()[:8],
		WorkspaceID: &workspaceID,
		Type:        databases.DatabaseTypePostgresLogical,
		PostgresqlLogical: &postgresql_logical.PostgresqlLogicalDatabase{
			Version:  tools.PostgresqlVersion16,
			Host:     env.TestLocalhost,
			Port:     port,
			Username: "testuser",
			Password: "testpassword",
			Database: &dbName,
			CpuCount: 1,
		},
	}

	w := workspaces_testing.MakeAPIRequest(router, "POST", "/api/v1/databases/create", "Bearer "+token, request)
	if w.Code != http.StatusCreated {
		panic(fmt.Sprintf("failed to create database for paddle test. Status: %d, Body: %s", w.Code, w.Body.String()))
	}

	var db databases.Database
	if err := json.Unmarshal(w.Body.Bytes(), &db); err != nil {
		panic(err)
	}

	return &db
}

func computePaddleSignature(body []byte, secret string) string {
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte(":"))
	mac.Write(body)
	h1 := hex.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("ts=%s;h1=%s", timestamp, h1)
}

func postPaddleWebhook(router *gin.Engine, body []byte, secret string) *httptest.ResponseRecorder {
	signature := computePaddleSignature(body, secret)

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/billing/paddle/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Paddle-Signature", signature)

	router.ServeHTTP(recorder, req)

	return recorder
}

func buildSubscriptionCreatedJSON(params TestSubscriptionCreatedPayload) []byte {
	payload := map[string]any{
		"event_id":   params.EventID,
		"event_type": "subscription.created",
		"data": map[string]any{
			"id":          params.SubID,
			"customer_id": params.CustomerID,
			"status":      "active",
			"items": []map[string]any{{
				"quantity": params.QuantityGB,
				"price":    map[string]any{"id": testPriceID},
			}},
			"current_billing_period": map[string]any{
				"starts_at": params.PeriodStart.Format(time.RFC3339),
				"ends_at":   params.PeriodEnd.Format(time.RFC3339),
			},
			"custom_data": map[string]any{
				"database_id": params.DatabaseID,
			},
		},
	}

	body, _ := json.Marshal(payload)

	return body
}

func buildSubscriptionUpdatedJSON(params TestSubscriptionUpdatedPayload) []byte {
	data := map[string]any{
		"id":          params.SubID,
		"customer_id": params.CustomerID,
		"status":      "active",
		"items": []map[string]any{{
			"quantity": params.QuantityGB,
			"price":    map[string]any{"id": testPriceID},
		}},
		"current_billing_period": map[string]any{
			"starts_at": params.PeriodStart.Format(time.RFC3339),
			"ends_at":   params.PeriodEnd.Format(time.RFC3339),
		},
	}

	if params.HasScheduledChange {
		action := params.ScheduledChangeAction
		if action == "" {
			action = "none"
		}

		data["scheduled_change"] = map[string]any{
			"action":       action,
			"effective_at": params.PeriodEnd.Format(time.RFC3339),
		}
	}

	payload := map[string]any{
		"event_id":   params.EventID,
		"event_type": "subscription.updated",
		"data":       data,
	}

	body, _ := json.Marshal(payload)

	return body
}

func buildSubscriptionCanceledJSON(params TestSubscriptionCanceledPayload) []byte {
	payload := map[string]any{
		"event_id":   params.EventID,
		"event_type": "subscription.canceled",
		"data": map[string]any{
			"id":          params.SubID,
			"customer_id": params.CustomerID,
			"status":      "canceled",
		},
	}

	body, _ := json.Marshal(payload)

	return body
}

func buildTransactionCompletedJSON(params TestTransactionCompletedPayload) []byte {
	payload := map[string]any{
		"event_id":   params.EventID,
		"event_type": "transaction.completed",
		"data": map[string]any{
			"id":              params.TxnID,
			"subscription_id": params.SubID,
			"customer_id":     params.CustomerID,
			"items": []map[string]any{{
				"quantity": params.QuantityGB,
				"price":    map[string]any{"id": testPriceID},
			}},
			"details": map[string]any{
				"totals": map[string]any{
					"total": strconv.FormatInt(params.TotalCents, 10),
				},
			},
			"billing_period": map[string]any{
				"starts_at": params.PeriodStart.Format(time.RFC3339),
				"ends_at":   params.PeriodEnd.Format(time.RFC3339),
			},
		},
	}

	body, _ := json.Marshal(payload)

	return body
}

func buildSubscriptionPastDueJSON(params TestSubscriptionPastDuePayload) []byte {
	payload := map[string]any{
		"event_id":   params.EventID,
		"event_type": "subscription.past_due",
		"data": map[string]any{
			"id":          params.SubID,
			"customer_id": params.CustomerID,
			"status":      "past_due",
			"items": []map[string]any{{
				"quantity": params.QuantityGB,
				"price":    map[string]any{"id": testPriceID},
			}},
			"current_billing_period": map[string]any{
				"starts_at": params.PeriodStart.Format(time.RFC3339),
				"ends_at":   params.PeriodEnd.Format(time.RFC3339),
			},
		},
	}

	body, _ := json.Marshal(payload)

	return body
}

func buildAdjustmentCreatedJSON(eventID, transactionID string) []byte {
	payload := map[string]any{
		"event_id":   eventID,
		"event_type": "adjustment.created",
		"data": map[string]any{
			"transaction_id": transactionID,
		},
	}

	body, _ := json.Marshal(payload)

	return body
}

func activateSubscriptionForPaddle(
	t *testing.T,
	router *gin.Engine,
	token string,
	databaseID uuid.UUID,
	storageGB int,
) *billing_models.Subscription {
	t.Helper()

	expireTrialSubscription(t, databaseID)

	providerSubID := "sub-" + uuid.New().String()
	providerCustomerID := "cus-" + uuid.New().String()
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	err := billing.GetBillingService().ActivateSubscription(slog.Default(), billing_models.WebhookEvent{
		RequestID:              uuid.New(),
		ProviderEventID:        "evt-" + uuid.New().String(),
		DatabaseID:             &databaseID,
		ProviderSubscriptionID: providerSubID,
		ProviderCustomerID:     providerCustomerID,
		QuantityGB:             storageGB,
		PeriodStart:            &now,
		PeriodEnd:              &periodEnd,
	})
	if err != nil {
		t.Fatalf("activateSubscriptionForPaddle: %v", err)
	}

	return getSubscriptionViaAPI(t, router, token, databaseID)
}

func expireTrialSubscription(t *testing.T, databaseID uuid.UUID) {
	t.Helper()

	subRepo := billing_repositories.SubscriptionRepository{}
	subs, err := subRepo.FindByDatabaseIDAndStatuses(
		databaseID,
		[]billing_models.SubscriptionStatus{
			billing_models.StatusTrial,
			billing_models.StatusActive,
			billing_models.StatusPastDue,
		},
	)
	if err != nil || len(subs) == 0 {
		return
	}

	for _, sub := range subs {
		sub.Status = billing_models.StatusExpired
		sub.UpdatedAt = time.Now().UTC()
		if saveErr := subRepo.Save(*sub); saveErr != nil {
			t.Fatalf("expireTrialSubscription: save failed: %v", saveErr)
		}
	}
}

func getSubscriptionViaAPI(
	t *testing.T,
	router *gin.Engine,
	token string,
	databaseID uuid.UUID,
) *billing_models.Subscription {
	t.Helper()

	var sub billing_models.Subscription
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/"+databaseID.String(),
		"Bearer "+token,
		http.StatusOK,
		&sub,
	)

	return &sub
}

func getEventsViaAPI(
	t *testing.T,
	router *gin.Engine,
	token string,
	subscriptionID uuid.UUID,
) []*billing_models.SubscriptionEvent {
	t.Helper()

	var response billing.GetSubscriptionEventsResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/events/"+subscriptionID.String(),
		"Bearer "+token,
		http.StatusOK,
		&response,
	)

	return response.Events
}

func getInvoicesViaAPI(
	t *testing.T,
	router *gin.Engine,
	token string,
	subscriptionID uuid.UUID,
) []*billing_models.Invoice {
	t.Helper()

	var response billing.GetInvoicesResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/invoices/"+subscriptionID.String(),
		"Bearer "+token,
		http.StatusOK,
		&response,
	)

	return response.Invoices
}

func refreshSubscription(t *testing.T, subID uuid.UUID) *billing_models.Subscription {
	t.Helper()

	subRepo := billing_repositories.SubscriptionRepository{}
	sub, err := subRepo.FindByID(subID)
	if err != nil || sub == nil {
		t.Fatalf("refreshSubscription: not found: %v", err)
	}

	return sub
}

func cleanupBillingTestData(databaseID uuid.UUID) {
	db := storage.GetDb()

	db.Exec(
		`DELETE FROM invoices WHERE subscription_id IN (SELECT id FROM subscriptions WHERE database_id = ?)`,
		databaseID,
	)
	db.Exec(
		`DELETE FROM subscription_events WHERE subscription_id IN (SELECT id FROM subscriptions WHERE database_id = ?)`,
		databaseID,
	)
	db.Exec(`DELETE FROM subscriptions WHERE database_id = ?`, databaseID)
}

func extractEventTypes(events []*billing_models.SubscriptionEvent) []billing_models.SubscriptionEventType {
	types := make([]billing_models.SubscriptionEventType, len(events))
	for i, e := range events {
		types[i] = e.Type
	}

	return types
}
