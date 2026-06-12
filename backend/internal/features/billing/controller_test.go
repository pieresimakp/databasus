package billing

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"databasus-backend/internal/config"
	"databasus-backend/internal/features/audit_logs"
	billing_models "databasus-backend/internal/features/billing/models"
	billing_provider "databasus-backend/internal/features/billing/provider"
	"databasus-backend/internal/features/databases"
	postgresql_logical "databasus-backend/internal/features/databases/databases/postgresql/logical"
	users_dto "databasus-backend/internal/features/users/dto"
	users_enums "databasus-backend/internal/features/users/enums"
	users_middleware "databasus-backend/internal/features/users/middleware"
	users_services "databasus-backend/internal/features/users/services"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_models "databasus-backend/internal/features/workspaces/models"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	"databasus-backend/internal/storage"
	test_utils "databasus-backend/internal/util/testing"
	"databasus-backend/internal/util/tools"
)

// ---- Category 1: Trial Subscription ----

func Test_CreateDatabase_WhenCloudEnabled_TrialSubscriptionCreated(t *testing.T) {
	router, owner, _, db := setupBillingTestWithoutProvider(t)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)

	assert.Equal(t, billing_models.StatusTrial, sub.Status)
	assert.Equal(t, config.GetEnv().TrialStorageGB, sub.StorageGB)
	assert.Nil(t, sub.ProviderSubID)
	assert.Nil(t, sub.ProviderCustomerID)
	assert.WithinDuration(t, time.Now().UTC().Add(config.GetEnv().TrialDuration), sub.CurrentPeriodEnd, 5*time.Second)
}

func Test_GetSubscription_WhenTrialActive_ReturnsTrialSubscription(t *testing.T) {
	router, owner, _, db := setupBillingTestWithoutProvider(t)

	var sub billing_models.Subscription
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/"+db.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&sub,
	)

	assert.Equal(t, billing_models.StatusTrial, sub.Status)
	assert.Equal(t, config.GetEnv().TrialStorageGB, sub.StorageGB)
}

// ---- Category 2: CreateSubscription Endpoint ----

func Test_CreateSubscription_ValidRequest_ReturnsCheckoutURL(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	expireActiveSubscription(t, db.ID)

	var resp CreateSubscriptionResponse
	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription",
		"Bearer "+owner.Token,
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusOK,
		&resp,
	)

	assert.Equal(t, "txn_mock_test_123", resp.PaddleTransactionID)
}

func Test_CreateSubscription_StorageBelowMinimum_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	expireActiveSubscription(t, db.ID)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription",
		"Bearer "+owner.Token,
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 10},
		http.StatusInternalServerError,
	)
}

func Test_CreateSubscription_StorageAboveMaximum_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	expireActiveSubscription(t, db.ID)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription",
		"Bearer "+owner.Token,
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 15000},
		http.StatusInternalServerError,
	)
}

func Test_CreateSubscription_WhenTrialExists_ReturnsCheckoutURL(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	// Trial is active — should allow checkout without expiring the trial yet
	trialSub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusTrial, trialSub.Status)

	var resp CreateSubscriptionResponse
	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription",
		"Bearer "+owner.Token,
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusOK,
		&resp,
	)

	assert.Equal(t, "txn_mock_test_123", resp.PaddleTransactionID)

	// Trial should still be active until paid subscription activates via webhook
	stillTrial := refreshSubscription(t, trialSub.ID)
	assert.Equal(t, billing_models.StatusTrial, stillTrial.Status)
}

func Test_ActivateSubscription_WhenTrialExists_ExpiresTrialAndActivatesPaid(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	// Trial is active
	trialSub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusTrial, trialSub.Status)

	// Activate paid subscription via webhook — should expire the trial
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)
	providerSubID := "sub-" + uuid.New().String()

	err := billingService.ActivateSubscription(slog.Default(), billing_models.WebhookEvent{
		RequestID:              uuid.New(),
		ProviderEventID:        "evt-" + uuid.New().String(),
		DatabaseID:             &db.ID,
		ProviderSubscriptionID: providerSubID,
		ProviderCustomerID:     "cus-" + uuid.New().String(),
		QuantityGB:             50,
		PeriodStart:            &now,
		PeriodEnd:              &periodEnd,
	})
	assert.NoError(t, err)

	// Verify trial was expired
	expiredTrial := refreshSubscription(t, trialSub.ID)
	assert.Equal(t, billing_models.StatusExpired, expiredTrial.Status)
	assert.NotNil(t, expiredTrial.CanceledAt)

	// Verify paid subscription is active
	activeSub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, activeSub.Status)
	assert.Equal(t, 50, activeSub.StorageGB)
}

func Test_CreateSubscription_ActiveSubscriptionExists_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	// Activate a real paid subscription
	activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	// Should block new subscription for non-trial active sub
	resp := test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription",
		"Bearer "+owner.Token,
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusInternalServerError,
	)

	assert.Contains(t, string(resp.Body), "Failed to create subscription")
}

func Test_CreateSubscription_UserWithoutAccess_ReturnsError(t *testing.T) {
	router, _, _, db, _ := setupBillingTest(t)

	otherUser := users_testing.CreateTestUser(users_enums.UserRoleMember)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription",
		"Bearer "+otherUser.Token,
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusInternalServerError,
	)
}

func Test_CreateSubscription_Unauthenticated_Returns401(t *testing.T) {
	router, _, _, db, _ := setupBillingTest(t)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription",
		"",
		CreateSubscriptionRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusUnauthorized,
	)
}

// ---- Category 3: GetSubscription Endpoint ----

func Test_GetSubscription_ActiveSubscription_ReturnsSubscription(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	activeSub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	var sub billing_models.Subscription
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/"+db.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&sub,
	)

	assert.Equal(t, activeSub.ID, sub.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)
	assert.Equal(t, 50, sub.StorageGB)
}

func Test_GetSubscription_UserWithoutAccess_ReturnsError(t *testing.T) {
	router, _, _, db, _ := setupBillingTest(t)

	otherUser := users_testing.CreateTestUser(users_enums.UserRoleMember)

	test_utils.MakeGetRequest(
		t, router,
		"/api/v1/billing/subscription/"+db.ID.String(),
		"Bearer "+otherUser.Token,
		http.StatusInternalServerError,
	)
}

func Test_GetSubscription_NonExistentDatabase_ReturnsError(t *testing.T) {
	router, owner, _, _, _ := setupBillingTest(t)

	test_utils.MakeGetRequest(
		t, router,
		"/api/v1/billing/subscription/"+uuid.New().String(),
		"Bearer "+owner.Token,
		http.StatusInternalServerError,
	)
}

// ---- Category 4: ChangeStorage Endpoint ----

func Test_ChangeStorage_WhenUpgrading_ReturnsImmediateMode(t *testing.T) {
	router, owner, _, db, mock := setupBillingTest(t)

	activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	var resp ChangeStorageResponse
	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/change-storage",
		"Bearer "+owner.Token,
		ChangeStorageRequest{DatabaseID: db.ID, StorageGB: 100},
		http.StatusOK,
		&resp,
	)

	assert.Equal(t, ChangeStorageApplyImmediate, resp.ApplyMode)
	assert.Equal(t, 50, resp.CurrentGB)
	assert.NotNil(t, resp.PendingGB)
	assert.Equal(t, 100, *resp.PendingGB)
	assert.Equal(t, 100, mock.upgradeCalledWithGB)
}

func Test_ChangeStorage_WhenDowngrading_ReturnsNextCycleMode(t *testing.T) {
	router, owner, _, db, mock := setupBillingTest(t)

	activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 100)

	var resp ChangeStorageResponse
	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/change-storage",
		"Bearer "+owner.Token,
		ChangeStorageRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusOK,
		&resp,
	)

	assert.Equal(t, ChangeStorageApplyNextCycle, resp.ApplyMode)
	assert.Equal(t, 100, resp.CurrentGB)
	assert.NotNil(t, resp.PendingGB)
	assert.Equal(t, 50, *resp.PendingGB)
	assert.Equal(t, 50, mock.scheduleDowngradeCalledWithGB)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.NotNil(t, sub.PendingStorageGB)
	assert.Equal(t, 50, *sub.PendingStorageGB)
}

func Test_ChangeStorage_WhenSameAsCurrentStorage_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription/change-storage",
		"Bearer "+owner.Token,
		ChangeStorageRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusInternalServerError,
	)
}

func Test_ChangeStorage_BelowMinimum_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription/change-storage",
		"Bearer "+owner.Token,
		ChangeStorageRequest{DatabaseID: db.ID, StorageGB: 5},
		http.StatusInternalServerError,
	)
}

func Test_ChangeStorage_WhenNoActiveSubscription_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	expireActiveSubscription(t, db.ID)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription/change-storage",
		"Bearer "+owner.Token,
		ChangeStorageRequest{DatabaseID: db.ID, StorageGB: 50},
		http.StatusInternalServerError,
	)
}

// ---- Category 5: GetPortalSession Endpoint ----

func Test_GetPortalSession_WhenSubscriptionActive_ReturnsPortalURL(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	var resp GetPortalSessionResponse
	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/portal/"+sub.ID.String(),
		"Bearer "+owner.Token,
		nil,
		http.StatusOK,
		&resp,
	)

	assert.Equal(t, "https://portal.test/mock-portal", resp.PortalURL)
}

func Test_GetPortalSession_WhenSubscriptionCanceled_ReturnsPortalURL(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	err := billingService.CancelSubscription(slog.Default(), sub, makeWebhookEvent(db.ID, "evt-cancel-portal", ""))
	assert.NoError(t, err)

	var resp GetPortalSessionResponse
	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/portal/"+sub.ID.String(),
		"Bearer "+owner.Token,
		nil,
		http.StatusOK,
		&resp,
	)

	assert.Equal(t, "https://portal.test/mock-portal", resp.PortalURL)
}

func Test_GetPortalSession_WhenSubscriptionExpired_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	sub.Status = billing_models.StatusExpired
	sub.UpdatedAt = time.Now().UTC()
	err := billingService.subscriptionRepository.Save(*sub)
	assert.NoError(t, err)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription/portal/"+sub.ID.String(),
		"Bearer "+owner.Token,
		nil,
		http.StatusInternalServerError,
	)
}

func Test_GetPortalSession_UserWithoutAccess_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	otherUser := users_testing.CreateTestUser(users_enums.UserRoleMember)

	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription/portal/"+sub.ID.String(),
		"Bearer "+otherUser.Token,
		nil,
		http.StatusInternalServerError,
	)
}

// ---- Category 6: GetSubscriptionEvents Endpoint ----

func Test_GetSubscriptionEvents_AfterMultipleChanges_ReturnsAllEvents(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	_ = billingService.RecordPaymentFailed(log, sub, makeWebhookEvent(db.ID, "evt-fail-1", ""))
	_ = billingService.RecordPaymentSuccess(
		log,
		sub,
		makeWebhookEvent(db.ID, "evt-pay-1", "inv-"+uuid.New().String()[:8]),
	)

	var response GetSubscriptionEventsResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/events/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&response,
	)

	assert.GreaterOrEqual(t, len(response.Events), 3)
	assert.GreaterOrEqual(t, response.Total, int64(3))

	eventTypes := extractEventTypes(response.Events)

	assert.Contains(t, eventTypes, billing_models.EventCreated)
	assert.Contains(t, eventTypes, billing_models.EventPastDue)
	assert.Contains(t, eventTypes, billing_models.EventRecoveredFromPastDue)
}

func Test_GetSubscriptionEvents_UserWithoutAccess_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	otherUser := users_testing.CreateTestUser(users_enums.UserRoleMember)

	test_utils.MakeGetRequest(
		t, router,
		"/api/v1/billing/subscription/events/"+sub.ID.String(),
		"Bearer "+otherUser.Token,
		http.StatusInternalServerError,
	)
}

func Test_GetSubscriptionEvents_WithPagination_ReturnsCorrectPage(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	_ = billingService.RecordPaymentFailed(log, sub, makeWebhookEvent(db.ID, "evt-pag-fail-1", ""))
	_ = billingService.RecordPaymentSuccess(
		log,
		sub,
		makeWebhookEvent(db.ID, "evt-pag-pay-1", "inv-"+uuid.New().String()[:8]),
	)

	var fullResponse GetSubscriptionEventsResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/events/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&fullResponse,
	)

	totalEvents := fullResponse.Total
	assert.GreaterOrEqual(t, totalEvents, int64(3))

	var paginatedResponse GetSubscriptionEventsResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/events/"+sub.ID.String()+"?limit=2&offset=1",
		"Bearer "+owner.Token,
		http.StatusOK,
		&paginatedResponse,
	)

	assert.Len(t, paginatedResponse.Events, 2)
	assert.Equal(t, totalEvents, paginatedResponse.Total)
	assert.Equal(t, 2, paginatedResponse.Limit)
	assert.Equal(t, 1, paginatedResponse.Offset)
}

// ---- Category 7: GetInvoices Endpoint ----

func Test_GetInvoices_AfterPayment_ReturnsInvoice(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	invoiceID := "inv-test-" + uuid.New().String()[:8]
	evt := makePaymentWebhookEvent(invoiceID, 50, 500)
	err := billingService.RecordPaymentSuccess(log, sub, evt)
	assert.NoError(t, err)

	var response GetInvoicesResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/invoices/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&response,
	)

	assert.Len(t, response.Invoices, 1)
	assert.Equal(t, int64(1), response.Total)
	assert.Equal(t, int64(500), response.Invoices[0].AmountCents)
	assert.Equal(t, 50, response.Invoices[0].StorageGB)
	assert.Equal(t, billing_models.InvoiceStatusPaid, response.Invoices[0].Status)
	assert.NotNil(t, response.Invoices[0].PaidAt)
}

func Test_GetInvoices_WhenDuplicatePayment_ReturnsOnlyOneInvoice(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	evt := makePaymentWebhookEvent("inv-dup-"+uuid.New().String()[:8], 50, 500)

	_ = billingService.RecordPaymentSuccess(log, sub, evt)
	_ = billingService.RecordPaymentSuccess(log, sub, evt)

	var response GetInvoicesResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/invoices/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&response,
	)

	assert.Len(t, response.Invoices, 1)
}

func Test_GetInvoices_UserWithoutAccess_ReturnsError(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	otherUser := users_testing.CreateTestUser(users_enums.UserRoleMember)

	test_utils.MakeGetRequest(
		t, router,
		"/api/v1/billing/subscription/invoices/"+sub.ID.String(),
		"Bearer "+otherUser.Token,
		http.StatusInternalServerError,
	)
}

func Test_GetInvoices_WithPagination_ReturnsCorrectPage(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	for i := range 3 {
		invoiceID := fmt.Sprintf("inv-pag-%d-%s", i, uuid.New().String()[:8])
		evt := makePaymentWebhookEvent(invoiceID, 50, int64(500+i*100))
		err := billingService.RecordPaymentSuccess(log, sub, evt)
		assert.NoError(t, err)
	}

	var fullResponse GetInvoicesResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/invoices/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&fullResponse,
	)

	assert.Equal(t, int64(3), fullResponse.Total)
	assert.Len(t, fullResponse.Invoices, 3)

	var paginatedResponse GetInvoicesResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/invoices/"+sub.ID.String()+"?limit=2&offset=1",
		"Bearer "+owner.Token,
		http.StatusOK,
		&paginatedResponse,
	)

	assert.Len(t, paginatedResponse.Invoices, 2)
	assert.Equal(t, int64(3), paginatedResponse.Total)
	assert.Equal(t, 2, paginatedResponse.Limit)
	assert.Equal(t, 1, paginatedResponse.Offset)
}

// ---- Category 8: Webhook Handlers ----

func Test_ActivateSubscription_ValidEvent_CreatesActiveSubscription(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	expireActiveSubscription(t, db.ID)

	providerSubID := "sub-activate-test-" + uuid.New().String()
	providerCustomerID := "cus-activate-test-" + uuid.New().String()

	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)
	err := billingService.ActivateSubscription(log, billing_models.WebhookEvent{
		RequestID:              uuid.New(),
		ProviderEventID:        "evt-created-1",
		DatabaseID:             &db.ID,
		ProviderSubscriptionID: providerSubID,
		ProviderCustomerID:     providerCustomerID,
		QuantityGB:             50,
		PeriodStart:            &now,
		PeriodEnd:              &periodEnd,
	})

	assert.NoError(t, err)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)
	assert.Equal(t, 50, sub.StorageGB)
	assert.NotNil(t, sub.ProviderSubID)
	assert.Equal(t, providerSubID, *sub.ProviderSubID)
	assert.NotNil(t, sub.ProviderCustomerID)
	assert.Equal(t, providerCustomerID, *sub.ProviderCustomerID)
}

func Test_ActivateSubscription_WhenDuplicate_SkipsIdempotently(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	expireActiveSubscription(t, db.ID)

	providerSubID := "sub-idempotent-" + uuid.New().String()
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)
	evt := billing_models.WebhookEvent{
		RequestID:              uuid.New(),
		ProviderEventID:        "evt-dup-created",
		DatabaseID:             &db.ID,
		ProviderSubscriptionID: providerSubID,
		ProviderCustomerID:     "cus-dup-test",
		QuantityGB:             50,
		PeriodStart:            &now,
		PeriodEnd:              &periodEnd,
	}

	err1 := billingService.ActivateSubscription(log, evt)
	err2 := billingService.ActivateSubscription(log, evt)

	assert.NoError(t, err1)
	assert.NoError(t, err2)

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)
}

func Test_SyncSubscriptionFromProvider_WhenSameQuantity_StartsNewBillingCycle(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	originalStorage := sub.StorageGB

	newStart := time.Now().UTC().Add(30 * 24 * time.Hour)
	newEnd := newStart.Add(30 * 24 * time.Hour)
	err := billingService.SyncSubscriptionFromProvider(log, sub, billing_models.WebhookEvent{
		RequestID:       uuid.New(),
		ProviderEventID: "evt-renewed-1",
		QuantityGB:      originalStorage,
		Status:          billing_models.StatusActive,
		PeriodStart:     &newStart,
		PeriodEnd:       &newEnd,
	})

	assert.NoError(t, err)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, updated.Status)
	assert.Equal(t, originalStorage, updated.StorageGB)
	assert.Nil(t, updated.PendingStorageGB)
	assert.WithinDuration(t, newEnd, updated.CurrentPeriodEnd, time.Second)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventNewBillingCycleStarted)
}

func Test_SyncSubscriptionFromProvider_WhenQuantityDecreased_AppliesDowngrade(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 100)

	newStart := time.Now().UTC().Add(30 * 24 * time.Hour)
	newEnd := newStart.Add(30 * 24 * time.Hour)
	err := billingService.SyncSubscriptionFromProvider(log, sub, billing_models.WebhookEvent{
		RequestID:       uuid.New(),
		ProviderEventID: "evt-renewed-pending",
		QuantityGB:      50,
		Status:          billing_models.StatusActive,
		PeriodStart:     &newStart,
		PeriodEnd:       &newEnd,
	})

	assert.NoError(t, err)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, 50, updated.StorageGB)
	assert.Nil(t, updated.PendingStorageGB)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventDowngraded)
}

func Test_CancelSubscription_FromActive_GracePeriodAfterPeriodEnd(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	periodEnd := sub.CurrentPeriodEnd

	err := billingService.CancelSubscription(log, sub, makeWebhookEvent(db.ID, "evt-cancel-active", ""))

	assert.NoError(t, err)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, updated.Status)
	assert.NotNil(t, updated.CanceledAt)
	assert.NotNil(t, updated.DataRetentionGracePeriodUntil)

	expectedGrace := periodEnd.Add(config.GetEnv().GracePeriod)
	assert.WithinDuration(t, expectedGrace, *updated.DataRetentionGracePeriodUntil, 5*time.Second)
}

func Test_CancelSubscription_FromPastDue_ImmediateGracePeriod(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	_ = billingService.RecordPaymentFailed(log, sub, makeWebhookEvent(db.ID, "evt-fail-for-cancel", "inv-fail-1"))

	sub = getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusPastDue, sub.Status)

	cancelTime := time.Now().UTC()
	err := billingService.CancelSubscription(log, sub, makeWebhookEvent(db.ID, "evt-cancel-pastdue", ""))

	assert.NoError(t, err)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, updated.Status)
	assert.NotNil(t, updated.DataRetentionGracePeriodUntil)

	expectedGrace := cancelTime.Add(config.GetEnv().GracePeriod)
	assert.WithinDuration(t, expectedGrace, *updated.DataRetentionGracePeriodUntil, 5*time.Second)
}

func Test_RecordPaymentSuccess_WhenActive_CreatesInvoice(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	err := billingService.RecordPaymentSuccess(
		log,
		sub,
		makePaymentWebhookEvent("inv-active-"+uuid.New().String()[:8], 50, 1000),
	)

	assert.NoError(t, err)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, updated.Status)

	invoices := getInvoicesViaAPI(t, router, owner.Token, sub.ID)
	assert.Len(t, invoices, 1)
	assert.Equal(t, int64(1000), invoices[0].AmountCents)
	assert.Equal(t, billing_models.InvoiceStatusPaid, invoices[0].Status)
}

func Test_RecordPaymentSuccess_WhenPastDue_RecoversToActive(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	_ = billingService.RecordPaymentFailed(log, sub, makeWebhookEvent(db.ID, "evt-fail-2", "inv-fail-2"))

	sub = getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusPastDue, sub.Status)

	err := billingService.RecordPaymentSuccess(
		log,
		sub,
		makePaymentWebhookEvent("inv-recovery-"+uuid.New().String()[:8], 50, 1000),
	)

	assert.NoError(t, err)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, updated.Status)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventRecoveredFromPastDue)
}

func Test_RecordPaymentFailed_WhenActive_SetsPastDue(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	err := billingService.RecordPaymentFailed(log, sub, makeWebhookEvent(db.ID, "evt-fail-3", "inv-fail-3"))

	assert.NoError(t, err)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusPastDue, updated.Status)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventPastDue)
}

func Test_SyncSubscriptionFromProvider_WhenQuantityIncreased_ConfirmsUpgrade(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)
	err := billingService.SyncSubscriptionFromProvider(log, sub, billing_models.WebhookEvent{
		RequestID:       uuid.New(),
		ProviderEventID: "evt-upgrade-confirm",
		QuantityGB:      100,
		Status:          billing_models.StatusActive,
		PeriodStart:     &now,
		PeriodEnd:       &periodEnd,
	})

	assert.NoError(t, err)

	updated := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, 100, updated.StorageGB)
	assert.Nil(t, updated.PendingStorageGB)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventUpgraded)
}

func Test_RecordDispute_WhenCalled_CancelsAndMarksInvoiceDisputed(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)

	invoiceID := "inv-dispute-" + uuid.New().String()[:8]
	_ = billingService.RecordPaymentSuccess(log, sub, makePaymentWebhookEvent(invoiceID, 50, 1000))

	err := billingService.RecordDispute(log, billing_models.WebhookEvent{
		RequestID:         uuid.New(),
		ProviderEventID:   "evt-dispute-1",
		ProviderInvoiceID: invoiceID,
	})

	assert.NoError(t, err)

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

// ---- Category 9: Background Processing ----

func Test_ProcessExpiredSubscriptions_WhenGracePeriodEnded_MarksExpired(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	_ = billingService.CancelSubscription(log, sub, makeWebhookEvent(db.ID, "evt-cancel-bg", ""))

	sub = refreshSubscription(t, sub.ID)
	pastGrace := time.Now().UTC().Add(-1 * time.Hour)
	sub.DataRetentionGracePeriodUntil = &pastGrace
	_ = billingService.subscriptionRepository.Save(*sub)

	err := billingService.processExpiredSubscriptions(log)
	assert.NoError(t, err)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusExpired, updated.Status)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventExpired)
}

func Test_ProcessExpiredSubscriptions_WhenGracePeriodActive_NoChange(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	_ = billingService.CancelSubscription(log, sub, makeWebhookEvent(db.ID, "evt-cancel-bg2", ""))

	sub = refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, sub.Status)

	err := billingService.processExpiredSubscriptions(log)
	assert.NoError(t, err)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusCanceled, updated.Status)
}

func Test_ProcessExpiredTrials_WhenTrialEnded_MarksExpired(t *testing.T) {
	router, owner, _, db := setupBillingTestWithoutProvider(t)
	log := slog.Default()

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusTrial, sub.Status)

	pastEnd := time.Now().UTC().Add(-1 * time.Hour)
	sub.CurrentPeriodEnd = pastEnd
	_ = billingService.subscriptionRepository.Save(*sub)

	err := billingService.processExpiredTrials(log)
	assert.NoError(t, err)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusExpired, updated.Status)
	assert.NotNil(t, updated.CanceledAt)

	events := getEventsViaAPI(t, router, owner.Token, sub.ID)
	eventTypes := extractEventTypes(events)
	assert.Contains(t, eventTypes, billing_models.EventExpired)
}

func Test_ProcessExpiredTrials_WhenTrialActive_NoChange(t *testing.T) {
	router, owner, _, db := setupBillingTestWithoutProvider(t)
	log := slog.Default()

	sub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusTrial, sub.Status)

	err := billingService.processExpiredTrials(log)
	assert.NoError(t, err)

	updated := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusTrial, updated.Status)
}

// ---- Category 10: Full Lifecycle ----

func Test_FullLifecycle_TrialToActiveToUpgradeToCancelToExpired(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	// Step 1: Trial created on DB creation
	trialSub := getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusTrial, trialSub.Status)

	// Step 2: Expire trial
	pastEnd := time.Now().UTC().Add(-1 * time.Hour)
	trialSub.CurrentPeriodEnd = pastEnd
	_ = billingService.subscriptionRepository.Save(*trialSub)
	_ = billingService.processExpiredTrials(log)

	// Step 3: Activate paid subscription
	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	assert.Equal(t, billing_models.StatusActive, sub.Status)

	// Step 4: Upgrade storage
	test_utils.MakePostRequest(
		t, router,
		"/api/v1/billing/subscription/change-storage",
		"Bearer "+owner.Token,
		ChangeStorageRequest{DatabaseID: db.ID, StorageGB: 100},
		http.StatusOK,
	)

	// Step 5: Confirm upgrade via webhook
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)
	_ = billingService.SyncSubscriptionFromProvider(log, sub, billing_models.WebhookEvent{
		RequestID:       uuid.New(),
		ProviderEventID: "evt-upgrade-lifecycle",
		QuantityGB:      100,
		Status:          billing_models.StatusActive,
		PeriodStart:     &now,
		PeriodEnd:       &periodEnd,
	})

	sub = getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, 100, sub.StorageGB)

	// Step 6: Payment success
	_ = billingService.RecordPaymentSuccess(
		log,
		sub,
		makePaymentWebhookEvent("inv-lifecycle-"+uuid.New().String()[:8], 100, 2000),
	)

	// Step 7: Cancel
	_ = billingService.CancelSubscription(log, sub, makeWebhookEvent(db.ID, "evt-cancel-lifecycle", ""))

	// Step 8: Expire
	sub = refreshSubscription(t, sub.ID)
	pastGrace := time.Now().UTC().Add(-1 * time.Hour)
	sub.DataRetentionGracePeriodUntil = &pastGrace
	_ = billingService.subscriptionRepository.Save(*sub)
	_ = billingService.processExpiredSubscriptions(log)

	finalSub := refreshSubscription(t, sub.ID)
	assert.Equal(t, billing_models.StatusExpired, finalSub.Status)

	// Step 9: Verify events via HTTP
	var eventsResponse GetSubscriptionEventsResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/events/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&eventsResponse,
	)
	eventTypes := extractEventTypes(eventsResponse.Events)
	assert.Contains(t, eventTypes, billing_models.EventCreated)
	assert.Contains(t, eventTypes, billing_models.EventUpgraded)
	assert.Contains(t, eventTypes, billing_models.EventCanceled)
	assert.Contains(t, eventTypes, billing_models.EventExpired)

	// Step 10: Verify invoice via HTTP
	var invoicesResponse GetInvoicesResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/invoices/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&invoicesResponse,
	)
	assert.Len(t, invoicesResponse.Invoices, 1)
	assert.Equal(t, billing_models.InvoiceStatusPaid, invoicesResponse.Invoices[0].Status)
}

func Test_FullLifecycle_ActiveToPastDueToRecovery(t *testing.T) {
	router, owner, _, db, _ := setupBillingTest(t)
	log := slog.Default()

	sub := activateSubscriptionViaWebhook(t, router, owner.Token, db.ID, 50)
	assert.Equal(t, billing_models.StatusActive, sub.Status)

	_ = billingService.RecordPaymentFailed(log, sub, makeWebhookEvent(db.ID, "evt-fail-lc", "inv-fail-lc"))
	sub = getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusPastDue, sub.Status)

	_ = billingService.RecordPaymentSuccess(
		log,
		sub,
		makePaymentWebhookEvent("inv-recovery-lc-"+uuid.New().String()[:8], 50, 1000),
	)

	sub = getSubscriptionViaAPI(t, router, owner.Token, db.ID)
	assert.Equal(t, billing_models.StatusActive, sub.Status)

	var eventsResponse GetSubscriptionEventsResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/billing/subscription/events/"+sub.ID.String(),
		"Bearer "+owner.Token,
		http.StatusOK,
		&eventsResponse,
	)
	eventTypes := extractEventTypes(eventsResponse.Events)
	assert.Contains(t, eventTypes, billing_models.EventCreated)
	assert.Contains(t, eventTypes, billing_models.EventPastDue)
	assert.Contains(t, eventTypes, billing_models.EventRecoveredFromPastDue)
}

// ---- Private Helpers ----

type mockBillingProvider struct {
	providerName                       billing_provider.ProviderName
	createCheckoutSessionTransactionID string
	createCheckoutSessionErr           error
	upgradeErr                         error
	upgradeCalledWithGB                int
	scheduleDowngradeErr               error
	scheduleDowngradeCalledWithGB      int
	getSubscriptionResult              billing_provider.ProviderSubscription
	getSubscriptionErr                 error
	createPortalSessionURL             string
	createPortalSessionErr             error
}

func (m *mockBillingProvider) GetProviderName() billing_provider.ProviderName {
	return m.providerName
}

func (m *mockBillingProvider) CreateCheckoutSession(
	_ *slog.Logger,
	_ billing_provider.CheckoutRequest,
) (string, error) {
	return m.createCheckoutSessionTransactionID, m.createCheckoutSessionErr
}

func (m *mockBillingProvider) UpgradeQuantityWithSurcharge(_ *slog.Logger, _ string, quantityGB int) error {
	m.upgradeCalledWithGB = quantityGB
	return m.upgradeErr
}

func (m *mockBillingProvider) ScheduleQuantityDowngradeFromNextBillingCycle(
	_ *slog.Logger,
	_ string,
	quantityGB int,
) error {
	m.scheduleDowngradeCalledWithGB = quantityGB
	return m.scheduleDowngradeErr
}

func (m *mockBillingProvider) GetSubscription(_ *slog.Logger, _ string) (billing_provider.ProviderSubscription, error) {
	return m.getSubscriptionResult, m.getSubscriptionErr
}

func (m *mockBillingProvider) CreatePortalSession(_ *slog.Logger, _, _ string) (string, error) {
	return m.createPortalSessionURL, m.createPortalSessionErr
}

func newMockProvider() *mockBillingProvider {
	mock := &mockBillingProvider{
		providerName:                       "mock",
		createCheckoutSessionTransactionID: "txn_mock_test_123",
		createPortalSessionURL:             "https://portal.test/mock-portal",
	}
	billingService.SetBillingProvider(mock)
	return mock
}

func enableCloud(t *testing.T) {
	t.Helper()
	config.GetEnv().IsCloud = true
	t.Cleanup(func() {
		config.GetEnv().IsCloud = false
	})
}

func createBillingTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	v1 := router.Group("/api/v1")
	protected := v1.Group("").Use(users_middleware.AuthMiddleware(users_services.GetUserService()))

	workspaces_controllers.GetWorkspaceController().RegisterRoutes(protected.(*gin.RouterGroup))
	workspaces_controllers.GetMembershipController().RegisterRoutes(protected.(*gin.RouterGroup))
	databases.GetDatabaseController().RegisterRoutes(protected.(*gin.RouterGroup))
	GetBillingController().RegisterRoutes(protected.(*gin.RouterGroup))

	SetupDependencies()
	audit_logs.SetupDependencies()

	return router
}

func setupBillingTestWithoutProvider(
	t *testing.T,
) (*gin.Engine, *users_dto.SignInResponseDTO, *workspaces_models.Workspace, *databases.Database) {
	t.Helper()

	router := createBillingTestRouter()
	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("billing-test-"+uuid.New().String()[:8], owner, router)

	// DB creation validates read-only user in cloud mode, so create DB first, then enable cloud
	db := createTestDatabaseForBilling(owner.Token, workspace.ID, router)
	enableCloud(t)

	t.Cleanup(func() {
		cleanupBillingTestData(db.ID)
		databases.RemoveTestDatabase(db)
		workspaces_testing.RemoveTestWorkspace(workspace, router)
	})

	return router, owner, workspace, db
}

func setupBillingTest(
	t *testing.T,
) (*gin.Engine, *users_dto.SignInResponseDTO, *workspaces_models.Workspace, *databases.Database, *mockBillingProvider) {
	t.Helper()
	router, owner, workspace, db := setupBillingTestWithoutProvider(t)
	mock := newMockProvider()
	return router, owner, workspace, db, mock
}

func createTestDatabaseForBilling(token string, workspaceID uuid.UUID, router *gin.Engine) *databases.Database {
	env := config.GetEnv()
	port, err := strconv.Atoi(env.TestLogicalPostgres16Port)
	if err != nil {
		panic(fmt.Sprintf("failed to parse TEST_LOGICAL_POSTGRES_16_PORT: %v", err))
	}

	dbName := "testdb"
	request := databases.Database{
		Name:        "billing-test-" + uuid.New().String()[:8],
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
		panic(fmt.Sprintf("failed to create database for billing test. Status: %d, Body: %s", w.Code, w.Body.String()))
	}

	var db databases.Database
	if err := json.Unmarshal(w.Body.Bytes(), &db); err != nil {
		panic(err)
	}

	return &db
}

func activateSubscriptionViaWebhook(
	t *testing.T,
	router *gin.Engine,
	token string,
	databaseID uuid.UUID,
	storageGB int,
) *billing_models.Subscription {
	t.Helper()

	expireActiveSubscription(t, databaseID)

	providerSubID := "sub-" + uuid.New().String()
	providerCustomerID := "cus-" + uuid.New().String()
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	err := billingService.ActivateSubscription(slog.Default(), billing_models.WebhookEvent{
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
		t.Fatalf("activateSubscriptionViaWebhook: %v", err)
	}

	return getSubscriptionViaAPI(t, router, token, databaseID)
}

func expireActiveSubscription(t *testing.T, databaseID uuid.UUID) {
	t.Helper()

	subs, err := billingService.subscriptionRepository.FindByDatabaseIDAndStatuses(
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
		if saveErr := billingService.subscriptionRepository.Save(*sub); saveErr != nil {
			t.Fatalf("expireActiveSubscription: save failed: %v", saveErr)
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

	var response GetSubscriptionEventsResponse
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

	var response GetInvoicesResponse
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

	sub, err := billingService.subscriptionRepository.FindByID(subID)
	if err != nil || sub == nil {
		t.Fatalf("refreshSubscription: not found: %v", err)
	}

	return sub
}

func makeWebhookEvent(databaseID uuid.UUID, providerEventID, providerInvoiceID string) billing_models.WebhookEvent {
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	return billing_models.WebhookEvent{
		RequestID:         uuid.New(),
		ProviderEventID:   providerEventID,
		DatabaseID:        &databaseID,
		ProviderInvoiceID: providerInvoiceID,
		PeriodStart:       &now,
		PeriodEnd:         &periodEnd,
	}
}

func makePaymentWebhookEvent(providerInvoiceID string, quantityGB int, amountCents int64) billing_models.WebhookEvent {
	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)

	return billing_models.WebhookEvent{
		RequestID:              uuid.New(),
		ProviderEventID:        "evt-" + uuid.New().String()[:8],
		ProviderSubscriptionID: "sub-test-123",
		ProviderInvoiceID:      providerInvoiceID,
		QuantityGB:             quantityGB,
		AmountCents:            amountCents,
		PeriodStart:            &now,
		PeriodEnd:              &periodEnd,
	}
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
