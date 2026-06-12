package backuping_physical

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	postgresql_executor "databasus-backend/internal/features/backups/backups/usecases/physical/postgresql"
	billing_models "databasus-backend/internal/features/billing/models"
	"databasus-backend/internal/features/notifiers"
)

type NotificationSender interface {
	SendNotification(notifier *notifiers.Notifier, title, message string)
}

type BillingService interface {
	GetSubscription(logger *slog.Logger, databaseID uuid.UUID) (*billing_models.Subscription, error)
}

type FullBackupExecutor interface {
	Execute(
		ctx context.Context,
		spec postgresql_executor.FullBackupSpec,
	) (postgresql_executor.PhysicalBackupResult, error)
}

type IncrementalBackupExecutor interface {
	Execute(
		ctx context.Context,
		spec postgresql_executor.IncrementalBackupSpec,
	) (postgresql_executor.PhysicalBackupResult, error)
}
