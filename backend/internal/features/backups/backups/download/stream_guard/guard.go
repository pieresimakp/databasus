package stream_guard

import (
	"log/slog"

	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups/download/bandwidth"
	"databasus-backend/internal/features/backups/backups/download/ratelimit"
)

// Guard enforces the per-user single-stream rule shared by downloads and
// restores: at most one heavy stream per user at a time, plus a bandwidth slot
// the bandwidth.Manager rebalances as streams come and go. DownloadTokenService
// and RestoreTokenService embed the SAME guard, so the lock namespace and
// bandwidth pool are shared across both — a user can't run a download and a
// restore at once.
type Guard struct {
	tracker          *Tracker
	bandwidthManager *bandwidth.Manager
	logger           *slog.Logger
}

func NewGuard(
	tracker *Tracker,
	bandwidthManager *bandwidth.Manager,
	logger *slog.Logger,
) *Guard {
	return &Guard{tracker, bandwidthManager, logger}
}

func (g *Guard) IsDownloadInProgress(userID uuid.UUID) bool {
	return g.tracker.IsDownloadInProgress(userID)
}

func (g *Guard) RefreshDownloadLock(userID uuid.UUID) {
	g.tracker.RefreshDownloadLock(userID)
}

func (g *Guard) ReleaseDownloadLock(userID uuid.UUID) {
	g.tracker.ReleaseDownloadLock(userID)
	g.logger.Info("released stream lock", "user_id", userID)
}

func (g *Guard) UnregisterDownload(userID uuid.UUID) {
	g.bandwidthManager.UnregisterDownload(userID)
	g.logger.Info("unregistered stream from bandwidth manager", "user_id", userID)
}

// AcquireSlot takes the per-user lock and a rate limiter, rolling the lock back
// if the bandwidth slot can't be obtained so a failed acquire never leaves a
// dangling lock.
func (g *Guard) AcquireSlot(userID uuid.UUID) (*ratelimit.Limiter, error) {
	if err := g.tracker.AcquireDownloadLock(userID); err != nil {
		return nil, err
	}

	rateLimiter, err := g.bandwidthManager.RegisterDownload(userID)
	if err != nil {
		g.tracker.ReleaseDownloadLock(userID)

		return nil, err
	}

	return rateLimiter, nil
}
