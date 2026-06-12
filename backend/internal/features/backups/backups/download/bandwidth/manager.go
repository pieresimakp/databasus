package bandwidth

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups/download/ratelimit"
)

type Manager struct {
	mu                        sync.RWMutex
	activeDownloads           map[uuid.UUID]*activeDownload
	maxTotalBytesPerSecond    int64
	bytesPerSecondPerDownload int64
}

type activeDownload struct {
	userID      uuid.UUID
	rateLimiter *ratelimit.Limiter
}

func NewManager(throughputMBs int) *Manager {
	// Use 75% of total throughput
	maxBytes := int64(throughputMBs) * 1024 * 1024 * 75 / 100

	return &Manager{
		activeDownloads:           make(map[uuid.UUID]*activeDownload),
		maxTotalBytesPerSecond:    maxBytes,
		bytesPerSecondPerDownload: maxBytes,
	}
}

func (bm *Manager) RegisterDownload(userID uuid.UUID) (*ratelimit.Limiter, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, exists := bm.activeDownloads[userID]; exists {
		return nil, fmt.Errorf("download already registered for user %s", userID)
	}

	rateLimiter := ratelimit.NewLimiter(bm.bytesPerSecondPerDownload)

	bm.activeDownloads[userID] = &activeDownload{
		userID:      userID,
		rateLimiter: rateLimiter,
	}

	bm.recalculateRates()

	return rateLimiter, nil
}

func (bm *Manager) UnregisterDownload(userID uuid.UUID) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	delete(bm.activeDownloads, userID)
	bm.recalculateRates()
}

func (bm *Manager) GetActiveDownloadCount() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return len(bm.activeDownloads)
}

func (bm *Manager) recalculateRates() {
	activeCount := len(bm.activeDownloads)

	if activeCount == 0 {
		bm.bytesPerSecondPerDownload = bm.maxTotalBytesPerSecond
		return
	}

	newRate := bm.maxTotalBytesPerSecond / int64(activeCount)
	bm.bytesPerSecondPerDownload = newRate

	for _, download := range bm.activeDownloads {
		download.rateLimiter.UpdateRate(newRate)
	}
}
