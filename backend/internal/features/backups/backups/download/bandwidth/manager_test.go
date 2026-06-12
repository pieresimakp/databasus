package bandwidth

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_Manager_RegisterSingleDownload(t *testing.T) {
	throughputMBs := 100
	manager := NewManager(throughputMBs)

	expectedBytesPerSec := int64(100 * 1024 * 1024 * 75 / 100)
	assert.Equal(t, expectedBytesPerSec, manager.maxTotalBytesPerSecond)
	assert.Equal(t, expectedBytesPerSec, manager.bytesPerSecondPerDownload)

	userID := uuid.New()
	rateLimiter, err := manager.RegisterDownload(userID)
	assert.NoError(t, err)
	assert.NotNil(t, rateLimiter)

	assert.Equal(t, 1, manager.GetActiveDownloadCount())
	assert.Equal(t, expectedBytesPerSec, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedBytesPerSec, rateLimiter.GetBytesPerSecond())
}

func Test_Manager_RegisterMultipleDownloads_BandwidthShared(t *testing.T) {
	throughputMBs := 100
	manager := NewManager(throughputMBs)

	maxBytes := int64(100 * 1024 * 1024 * 75 / 100)

	user1 := uuid.New()
	rateLimiter1, err := manager.RegisterDownload(user1)
	assert.NoError(t, err)
	assert.Equal(t, maxBytes, rateLimiter1.GetBytesPerSecond())

	user2 := uuid.New()
	rateLimiter2, err := manager.RegisterDownload(user2)
	assert.NoError(t, err)

	expectedPerDownload := maxBytes / 2
	assert.Equal(t, expectedPerDownload, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedPerDownload, rateLimiter1.GetBytesPerSecond())
	assert.Equal(t, expectedPerDownload, rateLimiter2.GetBytesPerSecond())

	user3 := uuid.New()
	rateLimiter3, err := manager.RegisterDownload(user3)
	assert.NoError(t, err)

	expectedPerDownload = maxBytes / 3
	assert.Equal(t, expectedPerDownload, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedPerDownload, rateLimiter1.GetBytesPerSecond())
	assert.Equal(t, expectedPerDownload, rateLimiter2.GetBytesPerSecond())
	assert.Equal(t, expectedPerDownload, rateLimiter3.GetBytesPerSecond())
	assert.Equal(t, 3, manager.GetActiveDownloadCount())
}

func Test_Manager_UnregisterDownload_BandwidthRebalanced(t *testing.T) {
	throughputMBs := 100
	manager := NewManager(throughputMBs)

	maxBytes := int64(100 * 1024 * 1024 * 75 / 100)

	user1 := uuid.New()
	rateLimiter1, _ := manager.RegisterDownload(user1)

	user2 := uuid.New()
	_, _ = manager.RegisterDownload(user2)

	user3 := uuid.New()
	rateLimiter3, _ := manager.RegisterDownload(user3)

	assert.Equal(t, 3, manager.GetActiveDownloadCount())
	expectedPerDownload := maxBytes / 3
	assert.Equal(t, expectedPerDownload, rateLimiter1.GetBytesPerSecond())

	manager.UnregisterDownload(user2)

	assert.Equal(t, 2, manager.GetActiveDownloadCount())
	expectedPerDownload = maxBytes / 2
	assert.Equal(t, expectedPerDownload, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedPerDownload, rateLimiter1.GetBytesPerSecond())
	assert.Equal(t, expectedPerDownload, rateLimiter3.GetBytesPerSecond())

	manager.UnregisterDownload(user1)

	assert.Equal(t, 1, manager.GetActiveDownloadCount())
	assert.Equal(t, maxBytes, manager.bytesPerSecondPerDownload)
	assert.Equal(t, maxBytes, rateLimiter3.GetBytesPerSecond())

	manager.UnregisterDownload(user3)
	assert.Equal(t, 0, manager.GetActiveDownloadCount())
	assert.Equal(t, maxBytes, manager.bytesPerSecondPerDownload)
}

func Test_Manager_RegisterDuplicateUser_ReturnsError(t *testing.T) {
	manager := NewManager(100)

	userID := uuid.New()
	_, err := manager.RegisterDownload(userID)
	assert.NoError(t, err)

	_, err = manager.RegisterDownload(userID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download already registered")
}
