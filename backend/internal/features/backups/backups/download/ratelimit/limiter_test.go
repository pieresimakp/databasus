package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Limiter_TokenBucketBasic(t *testing.T) {
	bytesPerSec := int64(1024 * 1024)
	limiter := NewLimiter(bytesPerSec)

	assert.Equal(t, bytesPerSec, limiter.bytesPerSecond)
	assert.Equal(t, bytesPerSec*2, limiter.bucketSize)

	start := time.Now()
	limiter.Wait(512 * 1024)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 100*time.Millisecond)
}

func Test_Limiter_UpdateRate(t *testing.T) {
	limiter := NewLimiter(1024 * 1024)

	assert.Equal(t, int64(1024*1024), limiter.bytesPerSecond)

	newRate := int64(2 * 1024 * 1024)
	limiter.UpdateRate(newRate)

	assert.Equal(t, newRate, limiter.bytesPerSecond)
	assert.Equal(t, newRate*2, limiter.bucketSize)
}

func Test_Limiter_ThrottlesCorrectly(t *testing.T) {
	bytesPerSec := int64(1024 * 1024)
	limiter := NewLimiter(bytesPerSec)

	limiter.availableTokens = 0

	start := time.Now()
	limiter.Wait(bytesPerSec / 2)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond)
	assert.LessOrEqual(t, elapsed, 700*time.Millisecond)
}
