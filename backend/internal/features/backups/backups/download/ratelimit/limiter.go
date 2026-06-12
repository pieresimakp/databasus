package ratelimit

import (
	"sync"
	"time"
)

type Limiter struct {
	mu              sync.Mutex
	bytesPerSecond  int64
	bucketSize      int64
	availableTokens float64
	lastRefill      time.Time
}

func NewLimiter(bytesPerSecond int64) *Limiter {
	if bytesPerSecond <= 0 {
		bytesPerSecond = 1024 * 1024 * 100
	}

	return &Limiter{
		bytesPerSecond:  bytesPerSecond,
		bucketSize:      bytesPerSecond * 2,
		availableTokens: float64(bytesPerSecond * 2),
		lastRefill:      time.Now().UTC(),
	}
}

func (rl *Limiter) UpdateRate(bytesPerSecond int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if bytesPerSecond <= 0 {
		bytesPerSecond = 1024 * 1024 * 100
	}

	rl.bytesPerSecond = bytesPerSecond
	rl.bucketSize = bytesPerSecond * 2

	if rl.availableTokens > float64(rl.bucketSize) {
		rl.availableTokens = float64(rl.bucketSize)
	}
}

// GetBytesPerSecond reports the limiter's current sustained rate. The
// BandwidthManager owns this value and rebalances it as streams come and go;
// exposed so other packages (and its tests) can observe the live rate.
func (rl *Limiter) GetBytesPerSecond() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	return rl.bytesPerSecond
}

func (rl *Limiter) Wait(bytes int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for {
		now := time.Now().UTC()
		elapsed := now.Sub(rl.lastRefill).Seconds()

		tokensToAdd := elapsed * float64(rl.bytesPerSecond)
		rl.availableTokens += tokensToAdd
		if rl.availableTokens > float64(rl.bucketSize) {
			rl.availableTokens = float64(rl.bucketSize)
		}
		rl.lastRefill = now

		if rl.availableTokens >= float64(bytes) {
			rl.availableTokens -= float64(bytes)
			return
		}

		tokensNeeded := float64(bytes) - rl.availableTokens
		waitTime := time.Duration(tokensNeeded/float64(rl.bytesPerSecond)*1000) * time.Millisecond

		waitTime = max(waitTime, time.Millisecond)

		rl.mu.Unlock()
		time.Sleep(waitTime)
		rl.mu.Lock()
	}
}
