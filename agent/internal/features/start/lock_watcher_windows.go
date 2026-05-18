//go:build windows

package start

import (
	"context"
	"log/slog"
	"os"
)

type LockWatcher struct{}

func NewLockWatcher(_ *os.File, _ context.CancelFunc, _ *slog.Logger) (*LockWatcher, error) {
	return &LockWatcher{}, nil
}

func (w *LockWatcher) Run(_ context.Context) {}
