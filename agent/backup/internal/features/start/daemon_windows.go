//go:build windows

package start

import (
	"errors"
	"log/slog"
)

func Stop(log *slog.Logger) error {
	return errors.New("stop is not supported on Windows — use Ctrl+C in the terminal where the agent is running")
}

func Status(log *slog.Logger) error {
	return errors.New("status is not supported on Windows — check the terminal where the agent is running")
}

func spawnDaemon(_ *slog.Logger) (int, error) {
	return 0, errors.New("daemon mode is not supported on Windows")
}
