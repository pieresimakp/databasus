package upgrade

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"databasus-agent/internal/features/api"
)

// CheckAndUpdate checks if a new version is available and upgrades the binary on disk.
// Returns (true, nil) if the binary was upgraded, (false, nil) if already up to date,
// or (false, err) on failure. Callers are responsible for re-exec or restart signaling.
func CheckAndUpdate(apiClient *api.Client, currentVersion string, isDev bool, log *slog.Logger) (bool, error) {
	if isDev {
		log.Info("Skipping update check (development mode)")

		return false, nil
	}

	serverVersion, err := apiClient.FetchServerVersion(context.Background())
	if err != nil {
		log.Warn("Could not reach server for update check", "error", err)

		return false, fmt.Errorf(
			"unable to check version, please verify Databasus server is available: %w",
			err,
		)
	}

	if serverVersion == currentVersion {
		log.Info("Agent version is up to date", "version", currentVersion)

		return false, nil
	}

	log.Info("Updating agent...", "current", currentVersion, "target", serverVersion)

	selfPath, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("failed to determine executable path: %w", err)
	}

	tempPath := selfPath + ".update"

	defer func() {
		_ = os.Remove(tempPath)
	}()

	if err := apiClient.DownloadAgentBinary(context.Background(), runtime.GOARCH, tempPath); err != nil {
		return false, fmt.Errorf("failed to download update: %w", err)
	}

	if err := os.Chmod(tempPath, 0o755); err != nil {
		return false, fmt.Errorf("failed to set permissions on update: %w", err)
	}

	if err := verifyBinary(tempPath, serverVersion); err != nil {
		return false, fmt.Errorf("update verification failed: %w", err)
	}

	if err := os.Rename(tempPath, selfPath); err != nil {
		return false, fmt.Errorf("failed to replace binary (try --skip-update if this persists): %w", err)
	}

	log.Info("Agent binary updated", "version", serverVersion)

	return true, nil
}

func verifyBinary(binaryPath, expectedVersion string) error {
	cmd := exec.CommandContext(context.Background(), binaryPath, "version")

	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("binary failed to execute: %w", err)
	}

	got := strings.TrimSpace(string(output))
	if got != expectedVersion {
		return fmt.Errorf("version mismatch: expected %q, got %q", expectedVersion, got)
	}

	return nil
}
