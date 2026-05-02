package files_utils

import (
	"fmt"
	"os"
)

func CreateTempFile(prefix, content string) (string, error) {
	tempFile, err := os.CreateTemp("", prefix+"_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}

	if _, err := tempFile.WriteString(content); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to write to temporary file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Set permissions to be readable only by the owner
	if err := os.Chmod(tempFile.Name(), 0o600); err != nil {
		os.Remove(tempFile.Name())
		return "", fmt.Errorf("failed to set temporary file permissions: %w", err)
	}

	return tempFile.Name(), nil
}
