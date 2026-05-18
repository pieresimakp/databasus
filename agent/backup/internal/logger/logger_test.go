package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Write_DataWrittenToFile(t *testing.T) {
	rw, logPath, _ := setupRotatingWriter(t, 1024)

	data := []byte("hello world\n")
	n, err := rw.Write(data)

	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, int64(len(data)), rw.currentSize)

	content, err := os.ReadFile(logPath)
	require.NoError(t, err)
	assert.Equal(t, string(data), string(content))
}

func Test_Write_WhenLimitExceeded_FileRotated(t *testing.T) {
	rw, logPath, oldLogPath := setupRotatingWriter(t, 100)

	firstData := []byte(strings.Repeat("A", 80))
	_, err := rw.Write(firstData)
	require.NoError(t, err)

	secondData := []byte(strings.Repeat("B", 30))
	_, err = rw.Write(secondData)
	require.NoError(t, err)

	oldContent, err := os.ReadFile(oldLogPath)
	require.NoError(t, err)
	assert.Equal(t, string(firstData), string(oldContent))

	newContent, err := os.ReadFile(logPath)
	require.NoError(t, err)
	assert.Equal(t, string(secondData), string(newContent))

	assert.Equal(t, int64(len(secondData)), rw.currentSize)
}

func Test_Write_WhenOldFileExists_OldFileReplaced(t *testing.T) {
	rw, _, oldLogPath := setupRotatingWriter(t, 100)

	require.NoError(t, os.WriteFile(oldLogPath, []byte("stale data"), 0o644))

	_, err := rw.Write([]byte(strings.Repeat("A", 80)))
	require.NoError(t, err)

	_, err = rw.Write([]byte(strings.Repeat("B", 30)))
	require.NoError(t, err)

	oldContent, err := os.ReadFile(oldLogPath)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("A", 80), string(oldContent))
}

func Test_Write_MultipleSmallWrites_CurrentSizeAccumulated(t *testing.T) {
	rw, _, _ := setupRotatingWriter(t, 1024)

	var totalWritten int64
	for range 10 {
		data := []byte("line\n")
		n, err := rw.Write(data)
		require.NoError(t, err)

		totalWritten += int64(n)
	}

	assert.Equal(t, totalWritten, rw.currentSize)
	assert.Equal(t, int64(50), rw.currentSize)
}

func Test_Write_ExactlyAtBoundary_NoRotationUntilNextByte(t *testing.T) {
	rw, logPath, oldLogPath := setupRotatingWriter(t, 100)

	exactData := []byte(strings.Repeat("X", 100))
	_, err := rw.Write(exactData)
	require.NoError(t, err)

	_, err = os.Stat(oldLogPath)
	assert.True(t, os.IsNotExist(err), ".old file should not exist yet")

	content, err := os.ReadFile(logPath)
	require.NoError(t, err)
	assert.Equal(t, string(exactData), string(content))

	_, err = rw.Write([]byte("Z"))
	require.NoError(t, err)

	_, err = os.Stat(oldLogPath)
	assert.NoError(t, err, ".old file should exist after exceeding limit")

	assert.Equal(t, int64(1), rw.currentSize)
}

func setupRotatingWriter(t *testing.T, maxSize int64) (*rotatingWriter, string, string) {
	t.Helper()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	oldLogPath := filepath.Join(dir, "test.log.old")

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)

	rw := &rotatingWriter{
		file:        f,
		currentSize: 0,
		maxSize:     maxSize,
		logPath:     logPath,
		oldLogPath:  oldLogPath,
	}

	t.Cleanup(func() {
		rw.file.Close()
	})

	return rw, logPath, oldLogPath
}
