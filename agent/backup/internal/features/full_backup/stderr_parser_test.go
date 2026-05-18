package full_backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParseBasebackupStderr_WithPG17FetchOutput_ExtractsCorrectSegments(t *testing.T) {
	stderr := `pg_basebackup: initiating base backup, waiting for checkpoint to complete
pg_basebackup: checkpoint completed
pg_basebackup: write-ahead log start point: 0/2000028 on timeline 1
pg_basebackup: starting background WAL receiver
pg_basebackup: write-ahead log end point: 0/2000100
pg_basebackup: waiting for background process to finish streaming ...
pg_basebackup: syncing data to disk ...
pg_basebackup: renaming backup_manifest.tmp to backup_manifest
pg_basebackup: base backup completed`

	startSeg, stopSeg, err := ParseBasebackupStderr(stderr)

	require.NoError(t, err)
	assert.Equal(t, "000000010000000000000002", startSeg)
	assert.Equal(t, "000000010000000000000002", stopSeg)
}

func Test_ParseBasebackupStderr_WithHighLSNValues_ExtractsCorrectSegments(t *testing.T) {
	stderr := `pg_basebackup: write-ahead log start point: 1/AB000028 on timeline 1
pg_basebackup: write-ahead log end point: 1/AC000000`

	startSeg, stopSeg, err := ParseBasebackupStderr(stderr)

	require.NoError(t, err)
	assert.Equal(t, "0000000100000001000000AB", startSeg)
	assert.Equal(t, "0000000100000001000000AC", stopSeg)
}

func Test_ParseBasebackupStderr_WithHighLogID_ExtractsCorrectSegments(t *testing.T) {
	stderr := `pg_basebackup: write-ahead log start point: A/FF000028 on timeline 1
pg_basebackup: write-ahead log end point: B/1000000`

	startSeg, stopSeg, err := ParseBasebackupStderr(stderr)

	require.NoError(t, err)
	assert.Equal(t, "000000010000000A000000FF", startSeg)
	assert.Equal(t, "000000010000000B00000001", stopSeg)
}

func Test_ParseBasebackupStderr_WithTimelineGreaterThanOne_UsesRealTimeline(t *testing.T) {
	stderr := `pg_basebackup: initiating base backup, waiting for checkpoint to complete
pg_basebackup: checkpoint completed
pg_basebackup: write-ahead log start point: 1D2/4A000028 on timeline 26
pg_basebackup: starting background WAL receiver
pg_basebackup: write-ahead log end point: 1D2/4A000100
pg_basebackup: base backup completed`

	startSeg, stopSeg, err := ParseBasebackupStderr(stderr)

	require.NoError(t, err)
	assert.Equal(t, "0000001A000001D20000004A", startSeg)
	assert.Equal(t, "0000001A000001D20000004A", stopSeg)
}

func Test_ParseBasebackupStderr_WhenTimelineMissing_FallsBackToOne(t *testing.T) {
	stderr := `pg_basebackup: write-ahead log start point: 0/2000028
pg_basebackup: write-ahead log end point: 0/2000100`

	startSeg, stopSeg, err := ParseBasebackupStderr(stderr)

	require.NoError(t, err)
	assert.Equal(t, "000000010000000000000002", startSeg)
	assert.Equal(t, "000000010000000000000002", stopSeg)
}

func Test_ParseBasebackupStderr_WhenStartLSNMissing_ReturnsError(t *testing.T) {
	stderr := `pg_basebackup: write-ahead log end point: 0/2000100
pg_basebackup: base backup completed`

	_, _, err := ParseBasebackupStderr(stderr)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse start WAL location")
}

func Test_ParseBasebackupStderr_WhenStopLSNMissing_ReturnsError(t *testing.T) {
	stderr := `pg_basebackup: write-ahead log start point: 0/2000028 on timeline 1
pg_basebackup: base backup completed`

	_, _, err := ParseBasebackupStderr(stderr)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse stop WAL location")
}

func Test_ParseBasebackupStderr_WhenEmptyStderr_ReturnsError(t *testing.T) {
	_, _, err := ParseBasebackupStderr("")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse start WAL location")
}

func Test_LSNToSegmentName_WithBoundaryValues_ConvertsCorrectly(t *testing.T) {
	tests := []struct {
		name     string
		lsn      string
		timeline uint32
		segSize  uint32
		expected string
	}{
		{
			name:     "first segment",
			lsn:      "0/1000000",
			timeline: 1,
			segSize:  16 * 1024 * 1024,
			expected: "000000010000000000000001",
		},
		{
			name:     "segment at boundary FF",
			lsn:      "0/FF000000",
			timeline: 1,
			segSize:  16 * 1024 * 1024,
			expected: "0000000100000000000000FF",
		},
		{
			name:     "segment in second log file",
			lsn:      "1/0",
			timeline: 1,
			segSize:  16 * 1024 * 1024,
			expected: "000000010000000100000000",
		},
		{
			name:     "segment with offset within 16MB",
			lsn:      "0/200ABCD",
			timeline: 1,
			segSize:  16 * 1024 * 1024,
			expected: "000000010000000000000002",
		},
		{
			name:     "zero LSN",
			lsn:      "0/0",
			timeline: 1,
			segSize:  16 * 1024 * 1024,
			expected: "000000010000000000000000",
		},
		{
			name:     "high timeline ID",
			lsn:      "0/1000000",
			timeline: 2,
			segSize:  16 * 1024 * 1024,
			expected: "000000020000000000000001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := LSNToSegmentName(tt.lsn, tt.timeline, tt.segSize)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_LSNToSegmentName_WithInvalidLSN_ReturnsError(t *testing.T) {
	tests := []struct {
		name string
		lsn  string
	}{
		{name: "no slash", lsn: "012345"},
		{name: "empty string", lsn: ""},
		{name: "invalid hex high", lsn: "GG/0"},
		{name: "invalid hex low", lsn: "0/ZZ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LSNToSegmentName(tt.lsn, 1, 16*1024*1024)

			require.Error(t, err)
		})
	}
}
