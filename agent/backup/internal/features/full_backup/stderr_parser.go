package full_backup

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const defaultWalSegmentSize uint32 = 16 * 1024 * 1024 // 16 MB

var (
	startLSNRegex = regexp.MustCompile(`write-ahead log start point: ([0-9A-Fa-f]+/[0-9A-Fa-f]+)`)
	stopLSNRegex  = regexp.MustCompile(`write-ahead log end point: ([0-9A-Fa-f]+/[0-9A-Fa-f]+)`)
	timelineRegex = regexp.MustCompile(`on timeline (\d+)`)
)

func ParseBasebackupStderr(stderr string) (startSegment, stopSegment string, err error) {
	startMatch := startLSNRegex.FindStringSubmatch(stderr)
	if len(startMatch) < 2 {
		return "", "", fmt.Errorf("failed to parse start WAL location from pg_basebackup stderr")
	}

	stopMatch := stopLSNRegex.FindStringSubmatch(stderr)
	if len(stopMatch) < 2 {
		return "", "", fmt.Errorf("failed to parse stop WAL location from pg_basebackup stderr")
	}

	timelineID, err := parseTimeline(stderr)
	if err != nil {
		return "", "", err
	}

	startSegment, err = LSNToSegmentName(startMatch[1], timelineID, defaultWalSegmentSize)
	if err != nil {
		return "", "", fmt.Errorf("failed to convert start LSN to segment name: %w", err)
	}

	stopSegment, err = LSNToSegmentName(stopMatch[1], timelineID, defaultWalSegmentSize)
	if err != nil {
		return "", "", fmt.Errorf("failed to convert stop LSN to segment name: %w", err)
	}

	return startSegment, stopSegment, nil
}

func parseTimeline(stderr string) (uint32, error) {
	match := timelineRegex.FindStringSubmatch(stderr)
	if len(match) < 2 {
		// pg_basebackup always prints "on timeline N" on the start point line; if it's
		// missing we fall back to 1 to preserve the pre-fix behavior for unusual outputs.
		return 1, nil
	}

	timeline, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid timeline in pg_basebackup stderr %q: %w", match[1], err)
	}

	return uint32(timeline), nil
}

func LSNToSegmentName(lsn string, timelineID, walSegmentSize uint32) (string, error) {
	high, low, err := parseLSN(lsn)
	if err != nil {
		return "", err
	}

	segmentsPerXLogID := uint32(0x100000000 / uint64(walSegmentSize))
	logID := high
	segmentOffset := low / walSegmentSize

	if segmentOffset >= segmentsPerXLogID {
		return "", fmt.Errorf("segment offset %d exceeds segments per XLogId %d", segmentOffset, segmentsPerXLogID)
	}

	return fmt.Sprintf("%08X%08X%08X", timelineID, logID, segmentOffset), nil
}

func parseLSN(lsn string) (high, low uint32, err error) {
	parts := strings.SplitN(lsn, "/", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid LSN format: %q (expected X/Y)", lsn)
	}

	highVal, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid LSN high part %q: %w", parts[0], err)
	}

	lowVal, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid LSN low part %q: %w", parts[1], err)
	}

	return uint32(highVal), uint32(lowVal), nil
}
