package wal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"databasus-agent/internal/config"
	"databasus-agent/internal/features/api"
)

var uploadIdleTimeout = 5 * time.Minute

const (
	pollInterval  = 10 * time.Second
	uploadTimeout = 5 * time.Minute
)

var segmentNameRegex = regexp.MustCompile(`^[0-9A-Fa-f]{24}$`)

type Streamer struct {
	cfg       *config.Config
	apiClient *api.Client
	log       *slog.Logger
}

func NewStreamer(cfg *config.Config, apiClient *api.Client, log *slog.Logger) *Streamer {
	return &Streamer{
		cfg:       cfg,
		apiClient: apiClient,
		log:       log,
	}
}

func (s *Streamer) Run(ctx context.Context) {
	s.log.Info("WAL streamer started", "pgWalDir", s.cfg.PgWalDir)

	s.processQueue(ctx)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("WAL streamer stopping")
			return
		case <-ticker.C:
			s.processQueue(ctx)
		}
	}
}

func (s *Streamer) processQueue(ctx context.Context) {
	segments, err := s.listSegments()
	if err != nil {
		s.log.Error("Failed to list WAL segments", "error", err)
		return
	}

	if len(segments) == 0 {
		s.log.Info("No WAL segments pending", "dir", s.cfg.PgWalDir)
		return
	}

	s.log.Info("WAL segments pending upload", "dir", s.cfg.PgWalDir, "count", len(segments))

	for _, segmentName := range segments {
		if ctx.Err() != nil {
			return
		}

		if err := s.uploadSegment(ctx, segmentName); err != nil {
			s.log.Error("Failed to upload WAL segment",
				"segment", segmentName,
				"error", err,
			)
			return
		}
	}
}

func (s *Streamer) listSegments() ([]string, error) {
	entries, err := os.ReadDir(s.cfg.PgWalDir)
	if err != nil {
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	var segments []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if strings.HasSuffix(name, ".tmp") {
			continue
		}

		if !segmentNameRegex.MatchString(name) {
			continue
		}

		segments = append(segments, name)
	}

	slices.Sort(segments)

	return segments, nil
}

func (s *Streamer) uploadSegment(ctx context.Context, segmentName string) error {
	filePath := filepath.Join(s.cfg.PgWalDir, segmentName)

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	go s.compressAndStream(pw, filePath)

	uploadCtx, timeoutCancel := context.WithTimeout(ctx, uploadTimeout)
	defer timeoutCancel()

	idleCtx, idleCancel := context.WithCancelCause(uploadCtx)
	defer idleCancel(nil)

	idleReader := api.NewIdleTimeoutReader(pr, uploadIdleTimeout, idleCancel)
	defer idleReader.Stop()

	s.log.Info("Uploading WAL segment", "segment", segmentName)

	result, err := s.apiClient.UploadWalSegment(idleCtx, segmentName, idleReader)
	if err != nil {
		if cause := context.Cause(idleCtx); cause != nil {
			return fmt.Errorf("upload WAL segment: %w", cause)
		}

		return err
	}

	if result.IsGapDetected {
		s.log.Warn("WAL chain gap detected",
			"segment", segmentName,
			"expected", result.ExpectedSegmentName,
			"received", result.ReceivedSegmentName,
		)

		return fmt.Errorf("gap detected for segment %s", segmentName)
	}

	s.log.Info("WAL segment uploaded", "segment", segmentName)

	if *s.cfg.IsDeleteWalAfterUpload {
		if err := os.Remove(filePath); err != nil {
			s.log.Warn("Failed to delete uploaded WAL segment",
				"segment", segmentName,
				"error", err,
			)
		}
	}

	return nil
}

func (s *Streamer) compressAndStream(pw *io.PipeWriter, filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		_ = pw.CloseWithError(fmt.Errorf("open file: %w", err))
		return
	}
	defer func() { _ = f.Close() }()

	encoder, err := zstd.NewWriter(pw,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(5)),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		_ = pw.CloseWithError(fmt.Errorf("create zstd encoder: %w", err))
		return
	}

	if _, err := io.Copy(encoder, f); err != nil {
		_ = encoder.Close()
		_ = pw.CloseWithError(fmt.Errorf("compress: %w", err))
		return
	}

	if err := encoder.Close(); err != nil {
		_ = pw.CloseWithError(fmt.Errorf("close encoder: %w", err))
		return
	}

	_ = pw.Close()
}
