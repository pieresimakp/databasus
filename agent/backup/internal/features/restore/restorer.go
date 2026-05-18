package restore

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"databasus-agent/internal/features/api"
)

const (
	walRestoreDir            = "databasus-wal-restore"
	maxRetryAttempts         = 3
	retryBaseDelay           = 1 * time.Second
	recoverySignalFile       = "recovery.signal"
	autoConfFile             = "postgresql.auto.conf"
	dockerContainerPgDataDir = "/var/lib/postgresql/data"
)

var retryDelayOverride *time.Duration

type Restorer struct {
	apiClient       *api.Client
	log             *slog.Logger
	targetPgDataDir string
	backupID        string
	targetTime      string
	pgType          string
}

func NewRestorer(
	apiClient *api.Client,
	log *slog.Logger,
	targetPgDataDir string,
	backupID string,
	targetTime string,
	pgType string,
) *Restorer {
	return &Restorer{
		apiClient,
		log,
		targetPgDataDir,
		backupID,
		targetTime,
		pgType,
	}
}

func (r *Restorer) Run(ctx context.Context) error {
	var parsedTargetTime *time.Time

	if r.targetTime != "" {
		parsed, err := time.Parse(time.RFC3339, r.targetTime)
		if err != nil {
			return fmt.Errorf("invalid --target-time format (expected RFC3339, e.g. 2026-02-28T14:30:00Z): %w", err)
		}

		parsedTargetTime = &parsed
	}

	if err := r.validateTargetPgDataDir(); err != nil {
		return err
	}

	plan, err := r.getRestorePlanFromServer(ctx)
	if err != nil {
		return err
	}

	r.logRestorePlan(plan, parsedTargetTime)

	r.log.Info("Downloading and extracting basebackup...")
	if err := r.downloadAndExtractBasebackup(ctx, plan.FullBackup.BackupID); err != nil {
		return fmt.Errorf("basebackup download failed: %w", err)
	}
	r.log.Info("Basebackup extracted successfully")

	if err := r.downloadAllWalSegments(ctx, plan.WalSegments); err != nil {
		return err
	}

	if err := r.configurePostgresRecovery(parsedTargetTime); err != nil {
		return fmt.Errorf("failed to configure recovery: %w", err)
	}

	if err := os.Chmod(r.targetPgDataDir, 0o700); err != nil {
		return fmt.Errorf("set PGDATA permissions: %w", err)
	}

	r.printCompletionMessage()

	return nil
}

func (r *Restorer) validateTargetPgDataDir() error {
	info, err := os.Stat(r.targetPgDataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("target pgdata directory does not exist: %s", r.targetPgDataDir)
		}

		return fmt.Errorf("cannot access target pgdata directory: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("target pgdata path is not a directory: %s", r.targetPgDataDir)
	}

	entries, err := os.ReadDir(r.targetPgDataDir)
	if err != nil {
		return fmt.Errorf("cannot read target pgdata directory: %w", err)
	}

	if len(entries) > 0 {
		return fmt.Errorf("target pgdata directory is not empty: %s", r.targetPgDataDir)
	}

	return nil
}

func (r *Restorer) getRestorePlanFromServer(ctx context.Context) (*api.GetRestorePlanResponse, error) {
	plan, planErr, err := r.apiClient.GetRestorePlan(ctx, r.backupID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch restore plan: %w", err)
	}

	if planErr != nil {
		if planErr.LastContiguousSegment != "" {
			return nil, fmt.Errorf("restore plan error: %s (last contiguous segment: %s)",
				planErr.Message, planErr.LastContiguousSegment)
		}

		return nil, fmt.Errorf("restore plan error: %s", planErr.Message)
	}

	return plan, nil
}

func (r *Restorer) logRestorePlan(plan *api.GetRestorePlanResponse, parsedTargetTime *time.Time) {
	recoveryTarget := "full recovery (all available WAL)"
	if parsedTargetTime != nil {
		recoveryTarget = parsedTargetTime.Format(time.RFC3339)
	}

	r.log.Info("Restore plan",
		"fullBackupID", plan.FullBackup.BackupID,
		"fullBackupCreatedAt", plan.FullBackup.CreatedAt.Format(time.RFC3339),
		"pgVersion", plan.FullBackup.PgVersion,
		"walSegmentCount", len(plan.WalSegments),
		"totalDownloadSize", formatSizeBytes(plan.TotalSizeBytes),
		"latestAvailableSegment", plan.LatestAvailableSegment,
		"recoveryTarget", recoveryTarget,
	)
}

func (r *Restorer) downloadAndExtractBasebackup(ctx context.Context, backupID string) error {
	body, err := r.apiClient.DownloadBackupFile(ctx, backupID)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	zstdReader, err := zstd.NewReader(body)
	if err != nil {
		return fmt.Errorf("create zstd decompressor: %w", err)
	}
	defer zstdReader.Close()

	tarReader := tar.NewReader(zstdReader)

	return r.extractTarArchive(tarReader)
}

func (r *Restorer) extractTarArchive(tarReader *tar.Reader) error {
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}

		targetPath := filepath.Join(r.targetPgDataDir, header.Name)

		relativePath, err := filepath.Rel(r.targetPgDataDir, targetPath)
		if err != nil || strings.HasPrefix(relativePath, "..") {
			return fmt.Errorf("tar entry attempts path traversal: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("create directory %s: %w", header.Name, err)
			}

		case tar.TypeReg:
			parentDir := filepath.Dir(targetPath)
			if err := os.MkdirAll(parentDir, 0o755); err != nil {
				return fmt.Errorf("create parent directory for %s: %w", header.Name, err)
			}

			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("create file %s: %w", header.Name, err)
			}

			if _, err := io.Copy(file, tarReader); err != nil {
				_ = file.Close()
				return fmt.Errorf("write file %s: %w", header.Name, err)
			}

			_ = file.Close()

		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				return fmt.Errorf("create symlink %s: %w", header.Name, err)
			}

		case tar.TypeLink:
			linkTarget := filepath.Join(r.targetPgDataDir, header.Linkname)
			if err := os.Link(linkTarget, targetPath); err != nil {
				return fmt.Errorf("create hard link %s: %w", header.Name, err)
			}

		default:
			r.log.Warn("Skipping unsupported tar entry type",
				"name", header.Name,
				"type", header.Typeflag,
			)
		}
	}
}

func (r *Restorer) downloadAllWalSegments(ctx context.Context, segments []api.RestorePlanWalSegment) error {
	walRestorePath := filepath.Join(r.targetPgDataDir, walRestoreDir)
	if err := os.MkdirAll(walRestorePath, 0o755); err != nil {
		return fmt.Errorf("create WAL restore directory: %w", err)
	}

	for segmentIndex, segment := range segments {
		if err := r.downloadWalSegmentWithRetry(ctx, segment, segmentIndex, len(segments)); err != nil {
			return err
		}
	}

	return nil
}

func (r *Restorer) downloadWalSegmentWithRetry(
	ctx context.Context,
	segment api.RestorePlanWalSegment,
	segmentIndex int,
	segmentsTotal int,
) error {
	r.log.Info("Downloading WAL segment",
		"segment", segment.SegmentName,
		"progress", fmt.Sprintf("%d/%d", segmentIndex+1, segmentsTotal),
	)

	var lastErr error

	for attempt := range maxRetryAttempts {
		if err := r.downloadWalSegment(ctx, segment); err != nil {
			lastErr = err

			delay := r.getRetryDelay(attempt)
			r.log.Warn("WAL segment download failed, retrying",
				"segment", segment.SegmentName,
				"attempt", attempt+1,
				"maxAttempts", maxRetryAttempts,
				"retryDelay", delay,
				"error", err,
			)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}

		return nil
	}

	return fmt.Errorf("failed to download WAL segment %s after %d attempts: %w",
		segment.SegmentName, maxRetryAttempts, lastErr)
}

func (r *Restorer) downloadWalSegment(ctx context.Context, segment api.RestorePlanWalSegment) error {
	body, err := r.apiClient.DownloadBackupFile(ctx, segment.BackupID)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	zstdReader, err := zstd.NewReader(body)
	if err != nil {
		return fmt.Errorf("create zstd decompressor: %w", err)
	}
	defer zstdReader.Close()

	segmentPath := filepath.Join(r.targetPgDataDir, walRestoreDir, segment.SegmentName)

	file, err := os.Create(segmentPath)
	if err != nil {
		return fmt.Errorf("create WAL segment file: %w", err)
	}
	defer func() { _ = file.Close() }()

	if _, err := io.Copy(file, zstdReader); err != nil {
		return fmt.Errorf("write WAL segment: %w", err)
	}

	return nil
}

func (r *Restorer) configurePostgresRecovery(parsedTargetTime *time.Time) error {
	recoverySignalPath := filepath.Join(r.targetPgDataDir, recoverySignalFile)
	if err := os.WriteFile(recoverySignalPath, []byte{}, 0o644); err != nil {
		return fmt.Errorf("create recovery.signal: %w", err)
	}

	walRestoreAbsPath, err := r.resolveWalRestorePath()
	if err != nil {
		return err
	}

	autoConfPath := filepath.Join(r.targetPgDataDir, autoConfFile)

	autoConfFile, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open postgresql.auto.conf: %w", err)
	}
	defer func() { _ = autoConfFile.Close() }()

	var configLines strings.Builder
	configLines.WriteString("\n# Added by databasus-agent restore\n")
	fmt.Fprintf(&configLines, "restore_command = 'cp %s/%%f %%p'\n", walRestoreAbsPath)
	fmt.Fprintf(&configLines, "recovery_end_command = 'rm -rf %s'\n", walRestoreAbsPath)
	configLines.WriteString("recovery_target_action = 'promote'\n")

	if parsedTargetTime != nil {
		fmt.Fprintf(&configLines, "recovery_target_time = '%s'\n", parsedTargetTime.Format(time.RFC3339))
	}

	if _, err := autoConfFile.WriteString(configLines.String()); err != nil {
		return fmt.Errorf("write to postgresql.auto.conf: %w", err)
	}

	return nil
}

func (r *Restorer) printCompletionMessage() {
	absPgDataDir, _ := filepath.Abs(r.targetPgDataDir)
	isDocker := r.pgType == "docker"

	fmt.Printf("\nRestore complete. PGDATA directory is ready at %s.\n", absPgDataDir)

	fmt.Print(`
What happens when you start PostgreSQL:
  1. PostgreSQL detects recovery.signal and enters recovery mode
  2. It replays WAL from the basebackup's consistency point
  3. It executes restore_command to fetch WAL segments from databasus-wal-restore/
  4. WAL replay continues until target_time (if PITR) or end of available WAL
  5. recovery_end_command automatically removes databasus-wal-restore/
  6. PostgreSQL promotes to primary and removes recovery.signal
  7. Normal operations resume
`)

	if isDocker {
		fmt.Printf(`
Start PostgreSQL by launching a container with the restored data mounted:
  docker run -d -v %s:%s postgres:<VERSION>

Or if you have an existing container:
  docker start <CONTAINER_NAME>

Ensure %s is mounted as the container's pgdata volume at %s.
`, absPgDataDir, dockerContainerPgDataDir, absPgDataDir, dockerContainerPgDataDir)
	} else {
		fmt.Printf(`
Start PostgreSQL:
  pg_ctl -D %s start

Note: If you move the PGDATA directory before starting PostgreSQL,
update restore_command and recovery_end_command paths in
postgresql.auto.conf accordingly.
`, absPgDataDir)
	}
}

func (r *Restorer) resolveWalRestorePath() (string, error) {
	if r.pgType == "docker" {
		return dockerContainerPgDataDir + "/" + walRestoreDir, nil
	}

	absPgDataDir, err := filepath.Abs(r.targetPgDataDir)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path: %w", err)
	}

	absPgDataDir = filepath.ToSlash(absPgDataDir)

	return absPgDataDir + "/" + walRestoreDir, nil
}

func (r *Restorer) getRetryDelay(attempt int) time.Duration {
	if retryDelayOverride != nil {
		return *retryDelayOverride
	}

	return retryBaseDelay * time.Duration(1<<attempt)
}

func formatSizeBytes(sizeBytes int64) string {
	const (
		kilobyte = 1024
		megabyte = 1024 * kilobyte
		gigabyte = 1024 * megabyte
	)

	switch {
	case sizeBytes >= gigabyte:
		return fmt.Sprintf("%.2f GB", float64(sizeBytes)/float64(gigabyte))
	case sizeBytes >= megabyte:
		return fmt.Sprintf("%.2f MB", float64(sizeBytes)/float64(megabyte))
	case sizeBytes >= kilobyte:
		return fmt.Sprintf("%.2f KB", float64(sizeBytes)/float64(kilobyte))
	default:
		return fmt.Sprintf("%d B", sizeBytes)
	}
}
