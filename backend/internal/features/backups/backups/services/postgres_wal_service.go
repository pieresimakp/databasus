package backups_services

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_dto "databasus-backend/internal/features/backups/backups/dto"
	backup_encryption "databasus-backend/internal/features/backups/backups/encryption"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/databases/databases/postgresql"
	encryption_secrets "databasus-backend/internal/features/encryption/secrets"
	util_encryption "databasus-backend/internal/util/encryption"
	util_wal "databasus-backend/internal/util/wal"
)

// PostgreWalBackupService handles WAL segment and basebackup uploads from the databasus-cli agent.
type PostgreWalBackupService struct {
	backupConfigService *backups_config.BackupConfigService
	backupRepository    *backups_core.BackupRepository
	fieldEncryptor      util_encryption.FieldEncryptor
	secretKeyService    *encryption_secrets.SecretKeyService
	logger              *slog.Logger
	backupService       *BackupService
}

// UploadWalSegment accepts a streaming WAL segment upload from the agent.
// WAL segments are accepted unconditionally.
func (s *PostgreWalBackupService) UploadWalSegment(
	ctx context.Context,
	database *databases.Database,
	walSegmentName string,
	body io.Reader,
) error {
	uploadStart := time.Now().UTC()

	if err := s.validateWalBackupType(database); err != nil {
		return err
	}

	backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(database.ID)
	if err != nil {
		return fmt.Errorf("failed to get backup config: %w", err)
	}

	if backupConfig.Storage == nil {
		return fmt.Errorf("no storage configured for database %s", database.ID)
	}

	existing, err := s.backupRepository.FindWalSegmentByName(database.ID, walSegmentName)
	if err != nil {
		return fmt.Errorf("failed to check for duplicate WAL segment: %w", err)
	}

	if existing != nil {
		return nil
	}

	backup := s.createWalSegmentRecord(
		database.ID,
		backupConfig.Storage.ID,
		database.Name,
		walSegmentName,
		backupConfig.Encryption,
	)

	if err := s.backupRepository.Save(backup); err != nil {
		return fmt.Errorf("failed to create backup record: %w", err)
	}

	inputCounter := &countingReader{r: body}
	progressDone := make(chan struct{})
	go s.startProgressTracker(backup, inputCounter, uploadStart, progressDone)

	sizeBytes, streamErr := s.streamToStorage(ctx, backup, backupConfig, inputCounter)
	close(progressDone)

	if streamErr != nil {
		errMsg := streamErr.Error()
		backup.BackupDurationMs = time.Since(uploadStart).Milliseconds()
		s.markFailed(backup, errMsg)

		return fmt.Errorf("upload failed: %w", streamErr)
	}

	backup.BackupDurationMs = time.Since(uploadStart).Milliseconds()
	s.markCompleted(backup, sizeBytes)

	return nil
}

// UploadBasebackup accepts a streaming basebackup upload from the agent (Phase 1).
// The backup stays IN_PROGRESS with UploadCompletedAt set after streaming finishes.
// The agent must call FinalizeBasebackup (Phase 2) with WAL segment names to complete.
func (s *PostgreWalBackupService) UploadBasebackup(
	ctx context.Context,
	database *databases.Database,
	body io.Reader,
) (uuid.UUID, error) {
	uploadStart := time.Now().UTC()

	if err := s.validateWalBackupType(database); err != nil {
		return uuid.Nil, err
	}

	backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(database.ID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get backup config: %w", err)
	}

	if backupConfig.Storage == nil {
		return uuid.Nil, fmt.Errorf("no storage configured for database %s", database.ID)
	}

	backup := s.createBasebackupRecord(
		database.ID,
		backupConfig.Storage.ID,
		database.Name,
		backupConfig.Encryption,
	)

	if err := s.backupRepository.Save(backup); err != nil {
		return uuid.Nil, fmt.Errorf("failed to create backup record: %w", err)
	}

	inputCounter := &countingReader{r: body}
	progressDone := make(chan struct{})
	go s.startProgressTracker(backup, inputCounter, uploadStart, progressDone)

	sizeBytes, streamErr := s.streamToStorage(ctx, backup, backupConfig, inputCounter)
	close(progressDone)

	if streamErr != nil {
		errMsg := streamErr.Error()
		backup.BackupDurationMs = time.Since(uploadStart).Milliseconds()
		s.markFailed(backup, errMsg)

		return uuid.Nil, fmt.Errorf("upload failed: %w", streamErr)
	}

	now := time.Now().UTC()
	backup.UploadCompletedAt = &now
	backup.BackupSizeMb = float64(sizeBytes) / (1024 * 1024)
	backup.BackupDurationMs = time.Since(uploadStart).Milliseconds()

	if err := s.backupRepository.Save(backup); err != nil {
		return uuid.Nil, fmt.Errorf("failed to update backup after upload: %w", err)
	}

	return backup.ID, nil
}

// FinalizeBasebackup completes a previously uploaded basebackup (Phase 2).
// Sets WAL segment names and marks the backup as COMPLETED, or marks it FAILED if errorMsg is provided.
func (s *PostgreWalBackupService) FinalizeBasebackup(
	database *databases.Database,
	backupID uuid.UUID,
	startSegment string,
	stopSegment string,
	errorMsg *string,
) error {
	if err := s.validateWalBackupType(database); err != nil {
		return err
	}

	backup, err := s.backupRepository.FindByID(backupID)
	if err != nil {
		return fmt.Errorf("backup not found: %w", err)
	}

	if backup.DatabaseID != database.ID {
		return fmt.Errorf("backup does not belong to this database")
	}

	if backup.Status != backups_core.BackupStatusInProgress || backup.UploadCompletedAt == nil {
		return fmt.Errorf("backup is not awaiting finalization")
	}

	if errorMsg != nil {
		s.markFailed(backup, *errorMsg)

		return nil
	}

	backup.PgFullBackupWalStartSegmentName = &startSegment
	backup.PgFullBackupWalStopSegmentName = &stopSegment
	backup.Status = backups_core.BackupStatusCompleted

	if err := s.backupRepository.Save(backup); err != nil {
		return fmt.Errorf("failed to finalize backup: %w", err)
	}

	return nil
}

func (s *PostgreWalBackupService) GetRestorePlan(
	database *databases.Database,
	backupID *uuid.UUID,
) (*backups_dto.GetRestorePlanResponse, *backups_dto.GetRestorePlanErrorResponse, error) {
	if err := s.validateWalBackupType(database); err != nil {
		return nil, nil, err
	}

	fullBackup, err := s.resolveFullBackup(database.ID, backupID)
	if err != nil {
		return nil, nil, err
	}

	if fullBackup == nil {
		msg := "no full backups available for this database"
		if backupID != nil {
			msg = fmt.Sprintf("full backup %s not found or not completed", backupID)
		}

		return nil, &backups_dto.GetRestorePlanErrorResponse{
			Error:   "no_backups",
			Message: msg,
		}, nil
	}

	startSegment := ""
	if fullBackup.PgFullBackupWalStartSegmentName != nil {
		startSegment = *fullBackup.PgFullBackupWalStartSegmentName
	}

	walSegments, err := s.backupRepository.FindCompletedWalSegmentsAfter(database.ID, startSegment)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query WAL segments: %w", err)
	}

	chainErr := s.validateRestoreWalChain(fullBackup, walSegments)
	if chainErr != nil {
		return nil, chainErr, nil
	}

	fullBackupSizeBytes := int64(fullBackup.BackupSizeMb * 1024 * 1024)

	pgVersion := ""
	if fullBackup.PgVersion != nil {
		pgVersion = *fullBackup.PgVersion
	}

	stopSegment := ""
	if fullBackup.PgFullBackupWalStopSegmentName != nil {
		stopSegment = *fullBackup.PgFullBackupWalStopSegmentName
	}

	response := &backups_dto.GetRestorePlanResponse{
		FullBackup: backups_dto.RestorePlanFullBackup{
			BackupID:                  fullBackup.ID,
			FullBackupWalStartSegment: startSegment,
			FullBackupWalStopSegment:  stopSegment,
			PgVersion:                 pgVersion,
			CreatedAt:                 fullBackup.CreatedAt,
			SizeBytes:                 fullBackupSizeBytes,
		},
		TotalSizeBytes: fullBackupSizeBytes,
	}

	for _, seg := range walSegments {
		segName := ""
		if seg.PgWalSegmentName != nil {
			segName = *seg.PgWalSegmentName
		}

		segSizeBytes := int64(seg.BackupSizeMb * 1024 * 1024)

		response.WalSegments = append(response.WalSegments, backups_dto.RestorePlanWalSegment{
			BackupID:    seg.ID,
			SegmentName: segName,
			SizeBytes:   segSizeBytes,
		})

		response.TotalSizeBytes += segSizeBytes
		response.LatestAvailableSegment = segName
	}

	return response, nil, nil
}

// DownloadBackupFile returns a reader for a backup file belonging to the given database.
// Decryption is handled transparently if the backup is encrypted.
func (s *PostgreWalBackupService) DownloadBackupFile(
	database *databases.Database,
	backupID uuid.UUID,
) (io.ReadCloser, error) {
	if err := s.validateWalBackupType(database); err != nil {
		return nil, err
	}

	backup, err := s.backupRepository.FindByID(backupID)
	if err != nil {
		return nil, fmt.Errorf("backup not found: %w", err)
	}

	if backup.DatabaseID != database.ID {
		return nil, fmt.Errorf("backup does not belong to this database")
	}

	if backup.Status != backups_core.BackupStatusCompleted {
		return nil, fmt.Errorf("backup is not completed")
	}

	return s.backupService.GetBackupReader(backupID)
}

func (s *PostgreWalBackupService) GetNextFullBackupTime(
	database *databases.Database,
) (*backups_dto.GetNextFullBackupTimeResponse, error) {
	if err := s.validateWalBackupType(database); err != nil {
		return nil, err
	}

	backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(database.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup config: %w", err)
	}

	if backupConfig.BackupInterval == nil {
		return nil, fmt.Errorf("no backup interval configured for database %s", database.ID)
	}

	lastFullBackup, err := s.backupRepository.FindLastCompletedFullWalBackupByDatabaseID(
		database.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query last full backup: %w", err)
	}

	var lastBackupTime *time.Time
	if lastFullBackup != nil {
		lastBackupTime = &lastFullBackup.CreatedAt
	}

	now := time.Now().UTC()
	nextTime := backupConfig.BackupInterval.NextTriggerTime(now, lastBackupTime)

	return &backups_dto.GetNextFullBackupTimeResponse{
		NextFullBackupTime: nextTime,
	}, nil
}

// ReportError creates a FAILED backup record with the agent's error message.
func (s *PostgreWalBackupService) ReportError(
	database *databases.Database,
	errorMsg string,
) error {
	if err := s.validateWalBackupType(database); err != nil {
		return err
	}

	backupConfig, err := s.backupConfigService.GetBackupConfigByDbId(database.ID)
	if err != nil {
		return fmt.Errorf("failed to get backup config: %w", err)
	}

	if backupConfig.Storage == nil {
		return fmt.Errorf("no storage configured for database %s", database.ID)
	}

	now := time.Now().UTC()
	backup := &backups_core.Backup{
		ID:          uuid.New(),
		DatabaseID:  database.ID,
		StorageID:   backupConfig.Storage.ID,
		Status:      backups_core.BackupStatusFailed,
		FailMessage: &errorMsg,
		Encryption:  backupConfig.Encryption,
		CreatedAt:   now,
	}

	backup.GenerateFilename(database.Name)

	if err := s.backupRepository.Save(backup); err != nil {
		return fmt.Errorf("failed to save error backup record: %w", err)
	}

	return nil
}

// IsWalChainValid checks whether the WAL chain is continuous since the last completed full backup.
func (s *PostgreWalBackupService) IsWalChainValid(
	database *databases.Database,
) (*backups_dto.IsWalChainValidResponse, error) {
	if err := s.validateWalBackupType(database); err != nil {
		return nil, err
	}

	fullBackup, err := s.backupRepository.FindLastCompletedFullWalBackupByDatabaseID(database.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to query full backup: %w", err)
	}

	if fullBackup == nil || fullBackup.PgFullBackupWalStopSegmentName == nil {
		return &backups_dto.IsWalChainValidResponse{
			IsValid: false,
			Error:   "no_full_backup",
		}, nil
	}

	startSegment := ""
	if fullBackup.PgFullBackupWalStartSegmentName != nil {
		startSegment = *fullBackup.PgFullBackupWalStartSegmentName
	}

	walSegments, err := s.backupRepository.FindCompletedWalSegmentsAfter(database.ID, startSegment)
	if err != nil {
		return nil, fmt.Errorf("failed to query WAL segments: %w", err)
	}

	chainErr := s.validateRestoreWalChain(fullBackup, walSegments)
	if chainErr != nil {
		return &backups_dto.IsWalChainValidResponse{
			IsValid:               false,
			Error:                 chainErr.Error,
			LastContiguousSegment: chainErr.LastContiguousSegment,
		}, nil
	}

	return &backups_dto.IsWalChainValidResponse{
		IsValid: true,
	}, nil
}

func (s *PostgreWalBackupService) createBasebackupRecord(
	databaseID uuid.UUID,
	storageID uuid.UUID,
	dbName string,
	encryption backups_config.BackupEncryption,
) *backups_core.Backup {
	now := time.Now().UTC()
	walBackupType := backups_core.PgWalBackupTypeFullBackup

	backup := &backups_core.Backup{
		ID:              uuid.New(),
		DatabaseID:      databaseID,
		StorageID:       storageID,
		Status:          backups_core.BackupStatusInProgress,
		PgWalBackupType: &walBackupType,
		Encryption:      encryption,
		CreatedAt:       now,
	}

	backup.GenerateFilename(dbName)

	return backup
}

func (s *PostgreWalBackupService) createWalSegmentRecord(
	databaseID uuid.UUID,
	storageID uuid.UUID,
	dbName string,
	walSegmentName string,
	encryption backups_config.BackupEncryption,
) *backups_core.Backup {
	now := time.Now().UTC()
	walBackupType := backups_core.PgWalBackupTypeWalSegment

	backup := &backups_core.Backup{
		ID:               uuid.New(),
		DatabaseID:       databaseID,
		StorageID:        storageID,
		Status:           backups_core.BackupStatusInProgress,
		PgWalBackupType:  &walBackupType,
		PgWalSegmentName: &walSegmentName,
		Encryption:       encryption,
		CreatedAt:        now,
	}

	backup.GenerateFilename(dbName)

	return backup
}

func (s *PostgreWalBackupService) streamToStorage(
	ctx context.Context,
	backup *backups_core.Backup,
	backupConfig *backups_config.BackupConfig,
	body io.Reader,
) (int64, error) {
	if backupConfig.Encryption == backups_config.BackupEncryptionEncrypted {
		return s.streamEncrypted(ctx, backup, backupConfig, body, backup.FileName)
	}

	return s.streamDirect(ctx, backupConfig, body, backup.FileName)
}

func (s *PostgreWalBackupService) streamDirect(
	ctx context.Context,
	backupConfig *backups_config.BackupConfig,
	body io.Reader,
	fileName string,
) (int64, error) {
	cr := &countingReader{r: body}

	if err := backupConfig.Storage.SaveFile(ctx, s.fieldEncryptor, s.logger, fileName, cr); err != nil {
		return 0, err
	}

	return cr.n.Load(), nil
}

func (s *PostgreWalBackupService) streamEncrypted(
	ctx context.Context,
	backup *backups_core.Backup,
	backupConfig *backups_config.BackupConfig,
	body io.Reader,
	fileName string,
) (int64, error) {
	masterKey, err := s.secretKeyService.GetSecretKey()
	if err != nil {
		return 0, fmt.Errorf("failed to get master encryption key: %w", err)
	}

	pipeReader, pipeWriter := io.Pipe()

	encryptionSetup, err := backup_encryption.SetupEncryptionWriter(
		pipeWriter,
		masterKey,
		backup.ID,
	)
	if err != nil {
		_ = pipeWriter.Close()
		return 0, err
	}

	copyErrCh := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(encryptionSetup.Writer, body)
		if copyErr != nil {
			_ = encryptionSetup.Writer.Close()
			_ = pipeWriter.CloseWithError(copyErr)
			copyErrCh <- copyErr
			return
		}

		if closeErr := encryptionSetup.Writer.Close(); closeErr != nil {
			_ = pipeWriter.CloseWithError(closeErr)
			copyErrCh <- closeErr
			return
		}

		copyErrCh <- pipeWriter.Close()
	}()

	cr := &countingReader{r: pipeReader}
	saveErr := backupConfig.Storage.SaveFile(ctx, s.fieldEncryptor, s.logger, fileName, cr)
	copyErr := <-copyErrCh

	if copyErr != nil {
		return 0, copyErr
	}

	if saveErr != nil {
		return 0, saveErr
	}

	backup.EncryptionSalt = &encryptionSetup.SaltBase64
	backup.EncryptionIV = &encryptionSetup.NonceBase64

	return cr.n.Load(), nil
}

func (s *PostgreWalBackupService) markCompleted(backup *backups_core.Backup, sizeBytes int64) {
	backup.Status = backups_core.BackupStatusCompleted
	backup.BackupSizeMb = float64(sizeBytes) / (1024 * 1024)

	if err := s.backupRepository.Save(backup); err != nil {
		s.logger.Error(
			"failed to mark WAL backup as completed",
			"backupId",
			backup.ID,
			"error",
			err,
		)
	}
}

func (s *PostgreWalBackupService) startProgressTracker(
	backup *backups_core.Backup,
	inputCounter *countingReader,
	uploadStart time.Time,
	done <-chan struct{},
) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			backup.BackupDurationMs = time.Since(uploadStart).Milliseconds()
			backup.BackupSizeMb = float64(inputCounter.n.Load()) / (1024 * 1024)

			if err := s.backupRepository.Save(backup); err != nil {
				s.logger.Error("failed to update backup progress",
					"backupId", backup.ID, "error", err)
			}
		}
	}
}

func (s *PostgreWalBackupService) markFailed(backup *backups_core.Backup, errMsg string) {
	backup.Status = backups_core.BackupStatusFailed
	backup.FailMessage = &errMsg

	if err := s.backupRepository.Save(backup); err != nil {
		s.logger.Error("failed to mark WAL backup as failed", "backupId", backup.ID, "error", err)
	}
}

func (s *PostgreWalBackupService) resolveFullBackup(
	databaseID uuid.UUID,
	backupID *uuid.UUID,
) (*backups_core.Backup, error) {
	if backupID == nil {
		return s.backupRepository.FindLastCompletedFullWalBackupByDatabaseID(databaseID)
	}

	fullBackup, err := s.backupRepository.FindCompletedFullWalBackupByID(databaseID, *backupID)
	if err != nil {
		return nil, err
	}

	if fullBackup != nil {
		return fullBackup, nil
	}

	backup, err := s.backupRepository.FindByID(*backupID)
	if err != nil {
		return nil, nil
	}

	if backup.DatabaseID != databaseID ||
		backup.Status != backups_core.BackupStatusCompleted ||
		backup.PgWalBackupType == nil ||
		*backup.PgWalBackupType != backups_core.PgWalBackupTypeWalSegment {
		return nil, nil
	}

	return s.backupRepository.FindLatestCompletedFullWalBackupBefore(databaseID, backup.CreatedAt)
}

func (s *PostgreWalBackupService) validateRestoreWalChain(
	fullBackup *backups_core.Backup,
	walSegments []*backups_core.Backup,
) *backups_dto.GetRestorePlanErrorResponse {
	if len(walSegments) == 0 {
		return nil
	}

	stopSegment := ""
	if fullBackup.PgFullBackupWalStopSegmentName != nil {
		stopSegment = *fullBackup.PgFullBackupWalStopSegmentName
	}

	walCalculator := util_wal.NewWalCalculator(0)
	expectedNext, err := walCalculator.NextSegment(stopSegment)
	if err != nil {
		return nil
	}

	for _, seg := range walSegments {
		segName := ""
		if seg.PgWalSegmentName != nil {
			segName = *seg.PgWalSegmentName
		}

		cmp, cmpErr := walCalculator.Compare(segName, stopSegment)
		if cmpErr != nil {
			return nil
		}

		// Skip segments that are <= stopSegment (they are part of the basebackup range)
		if cmp <= 0 {
			continue
		}

		if segName != expectedNext {
			lastContiguous := stopSegment
			// Walk back to find the segment before the gap
			for _, prev := range walSegments {
				prevName := ""
				if prev.PgWalSegmentName != nil {
					prevName = *prev.PgWalSegmentName
				}

				prevCmp, _ := walCalculator.Compare(prevName, stopSegment)
				if prevCmp <= 0 {
					continue
				}

				if prevName == segName {
					break
				}

				lastContiguous = prevName
			}

			return &backups_dto.GetRestorePlanErrorResponse{
				Error: "wal_chain_broken",
				Message: fmt.Sprintf(
					"WAL chain has a gap after segment %s. Recovery is only possible up to this segment.",
					lastContiguous,
				),
				LastContiguousSegment: lastContiguous,
			}
		}

		expectedNext, err = walCalculator.NextSegment(segName)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (s *PostgreWalBackupService) validateWalBackupType(database *databases.Database) error {
	if database.Postgresql == nil ||
		database.Postgresql.BackupType != postgresql.PostgresBackupTypeWalV1 {
		return fmt.Errorf("database %s is not configured for WAL backups", database.ID)
	}
	return nil
}

type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n.Add(int64(n))

	return n, err
}
