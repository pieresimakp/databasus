package backups_controllers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_dto "databasus-backend/internal/features/backups/backups/dto"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/databases/databases/postgresql"
	"databasus-backend/internal/features/intervals"
	"databasus-backend/internal/features/storages"
	local_storage "databasus-backend/internal/features/storages/models/local"
	users_enums "databasus-backend/internal/features/users/enums"
	users_testing "databasus-backend/internal/features/users/testing"
	workspaces_controllers "databasus-backend/internal/features/workspaces/controllers"
	workspaces_testing "databasus-backend/internal/features/workspaces/testing"
	test_utils "databasus-backend/internal/util/testing"
)

func Test_WalUpload_InProgressStatusSetBeforeStream(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	// Upload a completed full backup so WAL upload chain validation passes.
	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	pr, pw := io.Pipe()
	req := newWalSegmentUploadRequest(pr, agentToken, "000000010000000100000011")

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(w, req)
		close(done)
	}()

	// The SaveFile call blocks until the body reader is closed — check status while it's open.
	time.Sleep(150 * time.Millisecond)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)
	require.NotEmpty(t, backups)
	assert.Equal(t, backups_core.BackupStatusInProgress, backups[0].Status)

	// Allow the upload to finish.
	_ = pw.Close()
	<-done
}

func Test_WalUpload_CompletedStatusAfterSuccessfulStream(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	body := bytes.NewReader([]byte("wal segment content"))
	req := newWalSegmentUploadRequest(body, agentToken, "000000010000000100000011")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)

	WaitForBackupCompletion(t, db.ID, 1, 5*time.Second)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	var walBackup *backups_core.Backup
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment {
			walBackup = b
			break
		}
	}

	require.NotNil(t, walBackup)
	assert.Equal(t, backups_core.BackupStatusCompleted, walBackup.Status)
}

func Test_WalUpload_FailedStatusWithErrorOnStreamError(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	pr, pw := io.Pipe()
	req := newWalSegmentUploadRequest(pr, agentToken, "000000010000000100000011")

	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(w, req)
		close(done)
	}()

	// Simulate a body read error mid-stream.
	_ = pw.CloseWithError(errors.New("simulated network error"))
	<-done

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	var walBackup *backups_core.Backup
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment {
			walBackup = b
			break
		}
	}

	require.NotNil(t, walBackup)
	assert.Equal(t, backups_core.BackupStatusFailed, walBackup.Status)
	assert.NotNil(t, walBackup.FailMessage)
}

func Test_WalUpload_Basebackup_StreamingUpload_Returns200WithBackupId(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	body := bytes.NewReader([]byte("basebackup content"))
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/backups/postgres/wal/upload/full-start", body)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var response backups_dto.UploadBasebackupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.NotEqual(t, uuid.Nil, response.BackupID)

	backup, err := backups_core.GetBackupRepository().FindByID(response.BackupID)
	require.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusInProgress, backup.Status)
	assert.NotNil(t, backup.UploadCompletedAt)
}

func Test_FinalizeBasebackup_ValidSegments_MarksCompleted(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	backupID := uploadBasebackupPhase1(t, router, agentToken)

	completeFullBackupUpload(t, router, agentToken, backupID,
		"000000010000000100000001", "000000010000000100000010", nil)

	backup, err := backups_core.GetBackupRepository().FindByID(backupID)
	require.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusCompleted, backup.Status)
	require.NotNil(t, backup.PgFullBackupWalStartSegmentName)
	assert.Equal(t, "000000010000000100000001", *backup.PgFullBackupWalStartSegmentName)
	require.NotNil(t, backup.PgFullBackupWalStopSegmentName)
	assert.Equal(t, "000000010000000100000010", *backup.PgFullBackupWalStopSegmentName)
}

func Test_FinalizeBasebackup_WithError_MarksFailed(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	backupID := uploadBasebackupPhase1(t, router, agentToken)

	errMsg := "pg_basebackup stderr parse failed"
	completeFullBackupUpload(t, router, agentToken, backupID, "", "", &errMsg)

	backup, err := backups_core.GetBackupRepository().FindByID(backupID)
	require.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusFailed, backup.Status)
	require.NotNil(t, backup.FailMessage)
	assert.Equal(t, errMsg, *backup.FailMessage)
}

func Test_FinalizeBasebackup_InvalidBackupId_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	nonExistentID := uuid.New()
	body, _ := json.Marshal(backups_dto.FinalizeBasebackupRequest{
		BackupID:     nonExistentID,
		StartSegment: "000000010000000100000001",
		StopSegment:  "000000010000000100000010",
	})

	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/upload/full-complete",
		bytes.NewReader(body),
	)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func Test_FinalizeBasebackup_AlreadyCompleted_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	backupID := uploadBasebackupPhase1(t, router, agentToken)

	completeFullBackupUpload(t, router, agentToken, backupID,
		"000000010000000100000001", "000000010000000100000010", nil)

	// Second finalize should fail.
	body, _ := json.Marshal(backups_dto.FinalizeBasebackupRequest{
		BackupID:     backupID,
		StartSegment: "000000010000000100000001",
		StopSegment:  "000000010000000100000010",
	})

	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/upload/full-complete",
		bytes.NewReader(body),
	)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func Test_FinalizeBasebackup_InvalidToken_Returns401(t *testing.T) {
	router, db, storage, _, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	body, _ := json.Marshal(backups_dto.FinalizeBasebackupRequest{
		BackupID:     uuid.New(),
		StartSegment: "000000010000000100000001",
		StopSegment:  "000000010000000100000010",
	})

	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/upload/full-complete",
		bytes.NewReader(body),
	)
	req.Header.Set("Authorization", "invalid-token")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func Test_WalUpload_WalSegment_WithoutFullBackup_Returns204(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	body := bytes.NewReader([]byte("wal content"))
	req := newWalSegmentUploadRequest(body, agentToken, "000000010000000100000001")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func Test_WalUpload_WalSegment_WithGap_Returns204(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")

	// Skip ...0012, upload ...0013 — should succeed (no chain validation on upload).
	body := bytes.NewReader([]byte("wal content"))
	req := newWalSegmentUploadRequest(body, agentToken, "000000010000000100000013")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
}

func Test_WalUpload_WalSegment_DuplicateSegment_Returns200Idempotent(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	// Upload ...0011 once.
	body1 := bytes.NewReader([]byte("wal content"))
	req1 := newWalSegmentUploadRequest(body1, agentToken, "000000010000000100000011")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	require.Equal(t, http.StatusNoContent, w1.Code)

	// Upload the same segment again — must return 204 (idempotent).
	body2 := bytes.NewReader([]byte("wal content"))
	req2 := newWalSegmentUploadRequest(body2, agentToken, "000000010000000100000011")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusNoContent, w2.Code)

	// Ensure only ONE WAL segment record exists (no duplicate created).
	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	walCount := 0
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment {
			walCount++
		}
	}

	assert.Equal(t, 1, walCount, "duplicate upload must not create a second backup record")
}

func Test_WalUpload_WalSegment_ValidNextSegment_Returns200AndCreatesRecord(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	// First WAL segment after the full backup stop segment.
	body := bytes.NewReader([]byte("wal segment data"))
	req := newWalSegmentUploadRequest(body, agentToken, "000000010000000100000011")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)

	WaitForBackupCompletion(t, db.ID, 1, 5*time.Second)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	var walBackup *backups_core.Backup
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment {
			walBackup = b
			break
		}
	}

	require.NotNil(t, walBackup)
	assert.Equal(t, backups_core.BackupStatusCompleted, walBackup.Status)
	require.NotNil(t, walBackup.PgWalSegmentName)
	assert.Equal(t, "000000010000000100000011", *walBackup.PgWalSegmentName)
}

func Test_IsWalChainValid_NoFullBackup_ReturnsFalse(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	var response backups_dto.IsWalChainValidResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/is-wal-chain-valid-since-last-full-backup",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.False(t, response.IsValid)
	assert.Equal(t, "no_full_backup", response.Error)
}

func Test_IsWalChainValid_FullBackupOnly_ReturnsTrue(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	var response backups_dto.IsWalChainValidResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/is-wal-chain-valid-since-last-full-backup",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.True(t, response.IsValid)
	assert.Empty(t, response.Error)
}

func Test_IsWalChainValid_ContinuousChain_ReturnsTrue(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")
	uploadWalSegment(t, router, agentToken, "000000010000000100000012")
	uploadWalSegment(t, router, agentToken, "000000010000000100000013")

	var response backups_dto.IsWalChainValidResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/is-wal-chain-valid-since-last-full-backup",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.True(t, response.IsValid)
}

func Test_IsWalChainValid_BrokenChain_ReturnsFalse(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")
	uploadWalSegment(t, router, agentToken, "000000010000000100000012")
	uploadWalSegment(t, router, agentToken, "000000010000000100000013")

	// Delete the middle segment to create a gap.
	middleSeg, err := backups_core.GetBackupRepository().FindWalSegmentByName(
		db.ID, "000000010000000100000012",
	)
	require.NoError(t, err)
	require.NotNil(t, middleSeg)
	require.NoError(t, backups_core.GetBackupRepository().DeleteByID(middleSeg.ID))

	var response backups_dto.IsWalChainValidResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/is-wal-chain-valid-since-last-full-backup",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.False(t, response.IsValid)
	assert.Equal(t, "wal_chain_broken", response.Error)
	assert.Equal(t, "000000010000000100000011", response.LastContiguousSegment)
}

func Test_IsWalChainValid_InvalidToken_Returns401(t *testing.T) {
	router, db, storage, _, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/is-wal-chain-valid-since-last-full-backup",
		"invalid-token",
		http.StatusUnauthorized,
	)

	assert.Contains(t, string(resp.Body), "invalid agent token")
}

func Test_ReportError_ValidTokenAndError_CreatesFailedBackupRecord(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	errorMsg := "failed to parse pg_basebackup stderr: start WAL location not found"
	body, _ := json.Marshal(map[string]string{"error": errorMsg})

	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/error",
		bytes.NewReader(body),
	)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)
	require.NotEmpty(t, backups)

	assert.Equal(t, backups_core.BackupStatusFailed, backups[0].Status)
	require.NotNil(t, backups[0].FailMessage)
	assert.Equal(t, errorMsg, *backups[0].FailMessage)
}

func Test_ReportError_WithInvalidToken_ReturnsUnauthorized(t *testing.T) {
	router, db, storage, _, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	body, _ := json.Marshal(map[string]string{"error": "some error"})

	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/error",
		bytes.NewReader(body),
	)
	req.Header.Set("Authorization", "invalid-token")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func Test_ReportError_WithMissingErrorField_ReturnsBadRequest(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	body, _ := json.Marshal(map[string]string{})

	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/error",
		bytes.NewReader(body),
	)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func Test_GetNextFullBackupTime_WithValidToken_NoFullBackup_ReturnsNull(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	var response backups_dto.GetNextFullBackupTimeResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t,
		router,
		"/api/v1/backups/postgres/wal/next-full-backup-time",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.Nil(t, response.NextFullBackupTime, "should be nil when no full backup exists")
}

func Test_GetNextFullBackupTime_WithValidToken_HasFullBackup_ReturnsTime(t *testing.T) {
	cronExpr := "0 3 * * *"
	customTime := "14:30"

	tests := []struct {
		name         string
		interval     *intervals.Interval
		expectedHour int
		expectedMin  int
		checkHourMin bool
	}{
		{
			name:         "daily interval returns time at 04:00",
			interval:     nil, // use default (daily 04:00)
			expectedHour: 4,
			expectedMin:  0,
			checkHourMin: true,
		},
		{
			name: "hourly interval returns future time",
			interval: &intervals.Interval{
				Interval: intervals.IntervalHourly,
			},
			checkHourMin: false,
		},
		{
			name: "cron interval returns future time",
			interval: &intervals.Interval{
				Interval:       intervals.IntervalCron,
				CronExpression: &cronExpr,
			},
			checkHourMin: false,
		},
		{
			name: "daily interval with custom time 14:30",
			interval: &intervals.Interval{
				Interval:  intervals.IntervalDaily,
				TimeOfDay: &customTime,
			},
			expectedHour: 14,
			expectedMin:  30,
			checkHourMin: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router, db, storage, agentToken, ownerToken := createWalTestSetup(t)
			defer removeWalTestSetup(db, storage)

			if tt.interval != nil {
				var cfg backups_config.BackupConfig
				test_utils.MakeGetRequestAndUnmarshal(
					t, router,
					"/api/v1/backup-configs/database/"+db.ID.String(),
					"Bearer "+ownerToken,
					http.StatusOK, &cfg,
				)

				cfg.BackupInterval = tt.interval

				test_utils.MakePostRequestAndUnmarshal(
					t, router,
					"/api/v1/backup-configs/save",
					"Bearer "+ownerToken,
					cfg,
					http.StatusOK, &cfg,
				)
			}

			uploadBasebackup(
				t,
				router,
				agentToken,
				"000000010000000100000001",
				"000000010000000100000010",
			)

			now := time.Now().UTC()

			var response backups_dto.GetNextFullBackupTimeResponse
			test_utils.MakeGetRequestAndUnmarshal(
				t,
				router,
				"/api/v1/backups/postgres/wal/next-full-backup-time",
				agentToken,
				http.StatusOK,
				&response,
			)

			require.NotNil(t, response.NextFullBackupTime)
			nextTime := response.NextFullBackupTime.UTC()

			if tt.checkHourMin {
				assert.Equal(t, tt.expectedHour, nextTime.Hour(), "expected hour")
				assert.Equal(t, tt.expectedMin, nextTime.Minute(), "expected minute")
			}

			assert.True(t,
				nextTime.After(now.Add(-1*time.Minute)),
				"next backup time should not be in the past",
			)
			assert.True(t,
				nextTime.Before(now.Add(25*time.Hour)),
				"next backup time should be within 25 hours",
			)
		})
	}
}

func Test_GetNextFullBackupTime_WalSegmentAfterFullBackup_DoesNotImpactTime(t *testing.T) {
	router, db, storage, agentToken, ownerToken := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	setHourlyInterval(t, router, db.ID, ownerToken)

	uploadBasebackup(t, router, agentToken,
		"000000010000000100000001", "000000010000000100000010")

	// Shift the full backup's CreatedAt to 2 hours ago.
	twoHoursAgo := time.Now().UTC().Add(-2 * time.Hour)
	updateLastFullBackupTime(t, db.ID, twoHoursAgo)

	uploadWalSegment(t, router, agentToken, "000000010000000100000011")

	var response backups_dto.GetNextFullBackupTimeResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t,
		router,
		"/api/v1/backups/postgres/wal/next-full-backup-time",
		agentToken,
		http.StatusOK,
		&response,
	)

	require.NotNil(t, response.NextFullBackupTime)
	nextTime := response.NextFullBackupTime.UTC()

	// Hourly: nextTime = fullBackup.CreatedAt + 1h ≈ 1 hour ago (already past).
	// WAL segment should not have shifted it forward.
	expectedApprox := twoHoursAgo.Add(time.Hour)
	assert.WithinDuration(t, expectedApprox, nextTime, 5*time.Second,
		"next time should be based on full backup, not WAL segment",
	)
}

func Test_GetNextFullBackupTime_FailedBasebackup_DoesNotImpactTime(t *testing.T) {
	router, db, storage, agentToken, ownerToken := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	setHourlyInterval(t, router, db.ID, ownerToken)

	uploadBasebackup(t, router, agentToken,
		"000000010000000100000001", "000000010000000100000010")

	// Shift the full backup's CreatedAt to 2 hours ago.
	twoHoursAgo := time.Now().UTC().Add(-2 * time.Hour)
	updateLastFullBackupTime(t, db.ID, twoHoursAgo)

	// Report an error via the error endpoint.
	errorMsg := "pg_basebackup failed: connection refused"
	errBody, _ := json.Marshal(map[string]string{"error": errorMsg})
	errReq, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/error",
		bytes.NewReader(errBody),
	)
	errReq.Header.Set("Authorization", agentToken)
	errReq.Header.Set("Content-Type", "application/json")
	errW := httptest.NewRecorder()
	router.ServeHTTP(errW, errReq)
	require.Equal(t, http.StatusOK, errW.Code)

	var response backups_dto.GetNextFullBackupTimeResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t,
		router,
		"/api/v1/backups/postgres/wal/next-full-backup-time",
		agentToken,
		http.StatusOK,
		&response,
	)

	require.NotNil(t, response.NextFullBackupTime)
	nextTime := response.NextFullBackupTime.UTC()

	// Hourly: nextTime = completedFullBackup.CreatedAt + 1h ≈ 1 hour ago.
	// The error report should not have shifted it forward.
	expectedApprox := twoHoursAgo.Add(time.Hour)
	assert.WithinDuration(t, expectedApprox, nextTime, 5*time.Second,
		"next time should be based on completed full backup, not error report",
	)
}

func Test_GetNextFullBackupTime_NewCompletedFullBackup_ImpactsTime(t *testing.T) {
	router, db, storage, agentToken, ownerToken := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	setHourlyInterval(t, router, db.ID, ownerToken)

	uploadBasebackup(t, router, agentToken,
		"000000010000000100000001", "000000010000000100000010")

	// Shift the first backup's CreatedAt to 3 hours ago.
	threeHoursAgo := time.Now().UTC().Add(-3 * time.Hour)
	updateLastFullBackupTime(t, db.ID, threeHoursAgo)

	var firstResponse backups_dto.GetNextFullBackupTimeResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t,
		router,
		"/api/v1/backups/postgres/wal/next-full-backup-time",
		agentToken,
		http.StatusOK,
		&firstResponse,
	)

	require.NotNil(t, firstResponse.NextFullBackupTime)
	firstNextTime := firstResponse.NextFullBackupTime.UTC()

	// First result: 3h ago + 1h = 2h ago (in the past).
	assert.True(t, firstNextTime.Before(time.Now().UTC()),
		"first next time should be in the past (old backup)",
	)

	uploadBasebackup(t, router, agentToken,
		"000000010000000100000011", "000000010000000100000020")

	var secondResponse backups_dto.GetNextFullBackupTimeResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t,
		router,
		"/api/v1/backups/postgres/wal/next-full-backup-time",
		agentToken,
		http.StatusOK,
		&secondResponse,
	)

	require.NotNil(t, secondResponse.NextFullBackupTime)
	secondNextTime := secondResponse.NextFullBackupTime.UTC()

	// Second result: now + 1h (in the future).
	assert.True(t, secondNextTime.After(firstNextTime),
		"new full backup should shift next time forward",
	)
	assert.True(t, secondNextTime.After(time.Now().UTC()),
		"second next time should be in the future",
	)
}

func Test_GetNextFullBackupTime_WithInvalidToken_ReturnsUnauthorized(t *testing.T) {
	router, db, storage, _, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t,
		router,
		"/api/v1/backups/postgres/wal/next-full-backup-time",
		"invalid-token",
		http.StatusUnauthorized,
	)

	assert.Contains(t, string(resp.Body), "invalid agent token")
}

func Test_GetRestorePlan_NoFullBackup_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		agentToken,
		http.StatusBadRequest,
	)

	var errResp backups_dto.GetRestorePlanErrorResponse
	require.NoError(t, json.Unmarshal(resp.Body, &errResp))
	assert.Equal(t, "no_backups", errResp.Error)
}

func Test_GetRestorePlan_WithFullBackupOnly_Returns200(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	var response backups_dto.GetRestorePlanResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.NotEqual(t, uuid.Nil, response.FullBackup.BackupID)
	assert.Equal(t, "000000010000000100000001", response.FullBackup.FullBackupWalStartSegment)
	assert.Equal(t, "000000010000000100000010", response.FullBackup.FullBackupWalStopSegment)
	assert.Empty(t, response.WalSegments)
	assert.Greater(t, response.TotalSizeBytes, int64(0))
}

func Test_GetRestorePlan_WithFullBackupAndWalSegments_Returns200(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")
	uploadWalSegment(t, router, agentToken, "000000010000000100000012")
	uploadWalSegment(t, router, agentToken, "000000010000000100000013")

	var response backups_dto.GetRestorePlanResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.NotEqual(t, uuid.Nil, response.FullBackup.BackupID)
	require.Len(t, response.WalSegments, 3)
	assert.Equal(t, "000000010000000100000011", response.WalSegments[0].SegmentName)
	assert.Equal(t, "000000010000000100000012", response.WalSegments[1].SegmentName)
	assert.Equal(t, "000000010000000100000013", response.WalSegments[2].SegmentName)
	assert.Equal(t, "000000010000000100000013", response.LatestAvailableSegment)
	assert.Greater(t, response.TotalSizeBytes, int64(0))

	for _, seg := range response.WalSegments {
		assert.NotEqual(t, uuid.Nil, seg.BackupID)
	}
}

func Test_GetRestorePlan_WithSpecificBackupId_Returns200(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	firstBackup, err := backups_core.GetBackupRepository().
		FindLastCompletedFullWalBackupByDatabaseID(db.ID)
	require.NoError(t, err)
	require.NotNil(t, firstBackup)

	uploadWalSegment(t, router, agentToken, "000000010000000100000011")

	uploadBasebackup(t, router, agentToken, "000000010000000100000011", "000000010000000100000020")

	var response backups_dto.GetRestorePlanResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan?backupId="+firstBackup.ID.String(),
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.Equal(t, firstBackup.ID, response.FullBackup.BackupID)
	assert.Equal(t, "000000010000000100000001", response.FullBackup.FullBackupWalStartSegment)
	assert.Equal(t, "000000010000000100000010", response.FullBackup.FullBackupWalStopSegment)
}

func Test_GetRestorePlan_WithInvalidBackupId_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	nonExistentID := uuid.New()

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan?backupId="+nonExistentID.String(),
		agentToken,
		http.StatusBadRequest,
	)

	var errResp backups_dto.GetRestorePlanErrorResponse
	require.NoError(t, json.Unmarshal(resp.Body, &errResp))
	assert.Equal(t, "no_backups", errResp.Error)
}

func Test_GetRestorePlan_WithWalSegmentId_ResolvesFullBackupAndReturnsWals(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")
	uploadWalSegment(t, router, agentToken, "000000010000000100000012")
	uploadWalSegment(t, router, agentToken, "000000010000000100000013")

	WaitForBackupCompletion(t, db.ID, 3, 5*time.Second)

	walSegment, err := backups_core.GetBackupRepository().FindWalSegmentByName(
		db.ID, "000000010000000100000012",
	)
	require.NoError(t, err)
	require.NotNil(t, walSegment)

	var response backups_dto.GetRestorePlanResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan?backupId="+walSegment.ID.String(),
		agentToken,
		http.StatusOK,
		&response,
	)

	assert.NotEqual(t, uuid.Nil, response.FullBackup.BackupID)
	assert.Equal(t, "000000010000000100000001", response.FullBackup.FullBackupWalStartSegment)
	assert.Equal(t, "000000010000000100000010", response.FullBackup.FullBackupWalStopSegment)
	require.Len(t, response.WalSegments, 3)
	assert.Equal(t, "000000010000000100000011", response.WalSegments[0].SegmentName)
	assert.Equal(t, "000000010000000100000012", response.WalSegments[1].SegmentName)
	assert.Equal(t, "000000010000000100000013", response.WalSegments[2].SegmentName)
	assert.Greater(t, response.TotalSizeBytes, int64(0))
}

func Test_GetRestorePlan_WithInvalidToken_Returns401(t *testing.T) {
	router, db, storage, _, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		"invalid-token",
		http.StatusUnauthorized,
	)

	assert.Contains(t, string(resp.Body), "invalid agent token")
}

func Test_GetRestorePlan_WalChainBroken_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")
	uploadWalSegment(t, router, agentToken, "000000010000000100000012")
	uploadWalSegment(t, router, agentToken, "000000010000000100000013")

	middleSeg, err := backups_core.GetBackupRepository().FindWalSegmentByName(
		db.ID, "000000010000000100000012",
	)
	require.NoError(t, err)
	require.NotNil(t, middleSeg)
	require.NoError(t, backups_core.GetBackupRepository().DeleteByID(middleSeg.ID))

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		agentToken,
		http.StatusBadRequest,
	)

	var errResp backups_dto.GetRestorePlanErrorResponse
	require.NoError(t, json.Unmarshal(resp.Body, &errResp))
	assert.Equal(t, "wal_chain_broken", errResp.Error)
	assert.Equal(t, "000000010000000100000011", errResp.LastContiguousSegment)
}

func Test_GetRestorePlan_WithInvalidBackupIdFormat_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan?backupId=not-a-uuid",
		agentToken,
		http.StatusBadRequest,
	)

	assert.Contains(t, string(resp.Body), "invalid backupId format")
}

func Test_WalUpload_WalSegment_CompletedBackup_HasNonZeroDuration(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")
	uploadWalSegment(t, router, agentToken, "000000010000000100000011")

	WaitForBackupCompletion(t, db.ID, 1, 5*time.Second)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	var walBackup *backups_core.Backup
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment {
			walBackup = b
			break
		}
	}

	require.NotNil(t, walBackup)
	assert.Equal(t, backups_core.BackupStatusCompleted, walBackup.Status)
	assert.Greater(t, walBackup.BackupDurationMs, int64(0),
		"WAL segment backup should have non-zero duration")
}

func Test_WalUpload_Basebackup_CompletedBackup_HasNonZeroDuration(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	backupID := uploadBasebackupPhase1(t, router, agentToken)
	completeFullBackupUpload(t, router, agentToken, backupID,
		"000000010000000100000001", "000000010000000100000010", nil)

	backup, err := backups_core.GetBackupRepository().FindByID(backupID)
	require.NoError(t, err)
	assert.Equal(t, backups_core.BackupStatusCompleted, backup.Status)
	assert.Greater(t, backup.BackupDurationMs, int64(0),
		"base backup should have non-zero duration")
}

func Test_WalUpload_WalSegment_ProgressUpdatedDuringStream(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	pipeReader, pipeWriter := io.Pipe()
	req := newWalSegmentUploadRequest(pipeReader, agentToken, "000000010000000100000011")

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(recorder, req)
		close(done)
	}()

	// Write some data so the countingReader registers bytes.
	_, err := pipeWriter.Write([]byte("wal-segment-progress-data"))
	require.NoError(t, err)

	// Wait for the progress tracker to tick (1s interval + margin).
	time.Sleep(1500 * time.Millisecond)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	var walBackup *backups_core.Backup
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeWalSegment {
			walBackup = b
			break
		}
	}

	require.NotNil(t, walBackup)
	assert.Equal(t, backups_core.BackupStatusInProgress, walBackup.Status)
	assert.Greater(t, walBackup.BackupDurationMs, int64(0),
		"duration should be tracked in real-time during upload")
	assert.Greater(t, walBackup.BackupSizeMb, float64(0),
		"size should be tracked in real-time during upload")

	_ = pipeWriter.Close()
	<-done
}

func Test_WalUpload_Basebackup_ProgressUpdatedDuringStream(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	pipeReader, pipeWriter := io.Pipe()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/backups/postgres/wal/upload/full-start", pipeReader)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/octet-stream")

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(recorder, req)
		close(done)
	}()

	// Write some data so the countingReader registers bytes.
	_, err := pipeWriter.Write([]byte("basebackup-progress-data"))
	require.NoError(t, err)

	// Wait for the progress tracker to tick (1s interval + margin).
	time.Sleep(1500 * time.Millisecond)

	backups, err := backups_core.GetBackupRepository().FindByDatabaseID(db.ID)
	require.NoError(t, err)

	var fullBackup *backups_core.Backup
	for _, b := range backups {
		if b.PgWalBackupType != nil &&
			*b.PgWalBackupType == backups_core.PgWalBackupTypeFullBackup {
			fullBackup = b
			break
		}
	}

	require.NotNil(t, fullBackup)
	assert.Equal(t, backups_core.BackupStatusInProgress, fullBackup.Status)
	assert.Greater(t, fullBackup.BackupDurationMs, int64(0),
		"duration should be tracked in real-time during upload")
	assert.Greater(t, fullBackup.BackupSizeMb, float64(0),
		"size should be tracked in real-time during upload")

	_ = pipeWriter.Close()
	<-done
}

func Test_DownloadRestoreFile_UploadThenDownload_ContentMatches(t *testing.T) {
	tests := []struct {
		name       string
		encryption backups_config.BackupEncryption
	}{
		{
			name:       "unencrypted",
			encryption: backups_config.BackupEncryptionNone,
		},
		{
			name:       "encrypted",
			encryption: backups_config.BackupEncryptionEncrypted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router, db, storage, agentToken, ownerToken := createWalTestSetup(t)
			defer removeWalTestSetup(db, storage)

			setEncryption(t, router, db.ID, ownerToken, tt.encryption)

			uploadContent := "test-basebackup-content-for-download"
			body := bytes.NewReader([]byte(uploadContent))
			req, _ := http.NewRequest(http.MethodPost, "/api/v1/backups/postgres/wal/upload/full-start", body)
			req.Header.Set("Authorization", agentToken)
			req.Header.Set("Content-Type", "application/octet-stream")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			var uploadResp backups_dto.UploadBasebackupResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &uploadResp))

			completeFullBackupUpload(t, router, agentToken, uploadResp.BackupID,
				"000000010000000100000001", "000000010000000100000010", nil)

			var planResp backups_dto.GetRestorePlanResponse
			test_utils.MakeGetRequestAndUnmarshal(
				t, router,
				"/api/v1/backups/postgres/wal/restore/plan",
				agentToken,
				http.StatusOK,
				&planResp,
			)

			require.NotEqual(t, uuid.Nil, planResp.FullBackup.BackupID)

			downloadResp := test_utils.MakeGetRequest(
				t,
				router,
				"/api/v1/backups/postgres/wal/restore/download?backupId="+planResp.FullBackup.BackupID.String(),
				agentToken,
				http.StatusOK,
			)

			assert.Equal(t, uploadContent, string(downloadResp.Body))
		})
	}
}

func Test_DownloadRestoreFile_WalSegment_UploadThenDownload_ContentMatches(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	uploadBasebackup(t, router, agentToken, "000000010000000100000001", "000000010000000100000010")

	walContent := "test-wal-segment-content-for-download"
	body := bytes.NewReader([]byte(walContent))
	req := newWalSegmentUploadRequest(body, agentToken, "000000010000000100000011")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)

	WaitForBackupCompletion(t, db.ID, 1, 5*time.Second)

	var planResp backups_dto.GetRestorePlanResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		agentToken,
		http.StatusOK,
		&planResp,
	)

	require.Len(t, planResp.WalSegments, 1)

	downloadResp := test_utils.MakeGetRequest(
		t,
		router,
		"/api/v1/backups/postgres/wal/restore/download?backupId="+planResp.WalSegments[0].BackupID.String(),
		agentToken,
		http.StatusOK,
	)

	assert.Equal(t, walContent, string(downloadResp.Body))
}

func Test_DownloadRestoreFile_InvalidBackupId_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	nonExistentID := uuid.New()

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/download?backupId="+nonExistentID.String(),
		agentToken,
		http.StatusBadRequest,
	)

	assert.Contains(t, string(resp.Body), "backup not found")
}

func Test_DownloadRestoreFile_InvalidToken_Returns401(t *testing.T) {
	router, db, storage, _, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/download?backupId="+uuid.New().String(),
		"invalid-token",
		http.StatusUnauthorized,
	)

	assert.Contains(t, string(resp.Body), "invalid agent token")
}

func Test_DownloadRestoreFile_BackupFromOtherDatabase_Returns400(t *testing.T) {
	router, db1, storage1, agentToken1, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db1, storage1)

	_, db2, storage2, agentToken2, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db2, storage2)

	uploadBasebackup(t, router, agentToken1, "000000010000000100000001", "000000010000000100000010")

	WaitForBackupCompletion(t, db1.ID, 0, 5*time.Second)

	var planResp backups_dto.GetRestorePlanResponse
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backups/postgres/wal/restore/plan",
		agentToken1,
		http.StatusOK,
		&planResp,
	)

	resp := test_utils.MakeGetRequest(
		t,
		router,
		"/api/v1/backups/postgres/wal/restore/download?backupId="+planResp.FullBackup.BackupID.String(),
		agentToken2,
		http.StatusBadRequest,
	)

	assert.Contains(t, string(resp.Body), "backup does not belong to this database")
}

func Test_DownloadRestoreFile_MissingBackupId_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/download",
		agentToken,
		http.StatusBadRequest,
	)

	assert.Contains(t, string(resp.Body), "backupId is required")
}

func Test_DownloadRestoreFile_InvalidBackupIdFormat_Returns400(t *testing.T) {
	router, db, storage, agentToken, _ := createWalTestSetup(t)
	defer removeWalTestSetup(db, storage)

	resp := test_utils.MakeGetRequest(
		t, router,
		"/api/v1/backups/postgres/wal/restore/download?backupId=not-a-uuid",
		agentToken,
		http.StatusBadRequest,
	)

	assert.Contains(t, string(resp.Body), "invalid backupId format")
}

func createWalTestRouter() *gin.Engine {
	router := workspaces_testing.CreateTestRouter(
		workspaces_controllers.GetWorkspaceController(),
		workspaces_controllers.GetMembershipController(),
		databases.GetDatabaseController(),
		backups_config.GetBackupConfigController(),
		GetBackupController(),
	)

	v1 := router.Group("/api/v1")
	GetPostgresWalBackupController().RegisterRoutes(v1)

	return router
}

func createWalTestSetup(t *testing.T) (
	router *gin.Engine,
	db *databases.Database,
	storage *storages.Storage,
	agentToken string,
	ownerToken string,
) {
	t.Helper()

	router = createWalTestRouter()

	owner := users_testing.CreateTestUser(users_enums.UserRoleMember)
	workspace := workspaces_testing.CreateTestWorkspace("WAL Test Workspace", owner, router)

	db = createTestDatabase("WAL Test DB", workspace.ID, owner.Token, router)

	// Set backup type to WAL_V1 so the WAL service accepts requests.
	db.Postgresql.BackupType = postgresql.PostgresBackupTypeWalV1
	dbRepo := &databases.DatabaseRepository{}
	if _, err := dbRepo.Save(db); err != nil {
		t.Fatalf("failed to update database backup type: %v", err)
	}

	storage = &storages.Storage{
		WorkspaceID:  workspace.ID,
		Type:         storages.StorageTypeLocal,
		Name:         "WAL Test Storage " + uuid.New().String(),
		LocalStorage: &local_storage.LocalStorage{},
	}

	repo := &storages.StorageRepository{}
	storage, err := repo.Save(storage)
	if err != nil {
		t.Fatalf("failed to create test storage: %v", err)
	}

	configService := backups_config.GetBackupConfigService()
	cfg, err := configService.GetBackupConfigByDbId(db.ID)
	if err != nil {
		t.Fatalf("failed to get backup config: %v", err)
	}

	cfg.IsBackupsEnabled = true
	cfg.StorageID = &storage.ID
	cfg.Storage = storage
	_, err = configService.SaveBackupConfig(cfg)
	if err != nil {
		t.Fatalf("failed to save backup config: %v", err)
	}

	var tokenResp map[string]string
	test_utils.MakePostRequestAndUnmarshal(
		t,
		router,
		"/api/v1/databases/"+db.ID.String()+"/regenerate-token",
		"Bearer "+owner.Token,
		nil,
		http.StatusOK,
		&tokenResp,
	)

	agentToken = tokenResp["token"]
	ownerToken = owner.Token

	return router, db, storage, agentToken, ownerToken
}

func removeWalTestSetup(db *databases.Database, storage *storages.Storage) {
	databases.RemoveTestDatabase(db)
	storages.RemoveTestStorage(storage.ID)
}

func newWalSegmentUploadRequest(
	body io.Reader,
	agentToken string,
	segmentName string,
) *http.Request {
	req, err := http.NewRequest(http.MethodPost, "/api/v1/backups/postgres/wal/upload/wal", body)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Wal-Segment-Name", segmentName)

	return req
}

func uploadBasebackupPhase1(
	t *testing.T,
	router *gin.Engine,
	agentToken string,
) uuid.UUID {
	t.Helper()

	body := bytes.NewReader([]byte("test-basebackup-content"))

	req, _ := http.NewRequest(http.MethodPost, "/api/v1/backups/postgres/wal/upload/full-start", body)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/octet-stream")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var response backups_dto.UploadBasebackupResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.NotEqual(t, uuid.Nil, response.BackupID)

	return response.BackupID
}

func completeFullBackupUpload(
	t *testing.T,
	router *gin.Engine,
	agentToken string,
	backupID uuid.UUID,
	walStart string,
	walStop string,
	errMsg *string,
) {
	t.Helper()

	request := backups_dto.FinalizeBasebackupRequest{
		BackupID:     backupID,
		StartSegment: walStart,
		StopSegment:  walStop,
		Error:        errMsg,
	}

	reqBody, _ := json.Marshal(request)
	req, _ := http.NewRequest(
		http.MethodPost,
		"/api/v1/backups/postgres/wal/upload/full-complete",
		bytes.NewReader(reqBody),
	)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func uploadBasebackup(
	t *testing.T,
	router *gin.Engine,
	agentToken string,
	walStart string,
	walStop string,
) {
	t.Helper()

	backupID := uploadBasebackupPhase1(t, router, agentToken)
	completeFullBackupUpload(t, router, agentToken, backupID, walStart, walStop, nil)
}

func uploadWalSegment(
	t *testing.T,
	router *gin.Engine,
	agentToken string,
	segmentName string,
) {
	t.Helper()

	body := bytes.NewReader([]byte("test-wal-segment-content"))

	req, _ := http.NewRequest(http.MethodPost, "/api/v1/backups/postgres/wal/upload/wal", body)
	req.Header.Set("Authorization", agentToken)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Wal-Segment-Name", segmentName)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func updateLastFullBackupTime(t *testing.T, databaseID uuid.UUID, createdAt time.Time) {
	t.Helper()

	repo := backups_core.GetBackupRepository()

	backup, err := repo.FindLastCompletedFullWalBackupByDatabaseID(databaseID)
	if err != nil {
		t.Fatalf("updateLastFullBackupTime: find: %v", err)
	}

	require.NotNil(t, backup, "no completed full backup found to update")

	backup.CreatedAt = createdAt
	if err := repo.Save(backup); err != nil {
		t.Fatalf("updateLastFullBackupTime: save: %v", err)
	}
}

func setEncryption(
	t *testing.T,
	router *gin.Engine,
	databaseID uuid.UUID,
	ownerToken string,
	encryption backups_config.BackupEncryption,
) {
	t.Helper()

	var cfg backups_config.BackupConfig
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backup-configs/database/"+databaseID.String(),
		"Bearer "+ownerToken,
		http.StatusOK, &cfg,
	)

	cfg.Encryption = encryption

	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/backup-configs/save",
		"Bearer "+ownerToken,
		cfg,
		http.StatusOK, &cfg,
	)
}

func setHourlyInterval(t *testing.T, router *gin.Engine, databaseID uuid.UUID, ownerToken string) {
	t.Helper()

	var cfg backups_config.BackupConfig
	test_utils.MakeGetRequestAndUnmarshal(
		t, router,
		"/api/v1/backup-configs/database/"+databaseID.String(),
		"Bearer "+ownerToken,
		http.StatusOK, &cfg,
	)

	cfg.BackupInterval = &intervals.Interval{Interval: intervals.IntervalHourly}

	test_utils.MakePostRequestAndUnmarshal(
		t, router,
		"/api/v1/backup-configs/save",
		"Bearer "+ownerToken,
		cfg,
		http.StatusOK, &cfg,
	)
}
