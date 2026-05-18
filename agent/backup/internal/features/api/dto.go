package api

import "time"

type WalChainValidityResponse struct {
	IsValid               bool   `json:"isValid"`
	Error                 string `json:"error,omitempty"`
	LastContiguousSegment string `json:"lastContiguousSegment,omitempty"`
}

type NextFullBackupTimeResponse struct {
	NextFullBackupTime *time.Time `json:"nextFullBackupTime"`
}

type UploadWalSegmentResult struct {
	IsGapDetected       bool
	ExpectedSegmentName string
	ReceivedSegmentName string
}

type reportErrorRequest struct {
	Error string `json:"error"`
}

type versionResponse struct {
	Version string `json:"version"`
}

type UploadBasebackupResponse struct {
	BackupID string `json:"backupId"`
}

type finalizeBasebackupRequest struct {
	BackupID     string  `json:"backupId"`
	StartSegment string  `json:"startSegment"`
	StopSegment  string  `json:"stopSegment"`
	Error        *string `json:"error,omitempty"`
}

type uploadErrorResponse struct {
	Error               string `json:"error"`
	ExpectedSegmentName string `json:"expectedSegmentName"`
	ReceivedSegmentName string `json:"receivedSegmentName"`
}

type RestorePlanFullBackup struct {
	BackupID                  string    `json:"id"`
	FullBackupWalStartSegment string    `json:"fullBackupWalStartSegment"`
	FullBackupWalStopSegment  string    `json:"fullBackupWalStopSegment"`
	PgVersion                 string    `json:"pgVersion"`
	CreatedAt                 time.Time `json:"createdAt"`
	SizeBytes                 int64     `json:"sizeBytes"`
}

type RestorePlanWalSegment struct {
	BackupID    string `json:"backupId"`
	SegmentName string `json:"segmentName"`
	SizeBytes   int64  `json:"sizeBytes"`
}

type GetRestorePlanResponse struct {
	FullBackup             RestorePlanFullBackup   `json:"fullBackup"`
	WalSegments            []RestorePlanWalSegment `json:"walSegments"`
	TotalSizeBytes         int64                   `json:"totalSizeBytes"`
	LatestAvailableSegment string                  `json:"latestAvailableSegment"`
}

type GetRestorePlanErrorResponse struct {
	Error                 string `json:"error"`
	Message               string `json:"message"`
	LastContiguousSegment string `json:"lastContiguousSegment,omitempty"`
}
