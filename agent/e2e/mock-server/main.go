package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const backupStorageDir = "/backup-storage"

type walSegment struct {
	BackupID    string
	SegmentName string
	FilePath    string
	SizeBytes   int64
}

type server struct {
	mu         sync.RWMutex
	version    string
	binaryPath string

	backupID        string
	backupFilePath  string
	startSegment    string
	stopSegment     string
	isFinalized     bool
	walSegments     []walSegment
	backupCreatedAt time.Time
}

func main() {
	version := "v2.0.0"
	binaryPath := "/artifacts/agent-v2"
	port := "4050"

	_ = os.MkdirAll(backupStorageDir, 0o755)

	s := &server{version: version, binaryPath: binaryPath}

	// System endpoints
	http.HandleFunc("/api/v1/system/version", s.handleVersion)
	http.HandleFunc("/api/v1/system/agent", s.handleAgentDownload)

	// Backup endpoints
	http.HandleFunc("/api/v1/backups/postgres/wal/is-wal-chain-valid-since-last-full-backup", s.handleChainValidity)
	http.HandleFunc("/api/v1/backups/postgres/wal/next-full-backup-time", s.handleNextBackupTime)
	http.HandleFunc("/api/v1/backups/postgres/wal/upload/full-start", s.handleFullStart)
	http.HandleFunc("/api/v1/backups/postgres/wal/upload/full-complete", s.handleFullComplete)
	http.HandleFunc("/api/v1/backups/postgres/wal/upload/wal", s.handleWalUpload)
	http.HandleFunc("/api/v1/backups/postgres/wal/error", s.handleError)

	// Restore endpoints
	http.HandleFunc("/api/v1/backups/postgres/wal/restore/plan", s.handleRestorePlan)
	http.HandleFunc("/api/v1/backups/postgres/wal/restore/download", s.handleRestoreDownload)

	// Mock control endpoints
	http.HandleFunc("/mock/set-version", s.handleSetVersion)
	http.HandleFunc("/mock/set-binary-path", s.handleSetBinaryPath)
	http.HandleFunc("/mock/backup-status", s.handleBackupStatus)
	http.HandleFunc("/mock/reset", s.handleReset)
	http.HandleFunc("/health", s.handleHealth)

	addr := ":" + port
	log.Printf("Mock server starting on %s (version=%s, binary=%s)", addr, version, binaryPath)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// --- System handlers ---

func (s *server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	v := s.version
	s.mu.RUnlock()

	log.Printf("GET /api/v1/system/version -> %s", v)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"version": v})
}

func (s *server) handleAgentDownload(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	path := s.binaryPath
	s.mu.RUnlock()

	log.Printf("GET /api/v1/system/agent (arch=%s) -> serving %s", r.URL.Query().Get("arch"), path)

	http.ServeFile(w, r, path)
}

// --- Backup handlers ---

func (s *server) handleChainValidity(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	isFinalized := s.isFinalized
	s.mu.RUnlock()

	log.Printf("GET chain-validity -> isFinalized=%v", isFinalized)

	w.Header().Set("Content-Type", "application/json")

	if isFinalized {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"isValid": true,
		})
	} else {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"isValid": false,
			"error":   "no full backup found",
		})
	}
}

func (s *server) handleNextBackupTime(w http.ResponseWriter, _ *http.Request) {
	log.Printf("GET next-full-backup-time")

	nextTime := time.Now().UTC().Add(1 * time.Hour)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"nextFullBackupTime": nextTime.Format(time.RFC3339),
	})
}

func (s *server) handleFullStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	backupID := generateID()
	filePath := filepath.Join(backupStorageDir, backupID+".zst")

	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("ERROR creating backup file: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	bytesWritten, err := io.Copy(file, r.Body)
	_ = file.Close()

	if err != nil {
		log.Printf("ERROR writing backup data: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	s.backupID = backupID
	s.backupFilePath = filePath
	s.backupCreatedAt = time.Now().UTC()
	s.mu.Unlock()

	log.Printf("POST full-start -> backupID=%s, size=%d bytes", backupID, bytesWritten)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"backupId": backupID})
}

func (s *server) handleFullComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		BackupID     string  `json:"backupId"`
		StartSegment string  `json:"startSegment"`
		StopSegment  string  `json:"stopSegment"`
		Error        *string `json:"error,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Error != nil {
		log.Printf("POST full-complete -> backupID=%s ERROR: %s", body.BackupID, *body.Error)
		w.WriteHeader(http.StatusOK)
		return
	}

	s.mu.Lock()
	s.startSegment = body.StartSegment
	s.stopSegment = body.StopSegment
	s.isFinalized = true
	s.mu.Unlock()

	log.Printf(
		"POST full-complete -> backupID=%s, start=%s, stop=%s",
		body.BackupID,
		body.StartSegment,
		body.StopSegment,
	)

	w.WriteHeader(http.StatusOK)
}

func (s *server) handleWalUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	segmentName := r.Header.Get("X-Wal-Segment-Name")
	if segmentName == "" {
		http.Error(w, "missing X-Wal-Segment-Name header", http.StatusBadRequest)
		return
	}

	walBackupID := generateID()
	filePath := filepath.Join(backupStorageDir, walBackupID+".zst")

	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("ERROR creating WAL file: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	bytesWritten, err := io.Copy(file, r.Body)
	_ = file.Close()

	if err != nil {
		log.Printf("ERROR writing WAL data: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	s.walSegments = append(s.walSegments, walSegment{
		BackupID:    walBackupID,
		SegmentName: segmentName,
		FilePath:    filePath,
		SizeBytes:   bytesWritten,
	})
	s.mu.Unlock()

	log.Printf("POST wal-upload -> segment=%s, walBackupID=%s, size=%d", segmentName, walBackupID, bytesWritten)

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Error string `json:"error"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		log.Printf("POST error -> failed to decode: %v", err)
	} else {
		log.Printf("POST error -> %s", body.Error)
	}

	w.WriteHeader(http.StatusOK)
}

// --- Restore handlers ---

func (s *server) handleRestorePlan(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.isFinalized {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "no_backups",
			"message": "No full backups available",
		})
		return
	}

	backupFileInfo, err := os.Stat(s.backupFilePath)
	if err != nil {
		log.Printf("ERROR stat backup file: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	backupSizeBytes := backupFileInfo.Size()
	totalSizeBytes := backupSizeBytes

	walSegmentsJSON := make([]map[string]any, 0, len(s.walSegments))

	latestSegment := ""

	for _, segment := range s.walSegments {
		totalSizeBytes += segment.SizeBytes
		latestSegment = segment.SegmentName

		walSegmentsJSON = append(walSegmentsJSON, map[string]any{
			"backupId":    segment.BackupID,
			"segmentName": segment.SegmentName,
			"sizeBytes":   segment.SizeBytes,
		})
	}

	response := map[string]any{
		"fullBackup": map[string]any{
			"id":                        s.backupID,
			"fullBackupWalStartSegment": s.startSegment,
			"fullBackupWalStopSegment":  s.stopSegment,
			"pgVersion":                 "17",
			"createdAt":                 s.backupCreatedAt.Format(time.RFC3339),
			"sizeBytes":                 backupSizeBytes,
		},
		"walSegments":            walSegmentsJSON,
		"totalSizeBytes":         totalSizeBytes,
		"latestAvailableSegment": latestSegment,
	}

	log.Printf("GET restore-plan -> backupID=%s, walSegments=%d", s.backupID, len(s.walSegments))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *server) handleRestoreDownload(w http.ResponseWriter, r *http.Request) {
	requestedBackupID := r.URL.Query().Get("backupId")
	if requestedBackupID == "" {
		http.Error(w, "missing backupId query param", http.StatusBadRequest)
		return
	}

	filePath := s.findBackupFile(requestedBackupID)
	if filePath == "" {
		log.Printf("GET restore-download -> backupId=%s NOT FOUND", requestedBackupID)
		http.Error(w, "backup not found", http.StatusNotFound)
		return
	}

	log.Printf("GET restore-download -> backupId=%s, file=%s", requestedBackupID, filePath)

	http.ServeFile(w, r, filePath)
}

// --- Mock control handlers ---

func (s *server) handleSetVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Version string `json:"version"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.version = body.Version
	s.mu.Unlock()

	log.Printf("POST /mock/set-version -> %s", body.Version)

	_, _ = fmt.Fprintf(w, "version set to %s", body.Version)
}

func (s *server) handleSetBinaryPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		BinaryPath string `json:"binaryPath"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.binaryPath = body.BinaryPath
	s.mu.Unlock()

	log.Printf("POST /mock/set-binary-path -> %s", body.BinaryPath)

	_, _ = fmt.Fprintf(w, "binary path set to %s", body.BinaryPath)
}

func (s *server) handleBackupStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	isFinalized := s.isFinalized
	walSegmentCount := len(s.walSegments)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"isFinalized":     isFinalized,
		"walSegmentCount": walSegmentCount,
	})
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	s.backupID = ""
	s.backupFilePath = ""
	s.startSegment = ""
	s.stopSegment = ""
	s.isFinalized = false
	s.walSegments = nil
	s.backupCreatedAt = time.Time{}
	s.mu.Unlock()

	// Clean stored files
	entries, _ := os.ReadDir(backupStorageDir)
	for _, entry := range entries {
		_ = os.Remove(filepath.Join(backupStorageDir, entry.Name()))
	}

	log.Printf("POST /mock/reset -> state cleared")

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// --- Private helpers ---

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (s *server) findBackupFile(backupID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.backupID == backupID {
		return s.backupFilePath
	}

	for _, segment := range s.walSegments {
		if segment.BackupID == backupID {
			return segment.FilePath
		}
	}

	return ""
}
