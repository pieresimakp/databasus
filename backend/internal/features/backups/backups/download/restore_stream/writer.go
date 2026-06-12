package restore_stream

import (
	"archive/tar"
	"fmt"
	"io"

	"github.com/google/uuid"

	"databasus-backend/internal/features/backups/backups/core/physical/chain_view"
	physical_enums "databasus-backend/internal/features/backups/backups/core/physical/enums"
	physical_models "databasus-backend/internal/features/backups/backups/core/physical/models"
	"databasus-backend/internal/features/storages"
	util_encryption "databasus-backend/internal/util/encryption"
)

// Writer turns a resolved RestoreSet into the single tar stream the user (and,
// later, restore verification) consumes. It decrypts every artifact, decompresses
// the backups (full/ , incr-N/, each with a backup_manifest) so pg_combinebackup
// can read them directly, but leaves WAL compressed (wal/<segment>.zst) — those
// are the bulk of the stream and the recovery script decompresses them on demand.
// History files stay plaintext (wal/<name>.history). A trailing MANIFEST.sha256
// lets the recipient verify the transfer. The recovery script that consumes this
// layout (and takes the PITR target as a CLI argument) is served separately,
// not shipped in the tar. It is transport- and auth-agnostic: callers decide who
// may stream and where the bytes go.
type Writer struct {
	storageService *storages.StorageService
	fieldEncryptor util_encryption.FieldEncryptor
}

func NewWriter(
	storageService *storages.StorageService,
	fieldEncryptor util_encryption.FieldEncryptor,
) *Writer {
	return &Writer{storageService, fieldEncryptor}
}

// Write streams the whole restore tar into w. masterKey may be empty when no
// artifact is encrypted; an encrypted artifact with an empty key is a hard
// error rather than a silently-corrupt download.
func (rw *Writer) Write(w io.Writer, set *chain_view.RestoreSet, masterKey string) error {
	tarWriter := tar.NewWriter(w)

	storageCache := make(map[uuid.UUID]*storages.Storage)
	checksums := newChecksumLedger()

	incrDirNames := make([]string, len(set.Incrementals))
	for i := range set.Incrementals {
		incrDirNames[i] = fmt.Sprintf("incr-%d", i+1)
	}

	if err := rw.writeBackupDir(tarWriter, "full", backupArtifact{
		FileName:         deref(set.RootFull.FileName),
		Encryption:       set.RootFull.Encryption,
		EncryptionSalt:   set.RootFull.EncryptionSalt,
		EncryptionIV:     set.RootFull.EncryptionIV,
		ManifestFileName: set.RootFull.ManifestFileName,
		ManifestSalt:     set.RootFull.ManifestEncryptionSalt,
		ManifestIV:       set.RootFull.ManifestEncryptionIV,
		RowID:            set.RootFull.ID,
		StorageID:        set.RootFull.StorageID,
		Compression:      set.RootFull.Compression,
	}, masterKey, storageCache, checksums); err != nil {
		return fmt.Errorf("stream full backup: %w", err)
	}

	for i, incremental := range set.Incrementals {
		if err := rw.writeBackupDir(tarWriter, incrDirNames[i], backupArtifact{
			FileName:         deref(incremental.FileName),
			Encryption:       incremental.Encryption,
			EncryptionSalt:   incremental.EncryptionSalt,
			EncryptionIV:     incremental.EncryptionIV,
			ManifestFileName: incremental.ManifestFileName,
			ManifestSalt:     incremental.ManifestEncryptionSalt,
			ManifestIV:       incremental.ManifestEncryptionIV,
			RowID:            incremental.ID,
			StorageID:        incremental.StorageID,
			Compression:      incremental.Compression,
		}, masterKey, storageCache, checksums); err != nil {
			return fmt.Errorf("stream incremental %s: %w", incremental.ID, err)
		}
	}

	for _, segment := range set.WalSegments {
		if err := rw.writeWalSegment(tarWriter, segment, masterKey, storageCache, checksums); err != nil {
			return fmt.Errorf("stream wal segment %s: %w", segment.WalFilename, err)
		}
	}

	for _, history := range set.HistoryFiles {
		if err := rw.writeHistoryFile(tarWriter, history, masterKey, storageCache, checksums); err != nil {
			return fmt.Errorf("stream history file %s: %w", history.HistoryFilename, err)
		}
	}

	if err := writeTarBytes(tarWriter, "MANIFEST.sha256", 0o644, checksums.render(), checksums.skip()); err != nil {
		return err
	}

	return tarWriter.Close()
}

// writeBackupDir re-tars one backup's PGDATA under dirName/ and drops its
// reconstructed backup_manifest beside it (pg_combinebackup needs one per input
// directory).
func (rw *Writer) writeBackupDir(
	tarWriter *tar.Writer,
	dirName string,
	artifact backupArtifact,
	masterKey string,
	storageCache map[uuid.UUID]*storages.Storage,
	checksums *checksumLedger,
) error {
	if artifact.FileName == "" {
		return fmt.Errorf("backup %s has no stored file", artifact.RowID)
	}
	if artifact.ManifestFileName == nil {
		return fmt.Errorf("backup %s has no reconstructed manifest sidecar", artifact.RowID)
	}

	store, err := rw.resolveStorage(artifact.StorageID, storageCache)
	if err != nil {
		return err
	}

	reader, cleanup, err := openArtifact(store, rw.fieldEncryptor, masterKey, artifactSource{
		fileName:   artifact.FileName,
		encryption: artifact.Encryption,
		salt:       artifact.EncryptionSalt,
		iv:         artifact.EncryptionIV,
		keyID:      artifact.RowID,
		codec:      artifact.Compression,
	})
	if err != nil {
		return err
	}
	defer cleanup()

	if err := copyTarWithPrefix(tarWriter, reader, dirName, checksums); err != nil {
		return err
	}

	return rw.writeManifest(tarWriter, dirName, artifact, masterKey, storageCache, checksums)
}

func (rw *Writer) writeManifest(
	tarWriter *tar.Writer,
	dirName string,
	artifact backupArtifact,
	masterKey string,
	storageCache map[uuid.UUID]*storages.Storage,
	checksums *checksumLedger,
) error {
	store, err := rw.resolveStorage(artifact.StorageID, storageCache)
	if err != nil {
		return err
	}

	// The manifest sidecar is stored as raw bytes (encrypted with the backup's
	// row ID but a fresh salt/IV), never zstd-compressed.
	reader, cleanup, err := openArtifact(store, rw.fieldEncryptor, masterKey, artifactSource{
		fileName:   *artifact.ManifestFileName,
		encryption: artifact.Encryption,
		salt:       artifact.ManifestSalt,
		iv:         artifact.ManifestIV,
		keyID:      artifact.RowID,
		codec:      physical_enums.PhysicalBackupCompressionNone,
	})
	if err != nil {
		return err
	}
	defer cleanup()

	return streamTarEntry(tarWriter, dirName+"/backup_manifest", 0o600, reader, checksums)
}

func (rw *Writer) writeWalSegment(
	tarWriter *tar.Writer,
	segment *physical_models.PhysicalWalSegment,
	masterKey string,
	storageCache map[uuid.UUID]*storages.Storage,
	checksums *checksumLedger,
) error {
	if segment.FileName == nil {
		return fmt.Errorf("wal segment %s has no stored file", segment.WalFilename)
	}

	store, err := rw.resolveStorage(segment.StorageID, storageCache)
	if err != nil {
		return err
	}

	// WAL ships as stored (decrypt only, still zstd) under its bare name + .zst;
	// the recovery script decompresses on demand at replay time. Decompressing
	// here would re-inflate every near-empty segment back to a full 16 MB and
	// blow the download up by orders of magnitude.
	reader, cleanup, err := openArtifact(store, rw.fieldEncryptor, masterKey, artifactSource{
		fileName:   *segment.FileName,
		encryption: segment.Encryption,
		salt:       segment.EncryptionSalt,
		iv:         segment.EncryptionIV,
		keyID:      segment.ID,
		codec:      physical_enums.PhysicalBackupCompressionNone,
	})
	if err != nil {
		return err
	}
	defer cleanup()

	return streamTarEntry(tarWriter, "wal/"+segment.WalFilename+".zst", 0o600, reader, checksums)
}

func (rw *Writer) writeHistoryFile(
	tarWriter *tar.Writer,
	history *physical_models.PhysicalWalHistoryFile,
	masterKey string,
	storageCache map[uuid.UUID]*storages.Storage,
	checksums *checksumLedger,
) error {
	store, err := rw.resolveStorage(history.StorageID, storageCache)
	if err != nil {
		return err
	}

	// History files keep their encryption parameters only in the .metadata
	// sidecar, not on the catalog row — read it to learn how to decrypt.
	metadata, err := readHistoryMetadata(store, rw.fieldEncryptor, history.FileName)
	if err != nil {
		return err
	}

	reader, cleanup, err := openArtifact(store, rw.fieldEncryptor, masterKey, artifactSource{
		fileName:   history.FileName,
		encryption: metadata.Encryption,
		salt:       emptyToNil(metadata.EncryptionSalt),
		iv:         emptyToNil(metadata.EncryptionIV),
		keyID:      history.ID,
		codec:      physical_enums.PhysicalBackupCompressionZstd,
	})
	if err != nil {
		return err
	}
	defer cleanup()

	return streamTarEntry(tarWriter, "wal/"+history.HistoryFilename, 0o600, reader, checksums)
}

func (rw *Writer) resolveStorage(
	storageID uuid.UUID,
	storageCache map[uuid.UUID]*storages.Storage,
) (*storages.Storage, error) {
	if cached, ok := storageCache[storageID]; ok {
		return cached, nil
	}

	store, err := rw.storageService.GetStorageByID(storageID)
	if err != nil {
		return nil, fmt.Errorf("load storage %s: %w", storageID, err)
	}

	storageCache[storageID] = store

	return store, nil
}
