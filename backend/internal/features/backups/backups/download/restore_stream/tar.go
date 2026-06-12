package restore_stream

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

// copyTarWithPrefix reads an inner pg_basebackup tar from src and re-emits every
// entry under prefix/ into tarWriter, hashing regular-file bodies into the
// checksum ledger as they pass.
func copyTarWithPrefix(
	tarWriter *tar.Writer,
	src io.Reader,
	prefix string,
	checksums *checksumLedger,
) error {
	tarReader := tar.NewReader(src)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read inner tar header: %w", err)
		}

		name := strings.TrimPrefix(header.Name, "./")
		if name == "" || name == "." {
			continue
		}

		header.Name = prefix + "/" + name

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("write tar header %q: %w", header.Name, err)
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}

		hasher := checksums.begin(header.Name)
		if _, err := io.Copy(io.MultiWriter(tarWriter, hasher), tarReader); err != nil {
			return fmt.Errorf("copy tar entry %q: %w", header.Name, err)
		}

		checksums.commit(header.Name, hasher)
	}
}

// streamTarEntry writes a single regular-file entry of unknown length: the tar
// header needs the size up front, so the (small, 16 MB-or-less) body is read
// fully into memory first. Used for manifests, WAL segments and history files,
// never for arbitrarily large PGDATA members (those go through copyTarWithPrefix).
func streamTarEntry(
	tarWriter *tar.Writer,
	name string,
	mode int64,
	reader io.Reader,
	checksums *checksumLedger,
) error {
	body, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read %q body: %w", name, err)
	}

	return writeTarBytes(tarWriter, name, mode, body, checksums)
}

func writeTarBytes(
	tarWriter *tar.Writer,
	name string,
	mode int64,
	body []byte,
	checksums *checksumLedger,
) error {
	header := &tar.Header{
		Name:     name,
		Mode:     mode,
		Size:     int64(len(body)),
		Typeflag: tar.TypeReg,
		ModTime:  time.Unix(0, 0).UTC(),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("write tar header %q: %w", name, err)
	}

	hasher := checksums.begin(name)
	if _, err := io.Copy(io.MultiWriter(tarWriter, hasher), bytes.NewReader(body)); err != nil {
		return fmt.Errorf("write tar body %q: %w", name, err)
	}

	checksums.commit(name, hasher)

	return nil
}
