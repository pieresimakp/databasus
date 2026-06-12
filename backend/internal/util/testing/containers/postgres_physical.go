package containers

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// physicalDataDirTmpfsOptions sizes the tmpfs for physical containers well above the generic
// logical size: a replication source retains WAL (wal_keep_size plus a slot that pins every
// streamed segment until the uploader commits it), and a WAL-stream test forces a 16 MB segment
// per rotation, so the data dir can briefly hold hundreds of MB of un-recycled WAL. The 512m
// logical default fills mid-test and the source crashes with "No space left on device".
const physicalDataDirTmpfsOptions = "rw,size=2g"

// physicalPostgresOptions configures a replication-capable source container.
type physicalPostgresOptions struct {
	summarizeWal            bool
	withTablespace          bool
	omitReplicationHbaEntry bool
}

// PhysicalPostgresOption tunes a physical source container away from its replication-ready default.
type PhysicalPostgresOption func(*physicalPostgresOptions)

// WithoutSummarizer starts the cluster with summarize_wal=off, so an incremental pre-flight reaches
// the SUMMARIZER_OFF fallback deterministically.
func WithoutSummarizer() PhysicalPostgresOption {
	return func(o *physicalPostgresOptions) { o.summarizeWal = false }
}

// WithTablespace pre-creates a custom tablespace at first boot so the pre-flight rejects the cluster
// per ADR-0010 (physical backups do not support custom tablespaces).
func WithTablespace() PhysicalPostgresOption {
	return func(o *physicalPostgresOptions) { o.withTablespace = true }
}

// WithoutReplicationHbaEntry omits the "host replication" pg_hba line, leaving only the image's
// default "host all all all" rule. Such a cluster accepts ordinary and logical-replication
// connections but refuses physical replication — the mode pg_basebackup / pg_receivewal actually use.
func WithoutReplicationHbaEntry() PhysicalPostgresOption {
	return func(o *physicalPostgresOptions) { o.omitReplicationHbaEntry = true }
}

// allowReplicationInitScript appends the pg_hba replication rule at first boot. "host all all" does
// not cover replication connections, so pg_basebackup / pg_receivewal need this explicit line.
func allowReplicationInitScript() testcontainers.ContainerFile {
	body := "#!/bin/bash\nset -e\n" +
		`echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"` + "\n"

	return testcontainers.ContainerFile{
		Reader:            strings.NewReader(body),
		ContainerFilePath: "/docker-entrypoint-initdb.d/02_allow_replication.sh",
		FileMode:          0o755,
	}
}

// createTablespaceInitScript pre-creates the custom_ts tablespace at first boot.
func createTablespaceInitScript() testcontainers.ContainerFile {
	body := "#!/bin/bash\nset -e\n" +
		"mkdir -p /var/lib/postgresql/custom_tablespace_dir\n" +
		`psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL` + "\n" +
		"  CREATE TABLESPACE custom_ts LOCATION '/var/lib/postgresql/custom_tablespace_dir';\n" +
		"SQL\n"

	return testcontainers.ContainerFile{
		Reader:            strings.NewReader(body),
		ContainerFilePath: "/docker-entrypoint-initdb.d/01_create_tablespace.sh",
		FileMode:          0o755,
	}
}

func physicalPostgresCmd(opts physicalPostgresOptions) []string {
	summarize := "summarize_wal=on"
	if !opts.summarizeWal {
		summarize = "summarize_wal=off"
	}

	return []string{
		"-c", "fsync=off", "-c", "full_page_writes=off", "-c", "synchronous_commit=off",
		"-c", "wal_level=logical", "-c", summarize,
		"-c", "max_wal_senders=10", "-c", "max_replication_slots=10", "-c", "wal_keep_size=512MB",
	}
}

// StartPhysicalPostgres boots a replication-capable PostgreSQL source from image (e.g. "postgres:17"):
// the logical wal_level, WAL senders/slots and a pg_hba replication rule that pg_basebackup and
// pg_receivewal require, on top of the throwaway-server durability flags.
func StartPhysicalPostgres(t *testing.T, image string, opts ...PhysicalPostgresOption) Endpoint {
	t.Helper()

	options := physicalPostgresOptions{summarizeWal: true}
	for _, apply := range opts {
		apply(&options)
	}

	var files []testcontainers.ContainerFile
	if !options.omitReplicationHbaEntry {
		files = append(files, allowReplicationInitScript())
	}
	if options.withTablespace {
		files = append(files, createTablespaceInitScript())
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{postgresPort},
		Env:          postgresEnv(),
		Cmd:          physicalPostgresCmd(options),
		Files:        files,
		Tmpfs:        map[string]string{postgresDataDir(image): physicalDataDirTmpfsOptions},
		WaitingFor:   postgresReady(),
	}

	return start(t, req, postgresPort)
}

// StartPhysicalPostgresMtls builds and boots the replication-capable mTLS PostgreSQL source from
// contextDir (Dockerfile + server cert/key, ca.crt and a replication-aware pg_hba.conf). Same
// key-permission rationale as StartPostgresMtls: the Dockerfile chowns+chmods the key, which a
// copied-in file cannot. The test connects with sslmode=verify-ca and a client cert.
func StartPhysicalPostgresMtls(t *testing.T, contextDir string) Endpoint {
	t.Helper()

	return startPostgresBuild(t, contextDir, "databasus-test-postgres-physical-mtls")
}

// RestoreTarget is an idle restore-target container: PostgreSQL is not started on boot (the
// entrypoint sleeps), so the e2e test extracts the streamed bundle, runs pg_combinebackup and starts
// the cluster by hand inside it. The data dir lives on tmpfs and is discarded with the container.
type RestoreTarget struct {
	handle ContainerHandle
}

// StartPhysicalRestoreTarget builds (and caches via KeepImage) an idle restore-target image for
// image (e.g. "postgres:17"). zstd is added because the postgres image lacks the CLI; pg_combinebackup
// and pg_verifybackup ship with it. The build context (a two-line Dockerfile) is written to a temp
// dir so the helper needs no committed testdata.
func StartPhysicalRestoreTarget(t *testing.T, image string) RestoreTarget {
	t.Helper()

	contextDir := t.TempDir()
	dockerfile := fmt.Sprintf(
		"FROM %s\nRUN apt-get update && apt-get install -y --no-install-recommends zstd "+
			"&& rm -rf /var/lib/apt/lists/*\n",
		image,
	)
	if err := os.WriteFile(filepath.Join(contextDir, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatalf("failed to write restore-target Dockerfile: %v", err)
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:   contextDir,
			Repo:      "databasus-test-postgres-restore-target",
			Tag:       restoreTargetTag(image),
			KeepImage: true,
		},
		ExposedPorts: []string{postgresPort},
		Entrypoint:   []string{"sleep", "infinity"},
		Tmpfs:        map[string]string{"/restore": physicalDataDirTmpfsOptions},
		// postgres never starts on its own here, so a port/log wait would hang; confirm the
		// container is up by running a no-op exec instead.
		WaitingFor: wait.ForExec([]string{"true"}),
	}

	return RestoreTarget{handle: startContainer(t, req, postgresPort)}
}

// restoreTargetTag derives a stable per-version image tag so KeepImage caches 17 and 18 separately.
func restoreTargetTag(image string) string {
	if major := postgresMajorVersion(image); major > 0 {
		return fmt.Sprintf("pg%d", major)
	}

	return "latest"
}

// Host is the reachable host of the restore target (honours TESTCONTAINERS_HOST_OVERRIDE).
func (rt RestoreTarget) Host() string { return rt.handle.Host }

// MappedPort is the host port published for the restored cluster's 5432. Docker publishes it at
// container start, but a connection only succeeds once pg_ctl has started the cluster.
func (rt RestoreTarget) MappedPort() int { return rt.handle.Port }

// Exec runs args in the container and fails the test on a non-zero exit, returning combined output.
func (rt RestoreTarget) Exec(t *testing.T, args ...string) []byte {
	t.Helper()

	out, code := rt.exec(args, "")
	if code != 0 {
		t.Fatalf("restore-target exec %v failed (exit %d):\n%s", args, code, out)
	}

	return out
}

// ExecAs runs args as user (e.g. "postgres") and fails the test on a non-zero exit.
func (rt RestoreTarget) ExecAs(t *testing.T, user string, args ...string) []byte {
	t.Helper()

	out, code := rt.exec(args, user)
	if code != 0 {
		t.Fatalf("restore-target exec --user %s %v failed (exit %d):\n%s", user, args, code, out)
	}

	return out
}

// ExecBestEffort runs args ignoring any error, returning combined output. For cleanup paths that run
// after the test has finished, where failing the test object is neither possible nor wanted.
func (rt RestoreTarget) ExecBestEffort(user string, args ...string) []byte {
	out, _ := rt.exec(args, user)

	return out
}

// CopyFileToContainer copies a host file into the container at mode (replaces docker cp).
func (rt RestoreTarget) CopyFileToContainer(t *testing.T, hostPath, containerPath string, mode int64) {
	t.Helper()

	if err := rt.handle.Container.CopyFileToContainer(
		context.Background(), hostPath, containerPath, mode,
	); err != nil {
		t.Fatalf("failed to copy %s into restore target: %v", hostPath, err)
	}
}

func (rt RestoreTarget) exec(args []string, user string) ([]byte, int) {
	options := []tcexec.ProcessOption{tcexec.Multiplexed()}
	if user != "" {
		options = append(options, tcexec.WithUser(user))
	}

	code, reader, err := rt.handle.Container.Exec(context.Background(), args, options...)
	if err != nil {
		return []byte(err.Error()), -1
	}

	out, _ := io.ReadAll(reader)

	return out, code
}
