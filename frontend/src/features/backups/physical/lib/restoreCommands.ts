// Builds the shell commands the restore dialog shows. Pure string assembly - no
// network or React - so the wiring (which flag appears when, host vs Docker) is
// unit-tested in isolation. The served recovery_script.sh accepts an optional
// `--pg-bin <dir>` plus positional `<bundle> [output-dir]`; the manual steps mirror
// what that script does by hand for users who would rather not pipe curl into sh.

export type RestoreEnvironment = 'host' | 'docker';

export interface ScriptCommandParams {
  scriptUrl: string;
  bundleUrl: string;
  outputDir: string;
  pgBin: string;
  // PostgreSQL-parseable UTC timestamp; empty for a "latest" restore.
  targetTime: string;
}

export interface DockerScriptCommandParams {
  scriptUrl: string;
  bundleUrl: string;
  outputDir: string;
  image: string;
  // PostgreSQL-parseable UTC timestamp; empty for a "latest" restore.
  targetTime: string;
}

export interface ManualStepsParams {
  bundleUrl: string;
  outputDir: string;
  pgBin: string;
  image: string;
  environment: RestoreEnvironment;
  // a per-backup restore ships no WAL; a point-in-time / latest restore does and so
  // needs the recovery-wiring step.
  hasWal: boolean;
  // preformatted timestamp PostgreSQL can parse (empty for a "latest" restore).
  targetTime?: string;
}

export interface RestoreStep {
  title: string;
  code: string;
}

const combineBinary = (pgBin: string): string => {
  const trimmed = pgBin.trim();

  return trimmed ? `"${trimmed}/pg_combinebackup"` : 'pg_combinebackup';
};

// `bundle/full` plus every `bundle/incr-N` in numeric order - the inputs
// pg_combinebackup needs, oldest to newest.
const combineInvocation = (pgBin: string, outputDir: string): string =>
  `${combineBinary(pgBin)} bundle/full $(ls -d bundle/incr-* 2>/dev/null | sort -V) -o "${outputDir}/data"`;

export const buildScriptCommand = ({
  scriptUrl,
  bundleUrl,
  outputDir,
  pgBin,
  targetTime,
}: ScriptCommandParams): string => {
  const pgBinArg = pgBin.trim() ? `--pg-bin "${pgBin.trim()}" ` : '';
  const targetArg = targetTime.trim() ? `--target-time "${targetTime.trim()}" ` : '';

  return `curl -fsSL "${scriptUrl}" | sh -s -- ${pgBinArg}${targetArg}"${bundleUrl}" "${outputDir}"`;
};

export const buildDockerScriptCommand = ({
  scriptUrl,
  bundleUrl,
  outputDir,
  image,
  targetTime,
}: DockerScriptCommandParams): string => {
  const targetArg = targetTime.trim() ? `--target-time "${targetTime.trim()}" ` : '';

  return [
    `curl -fsSL "${scriptUrl}" -o databasus-recovery.sh`,
    `curl -fsSL "${bundleUrl}" -o restore.tar`,
    `docker run --rm -v "$PWD:/work" -w /work ${image} \\`,
    `  sh databasus-recovery.sh ${targetArg}restore.tar "${outputDir}"`,
  ].join('\n');
};

export const buildManualSteps = ({
  bundleUrl,
  outputDir,
  pgBin,
  image,
  environment,
  hasWal,
  targetTime,
}: ManualStepsParams): RestoreStep[] => {
  const combine =
    environment === 'docker'
      ? `docker run --rm -v "$PWD:/work" -w /work ${image} \\\n  sh -c '${combineInvocation('', outputDir)} && chmod 700 "${outputDir}/data"'`
      : `${combineInvocation(pgBin, outputDir)}\nchmod 700 "${outputDir}/data"`;

  const steps: RestoreStep[] = [
    {
      title: 'Download the bundle',
      code: `curl -fsSL "${bundleUrl}" -o restore.tar`,
    },
    {
      title: 'Extract it',
      code: `mkdir -p bundle\ntar -xf restore.tar -C bundle`,
    },
    {
      title: 'Verify the transfer',
      code: `(cd bundle && sha256sum -c MANIFEST.sha256)`,
    },
    {
      title: 'Reconstruct the data directory',
      code: combine,
    },
  ];

  if (hasWal) {
    const targetLines = targetTime
      ? `\n  echo "recovery_target_time = '${targetTime}'"\n  echo "recovery_target_action = 'promote'"`
      : '';

    steps.push({
      title: 'Decompress WAL and wire up recovery',
      code: [
        `mkdir -p "${outputDir}/wal"`,
        `for f in bundle/wal/*; do`,
        `  [ -e "$f" ] || continue`,
        `  case "$f" in`,
        `    *.zst) zstd -dq "$f" -o "${outputDir}/wal/$(basename "\${f%.zst}")" ;;`,
        `    *) cp "$f" "${outputDir}/wal/" ;;`,
        `  esac`,
        `done`,
        `wal_abs=$(cd "${outputDir}/wal" && pwd)`,
        `{`,
        `  echo "restore_command = 'cp \\"$wal_abs/%f\\" \\"%p\\"'"${targetLines}`,
        `} >> "${outputDir}/data/postgresql.auto.conf"`,
        `touch "${outputDir}/data/recovery.signal"`,
      ].join('\n'),
    });
  }

  return steps;
};
