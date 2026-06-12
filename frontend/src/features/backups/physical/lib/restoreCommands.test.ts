import { describe, expect, it } from 'vitest';

import { buildDockerScriptCommand, buildManualSteps, buildScriptCommand } from './restoreCommands';

const scriptUrl = 'https://app.example.com/api/v1/backups/physical/recovery-script';
const bundleUrl = 'https://app.example.com/api/v1/backups/physical/restore-stream?token=abc';

describe('buildScriptCommand', () => {
  it('omits --pg-bin and --target-time when neither is set', () => {
    const command = buildScriptCommand({
      scriptUrl,
      bundleUrl,
      outputDir: './databasus-restore',
      pgBin: '',
      targetTime: '',
    });

    expect(command).not.toContain('--pg-bin');
    expect(command).not.toContain('--target-time');
    expect(command).toContain(`"${bundleUrl}" "./databasus-restore"`);
  });

  it('includes --pg-bin when a bin path is set', () => {
    const command = buildScriptCommand({
      scriptUrl,
      bundleUrl,
      outputDir: './databasus-restore',
      pgBin: '/usr/lib/postgresql/18/bin',
      targetTime: '',
    });

    expect(command).toContain('--pg-bin "/usr/lib/postgresql/18/bin"');
  });

  it('includes --target-time when a target time is set', () => {
    const command = buildScriptCommand({
      scriptUrl,
      bundleUrl,
      outputDir: './databasus-restore',
      pgBin: '',
      targetTime: '2026-06-12 14:30:00+00:00',
    });

    expect(command).toContain('--target-time "2026-06-12 14:30:00+00:00"');
  });
});

describe('buildDockerScriptCommand', () => {
  it('embeds the chosen image and downloads the bundle once', () => {
    const command = buildDockerScriptCommand({
      scriptUrl,
      bundleUrl,
      outputDir: './databasus-restore',
      image: 'postgres:18',
      targetTime: '',
    });

    expect(command).toContain('docker run --rm -v "$PWD:/work" -w /work postgres:18');
    expect(command).not.toContain('--target-time');
    expect(
      command.match(/curl -fsSL "https:\/\/app.example.com[^"]*restore-stream[^"]*"/g),
    ).toHaveLength(1);
  });

  it('passes --target-time to the in-container script when a target time is set', () => {
    const command = buildDockerScriptCommand({
      scriptUrl,
      bundleUrl,
      outputDir: './databasus-restore',
      image: 'postgres:18',
      targetTime: '2026-06-12 14:30:00+00:00',
    });

    expect(command).toContain(
      'sh databasus-recovery.sh --target-time "2026-06-12 14:30:00+00:00" restore.tar',
    );
  });
});

describe('buildManualSteps', () => {
  const base = {
    bundleUrl,
    outputDir: './databasus-restore',
    pgBin: '',
    image: 'postgres:18',
    targetTime: '',
  };

  it('reconstructs with pg_combinebackup and skips recovery when there is no WAL', () => {
    const steps = buildManualSteps({ ...base, environment: 'host', hasWal: false });
    const titles = steps.map((step) => step.title);

    expect(steps.some((step) => step.code.includes('pg_combinebackup'))).toBe(true);
    expect(titles).not.toContain('Decompress WAL and wire up recovery');
  });

  it('includes the WAL recovery step when there is WAL', () => {
    const steps = buildManualSteps({ ...base, environment: 'host', hasWal: true });
    const recovery = steps.find((step) => step.title === 'Decompress WAL and wire up recovery');

    expect(recovery).toBeDefined();
    expect(recovery?.code).toContain('recovery.signal');
  });

  it('bakes the target time into recovery settings only when one is given', () => {
    const withTarget = buildManualSteps({
      ...base,
      environment: 'host',
      hasWal: true,
      targetTime: '2026-06-12 14:30:00+00:00',
    });
    const recovery = withTarget.find(
      (step) => step.title === 'Decompress WAL and wire up recovery',
    );

    expect(recovery?.code).toContain("recovery_target_time = '2026-06-12 14:30:00+00:00'");
    expect(recovery?.code).toContain("recovery_target_action = 'promote'");

    const withoutTarget = buildManualSteps({ ...base, environment: 'host', hasWal: true });
    const latestRecovery = withoutTarget.find(
      (step) => step.title === 'Decompress WAL and wire up recovery',
    );

    expect(latestRecovery?.code).not.toContain('recovery_target_time');
  });

  it('runs pg_combinebackup through docker in the docker environment', () => {
    const steps = buildManualSteps({ ...base, environment: 'docker', hasWal: false });
    const combine = steps.find((step) => step.title === 'Reconstruct the data directory');

    expect(combine?.code).toContain('docker run --rm -v "$PWD:/work" -w /work postgres:18');
    expect(combine?.code).toContain('pg_combinebackup');
  });
});
