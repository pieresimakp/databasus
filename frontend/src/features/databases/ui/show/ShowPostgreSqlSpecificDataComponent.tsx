import { type Database, PostgresBackupType, PostgresqlVersion } from '../../../../entity/databases';

interface Props {
  database: Database;
}

const postgresqlVersionLabels = {
  [PostgresqlVersion.PostgresqlVersion12]: '12',
  [PostgresqlVersion.PostgresqlVersion13]: '13',
  [PostgresqlVersion.PostgresqlVersion14]: '14',
  [PostgresqlVersion.PostgresqlVersion15]: '15',
  [PostgresqlVersion.PostgresqlVersion16]: '16',
  [PostgresqlVersion.PostgresqlVersion17]: '17',
  [PostgresqlVersion.PostgresqlVersion18]: '18',
};

const backupTypeLabels: Record<string, string> = {
  [PostgresBackupType.PG_DUMP]: 'Remote (logical)',
  [PostgresBackupType.WAL_V1]: 'Agent (physical)',
};

export const ShowPostgreSqlSpecificDataComponent = ({ database }: Props) => {
  const backupType = database.postgresql?.backupType;
  const backupTypeLabel = backupType
    ? (backupTypeLabels[backupType] ?? backupType)
    : 'Remote (pg_dump)';

  const renderPgDumpDetails = () => (
    <>
      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">PG version</div>
        <div>
          {database.postgresql?.version ? postgresqlVersionLabels[database.postgresql.version] : ''}
        </div>
      </div>

      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px] break-all">Host</div>
        <div>{database.postgresql?.host || ''}</div>
      </div>

      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">Port</div>
        <div>{database.postgresql?.port || ''}</div>
      </div>

      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">Username</div>
        <div>{database.postgresql?.username || ''}</div>
      </div>

      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">Password</div>
        <div>{'*************'}</div>
      </div>

      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">DB name</div>
        <div>{database.postgresql?.database || ''}</div>
      </div>

      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">Use HTTPS</div>
        <div>{database.postgresql?.isHttps ? 'Yes' : 'No'}</div>
      </div>

      {!!database.postgresql?.includeSchemas?.length && (
        <div className="mb-1 flex w-full items-center">
          <div className="min-w-[150px]">Include schemas</div>
          <div>{database.postgresql.includeSchemas.join(', ')}</div>
        </div>
      )}
    </>
  );

  const renderWalDetails = () => (
    <>
      {database.postgresql?.version && (
        <div className="mb-1 flex w-full items-center">
          <div className="min-w-[150px]">PG version</div>
          <div>{postgresqlVersionLabels[database.postgresql.version]}</div>
        </div>
      )}
    </>
  );

  const renderDetails = () => {
    switch (backupType) {
      case PostgresBackupType.WAL_V1:
        return renderWalDetails();
      default:
        return renderPgDumpDetails();
    }
  };

  return (
    <div>
      <div className="mb-1 flex w-full items-center">
        <div className="min-w-[150px]">Backup type</div>
        <div>{backupTypeLabel}</div>
      </div>

      {renderDetails()}
    </div>
  );
};
