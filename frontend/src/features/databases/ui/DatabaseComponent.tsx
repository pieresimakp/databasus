import { Spin } from 'antd';
import { useRef, useState } from 'react';
import { useEffect } from 'react';

import { IS_CLOUD } from '../../../constants';
import { backupsApi } from '../../../entity/backups';
import { type Database, PostgresBackupType, databaseApi } from '../../../entity/databases';
import type { UserProfile } from '../../../entity/users';
import { BackupsComponent } from '../../backups';
import { BillingComponent } from '../../billing';
import { HealthckeckAttemptsComponent } from '../../healthcheck';
import { AgentInstallationComponent } from './AgentInstallationComponent';
import { DatabaseConfigComponent } from './DatabaseConfigComponent';

interface Props {
  contentHeight: number;
  databaseId: string;
  user: UserProfile;
  onDatabaseChanged: (database: Database) => void;
  onDatabaseDeleted: () => void;
  isCanManageDBs: boolean;
}

export const DatabaseComponent = ({
  contentHeight,
  databaseId,
  user,
  onDatabaseChanged,
  onDatabaseDeleted,
  isCanManageDBs,
}: Props) => {
  const [currentTab, setCurrentTab] = useState<'config' | 'backups' | 'installation' | 'billing'>(
    'backups',
  );

  const [database, setDatabase] = useState<Database | undefined>();
  const [editDatabase, setEditDatabase] = useState<Database | undefined>();

  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const [isHealthcheckVisible, setIsHealthcheckVisible] = useState(false);

  const handleHealthcheckVisibilityChange = (isVisible: boolean) => {
    setIsHealthcheckVisible(isVisible);
  };

  const isWalDatabase = database?.postgresql?.backupType === PostgresBackupType.WAL_V1;

  const loadSettings = () => {
    setDatabase(undefined);
    setEditDatabase(undefined);
    databaseApi.getDatabase(databaseId).then(setDatabase);
  };

  useEffect(() => {
    loadSettings();
  }, [databaseId]);

  useEffect(() => {
    if (!database) return;

    if (!isWalDatabase) {
      setCurrentTab((prev) => (prev === 'installation' ? 'backups' : prev));
      return;
    }

    backupsApi.getBackups(database.id, 1, 0).then((response) => {
      if (response.total === 0) {
        setCurrentTab('installation');
      }
    });
  }, [database]);

  if (!database) {
    return <Spin />;
  }

  return (
    <div
      className="w-full overflow-y-auto"
      style={{ maxHeight: contentHeight }}
      ref={scrollContainerRef}
    >
      <div className="flex">
        <div
          className={`mr-2 cursor-pointer rounded-tl-md rounded-tr-md px-6 py-2 ${currentTab === 'config' ? 'bg-white dark:bg-gray-800' : 'bg-gray-200 dark:bg-gray-700'}`}
          onClick={() => setCurrentTab('config')}
        >
          Config
        </div>

        <div
          className={`mr-2 cursor-pointer rounded-tl-md rounded-tr-md px-6 py-2 ${currentTab === 'backups' ? 'bg-white dark:bg-gray-800' : 'bg-gray-200 dark:bg-gray-700'}`}
          onClick={() => setCurrentTab('backups')}
        >
          Backups
        </div>

        {isWalDatabase && (
          <div
            className={`mr-2 cursor-pointer rounded-tl-md rounded-tr-md px-6 py-2 ${currentTab === 'installation' ? 'bg-white dark:bg-gray-800' : 'bg-gray-200 dark:bg-gray-700'}`}
            onClick={() => setCurrentTab('installation')}
          >
            Agent
          </div>
        )}

        {IS_CLOUD && isCanManageDBs && (
          <div
            className={`mr-2 cursor-pointer rounded-tl-md rounded-tr-md px-6 py-2 ${currentTab === 'billing' ? 'bg-white dark:bg-gray-800' : 'bg-gray-200 dark:bg-gray-700'}`}
            onClick={() => setCurrentTab('billing')}
          >
            Billing
          </div>
        )}
      </div>

      {currentTab === 'config' && (
        <DatabaseConfigComponent
          database={database}
          user={user}
          setDatabase={setDatabase}
          onDatabaseChanged={onDatabaseChanged}
          onDatabaseDeleted={onDatabaseDeleted}
          editDatabase={editDatabase}
          setEditDatabase={setEditDatabase}
          isCanManageDBs={isCanManageDBs}
        />
      )}

      {currentTab === 'backups' && (
        <>
          <HealthckeckAttemptsComponent
            database={database}
            onVisibilityChange={handleHealthcheckVisibilityChange}
          />
          <BackupsComponent
            database={database}
            isCanManageDBs={isCanManageDBs}
            isDirectlyUnderTab={!isHealthcheckVisible}
            scrollContainerRef={scrollContainerRef}
            onNavigateToBilling={() => setCurrentTab('billing')}
          />
        </>
      )}

      {currentTab === 'installation' && isWalDatabase && (
        <AgentInstallationComponent database={database} onTokenGenerated={loadSettings} />
      )}

      {currentTab === 'billing' && IS_CLOUD && isCanManageDBs && (
        <BillingComponent database={database} isCanManageDBs={isCanManageDBs} />
      )}
    </div>
  );
};
