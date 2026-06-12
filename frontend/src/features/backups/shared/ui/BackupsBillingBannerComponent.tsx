import { Button } from 'antd';
import dayjs from 'dayjs';
import { useEffect, useState } from 'react';

import { type Subscription, SubscriptionStatus, billingApi } from '../../../../entity/billing';
import { getUserShortTimeFormat } from '../../../../shared/time';
import { PurchaseComponent } from '../../../billing';

interface Props {
  databaseId: string;
  isCanManageDBs: boolean;
  onNavigateToBilling?: () => void;
}

export const BackupsBillingBannerComponent = ({
  databaseId,
  isCanManageDBs,
  onNavigateToBilling,
}: Props) => {
  const [subscription, setSubscription] = useState<Subscription | null>(null);
  const [isPurchaseModalOpen, setIsPurchaseModalOpen] = useState(false);

  const loadSubscription = async () => {
    try {
      const sub = await billingApi.getSubscription(databaseId);
      setSubscription(sub);
    } catch {
      setSubscription(null);
    }
  };

  useEffect(() => {
    loadSubscription();
  }, [databaseId]);

  if (
    !subscription ||
    (subscription.status !== SubscriptionStatus.Trial &&
      subscription.status !== SubscriptionStatus.Canceled &&
      subscription.status !== SubscriptionStatus.Expired)
  ) {
    return null;
  }

  return (
    <>
      <div
        className={`mt-3 rounded-lg px-4 py-3 text-sm ${
          subscription.status === SubscriptionStatus.Canceled ||
          subscription.status === SubscriptionStatus.Expired
            ? 'border border-red-300/50 bg-red-50 dark:border-red-600/30 dark:bg-red-900/20'
            : 'border border-yellow-300/50 bg-yellow-50 dark:border-yellow-600/30 dark:bg-yellow-900/20'
        } max-w-[500px]`}
      >
        <p
          className={
            subscription.status === SubscriptionStatus.Canceled ||
            subscription.status === SubscriptionStatus.Expired
              ? 'text-red-700 dark:text-red-400'
              : 'text-yellow-700 dark:text-yellow-400'
          }
        >
          {subscription.status === SubscriptionStatus.Trial && (
            <>
              You are on a free trial. Your trial ends on{' '}
              <span className="font-medium">
                {dayjs
                  .utc(subscription.currentPeriodEnd)
                  .local()
                  .format(getUserShortTimeFormat().format)}
              </span>{' '}
              ({dayjs.utc(subscription.currentPeriodEnd).local().fromNow()}). After that, backups
              will be removed.
            </>
          )}

          {subscription.status === SubscriptionStatus.Canceled && (
            <>
              Your subscription has been canceled.{' '}
              {subscription.dataRetentionGracePeriodUntil ? (
                <>
                  Backups will be removed on{' '}
                  <span className="font-medium">
                    {dayjs
                      .utc(subscription.dataRetentionGracePeriodUntil)
                      .local()
                      .format(getUserShortTimeFormat().format)}
                  </span>{' '}
                  ({dayjs.utc(subscription.dataRetentionGracePeriodUntil).local().fromNow()}).
                </>
              ) : (
                <> Backups will be removed after the grace period.</>
              )}
            </>
          )}

          {subscription.status === SubscriptionStatus.Expired && (
            <>Your subscription has expired.</>
          )}
        </p>

        {isCanManageDBs &&
          subscription.status === SubscriptionStatus.Canceled &&
          onNavigateToBilling && (
            <Button type="primary" size="small" className="mt-2" onClick={onNavigateToBilling}>
              Go to Billing
            </Button>
          )}

        {isCanManageDBs && subscription.status !== SubscriptionStatus.Canceled && (
          <Button
            type="primary"
            size="small"
            className="mt-2"
            onClick={() => setIsPurchaseModalOpen(true)}
          >
            Purchase storage
          </Button>
        )}
      </div>

      {isPurchaseModalOpen && (
        <PurchaseComponent
          databaseId={databaseId}
          onSubscriptionChanged={() => loadSubscription()}
          onClose={() => setIsPurchaseModalOpen(false)}
        />
      )}
    </>
  );
};
