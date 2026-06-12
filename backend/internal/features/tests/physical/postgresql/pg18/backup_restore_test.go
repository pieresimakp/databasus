package pg18

import (
	"testing"

	physicaltesting "databasus-backend/internal/features/tests/physical/postgresql/shared"
)

const (
	pgVersion = "18"
	pgImage   = "postgres:18"
)

func Test_PhysicalRestore_FullOnly_RecoversBaseRows(t *testing.T) {
	physicaltesting.RunFullOnlyRecoversBaseRows(t, pgVersion, pgImage)
}

func Test_PhysicalRestore_FullPlusTwoIncrementals_RecoversAllRows(t *testing.T) {
	physicaltesting.RunFullPlusTwoIncrementalsRecoversAllRows(t, pgVersion, pgImage)
}

func Test_PhysicalRestore_FullTwoIncrementalsPlusWal_RecoversToTarget(t *testing.T) {
	physicaltesting.RunFullTwoIncrementalsPlusWalRecoversToTarget(t, pgVersion, pgImage)
}

func Test_PhysicalRestore_WhenWalGapBeforeTarget_TokenRequestReturns422(t *testing.T) {
	physicaltesting.RunWhenWalGapBeforeTargetTokenRequestReturns422(t, pgVersion, pgImage)
}

func Test_PhysicalWalSlot_AppearsWhenBackupingStarts_RemovedWhenDatabaseDeleted(t *testing.T) {
	physicaltesting.RunWalSlotAppearsWhenBackupingStartsRemovedWhenDatabaseDeleted(t, pgVersion, pgImage)
}

func Test_PhysicalWalSlot_WhenDatabaseDeletedWithStreamedWal_SlotRemovedSoNoWalStuck(t *testing.T) {
	physicaltesting.RunWalSlotWhenDatabaseDeletedWithStreamedWalSlotRemovedSoNoWalStuck(t, pgVersion, pgImage)
}
