package postgresql_shared

// Renaming a value requires updating any client that switches on it.
type ConnectionErrorCode string

const (
	ConnErrConnectionFailed         ConnectionErrorCode = "connection_failed"
	ConnErrPgHbaNoEntry             ConnectionErrorCode = "pg_hba_no_entry"
	ConnErrBadCredentials           ConnectionErrorCode = "bad_credentials"
	ConnErrNoReplicationPrivilege   ConnectionErrorCode = "no_replication_privilege"
	ConnErrWalLevelInvalid          ConnectionErrorCode = "wal_level_invalid"
	ConnErrNoWalSenders             ConnectionErrorCode = "no_wal_senders"
	ConnErrNoReplicationSlots       ConnectionErrorCode = "no_replication_slots"
	ConnErrWalSummaryDisabled       ConnectionErrorCode = "wal_summary_disabled"
	ConnErrCustomTablespaces        ConnectionErrorCode = "custom_tablespaces"
	ConnErrSystemIdentifierMismatch ConnectionErrorCode = "system_identifier_mismatch"
)

// ConnectionTestError carries a machine-readable Code for a classified connection failure.
// All human-facing copy - the message, the fix command and the instructions - lives on the
// client, keyed by Code; the backend stays a thin classifier.
type ConnectionTestError struct {
	Code ConnectionErrorCode `json:"code"`
}

func (e *ConnectionTestError) Error() string {
	return string(e.Code)
}
