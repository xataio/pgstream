// SPDX-License-Identifier: Apache-2.0

package preflight

// Finding ids name the kind of problem a check reports, never the instance of
// it. An id is stable: once released it does not change, because consumers key
// metrics and their own copy on it. An id contains lowercase letters, digits
// and underscores only, so it can never carry a value read from the database
// under test.
const (
	FindingIDConnectionFailed     = "connection_failed"
	FindingIDConnectionPingFailed = "connection_ping_failed"

	FindingIDWALLevelNotLogical               = "wal_level_not_logical"
	FindingIDWAL2JSONUnavailable              = "wal2json_unavailable"
	FindingIDReplicationSlotHeadroomExhausted = "replication_slot_headroom_exhausted"
	FindingIDReplicationRoleAttributeMissing  = "replication_role_attribute_missing"

	FindingIDReplicaIdentityNoPrimaryKey  = "replica_identity_no_primary_key"
	FindingIDReplicaIdentityNothing       = "replica_identity_nothing"
	FindingIDReplicaIdentityIndexUnusable = "replica_identity_index_unusable"
	FindingIDReplicaIdentityUnknown       = "replica_identity_unknown"

	FindingIDSourceTableSelectPrivilegeMissing    = "source_table_select_privilege_missing"
	FindingIDSourceSequenceSelectPrivilegeMissing = "source_sequence_select_privilege_missing"
	FindingIDTargetCreateDBPrivilegeMissing       = "target_createdb_privilege_missing"
	FindingIDTargetCreateRolePrivilegeMissing     = "target_createrole_privilege_missing"

	FindingIDUnsupportedColumnType  = "unsupported_column_type"
	FindingIDUnsupportedRangeType   = "unsupported_range_type"
	FindingIDTargetExtensionMissing = "target_extension_missing"

	FindingIDSnapshotConnectionHeadroomInsufficient = "snapshot_connection_headroom_insufficient"
	FindingIDSourceMultipleInstances                = "source_multiple_instances"

	FindingIDTargetVersionOlderThanSource = "target_version_older_than_source"
)
