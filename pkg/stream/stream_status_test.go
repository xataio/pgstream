// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitStatus_GetErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initStatus *InitStatus
		wantErrors []string
	}{
		{
			name: "all components valid",
			initStatus: &InitStatus{
				PgstreamSchema: &SchemaStatus{
					SchemaExists:         true,
					SchemaLogTableExists: true,
				},
				Migration: &MigrationStatus{
					Version: 5,
					Dirty:   false,
				},
				ReplicationSlot: &ReplicationSlotStatus{
					Name:     "pgstream_db_slot",
					Plugin:   "wal2json",
					Database: "db",
				},
			},
			wantErrors: []string{},
		},
		{
			name: "schema errors",
			initStatus: &InitStatus{
				PgstreamSchema: &SchemaStatus{
					SchemaExists:         false,
					SchemaLogTableExists: false,
					Errors:               []string{"pgstream schema does not exist in the configured postgres database"},
				},
			},
			wantErrors: []string{"pgstream schema does not exist in the configured postgres database"},
		},
		{
			name: "migration errors",
			initStatus: &InitStatus{
				Migration: &MigrationStatus{
					Version: 3,
					Dirty:   true,
					Errors:  []string{"migration version 3 is dirty"},
				},
			},
			wantErrors: []string{"migration version 3 is dirty"},
		},
		{
			name: "replication slot errors",
			initStatus: &InitStatus{
				ReplicationSlot: &ReplicationSlotStatus{
					Name:     "pgstream_db_slot",
					Plugin:   "wrong_plugin",
					Database: "db",
					Errors:   []string{"replication slot pgstream_db_slot is not using the wal2json plugin"},
				},
			},
			wantErrors: []string{"replication slot pgstream_db_slot is not using the wal2json plugin"},
		},
		{
			name: "multiple errors",
			initStatus: &InitStatus{
				PgstreamSchema: &SchemaStatus{
					SchemaExists:         false,
					SchemaLogTableExists: false,
					Errors:               []string{"pgstream schema does not exist in the configured postgres database"},
				},
				Migration: &MigrationStatus{
					Version: 3,
					Dirty:   true,
					Errors:  []string{"migration version 3 is dirty"},
				},
				ReplicationSlot: &ReplicationSlotStatus{
					Name:     "pgstream_db_slot",
					Plugin:   "wrong_plugin",
					Database: "db",
					Errors:   []string{"replication slot pgstream_db_slot is not using the wal2json plugin"},
				},
			},
			wantErrors: []string{
				"pgstream schema does not exist in the configured postgres database",
				"migration version 3 is dirty",
				"replication slot pgstream_db_slot is not using the wal2json plugin",
			},
		},
		{
			name:       "nil InitStatus",
			initStatus: nil,
			wantErrors: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			errors := tc.initStatus.GetErrors()
			require.Equal(t, tc.wantErrors, errors)
		})
	}
}

func TestStatus_PrettyPrint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     *Status
		wantOutput string
	}{
		{
			name: "all components valid",
			status: &Status{
				Init: &InitStatus{
					PgstreamSchema: &SchemaStatus{
						SchemaExists:         true,
						SchemaLogTableExists: true,
					},
					Migration: &MigrationStatus{
						Version: 5,
						Dirty:   false,
					},
					ReplicationSlot: &ReplicationSlotStatus{
						Name:     "pgstream_db_slot",
						Plugin:   "wal2json",
						Database: "db",
					},
				},
				Config: &ConfigStatus{
					Valid: true,
				},
				Source: &SourceStatus{
					Reachable: true,
				},
				TransformationRules: &TransformationRulesStatus{
					Valid: true,
				},
			},
			wantOutput: `Initialisation status:
 - Pgstream schema exists: true
 - Pgstream schema_log table exists: true
 - Migration current version: 5
 - Migration status: success
 - Replication slot name: pgstream_db_slot
 - Replication slot plugin: wal2json
 - Replication slot database: db
Config status:
 - Valid: true
Transformation rules status:
 - Valid: true
Source status:
 - Reachable: true`,
		},
		{
			name: "components with errors",
			status: &Status{
				Init: &InitStatus{
					PgstreamSchema: &SchemaStatus{
						SchemaExists:         false,
						SchemaLogTableExists: false,
						Errors:               []string{"pgstream schema does not exist"},
					},
					Migration: &MigrationStatus{
						Version: 3,
						Dirty:   true,
						Errors:  []string{"migration version 3 is dirty"},
					},
					ReplicationSlot: &ReplicationSlotStatus{
						Name:     "pgstream_db_slot",
						Plugin:   "wrong_plugin",
						Database: "db",
						Errors:   []string{"replication slot is not using wal2json plugin"},
					},
				},
				Config: &ConfigStatus{
					Valid:  false,
					Errors: []string{"invalid configuration"},
				},
				Source: &SourceStatus{
					Reachable: false,
					Errors:    []string{"source not reachable"},
				},
				TransformationRules: &TransformationRulesStatus{
					Valid:  false,
					Errors: []string{"invalid transformation rules"},
				},
			},
			wantOutput: `Initialisation status:
 - Pgstream schema exists: false
 - Pgstream schema_log table exists: false
 - Pgstream schema errors: [pgstream schema does not exist]
 - Migration current version: 3
 - Migration status: failed
 - Migration errors: [migration version 3 is dirty]
 - Replication slot name: pgstream_db_slot
 - Replication slot plugin: wrong_plugin
 - Replication slot database: db
 - Replication slot errors: [replication slot is not using wal2json plugin]
Config status:
 - Valid: false
 - Errors: [invalid configuration]
Transformation rules status:
 - Valid: false
 - Errors: [invalid transformation rules]
Source status:
 - Reachable: false
 - Errors: [source not reachable]`,
		},
		{
			name:       "nil status",
			status:     nil,
			wantOutput: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			output := tc.status.PrettyPrint()
			require.Equal(t, tc.wantOutput, output)
		})
	}
}

func TestInitStatus_PrettyPrint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initStatus *InitStatus
		wantOutput string
	}{
		{
			name: "all components valid",
			initStatus: &InitStatus{
				PgstreamSchema: &SchemaStatus{
					SchemaExists:         true,
					SchemaLogTableExists: true,
				},
				Migration: &MigrationStatus{
					Version: 5,
					Dirty:   false,
				},
				ReplicationSlot: &ReplicationSlotStatus{
					Name:     "pgstream_db_slot",
					Plugin:   "wal2json",
					Database: "db",
				},
			},
			wantOutput: `Initialisation status:
 - Pgstream schema exists: true
 - Pgstream schema_log table exists: true
 - Migration current version: 5
 - Migration status: success
 - Replication slot name: pgstream_db_slot
 - Replication slot plugin: wal2json
 - Replication slot database: db`,
		},
		{
			name: "schema errors",
			initStatus: &InitStatus{
				PgstreamSchema: &SchemaStatus{
					SchemaExists:         false,
					SchemaLogTableExists: false,
					Errors:               []string{"pgstream schema does not exist in the configured postgres database"},
				},
			},
			wantOutput: `Initialisation status:
 - Pgstream schema exists: false
 - Pgstream schema_log table exists: false
 - Pgstream schema errors: [pgstream schema does not exist in the configured postgres database]`,
		},
		{
			name: "migration errors",
			initStatus: &InitStatus{
				Migration: &MigrationStatus{
					Version: 3,
					Dirty:   true,
					Errors:  []string{"migration version 3 is dirty"},
				},
			},
			wantOutput: `Initialisation status:
 - Migration current version: 3
 - Migration status: failed
 - Migration errors: [migration version 3 is dirty]`,
		},
		{
			name: "replication slot errors",
			initStatus: &InitStatus{
				ReplicationSlot: &ReplicationSlotStatus{
					Name:     "pgstream_db_slot",
					Plugin:   "wrong_plugin",
					Database: "db",
					Errors:   []string{"replication slot pgstream_db_slot is not using the wal2json plugin"},
				},
			},
			wantOutput: `Initialisation status:
 - Replication slot name: pgstream_db_slot
 - Replication slot plugin: wrong_plugin
 - Replication slot database: db
 - Replication slot errors: [replication slot pgstream_db_slot is not using the wal2json plugin]`,
		},
		{
			name:       "nil InitStatus",
			initStatus: nil,
			wantOutput: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			output := tc.initStatus.PrettyPrint()
			require.Equal(t, tc.wantOutput, output)
		})
	}
}
