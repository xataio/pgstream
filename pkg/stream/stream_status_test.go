// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	pgmigrations "github.com/xataio/pgstream/migrations/postgres"
)

func TestStatusChecker_InitStatus(t *testing.T) {
	t.Parallel()

	migrationVersion := uint(len(pgmigrations.AssetNames()) / 2)
	const testReplicationSlot = "pgstream_db_slot"
	const testDB = "db"
	errTest := errors.New("oh noes")

	tests := []struct {
		name                string
		connBuilder         pglib.QuerierBuilder
		configParser        func(pgURL string) (*pgx.ConnConfig, error)
		migratorBuilder     func(string) (migrator, error)
		pgurl               string
		replicationSlotName string

		wantstatus *InitStatus
		wantErr    error
	}{
		{
			name: "ok",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1":
							require.Len(t, args, 1)
							require.Equal(t, args[0], testReplicationSlot)
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 3)

								name, ok := dest[0].(*string)
								require.True(t, ok)
								*name = testReplicationSlot

								plugin, ok := dest[1].(*string)
								require.True(t, ok)
								*plugin = wal2jsonPlugin

								db, ok := dest[2].(*string)
								require.True(t, ok)
								*db = testDB

								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			migratorBuilder: func(s string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return migrationVersion, false, nil
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",

			wantstatus: &InitStatus{
				PgstreamSchema: &SchemaStatus{
					SchemaExists:         true,
					SchemaLogTableExists: true,
				},
				ReplicationSlot: &ReplicationSlotStatus{
					Name:     testReplicationSlot,
					Plugin:   wal2jsonPlugin,
					Database: testDB,
				},
				Migration: &MigrationStatus{
					Version: migrationVersion,
					Dirty:   false,
				},
			},
			wantErr: nil,
		},
		{
			name:    "error - missing postgres URL",
			pgurl:   "",
			wantErr: errMissingPostgresURL,
		},
		{
			name: "error - validating schema",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								return errTest
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			migratorBuilder: func(s string) (migrator, error) {
				return nil, errors.New("unexpected call to migrator builder")
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return nil, errors.New("unexpected call to config parser")
			},
			pgurl: "postgres://user:password@localhost:5432/db",

			wantstatus: nil,
			wantErr:    errTest,
		},
		{
			name: "error - validating migrations",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			migratorBuilder: func(s string) (migrator, error) {
				return nil, errTest
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return nil, errors.New("unexpected call to config parser")
			},
			pgurl: "postgres://user:password@localhost:5432/db",

			wantstatus: nil,
			wantErr:    errTest,
		},
		{
			name: "error - validating replication slot",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								return errTest
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			migratorBuilder: func(s string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return migrationVersion, false, nil
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",

			wantstatus: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.configParser = tc.configParser
			sc.connBuilder = tc.connBuilder
			sc.migratorBuilder = tc.migratorBuilder

			status, err := sc.InitStatus(context.Background(), tc.pgurl, tc.replicationSlotName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantstatus)
		})
	}
}

func TestStatusChecker_validateSchemaStatus(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		connBuilder pglib.QuerierBuilder
		pgurl       string

		wantStatus *SchemaStatus
		wantErr    error
	}{
		{
			name: "ok - schema and schema_log table exist",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &SchemaStatus{
				SchemaExists:         true,
				SchemaLogTableExists: true,
			},
			wantErr: nil,
		},
		{
			name: "ok - schema exists but schema_log table does not",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = false
								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &SchemaStatus{
				SchemaExists:         true,
				SchemaLogTableExists: false,
				Errors:               []string{noPgstreamSchemaLogTableErrMsg},
			},
			wantErr: nil,
		},
		{
			name: "ok - schema does not exist",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = false
								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &SchemaStatus{
				SchemaExists:         false,
				SchemaLogTableExists: false,
				Errors:               []string{noPgstreamSchemaErrMsg},
			},
			wantErr: nil,
		},
		{
			name: "error - query failure when checking schema existence",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								return errTest
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			pgurl:      "postgres://user:password@localhost:5432/db",
			wantStatus: nil,
			wantErr:    errTest,
		},
		{
			name: "error - query failure when checking schema_log table existence",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgstream')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 1)
								exists, ok := dest[0].(*bool)
								require.True(t, ok)
								*exists = true
								return nil
							}}
						case "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgstream' AND table_name = 'schema_log')":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								return errTest
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			pgurl:      "postgres://user:password@localhost:5432/db",
			wantStatus: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.connBuilder = tc.connBuilder

			status, err := sc.validateSchemaStatus(context.Background(), tc.pgurl)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantStatus)
		})
	}
}

func TestStatusChecker_validateMigrationStatus(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")
	okMigrationVersion := uint(len(pgmigrations.AssetNames()) / 2)

	tests := []struct {
		name            string
		migratorBuilder func(string) (migrator, error)
		pgurl           string

		wantStatus *MigrationStatus
		wantErr    error
	}{
		{
			name: "ok - valid migration status",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return okMigrationVersion, false, nil
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &MigrationStatus{
				Version: okMigrationVersion,
				Dirty:   false,
			},
			wantErr: nil,
		},
		{
			name: "ok - pgstream schema doesn't exist",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return nil, errors.New("failed to open database: no schema")
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &MigrationStatus{
				Errors: []string{noMigrationsTableErrMsg},
			},
			wantErr: nil,
		},
		{
			name: "ok - dirty migration",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return okMigrationVersion, true, nil
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &MigrationStatus{
				Version: okMigrationVersion,
				Dirty:   true,
				Errors:  []string{fmt.Sprintf("migration version %d is dirty", okMigrationVersion)},
			},
			wantErr: nil,
		},
		{
			name: "ok - mismatched migration files",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return 3, false, nil
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &MigrationStatus{
				Version: 3,
				Dirty:   false,
				Errors:  []string{fmt.Sprintf("migration version (3) does not match the number of migration files (%d)", okMigrationVersion)},
			},
			wantErr: nil,
		},
		{
			name: "ok - multiple migration errors",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return 3, true, nil
					},
				}, nil
			},
			pgurl: "postgres://user:password@localhost:5432/db",
			wantStatus: &MigrationStatus{
				Version: 3,
				Dirty:   true,
				Errors:  []string{fmt.Sprintf("migration version (3) does not match the number of migration files (%d)", okMigrationVersion), "migration version 3 is dirty"},
			},
			wantErr: nil,
		},
		{
			name: "error - failed to create migrator",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return nil, errTest
			},
			pgurl:      "postgres://user:password@localhost:5432/db",
			wantStatus: nil,
			wantErr:    errTest,
		},
		{
			name: "error - failed to get migration version",
			migratorBuilder: func(pgURL string) (migrator, error) {
				return &mockMigrator{
					versionFn: func() (uint, bool, error) {
						return 0, false, errTest
					},
				}, nil
			},
			pgurl:      "postgres://user:password@localhost:5432/db",
			wantStatus: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.migratorBuilder = tc.migratorBuilder

			status, err := sc.validateMigrationStatus(tc.pgurl)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantStatus)
		})
	}
}

func TestStatusChecker_validateReplicationSlotStatus(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")
	const testReplicationSlot = "pgstream_db_slot"
	const testDB = "db"

	tests := []struct {
		name                string
		connBuilder         pglib.QuerierBuilder
		configParser        func(pgURL string) (*pgx.ConnConfig, error)
		replicationSlotName string
		pgurl               string

		wantStatus *ReplicationSlotStatus
		wantErr    error
	}{
		{
			name: "ok - valid replication slot status",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1":
							require.Len(t, args, 1)
							require.Equal(t, args[0], testReplicationSlot)
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 3)

								name, ok := dest[0].(*string)
								require.True(t, ok)
								*name = testReplicationSlot

								plugin, ok := dest[1].(*string)
								require.True(t, ok)
								*plugin = wal2jsonPlugin

								db, ok := dest[2].(*string)
								require.True(t, ok)
								*db = testDB

								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			replicationSlotName: testReplicationSlot,
			pgurl:               "postgres://user:password@localhost:5432/db",
			wantStatus: &ReplicationSlotStatus{
				Name:     testReplicationSlot,
				Plugin:   wal2jsonPlugin,
				Database: testDB,
			},
			wantErr: nil,
		},
		{
			name: "ok - replication slot does not exist",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								return pglib.ErrNoRows
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			replicationSlotName: testReplicationSlot,
			pgurl:               "postgres://user:password@localhost:5432/db",
			wantStatus: &ReplicationSlotStatus{
				Errors: []string{fmt.Sprintf("replication slot %s does not exist in the configured database", testReplicationSlot)},
			},
			wantErr: nil,
		},
		{
			name: "ok - replication slot on wrong database",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 3)

								name, ok := dest[0].(*string)
								require.True(t, ok)
								*name = testReplicationSlot

								plugin, ok := dest[1].(*string)
								require.True(t, ok)
								*plugin = wal2jsonPlugin

								db, ok := dest[2].(*string)
								require.True(t, ok)
								*db = "wrong_db"

								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			replicationSlotName: testReplicationSlot,
			pgurl:               "postgres://user:password@localhost:5432/db",
			wantStatus: &ReplicationSlotStatus{
				Name:     testReplicationSlot,
				Plugin:   wal2jsonPlugin,
				Database: "wrong_db",
				Errors:   []string{fmt.Sprintf("replication slot %s does not exist in the configured database", testReplicationSlot)},
			},
			wantErr: nil,
		},
		{
			name: "ok - replication slot using wrong plugin",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						switch sql {
						case "SELECT slot_name, plugin, database FROM pg_replication_slots WHERE slot_name = $1":
							return &pgmocks.Row{ScanFn: func(dest ...any) error {
								require.Len(t, dest, 3)

								name, ok := dest[0].(*string)
								require.True(t, ok)
								*name = testReplicationSlot

								plugin, ok := dest[1].(*string)
								require.True(t, ok)
								*plugin = "wrong_plugin"

								db, ok := dest[2].(*string)
								require.True(t, ok)
								*db = testDB

								return nil
							}}
						default:
							return &pgmocks.Row{ScanFn: func(args ...any) error { return fmt.Errorf("unexpected query: %v", sql) }}
						}
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			replicationSlotName: testReplicationSlot,
			pgurl:               "postgres://user:password@localhost:5432/db",
			wantStatus: &ReplicationSlotStatus{
				Name:     testReplicationSlot,
				Plugin:   "wrong_plugin",
				Database: testDB,
				Errors:   []string{fmt.Sprintf("replication slot %s is not using the wal2json plugin", testReplicationSlot)},
			},
			wantErr: nil,
		},
		{
			name: "error - query failure",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					QueryRowFn: func(ctx context.Context, sql string, args ...any) pglib.Row {
						return &pgmocks.Row{ScanFn: func(dest ...any) error {
							return errTest
						}}
					},
				}, nil
			},
			configParser: func(pgURL string) (*pgx.ConnConfig, error) {
				return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
			},
			replicationSlotName: testReplicationSlot,
			pgurl:               "postgres://user:password@localhost:5432/db",
			wantStatus:          nil,
			wantErr:             errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.connBuilder = tc.connBuilder
			sc.configParser = tc.configParser

			status, err := sc.validateReplicationSlotStatus(context.Background(), tc.pgurl, tc.replicationSlotName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantStatus)
		})
	}
}

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
			wantOutput: `Pgstream schema exists: true
Pgstream schema_log table exists: true
Migration current version: 5
Migration status: success
Replication slot name: pgstream_db_slot
Replication slot plugin: wal2json
Replication slot database: db`,
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
			wantOutput: `Pgstream schema exists: false
Pgstream schema_log table exists: false
Pgstream schema errors: [pgstream schema does not exist in the configured postgres database]`,
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
			wantOutput: `Migration current version: 3
Migration status: failed
Migration errors: [migration version 3 is dirty]`,
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
			wantOutput: `Replication slot name: pgstream_db_slot
Replication slot plugin: wrong_plugin
Replication slot database: db
Replication slot errors: [replication slot pgstream_db_slot is not using the wal2json plugin]`,
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
