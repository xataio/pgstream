// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	pgmigrations "github.com/xataio/pgstream/migrations/postgres"
	pgprocessor "github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	replicationpg "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

func TestStatusChecker_Status(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")
	migrationVersion := uint(len(pgmigrations.AssetNames()) / 2)
	const testReplicationSlot = "pgstream_db_slot"
	const testDB = "db"

	validConfig := &Config{
		Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
			PostgresURL:         "postgres://user:password@localhost:5432/db",
			ReplicationSlotName: testReplicationSlot,
		}}},
		Processor: ProcessorConfig{
			Postgres: &PostgresProcessorConfig{
				BatchWriter: pgprocessor.Config{
					URL: "postgres://user:password@localhost:7654/db",
				},
			},
			Transformer: &transformer.Config{
				TransformerRules: []transformer.TableRules{},
			},
		},
	}

	validConfigParser := func(pgURL string) (*pgx.ConnConfig, error) {
		return &pgx.ConnConfig{Config: pgconn.Config{Database: testDB}}, nil
	}

	validMigratorBuilder := func(s string) (migrator, error) {
		return &mockMigrator{
			versionFn: func() (uint, bool, error) {
				return migrationVersion, false, nil
			},
		}, nil
	}

	validConnBuilder := func(ctx context.Context, pgURL string) (pglib.Querier, error) {
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
	}

	validRuleValidatorBuilder := func(context.Context, string) (ruleValidator, error) {
		return func(rules []transformer.TableRules) (map[string]transformer.ColumnTransformers, error) {
			return nil, nil
		}, nil
	}

	tests := []struct {
		name                 string
		connBuilder          pglib.QuerierBuilder
		migratorBuilder      func(string) (migrator, error)
		config               *Config
		ruleValidatorBuilder func(context.Context, string) (ruleValidator, error)

		wantStatus *Status
		wantErr    error
	}{
		{
			name:                 "ok - all components valid",
			connBuilder:          validConnBuilder,
			migratorBuilder:      validMigratorBuilder,
			config:               validConfig,
			ruleValidatorBuilder: validRuleValidatorBuilder,

			wantStatus: &Status{
				Init: &InitStatus{
					PgstreamSchema: &SchemaStatus{
						SchemaExists:         true,
						SchemaLogTableExists: true,
					},
					Migration: &MigrationStatus{
						Version: migrationVersion,
						Dirty:   false,
					},
					ReplicationSlot: &ReplicationSlotStatus{
						Name:     testReplicationSlot,
						Plugin:   wal2jsonPlugin,
						Database: testDB,
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
			wantErr: nil,
		},
		{
			name: "error - checking source status",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return nil, errTest
			},
			migratorBuilder:      validMigratorBuilder,
			config:               validConfig,
			ruleValidatorBuilder: validRuleValidatorBuilder,

			wantStatus: nil,
			wantErr:    errTest,
		},
		{
			name:        "error - checking init status",
			connBuilder: validConnBuilder,
			migratorBuilder: func(s string) (migrator, error) {
				return nil, errTest
			},
			config:               validConfig,
			ruleValidatorBuilder: validRuleValidatorBuilder,

			wantStatus: nil,
			wantErr:    errTest,
		},
		{
			name:            "error - checking transformation rules status",
			connBuilder:     validConnBuilder,
			migratorBuilder: validMigratorBuilder,
			config:          validConfig,
			ruleValidatorBuilder: func(ctx context.Context, s string) (ruleValidator, error) {
				return nil, errTest
			},

			wantStatus: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.connBuilder = tc.connBuilder
			sc.configParser = validConfigParser
			sc.migratorBuilder = tc.migratorBuilder
			sc.ruleValidatorBuilder = tc.ruleValidatorBuilder

			status, err := sc.Status(context.Background(), tc.config)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantStatus)
		})
	}
}

func TestStatusChecker_configStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		config     *Config
		wantStatus *ConfigStatus
	}{
		{
			name: "valid config",
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
				Processor: ProcessorConfig{
					Postgres: &PostgresProcessorConfig{
						BatchWriter: pgprocessor.Config{
							URL: "postgres://user:password@localhost:7654/db",
						},
					},
				},
			},
			wantStatus: &ConfigStatus{
				Valid: true,
			},
		},
		{
			name: "invalid config",
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: &ConfigStatus{
				Valid:  false,
				Errors: []string{"need at least one processor configured"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			status := sc.configStatus(tc.config)
			require.Equal(t, tc.wantStatus, status)
		})
	}
}

func TestStatusChecker_sourceStatus(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		connBuilder pglib.QuerierBuilder
		config      *Config
		wantStatus  *SourceStatus
		wantErr     error
	}{
		{
			name: "ok - source reachable",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					PingFn: func(ctx context.Context) error {
						return nil
					},
				}, nil
			},
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: &SourceStatus{
				Reachable: true,
			},
			wantErr: nil,
		},
		{
			name: "error - source not provided",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return nil, errors.New("unexpected call to connBuilder")
			},
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "",
				}}},
			},
			wantStatus: &SourceStatus{
				Reachable: false,
				Errors:    []string{sourceNotProvided},
			},
			wantErr: nil,
		},
		{
			name: "error - connection refused",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return nil, syscall.ECONNREFUSED
			},
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: &SourceStatus{
				Reachable: false,
				Errors:    []string{fmt.Sprintf("%s: %v", sourcePostgresNotReachable, syscall.ECONNREFUSED)},
			},
			wantErr: nil,
		},
		{
			name: "error - ping failure",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return &pgmocks.Querier{
					PingFn: func(ctx context.Context) error {
						return errTest
					},
				}, nil
			},
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: &SourceStatus{
				Reachable: false,
				Errors:    []string{errTest.Error()},
			},
			wantErr: nil,
		},
		{
			name: "error - unexpected connection error",
			connBuilder: func(ctx context.Context, pgURL string) (pglib.Querier, error) {
				return nil, errTest
			},
			config: &Config{
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.connBuilder = tc.connBuilder

			status, err := sc.sourceStatus(context.Background(), tc.config)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantStatus)
		})
	}
}

func TestStatusChecker_initStatus(t *testing.T) {
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

			status, err := sc.initStatus(context.Background(), tc.pgurl, tc.replicationSlotName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantstatus)
		})
	}
}

func TestStatusChecker_transformationRulesStatus(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name                 string
		ruleValidatorBuilder func(context.Context, string) (ruleValidator, error)
		config               *Config
		wantStatus           *TransformationRulesStatus
		wantErr              error
	}{
		{
			name: "ok - valid transformation rules",
			ruleValidatorBuilder: func(ctx context.Context, pgURL string) (ruleValidator, error) {
				return func(rules []transformer.TableRules) (map[string]transformer.ColumnTransformers, error) {
					return nil, nil
				}, nil
			},
			config: &Config{
				Processor: ProcessorConfig{
					Transformer: &transformer.Config{
						TransformerRules: []transformer.TableRules{},
					},
				},
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: &TransformationRulesStatus{
				Valid: true,
			},
			wantErr: nil,
		},
		{
			name: "ok - no transformer configured",
			ruleValidatorBuilder: func(ctx context.Context, pgURL string) (ruleValidator, error) {
				return nil, errors.New("unexpected call to ruleValidatorBuilder")
			},
			config: &Config{
				Processor: ProcessorConfig{
					Transformer: nil,
				},
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: nil,
			wantErr:    nil,
		},
		{
			name: "error - source postgres URL not provided",
			ruleValidatorBuilder: func(ctx context.Context, pgURL string) (ruleValidator, error) {
				return nil, errors.New("unexpected call to ruleValidatorBuilder")
			},
			config: &Config{
				Processor: ProcessorConfig{
					Transformer: &transformer.Config{
						TransformerRules: []transformer.TableRules{},
					},
				},
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "",
				}}},
			},
			wantStatus: &TransformationRulesStatus{
				Valid:  false,
				Errors: []string{fmt.Sprintf("cannot validate transformer rules: %s", sourceNotProvided)},
			},
			wantErr: nil,
		},
		{
			name: "error - rule validation failure",
			ruleValidatorBuilder: func(ctx context.Context, pgURL string) (ruleValidator, error) {
				return func(rules []transformer.TableRules) (map[string]transformer.ColumnTransformers, error) {
					return nil, errTest
				}, nil
			},
			config: &Config{
				Processor: ProcessorConfig{
					Transformer: &transformer.Config{
						TransformerRules: []transformer.TableRules{},
					},
				},
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: &TransformationRulesStatus{
				Valid:  false,
				Errors: []string{errTest.Error()},
			},
			wantErr: nil,
		},
		{
			name: "error - ruleValidatorBuilder failure",
			ruleValidatorBuilder: func(ctx context.Context, pgURL string) (ruleValidator, error) {
				return nil, errTest
			},
			config: &Config{
				Processor: ProcessorConfig{
					Transformer: &transformer.Config{
						TransformerRules: []transformer.TableRules{},
					},
				},
				Listener: ListenerConfig{Postgres: &PostgresListenerConfig{Replication: replicationpg.Config{
					PostgresURL: "postgres://user:password@localhost:5432/db",
				}}},
			},
			wantStatus: nil,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewStatusChecker()
			sc.ruleValidatorBuilder = tc.ruleValidatorBuilder

			status, err := sc.transformationRulesStatus(context.Background(), tc.config)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, status, tc.wantStatus)
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
