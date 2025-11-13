// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	generatormocks "github.com/xataio/pgstream/pkg/snapshot/generator/mocks"
)

func TestSnapshotGenerator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	schemaDump := []byte("schema dump\nCREATE SEQUENCE test.test_sequence\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\nCREATE INDEX a;\n")
	schemaDumpNoSequences := []byte("schema dump\n")
	filteredDump := []byte("schema dump\nCREATE SEQUENCE test.test_sequence\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\n")
	sequenceDump := []byte("sequence dump\n")
	indexDump := []byte("CREATE INDEX a;\n\n")
	rolesDumpOriginal := []byte("roles dump\nCREATE ROLE postgres\nCREATE ROLE test_role\nCREATE ROLE test_role2\nALTER ROLE test_role3 INHERIT FROM test_role;\n")
	rolesDumpFiltered := []byte("roles dump\nCREATE ROLE test_role\nCREATE ROLE test_role2\nGRANT \"test_role\" TO CURRENT_USER;\nGRANT ALL ON SCHEMA \"public\" TO \"test_role\";\n")
	rolesDumpFilteredWithCleanup := []byte("REVOKE ALL ON SCHEMA \"public\" FROM \"test_role\";\nroles dump\nCREATE ROLE test_role\nCREATE ROLE test_role2\nGRANT \"test_role\" TO CURRENT_USER;\nGRANT ALL ON SCHEMA \"public\" TO \"test_role\";\n")
	roleDumpEmpty := []byte("roles dump\n")
	cleanupDump := []byte("cleanup dump\n")
	testSchema := "test_schema"
	testTable := "test_table"
	excludedTable := "excluded_test_table"
	excludedTable2 := "excluded_test_table_2"
	excludedSchema := "excluded_test_schema"
	errTest := errors.New("oh noes")
	testSequence := pglib.QuoteQualifiedIdentifier("test", "test_sequence")
	testRole := "test_role"

	restoreFullDump := rolesDumpFiltered
	restoreFullDump = append(restoreFullDump, filteredDump...)
	restoreFullDump = append(restoreFullDump, sequenceDump...)
	restoreFullDump = append(restoreFullDump, indexDump...)

	validQuerier := func() *mocks.Querier {
		return &mocks.Querier{
			ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
				require.Equal(t, "CREATE SCHEMA IF NOT EXISTS "+testSchema, query)
				return pglib.CommandTag{}, nil
			},
			QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
				switch query {
				case fmt.Sprintf(selectSchemasQuery, "$1"):
					require.Equal(t, []any{testSchema}, args)
					return &mocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 1)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = excludedSchema
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				case fmt.Sprintf(selectSchemaTablesQuery, testSchema, "$1"):
					require.Equal(t, []any{testTable}, args)
					return &mocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = excludedSchema
							tableName, ok := dest[1].(*string)
							require.True(t, ok)
							*tableName = excludedTable
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				case fmt.Sprintf(selectTablesQuery, "$1"):
					require.Equal(t, []any{testTable}, args)
					return &mocks.Rows{
						CloseFn: func() {},
						NextFn:  func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schemaName, ok := dest[0].(*string)
							require.True(t, ok)
							*schemaName = excludedSchema
							tableName, ok := dest[1].(*string)
							require.True(t, ok)
							*tableName = excludedTable
							return nil
						},
						ErrFn: func() error { return nil },
					}, nil
				default:
					return nil, fmt.Errorf("unexpected query: %s", query)
				}
			},
		}
	}

	tests := []struct {
		name              string
		snapshot          *snapshot.Snapshot
		conn              pglib.Querier
		connBuilder       pglib.QuerierBuilder
		pgdumpFn          pglib.PGDumpFn
		pgdumpallFn       pglib.PGDumpAllFn
		pgrestoreFn       pglib.PGRestoreFn
		schemalogStore    schemalog.Store
		generator         generator.SnapshotGenerator
		role              string
		noOwner           bool
		noPrivileges      bool
		rolesSnapshotMode string
		cleanTargetDB     bool

		wantErr error
	}{
		{
			name: "ok",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
						ExcludeTables:    []string{pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable)},
						Role:             testRole,
						NoOwner:          true,
						NoPrivileges:     true,
					}, po)
					return schemaDump, nil

				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
						Role:             testRole,
						NoOwner:          true,
						NoPrivileges:     true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				require.Equal(t, string(restoreFullDump), string(dump))
				return "", nil
			},
			role:         testRole,
			noOwner:      true,
			noPrivileges: true,

			wantErr: nil,
		},
		{
			name: "ok - no sequence dump",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
						ExcludeTables:    []string{pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable)},
					}, po)
					return schemaDumpNoSequences, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				require.Equal(t, string(append(roleDumpEmpty, schemaDumpNoSequences...)), string(dump))
				return "", nil
			},

			wantErr: nil,
		},
		{
			name: "ok - with generator",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
						ExcludeTables:    []string{pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				switch i {
				case 1:
					require.Equal(t, string(append(rolesDumpFiltered, filteredDump...)), string(dump))
				case 2:
					require.Equal(t, string(append(sequenceDump, indexDump...)), string(dump))
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
				return "", nil
			}),
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, &snapshot.Snapshot{
						SchemaTables: map[string][]string{
							testSchema: {testTable},
						},
					}, ss)
					return nil
				},
			},

			wantErr: nil,
		},
		{
			name: "ok - wildcard table",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {wildcard},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				require.Equal(t, string(restoreFullDump), string(dump))
				return "", nil
			},

			wantErr: nil,
		},
		{
			name: "ok - wildcard schema",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					wildcard: {testTable},
				},
				SchemaExcludedTables: map[string][]string{
					excludedSchema: {excludedTable2},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeTables:    []string{pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable), pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable2)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				require.Equal(t, string(restoreFullDump), string(dump))
				return "", nil
			},

			wantErr: nil,
		},
		{
			name: "ok - wildcard schema and table",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					wildcard: {wildcard},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)

				require.Equal(t, string(restoreFullDump), string(dump))
				return "", nil
			},

			wantErr: nil,
		},
		{
			name:              "ok - no roles dump",
			rolesSnapshotMode: "disabled",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {wildcard},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
						NoPasswords:      false,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				require.Equal(t, append(filteredDump, append(sequenceDump, indexDump...)...), dump)
				return "", nil
			},

			wantErr: nil,
		},
		{
			name:              "ok - roles dump with no passwords",
			rolesSnapshotMode: "no_passwords",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {wildcard},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
						NoPasswords:      true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				require.Equal(t, string(restoreFullDump), string(dump))
				return "", nil
			},

			wantErr: nil,
		},
		{
			name:          "ok - with clean option",
			cleanTargetDB: true,
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					wildcard: {wildcard},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						Clean:            true,
					}, po)
					return append(cleanupDump, schemaDump...), nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						Clean:            false,
					}, po)
					return schemaDump, nil
				case 3:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
						Clean:            true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
					Clean:            true,
				}, po)
				expectedDump := cleanupDump
				expectedDump = append(expectedDump, rolesDumpFilteredWithCleanup...)
				expectedDump = append(expectedDump, cleanupDump...) // because we're not removing the duplicate cleanup dump for now
				expectedDump = append(expectedDump, filteredDump...)
				expectedDump = append(expectedDump, sequenceDump...)
				expectedDump = append(expectedDump, indexDump...)
				require.Equal(t, string(expectedDump), string(dump))
				return "", nil
			},

			wantErr: nil,
		},
		{
			name: "ok - no tables in public schema",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					publicSchema: {},
				},
			},
			conn: &mocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					return nil, fmt.Errorf("unexpected query: %s", query)
				},
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errors.New("ExecFn: should not be called")
				},
			},
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return nil, errors.New("pgdumpFn: should not be called")
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errors.New("pgdumpallFn: should not be called")
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: nil,
		},
		{
			name: "error - querying excluded schemas",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: &mocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return nil, errors.New("pgdumpFn: should not be called")
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errors.New("pgdumpallFn: should not be called")
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - scanning excluded tables",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: &mocks.Querier{
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case fmt.Sprintf(selectSchemasQuery, "$1"):
						require.Equal(t, []any{testSchema}, args)
						return &mocks.Rows{
							CloseFn: func() {},
							NextFn:  func(i uint) bool { return i == 1 },
							ScanFn: func(i uint, dest ...any) error {
								require.Len(t, dest, 1)
								schemaName, ok := dest[0].(*string)
								require.True(t, ok)
								*schemaName = excludedSchema
								return nil
							},
							ErrFn: func() error { return nil },
						}, nil
					case fmt.Sprintf(selectSchemaTablesQuery, testSchema, "$1"):
						require.Equal(t, []any{testTable}, args)
						return &mocks.Rows{
							CloseFn: func() {},
							NextFn:  func(i uint) bool { return i == 1 },
							ScanFn: func(i uint, dest ...any) error {
								return errTest
							},
							ErrFn: func() error { return nil },
						}, nil
					default:
						return nil, fmt.Errorf("unexpected query: %s", query)
					}
				},
			},
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return nil, errors.New("pgdumpFn: should not be called")
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errors.New("pgdumpallFn: should not be called")
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - excluded tables rows",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: &mocks.Querier{
				ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					require.Equal(t, "CREATE SCHEMA IF NOT EXISTS "+testSchema, query)
					return pglib.CommandTag{}, nil
				},
				QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case fmt.Sprintf(selectSchemasQuery, "$1"):
						require.Equal(t, []any{testSchema}, args)
						return &mocks.Rows{
							CloseFn: func() {},
							NextFn:  func(i uint) bool { return i == 1 },
							ScanFn: func(i uint, dest ...any) error {
								require.Len(t, dest, 1)
								schemaName, ok := dest[0].(*string)
								require.True(t, ok)
								*schemaName = excludedSchema
								return nil
							},
							ErrFn: func() error { return nil },
						}, nil
					case fmt.Sprintf(selectSchemaTablesQuery, testSchema, "$1"):
						require.Equal(t, []any{testTable}, args)
						return &mocks.Rows{
							CloseFn: func() {},
							NextFn:  func(i uint) bool { return i == 1 },
							ScanFn: func(i uint, dest ...any) error {
								require.Len(t, dest, 2)
								schemaName, ok := dest[0].(*string)
								require.True(t, ok)
								*schemaName = excludedSchema
								tableName, ok := dest[1].(*string)
								require.True(t, ok)
								*tableName = excludedTable
								return nil
							},
							ErrFn: func() error { return errTest },
						}, nil
					default:
						return nil, fmt.Errorf("unexpected query: %s", query)
					}
				},
			},
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return nil, errors.New("pgdumpFn: should not be called")
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errors.New("pgdumpallFn: should not be called")
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - dumping schema",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					return nil, errTest
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errors.New("pgdumpallFn: should not be called")
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - dumping sequence values",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					return schemaDump, nil
				case 2:
					return nil, errTest
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errors.New("pgdumpallFn: should not be called")
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - dumping roles",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return schemaDump, nil
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return nil, errTest
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - restoring schema dump",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return schemaDump, nil
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return rolesDumpOriginal, nil
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errTest
			},

			wantErr: errTest,
		},
		{
			name: "error - restoring filtered dump",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
						ExcludeTables:    []string{pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				switch i {
				case 1:
					return "", errTest
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
			}),
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, &snapshot.Snapshot{
						SchemaTables: map[string][]string{
							testSchema: {testTable},
						},
					}, ss)
					return errors.New("CreateSnapshotFn: should not be called")
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - calling internal generator CreateSnapshot",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						ExcludeSchemas:   []string{pglib.QuoteIdentifier(excludedSchema)},
						ExcludeTables:    []string{pglib.QuoteQualifiedIdentifier(excludedSchema, excludedTable)},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						DataOnly:         true,
						Tables:           []string{testSequence},
					}, po)
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					require.Equal(t, pglib.PGDumpAllOptions{
						ConnectionString: "source-url",
						RolesOnly:        true,
					}, po)
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				switch i {
				case 1, 2:
					return "", nil
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
			}),
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errTest
				},
			},

			wantErr: errTest,
		},
		{
			name: "error - performing pgrestore output critical error",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return schemaDump, nil
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return rolesDumpOriginal, nil
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", pglib.NewPGRestoreErrors(errTest)
			},

			wantErr: pglib.NewPGRestoreErrors(errTest),
		},
		{
			name: "error - performing pgrestore output ignored errors",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return schemaDump, nil
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return rolesDumpOriginal, nil
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", pglib.NewPGRestoreErrors(&pglib.ErrRelationAlreadyExists{})
			},

			wantErr: nil,
		},
		{
			name: "error - getting target conn",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				if s == "target-url" {
					return nil, errTest
				}
				return validQuerier(), nil
			},
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return schemaDump, nil
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return rolesDumpOriginal, nil
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - creating schema",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: func() *mocks.Querier {
				c := validQuerier()
				c.ExecFn = func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, errTest
				}
				return c
			}(),
			pgdumpFn: func(_ context.Context, po pglib.PGDumpOptions) ([]byte, error) {
				return schemaDump, nil
			},
			pgdumpallFn: func(_ context.Context, po pglib.PGDumpAllOptions) ([]byte, error) {
				return rolesDumpOriginal, nil
			},
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				return "", errors.New("pgrestoreFn: should not be called")
			},

			wantErr: errTest,
		},
		{
			name: "error - schemalog insertion fails",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					return schemaDump, nil
				case 2:
					return sequenceDump, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpFn: %d", i)
				}
			}),
			pgdumpallFn: newMockPgdumpall(func(_ context.Context, i uint, po pglib.PGDumpAllOptions) ([]byte, error) {
				switch i {
				case 1:
					return rolesDumpOriginal, nil
				default:
					return nil, fmt.Errorf("unexpected call to pgdumpallFn: %d", i)
				}
			}),
			pgrestoreFn: func(_ context.Context, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, string(restoreFullDump), string(dump))
				return "", nil
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},

			wantErr: fmt.Errorf("inserting schemalog entry for schema %q after schema snapshot: %w", testSchema, errTest),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sg := SnapshotGenerator{
				sourceURL:      "source-url",
				targetURL:      "target-url",
				connBuilder:    func(ctx context.Context, s string) (pglib.Querier, error) { return tc.conn, nil },
				pgDumpFn:       tc.pgdumpFn,
				pgDumpAllFn:    tc.pgdumpallFn,
				pgRestoreFn:    tc.pgrestoreFn,
				schemalogStore: tc.schemalogStore,
				logger:         log.NewNoopLogger(),
				generator:      tc.generator,
				roleSQLParser:  &roleSQLParser{},
				optionGenerator: &optionGenerator{
					sourceURL:              "source-url",
					targetURL:              "target-url",
					includeGlobalDBObjects: true,
					role:                   tc.role,
					noOwner:                tc.noOwner,
					noPrivileges:           tc.noPrivileges,
					rolesSnapshotMode:      tc.rolesSnapshotMode,
					cleanTargetDB:          tc.cleanTargetDB,
					connBuilder:            func(ctx context.Context, s string) (pglib.Querier, error) { return tc.conn, nil },
				},
			}

			if tc.connBuilder != nil {
				sg.connBuilder = tc.connBuilder
				sg.optionGenerator.connBuilder = tc.connBuilder
			}

			err := sg.CreateSnapshot(context.Background(), tc.snapshot)
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
			sg.Close()
		})
	}
}

func TestSnapshotGenerator_schemalogExists(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		connBuilder pglib.QuerierBuilder

		wantExists bool
		wantErr    error
	}{
		{
			name: "ok - schemalog exists",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Equal(t, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)", query)
						require.Equal(t, []any{schemalog.SchemaName, schemalog.TableName}, args)
						require.Len(t, dest, 1)
						exists, ok := dest[0].(*bool)
						require.True(t, ok, fmt.Sprintf("exists, expected *bool, got %T", dest[0]))
						*exists = true
						return nil
					},
				}, nil
			},

			wantExists: true,
			wantErr:    nil,
		},
		{
			name: "ok - schemalog does not exist",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Equal(t, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)", query)
						require.Equal(t, []any{schemalog.SchemaName, schemalog.TableName}, args)
						require.Len(t, dest, 1)
						exists, ok := dest[0].(*bool)
						require.True(t, ok, fmt.Sprintf("exists, expected *bool, got %T", dest[0]))
						*exists = false
						return nil
					},
				}, nil
			},

			wantExists: false,
			wantErr:    nil,
		},
		{
			name: "error - getting source connection",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return nil, errTest
			},

			wantExists: false,
			wantErr:    errTest,
		},
		{
			name: "error - scanning",
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryRowFn: func(ctx context.Context, dest []any, query string, args ...any) error {
						require.Equal(t, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)", query)
						require.Equal(t, []any{schemalog.SchemaName, schemalog.TableName}, args)
						return errTest
					},
				}, nil
			},

			wantExists: false,
			wantErr:    errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sg := SnapshotGenerator{
				connBuilder: tc.connBuilder,
			}

			exists, err := sg.schemalogExists(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantExists, exists)
		})
	}
}

func TestSnapshotGenerator_parseDump(t *testing.T) {
	t.Parallel()

	// Read the file as []byte
	dumpBytes, err := os.ReadFile("test/test_dump.sql")
	require.NoError(t, err)

	wantFilteredDumpBytes, err := os.ReadFile("test/test_dump_filtered.sql")
	require.NoError(t, err)

	wantConstraintsBytes, err := os.ReadFile("test/test_dump_constraints.sql")
	require.NoError(t, err)

	sg := &SnapshotGenerator{
		excludedSecurityLabels: []string{"anon"},
	}
	dump := sg.parseDump(dumpBytes)

	filteredStr := strings.Trim(string(dump.filtered), "\n")
	wantFilteredStr := strings.Trim(string(wantFilteredDumpBytes), "\n")
	constraintsStr := strings.Trim(string(dump.indicesAndConstraints), "\n")
	wantConstraintsStr := strings.Trim(string(wantConstraintsBytes), "\n")
	wantSequences := []string{`"musicbrainz"."alternative_medium_id_seq"`, `"musicbrainz"."Alternative_medium_id_seq"`}

	require.Equal(t, wantFilteredStr, filteredStr)
	require.Equal(t, wantConstraintsStr, constraintsStr)
	require.Equal(t, wantSequences, dump.sequences)
}

func TestGetDumpsDiff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		d1   []byte
		d2   []byte
		want []byte
	}{
		{
			name: "empty dumps",
			d1:   []byte{},
			d2:   []byte{},
			want: []byte{},
		},
		{
			name: "identical dumps",
			d1:   []byte("line1\nline2\nline3"),
			d2:   []byte("line1\nline2\nline3"),
			want: []byte{},
		},
		{
			name: "first dump has extra lines",
			d1:   []byte("line1\nline2\nline3\nline4"),
			d2:   []byte("line1\nline2\nline3"),
			want: []byte("line4\n"),
		},
		{
			name: "second dump has extra lines",
			d1:   []byte("line1\nline2\nline3"),
			d2:   []byte("line1\nline2\nline3\nline4"),
			want: []byte{},
		},
		{
			name: "completely different dumps",
			d1:   []byte("lineA\nlineB\nlineC"),
			d2:   []byte("line1\nline2\nline3"),
			want: []byte("lineA\nlineB\nlineC\n"),
		},
		{
			name: "partially different dumps",
			d1:   []byte("line1\nline2\nlineC\nlineD"),
			d2:   []byte("line1\nline2\nline3"),
			want: []byte("lineC\nlineD\n"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getDumpsDiff(tc.d1, tc.d2)
			require.Equal(t, string(tc.want), string(got))
		})
	}
}

func TestSnapshotGenerator_syncSchemaLog(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testSchema2 := "test_schema_2"
	excludedSchema := "excluded_schema"
	errTest := errors.New("oh noes")

	tests := []struct {
		name                string
		schemalogStore      schemalog.Store
		connBuilder         pglib.QuerierBuilder
		schemaTables        map[string][]string
		excludeSchemaTables map[string][]string
		wantErr             error
	}{
		{
			name:           "ok - no schemalog store",
			schemalogStore: nil,
			schemaTables: map[string][]string{
				testSchema: {"table1"},
			},
			excludeSchemaTables: map[string][]string{},
			wantErr:             nil,
		},
		{
			name: "ok - single schema",
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					require.Equal(t, testSchema, schemaName)
					return &schemalog.LogEntry{SchemaName: schemaName}, nil
				},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{}, nil
			},
			schemaTables: map[string][]string{
				testSchema: {"table1"},
			},
			excludeSchemaTables: map[string][]string{},
			wantErr:             nil,
		},
		{
			name: "ok - wildcard schema",
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					if schemaName != testSchema && schemaName != testSchema2 {
						return nil, fmt.Errorf("unexpected schema name: %s", schemaName)
					}
					return &schemalog.LogEntry{SchemaName: schemaName}, nil
				},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
						require.Contains(t, query, pglib.DiscoverAllSchemasQuery)
						return &mocks.Rows{
							CloseFn: func() {},
							NextFn: func(i uint) bool {
								return i < 2
							},
							ScanFn: func(i uint, dest ...any) error {
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								if *schema == "" {
									*schema = testSchema
								} else {
									*schema = testSchema2
								}
								return nil
							},
							ErrFn: func() error { return nil },
						}, nil
					},
				}, nil
			},
			schemaTables: map[string][]string{
				wildcard: {"table1"},
			},
			excludeSchemaTables: map[string][]string{},
			wantErr:             nil,
		},
		{
			name: "ok - excluded schema skipped",
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					require.NotEqual(t, excludedSchema, schemaName)
					return &schemalog.LogEntry{SchemaName: schemaName}, nil
				},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{}, nil
			},
			schemaTables: map[string][]string{
				testSchema:     {"table1"},
				excludedSchema: {"table2"},
			},
			excludeSchemaTables: map[string][]string{
				excludedSchema: {"table2"},
			},
			wantErr: nil,
		},
		{
			name: "error - schema log insert fails",
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{}, nil
			},
			schemaTables: map[string][]string{
				testSchema: {"table1"},
			},
			excludeSchemaTables: map[string][]string{},
			wantErr:             errTest,
		},
		{
			name:           "error - getting source connection",
			schemalogStore: &schemalogmocks.Store{},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return nil, errTest
			},
			schemaTables: map[string][]string{
				testSchema: {"table1"},
			},
			excludeSchemaTables: map[string][]string{},
			wantErr:             errTest,
		},
		{
			name: "error - discovering schemas",
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					if schemaName != testSchema && schemaName != testSchema2 {
						return nil, fmt.Errorf("unexpected schema name: %s", schemaName)
					}
					return &schemalog.LogEntry{SchemaName: schemaName}, nil
				},
			},
			connBuilder: func(ctx context.Context, s string) (pglib.Querier, error) {
				return &mocks.Querier{
					QueryFn: func(ctx context.Context, query string, args ...any) (pglib.Rows, error) {
						require.Contains(t, query, pglib.DiscoverAllSchemasQuery)
						return &mocks.Rows{
							CloseFn: func() {},
							NextFn: func(i uint) bool {
								return i < 2
							},
							ScanFn: func(i uint, dest ...any) error {
								return errTest
							},
							ErrFn: func() error { return nil },
						}, nil
					},
				}, nil
			},
			schemaTables: map[string][]string{
				wildcard: {"table1"},
			},
			excludeSchemaTables: map[string][]string{},
			wantErr:             errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sg := &SnapshotGenerator{
				schemalogStore: tc.schemalogStore,
				connBuilder:    tc.connBuilder,
				logger:         log.NewNoopLogger(),
			}
			err := sg.syncSchemaLog(context.Background(), tc.schemaTables, tc.excludeSchemaTables)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
