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
	"github.com/xataio/pgstream/pkg/snapshot"
	"github.com/xataio/pgstream/pkg/snapshot/generator"
	generatormocks "github.com/xataio/pgstream/pkg/snapshot/generator/mocks"
)

func TestSnapshotGenerator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	schemaDump := []byte("schema dump\nCREATE SEQUENCE test.test_sequence\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\nCREATE INDEX a;\n")
	schemaDumpNoSequences := []byte("schema dump\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\nCREATE INDEX a;\n")
	filteredDumpNoSequences := []byte("schema dump\nGRANT ALL ON SCHEMA \"public\" TO \"test_role\";\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\n")
	filteredDump := []byte("schema dump\nCREATE SEQUENCE test.test_sequence\nGRANT ALL ON SCHEMA \"public\" TO \"test_role\";\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\n")
	sequenceDump := []byte("sequence dump\n")
	indexDump := []byte("CREATE INDEX a;\n\n")
	rolesDumpOriginal := []byte("roles dump\nCREATE ROLE postgres\nCREATE ROLE test_role\nCREATE ROLE test_role2\nALTER ROLE test_role3 INHERIT FROM test_role;\n")
	rolesDumpFiltered := []byte("roles dump\nCREATE ROLE test_role\nCREATE ROLE test_role2\nGRANT \"test_role\" TO CURRENT_USER;\n")
	cleanupDump := []byte("cleanup dump\n")
	testSchema := "test_schema"
	quotedTestSchema := `"test_schema"`
	testTable := "test_table"
	excludedTable := "excluded_test_table"
	excludedTable2 := "excluded_test_table_2"
	excludedSchema := "excluded_test_schema"
	errTest := errors.New("oh noes")
	testSequence := pglib.QuoteQualifiedIdentifier("test", "test_sequence")
	testRole := "test_role"
	schemaCreateDump := fmt.Appendf(nil, "CREATE SCHEMA IF NOT EXISTS %s;\n", quotedTestSchema)

	fullDumpRestoreFn := func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
		require.Equal(t, pglib.PGRestoreOptions{
			ConnectionString: "target-url",
			Format:           "p",
		}, po)
		switch i {
		case 1:
			require.Equal(t, string(schemaCreateDump), string(dump))
		case 2:
			require.Equal(t, string(rolesDumpFiltered), string(dump))
		case 3:
			require.Equal(t, string(filteredDump), string(dump))
		case 4:
			require.Equal(t, string(sequenceDump), string(dump))
		case 5:
			require.Equal(t, string(indexDump), string(dump))
		default:
			return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
		}
		return "", nil
	}

	fullDumpNoSchemaCreateRestoreFn := func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
		require.Equal(t, pglib.PGRestoreOptions{
			ConnectionString: "target-url",
			Format:           "p",
		}, po)
		switch i {
		case 1:
			require.Equal(t, string(rolesDumpFiltered), string(dump))
		case 2:
			require.Equal(t, string(filteredDump), string(dump))
		case 3:
			require.Equal(t, string(sequenceDump), string(dump))
		case 4:
			require.Equal(t, string(indexDump), string(dump))
		default:
			return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
		}
		return "", nil
	}

	validQuerier := func() *mocks.Querier {
		return &mocks.Querier{
			ExecFn: func(ctx context.Context, i uint, query string, args ...any) (pglib.CommandTag, error) {
				require.Equal(t, "CREATE SCHEMA IF NOT EXISTS "+quotedTestSchema, query)
				return pglib.CommandTag{}, nil
			},
			QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
				switch query {
				case selectSchemasQuery:
					require.Equal(t, []any{[]string{testSchema}}, args)
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
				case selectSchemaTablesQuery:
					require.Equal(t, []any{testSchema, []string{testTable}}, args)
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
				case selectTablesQuery:
					require.Equal(t, []any{[]string{testTable}}, args)
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
		pgdumpFn          pglib.PGDumpFn
		pgdumpallFn       pglib.PGDumpAllFn
		pgrestoreFn       pglib.PGRestoreFn
		generator         generator.SnapshotGenerator
		role              string
		noOwner           bool
		noPrivileges      bool
		rolesSnapshotMode string
		cleanTargetDB     bool
		snapshotTracker   snapshotProgressTracker

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
			pgrestoreFn:  newMockPgrestore(fullDumpRestoreFn),
			role:         testRole,
			noOwner:      true,
			noPrivileges: true,

			wantErr: nil,
		},
		{
			name: "ok - with tracking",
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
			pgrestoreFn:  newMockPgrestore(fullDumpRestoreFn),
			role:         testRole,
			noOwner:      true,
			noPrivileges: true,
			snapshotTracker: &mockSnapshotTracker{
				trackIndexesCreationFn: func(ctx context.Context) {},
			},

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
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				switch i {
				case 1:
					require.Equal(t, string(schemaCreateDump), string(dump))
				case 2:
					require.Equal(t, string(rolesDumpFiltered), string(dump))
				case 3:
					require.Equal(t, string(filteredDumpNoSequences), string(dump))
				case 4:
					require.Equal(t, string(indexDump), string(dump))
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
				return "", nil
			}),

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
			pgrestoreFn: newMockPgrestore(fullDumpRestoreFn),

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
			pgrestoreFn: newMockPgrestore(fullDumpRestoreFn),

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
			pgrestoreFn: newMockPgrestore(fullDumpNoSchemaCreateRestoreFn),

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
			pgrestoreFn: newMockPgrestore(fullDumpNoSchemaCreateRestoreFn),

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
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
				}, po)
				switch i {
				case 1:
					require.Equal(t, string(schemaCreateDump), string(dump))
				case 2:
					require.Equal(t, string(filteredDump), string(dump))
				case 3:
					require.Equal(t, string(sequenceDump), string(dump))
				case 4:
					require.Equal(t, string(indexDump), string(dump))
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
				return "", nil
			}),

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
			pgrestoreFn: newMockPgrestore(fullDumpRestoreFn),

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
						Clean:            false,
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						Clean:            true,
					}, po)
					return append(cleanupDump, schemaDump...), nil
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
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
				require.Equal(t, pglib.PGRestoreOptions{
					ConnectionString: "target-url",
					Format:           "p",
					Clean:            true,
				}, po)
				switch i {
				case 1:
					require.Equal(t, string(cleanupDump), string(dump))
				case 2:
					require.Equal(t, string(rolesDumpFiltered), string(dump))
				case 3:
					require.Equal(t, string(filteredDump), string(dump))
				case 4:
					require.Equal(t, string(sequenceDump), string(dump))
				case 5:
					require.Equal(t, string(indexDump), string(dump))
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
				return "", nil
			}),

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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
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
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case selectSchemasQuery:
						require.Equal(t, []any{[]string{testSchema}}, args)
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
					case selectSchemaTablesQuery:
						require.Equal(t, []any{"test_schema", []string{testTable}}, args)
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
					require.Equal(t, "CREATE SCHEMA IF NOT EXISTS "+quotedTestSchema, query)
					return pglib.CommandTag{}, nil
				},
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case selectSchemasQuery:
						require.Equal(t, []any{[]string{testSchema}}, args)
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
					case selectSchemaTablesQuery:
						require.Equal(t, []any{"test_schema", []string{testTable}}, args)
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
				case 1, 2, 3:
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
			name: "error - creating schema",
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
				require.Equal(t, string(schemaCreateDump), string(dump))
				return "", errTest
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sg := SnapshotGenerator{
				sourceURL:     "source-url",
				targetURL:     "target-url",
				sourceQuerier: tc.conn,
				pgDumpFn:      tc.pgdumpFn,
				pgDumpAllFn:   tc.pgdumpallFn,
				pgRestoreFn:   tc.pgrestoreFn,
				logger:        log.NewNoopLogger(),
				generator:     tc.generator,
				roleSQLParser: &roleSQLParser{},
				optionGenerator: &optionGenerator{
					sourceURL:              "source-url",
					targetURL:              "target-url",
					includeGlobalDBObjects: true,
					role:                   tc.role,
					noOwner:                tc.noOwner,
					noPrivileges:           tc.noPrivileges,
					rolesSnapshotMode:      tc.rolesSnapshotMode,
					cleanTargetDB:          tc.cleanTargetDB,
					querier:                tc.conn,
				},
			}

			if tc.snapshotTracker != nil {
				sg.snapshotTracker = tc.snapshotTracker
			}

			err := sg.CreateSnapshot(context.Background(), tc.snapshot)
			if !errors.Is(err, tc.wantErr) {
				require.Equal(t, tc.wantErr, err)
			}
			sg.Close()
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

	wantEventTriggersBytes, err := os.ReadFile("test/test_dump_event_triggers.sql")
	require.NoError(t, err)

	sg := &SnapshotGenerator{
		excludedSecurityLabels: []string{"anon"},
	}
	dump := sg.parseDump(dumpBytes)

	filteredStr := strings.Trim(string(dump.filtered), "\n")
	wantFilteredStr := strings.Trim(string(wantFilteredDumpBytes), "\n")
	constraintsStr := strings.Trim(string(dump.indicesAndConstraints), "\n")
	wantConstraintsStr := strings.Trim(string(wantConstraintsBytes), "\n")
	eventTriggersStr := strings.Trim(string(dump.eventTriggers), "\n")
	wantEventTriggersStr := strings.Trim(string(wantEventTriggersBytes), "\n")
	wantSequences := []string{`"musicbrainz"."alternative_medium_id_seq"`, `"musicbrainz"."Alternative_medium_id_seq"`}

	require.Equal(t, wantFilteredStr, filteredStr)
	require.Equal(t, wantConstraintsStr, constraintsStr)
	require.Equal(t, wantSequences, dump.sequences)
	require.Equal(t, wantEventTriggersStr, eventTriggersStr)
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

func TestSnapshotGenerator_removeRestrictedRoleAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		line  string
		isAWS bool

		wantLine string
	}{
		{
			name:     "no replication attribute - no change",
			line:     "CREATE ROLE test_role SUPERUSER CREATEDB;",
			wantLine: "CREATE ROLE test_role SUPERUSER CREATEDB;",
		},
		{
			name:     "replication attribute on non-AWS target - no change",
			line:     "CREATE ROLE test_role REPLICATION SUPERUSER;",
			wantLine: "CREATE ROLE test_role REPLICATION SUPERUSER;",
		},
		{
			name:     "replication attribute on AWS target - removed and grant added",
			line:     "CREATE ROLE test_role REPLICATION SUPERUSER;",
			isAWS:    true,
			wantLine: "CREATE ROLE test_role SUPERUSER;\nGRANT rds_replication TO test_role;",
		},
		{
			name:     "replication in role name - not affected",
			line:     "CREATE ROLE replication_user SUPERUSER;",
			wantLine: "CREATE ROLE replication_user SUPERUSER;",
		},
		{
			name:     "quoted role name with replication attribute on AWS",
			line:     "CREATE ROLE \"test_role\" REPLICATION LOGIN;",
			isAWS:    true,
			wantLine: "CREATE ROLE \"test_role\" LOGIN;\nGRANT rds_replication TO \"test_role\";",
		},
		{
			name:     "alter role with replication on AWS",
			line:     "ALTER ROLE existing_role REPLICATION CREATEDB;",
			isAWS:    true,
			wantLine: "ALTER ROLE existing_role CREATEDB;\nGRANT rds_replication TO existing_role;",
		},
		{
			name:     "no role name found - no grant added",
			line:     "COMMENT ON ROLE REPLICATION;",
			wantLine: "COMMENT ON ROLE;",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sg := SnapshotGenerator{
				roleSQLParser: &roleSQLParser{},
				targetURL: func(isAWS bool) string {
					if isAWS {
						return "test.url.rds.amazonaws.com"
					}
					return "test.url"
				}(tc.isAWS),
			}

			sg.removeRestrictedRoleAttributes(tc.line)
		})
	}
}

func TestSnapshotGenerator_filterTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		input           string
		excludedSchemas []string
		want            string
	}{
		{
			name:            "filters triggers from excluded schema",
			input:           "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1();\nCREATE EVENT TRIGGER trigger2 ON sql_drop EXECUTE FUNCTION excluded_schema.function2();",
			excludedSchemas: []string{"\"excluded_schema\""},
			want:            "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1();\n",
		},
		{
			name:            "keeps all triggers when no schema excluded",
			input:           "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1();\nCREATE EVENT TRIGGER trigger2 ON sql_drop EXECUTE FUNCTION excluded_schema.function2();",
			excludedSchemas: []string{},
			want:            "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1();\nCREATE EVENT TRIGGER trigger2 ON sql_drop EXECUTE FUNCTION excluded_schema.function2();",
		},
		{
			name:            "malformed trigger line - skipped",
			input:           "INVALID TRIGGER LINE\nCREATE EVENT TRIGGER trigger2 ON sql_drop EXECUTE FUNCTION schema2.function2();",
			excludedSchemas: []string{"\"schema2\""},
			want:            "",
		},
		{
			name:            "exclude trigger with function parameters",
			input:           "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1('param1', 'param2');\n",
			excludedSchemas: []string{"\"schema1\""},
			want:            "",
		},
		{
			name:            "excludes multiple triggers from different excluded schemas",
			input:           "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1();\nCREATE EVENT TRIGGER trigger2 ON sql_drop EXECUTE FUNCTION excluded1.function2();\nCREATE EVENT TRIGGER trigger3 ON ddl_command_start EXECUTE FUNCTION excluded2.function3();\nCREATE EVENT TRIGGER trigger4 ON sql_drop EXECUTE FUNCTION schema2.function4();",
			excludedSchemas: []string{"\"excluded1\"", "\"excluded2\""},
			want:            "CREATE EVENT TRIGGER trigger1 ON ddl_command_end EXECUTE FUNCTION schema1.function1();\nCREATE EVENT TRIGGER trigger4 ON sql_drop EXECUTE FUNCTION schema2.function4();\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &SnapshotGenerator{}

			got := s.filterTriggers([]byte(tc.input), tc.excludedSchemas)
			require.Equal(t, string(tc.want), string(got))
		})
	}
}

func TestExtractDollarQuoteTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		want string
	}{
		{name: "underscore tag", line: "    AS $_$", want: "$_$"},
		{name: "empty tag", line: "    AS $$", want: "$$"},
		{name: "body tag", line: "    AS $BODY$", want: "$BODY$"},
		{name: "closing tag", line: "$_$;", want: "$_$"},
		{name: "closing empty tag", line: "$$;", want: "$$"},
		{name: "param placeholder $1", line: "WHERE attrelid = $1 AND attname = $2", want: ""},
		{name: "numeric literal", line: "    amount NUMERIC(10, 2),", want: ""},
		{name: "no dollar sign", line: "CREATE TRIGGER %s", want: ""},
		{name: "cast with regclass", line: "SELECT $1::regclass", want: ""},
		{name: "dollar in string literal", line: "E'costs $5'", want: ""},
		{name: "plain text", line: "ALTER TABLE public.users ADD COLUMN name TEXT;", want: ""},
		// PostgreSQL spec: tag must start with letter or underscore, not digit
		{name: "digit-first tag $5$", line: "'costs $5$ each'", want: ""},
		{name: "digit-first tag $1$", line: "COMMENT ON INDEX idx IS 'val $1$ end'", want: ""},
		{name: "digit tag $123$", line: "$123$", want: ""},
		// Valid tags that start with letter/underscore followed by digits
		{name: "letter then digit $x1$", line: "AS $x1$", want: "$x1$"},
		{name: "underscore then digit $_1$", line: "AS $_1$", want: "$_1$"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractDollarQuoteTag(tc.line)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseDump_DollarQuotedBlocks(t *testing.T) {
	t.Parallel()

	// A pg_dump output with a function containing "CREATE TRIGGER %s" inside
	// a dollar-quoted body. Without the fix, parseDump rips this line into
	// indicesAndConstraints. With the fix, it stays in filteredDump.
	dumpInput := strings.Join([]string{
		"CREATE FUNCTION public.clone_triggers() RETURNS text",
		"    LANGUAGE plpgsql",
		"    AS $_$",
		"BEGIN",
		"    script := format('",
		"CREATE TRIGGER %s",
		"BEFORE INSERT ON %I",
		"FOR EACH ROW EXECUTE FUNCTION %s();",
		"', trig_name, tbl_name, func_name);",
		"    RETURN script;",
		"END;",
		"$_$;",
		"",
		"CREATE TRIGGER real_trigger BEFORE UPDATE ON public.test_table FOR EACH ROW EXECUTE FUNCTION public.update_ts();",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// The "CREATE TRIGGER %s" line must stay in the filtered dump (function body)
	require.Contains(t, filtered, "CREATE TRIGGER %s", "dollar-quoted CREATE TRIGGER should stay in filtered dump")

	// The real top-level trigger should be in indicesAndConstraints
	require.Contains(t, indices, "CREATE TRIGGER real_trigger", "top-level trigger should be in indices section")

	// The real trigger should NOT be in the filtered dump
	require.NotContains(t, filtered, "CREATE TRIGGER real_trigger", "top-level trigger should not be in filtered dump")

	// The dollar-quoted function body line should NOT be in indices
	require.NotContains(t, indices, "CREATE TRIGGER %s", "dollar-quoted line should not be in indices section")
}

func TestParseDump_OddDollarQuoteCount(t *testing.T) {
	t.Parallel()

	// A line with 3 occurrences of $$ means: open, close, re-open.
	// The parser must recognize we're still inside a dollar-quoted block
	// after that line, so "CREATE TRIGGER inside" on the next line stays
	// in filteredDump.
	dumpInput := strings.Join([]string{
		"CREATE FUNCTION public.f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE $$ || $$ ",
		"CREATE TRIGGER inside_odd BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop();",
		"$$;",
		"",
		"CREATE TRIGGER real_outside BEFORE UPDATE ON public.t FOR EACH ROW EXECUTE FUNCTION public.noop();",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// The trigger inside the odd-count dollar-quoted block must stay in filteredDump
	require.Contains(t, filtered, "CREATE TRIGGER inside_odd", "trigger inside odd-count dollar block should stay in filtered dump")
	require.NotContains(t, indices, "CREATE TRIGGER inside_odd", "trigger inside odd-count dollar block should not be in indices")

	// The real top-level trigger should be in indicesAndConstraints
	require.Contains(t, indices, "CREATE TRIGGER real_outside", "top-level trigger should be in indices section")
}

func TestExtractDollarQuoteTag_IgnoresSingleQuotedStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		want string
	}{
		{name: "dollar tag inside single quotes", line: "COMMENT ON INDEX my_idx IS 'use $$ for quoting';", want: ""},
		{name: "named tag inside single quotes", line: "COMMENT ON INDEX my_idx IS 'use $_$ here';", want: ""},
		{name: "body tag inside single quotes", line: "COMMENT ON TRIGGER t IS '$BODY$ is a tag';", want: ""},
		{name: "escaped quote before dollar", line: "COMMENT ON INDEX i IS 'it''s $$ fine';", want: ""},
		{name: "dollar after closing quote", line: "SELECT 'text' || $$", want: "$$"},
		{name: "real tag not in quotes", line: "    AS $$", want: "$$"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractDollarQuoteTag(tc.line)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseDump_DollarInsideSingleQuotedComment(t *testing.T) {
	t.Parallel()

	// A COMMENT ON INDEX with $$ inside single quotes must NOT trigger
	// dollar-quote state. The comment should go to indicesAndConstraints,
	// and the CREATE INDEX on the next line should also go there.
	dumpInput := strings.Join([]string{
		"COMMENT ON INDEX public.my_idx IS 'use $$ for quoting';",
		"CREATE INDEX idx_name ON public.test_table USING btree (col1);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// COMMENT ON INDEX should be in indices, not filtered
	require.Contains(t, indices, "COMMENT ON INDEX", "COMMENT ON INDEX should be in indices section")
	require.NotContains(t, filtered, "COMMENT ON INDEX", "COMMENT ON INDEX should not be in filtered dump")

	// CREATE INDEX should be in indices, not filtered
	require.Contains(t, indices, "CREATE INDEX idx_name", "CREATE INDEX should be in indices section")
	require.NotContains(t, filtered, "CREATE INDEX idx_name", "CREATE INDEX should not be in filtered dump")
}

func TestParseDump_BalancedDollarQuoteOnIndexLine(t *testing.T) {
	t.Parallel()

	// A COMMENT ON INDEX with a balanced dollar-quoted literal (opens and
	// closes on the same line) must still be routed to indicesAndConstraints,
	// not short-circuited to filteredDump.
	dumpInput := strings.Join([]string{
		"COMMENT ON INDEX public.my_idx IS $$some comment$$;",
		"CREATE INDEX idx_other ON public.test_table USING btree (col2);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	require.Contains(t, indices, "COMMENT ON INDEX", "balanced dollar-quoted COMMENT ON INDEX should be in indices")
	require.NotContains(t, filtered, "COMMENT ON INDEX", "balanced dollar-quoted COMMENT ON INDEX should not be in filtered")

	require.Contains(t, indices, "CREATE INDEX idx_other", "CREATE INDEX after balanced line should be in indices")
	require.NotContains(t, filtered, "CREATE INDEX idx_other", "CREATE INDEX after balanced line should not be in filtered")
}

func TestExtractDollarQuoteTag_IgnoresDoubleQuotedIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		want string
	}{
		{name: "dollar tag inside double quotes", line: `CREATE INDEX "my$$idx" ON public.t (col);`, want: ""},
		{name: "named tag inside double quotes", line: `ALTER TABLE "schema$_$name".t ADD COLUMN x INT;`, want: ""},
		{name: "dollar after closing double quote", line: `CREATE INDEX "name" ON t (col) WHERE x = $$`, want: "$$"},
		{name: "real tag not in quotes", line: "    AS $_$", want: "$_$"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractDollarQuoteTag(tc.line)
			require.Equal(t, tc.want, got)
		})
	}
}
