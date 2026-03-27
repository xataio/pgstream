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
	schemaDumpWithViews := []byte("schema dump\nCREATE SEQUENCE test.test_sequence\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\nCREATE VIEW public.test_view AS\n SELECT 1;\nCREATE INDEX a;\n")
	schemaDumpNoSequences := []byte("schema dump\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\nCREATE INDEX a;\n")
	filteredDumpNoSequences := []byte("schema dump\nGRANT ALL ON SCHEMA \"public\" TO \"test_role\";\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\n")
	filteredDump := []byte("schema dump\nCREATE SEQUENCE test.test_sequence\nGRANT ALL ON SCHEMA \"public\" TO \"test_role\";\nALTER TABLE public.test_table OWNER TO test_role;\nGRANT ALL ON TABLE public.test_table TO test_role2;\n")
	sequenceDump := []byte("sequence dump\n")
	indexDump := []byte("CREATE INDEX a;\n\n")
	testViewsDump := []byte("CREATE VIEW public.test_view AS\n SELECT 1;\n\n")
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
			name: "ok - with views",
			snapshot: &snapshot.Snapshot{
				SchemaTables: map[string][]string{
					testSchema: {testTable},
				},
			},
			conn: validQuerier(),
			pgdumpFn: newMockPgdump(func(_ context.Context, i uint, po pglib.PGDumpOptions) ([]byte, error) {
				switch i {
				case 1:
					return schemaDumpWithViews, nil
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
			pgrestoreFn: newMockPgrestore(func(_ context.Context, i uint, po pglib.PGRestoreOptions, dump []byte) (string, error) {
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
				case 6:
					require.Equal(t, string(testViewsDump), string(dump))
				default:
					return "", fmt.Errorf("unexpected call to pgrestoreFn: %d", i)
				}
				return "", nil
			}),

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
						ExcludeSchemas:   []string{`"pgstream"`},
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
						ExcludeSchemas:   []string{`"pgstream"`},
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
						ExcludeSchemas:   []string{`"pgstream"`},
					}, po)
					return schemaDump, nil
				case 2:
					require.Equal(t, pglib.PGDumpOptions{
						ConnectionString: "source-url",
						Format:           "p",
						SchemaOnly:       true,
						Clean:            true,
						ExcludeSchemas:   []string{`"pgstream"`},
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

	wantViewsBytes, err := os.ReadFile("test/test_dump_views.sql")
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
	viewsStr := strings.Trim(string(dump.views), "\n")
	wantViewsStr := strings.Trim(string(wantViewsBytes), "\n")
	wantSequences := []string{`"musicbrainz"."alternative_medium_id_seq"`, `"musicbrainz"."Alternative_medium_id_seq"`}

	require.Equal(t, wantFilteredStr, filteredStr)
	require.Equal(t, wantConstraintsStr, constraintsStr)
	require.Equal(t, wantSequences, dump.sequences)
	require.Equal(t, wantEventTriggersStr, eventTriggersStr)
	require.Equal(t, wantViewsStr, viewsStr)
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
		// Non-ASCII Unicode letters are valid in dollar-quote tag identifiers
		{name: "non-ASCII tag $ñ$", line: "AS $ñ$", want: "$ñ$"},
		{name: "non-ASCII tag $función$", line: "AS $función$", want: "$función$"},
		{name: "CJK tag $表$", line: "AS $表$", want: "$表$"},
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

func TestParseDump_DisableTriggerGoesToIndicesAndConstraints(t *testing.T) {
	t.Parallel()

	// pg_dump emits ALTER TABLE ... DISABLE TRIGGER after CREATE TRIGGER.
	// Both must land in indicesAndConstraints so the DISABLE runs after the
	// trigger is created (phase 6). If DISABLE falls to filteredDump (phase 4),
	// it executes before CREATE TRIGGER and fails with "trigger does not exist".
	dumpInput := strings.Join([]string{
		"CREATE TABLE public.profiles (id integer NOT NULL, name text);",
		"CREATE TRIGGER trg_audit AFTER INSERT ON public.profiles FOR EACH ROW EXECUTE FUNCTION public.audit_fn();",
		"ALTER TABLE public.profiles DISABLE TRIGGER trg_audit;",
		"CREATE TRIGGER trg_log AFTER UPDATE ON public.profiles FOR EACH ROW EXECUTE FUNCTION public.log_fn();",
		"ALTER TABLE public.profiles ENABLE REPLICA TRIGGER trg_log;",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// CREATE TRIGGER must be in indices
	require.Contains(t, indices, "CREATE TRIGGER trg_audit", "CREATE TRIGGER trg_audit should be in indices")
	require.Contains(t, indices, "CREATE TRIGGER trg_log", "CREATE TRIGGER trg_log should be in indices")

	// DISABLE TRIGGER must be in indices (same phase as CREATE TRIGGER)
	require.Contains(t, indices, "ALTER TABLE public.profiles DISABLE TRIGGER trg_audit",
		"DISABLE TRIGGER should be in indices so it runs after CREATE TRIGGER")
	require.NotContains(t, filtered, "DISABLE TRIGGER",
		"DISABLE TRIGGER should NOT be in filtered dump (would run before trigger exists)")

	// ENABLE REPLICA TRIGGER must also be in indices
	require.Contains(t, indices, "ALTER TABLE public.profiles ENABLE REPLICA TRIGGER trg_log",
		"ENABLE REPLICA TRIGGER should be in indices")
	require.NotContains(t, filtered, "ENABLE REPLICA TRIGGER",
		"ENABLE REPLICA TRIGGER should NOT be in filtered dump")
}

func TestParseDump_MultiLineCommentOnTrigger(t *testing.T) {
	t.Parallel()

	// pg_dump can emit multi-line COMMENT ON TRIGGER where the comment text
	// spans multiple lines. The current code writes the first line + "\n\n"
	// with no multi-line state tracking, so the continuation line falls to
	// default: → filteredDump. This causes a syntax error during restore
	// because the continuation (e.g. "Maintains ...") is not valid SQL.
	dumpInput := strings.Join([]string{
		"COMMENT ON TRIGGER trg_sync ON public.metadata IS 'Keeps metadata_silver in sync.",
		"Maintains referential integrity between gold and silver tables.';",
		"CREATE INDEX idx_meta ON public.metadata USING btree (name);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// Both lines of the COMMENT ON TRIGGER must be in indices
	require.Contains(t, indices, "COMMENT ON TRIGGER trg_sync",
		"first line of multi-line COMMENT ON TRIGGER should be in indices")
	require.Contains(t, indices, "Maintains referential integrity",
		"continuation line of multi-line COMMENT ON TRIGGER should be in indices")

	// Neither line should be in filtered dump
	require.NotContains(t, filtered, "Maintains referential integrity",
		"continuation of COMMENT ON TRIGGER must NOT be in filtered dump (causes syntax error)")

	// The next statement after the comment closes should still route correctly
	require.Contains(t, indices, "CREATE INDEX idx_meta",
		"CREATE INDEX after multi-line comment should be in indices")
}

func TestParseDump_MultiLineCommentOnIndex(t *testing.T) {
	t.Parallel()

	// Same bug applies to COMMENT ON INDEX with multi-line text.
	dumpInput := strings.Join([]string{
		"COMMENT ON INDEX public.idx_users_email IS 'Speeds up email lookups.",
		"Required for the authentication flow.';",
		"CREATE TRIGGER trg_after ON public.users FOR EACH ROW EXECUTE FUNCTION public.noop();",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	require.Contains(t, indices, "COMMENT ON INDEX public.idx_users_email",
		"first line of multi-line COMMENT ON INDEX should be in indices")
	require.Contains(t, indices, "Required for the authentication flow.",
		"continuation line of multi-line COMMENT ON INDEX should be in indices")
	require.NotContains(t, filtered, "Required for the authentication flow.",
		"continuation of COMMENT ON INDEX must NOT be in filtered dump")
}

func TestParseDump_MultiLineCommentOnConstraint(t *testing.T) {
	t.Parallel()

	// Same bug applies to COMMENT ON CONSTRAINT with multi-line text.
	dumpInput := strings.Join([]string{
		"COMMENT ON CONSTRAINT users_pkey ON public.users IS 'Primary key.",
		"Auto-generated by migration v42.';",
		"CREATE INDEX idx_next ON public.users USING btree (created_at);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	require.Contains(t, indices, "COMMENT ON CONSTRAINT users_pkey",
		"first line of multi-line COMMENT ON CONSTRAINT should be in indices")
	require.Contains(t, indices, "Auto-generated by migration v42.",
		"continuation line of multi-line COMMENT ON CONSTRAINT should be in indices")
	require.NotContains(t, filtered, "Auto-generated by migration v42.",
		"continuation of COMMENT ON CONSTRAINT must NOT be in filtered dump")
}

func TestParseDump_MultiLineCommentWithSemicolonInText(t *testing.T) {
	t.Parallel()

	// The COMMENT ON case checks strings.HasSuffix(line, ";") to decide
	// if the statement is complete. But the first line of a multi-line
	// comment can end with ";" if the comment TEXT contains a semicolon
	// at the end of a physical line. COMMENT ON always terminates with
	// '; (closing single quote + semicolon), not bare ";".
	//
	// This causes the code to treat the first line as complete, so the
	// continuation falls to default: → filteredDump → syntax error.
	dumpInput := strings.Join([]string{
		"COMMENT ON TRIGGER trg_ref ON public.docs IS 'See RFC 7231;",
		"This trigger is important.';",
		"CREATE INDEX idx_docs ON public.docs USING btree (title);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// Both lines of the comment must be in indices
	require.Contains(t, indices, "COMMENT ON TRIGGER trg_ref",
		"first line of comment should be in indices")
	require.Contains(t, indices, "This trigger is important.",
		"continuation after semicolon-in-text must stay in indices")
	require.NotContains(t, filtered, "This trigger is important.",
		"continuation must NOT leak to filtered dump")
}

func TestParseDump_MultiLineCommentWithDollarSignOnContinuation(t *testing.T) {
	t.Parallel()

	// Dollar-quote tracking runs BEFORE the pendingComment check.
	// extractDollarQuoteTag starts each line with inSingleQuote=false.
	// When $$ appears on a CONTINUATION line (not the first line where
	// extractDollarQuoteTag correctly tracks inSingleQuote from the
	// opening '), it gets misidentified as a dollar-quote tag.
	// updateDollarQuoteState sees 1 occurrence (odd) → inDollarQuote=true
	// → line goes to filteredDump via continue, pendingComment left
	// dangling, and ALL subsequent lines are corrupted.
	dumpInput := strings.Join([]string{
		"COMMENT ON TRIGGER trg_tmpl ON public.funcs IS 'First line.",
		"Use $$ for dollar quoting in functions.';",
		"CREATE INDEX idx_funcs ON public.funcs USING btree (name);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// Both lines of the comment must be in indices
	require.Contains(t, indices, "COMMENT ON TRIGGER trg_tmpl",
		"first line should be in indices")
	require.Contains(t, indices, "Use $$ for dollar quoting",
		"continuation with $$ must stay in indices, not leak via dollar-quote shortcircuit")
	require.NotContains(t, filtered, "Use $$ for dollar quoting",
		"continuation must NOT go to filtered dump")

	// The CREATE INDEX after the comment must also route correctly
	// (if dollar-quote state is corrupted, this gets eaten too)
	require.Contains(t, indices, "CREATE INDEX idx_funcs",
		"CREATE INDEX after multi-line comment must not be corrupted by dangling dollar-quote state")
	require.NotContains(t, filtered, "CREATE INDEX idx_funcs",
		"CREATE INDEX must not leak to filtered dump")
}

func TestParseDump_MultiLineCommentWithEscapedQuoteBeforeSemicolon(t *testing.T) {
	t.Parallel()

	// pg_dump escapes single quotes by doubling them. If the comment text
	// contains a literal quote followed by ";" at a line boundary, the
	// output ends with ''; — which matches HasSuffix(line, "';") even
	// though the string literal is still open (the ' is the second half
	// of an '' escape, not a closing quote).
	//
	// Original comment text: "ends with a quote';\nMore text"
	// pg_dump escapes ' → '' giving: 'ends with a quote'';
	// Line ends with ''; — HasSuffix(line,"';") is TRUE because
	// '; is a suffix of ''; — but the ' is the second half of the
	// '' escape, not a closing quote. The string literal is still open.
	dumpInput := strings.Join([]string{
		"COMMENT ON TRIGGER trg_esc ON public.rules IS 'ends with a quote'';",
		"More text about the trigger.';",
		"CREATE INDEX idx_rules ON public.rules USING btree (name);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	require.Contains(t, indices, "COMMENT ON TRIGGER trg_esc",
		"first line of comment must be in indices")
	require.Contains(t, indices, "More text about the trigger.",
		"continuation after escaped-quote+semicolon must stay in indices")
	require.NotContains(t, filtered, "More text about the trigger.",
		"continuation must NOT leak to filtered dump")
	require.Contains(t, indices, "CREATE INDEX idx_rules",
		"CREATE INDEX after comment must route correctly")
}

func TestParseDump_CommentWithDollarTag(t *testing.T) {
	t.Parallel()

	// A -- comment line containing $$ must not flip dollar-quote state.
	// Without the fix, parseDump enters dollar-quote mode on the comment
	// and misroutes the subsequent CREATE INDEX to filteredDump.
	dumpInput := strings.Join([]string{
		"-- This function uses $$ dollar quoting",
		"CREATE INDEX idx_after_comment ON public.t USING btree (col);",
		"CREATE TRIGGER trg_after_comment BEFORE UPDATE ON public.t FOR EACH ROW EXECUTE FUNCTION public.noop();",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	require.Contains(t, indices, "CREATE INDEX idx_after_comment", "CREATE INDEX after comment should be in indices")
	require.NotContains(t, filtered, "CREATE INDEX idx_after_comment", "CREATE INDEX after comment should not be in filtered")

	require.Contains(t, indices, "CREATE TRIGGER trg_after_comment", "CREATE TRIGGER after comment should be in indices")
	require.NotContains(t, filtered, "CREATE TRIGGER trg_after_comment", "CREATE TRIGGER after comment should not be in filtered")
}

func TestParseDump_ExtensionMapper(t *testing.T) {
	t.Parallel()

	dumpInput := strings.Join([]string{
		"CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;",
		"CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;",
		"CREATE EXTENSION IF NOT EXISTS vectorscale WITH SCHEMA public;",
		"COMMENT ON EXTENSION vectorscale IS 'pgvectorscale';",
		"DROP EXTENSION IF EXISTS vectorscale;",
		"CREATE TABLE public.chunk (id integer, embedding vector(1536));",
		"CREATE INDEX idx_chunk_embedding ON public.chunk USING diskann (embedding) WITH (storage_layout=memory_optimized, num_neighbors='50');",
		"CREATE INDEX idx_chunk_name ON public.chunk USING btree (id);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
		logger:        log.NewNoopLogger(),
		extensionMap: map[string]ExtensionMapping{
			"vectorscale": {
				ReplaceWith:  "vector",
				IndexMap:     map[string]string{"diskann": "hnsw"},
				IndexOpClass: "vector_cosine_ops",
			},
		},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)
	indices := string(result.indicesAndConstraints)

	// vectorscale CREATE EXTENSION should be rewritten to vector
	require.NotContains(t, filtered, "vectorscale", "vectorscale should be replaced everywhere in filtered dump")
	require.Contains(t, filtered, "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;", "vectorscale should be rewritten to vector")

	// Original vector extension should still be there
	require.Contains(t, filtered, "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;")

	// plpgsql should be untouched
	require.Contains(t, filtered, "CREATE EXTENSION IF NOT EXISTS plpgsql")

	// Table definition should be untouched
	require.Contains(t, filtered, "CREATE TABLE public.chunk")

	// diskann index should be rewritten to hnsw with WITH clause stripped and opclass added
	require.Contains(t, indices, "USING hnsw", "diskann should be rewritten to hnsw")
	require.NotContains(t, indices, "diskann", "diskann should not appear in indices")
	require.NotContains(t, indices, "storage_layout", "WITH clause should be stripped")
	require.NotContains(t, indices, "num_neighbors", "WITH clause should be stripped")
	require.Contains(t, indices, "vector_cosine_ops", "default operator class should be injected")

	// btree index should be untouched
	require.Contains(t, indices, "USING btree (id)", "btree index should be untouched")
}

func TestParseDump_ExtensionMapperDropExtension(t *testing.T) {
	t.Parallel()

	dumpInput := strings.Join([]string{
		"CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;",
		"CREATE TABLE public.data (id integer);",
		"",
	}, "\n")

	s := &SnapshotGenerator{
		roleSQLParser: &roleSQLParser{},
		logger:        log.NewNoopLogger(),
		extensionMap: map[string]ExtensionMapping{
			"timescaledb": {
				ReplaceWith: "", // drop entirely
			},
		},
	}
	result := s.parseDump([]byte(dumpInput))

	filtered := string(result.filtered)

	require.NotContains(t, filtered, "timescaledb", "dropped extension should not appear")
	require.Contains(t, filtered, "CREATE TABLE public.data", "table should be untouched")
}
