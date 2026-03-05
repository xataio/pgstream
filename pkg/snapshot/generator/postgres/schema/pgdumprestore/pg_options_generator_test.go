// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pglibmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

func TestOptionsGenerator_pgdumpSequenceDataOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sequences []string

		wantOpts *pglib.PGDumpOptions
	}{
		{
			name:      "ok",
			sequences: []string{"seq1", "seq2"},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				DataOnly:         true,
				Tables:           []string{"seq1", "seq2"},
			},
		},
		{
			name:      "no sequences",
			sequences: []string{},

			wantOpts: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			og := newOptionGenerator(nil, &Config{SourcePGURL: "source-url"})

			got := og.pgdumpSequenceDataOptions(tc.sequences)
			require.Equal(t, tc.wantOpts, got)
		})
	}
}

func TestOptionsGenerator_pgdumpRolesOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		rolesSnapshotMode string
		cleanTargetDB     bool
		role              string
		noOwner           bool
		noPrivileges      bool

		wantOpts *pglib.PGDumpAllOptions
	}{
		{
			name:              "roles snapshot disabled",
			rolesSnapshotMode: roleSnapshotDisabled,
			wantOpts:          nil,
		},
		{
			name:              "roles snapshot enabled with passwords",
			rolesSnapshotMode: "",
			cleanTargetDB:     true,
			role:              "myrole",
			noOwner:           true,
			noPrivileges:      true,
			wantOpts: &pglib.PGDumpAllOptions{
				ConnectionString: "source-url",
				RolesOnly:        true,
				Clean:            true,
				Role:             "myrole",
				NoOwner:          true,
				NoPrivileges:     true,
				NoPasswords:      false,
			},
		},
		{
			name:              "roles snapshot no passwords",
			rolesSnapshotMode: roleSnapshotNoPasswords,
			wantOpts: &pglib.PGDumpAllOptions{
				ConnectionString: "source-url",
				RolesOnly:        true,
				Clean:            false,
				Role:             "",
				NoOwner:          false,
				NoPrivileges:     false,
				NoPasswords:      true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			og := &optionGenerator{
				sourceURL:         "source-url",
				rolesSnapshotMode: tc.rolesSnapshotMode,
				cleanTargetDB:     tc.cleanTargetDB,
				role:              tc.role,
				noOwner:           tc.noOwner,
				noPrivileges:      tc.noPrivileges,
			}
			got := og.pgdumpRolesOptions()
			require.Equal(t, tc.wantOpts, got)
		})
	}
}

func TestOptionsGenerator_pgrestoreOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		targetURL      string
		cleanTargetDB  bool
		createTargetDB bool

		wantOpts pglib.PGRestoreOptions
	}{
		{
			name:           "basic restore options",
			targetURL:      "target-url",
			cleanTargetDB:  true,
			createTargetDB: false,
			wantOpts: pglib.PGRestoreOptions{
				ConnectionString: "target-url",
				Clean:            true,
				Format:           "p",
				Create:           false,
			},
		},
		{
			name:           "create target db",
			targetURL:      "target-url2",
			cleanTargetDB:  false,
			createTargetDB: true,
			wantOpts: pglib.PGRestoreOptions{
				ConnectionString: "target-url2",
				Clean:            false,
				Format:           "p",
				Create:           true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			og := &optionGenerator{
				targetURL:      tc.targetURL,
				cleanTargetDB:  tc.cleanTargetDB,
				createTargetDB: tc.createTargetDB,
			}
			got := og.pgrestoreOptions()
			require.Equal(t, tc.wantOpts, got)
		})
	}
}

func TestOptionsGenerator_pgdumpOptions(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name           string
		schemaTables   map[string][]string
		excludedTables map[string][]string
		includeGlobal  bool
		conn           *pglibmocks.Querier

		wantOpts *pglib.PGDumpOptions
		wantErr  error
	}{
		{
			name: "schema tables",
			schemaTables: map[string][]string{
				"public": {"table1", "table2"},
			},
			excludedTables: map[string][]string{},
			includeGlobal:  false,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemaTablesQuery, query)
					require.Equal(t, []any{"public", []string{"table1", "table2"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = "public"
							table, ok := dest[1].(*string)
							require.True(t, ok)
							*table = "table3"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				Schemas:          []string{`"public"`},
				ExcludeSchemas:   nil,
				SchemaOnly:       true,
				ExcludeTables:    []string{`"public"."table3"`},
			},
			wantErr: nil,
		},
		{
			name: "schema tables and excluded tables",
			schemaTables: map[string][]string{
				"public": {"table1", "table2"},
			},
			excludedTables: map[string][]string{
				"public": {"table4"},
			},
			includeGlobal: false,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemaTablesQuery, query)
					require.Equal(t, []any{"public", []string{"table1", "table2"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = "public"
							table, ok := dest[1].(*string)
							require.True(t, ok)
							*table = "table3"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				Schemas:          []string{`"public"`},
				ExcludeSchemas:   nil,
				SchemaOnly:       true,
				ExcludeTables:    []string{`"public"."table3"`, `"public"."table4"`},
			},
			wantErr: nil,
		},
		{
			name: "wildcard schema tables and excluded wildcard tables",
			schemaTables: map[string][]string{
				"*": {"*"},
			},
			excludedTables: map[string][]string{
				"excluded_schema": {"*"},
			},
			includeGlobal: false,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, fmt.Errorf("QueryFn should not be called")
				},
			},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				ExcludeSchemas:   []string{`"excluded_schema"`},
				SchemaOnly:       true,
			},
			wantErr: nil,
		},
		{
			name: "wildcard schema with specific tables",
			schemaTables: map[string][]string{
				"*": {"table1", "table2"},
			},
			excludedTables: map[string][]string{},
			includeGlobal:  false,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectTablesQuery, query)
					require.Equal(t, []any{[]string{"table1", "table2"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = "public"
							table, ok := dest[1].(*string)
							require.True(t, ok)
							*table = "table3"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				ExcludeSchemas:   nil,
				SchemaOnly:       true,
				ExcludeTables:    []string{`"public"."table3"`},
			},
			wantErr: nil,
		},
		{
			name: "wildcard schema and wildcard tables",
			schemaTables: map[string][]string{
				"*": {"*"},
			},
			excludedTables: map[string][]string{},
			includeGlobal:  false,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errors.New("QueryFn should not be called")
				},
			},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				ExcludeSchemas:   nil,
				SchemaOnly:       true,
			},
			wantErr: nil,
		},
		{
			name: "schema and tables with include global objects enabled",
			schemaTables: map[string][]string{
				"public": {"table1", "table2"},
			},
			excludedTables: map[string][]string{},
			includeGlobal:  true,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case selectSchemaTablesQuery:
						require.Equal(t, []any{"public", []string{"table1", "table2"}}, args)
						return &pglibmocks.Rows{
							NextFn: func(i uint) bool { return i == 1 },
							ScanFn: func(i uint, dest ...any) error {
								require.Len(t, dest, 2)
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "public"
								table, ok := dest[1].(*string)
								require.True(t, ok)
								*table = "table3"
								return nil
							},
							ErrFn:   func() error { return nil },
							CloseFn: func() {},
						}, nil
					case selectSchemasQuery:
						require.Equal(t, []any{[]string{"public"}}, args)
						return &pglibmocks.Rows{
							NextFn: func(i uint) bool { return i == 1 },
							ScanFn: func(i uint, dest ...any) error {
								require.Len(t, dest, 1)
								schema, ok := dest[0].(*string)
								require.True(t, ok)
								*schema = "excluded_schema"
								return nil
							},
							ErrFn:   func() error { return nil },
							CloseFn: func() {},
						}, nil
					default:
						return nil, fmt.Errorf("unexpected query: %s", query)
					}
				},
			},

			wantOpts: &pglib.PGDumpOptions{
				ConnectionString: "source-url",
				Format:           "p",
				ExcludeSchemas:   []string{`"excluded_schema"`},
				SchemaOnly:       true,
				ExcludeTables:    []string{`"public"."table3"`},
			},
			wantErr: nil,
		},
		{
			name: "error getting excluded schemas",
			schemaTables: map[string][]string{
				"public": {"table1", "table2"},
			},
			excludedTables: map[string][]string{},
			includeGlobal:  true,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					switch query {
					case selectSchemasQuery:
						require.Equal(t, []any{[]string{"public"}}, args)
						return nil, errTest
					default:
						return nil, fmt.Errorf("unexpected query: %s", query)
					}
				},
			},

			wantOpts: nil,
			wantErr:  errTest,
		},
		{
			name: "error getting excluded tables",
			schemaTables: map[string][]string{
				"public": {"table1", "table2"},
			},
			excludedTables: map[string][]string{},
			includeGlobal:  false,
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemaTablesQuery, query)
					require.Equal(t, []any{"public", []string{"table1", "table2"}}, args)
					return nil, errTest
				},
			},

			wantOpts: nil,
			wantErr:  errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			og := &optionGenerator{
				sourceURL:              "source-url",
				cleanTargetDB:          false,
				createTargetDB:         false,
				includeGlobalDBObjects: tc.includeGlobal,
				querier:                tc.conn,
			}
			opts, err := og.pgdumpOptions(
				context.Background(),
				tc.schemaTables,
				tc.excludedTables,
			)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantOpts, opts)
		})
	}
}

func TestOptionsGenerator_pgdumpExcludedTables(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name         string
		schemaTables map[string][]string
		conn         *pglibmocks.Querier
		wantExcluded []string
		wantErr      error
	}{
		{
			name: "single schema with tables",
			schemaTables: map[string][]string{
				"public": {"table1", "table2"},
			},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemaTablesQuery, query)
					require.Equal(t, []any{"public", []string{"table1", "table2"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = "public"
							table, ok := dest[1].(*string)
							require.True(t, ok)
							*table = "excluded_table"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: []string{`"public"."excluded_table"`},
			wantErr:      nil,
		},
		{
			name: "quoted schema with quoted tables",
			schemaTables: map[string][]string{
				`"public"`: {`"table1"`, `"table2"`},
			},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemaTablesQuery, query)
					require.Equal(t, []any{"public", []string{"table1", "table2"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = "public"
							table, ok := dest[1].(*string)
							require.True(t, ok)
							*table = "excluded_table"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: []string{`"public"."excluded_table"`},
			wantErr:      nil,
		},
		{
			name: "wildcard schema with specific tables",
			schemaTables: map[string][]string{
				"*": {"table1", "table2"},
			},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectTablesQuery, query)
					require.Equal(t, []any{[]string{"table1", "table2"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 2)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = fmt.Sprintf("schema%d", i)
							table, ok := dest[1].(*string)
							require.True(t, ok)
							*table = "excluded_table"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: []string{`"schema1"."excluded_table"`, `"schema2"."excluded_table"`},
			wantErr:      nil,
		},
		{
			name: "error from query",
			schemaTables: map[string][]string{
				"public": {"table1"},
			},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},
			wantExcluded: nil,
			wantErr:      errTest,
		},
		{
			name: "error from scan",
			schemaTables: map[string][]string{
				"public": {"table1"},
			},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: nil,
			wantErr:      errTest,
		},
		{
			name: "error from rows.Err",
			schemaTables: map[string][]string{
				"public": {"table1"},
			},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return &pglibmocks.Rows{
						NextFn:  func(i uint) bool { return false },
						ScanFn:  func(i uint, dest ...any) error { return nil },
						ErrFn:   func() error { return errTest },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			og := &optionGenerator{
				sourceURL: "source-url",
				querier:   tc.conn,
			}

			for schema, tables := range tc.schemaTables {
				excluded, err := og.pgdumpExcludedTables(context.Background(), schema, tables)
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, tc.wantExcluded, excluded)
			}
		})
	}
}

func TestOptionsGenerator_pgdumpExcludedSchemas(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name           string
		includeSchemas []string
		conn           *pglibmocks.Querier
		wantExcluded   []string
		wantErr        error
	}{
		{
			name:           "single schema excluded",
			includeSchemas: []string{"public"},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemasQuery, query)
					require.Equal(t, []any{[]string{"public"}}, args)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 1)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = "excluded_schema"
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: []string{`"excluded_schema"`},
			wantErr:      nil,
		},
		{
			name:           "multiple schemas excluded",
			includeSchemas: []string{"public", "other"},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					require.Equal(t, selectSchemasQuery, query)
					require.Len(t, args, 1)
					schemas, ok := args[0].([]string)
					require.True(t, ok)
					require.ElementsMatch(t, []string{"public", "other"}, schemas)
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i <= 2 },
						ScanFn: func(i uint, dest ...any) error {
							require.Len(t, dest, 1)
							schema, ok := dest[0].(*string)
							require.True(t, ok)
							*schema = fmt.Sprintf("excluded_schema%d", i)
							return nil
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: []string{`"excluded_schema1"`, `"excluded_schema2"`},
			wantErr:      nil,
		},
		{
			name:           "error from query",
			includeSchemas: []string{"public"},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return nil, errTest
				},
			},
			wantExcluded: nil,
			wantErr:      errTest,
		},
		{
			name:           "error from scan",
			includeSchemas: []string{"public"},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return &pglibmocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(i uint, dest ...any) error {
							return errTest
						},
						ErrFn:   func() error { return nil },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: nil,
			wantErr:      errTest,
		},
		{
			name:           "error from rows.Err",
			includeSchemas: []string{"public"},
			conn: &pglibmocks.Querier{
				QueryFn: func(ctx context.Context, _ uint, query string, args ...any) (pglib.Rows, error) {
					return &pglibmocks.Rows{
						NextFn:  func(i uint) bool { return false },
						ScanFn:  func(i uint, dest ...any) error { return nil },
						ErrFn:   func() error { return errTest },
						CloseFn: func() {},
					}, nil
				},
			},
			wantExcluded: nil,
			wantErr:      errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			og := &optionGenerator{
				sourceURL: "source-url",
				querier:   tc.conn,
			}

			excluded, err := og.pgdumpExcludedSchemas(context.Background(), tc.includeSchemas)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantExcluded, excluded)
		})
	}
}
