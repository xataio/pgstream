// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/stream"
	pgprocessor "github.com/xataio/pgstream/pkg/wal/processor/postgres"
)

const (
	oidInt4    = 23     // resolvable via the pgx static type map (built-in)
	oidText    = 25     // built-in
	oidUnknown = 999999 // not a built-in type; pgx cannot encode/decode it
)

func TestSchemaTypeCompatibilityCheck_Run_AllTypesBuiltIn(t *testing.T) {
	t.Parallel()

	check := &SchemaTypeCompatibilityCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "orders", Column: "id", BaseOID: oidInt4, TypeName: "int4", TypeKind: "b"},
			{Schema: "public", Table: "orders", Column: "name", BaseOID: oidText, TypeName: "text", TypeKind: "b"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestSchemaTypeCompatibilityCheck_Run_UserDefinedTypeReturnsFinding(t *testing.T) {
	t.Parallel()

	check := &SchemaTypeCompatibilityCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "orders", Column: "id", BaseOID: oidInt4, TypeName: "int4", TypeKind: "b"},
			{Schema: "public", Table: "orders", Column: "amount", BaseOID: oidUnknown, TypeName: "money_amount", TypeKind: "c"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `"public"."orders"."amount"`)
	require.Contains(t, findings[0].Message, `composite type "money_amount"`)
	require.Contains(t, findings[0].Message, "exclude the table")
}

func TestSchemaTypeCompatibilityCheck_Run_SkipsEnums(t *testing.T) {
	t.Parallel()
	check := &SchemaTypeCompatibilityCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "orders", Column: "status", BaseOID: oidUnknown, TypeName: "order_status", TypeKind: "e"},
			{Schema: "public", Table: "orders", Column: "tags", BaseOID: oidUnknown + 1, TypeName: "_order_status", TypeKind: "b", ElemTypeKind: "e"},
			{Schema: "public", Table: "orders", Column: "amount", BaseOID: oidUnknown + 2, TypeName: "money_amount", TypeKind: "c"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `"public"."orders"."amount"`)
	require.Contains(t, findings[0].Message, "composite type")
}

func TestSchemaTypeCompatibilityCheck_Run_PgstreamExtraTypesPass(t *testing.T) {
	t.Parallel()

	// hstore, pgvector (vector/halfvec/sparsevec), cube and ltree are extension
	// types pgx has no codec for, but pgstream registers them on every connection
	// so they are supported; a genuinely unsupported extension type still flags.
	check := &SchemaTypeCompatibilityCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "t", Column: "h", BaseOID: 16400, TypeName: "hstore", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "v", BaseOID: 16410, TypeName: "vector", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "hv", BaseOID: 16420, TypeName: "halfvec", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "sv", BaseOID: 16430, TypeName: "sparsevec", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "va", BaseOID: 16440, TypeName: "_vector", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "hva", BaseOID: 16450, TypeName: "_halfvec", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "sva", BaseOID: 16460, TypeName: "_sparsevec", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "c", BaseOID: 16500, TypeName: "cube", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "l", BaseOID: 16510, TypeName: "ltree", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "g", BaseOID: 16600, TypeName: "geometry", TypeKind: "b"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `"public"."t"."g"`)
	require.Contains(t, findings[0].Message, `type "geometry"`)
}

func TestPgstreamSupportedTypesMatchesRegistry(t *testing.T) {
	t.Parallel()

	// Every extension type pgstream registers must be accepted by the check (no
	// finding), so the check can't drift from the registry. Exercised
	// behaviourally — with a synthetic non-builtin OID per type — so it doesn't
	// couple to the internal representation of the supported set.
	names := postgres.ExtensionTypeNames()
	require.Contains(t, names, "hstore")
	require.Contains(t, names, "vector")

	rows := make([]schemaColumnRow, len(names))
	for i, n := range names {
		rows[i] = schemaColumnRow{Schema: "public", Table: "t", Column: n, BaseOID: int64(20000 + i), TypeName: n, TypeKind: "b"}
	}
	check := &SchemaTypeCompatibilityCheck{Source: sourceWithColumns(t, rows)}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Emptyf(t, findings, "registered extension types should all be supported, got: %v", findings)
}

func TestSchemaTypeCompatibilityCheck_Run_FiltersTablesByScope(t *testing.T) {
	t.Parallel()

	sel, err := stream.NewTableSelection([]string{"public.*"}, []string{"public.audit_log"})
	require.NoError(t, err)

	check := &SchemaTypeCompatibilityCheck{
		Selection: sel,
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "billing", Table: "invoices", Column: "c", BaseOID: oidUnknown, TypeName: "money_amount", TypeKind: "c"},
			{Schema: "public", Table: "audit_log", Column: "c", BaseOID: oidUnknown, TypeName: "money_amount", TypeKind: "c"},
			{Schema: "public", Table: "orders", Column: "c", BaseOID: oidUnknown, TypeName: "money_amount", TypeKind: "c"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `"public"."orders"."c"`)
	require.Contains(t, findings[0].Message, "composite type")
}

func TestSchemaTypeCompatibilityCheck_Run_SourceAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, checkErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestSchemaTypeCompatibilityCheck_Run_QueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return nil, queryErr
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying source column types")
}

func TestSchemaTypeCompatibilityCheck_Run_ScanFails(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("scan failed")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return &mocks.Rows{
						NextFn: func(i uint) bool { return i == 1 },
						ScanFn: func(uint, ...any) error { return scanErr },
						ErrFn:  func() error { return nil },
					}, nil
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, scanErr)
	require.ErrorContains(t, err, "scanning row")
}

func TestSchemaTypeCompatibilityCheck_Run_RowsErr(t *testing.T) {
	t.Parallel()

	rowsErr := errors.New("rows failed")
	check := &SchemaTypeCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return &mocks.Rows{
						NextFn: func(uint) bool { return false },
						ScanFn: func(uint, ...any) error {
							t.Fatal("Scan should not be called")
							return nil
						},
						ErrFn: func() error { return rowsErr },
					}, nil
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, rowsErr)
	require.ErrorContains(t, err, "iterating rows")
}

func TestSchemaTypeCompatibilityCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "schema_type_compatibility", (&SchemaTypeCompatibilityCheck{}).Name())
}

func TestTypeKindLabel(t *testing.T) {
	t.Parallel()

	require.Equal(t, "composite type", typeKindLabel("c"))
	require.Equal(t, "enum type", typeKindLabel("e"))
	require.Equal(t, "range type", typeKindLabel("r"))
	require.Equal(t, "multirange type", typeKindLabel("m"))
	require.Equal(t, "pseudo-type", typeKindLabel("p"))
	require.Equal(t, "", typeKindLabel("b"))
}

func TestUnsupportedColumnTypeMessage_BaseTypeOmitsKindNoun(t *testing.T) {
	t.Parallel()

	// A built-in system type pgx doesn't register (typtype 'b') should read as a
	// bare type name, not be mislabelled as a "user-defined type".
	msg := unsupportedColumnTypeMessage(schemaColumnRow{
		Schema: "public", Table: "t", Column: "relid", TypeName: "regclass", TypeKind: "b",
	})
	require.Contains(t, msg, `"public"."t"."relid": type "regclass"`)
	require.NotContains(t, msg, "user-defined type")
}

func TestPostgresRangeTypeCheck_Run_SupportedRangesPass(t *testing.T) {
	t.Parallel()

	check := &PostgresRangeTypeCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "t", Column: "a", BaseOID: oidInt4, TypeName: "int4", TypeKind: "b"},
			{Schema: "public", Table: "t", Column: "b", BaseOID: 3904, TypeName: "int4range", TypeKind: "r"},
			{Schema: "public", Table: "t", Column: "c", BaseOID: 3926, TypeName: "int8range", TypeKind: "r"},
			{Schema: "public", Table: "t", Column: "d", BaseOID: 3910, TypeName: "tstzrange", TypeKind: "r"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestPostgresRangeTypeCheck_Run_UnsupportedRangesReturnFindings(t *testing.T) {
	t.Parallel()

	check := &PostgresRangeTypeCheck{
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "t", Column: "n", BaseOID: 3906, TypeName: "numrange", TypeKind: "r"},
			{Schema: "public", Table: "t", Column: "d", BaseOID: 3912, TypeName: "daterange", TypeKind: "r"},
			{Schema: "public", Table: "t", Column: "m", BaseOID: 4451, TypeName: "int4multirange", TypeKind: "m"},
			{Schema: "public", Table: "t", Column: "ok", BaseOID: 3904, TypeName: "int4range", TypeKind: "r"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 3)
	require.Contains(t, findings[0].Message, `"public"."t"."n"`)
	require.Contains(t, findings[0].Message, `range type "numrange"`)
	require.Contains(t, findings[1].Message, `range type "daterange"`)
	require.Contains(t, findings[2].Message, `multirange type "int4multirange"`)
}

func TestPostgresRangeTypeCheck_Run_FiltersTablesByScope(t *testing.T) {
	t.Parallel()

	sel, err := stream.NewTableSelection([]string{"public.orders"}, nil)
	require.NoError(t, err)

	check := &PostgresRangeTypeCheck{
		Selection: sel,
		Source: sourceWithColumns(t, []schemaColumnRow{
			{Schema: "public", Table: "events", Column: "r", BaseOID: 3906, TypeName: "numrange", TypeKind: "r"},
			{Schema: "public", Table: "orders", Column: "r", BaseOID: 3906, TypeName: "numrange", TypeKind: "r"},
		}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, `"public"."orders"."r"`)
}

func TestPostgresRangeTypeCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "postgres_range_type_support", (&PostgresRangeTypeCheck{}).Name())
}

func TestUnsupportedRangeTypeMessage(t *testing.T) {
	t.Parallel()

	// non-range types and supported ranges produce nothing
	require.Empty(t, unsupportedRangeTypeMessage(schemaColumnRow{TypeName: "text", TypeKind: "b"}))
	require.Empty(t, unsupportedRangeTypeMessage(schemaColumnRow{TypeName: "int4range", TypeKind: "r"}))
	require.Empty(t, unsupportedRangeTypeMessage(schemaColumnRow{TypeName: "tstzrange", TypeKind: "r"}))

	// unsupported range/multirange types produce a finding
	require.NotEmpty(t, unsupportedRangeTypeMessage(schemaColumnRow{TypeName: "numrange", TypeKind: "r"}))
	require.NotEmpty(t, unsupportedRangeTypeMessage(schemaColumnRow{TypeName: "tstzmultirange", TypeKind: "m"}))
}

func TestSchemaExtensionCompatibilityCheck_Run_AllPresentOnTarget(t *testing.T) {
	t.Parallel()

	check := &SchemaExtensionCompatibilityCheck{
		Source: extensionSource(t, []sourceExtensionRow{
			{Name: "hstore", Schema: "public"},
			{Name: "postgis", Schema: "public"},
		}),
		Target: extensionTarget(t, []string{"hstore", "postgis", "pg_stat_statements"}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
	// even when nothing is missing, the report records every source extension
	require.Equal(t, map[string]any{"source_extensions": []string{"hstore", "postgis"}}, check.Details())
}

func TestSchemaExtensionCompatibilityCheck_Details_EmptyWhenNoExtensions(t *testing.T) {
	t.Parallel()

	check := &SchemaExtensionCompatibilityCheck{}

	// before Run (or with no source extensions) it is an empty array, never nil,
	// so the JSON report always renders source_extensions as [].
	require.Equal(t, map[string]any{"source_extensions": []string{}}, check.Details())
	data, err := json.Marshal(CheckResult{Name: "x", Details: check.Details()})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"x","findings":null,"source_extensions":[]}`, string(data))
}

func TestSchemaExtensionCompatibilityCheck_Run_MissingExtensionReturnsFinding(t *testing.T) {
	t.Parallel()

	check := &SchemaExtensionCompatibilityCheck{
		Source: extensionSource(t, []sourceExtensionRow{
			{Name: "hstore", Schema: "public"},
			{Name: "postgis", Schema: "gis"},
		}),
		Target: extensionTarget(t, []string{"hstore"}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, "1 extension installed on source")
	require.Contains(t, findings[0].Message, `"postgis" (source schema "gis")`)
	require.Contains(t, findings[0].Message, `CREATE EXTENSION IF NOT EXISTS "postgis";`)
}

func TestSchemaExtensionCompatibilityCheck_Run_MultipleMissingCollectedInOneFinding(t *testing.T) {
	t.Parallel()

	check := &SchemaExtensionCompatibilityCheck{
		Source: extensionSource(t, []sourceExtensionRow{
			{Name: "hstore", Schema: "public"},
			{Name: "postgis", Schema: "gis"},
			{Name: "pg_trgm", Schema: "public"},
		}),
		Target: extensionTarget(t, []string{"hstore"}),
	}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Message, "2 extensions installed on source")
	require.Contains(t, findings[0].Message, `"postgis" (source schema "gis")`)
	require.Contains(t, findings[0].Message, `"pg_trgm" (source schema "public")`)
	require.Contains(t, findings[0].Message, `CREATE EXTENSION IF NOT EXISTS "postgis";`)
	require.Contains(t, findings[0].Message, `CREATE EXTENSION IF NOT EXISTS "pg_trgm";`)
	require.Equal(t, []string{"hstore", "postgis", "pg_trgm"}, check.Details()["source_extensions"])
}

func TestSchemaExtensionCompatibilityCheck_Run_TargetAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &SchemaExtensionCompatibilityCheck{
		Source: extensionSource(t, nil),
		Target: func(context.Context) (postgres.Querier, error) {
			return nil, checkErr
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to target")
}

func TestSchemaExtensionCompatibilityCheck_Run_TargetQueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &SchemaExtensionCompatibilityCheck{
		Source: extensionSource(t, nil),
		Target: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
					return nil, queryErr
				},
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying target extensions")
}

func TestSchemaExtensionCompatibilityCheck_Run_SourceAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &SchemaExtensionCompatibilityCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return nil, checkErr
		},
		Target: extensionTarget(t, []string{"hstore"}),
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestSchemaExtensionCompatibilityCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "schema_extension_compatibility", (&SchemaExtensionCompatibilityCheck{}).Name())
}

func TestBuildSchemaChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       *stream.Config
		wantNames []string
	}{
		{
			name: "no source postgres url returns no checks",
			cfg:  &stream.Config{},
		},
		{
			name: "non-postgres target returns only the type compatibility check",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
			},
			wantNames: []string{"schema_type_compatibility"},
		},
		{
			name: "postgres target also returns the range type check",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
				Processor: stream.ProcessorConfig{
					Postgres: &stream.PostgresProcessorConfig{},
				},
			},
			wantNames: []string{"schema_type_compatibility", "postgres_range_type_support"},
		},
		{
			name: "postgres target with url also returns the extension check",
			cfg: &stream.Config{
				Listener: stream.ListenerConfig{
					Postgres: &stream.PostgresListenerConfig{URL: "postgres://source"},
				},
				Processor: stream.ProcessorConfig{
					Postgres: &stream.PostgresProcessorConfig{
						BatchWriter: pgprocessor.Config{URL: "postgres://target"},
					},
				},
			},
			wantNames: []string{"schema_type_compatibility", "postgres_range_type_support", "postgres_version_compatibility", "schema_extension_compatibility"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks, cleanup := BuildSchemaChecks(tc.cfg)

			require.Len(t, checks, len(tc.wantNames))
			if len(tc.wantNames) == 0 {
				require.Nil(t, cleanup)
				return
			}
			require.NotNil(t, cleanup)
			gotNames := make([]string, len(checks))
			for i, c := range checks {
				gotNames[i] = c.Name()
			}
			require.Equal(t, tc.wantNames, gotNames)
		})
	}
}

func TestBuildChecks_SelectedSchemaOnly(t *testing.T) {
	t.Parallel()

	cfg := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: "postgres://source",
			},
		},
	}

	checks, cleanup := BuildChecks(cfg, []Category{CategorySchema})

	require.NotNil(t, cleanup)
	require.Len(t, checks, 1)
	require.Equal(t, "schema_type_compatibility", checks[0].Name())
}

type sourceExtensionRow struct {
	Name   string
	Schema string
}

// extensionSource returns an AcquireFunc whose Querier serves the given
// extension rows (name, schema) from Query.
func extensionSource(t *testing.T, rows []sourceExtensionRow) postgres.AcquireFunc {
	t.Helper()
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return &mocks.Rows{
					NextFn: func(i uint) bool { return int(i) <= len(rows) },
					ScanFn: func(i uint, dest ...any) error {
						require.Len(t, dest, 2)
						name, ok := dest[0].(*string)
						require.True(t, ok)
						schema, ok := dest[1].(*string)
						require.True(t, ok)
						*name = rows[i-1].Name
						*schema = rows[i-1].Schema
						return nil
					},
					ErrFn: func() error { return nil },
				}, nil
			},
		}, nil
	}
}

// extensionTarget returns an AcquireFunc whose Querier serves the given
// extension names (single column) from Query.
func extensionTarget(t *testing.T, names []string) postgres.AcquireFunc {
	t.Helper()
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return &mocks.Rows{
					NextFn: func(i uint) bool { return int(i) <= len(names) },
					ScanFn: func(i uint, dest ...any) error {
						require.Len(t, dest, 1)
						name, ok := dest[0].(*string)
						require.True(t, ok)
						*name = names[i-1]
						return nil
					},
					ErrFn: func() error { return nil },
				}, nil
			},
		}, nil
	}
}

// sourceWithColumns returns an AcquireFunc whose Querier serves the given column
// rows from Query.
func sourceWithColumns(t *testing.T, rows []schemaColumnRow) postgres.AcquireFunc {
	t.Helper()
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryFn: func(context.Context, uint, string, ...any) (postgres.Rows, error) {
				return schemaColumnRows(t, rows), nil
			},
		}, nil
	}
}

func schemaColumnRows(t *testing.T, rows []schemaColumnRow) postgres.Rows {
	t.Helper()
	return &mocks.Rows{
		NextFn: func(i uint) bool {
			return int(i) <= len(rows)
		},
		ScanFn: func(i uint, dest ...any) error {
			require.Len(t, dest, 7)
			row := rows[i-1]
			schema, ok := dest[0].(*string)
			require.True(t, ok)
			table, ok := dest[1].(*string)
			require.True(t, ok)
			column, ok := dest[2].(*string)
			require.True(t, ok)
			baseOID, ok := dest[3].(*int64)
			require.True(t, ok)
			typeName, ok := dest[4].(*string)
			require.True(t, ok)
			typeKind, ok := dest[5].(*string)
			require.True(t, ok)
			elemTypeKind, ok := dest[6].(*string)
			require.True(t, ok)
			*schema = row.Schema
			*table = row.Table
			*column = row.Column
			*baseOID = row.BaseOID
			*typeName = row.TypeName
			*typeKind = row.TypeKind
			*elemTypeKind = row.ElemTypeKind
			return nil
		},
		ErrFn: func() error { return nil },
	}
}
