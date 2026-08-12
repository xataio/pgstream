// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestPgArrayType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		colType string
		want    string
	}{
		{"integer", "int4[]"},
		{"int4", "int4[]"},
		{"bigint", "int8[]"},
		{"int8", "int8[]"},
		{"smallint", "int2[]"},
		{"int2", "int2[]"},
		{"text", "text[]"},
		{"uuid", "uuid[]"},
		{"character varying", "text[]"},
		{"varchar", "text[]"},
		{"boolean", "boolean[]"},
	}

	for _, tc := range tests {
		t.Run(tc.colType, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, pgArrayType(tc.colType))
		})
	}
}

func TestBuildBulkDeleteQuery_SinglePK(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "users", "id", "bigint", float64(1)),
		deleteEvent("public", "users", "id", "bigint", float64(2)),
		deleteEvent("public", "users", "id", "bigint", float64(3)),
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.NoError(t, err)
	require.Len(t, queries, 1)

	q := queries[0]
	require.Contains(t, q.sql, "ANY($1::int8[])")
	require.Contains(t, q.sql, `DELETE FROM "public"."users"`)
	require.Len(t, q.args, 1)

	values, ok := q.args[0].([]any)
	require.True(t, ok)
	require.Len(t, values, 3)
}

func TestBuildBulkDeleteQuery_SinglePK_UUID(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "items", "id", "uuid", "550e8400-e29b-41d4-a716-446655440000"),
		deleteEvent("public", "items", "id", "uuid", "550e8400-e29b-41d4-a716-446655440001"),
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Contains(t, queries[0].sql, "ANY($1::uuid[])")
}

func TestBuildBulkDeleteQuery_CompositePK(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := make([]*wal.Data, 3)
	for i := range events {
		events[i] = &wal.Data{
			Action: "D",
			Schema: "public",
			Table:  "orders",
			Identity: []wal.Column{
				{Name: "user_id", Type: "bigint", Value: float64(i + 1)},
				{Name: "order_id", Type: "bigint", Value: float64((i + 1) * 10)},
			},
		}
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.NoError(t, err)
	require.Len(t, queries, 1)

	q := queries[0]
	require.Contains(t, q.sql, `("user_id","order_id") IN (SELECT * FROM unnest($1::int8[],$2::int8[]))`)
	// one array arg per pk col
	require.Len(t, q.args, 2)
	for _, arg := range q.args {
		values, ok := arg.([]any)
		require.True(t, ok)
		require.Len(t, values, 3) // one value per event
	}
}

// Identity columns typed as a user-defined enum must be bound as text[] and
// cast back on the target. pgx registers no codec for the database-specific
// enum array OID, so binding the values against $1::asset_kind[] fails to
// encode client-side with "unable to encode []interface {}{...} into text
// format for unknown type", poisoning the whole batch.
func TestBuildBulkDeleteQuery_SinglePK_Enum(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "asset", "kind", "asset_kind", "BiteScan"),
		deleteEvent("public", "asset", "kind", "asset_kind", "UpperJawScan"),
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{`"kind"`: {enumType: "public.asset_kind"}}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)

	q := queries[0]
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE "kind" = ANY($1::text[]::public.asset_kind[])`,
		q.sql)
	// the column side stays uncast so an index on it remains usable
	require.NotContains(t, q.sql, `"kind"::`)
	require.Equal(t, []any{[]any{"BiteScan", "UpperJawScan"}}, q.args)
}

// A composite PK mixing an enum column with regular ones: only the enum column
// is bound as text[], and the cast is applied inside the unnest projection.
func TestBuildBulkDeleteQuery_CompositePK_Enum(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		{
			Action: "D", Schema: "public", Table: "asset",
			Identity: []wal.Column{
				{Name: "export_id", Type: "uuid", Value: "550e8400-e29b-41d4-a716-446655440000"},
				{Name: "kind", Type: "asset_kind", Value: "BiteScan"},
				{Name: "number", Type: "integer", Value: float64(1)},
			},
		},
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{`"kind"`: {enumType: "public.asset_kind"}}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)

	q := queries[0]
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE ("export_id","kind","number") IN `+
			`(SELECT c1,c2::public.asset_kind,c3 FROM unnest($1::uuid[],$2::text[],$3::int4[]) AS u(c1,c2,c3))`,
		q.sql)
	require.Equal(t, []any{
		[]any{"550e8400-e29b-41d4-a716-446655440000"},
		[]any{"BiteScan"},
		[]any{float64(1)},
	}, q.args)
}

// An array-of-enum identity column is reported in enumColumns too, and wal2json
// delivers its value as a postgres array literal, so the same text[] binding
// works with the array type as the cast target. It needs IN (SELECT ...) rather
// than = ANY(ARRAY(...)), which unwraps an array level and leaves postgres
// comparing asset_kind[] against asset_kind.
func TestBuildBulkDeleteQuery_SinglePK_EnumArray(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "asset", "kinds", "asset_kind[]", "{BiteScan}"),
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{`"kinds"`: {enumType: "public.asset_kind", isArray: true}}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE "kinds" IN (SELECT unnest($1::text[])::public.asset_kind[])`,
		queries[0].sql)
	require.Equal(t, []any{[]any{"{BiteScan}"}}, queries[0].args)
}

// A domain over an enum needs the *column* side cast as well: the enum
// comparison operators are polymorphic over anyenum, which does not accept a
// domain, so postgres rejects `my_domain = my_enum` with "operator does not
// exist". Casting the column forfeits an index on it, but no index-friendly
// form exists — and without this, every shape of delete on such a column fails,
// including the per-row form that predates bulk coalescing.
func TestBuildBulkDeleteQuery_SinglePK_DomainOverEnum(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "asset", "kind", "kind_dom", "BiteScan"),
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{
		`"kind"`: {enumType: "public.asset_kind", isDomain: true},
	}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE "kind"::public.asset_kind = ANY($1::text[]::public.asset_kind[])`,
		queries[0].sql)
}

// A domain over an array of enums compares as a whole array, like a plain enum
// array — the column side needs no cast because the comparison is against
// my_enum[] rather than the polymorphic anyenum.
func TestBuildBulkDeleteQuery_SinglePK_DomainOverEnumArray(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "asset", "kinds", "kind_arr_dom", "{BiteScan}"),
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{
		`"kinds"`: {enumType: "public.asset_kind", isArray: true, isDomain: true},
	}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE "kinds" IN (SELECT unnest($1::text[])::public.asset_kind[])`,
		queries[0].sql)
}

// The composite path casts the column side for a domain too, inside the row
// constructor.
func TestBuildBulkDeleteQuery_CompositePK_DomainOverEnum(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		{
			Action: "D", Schema: "public", Table: "asset",
			Identity: []wal.Column{
				{Name: "id", Type: "integer", Value: float64(1)},
				{Name: "kind", Type: "kind_dom", Value: "BiteScan"},
			},
		},
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{
		`"kind"`: {enumType: "public.asset_kind", isDomain: true},
	}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE ("id","kind"::public.asset_kind) IN `+
			`(SELECT c1,c2::public.asset_kind FROM unnest($1::int4[],$2::text[]) AS u(c1,c2))`,
		queries[0].sql)
}

// The cast target comes from the target catalog, never from the column type
// carried by the replication stream — which for a kafka source is unvalidated
// input. A hostile type string must not reach the generated SQL.
func TestBuildBulkDeleteQuery_CastTargetIgnoresStreamType(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "asset", "kind", `asset_kind)) OR true --`, "BiteScan"),
	}
	si := schemaInfo{enumColumns: map[string]enumColumn{
		`"kind"`: {enumType: "public.asset_kind"},
	}}

	queries, err := adapter.buildBulkDeleteQuery(events, si)
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Equal(t,
		`DELETE FROM "public"."asset" WHERE "kind" = ANY($1::text[]::public.asset_kind[])`,
		queries[0].sql)
	require.NotContains(t, queries[0].sql, "OR true")
}

// Tables with no enum identity columns must emit exactly the SQL they did
// before: no unnest aliases, no casts.
func TestBuildBulkDeleteQuery_CompositePK_NoEnumUnchanged(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		{
			Action: "D", Schema: "public", Table: "orders",
			Identity: []wal.Column{
				{Name: "user_id", Type: "bigint", Value: float64(1)},
				{Name: "order_id", Type: "bigint", Value: float64(10)},
			},
		},
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{enumColumns: map[string]enumColumn{`"other"`: {enumType: "public.asset_kind"}}})
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Equal(t,
		`DELETE FROM "public"."orders" WHERE ("user_id","order_id") IN (SELECT * FROM unnest($1::int8[],$2::int8[]))`,
		queries[0].sql)
}

// A large composite-PK delete must stay a single query with a constant
// parameter count (one array per PK column). The previous row-constructor IN
// form produced a per-tuple OR tree that overflowed the target's
// max_stack_depth (SQLSTATE 54001) well before the param cap; the unnest form
// has no such limit, so no splitting is needed.
func TestBuildBulkDeleteQuery_CompositePK_LargeBatch(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	numPKCols := 3
	// far more tuples than the ~9-10k that overflowed the old OR-tree form
	numEvents := 50000
	events := make([]*wal.Data, numEvents)
	for i := range events {
		events[i] = &wal.Data{
			Action: "D",
			Schema: "public",
			Table:  "t",
			Identity: []wal.Column{
				{Name: "a", Type: "bigint", Value: float64(i)},
				{Name: "b", Type: "bigint", Value: float64(i * 10)},
				{Name: "c", Type: "bigint", Value: float64(i * 100)},
			},
		}
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.NoError(t, err)
	require.Len(t, queries, 1, "unnest form needs no splitting")

	q := queries[0]
	require.Contains(t, q.sql, "unnest($1::int8[],$2::int8[],$3::int8[])")
	// constant param count: one array per PK column, regardless of row count
	require.Len(t, q.args, numPKCols)
	for _, arg := range q.args {
		values, ok := arg.([]any)
		require.True(t, ok)
		require.Len(t, values, numEvents) // every event covered
	}
}

func TestBuildBulkDeleteQuery_NullIdentity(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "t", "id", "bigint", float64(1)),
		deleteEvent("public", "t", "id", "bigint", nil), // null PK
		deleteEvent("public", "t", "id", "bigint", float64(3)),
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.NoError(t, err)

	// should have 2 queries: 1 bulk for non-null, 1 individual for null
	require.Len(t, queries, 2)

	// the individual null query
	var nullQuery *query
	var bulkQuery *query
	for _, q := range queries {
		if strings.Contains(q.sql, "IS NULL") {
			nullQuery = q
		} else {
			bulkQuery = q
		}
	}

	require.NotNil(t, nullQuery, "expected a query with IS NULL")
	require.NotNil(t, bulkQuery, "expected a bulk query with ANY")
	require.Contains(t, bulkQuery.sql, "ANY")
}

func TestBuildBulkDeleteQuery_NoIdentity(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		{
			Action: "D",
			Schema: "public",
			Table:  "t",
			// no Identity and no InternalColIDs
		},
	}

	_, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.Error(t, err)
}

func TestBuildBulkDeleteQuery_Empty(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	queries, err := adapter.buildBulkDeleteQuery(nil, schemaInfo{})
	require.NoError(t, err)
	require.Nil(t, queries)
}

func TestBuildBulkDeleteQuery_InternalColIDs(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		{
			Action: "D",
			Schema: "public",
			Table:  "t",
			Columns: []wal.Column{
				{ID: "col1", Name: "id", Type: "bigint", Value: float64(1)},
				{ID: "col2", Name: "name", Type: "text", Value: "alice"},
			},
			Metadata: wal.Metadata{InternalColIDs: []string{"col1"}},
		},
		{
			Action: "D",
			Schema: "public",
			Table:  "t",
			Columns: []wal.Column{
				{ID: "col1", Name: "id", Type: "bigint", Value: float64(2)},
				{ID: "col2", Name: "name", Type: "text", Value: "bob"},
			},
			Metadata: wal.Metadata{InternalColIDs: []string{"col1"}},
		},
	}

	queries, err := adapter.buildBulkDeleteQuery(events, schemaInfo{})
	require.NoError(t, err)
	require.Len(t, queries, 1)
	require.Contains(t, queries[0].sql, "ANY")
}

func TestBuildBulkInsertQueries(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)
	si := schemaInfo{
		generatedColumns: map[string]struct{}{},
		sequenceColumns:  map[string]string{},
	}

	events := []*wal.Data{
		{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Type: "bigint", Value: float64(1)},
				{Name: "name", Type: "text", Value: "alice"},
			},
		},
		{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Type: "bigint", Value: float64(2)},
				{Name: "name", Type: "text", Value: "bob"},
			},
		},
	}

	queries := adapter.buildBulkInsertQueries(events, si)
	require.Len(t, queries, 1)

	q := queries[0]
	require.Contains(t, q.sql, "INSERT INTO")
	require.Contains(t, q.sql, "OVERRIDING SYSTEM VALUE")
	require.Contains(t, q.sql, "VALUES")
	// 2 rows * 2 cols = 4 args
	require.Len(t, q.args, 4)
	// should have two value tuples
	require.Equal(t, 2, strings.Count(q.sql, "($"))
}

// TestBuildBulkInsertQueries_NeedsTextCopy pins the COPY-format decision on the
// bulk path: a batch touching a user-defined enum column must be routed to
// text-format COPY, since pgx has no binary codec for the enum's OID.
func TestBuildBulkInsertQueries_NeedsTextCopy(t *testing.T) {
	t.Parallel()

	newEvents := func(moodType string) []*wal.Data {
		return []*wal.Data{
			{
				Action: "I",
				Schema: "public",
				Table:  "users",
				Columns: []wal.Column{
					{Name: "id", Type: "bigint", Value: float64(1)},
					{Name: "mood", Type: moodType, Value: "happy"},
				},
			},
		}
	}

	tests := []struct {
		name        string
		events      []*wal.Data
		enumColumns map[string]enumColumn

		wantNeedsTextCopy bool
	}{
		{
			name:              "no enum columns",
			events:            newEvents("text"),
			enumColumns:       nil,
			wantNeedsTextCopy: false,
		},
		{
			name:              "enum column in the batch",
			events:            newEvents("mood"),
			enumColumns:       map[string]enumColumn{`"mood"`: {enumType: "public.mood"}},
			wantNeedsTextCopy: true,
		},
		{
			name:              "enum column of another table only",
			events:            newEvents("text"),
			enumColumns:       map[string]enumColumn{`"status"`: {enumType: "public.status"}},
			wantNeedsTextCopy: false,
		},
		{
			name:              "static text-only type",
			events:            newEvents("ltree"),
			enumColumns:       nil,
			wantNeedsTextCopy: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			queries := newTestDMLAdapter(t).buildBulkInsertQueries(tc.events, schemaInfo{
				generatedColumns: map[string]struct{}{},
				sequenceColumns:  map[string]string{},
				enumColumns:      tc.enumColumns,
			})
			require.Len(t, queries, 1)
			require.Equal(t, tc.wantNeedsTextCopy, queries[0].needsTextCopy)
		})
	}
}

func TestBuildBulkInsertQueries_WithSequence(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)
	si := schemaInfo{
		generatedColumns: map[string]struct{}{},
		sequenceColumns: map[string]string{
			`"id"`: "users_id_seq",
		},
	}

	events := []*wal.Data{
		{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Type: "bigint", Value: float64(5)},
				{Name: "name", Type: "text", Value: "alice"},
			},
		},
		{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Type: "bigint", Value: float64(10)},
				{Name: "name", Type: "text", Value: "bob"},
			},
		},
		{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{Name: "id", Type: "bigint", Value: float64(3)},
				{Name: "name", Type: "text", Value: "charlie"},
			},
		},
	}

	queries := adapter.buildBulkInsertQueries(events, si)
	// 1 INSERT + 1 setval
	require.Len(t, queries, 2)

	// the setval should use the max value (10)
	setvalQuery := queries[1]
	require.Equal(t, "SELECT setval($1::regclass, $2::bigint, true)", setvalQuery.sql)
	require.Equal(t, []any{"users_id_seq", int64(10)}, setvalQuery.args)
}

func TestBuildBulkInsertQueries_WithGeneratedColumns(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)
	si := schemaInfo{
		generatedColumns: map[string]struct{}{`"gen_col"`: {}},
		sequenceColumns:  map[string]string{},
	}

	events := []*wal.Data{
		{
			Action: "I",
			Schema: "public",
			Table:  "t",
			Columns: []wal.Column{
				{Name: "id", Type: "bigint", Value: float64(1)},
				{Name: "gen_col", Type: "text", Value: "generated"},
				{Name: "name", Type: "text", Value: "alice"},
			},
		},
	}

	queries := adapter.buildBulkInsertQueries(events, si)
	require.Len(t, queries, 1)

	q := queries[0]
	require.NotContains(t, q.sql, "gen_col")
	// only 2 args (id, name), not 3
	require.Len(t, q.args, 2)
}

func TestBuildBulkInsertQueries_OnConflictUpdate(t *testing.T) {
	t.Parallel()

	a, err := newDMLAdapter("update", false, loglib.NewNoopLogger())
	require.NoError(t, err)

	si := schemaInfo{
		generatedColumns: map[string]struct{}{},
		sequenceColumns:  map[string]string{},
	}

	events := []*wal.Data{
		{
			Action: "I",
			Schema: "public",
			Table:  "users",
			Columns: []wal.Column{
				{ID: "col1", Name: "id", Type: "bigint", Value: float64(1)},
				{ID: "col2", Name: "name", Type: "text", Value: "alice"},
			},
			Metadata: wal.Metadata{InternalColIDs: []string{"col1"}},
		},
	}

	queries := a.buildBulkInsertQueries(events, si)
	require.Len(t, queries, 1)
	require.Contains(t, queries[0].sql, "ON CONFLICT")
	require.Contains(t, queries[0].sql, "DO UPDATE SET")
}

func TestBuildBulkInsertQueries_Empty(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)
	queries := adapter.buildBulkInsertQueries(nil, schemaInfo{})
	require.Nil(t, queries)
}

func TestBuildBulkInsertQueries_SplitAtLimit(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)
	si := schemaInfo{
		generatedColumns: map[string]struct{}{},
		sequenceColumns:  map[string]string{},
	}

	numCols := 3
	numEvents := (maxParamsPerQuery / numCols) + 10
	events := make([]*wal.Data, numEvents)
	for i := range events {
		events[i] = &wal.Data{
			Action: "I",
			Schema: "public",
			Table:  "t",
			Columns: []wal.Column{
				{Name: "a", Type: "bigint", Value: float64(i)},
				{Name: "b", Type: "text", Value: fmt.Sprintf("val_%d", i)},
				{Name: "c", Type: "bigint", Value: float64(i * 10)},
			},
		}
	}

	queries := adapter.buildBulkInsertQueries(events, si)
	require.Greater(t, len(queries), 1, "should split into multiple INSERT queries")
}

func TestBuildBulkInsertQueries_TypesIntRangeArgs(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)
	events := []*wal.Data{
		{
			Action: "I",
			Schema: "public",
			Table:  "range_example",
			Columns: []wal.Column{
				{Name: "id", Type: "int4", Value: int32(1)},
				{
					Name: "small_range",
					Type: "int4range",
					Value: pgtype.Range[any]{
						Lower:     int64(7),
						Upper:     int64(11),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					},
				},
				{
					Name: "large_range",
					Type: "int8range",
					Value: pgtype.Range[any]{
						Lower:     int64(13275),
						Upper:     int64(13279),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					},
				},
			},
		},
	}

	queries := adapter.buildBulkInsertQueries(events, schemaInfo{})
	require.Len(t, queries, 1)
	require.IsType(t, pgtype.Range[int32]{}, queries[0].args[1])
	require.Equal(t, pgtype.Range[int32]{
		Lower:     7,
		Upper:     11,
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}, queries[0].args[1])
	require.IsType(t, pgtype.Range[int64]{}, queries[0].args[2])
	require.Equal(t, pgtype.Range[int64]{
		Lower:     13275,
		Upper:     13279,
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}, queries[0].args[2])
}

func deleteEvent(schema, table, colName, colType string, colValue any) *wal.Data {
	return &wal.Data{
		Action: "D",
		Schema: schema,
		Table:  table,
		Identity: []wal.Column{
			{Name: colName, Type: colType, Value: colValue},
		},
	}
}

func newTestDMLAdapter(t *testing.T) *dmlAdapter {
	t.Helper()
	a, err := newDMLAdapter("", false, loglib.NewNoopLogger())
	require.NoError(t, err)
	return a
}

// newTestDMLAdapterForCopy returns an adapter in bulk-COPY mode, where values
// are rewritten for the COPY encoder.
func newTestDMLAdapterForCopy(t *testing.T) *dmlAdapter {
	t.Helper()
	a, err := newDMLAdapter("", true, loglib.NewNoopLogger())
	require.NoError(t, err)
	return a
}
