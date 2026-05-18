// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"
	"testing"

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

	queries, err := adapter.buildBulkDeleteQuery(events)
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

	queries, err := adapter.buildBulkDeleteQuery(events)
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

	queries, err := adapter.buildBulkDeleteQuery(events)
	require.NoError(t, err)
	require.Len(t, queries, 1)

	q := queries[0]
	require.Contains(t, q.sql, "IN")
	require.Contains(t, q.sql, `("user_id","order_id")`)
	// 3 events * 2 pk cols = 6 params
	require.Len(t, q.args, 6)
}

func TestBuildBulkDeleteQuery_CompositePK_SplitAtLimit(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	numPKCols := 3
	// create enough events to exceed maxParamsPerQuery
	numEvents := (maxParamsPerQuery / numPKCols) + 10
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

	queries, err := adapter.buildBulkDeleteQuery(events)
	require.NoError(t, err)
	require.Greater(t, len(queries), 1, "should split into multiple queries")

	// verify all events are covered
	totalParams := 0
	for _, q := range queries {
		totalParams += len(q.args)
	}
	require.Equal(t, numEvents*numPKCols, totalParams)
}

func TestBuildBulkDeleteQuery_NullIdentity(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	events := []*wal.Data{
		deleteEvent("public", "t", "id", "bigint", float64(1)),
		deleteEvent("public", "t", "id", "bigint", nil), // null PK
		deleteEvent("public", "t", "id", "bigint", float64(3)),
	}

	queries, err := adapter.buildBulkDeleteQuery(events)
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

	_, err := adapter.buildBulkDeleteQuery(events)
	require.Error(t, err)
}

func TestBuildBulkDeleteQuery_Empty(t *testing.T) {
	t.Parallel()

	adapter := newTestDMLAdapter(t)

	queries, err := adapter.buildBulkDeleteQuery(nil)
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

	queries, err := adapter.buildBulkDeleteQuery(events)
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
