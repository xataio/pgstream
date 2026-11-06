// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

// TestDMLAdapter_onConflictUpdate_alwaysIdentityColumn pins that the upsert SET
// list never assigns a GENERATED ALWAYS AS IDENTITY column. Postgres rejects
// such an assignment with `column "id" can only be updated to DEFAULT`:
// OVERRIDING SYSTEM VALUE covers the inserted values, not the ON CONFLICT SET
// list.
//
// The schema metadata reaches the adapter in two ways, and both must produce a
// valid statement: from a catalog lookup (cache miss) and from a DDL event.
func TestDMLAdapter_onConflictUpdate_alwaysIdentityColumn(t *testing.T) {
	t.Parallel()

	const (
		schemaName = "test_schema"
		tableName  = "test_table"
	)

	newInsert := func(id int64, name string) *wal.Data {
		return &wal.Data{
			Action: "I",
			Schema: schemaName,
			Table:  tableName,
			Columns: []wal.Column{
				{ID: "table-1", Name: "id", Type: "int8", Value: id},
				{ID: "table-2", Name: "name", Type: "text", Value: name},
			},
			Metadata: wal.Metadata{
				InternalColIDs: []string{"table-1"},
			},
		}
	}

	always := "ALWAYS"
	schemaInfos := map[string]func(t *testing.T) schemaInfo{
		"schema from catalog lookup": func(*testing.T) schemaInfo {
			// the catalog queries report an identity column as always-identity
			// only, never as generated
			return schemaInfo{
				alwaysIdentityColumns: map[string]struct{}{`"id"`: {}},
			}
		},
		"schema from DDL event": func(t *testing.T) schemaInfo {
			return schemaInfoFromDDLEvent(t, schemaName, tableName, []wal.DDLColumn{
				{Attnum: 1, Name: "id", Type: "bigint", Identity: &always},
				{Attnum: 2, Name: "name", Type: "text", Nullable: true},
			})
		},
	}

	builders := map[string]func(t *testing.T, a *dmlAdapter, si schemaInfo) []*query{
		"single row insert": func(t *testing.T, a *dmlAdapter, si schemaInfo) []*query {
			queries, err := a.walDataToQueries(newInsert(1, "alice"), si)
			require.NoError(t, err)
			return queries
		},
		"bulk insert": func(_ *testing.T, a *dmlAdapter, si schemaInfo) []*query {
			return a.buildBulkInsertQueries([]*wal.Data{newInsert(1, "alice"), newInsert(2, "bob")}, si)
		},
	}

	for infoName, newSchemaInfo := range schemaInfos {
		for builderName, build := range builders {
			t.Run(infoName+"/"+builderName, func(t *testing.T) {
				t.Parallel()

				adapter := &dmlAdapter{
					logger:           loglib.NewNoopLogger(),
					onConflictAction: onConflictUpdate,
					pgTypeMap:        pgtype.NewMap(),
				}

				queries := build(t, adapter, newSchemaInfo(t))
				require.NotEmpty(t, queries)

				insert := queries[0].sql
				require.Contains(t, insert, `ON CONFLICT ("id") DO UPDATE SET`)
				require.Contains(t, insert, `"name" = EXCLUDED."name"`)
				require.NotContains(t, insert, `"id" = EXCLUDED."id"`,
					"postgres rejects an explicit value for an always-identity column in the ON CONFLICT SET list")
			})
		}
	}
}

// schemaInfoFromDDLEvent returns the schema metadata the observer serves for a
// table it only knows from a DDL event.
func schemaInfoFromDDLEvent(t *testing.T, schemaName, tableName string, columns []wal.DDLColumn) schemaInfo {
	t.Helper()

	observer := &pgSchemaObserver{
		logger: loglib.NewNoopLogger(),
		// the DDL event does not carry enum membership, so that lookup is the
		// only one that still queries postgres
		pgConn: &pgmocks.Querier{
			QueryFn: func(_ context.Context, _ uint, query string, _ ...any) (pglib.Rows, error) {
				require.Equal(t, enumTableColumnsQuery, query)
				return &pgmocks.Rows{
					CloseFn: func() {},
					NextFn:  func(uint) bool { return false },
					ErrFn:   func() error { return nil },
				}, nil
			},
		},
		generatedTableColumns:      synclib.NewMap[string, map[string]struct{}](),
		alwaysIdentityTableColumns: synclib.NewMap[string, map[string]struct{}](),
		materializedViews:          synclib.NewMap[string, map[string]struct{}](),
		columnTableSequences:       synclib.NewMap[string, map[string]string](),
		enumTableColumns:           synclib.NewMap[string, map[string]enumColumn](),
	}

	observer.update(&wal.DDLEvent{
		DDL:        "CREATE TABLE " + schemaName + "." + tableName,
		SchemaName: schemaName,
		CommandTag: "CREATE TABLE",
		Objects: []wal.DDLObject{
			{
				Type:              "table",
				Identity:          schemaName + "." + tableName,
				Schema:            schemaName,
				Columns:           columns,
				PrimaryKeyColumns: []string{"id"},
			},
		},
	})

	info, err := observer.getSchemaInfo(context.Background(), schemaName, tableName)
	require.NoError(t, err)
	return info
}
