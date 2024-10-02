// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/postgres"
	postgresmocks "github.com/xataio/pgstream/internal/postgres/mocks"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogmocks "github.com/xataio/pgstream/pkg/schemalog/mocks"
	"github.com/xataio/pgstream/pkg/snapshot"
)

func TestSnapshotGenerator_CreateSnapshot(t *testing.T) {
	t.Parallel()

	testBatchQuery := func(whereQuery string) string {
		return fmt.Sprintf(`
    WITH batch AS (
      SELECT "id" FROM "test-schema"."test-table" %s ORDER BY "id" LIMIT 10 FOR NO KEY UPDATE
    ), update AS (
      UPDATE "test-schema"."test-table" SET "id"="test-schema"."test-table"."id" FROM batch WHERE "test-schema"."test-table"."id" = batch."id" RETURNING "test-schema"."test-table"."id"
    )
    SELECT LAST_VALUE("id") OVER() FROM update
    `, whereQuery)
	}

	testSnapshot := &snapshot.Snapshot{
		SchemaName:          "test-schema",
		TableName:           "test-table",
		IdentityColumnNames: []string{"id"},
	}

	errTest := errors.New("oh noes")

	tests := []struct {
		name           string
		querier        postgres.Querier
		schemalogStore schemalogStore
		snapshot       *snapshot.Snapshot

		wantErr error
	}{
		{
			name: "ok - nothing to snapshot",
			querier: &postgresmocks.Querier{
				QueryRowFn: func(ctx context.Context, _ uint, query string, args ...any) postgres.Row {
					wantQuery := testBatchQuery("")
					require.Equal(t, wantQuery, query)
					require.Empty(t, args)
					return &postgresmocks.Row{
						ScanFn: func(_ uint, args ...any) error { return postgres.ErrNoRows },
					}
				},
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) { return nil, nil },
			},
			snapshot: testSnapshot,

			wantErr: nil,
		},
		{
			name: "ok - multiple batches",
			querier: &postgresmocks.Querier{
				QueryRowFn: func(ctx context.Context, i uint, query string, args ...any) postgres.Row {
					var wantQuery string
					switch i {
					case 1:
						wantQuery = testBatchQuery("")
						require.Equal(t, wantQuery, query)
						require.Empty(t, args)
						return &postgresmocks.Row{
							ScanFn: func(_ uint, args ...any) error {
								require.Len(t, args, 1)
								lastValue, ok := args[0].(*string)
								require.True(t, ok, "type is %T", args[0])
								*lastValue = "10"
								return nil
							},
						}
					case 2:
						wantQuery = testBatchQuery(`WHERE "id" > '10'`)
						require.Equal(t, wantQuery, query)
						require.Empty(t, args)
						return &postgresmocks.Row{
							ScanFn: func(_ uint, args ...any) error {
								require.Len(t, args, 1)
								lastValue, ok := args[0].(*string)
								require.True(t, ok, "type is %T", args[0])
								*lastValue = "20"
								return nil
							},
						}
					case 3:
						wantQuery = testBatchQuery(`WHERE "id" > '20'`)
						require.Equal(t, wantQuery, query)
						require.Empty(t, args)
						return &postgresmocks.Row{
							ScanFn: func(_ uint, args ...any) error {
								return postgres.ErrNoRows
							},
						}
					default:
						return &postgresmocks.Row{
							ScanFn: func(_ uint, args ...any) error { return fmt.Errorf("unexpected call to query row: %d", i) },
						}
					}
				},
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) { return nil, nil },
			},
			snapshot: testSnapshot,

			wantErr: nil,
		},
		{
			name: "error - invalid snapshot",
			querier: &postgresmocks.Querier{
				QueryRowFn: func(ctx context.Context, _ uint, query string, args ...any) postgres.Row {
					return &postgresmocks.Row{
						ScanFn: func(_ uint, args ...any) error { return errors.New("QueryRowFn: should not be called") },
					}
				},
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					return nil, errors.New("InsertFn: should not be called")
				},
			},
			snapshot: &snapshot.Snapshot{},

			wantErr: errInvalidSnapshot,
		},
		{
			name: "error - inserting schema log",
			querier: &postgresmocks.Querier{
				QueryRowFn: func(ctx context.Context, _ uint, query string, args ...any) postgres.Row {
					return &postgresmocks.Row{
						ScanFn: func(_ uint, args ...any) error { return errors.New("QueryRowFn: should not be called") },
					}
				},
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
					return nil, errTest
				},
			},
			snapshot: testSnapshot,

			wantErr: errTest,
		},
		{
			name: "error - updating batch",
			querier: &postgresmocks.Querier{
				QueryRowFn: func(ctx context.Context, _ uint, query string, args ...any) postgres.Row {
					wantQuery := testBatchQuery("")
					require.Equal(t, wantQuery, query)
					require.Empty(t, args)
					return &postgresmocks.Row{
						ScanFn: func(_ uint, args ...any) error { return errTest },
					}
				},
			},
			schemalogStore: &schemalogmocks.Store{
				InsertFn: func(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) { return nil, nil },
			},
			snapshot: testSnapshot,

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			generator := SnapshotGenerator{
				batchSize:      10,
				conn:           tc.querier,
				schemalogStore: tc.schemalogStore,
			}
			err := generator.CreateSnapshot(context.Background(), tc.snapshot)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
