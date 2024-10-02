// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/snapshot"
)

type SnapshotGenerator struct {
	conn           postgres.Querier
	schemalogStore schemalogStore
	batchSize      uint
}

type schemalogStore interface {
	Insert(ctx context.Context, schemaName string) (*schemalog.LogEntry, error)
}

var errInvalidSnapshot = errors.New("invalid snapshot details")

func New(ctx context.Context, cfg *Config, schemalogStore schemalogStore) (*SnapshotGenerator, error) {
	conn, err := postgres.NewConnPool(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	return &SnapshotGenerator{
		batchSize:      cfg.batchSize(),
		conn:           conn,
		schemalogStore: schemalogStore,
	}, nil
}

func (sg *SnapshotGenerator) CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) error {
	if !snapshot.IsValid() {
		return errInvalidSnapshot
	}

	// make sure there's a schema entry in the schema log store before
	// triggering the snapshot
	if _, err := sg.schemalogStore.Insert(ctx, snapshot.TableName); err != nil {
		return fmt.Errorf("ensuring schemalog: %w", err)
	}

	return sg.run(ctx, snapshot)
}

func (sg *SnapshotGenerator) Close(ctx context.Context) error {
	return sg.conn.Close(ctx)
}

func (sg *SnapshotGenerator) run(ctx context.Context, snapshot *snapshot.Snapshot) error {
	var lastValue string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var err error
			lastValue, err = sg.updateBatch(ctx, snapshot, lastValue)
			if err != nil {
				if errors.Is(err, postgres.ErrNoRows) {
					return nil
				}
				return fmt.Errorf("error updating batch: %w", err)
			}
		}
	}
}

func (sg *SnapshotGenerator) updateBatch(ctx context.Context, snapshot *snapshot.Snapshot, lastValue string) (string, error) {
	query := sg.buildBatchQuery(snapshot, lastValue)
	err := sg.conn.QueryRow(ctx, query).Scan(&lastValue)
	return lastValue, err
}

// buildBatchQuery builds the query used to update the next batch of rows.
func (sg *SnapshotGenerator) buildBatchQuery(snapshot *snapshot.Snapshot, lastValue string) string {
	identityName := snapshot.IdentityColumnNames[0]
	whereClause := ""
	if lastValue != "" {
		whereClause = fmt.Sprintf("WHERE %s > %v", pq.QuoteIdentifier(identityName), pq.QuoteLiteral(lastValue))
	}

	return fmt.Sprintf(`
    WITH batch AS (
      SELECT %[1]s FROM %[2]s %[4]s ORDER BY %[1]s LIMIT %[3]d FOR NO KEY UPDATE
    ), update AS (
      UPDATE %[2]s SET %[1]s=%[2]s.%[1]s FROM batch WHERE %[2]s.%[1]s = batch.%[1]s RETURNING %[2]s.%[1]s
    )
    SELECT LAST_VALUE(%[1]s) OVER() FROM update
    `,
		pq.QuoteIdentifier(identityName),
		sg.table(snapshot),
		sg.batchSize,
		whereClause)
}

func (sg *SnapshotGenerator) table(snapshot *snapshot.Snapshot) string {
	return fmt.Sprintf("%s.%s", pq.QuoteIdentifier(snapshot.SchemaName), pq.QuoteIdentifier(snapshot.TableName))
}
