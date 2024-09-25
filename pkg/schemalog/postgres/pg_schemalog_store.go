// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"
)

// Store is a postgres implementation of the schemalog.Store interface
type Store struct {
	querier pglib.Querier
}

type Config struct {
	URL string
}

func NewStore(ctx context.Context, cfg Config) (*Store, error) {
	pool, err := pglib.NewConnPool(ctx, cfg.URL)
	if err != nil {
		return nil, err
	}
	return &Store{
		querier: pool,
	}, nil
}

func NewStoreWithQuerier(querier pglib.Querier) *Store {
	return &Store{
		querier: querier,
	}
}

// Insert inserts a new schema log for the schema name on input. It will insert
// a new row with the latest schema view with an incremented version, or if no
// row exists for the schema, with version 1.
func (s *Store) Insert(ctx context.Context, schemaName string) (*schemalog.LogEntry, error) {
	l := &schemalog.LogEntry{}
	err := s.querier.ExecInTx(ctx, func(tx pglib.Tx) error {
		nextVersionQuery := fmt.Sprintf(`select coalesce((select version+1 from %s where schema_name = $1 order by version desc limit 1), 1)`, s.table())
		var nextVersion int
		err := tx.QueryRow(ctx, nextVersionQuery, schemaName).Scan(&nextVersion)
		if err != nil {
			return fmt.Errorf("query for next schema log version: %w", err)
		}

		insertQuery := fmt.Sprintf(`insert into %s (version, schema_name, schema) values ($1, $2, pgstream.get_schema($2))
	returning id, version, schema_name, schema, created_at, acked`, s.table())
		if err := tx.QueryRow(ctx, insertQuery, nextVersion, schemaName).Scan(&l.ID, &l.Version, &l.SchemaName, &l.Schema, &l.CreatedAt, &l.Acked); err != nil {
			return mapError(fmt.Errorf("error inserting schema log for schema %s: %w", schemaName, err))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return l, nil
}

// Fetch retrieves the latest schema log entry for schemaName. If ackedOnly is set, the function returns the last acked
// entry. If ackedOnly is NOT set, the function returns the latest entry no matter the acked flag.
func (s *Store) Fetch(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
	ackCondition := ""
	if ackedOnly {
		ackCondition = "and acked"
	}
	sql := fmt.Sprintf(`select id, version, schema_name, schema, created_at, acked from %s where schema_name = $1 %s order by version desc limit 1`, s.table(), ackCondition)

	l := &schemalog.LogEntry{}
	err := s.querier.QueryRow(ctx, sql, schemaName).Scan(&l.ID, &l.Version, &l.SchemaName, &l.Schema, &l.CreatedAt, &l.Acked)
	if err != nil {
		return nil, mapError(fmt.Errorf("error fetching schema log for schema %s: %w", schemaName, err))
	}

	return l, nil
}

func (s *Store) Ack(ctx context.Context, logEntry *schemalog.LogEntry) error {
	sql := fmt.Sprintf(`update %s set acked = true where id = $1 and schema_name = $2`, s.table())
	_, err := s.querier.Exec(ctx, sql, logEntry.ID.String(), logEntry.SchemaName)
	if err != nil {
		return mapError(fmt.Errorf("failed to ack schema log (%s) for schema (%s) version (%d): %w", logEntry.ID, logEntry.SchemaName, logEntry.Version, err))
	}

	return nil
}

func (s *Store) Close() error {
	s.querier.Close(context.Background())
	return nil
}

func (s *Store) table() string {
	return fmt.Sprintf("%q.%q", schemalog.SchemaName, schemalog.TableName)
}

func mapError(err error) error {
	if errors.Is(err, pglib.ErrNoRows) {
		return schemalog.ErrNoRows
	}
	return err
}
