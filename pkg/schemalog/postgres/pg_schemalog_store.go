// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/schemalog"

	"github.com/jackc/pgx/v5"
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

// Fetch retrieves the latest schema log entry for schemaName. If ackedOnly is set, the function returns the last acked
// entry. If ackedOnly is NOT set, the function returns the latest entry no matter the acked flag.
func (s *Store) Fetch(ctx context.Context, schemaName string, ackedOnly bool) (*schemalog.LogEntry, error) {
	ackCondition := ""
	if ackedOnly {
		ackCondition = "and acked"
	}
	sql := fmt.Sprintf(`select id, version, schema_name, schema, created_at, acked from %s.%s where schema_name = $1 %s order by version desc limit 1`, schemalog.SchemaName, schemalog.TableName, ackCondition)

	l := &schemalog.LogEntry{}
	if err := s.querier.QueryRow(ctx, sql, schemaName).Scan(&l.ID, &l.Version, &l.SchemaName, &l.Schema, &l.CreatedAt, &l.Acked); err != nil {
		return nil, mapError(fmt.Errorf("error fetching schema log for schema %s: %w", schemaName, err))
	}

	return l, nil
}

func (s *Store) Ack(ctx context.Context, logEntry *schemalog.LogEntry) error {
	sql := fmt.Sprintf(`update %s.%s set acked = true where id = $1 and schema_name = $2`, schemalog.SchemaName, schemalog.TableName)
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

func mapError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return schemalog.ErrNoRows
	}
	return err
}
