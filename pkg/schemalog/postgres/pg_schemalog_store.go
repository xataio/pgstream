// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/xataio/pgstream/pkg/schemalog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Store is a postgres implementation of the schemalog.Store interface
type Store struct {
	querier querier
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (tag pgconn.CommandTag, err error)
	Close(ctx context.Context)
}

func NewStore(ctx context.Context, cfg *pgx.ConnConfig) (*Store, error) {
	pgConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres client: %w", err)
	}
	return &Store{
		querier: &pgxConn{Conn: pgConn},
	}, nil
}

func NewStoreWithQuerier(querier querier) *Store {
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
		log.Ctx(ctx).Warn().Err(err).Msgf("error fetching schema log for schema %s", schemaName)
		return nil, mapError(err)
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

// pgxConn is a wrapper around the pgx.Conn to implement the same interface all
// other pg packages use for pool/connections
type pgxConn struct {
	*pgx.Conn
}

func (c *pgxConn) Close(ctx context.Context) {
	c.Conn.Close(ctx)
}

func mapError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return schemalog.ErrNoRows
	}
	return err
}
