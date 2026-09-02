// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
)

// tableReader abstracts the strategy used to read a schema's tables during a
// data snapshot. It does not read tables itself: it opens a read session per
// schema, and the session does the reading. This keeps the per-schema state a
// strategy needs (a shared transaction snapshot, in the ctid reader's case)
// inside the strategy that understands it, instead of travelling through the
// snapshot generator.
type tableReader interface {
	// beginSchema opens a read session for the given schema tables and invokes
	// fn with it. The session, and any resource backing it, is only valid for
	// the duration of fn.
	beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, readSession) error) error
}

// readSession reads the tables of a single schema. It is created by a
// tableReader in beginSchema, already bound to the schema it reads and to
// whatever per-schema state its strategy needs, and it is only valid for the
// duration of the beginSchema callback that produced it.
type readSession interface {
	// readTable snapshots a single table of the session's schema.
	readTable(ctx context.Context, table *table) error
	// totalBytes returns the on disk size of the schema tables this session
	// reads, so the caller can size its progress reporting.
	totalBytes(ctx context.Context) (int64, error)
	// logFields returns the session specific fields to attach to the log
	// entries of the schema being read.
	logFields() loglib.Fields
}

// newTableReader builds the strategy used to read the snapshot tables and wraps
// it with any active decorator. Picking between strategies belongs here, so
// that the snapshot generator only ever sees a tableReader.
func newTableReader(conn pglib.Querier, logger loglib.Logger, sink rowSink, cfg *Config, instrumentation *otel.Instrumentation) tableReader {
	var reader tableReader = &ctidReader{
		conn:         conn,
		logger:       logger,
		sink:         sink,
		tableWorkers: cfg.tableWorkers(),
		batchBytes:   cfg.batchBytes(),
	}

	if instrumentation != nil {
		reader = newInstrumentedTableReader(reader, instrumentation)
	}

	return reader
}
