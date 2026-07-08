// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

// readSession carries the per-schema state that a tableReader needs to snapshot
// individual tables. For the ctid reader it holds the exported transaction
// snapshot id shared by all the table read transactions. It is only valid for
// the duration of the beginSchema callback that produced it.
type readSession struct {
	snapshotID string
}

// tableReader abstracts the strategy used to read a schema's tables during a
// data snapshot. The ctid reader ranges over a stable transaction snapshot
// using the ctid to parallelise the work; future implementations (e.g. a
// primary-key keyset reader) can provide alternative behaviour behind the same
// seam.
type tableReader interface {
	// beginSchema prepares the reader to snapshot the given schema tables and
	// invokes fn with the readSession that must be used for every readTable call
	// belonging to that schema. The session is only valid for the duration of
	// fn.
	beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, *readSession) error) error
	// readTable snapshots a single table using the session provided by
	// beginSchema.
	readTable(ctx context.Context, session *readSession, table *table) error
}
