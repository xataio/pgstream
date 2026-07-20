// SPDX-License-Identifier: Apache-2.0

package postgres

import "context"

// readSession carries the per-schema state that a tableReader needs to snapshot
// individual tables. It is only valid for the duration of the beginSchema
// callback that produced it.
type readSession struct {
	// snapshotID is the exported transaction snapshot shared by all of a
	// schema's table read transactions. It is empty when the schema has no
	// tables to read, in which case readTable must not be called (an empty id
	// would make SET TRANSACTION SNAPSHOT fail).
	snapshotID string
}

// tableReader abstracts the strategy used to read a schema's tables during a
// data snapshot.
type tableReader interface {
	// beginSchema prepares the reader to snapshot the given schema tables and
	// invokes fn with the readSession that must be used for every readTable call
	// belonging to that schema.
	// The session is only valid for the duration of fn.
	beginSchema(ctx context.Context, st *schemaTables, fn func(context.Context, *readSession) error) error
	// readTable snapshots a single table using the session provided by beginSchema.
	readTable(ctx context.Context, session *readSession, table *table) error
}
