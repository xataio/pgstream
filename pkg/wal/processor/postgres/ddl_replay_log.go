// SPDX-License-Identifier: Apache-2.0

package postgres

// ddlReplayLog remembers the DDL statements replayed for the source
// transaction being applied.
//
// The emit_ddl event trigger sends current_query(), the whole client
// statement, once for each DDL command it runs. A statement with several
// commands, such as a migration script, therefore arrives once per command, and
// each copy carries the schema of its own command. The writer replays each
// copy with that schema first in the search path, so a second copy can apply
// unqualified objects in a schema the source never used, or fail on work the
// first copy already did. The source ran the statement once, and the target
// should too.
//
// The log needs transaction ids (the wal2json include-xids option). Events
// without one are never skipped. A transaction's events arrive together, so the
// log only keeps the current one. It is not safe for concurrent use: the batch
// writer sends its batches one at a time.
type ddlReplayLog struct {
	xid      int64
	replayed map[string]struct{}
}

// done reports whether sql was already replayed in transaction xid.
func (l *ddlReplayLog) done(xid int64, sql string) bool {
	if xid == 0 || xid != l.xid {
		return false
	}
	_, ok := l.replayed[sql]
	return ok
}

// record marks sql as replayed in transaction xid, forgetting any previous
// transaction.
func (l *ddlReplayLog) record(xid int64, sql string) {
	if xid == 0 {
		return
	}
	if xid != l.xid || l.replayed == nil {
		l.xid = xid
		l.replayed = map[string]struct{}{}
	}
	l.replayed[sql] = struct{}{}
}
