// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type Tx interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, dest []any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	// ExecBatch runs the queries as one pipelined exchange. It does not use one
	// round trip for each query. A query that fails comes back as a
	// BatchQueryError carrying its index; any other error belongs to the batch.
	ExecBatch(ctx context.Context, queries []BatchQuery) error
	CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
	CopyFromText(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
	CopyToWriter(ctx context.Context, w io.Writer, sql string) (int64, error)
	CopyFromReader(ctx context.Context, r io.Reader, sql string) (int64, error)
}

// ErrTxRollback ends a transaction without keeping what it did and without
// reporting a failure. A function given to ExecInTx returns it when the
// transaction was only there to learn something: the work is rolled back and
// this sentinel reaches the caller unchanged.
//
// Every layer between that function and the caller has to let it through as
// it is. It is an answer rather than a failure, so nothing may retry it:
// running the same pass again costs the same round trips and returns the same
// thing.
var ErrTxRollback = errors.New("transaction rolled back on request")

type TxIsolationLevel string

const (
	Serializable    TxIsolationLevel = "serializable"
	RepeatableRead  TxIsolationLevel = "repeatable read"
	ReadCommitted   TxIsolationLevel = "read committed"
	ReadUncommitted TxIsolationLevel = "read uncommitted"
)

type TxAccessMode string

const (
	ReadWrite TxAccessMode = "read write"
	ReadOnly  TxAccessMode = "read only"
)

type TxOptions struct {
	IsolationLevel TxIsolationLevel
	AccessMode     TxAccessMode
}

type Txn struct {
	pgx.Tx
}

func (t *Txn) QueryRow(ctx context.Context, dest []any, query string, args ...any) error {
	row := t.Tx.QueryRow(ctx, query, args...)
	return MapError(row.Scan(dest...))
}

func (t *Txn) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := t.Tx.Query(ctx, query, args...)
	return rows, MapError(err)
}

func (t *Txn) Exec(ctx context.Context, query string, args ...any) (CommandTag, error) {
	tag, err := t.Tx.Exec(ctx, query, args...)
	return CommandTag{tag}, MapError(err)
}

// BatchQueryError is the failure of one query inside a batch, and carries the
// index of the query the server rejected.
//
// An error from ExecBatch that is not a BatchQueryError belongs to the batch
// as a whole: the queries never reached the server, or the exchange failed
// after every one of them had answered. No single query can be blamed for
// that, and a caller must not treat one as the culprit.
type BatchQueryError struct {
	Index int
	Err   error
}

func (e *BatchQueryError) Error() string {
	return fmt.Sprintf("batch query %d: %s", e.Index, e.Err)
}

func (e *BatchQueryError) Unwrap() error { return e.Err }

// BatchQuery is one query for ExecBatch.
type BatchQuery struct {
	SQL  string
	Args []any
}

// ExecBatch sends all queries together and then reads the results. This
// removes one network round trip for each query. The saving is large when the
// database is far from the client.
//
// The results come back in the same sequence as the queries, so the first
// error the server reports identifies the query that failed, and it comes back
// as a BatchQueryError. Postgres stops the transaction at that query, so all
// later queries also fail, and ExecBatch stops at the first error.
//
// Two failures carry no index and come back as plain errors. The batch may
// never reach the server, in which case the first read fails with a transport
// error rather than an answer about a query. The exchange may also fail while
// its results are closed, after every query has answered. Reporting either as
// the failure of a query would name one that did nothing wrong.
func (t *Txn) ExecBatch(ctx context.Context, queries []BatchQuery) error {
	batch := &pgx.Batch{}
	for _, q := range queries {
		batch.Queue(q.SQL, q.Args...)
	}

	results := t.SendBatch(ctx, batch)
	for i := range queries {
		if _, err := results.Exec(); err != nil {
			// Close reports the same failure again. The error from the read is
			// the useful one, because it says which query the server answered.
			_ = results.Close()
			return batchError(queries, i, err)
		}
	}
	return MapError(results.Close())
}

// batchError names the query that failed, when the failure can be named.
//
// The index of the result being read is not the answer on its own. pgx
// prepares every statement of the batch, and encodes every argument, before it
// executes any of them. A failure of either step therefore surfaces on the
// first read, whichever statement owns it. Reading that index as the culprit
// drops a statement that did nothing wrong, and the real one runs again in the
// retry, which is the silent loss this type exists to prevent.
//
// A preprocessing failure carries the SQL of the statement it belongs to, so
// it can be matched back. When two queries of the batch share that SQL the
// match is not unique, and the failure stays with the batch.
//
// Anything else that is not a server error for the statement just read is a
// failure of the batch. The caller isolates those by running the queries one
// at a time.
func batchError(queries []BatchQuery, index int, err error) error {
	// A failure that names its own statement can be given to it. pgx reports
	// a preprocessing failure as pgx.ErrPreprocessingBatch, which carries the
	// SQL, and that is the only error on this path that names one.
	var named interface{ SQL() string }
	if errors.As(err, &named) {
		if at := uniqueSQLIndex(queries, named.SQL()); at >= 0 {
			return &BatchQueryError{Index: at, Err: MapError(err)}
		}
		return MapError(err)
	}

	// Only the server can say that this query failed. Anything else is the
	// batch failing around it.
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return MapError(err)
	}
	if index < 0 || index >= len(queries) {
		return MapError(err)
	}
	return &BatchQueryError{Index: index, Err: MapError(err)}
}

// uniqueSQLIndex returns the position of the only query with this SQL, or -1
// when there is none or more than one.
func uniqueSQLIndex(queries []BatchQuery, sql string) int {
	found := -1
	for i := range queries {
		if queries[i].SQL != sql {
			continue
		}
		if found >= 0 {
			return -1
		}
		found = i
	}
	return found
}

// CopyFrom uses pgx's binary-format COPY, which is the fast path for any
// column type pgx has a binary codec for. Callers must use CopyFromText
// instead when the target table contains extension columns whose binary
// representation pgx cannot produce.
func (t *Txn) CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error) {
	identifier, err := newIdentifier(tableName)
	if err != nil {
		return -1, err
	}
	for i, c := range columnNames {
		columnNames[i] = removeQuotes(c)
	}
	return t.Tx.CopyFrom(ctx, identifier, columnNames, pgx.CopyFromRows(srcRows))
}

// CopyFromText runs the postgres COPY protocol in text format, rather than
// the binary format pgx defaults to. Text format makes the destination
// Postgres parse each value through the per-type input function.
// This matches pg_dump/pg_restore behaviour at the cost of a
// modest serialisation/wire-size overhead vs. binary COPY.
func (t *Txn) CopyFromText(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error) {
	if len(srcRows) == 0 {
		return 0, nil
	}

	for i, c := range columnNames {
		columnNames[i] = removeQuotes(c)
	}

	identifier, err := newIdentifier(tableName)
	if err != nil {
		return -1, err
	}

	conn := t.Conn()
	tm := conn.TypeMap()

	// Look up the column OIDs once via a prepared statement description so
	// each value can be encoded with the right text codec.
	quotedCols := make([]string, len(columnNames))
	for i, c := range columnNames {
		quotedCols[i] = pgx.Identifier{c}.Sanitize()
	}
	sd, err := conn.Prepare(ctx, "",
		fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedCols, ", "), identifier.Sanitize()))
	if err != nil {
		return -1, fmt.Errorf("describing copy target: %w", err)
	}
	if len(sd.Fields) != len(columnNames) {
		return -1, fmt.Errorf("copy target returned %d fields, expected %d", len(sd.Fields), len(columnNames))
	}

	// Bulk batches are bounded by maxParamsPerQuery/numCols upstream
	// so we can serialise the COPY payload into a single buffer up front.
	buf := bytes.NewBuffer(make([]byte, 0, estimateCopyTextSize(srcRows)))
	for _, row := range srcRows {
		if len(row) != len(columnNames) {
			return -1, fmt.Errorf("row has %d values, expected %d", len(row), len(columnNames))
		}
		for i, val := range row {
			if i > 0 {
				buf.WriteByte('\t')
			}
			if err := writeCopyTextValue(buf, tm, sd.Fields[i].DataTypeOID, val); err != nil {
				return -1, fmt.Errorf("encoding column %s: %w", columnNames[i], err)
			}
		}
		buf.WriteByte('\n')
	}

	sql := fmt.Sprintf("COPY %s ( %s ) FROM STDIN", identifier.Sanitize(), strings.Join(quotedCols, ", "))
	tag, err := conn.PgConn().CopyFrom(ctx, bytes.NewReader(buf.Bytes()), sql)
	if err != nil {
		return -1, err
	}
	return tag.RowsAffected(), nil
}

// estimateCopyTextSize returns a rough byte budget for the buffer that will
// hold the entire COPY text payload. Slightly over-estimating avoids slice
// regrowth while keeping the upfront allocation bounded.
func estimateCopyTextSize(rows [][]any) int {
	if len(rows) == 0 {
		return 0
	}
	const perValueOverhead = 16 // delimiter + escape headroom
	var perRow int
	for _, v := range rows[0] {
		switch val := v.(type) {
		case string:
			perRow += len(val) + perValueOverhead
		case []byte:
			perRow += len(val) + perValueOverhead
		default:
			_ = val
			perRow += 24 // numeric/timestamp text form fits well under this
		}
	}
	return perRow * len(rows)
}

// writeCopyTextValue appends the COPY-text-format encoding of v to buf,
// looking up the encoder for oid in the type map. NULL is emitted as `\N`.
// Special characters in the text encoding are escaped per the rules in
// https://www.postgresql.org/docs/current/sql-copy.html (Text Format).
func writeCopyTextValue(buf *bytes.Buffer, tm *pgtype.Map, oid uint32, v any) error {
	if v == nil {
		buf.WriteString(`\N`)
		return nil
	}
	// The destination buffer must be non-nil: with a nil buffer, encoding a
	// non-NULL empty string returns (nil, nil) and would be indistinguishable
	// from SQL NULL below.
	encoded, err := tm.Encode(oid, pgtype.TextFormatCode, v, make([]byte, 0, 16))
	if err != nil {
		return err
	}
	if encoded == nil {
		buf.WriteString(`\N`)
		return nil
	}
	writeCopyTextEscaped(buf, encoded)
	return nil
}

// copyTextEscapes maps each byte that needs escaping in COPY text format to
// its replacement. A 256-entry table is faster than a switch and lets us
// detect whether a byte needs escaping with a single index + len check.
var copyTextEscapes = func() [256]string {
	var t [256]string
	t['\b'] = `\b`
	t['\f'] = `\f`
	t['\n'] = `\n`
	t['\r'] = `\r`
	t['\t'] = `\t`
	t['\v'] = `\v`
	t['\\'] = `\\`
	return t
}()

// writeCopyTextEscaped writes b to buf escaping the byte values that have a
// special meaning in COPY text format: \b \f \n \r \t \v \\. Runs of bytes
// that do not need escaping are emitted in a single Write to keep the hot
// path cheap for typical (escape-free) strings.
func writeCopyTextEscaped(buf *bytes.Buffer, b []byte) {
	start := 0
	for i, c := range b {
		if esc := copyTextEscapes[c]; esc != "" {
			if i > start {
				buf.Write(b[start:i])
			}
			buf.WriteString(esc)
			start = i + 1
		}
	}
	if start < len(b) {
		buf.Write(b[start:])
	}
}

func (t *Txn) CopyToWriter(ctx context.Context, w io.Writer, sql string) (int64, error) {
	tag, err := t.Conn().PgConn().CopyTo(ctx, w, sql)
	if err != nil {
		return -1, MapError(err)
	}
	return tag.RowsAffected(), nil
}

func (t *Txn) CopyFromReader(ctx context.Context, r io.Reader, sql string) (int64, error) {
	tag, err := t.Conn().PgConn().CopyFrom(ctx, r, sql)
	if err != nil {
		return -1, MapError(err)
	}
	return tag.RowsAffected(), nil
}

func toTxOptions(opts TxOptions) pgx.TxOptions {
	return pgx.TxOptions{
		IsoLevel:   pgx.TxIsoLevel(opts.IsolationLevel),
		AccessMode: pgx.TxAccessMode(opts.AccessMode),
	}
}
