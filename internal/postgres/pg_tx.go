// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Tx interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, dest []any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) (CommandTag, error)
	CopyFrom(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
	CopyFromText(ctx context.Context, tableName string, columnNames []string, srcRows [][]any) (int64, error)
}

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
	encoded, err := tm.Encode(oid, pgtype.TextFormatCode, v, nil)
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

func toTxOptions(opts TxOptions) pgx.TxOptions {
	return pgx.TxOptions{
		IsoLevel:   pgx.TxIsoLevel(opts.IsolationLevel),
		AccessMode: pgx.TxAccessMode(opts.AccessMode),
	}
}
