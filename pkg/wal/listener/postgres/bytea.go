// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/hex"
	"strings"

	"github.com/xataio/pgstream/pkg/wal"
)

const byteaType = "bytea"

// decodeByteaColumns converts the hex text wal2json emits for bytea columns
// into the raw bytes the rest of the pipeline expects.
//
// wal2json renders a bytea column as bare hex digits ("deadbeef"); the postgres
// hex format ("\xdeadbeef") is accepted too, since that is the form
// pkg/transformers already documents for the replication path. Snapshots are
// unaffected: pgx hands those values over as []byte to begin with, which is
// what makes this the point where the two paths converge.
//
// Without it the hex text travels all the way to the writer and is handed to
// pgx as the parameter value for a bytea column, storing its ASCII characters
// as the column contents — '\xdeadbeef' on the source becomes
// '\x6465616462656566' on the target. Nothing else about the row changes, so
// row counts and keys still match and the corruption only surfaces when
// something parses the column.
//
// Both Columns and Identity are covered, so insert values, the SET clause, the
// WHERE clause and the bulk-delete builders all see decoded bytes.
//
// A value that is not valid hex is left untouched rather than dropped, so an
// unexpected producer format degrades to the previous behaviour instead of
// failing the batch.
func decodeByteaColumns(d *wal.Data) {
	if d == nil {
		return
	}
	decodeByteaValues(d.Columns)
	decodeByteaValues(d.Identity)
}

func decodeByteaValues(cols []wal.Column) {
	for i := range cols {
		if cols[i].Type != byteaType {
			continue
		}
		strVal, ok := cols[i].Value.(string)
		if !ok {
			continue
		}
		decoded, err := hex.DecodeString(strings.TrimPrefix(strVal, `\x`))
		if err != nil {
			continue
		}
		cols[i].Value = decoded
	}
}
