// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"strconv"

	"github.com/xataio/pgstream/pkg/wal"
)

// wal2json prints every postgres number as a JSON number, and a JSON number
// that a float64 holds keeps about 17 digits. A numeric keeps up to 1000, so a
// value that needs more comes back rounded, with no error and no counter:
//
//	106.000000000000000001            ->  106
//	0.10000000000000000000000000001   ->  0.1
//	-9223372036854775808.5            ->  -9223372036854776000
//
// The listener therefore decodes numbers as json.Number, which keeps the text
// the source printed, and this pass gives each value the Go type its column
// needs. A numeric keeps its text, so postgres parses the same digits back.
//
// The types below are the names wal2json prints in `columntypes`.
var (
	integerTypes = map[string]struct{}{
		"smallint": {}, "integer": {}, "bigint": {},
		"int2": {}, "int4": {}, "int8": {},
		"smallserial": {}, "serial": {}, "bigserial": {},
		"oid": {},
	}
	floatTypes = map[string]struct{}{
		"real": {}, "double precision": {},
		"float4": {}, "float8": {},
	}
)

// decodeNumberColumns gives every number the Go type its column needs.
func decodeNumberColumns(d *wal.Data) {
	if d == nil {
		return
	}
	decodeNumberValues(d.Columns)
	decodeNumberValues(d.Identity)
}

func decodeNumberValues(cols []wal.Column) {
	for i := range cols {
		num, ok := cols[i].Value.(json.Number)
		if !ok {
			continue
		}
		cols[i].Value = numberValue(num, cols[i].Type)
	}
}

// numberValue converts one JSON number.
//
// An integer column becomes an int64, which is what the writer and the
// transformers expect. A float column becomes a float64, because a float64 is
// what the source holds. Every other numeric column keeps its text, which
// postgres parses with no loss.
func numberValue(num json.Number, colType string) any {
	if _, found := integerTypes[colType]; found {
		if v, err := strconv.ParseInt(num.String(), 10, 64); err == nil {
			return v
		}
		return num.String()
	}
	if _, found := floatTypes[colType]; found {
		if v, err := strconv.ParseFloat(num.String(), 64); err == nil {
			return v
		}
		return num.String()
	}
	return num.String()
}
