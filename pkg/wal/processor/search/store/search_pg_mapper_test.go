// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/searchstore/opensearch"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
)

const termByteLengthLimit = 32766

func TestMapper_ColumnToSearchMapping(t *testing.T) {
	tests := map[string]struct {
		pg      string
		mapping map[string]any
	}{
		"int8": {
			pg:      "int8",
			mapping: map[string]any{"type": "long"},
		},
		"bigint": {
			pg:      "bigint",
			mapping: map[string]any{"type": "long"},
		},
		"int2[]": {
			pg:      "int2[]",
			mapping: map[string]any{"type": "long"},
		},
		"integer": {
			pg:      "integer",
			mapping: map[string]any{"type": "long"},
		},
		"float": {
			pg:      "float",
			mapping: map[string]any{"type": "double"},
		},
		"float4[]": {
			pg:      "float4[]",
			mapping: map[string]any{"type": "double"},
		},
		"numeric": {
			pg:      "numeric",
			mapping: map[string]any{"type": "double"},
		},
		"numeric(5,2)": {
			pg:      "numeric(5,2)",
			mapping: map[string]any{"type": "double"},
		},
		"numeric(100,2)[]": {
			pg:      "numeric(100,2)[]",
			mapping: map[string]any{"type": "double"},
		},
		"boolean": {
			pg:      "boolean",
			mapping: map[string]any{"type": "boolean"},
		},
		"boolean[]": {
			pg:      "boolean[]",
			mapping: map[string]any{"type": "boolean"},
		},
		"bytea": {
			pg: "bytea",
			mapping: map[string]any{
				"type":         "keyword",
				"ignore_above": termByteLengthLimit,
				"fields": map[string]any{
					"text": map[string]any{
						"type": "text",
					},
				},
			},
		},
		"text": {
			pg: "text",
			mapping: map[string]any{
				"type":         "keyword",
				"ignore_above": termByteLengthLimit,
				"fields": map[string]any{
					"text": map[string]any{
						"type": "text",
					},
				},
			},
		},
		"text[]": {
			pg: "text[]",
			mapping: map[string]any{
				"type":         "keyword",
				"ignore_above": termByteLengthLimit,
				"fields": map[string]any{
					"text": map[string]any{
						"type": "text",
					},
				},
			},
		},
		"varchar(100)[]": {
			pg: "varchar(100)[]",
			mapping: map[string]any{
				"type":         "keyword",
				"ignore_above": termByteLengthLimit,
				"fields": map[string]any{
					"text": map[string]any{
						"type": "text",
					},
				},
			},
		},
		"time": {
			pg: "time",
			mapping: map[string]any{
				"type":   "date",
				"format": "HH:mm:ss[.SS][x][Z]||HH:mm:ss[.SSS][x][Z]||HH:mm:ss[.SSSSSS][x][Z]",
			},
		},
		"date": {
			pg: "date",
			mapping: map[string]any{
				"type":   "date",
				"format": "date",
			},
		},
		"timestamptz": {
			pg: "timestamptz",
			mapping: map[string]any{
				"type":   "date",
				"format": "yyyy-MM-dd HH:mm:ss[.SSS][x]||yyyy-MM-dd HH:mm:ss[.SS][x]||yyyy-MM-dd HH:mm:ss[.S][x]||yyyy-MM-dd'T'HH:mm:ss[.SSS][X]",
			},
		},
		"timestamp": {
			pg: "timestamp",
			mapping: map[string]any{
				"type":   "date",
				"format": "yyyy-MM-dd HH:mm:ss[.SSS][x]||yyyy-MM-dd HH:mm:ss[.SS][x]||yyyy-MM-dd HH:mm:ss[.S][x]||yyyy-MM-dd'T'HH:mm:ss[.SSS][X]",
			},
		},
		"timestamp with time zone[]": {
			pg: "timestamp with time zone[]",
			mapping: map[string]any{
				"type":   "date",
				"format": "yyyy-MM-dd HH:mm:ss[.SSS][x]||yyyy-MM-dd HH:mm:ss[.SS][x]||yyyy-MM-dd HH:mm:ss[.S][x]||yyyy-MM-dd'T'HH:mm:ss[.SSS][X]",
			},
		},
		"real[]": {
			pg:      "real[]",
			mapping: map[string]any{"type": "double"},
		},
		"jsonb": {
			pg:      "jsonb",
			mapping: map[string]any{"type": "text"},
		},
		"json": {
			pg:      "json",
			mapping: map[string]any{"type": "text"},
		},
		"macaddr": {
			pg: "macaddr",
			mapping: map[string]any{
				"type":         "keyword",
				"ignore_above": termByteLengthLimit,
				"fields": map[string]any{
					"text": map[string]any{
						"type": "text",
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			m := NewPostgresMapper(opensearch.NewMapper())
			mapping, err := m.ColumnToSearchMapping(&wal.DDLColumn{
				Type: test.pg,
			})
			require.NoError(t, err)
			require.Equal(t, test.mapping, mapping)
		})
	}

	errorTests := map[string]struct {
		pg string
	}{
		"invalid type": {
			pg: "a",
		},
		"badly formatted parameters": {
			pg: "numeric)[]",
		},
	}

	for name, test := range errorTests {
		t.Run(name, func(t *testing.T) {
			m := NewPostgresMapper(opensearch.NewMapper())
			_, err := m.ColumnToSearchMapping(&wal.DDLColumn{Type: test.pg})
			require.Error(t, err)

			var k search.ErrTypeInvalid
			require.True(t, errors.As(err, &k))
		})
	}
}

func TestMapper_MapColumnValue(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tsNow := now.Truncate(time.Millisecond).Format(timestampFormat)
	tstzNow := now.Truncate(time.Millisecond).Format(timestampTZFormat)
	const pgFormatTz = "2006-01-02 15:04:05.000000+00"
	const pgFormat = "2006-01-02 15:04:05.000000"

	tests := []struct {
		name     string
		dataType string
		value    any

		wantValue any
		wantErr   error
	}{
		{
			name:     "date",
			dataType: "date",
			value:    "2024-03-12",

			wantValue: "2024-03-12",
			wantErr:   nil,
		},
		{
			name:     "timestamp",
			dataType: "timestamp",
			value:    now,

			wantValue: tsNow,
			wantErr:   nil,
		},
		{
			name:     "timestamp with time zone",
			dataType: "timestamptz",
			value:    now,

			wantValue: tstzNow,
			wantErr:   nil,
		},
		{
			name:     "timestamp with time zone array",
			dataType: "timestamptz[]",
			value:    fmt.Sprintf("{%q}", now.Format(pgFormatTz)),

			wantValue: []string{tstzNow},
			wantErr:   nil,
		},
		{
			name:     "timestamp array",
			dataType: "timestamp[]",
			value:    fmt.Sprintf("{%q}", now.Format(pgFormat)),

			wantValue: []string{tsNow},
			wantErr:   nil,
		},
		{
			name:     "uuid",
			dataType: "uuid",
			value:    [16]uint8{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0},

			wantValue: "12345678-9abc-def0-1234-56789abcdef0",
			wantErr:   nil,
		},
		{
			name:     "string array",
			dataType: "text[]",
			value:    "{value1,value2}",

			wantValue: []string{"value1", "value2"},
			wantErr:   nil,
		},
		{
			name:     "unknown column type",
			dataType: "custom_type",
			value:    "value",

			wantValue: nil,
			wantErr:   errors.New("mapping column from pg to os: unsupported type: custom_type"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mapper := NewPostgresMapper(opensearch.NewMapper())
			value, err := mapper.MapColumnValue(&wal.Column{Type: tc.dataType, Value: tc.value})
			if !errors.Is(err, tc.wantErr) {
				require.Error(t, err, tc.wantErr.Error())
			}
			require.Equal(t, tc.wantValue, value)
		})
	}
}
