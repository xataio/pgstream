// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers/internal/lookup"
)

// setLookupQuerier points the load at a stub serving the given values. The
// tests that call it must not run in parallel: the seam they replace is
// package level.
func setLookupQuerier(t *testing.T, oid uint32, values []any, opts lookup.StubOptions) {
	t.Helper()
	original := lookup.NewQuerier
	t.Cleanup(func() { lookup.NewQuerier = original })
	lookup.NewQuerier = lookup.StubQuerier(oid, values, opts)
}

func testUUID(last byte) [16]byte {
	return [16]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, last}
}

func TestNewLookupChoiceTransformer(t *testing.T) {
	errTest := errors.New("oh noes")

	validParams := func() ParameterValues {
		return ParameterValues{
			"lookup_table":  "reference.countries",
			"lookup_column": "id",
			"postgres_url":  "postgres://user:pass@localhost:5432/db",
		}
	}
	withParam := func(name string, value any) ParameterValues {
		params := validParams()
		params[name] = value
		return params
	}

	tests := []struct {
		name       string
		params     ParameterValues
		oid        uint32
		values     []any
		opts       lookup.StubOptions
		wantValues []any
		wantTypes  []SupportedDataType
		wantErr    error
	}{
		{
			name:       "ok - values loaded",
			params:     validParams(),
			oid:        pgtype.Int8OID,
			values:     []any{int64(1), int64(2), int64(3)},
			wantValues: []any{int64(1), int64(2), int64(3)},
			wantTypes:  []SupportedDataType{Integer64DataType},
		},
		{
			name:       "ok - a narrower integer column is assignable to a wider one",
			params:     validParams(),
			oid:        pgtype.Int4OID,
			values:     []any{int32(1)},
			wantValues: []any{int32(1)},
			wantTypes:  []SupportedDataType{Integer32DataType, Integer64DataType},
		},
		{
			name:       "ok - an extension type falls back to the decoded Go type",
			params:     validParams(),
			oid:        16385, // citext has no fixed OID
			values:     []any{"one"},
			wantValues: []any{"one"},
			wantTypes:  []SupportedDataType{StringDataType, CitextDataType},
		},
		{
			name: "ok - ignore_values removes matching values",
			// the configuration parses ids as int, while the database
			// returns them as int64
			params:     withParam("ignore_values", []any{1, 3}),
			oid:        pgtype.Int8OID,
			values:     []any{int64(1), int64(2), int64(3)},
			wantValues: []any{int64(2)},
			wantTypes:  []SupportedDataType{Integer64DataType},
		},
		{
			name:       "ok - ignore_values matches a uuid written as text",
			params:     withParam("ignore_values", []any{"aabbccdd-eeff-0011-2233-445566778801"}),
			oid:        pgtype.UUIDOID,
			values:     []any{testUUID(0x01), testUUID(0x02)},
			wantValues: []any{testUUID(0x02)},
			wantTypes:  []SupportedDataType{UInt8ArrayOf16DataType},
		},
		{
			name:       "ok - ignore_values matches a date written as text",
			params:     withParam("ignore_values", []any{"2024-01-01"}),
			oid:        pgtype.DateOID,
			values:     []any{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
			wantValues: []any{time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
			wantTypes:  []SupportedDataType{DateDataType},
		},
		{
			name:    "error - ignore_values entry matches nothing",
			params:  withParam("ignore_values", []any{4}),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1), int64(2)},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - lookup table is empty",
			params:  validParams(),
			oid:     pgtype.Int8OID,
			values:  []any{},
			wantErr: errors.New("lookup_choice: no values found in column id of table reference.countries"),
		},
		{
			name:    "error - all values ignored",
			params:  withParam("ignore_values", []any{1, 2}),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1), int64(2)},
			wantErr: errors.New("lookup_choice: every value in column id of table reference.countries is excluded by ignore_values"),
		},
		{
			name:    "error - unsupported lookup column type",
			params:  validParams(),
			oid:     pgtype.NumericOID,
			values:  []any{pgtype.Numeric{}},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - deterministic generator on a timestamp column",
			params:  withParam("generator", "deterministic"),
			oid:     pgtype.TimestamptzOID,
			values:  []any{time.Now()},
			wantErr: ErrInvalidParameters,
		},
		{
			name:       "ok - random generator on a timestamp column",
			params:     withParam("generator", "random"),
			oid:        pgtype.TimestampOID,
			values:     []any{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			wantValues: []any{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			wantTypes:  []SupportedDataType{DatetimeDataType},
		},
		{
			name: "error - lookup_table missing",
			params: ParameterValues{
				"lookup_column": "id",
				"postgres_url":  "postgres://user:pass@localhost:5432/db",
			},
			wantErr: errLookupTableNotFound,
		},
		{
			name: "error - lookup_column missing",
			params: ParameterValues{
				"lookup_table": "reference.countries",
				"postgres_url": "postgres://user:pass@localhost:5432/db",
			},
			wantErr: errLookupColumnNotFound,
		},
		{
			name: "error - postgres_url missing",
			params: ParameterValues{
				"lookup_table":  "reference.countries",
				"lookup_column": "id",
			},
			wantErr: errLookupURLNotFound,
		},
		{
			name:    "error - postgres_url empty",
			params:  withParam("postgres_url", ""),
			wantErr: errLookupURLNotFound,
		},
		{
			name:    "error - unknown generator",
			params:  withParam("generator", "sequential"),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1)},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - ignore_values is not a list",
			params:  withParam("ignore_values", 1),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1)},
			wantErr: ErrInvalidParameters,
		},
		{
			name:    "error - lookup_table is not a valid qualified name",
			params:  withParam("lookup_table", "a.b.c"),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1)},
			wantErr: errors.New(`lookup_choice: invalid lookup_table "a.b.c": unexpected qualified name format`),
		},
		{
			name:    "error - opening the connection fails",
			params:  validParams(),
			opts:    lookup.StubOptions{NewQuerierErr: errTest},
			wantErr: errTest,
		},
		{
			name:    "error - query fails",
			params:  validParams(),
			opts:    lookup.StubOptions{QueryErr: errTest},
			wantErr: errTest,
		},
		{
			name:    "error - scanning a row fails",
			params:  validParams(),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1)},
			opts:    lookup.StubOptions{ScanErr: errTest},
			wantErr: errTest,
		},
		{
			name:    "error - iterating the rows fails",
			params:  validParams(),
			oid:     pgtype.Int8OID,
			values:  []any{int64(1)},
			opts:    lookup.StubOptions{RowsErr: errTest},
			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setLookupQuerier(t, tc.oid, tc.values, tc.opts)

			transformer, err := NewLookupChoiceTransformer(tc.params)
			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.EqualError(t, err, tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantValues, transformer.values)
			require.Equal(t, tc.wantTypes, transformer.CompatibleTypes())
		})
	}
}

// the load holds a connection only while it reads, so a leak would keep a
// pool open for the lifetime of the process
func TestNewLookupChoiceTransformer_closesTheConnection(t *testing.T) {
	errTest := errors.New("oh noes")

	tests := map[string]lookup.StubOptions{
		"after a successful load": {},
		"after a failed read":     {RowsErr: errTest},
	}

	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			closed := false
			opts.Closed = &closed
			setLookupQuerier(t, pgtype.Int8OID, []any{int64(1)}, opts)

			_, err := NewLookupChoiceTransformer(ParameterValues{
				"lookup_table":  "reference.countries",
				"lookup_column": "id",
				"postgres_url":  "postgres://user:pass@localhost:5432/db",
			})
			if opts.RowsErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, closed, "the lookup connection was not closed")
		})
	}
}

func TestNewLookupChoiceTransformer_query(t *testing.T) {
	var gotQuery string
	setLookupQuerier(t, pgtype.Int8OID, []any{int64(1)}, lookup.StubOptions{Query: &gotQuery})

	_, err := NewLookupChoiceTransformer(ParameterValues{
		"lookup_table":  "reference.countries",
		"lookup_column": "id",
		"postgres_url":  "postgres://user:pass@localhost:5432/db",
	})
	require.NoError(t, err)
	require.Equal(t, `SELECT "id" FROM "reference"."countries" WHERE "id" IS NOT NULL ORDER BY "id"`, gotQuery)

	// an unqualified table name defaults to the public schema
	_, err = NewLookupChoiceTransformer(ParameterValues{
		"lookup_table":  "countries",
		"lookup_column": "id",
		"postgres_url":  "postgres://user:pass@localhost:5432/db",
	})
	require.NoError(t, err)
	require.Equal(t, `SELECT "id" FROM "public"."countries" WHERE "id" IS NOT NULL ORDER BY "id"`, gotQuery)
}

func TestLookupChoiceTransformer_Transform(t *testing.T) {
	t.Parallel()

	values := []any{int64(10), int64(20), int64(30), int64(40)}
	inputs := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	intTypes := []SupportedDataType{Integer64DataType}

	newTransformer := func(t *testing.T, values []any, generatorType string) *LookupChoiceTransformer {
		t.Helper()
		transformer, err := newLookupChoiceTransformer(values, generatorType, intTypes, false)
		require.NoError(t, err)
		return transformer
	}

	t.Run("random - every output comes from the lookup values", func(t *testing.T) {
		t.Parallel()

		transformer := newTransformer(t, values, "random")
		seen := map[any]bool{}
		for range 100 {
			got, err := transformer.Transform(context.Background(), NewValue("a", "int8", nil))
			require.NoError(t, err)
			require.Contains(t, values, got)
			seen[got] = true
		}
		// the choice must not collapse onto a single value
		require.Greater(t, len(seen), 1)
	})

	t.Run("deterministic - the same input always gives the same value", func(t *testing.T) {
		t.Parallel()

		transformer := newTransformer(t, values, "deterministic")
		// a separately built transformer over the same values must agree, so
		// that the mapping survives a restart
		other := newTransformer(t, values, "deterministic")

		seen := map[any]bool{}
		for _, input := range inputs {
			want, err := transformer.Transform(context.Background(), NewValue(input, "text", nil))
			require.NoError(t, err)
			require.Contains(t, values, want)
			seen[want] = true

			for range 10 {
				got, err := transformer.Transform(context.Background(), NewValue(input, "text", nil))
				require.NoError(t, err)
				require.Equal(t, want, got)
			}

			got, err := other.Transform(context.Background(), NewValue(input, "text", nil))
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		require.Greater(t, len(seen), 1)
	})

	t.Run("deterministic - the mapping is pinned to these values", func(t *testing.T) {
		t.Parallel()

		// the worked example in docs/transformers.md quotes these outputs; a
		// change to the hash or the index arithmetic must not pass silently
		transformer := newTransformer(t, []any{int64(1), int64(2), int64(3)}, "deterministic")

		got, err := transformer.Transform(context.Background(), NewValue(int64(7), "int8", nil))
		require.NoError(t, err)
		require.Equal(t, int64(2), got)

		got, err = transformer.Transform(context.Background(), NewValue(int64(8), "int8", nil))
		require.NoError(t, err)
		require.Equal(t, int64(1), got)
	})

	t.Run("deterministic - equal values map to equal values regardless of type", func(t *testing.T) {
		t.Parallel()

		transformer := newTransformer(t, values, "deterministic")

		fromString, err := transformer.Transform(context.Background(), NewValue("42", "text", nil))
		require.NoError(t, err)
		fromBytes, err := transformer.Transform(context.Background(), NewValue([]byte("42"), "bytea", nil))
		require.NoError(t, err)
		fromInt, err := transformer.Transform(context.Background(), NewValue(int64(42), "int8", nil))
		require.NoError(t, err)

		require.Equal(t, fromString, fromBytes)
		require.Equal(t, fromString, fromInt)
	})

	t.Run("deterministic - a uuid maps the same from both ingestion paths", func(t *testing.T) {
		t.Parallel()

		transformer := newTransformer(t, values, "deterministic")

		// a snapshot delivers what pgx decoded, replication delivers the text
		// wal2json emitted
		fromSnapshot, err := transformer.Transform(context.Background(), NewValue(testUUID(0x01), "uuid", nil))
		require.NoError(t, err)
		fromReplication, err := transformer.Transform(context.Background(), NewValue("aabbccdd-eeff-0011-2233-445566778801", "uuid", nil))
		require.NoError(t, err)

		require.Equal(t, fromSnapshot, fromReplication)
	})

	t.Run("deterministic - a date maps the same from both ingestion paths", func(t *testing.T) {
		t.Parallel()

		transformer, err := newLookupChoiceTransformer(values, "deterministic", intTypes, true)
		require.NoError(t, err)

		fromSnapshot, err := transformer.Transform(context.Background(), NewValue(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "date", nil))
		require.NoError(t, err)
		fromReplication, err := transformer.Transform(context.Background(), NewValue("2024-01-01", "date", nil))
		require.NoError(t, err)

		require.Equal(t, fromSnapshot, fromReplication)
	})

	t.Run("a single value is always chosen", func(t *testing.T) {
		t.Parallel()

		transformer := newTransformer(t, []any{int64(7)}, "random")
		got, err := transformer.Transform(context.Background(), NewValue("a", "int8", nil))
		require.NoError(t, err)
		require.Equal(t, int64(7), got)
	})

	t.Run("error - no values to choose from", func(t *testing.T) {
		t.Parallel()

		_, err := newLookupChoiceTransformer(nil, "random", intTypes, false)
		require.ErrorIs(t, err, errLookupNoValues)
	})
}

func TestLookupChoiceTransformer_interface(t *testing.T) {
	t.Parallel()

	transformer, err := newLookupChoiceTransformer([]any{int64(1)}, "random", []SupportedDataType{Integer64DataType}, false)
	require.NoError(t, err)

	require.Equal(t, LookupChoice, transformer.Type())
	require.False(t, transformer.IsDynamic())
	require.Equal(t, UniquenessLossy, transformer.Uniqueness())
	require.NoError(t, transformer.Close())
}
