// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

// testMapper implements the mapper interface for unit tests.
type testMapper struct{}

func (m *testMapper) TypeForOID(_ context.Context, oid uint32) (string, error) {
	switch oid {
	case pgtype.Int4OID:
		return "integer", nil
	case pgtype.TextOID:
		return "text", nil
	default:
		return "unknown", nil
	}
}

// TestRowToWalEvent_WithMetadata_PopulatesInternalColIDs calls the real
// rowToWalEvent with tableMetadata and verifies the output event has
// Metadata.InternalColIDs and Column.ID set correctly. These fields are
// required by the downstream buildOnConflictQuery to generate ON CONFLICT
// for deduplication in the Kafka path.
func TestRowToWalEvent_WithMetadata_PopulatesInternalColIDs(t *testing.T) {
	t.Parallel()

	adapt := newAdapter(&testMapper{}, nil)

	meta := &tableMetadata{
		tablePgstreamID: "public.test_table",
		pkColumnNames:   []string{"id"},
	}

	// Real pgconn.FieldDescription — same struct pgx produces from query results
	fieldDescs := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int4OID},
		{Name: "name", DataTypeOID: pgtype.TextOID},
	}
	values := []any{int32(1), "test_value"} // pgx Int4Codec.DecodeValue returns int32
	rawValues := [][]byte{{1}, {1}} // non-nil = not SQL NULL

	// Call the REAL rowToWalEvent with metadata
	event := adapt.rowToWalEvent(context.Background(), "public", "test_table", fieldDescs, values, rawValues, meta)
	require.NotNil(t, event)

	// Metadata must be populated
	require.Equal(t, "public.test_table", event.Data.Metadata.TablePgstreamID)
	require.Equal(t, []string{"id"}, event.Data.Metadata.InternalColIDs,
		"InternalColIDs must contain PK column names for ON CONFLICT dedup")

	// Column.ID must be set to column name (for extractPrimaryKeyColumns matching)
	require.Equal(t, "id", event.Data.Columns[0].ID,
		"Column.ID must be set to column name so extractPrimaryKeyColumns can match against InternalColIDs")
	require.Equal(t, "name", event.Data.Columns[1].ID)

	// Column data must still be correct (pgx returns int32 for int4)
	require.Equal(t, "integer", event.Data.Columns[0].Type)
	require.Equal(t, int32(1), event.Data.Columns[0].Value)
	require.Equal(t, "text", event.Data.Columns[1].Type)
	require.Equal(t, "test_value", event.Data.Columns[1].Value)

	// Metadata must survive Kafka round-trip
	kafkaBytes, err := sonicjson.Marshal(event.Data)
	require.NoError(t, err)
	var roundTripped wal.Data
	require.NoError(t, sonicjson.Unmarshal(kafkaBytes, &roundTripped))

	require.Equal(t, []string{"id"}, roundTripped.Metadata.InternalColIDs,
		"InternalColIDs must survive Kafka JSON round-trip")
	require.Equal(t, "id", roundTripped.Columns[0].ID,
		"Column.ID must survive Kafka JSON round-trip")
}

// TestRowToWalEvent_WithoutMetadata_EmptyInternalColIDs calls rowToWalEvent
// with nil tableMetadata (pre-fix behavior) and verifies metadata is empty —
// proving that without the fix, ON CONFLICT cannot work.
func TestRowToWalEvent_WithoutMetadata_EmptyInternalColIDs(t *testing.T) {
	t.Parallel()

	adapt := newAdapter(&testMapper{}, nil)

	fieldDescs := []pgconn.FieldDescription{
		{Name: "id", DataTypeOID: pgtype.Int4OID},
		{Name: "name", DataTypeOID: pgtype.TextOID},
	}
	values := []any{int32(1), "test_value"} // pgx Int4Codec.DecodeValue returns int32
	rawValues := [][]byte{{1}, {1}}

	// nil meta = pre-fix behavior (no PK info)
	event := adapt.rowToWalEvent(context.Background(), "public", "test_table", fieldDescs, values, rawValues, nil)
	require.NotNil(t, event)

	require.True(t, event.Data.Metadata.IsEmpty(),
		"Without metadata, Metadata must be empty — ON CONFLICT cannot identify PK columns")
	require.Empty(t, event.Data.Metadata.InternalColIDs)
	require.Empty(t, event.Data.Columns[0].ID,
		"Without metadata, Column.ID is not set — extractPrimaryKeyColumns returns empty")
}
