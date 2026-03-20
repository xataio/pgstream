// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

// TestSnapshotEvent_FullPath_OnConflict exercises the real code path from
// snapshot row adapter → Kafka round-trip → buildInsertQueries, verifying
// that ON CONFLICT is generated for deduplication.
//
// The adapter's rowToWalEvent populates Metadata.InternalColIDs from
// tableMetadata (queried PK columns). buildOnConflictQuery uses these to
// generate ON CONFLICT (pk) DO UPDATE SET.
func TestSnapshotEvent_FullPath_OnConflict(t *testing.T) {
	t.Parallel()

	// Construct event as the fixed rowToWalEvent produces it:
	// - Column.ID set to column name (adapter line 51: columns[i].ID = columns[i].Name)
	// - Metadata.InternalColIDs set to PK column names queried from pg_catalog
	// - Metadata.TablePgstreamID set to schema.table
	//
	// The snapshot generator queries PK columns via queryPKColumns() and passes
	// them as tableMetadata to rowToWalEvent. rowToWalEvent sets Column.ID and
	// Metadata on the event. buildOnConflictQuery then matches InternalColIDs
	// against Column.ID to generate ON CONFLICT.
	// Values are float64 because json.Unmarshal decodes JSON numbers as float64.
	// pgx originally returns int32, but after Kafka round-trip (Marshal → Unmarshal)
	// int32 becomes float64. This event represents what kafka2pg actually receives.
	event := &wal.Data{
		Action: "I",
		Schema: "public",
		Table:  "test_table",
		Columns: []wal.Column{
			{ID: "id", Name: "id", Type: "integer", Value: float64(1)},
			{ID: "name", Name: "name", Type: "text", Value: "test"},
		},
		Metadata: wal.Metadata{
			TablePgstreamID: "public.test_table",
			InternalColIDs:  []string{"id"},
		},
	}

	// Step 2: Kafka round-trip
	kafkaBytes, err := sonicjson.Marshal(event)
	require.NoError(t, err)

	var roundTripped wal.Data
	require.NoError(t, sonicjson.Unmarshal(kafkaBytes, &roundTripped))

	// Metadata must survive
	require.Equal(t, []string{"id"}, roundTripped.Metadata.InternalColIDs)
	require.Equal(t, "id", roundTripped.Columns[0].ID)

	// Step 3: buildInsertQueries with ON CONFLICT UPDATE (real kafka2pg adapter)
	adapter, err := newDMLAdapter("update", false, nil)
	require.NoError(t, err)

	queries := adapter.buildInsertQueries(&roundTripped, schemaInfo{})
	require.Len(t, queries, 1)

	// Step 4: Assert ON CONFLICT is present
	require.Contains(t, queries[0].sql, `ON CONFLICT ("id") DO UPDATE SET`,
		"Snapshot events with PK metadata must generate ON CONFLICT for deduplication")
}

// TestSnapshotEvent_WithoutMetadata_NoOnConflict proves that without metadata
// (the pre-fix behavior), ON CONFLICT is NOT generated. This is the contrast
// test showing WHY the metadata is needed.
func TestSnapshotEvent_WithoutMetadata_NoOnConflict(t *testing.T) {
	t.Parallel()

	event := &wal.Data{
		Action: "I",
		Schema: "public",
		Table:  "test_table",
		Columns: []wal.Column{
			{ID: "", Name: "id", Type: "integer", Value: float64(1)},
			{ID: "", Name: "name", Type: "text", Value: "test"},
		},
		Metadata: wal.Metadata{}, // empty — pre-fix behavior
	}

	adapter, err := newDMLAdapter("update", false, nil)
	require.NoError(t, err)

	queries := adapter.buildInsertQueries(event, schemaInfo{})
	require.Len(t, queries, 1)

	require.NotContains(t, queries[0].sql, "ON CONFLICT",
		"Without metadata, ON CONFLICT cannot be generated — this is the pre-fix behavior that causes duplicates")
}
