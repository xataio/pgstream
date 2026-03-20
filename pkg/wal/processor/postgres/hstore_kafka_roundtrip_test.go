// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

// TestFilterRowColumns_Hstore_SnapshotKafkaRoundTrip_MustBeEncodable verifies
// that hstore values survive the Kafka JSON round-trip in a form pgx can encode.
//
// pgx returns hstore as map[string]*string (pgtype.Hstore). After the Kafka
// round-trip (json.Marshal → json.Unmarshal), it becomes map[string]interface{}.
// pgx's HstoreCodec.PlanEncode checks if the value implements HstoreValuer.
// map[string]interface{} does NOT implement HstoreValuer → PlanEncode returns
// nil → pgx has no encode plan → INSERT fails → row lost.
//
// filterRowColumns must convert map[string]interface{} back to
// map[string]*string (pgtype.Hstore) so pgx can encode it.
func TestFilterRowColumns_Hstore_SnapshotKafkaRoundTrip_MustBeEncodable(t *testing.T) {
	t.Parallel()

	// Step 1: Construct a wal.Data as the snapshot adapter produces.
	// pgx returns hstore as map[string]*string (pgtype.Hstore) via HstoreCodec
	// registered in registerTypesToConnMap (pg_utils.go line 153-161).
	val1 := "value1"
	val2 := "value2"

	originalData := wal.Data{
		Action: "I",
		Schema: "public",
		Table:  "hstore_data",
		Columns: []wal.Column{
			{Name: "id", Type: "integer", Value: int32(1)}, // pgx Int4Codec.DecodeValue returns int32
			{Name: "data", Type: "hstore", Value: map[string]*string{
				"key1": &val1,
				"key2": &val2,
				"key3": nil,
			}},
		},
	}

	// Step 2: Kafka round-trip (pg2kafka json.Marshal → kafka2pg json.Unmarshal)
	kafkaBytes, err := sonicjson.Marshal(&originalData)
	require.NoError(t, err)

	var roundTripped wal.Data
	require.NoError(t, sonicjson.Unmarshal(kafkaBytes, &roundTripped))

	// Step 3: Pass through filterRowColumns (the real kafka2pg code path)
	adapter := &dmlAdapter{forCopy: false}
	_, values := adapter.filterRowColumns(roundTripped.Columns, schemaInfo{})

	hstoreResult := values[1]

	// Step 4: Verify pgx can ACTUALLY encode the result.
	// This is the real pgx encoder — not a mock, not an assumption.
	// HstoreCodec.PlanEncode returns nil if the value doesn't implement HstoreValuer.
	codec := pgtype.HstoreCodec{}
	typeMap := pgtype.NewMap()

	// Look up the hstore OID — use a known test OID since we don't have a live DB
	// pgtype uses OID matching, but PlanEncode primarily checks the HstoreValuer interface.
	// We use TextFormatCode since that's what parameterized INSERT uses.
	encodePlan := codec.PlanEncode(typeMap, 0, pgtype.TextFormatCode, hstoreResult)

	require.NotNil(t, encodePlan,
		"pgx HstoreCodec.PlanEncode must return a non-nil plan for the value after "+
			"Kafka round-trip + filterRowColumns. Got nil — pgx can't encode %T. "+
			"This causes INSERT failure and data loss.", hstoreResult)
}

// TestFilterRowColumns_Hstore_WALPath_StringPassthrough verifies that the
// WAL/CDC path for hstore works correctly. wal2json emits hstore as a
// text string. This string survives the Kafka round-trip and pgx can
// handle it when the hstore type is registered.
func TestFilterRowColumns_Hstore_WALPath_StringPassthrough(t *testing.T) {
	t.Parallel()

	wal2jsonHstore := `"key1"=>"value1", "key2"=>"value2", "key3"=>NULL`

	originalData := wal.Data{
		Action: "I",
		Schema: "public",
		Table:  "hstore_data",
		Columns: []wal.Column{
			{Name: "id", Type: "integer", Value: int32(1)}, // pgx Int4Codec.DecodeValue returns int32
			{Name: "data", Type: "hstore", Value: wal2jsonHstore},
		},
	}

	kafkaBytes, err := sonicjson.Marshal(&originalData)
	require.NoError(t, err)

	var roundTripped wal.Data
	require.NoError(t, sonicjson.Unmarshal(kafkaBytes, &roundTripped))

	adapter := &dmlAdapter{forCopy: false}
	_, values := adapter.filterRowColumns(roundTripped.Columns, schemaInfo{})

	result, isString := values[1].(string)
	require.True(t, isString,
		"WAL path hstore should pass through as string (pgx handles text format)")
	require.Contains(t, result, "key1")
}
