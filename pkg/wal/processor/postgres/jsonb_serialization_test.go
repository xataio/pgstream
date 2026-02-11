// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

func TestFilterRowColumnsJSONBHandling(t *testing.T) {
	t.Parallel()

	var jsonbValue map[string]any
	require.NoError(t, sonicjson.Unmarshal([]byte(`{
		"name": "David Richard 🏳️‍🌈",
		"location": "São Paulo",
		"quote": "said \"hello\""
	}`), &jsonbValue))

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "data", Type: "jsonb", Value: jsonbValue},
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	jsonbResult, ok := values[1].([]byte)
	require.True(t, ok, "JSONB map should be pre-serialized to []byte, got %T", values[1])

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Equal(t, "David Richard 🏳️‍🌈", parsed["name"])
}

func TestFilterRowColumnsJSONBStringSerializedToBytes(t *testing.T) {
	t.Parallel()

	// A Go string in a JSONB column means jsonb_typeof='string' from wal2json.
	// It must be JSON-marshaled to []byte for correct COPY encoding.
	// (The old schemalog snapshot generator that passed object-strings was
	// removed in v1.0.0 — Go strings in JSONB columns are always JSON strings.)
	originalJSON := `{"tables":[{"name":"users"}]}`

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "schema", Type: "jsonb", Value: originalJSON},
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	// String values must be serialized to []byte for COPY binary format
	result, ok := values[1].([]byte)
	require.True(t, ok, "JSONB string should be serialized to []byte, got %T", values[1])
	require.True(t, json.Valid(result), "serialized JSONB string must be valid JSON")

	// Unmarshal should recover the original string
	var decoded string
	require.NoError(t, json.Unmarshal(result, &decoded))
	require.Equal(t, originalJSON, decoded)
}

func TestFilterRowColumnsJSONBArrayHandling(t *testing.T) {
	t.Parallel()

	var jsonbValue []any
	require.NoError(t, sonicjson.Unmarshal([]byte(`[
		{"name": "item1", "emoji": "🎉"},
		{"name": "item2", "location": "São Paulo"}
	]`), &jsonbValue))

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "items", Type: "jsonb", Value: jsonbValue},
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	jsonbResult, ok := values[1].([]byte)
	require.True(t, ok, "JSONB array should be pre-serialized to []byte, got %T", values[1])

	var parsed []any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Len(t, parsed, 2)
	emoji, ok := parsed[0].(map[string]any)["emoji"]
	require.True(t, ok, "first array item should have emoji field")
	require.Equal(t, "🎉", emoji)
}

// TestSerializeJSONBNullLiteral reproduces ARD-638: 'null'::jsonb is a valid
// non-NULL JSONB value (jsonb_typeof='null'), but wal2json decodes it as Go nil.
// The old serializeJSONBValue skips nil entirely (val != nil guard), so during
// COPY nil becomes SQL NULL (\N), violating NOT NULL constraints.
//
// Production: Robynn AI connector_59a59e6a, agent_executions_v2.metadata column.
func TestSerializeJSONBNullLiteral(t *testing.T) {
	t.Parallel()

	// wal2json decodes 'null'::jsonb as Go nil
	result := serializeJSONBValue("jsonb", nil)

	// Must produce []byte("null"), NOT Go nil (which becomes SQL NULL in COPY)
	jsonbBytes, ok := result.([]byte)
	require.True(t, ok, "JSONB null literal must serialize to []byte, got %T (nil=%v)", result, result == nil)
	require.Equal(t, "null", string(jsonbBytes))
}

// TestSerializeJSONBNullLiteralInRow reproduces ARD-638 end-to-end through
// filterRowColumns, mirroring the exact seed data from our integration test:
// 3 normal rows + 2 rows with 'null'::jsonb in a NOT NULL column.
func TestSerializeJSONBNullLiteralInRow(t *testing.T) {
	t.Parallel()

	// Mirrors seed_null_literal.py: json_null_row_1 with metadata = 'null'::jsonb
	cols := []wal.Column{
		{Name: "id", Type: "uuid", Value: "550e8400-e29b-41d4-a716-446655440000"},
		{Name: "name", Type: "text", Value: "json_null_row_1"},
		{Name: "metadata", Type: "jsonb", Value: nil}, // 'null'::jsonb from wal2json
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	// metadata must NOT be Go nil — it must be []byte("null")
	require.NotNil(t, values[2], "JSONB null literal must not become Go nil (would be SQL NULL in COPY)")
	jsonbBytes, ok := values[2].([]byte)
	require.True(t, ok, "JSONB null literal must serialize to []byte, got %T", values[2])
	require.Equal(t, "null", string(jsonbBytes))
}

// TestSerializeJSONBStringType reproduces ARD-636: JSONB columns with
// jsonb_typeof='string' (e.g. '"hello"'::jsonb) arrive from wal2json as
// Go string. The old serializeJSONBValue only handles map/slice, so the
// string passes through raw, and during COPY pgx sends it without proper
// JSON quoting → "invalid input syntax for type json".
//
// Production: Zennagents connector_cf9431c6, brightdata_profile_relevance_process_log
// on silver_linkedin_profiles* tables, values up to 137KB.
func TestSerializeJSONBStringType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string // Go string as wal2json would produce
	}{
		{
			name:  "simple string",
			input: "line1\nline2\nline3",
		},
		{
			name:  "nested escaped JSON",
			input: "Starting workflow\n{\"key\":\"value\",\"nested\":{\"num\":42}}",
		},
		{
			name:  "deeply escaped - production pattern",
			input: "Starting workflow: profile_evaluator\nInitial context: {\n  \"mode\": \"evaluator\",\n  \"data\": \"{\\\"input\\\":{\\\"url\\\":\\\"https://example.com/profile\\\"},\\\"name\\\":\\\"Test User\\\",\\\"city\\\":\\\"San Francisco\\\"}\"\n}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := serializeJSONBValue("jsonb", tc.input)

			// Must produce []byte (JSON-encoded string), NOT raw Go string
			jsonbBytes, ok := result.([]byte)
			require.True(t, ok, "JSONB string value must serialize to []byte, got %T", result)

			// The []byte must be valid JSON (a quoted string)
			require.True(t, json.Valid(jsonbBytes), "serialized JSONB string must be valid JSON, got: %s", string(jsonbBytes))

			// Unmarshal back and confirm round-trip
			var decoded string
			require.NoError(t, json.Unmarshal(jsonbBytes, &decoded))
			require.Equal(t, tc.input, decoded, "round-trip through JSON marshal/unmarshal must preserve value")
		})
	}
}

// TestSerializeJSONBStringTypeInRow reproduces ARD-636 end-to-end through
// filterRowColumns with the exact deeply-escaped production pattern.
func TestSerializeJSONBStringTypeInRow(t *testing.T) {
	t.Parallel()

	// Mirrors seed_string_type.py row 4: deeply_escaped
	deeplyEscaped := "Starting workflow: profile_evaluator\nInitial context: {\n  \"mode\": \"evaluator\",\n  \"data\": \"{\\\"input\\\":{\\\"url\\\":\\\"https://example.com/profile\\\"},\\\"name\\\":\\\"Test User\\\",\\\"city\\\":\\\"San Francisco\\\"}\"\n}"

	cols := []wal.Column{
		{Name: "id", Type: "uuid", Value: "550e8400-e29b-41d4-a716-446655440001"},
		{Name: "name", Type: "text", Value: "deeply_escaped"},
		{Name: "log_data", Type: "jsonb", Value: deeplyEscaped}, // string from wal2json
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	// log_data must be []byte (valid JSON), not raw Go string
	jsonbBytes, ok := values[2].([]byte)
	require.True(t, ok, "JSONB string value must serialize to []byte for COPY, got %T", values[2])
	require.True(t, json.Valid(jsonbBytes), "serialized value must be valid JSON")
}

func TestBuildWhereQueryJSONBHandling(t *testing.T) {
	t.Parallel()

	// Simulates REPLICA IDENTITY FULL with JSONB column in identity
	var jsonbValue map[string]any
	require.NoError(t, sonicjson.Unmarshal([]byte(`{"name": "test 🎉"}`), &jsonbValue))

	d := &wal.Data{
		Identity: []wal.Column{
			{Name: "id", Type: "integer", Value: 1},
			{Name: "data", Type: "jsonb", Value: jsonbValue},
		},
	}

	adapter := &dmlAdapter{}
	_, whereValues, err := adapter.buildWhereQuery(d, 0)
	require.NoError(t, err)

	// JSONB in WHERE clause should also be pre-serialized
	jsonbResult, ok := whereValues[1].([]byte)
	require.True(t, ok, "JSONB in WHERE should be pre-serialized to []byte, got %T", whereValues[1])

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Equal(t, "test 🎉", parsed["name"])
}
