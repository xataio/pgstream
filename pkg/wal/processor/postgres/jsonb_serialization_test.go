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

// TestFilterRowColumns_SQLNull_NullableJSONB_BecomesJSONBNull reproduces the
// bug where SQL NULL in a nullable JSONB column gets silently converted to the
// JSONB null literal ('null'::jsonb) during filterRowColumns serialization.
//
// These are semantically different values in PostgreSQL:
//   - SQL NULL:  column IS NULL → true,  column = 'null'::jsonb → NULL (unknown)
//   - JSONB null: column IS NULL → false, column = 'null'::jsonb → true
//
// The ARD-638 fix (nil → []byte("null")) correctly handles 'null'::jsonb in
// NOT NULL columns, but breaks nullable JSONB columns where SQL NULL is the
// intended value. Any CHECK constraint using "column IS NULL" will fail.
//
// Production: Robynn AI org_facts table — CHECK constraint "valid_state":
//
//	(user_approval = true  AND user_corrected_document_metadata IS NULL)
//	OR (user_approval = false AND user_corrected_document_metadata IS NOT NULL)
//
// 37 rows have user_approval=true with user_corrected_document_metadata as SQL
// NULL. pgstream COPY converts the NULL to 'null'::jsonb, making IS NULL false,
// violating the constraint. All 43 rows rejected.
func TestFilterRowColumns_SQLNull_NullableJSONB_BecomesJSONBNull(t *testing.T) {
	t.Parallel()

	// Simulates a snapshot row from org_facts where user_approval=true.
	// The user_corrected_document_metadata column is nullable JSONB with SQL NULL.
	// In the snapshot path, pgx rows.Values() returns Go nil for SQL NULL.
	// The snapshot adapter detects SQL NULL via rawValues (nil raw bytes) and
	// sets IsNull=true on the Column.
	cols := []wal.Column{
		{Name: "id", Type: "uuid", Value: "8207b5df-e961-4c62-a2ad-4dcd149f07d5"},
		{Name: "organization_id", Type: "uuid", Value: "fab355bc-d7e3-462c-befc-880a5a8a5bd7"},
		{Name: "user_approval", Type: "bool", Value: true},
		{Name: "generated_document_metadata", Type: "jsonb", Value: map[string]any{"category": "Meeting"}},
		{Name: "user_corrected_document_name", Type: "text", Value: nil, IsSQLNull: true},      // SQL NULL
		{Name: "user_corrected_document_value", Type: "text", Value: nil, IsSQLNull: true},     // SQL NULL
		{Name: "user_corrected_document_metadata", Type: "jsonb", Value: nil, IsSQLNull: true}, // SQL NULL — must stay nil
	}

	_, values := (&dmlAdapter{}).filterRowColumns(cols, schemaInfo{})

	// Non-JSONB SQL NULL columns should pass through as nil (they do)
	require.Nil(t, values[4], "SQL NULL text column should stay nil")
	require.Nil(t, values[5], "SQL NULL text column should stay nil")

	// generated_document_metadata (non-null JSONB object) should be serialized
	require.IsType(t, []byte{}, values[3], "Non-null JSONB object should serialize to []byte")

	// BUG: SQL NULL JSONB column gets converted to []byte("null") by serializeJSONBValue.
	// This makes PostgreSQL see 'null'::jsonb (IS NOT NULL = true) instead of SQL NULL
	// (IS NULL = true), breaking the CHECK constraint.
	require.Nil(t, values[6],
		"SQL NULL in nullable JSONB column must stay nil for COPY (so PostgreSQL sees IS NULL = true), "+
			"but serializeJSONBValue converts it to []byte(\"null\") which is JSONB null literal (IS NULL = false)")
}

// TestFilterRowColumns_CheckConstraint_OrgFactsScenario models the exact
// org_facts production failure end-to-end. Both row variants must serialize
// correctly for the CHECK constraint to pass.
func TestFilterRowColumns_CheckConstraint_OrgFactsScenario(t *testing.T) {
	t.Parallel()

	// Row variant 1: user_approval=true, all corrected fields SQL NULL
	// CHECK requires: user_corrected_document_metadata IS NULL
	// Snapshot adapter sets IsNull=true because pgx rawValues are nil for SQL NULL.
	approvedRow := []wal.Column{
		{Name: "id", Type: "uuid", Value: "8207b5df-e961-4c62-a2ad-4dcd149f07d5"},
		{Name: "user_approval", Type: "bool", Value: true},
		{Name: "user_corrected_document_metadata", Type: "jsonb", Value: nil, IsSQLNull: true}, // SQL NULL
	}

	_, approvedValues := (&dmlAdapter{}).filterRowColumns(approvedRow, schemaInfo{})

	// For user_approval=true: metadata MUST be nil (SQL NULL) to satisfy IS NULL check
	require.Nil(t, approvedValues[2],
		"approved row: SQL NULL JSONB must stay nil — CHECK requires IS NULL")

	// Row variant 2: user_approval=false, corrected fields populated
	// CHECK requires: user_corrected_document_metadata IS NOT NULL
	correctedRow := []wal.Column{
		{Name: "id", Type: "uuid", Value: "b7f900b7-f6db-42ae-90ef-4080dd6fb744"},
		{Name: "user_approval", Type: "bool", Value: false},
		{Name: "user_corrected_document_metadata", Type: "jsonb", Value: map[string]any{}}, // empty JSONB object {}
	}

	_, correctedValues := (&dmlAdapter{}).filterRowColumns(correctedRow, schemaInfo{})

	// For user_approval=false: metadata MUST be non-nil to satisfy IS NOT NULL check
	require.NotNil(t, correctedValues[2],
		"corrected row: empty JSONB object {} must serialize to non-nil []byte")
	jsonbBytes, ok := correctedValues[2].([]byte)
	require.True(t, ok, "empty JSONB object should serialize to []byte, got %T", correctedValues[2])
	require.Equal(t, "{}", string(jsonbBytes),
		"empty JSONB object must round-trip as {}")
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
