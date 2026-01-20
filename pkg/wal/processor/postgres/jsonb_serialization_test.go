// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	sonicjson "github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/wal"
)

// TestFilterRowColumnsJSONBHandling verifies JSONB columns are pre-serialized
// with Sonic to prevent encoding mismatches with pgx (which uses encoding/json).
func TestFilterRowColumnsJSONBHandling(t *testing.T) {
	t.Parallel()

	// JSONB with emoji, unicode, and quotes - the problematic cases
	var jsonbValue map[string]any
	err := sonicjson.Unmarshal([]byte(`{
		"name": "David Richard 🏳️‍🌈",
		"location": "São Paulo",
		"quote": "said \"hello\""
	}`), &jsonbValue)
	require.NoError(t, err)

	cols := []wal.Column{
		{Name: "id", Type: "integer", Value: 1},
		{Name: "data", Type: "jsonb", Value: jsonbValue},
	}

	adapter := &dmlAdapter{forCopy: false}
	_, values := adapter.filterRowColumns(cols, schemaInfo{})

	// After fix: JSONB should be []byte (pre-serialized), not map[string]any
	jsonbResult, ok := values[1].([]byte)
	require.True(t, ok, "JSONB should be pre-serialized to []byte, got %T", values[1])

	// Verify it's valid JSON
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(jsonbResult, &parsed))
	require.Equal(t, "David Richard 🏳️‍🌈", parsed["name"])
}
