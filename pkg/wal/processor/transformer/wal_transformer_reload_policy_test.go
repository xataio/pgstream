// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

func reloadTestDDLEvent() *wal.Event {
	return &wal.Event{
		Data: &wal.Data{
			Action: wal.LogicalMessageAction,
			Prefix: wal.DDLPrefix,
		},
	}
}

func reloadTestProcessorAllowingEvents() *mocks.Processor {
	return &mocks.Processor{
		ProcessWALEventFn: func(context.Context, *wal.Event) error { return nil },
	}
}

func TestTransformer_Reload_AppliesGlobalValidationPolicyChange(t *testing.T) {
	t.Parallel()

	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return value.TransformValue, nil
		},
	}
	newTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return value.TransformValue, nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		return reloadTestMap(newTransformer), nil
	}

	candidate := reloadTestConfig("new")
	candidate.ValidationMode = validationModeStrict
	loader := func(context.Context) (*Config, error) {
		return candidate, nil
	}

	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessorAllowingEvents(), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)
	transformer.walDataToDDLEvent = func(*wal.Data) (*wal.DDLEvent, error) {
		return &wal.DDLEvent{}, nil
	}
	transformer.ddlEventToSchemaDiff = func(*wal.DDLEvent) (*wal.SchemaDiff, error) {
		return &wal.SchemaDiff{
			SchemaName: "public",
			TablesAdded: []wal.DDLObject{
				{Type: "table", Identity: "new_table", Schema: "public"},
			},
		}, nil
	}

	// Relaxed mode allows the DDL before the policy change.
	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestDDLEvent()))

	changed, err := transformer.Reload(context.Background())
	require.NoError(t, err)
	require.True(t, changed)

	// Strict mode now rejects the same uncovered table.
	require.ErrorIs(t,
		transformer.ProcessWALEvent(context.Background(), reloadTestDDLEvent()),
		errDDLNotSupportedInStrictMode)
}

func TestTransformer_Reload_AppliesTableValidationPolicyChange(t *testing.T) {
	t.Parallel()

	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return value.TransformValue, nil
		},
	}
	newTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return value.TransformValue, nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		return reloadTestMap(newTransformer), nil
	}

	initial := reloadTestConfig("old")
	initial.ValidationMode = validationModeTableLevel
	initial.TransformerRules[0].ValidationMode = validationModeStrict

	candidate := reloadTestConfig("new")
	candidate.ValidationMode = validationModeTableLevel
	candidate.TransformerRules[0].ValidationMode = validationModeRelaxed
	loader := func(context.Context) (*Config, error) {
		return candidate, nil
	}

	transformer, err := New(context.Background(), initial, reloadTestProcessorAllowingEvents(), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)
	transformer.walDataToDDLEvent = func(*wal.Data) (*wal.DDLEvent, error) {
		return &wal.DDLEvent{}, nil
	}
	transformer.ddlEventToSchemaDiff = func(*wal.DDLEvent) (*wal.SchemaDiff, error) {
		return &wal.SchemaDiff{
			SchemaName: "test_schema",
			TablesChanged: []wal.TableDiff{
				{
					TableName: "test_table",
					ColumnsAdded: []wal.DDLColumn{
						{Name: "new_column"},
					},
				},
			},
		}, nil
	}

	// The table is strict before the reload.
	require.ErrorIs(t,
		transformer.ProcessWALEvent(context.Background(), reloadTestDDLEvent()),
		errDDLNotSupportedInStrictMode)

	changed, err := transformer.Reload(context.Background())
	require.NoError(t, err)
	require.True(t, changed)

	// The same table is relaxed after the reload.
	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestDDLEvent()))
}

func TestTransformer_Reload_RejectsChangesToFixedSettings(t *testing.T) {
	t.Parallel()

	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "old:" + value.TransformValue.(string), nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		parserCalls.Add(1)
		return reloadTestMap(oldTransformer), nil
	}

	initial := reloadTestConfig("old")
	initial.OnError = OnErrorNull
	candidate := reloadTestConfig("new")
	candidate.OnError = OnErrorFail
	loader := func(context.Context) (*Config, error) {
		return candidate, nil
	}

	values := make(chan any, 1)
	transformer, err := New(context.Background(), initial, reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.Error(t, err)
	require.False(t, changed)
	require.Equal(t, OnErrorNull, transformer.onError)
	require.Equal(t, int32(1), parserCalls.Load())

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))
}

func TestTransformer_Reload_RejectsEmptyTransformationRules(t *testing.T) {
	t.Parallel()

	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "old:" + value.TransformValue.(string), nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		parserCalls.Add(1)
		return reloadTestMap(oldTransformer), nil
	}
	loader := func(context.Context) (*Config, error) {
		return &Config{ValidationMode: validationModeRelaxed}, nil
	}

	values := make(chan any, 1)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.Error(t, err)
	require.False(t, changed)
	require.Equal(t, int32(1), parserCalls.Load())

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))
}
