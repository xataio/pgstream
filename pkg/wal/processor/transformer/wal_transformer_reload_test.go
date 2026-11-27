// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
)

const reloadTestTimeout = 5 * time.Second

// reloadTestTransformer is a minimal transformer whose Transform and Close
// behavior can be controlled by the test.
type reloadTestTransformer struct {
	transformFn func(context.Context, transformers.Value) (any, error)
	closeFn     func() error
	closeCalls  atomic.Int32
}

func (t *reloadTestTransformer) Transform(ctx context.Context, value transformers.Value) (any, error) {
	return t.transformFn(ctx, value)
}

func (t *reloadTestTransformer) IsDynamic() bool {
	return false
}

func (t *reloadTestTransformer) CompatibleTypes() []transformers.SupportedDataType {
	return []transformers.SupportedDataType{transformers.AllDataTypes}
}

func (t *reloadTestTransformer) Type() transformers.TransformerType {
	return "reload_test"
}

func (t *reloadTestTransformer) Close() error {
	t.closeCalls.Add(1)
	if t.closeFn != nil {
		return t.closeFn()
	}
	return nil
}

func reloadTestConfig(marker string) *Config {
	return &Config{
		TransformerRules: []TableRules{
			{
				Schema: "test_schema",
				Table:  "test_table",
				ColumnRules: map[string]TransformerRules{
					"column_1": {
						Name:       "string",
						Parameters: map[string]any{"marker": marker},
					},
				},
			},
		},
		ValidationMode: validationModeRelaxed,
	}
}

func reloadTestMap(transformer transformers.Transformer) *TransformerMap {
	transformerMap := NewTransformerMap()
	transformerMap.AddActiveTransformer("test_schema", "test_table", "column_1", transformer)
	return transformerMap
}

func reloadTestEvent(value string) *wal.Event {
	return &wal.Event{
		Data: &wal.Data{
			Action: "I",
			Schema: "test_schema",
			Table:  "test_table",
			Columns: []wal.Column{
				{Name: "column_1", Type: "text", Value: value},
			},
		},
	}
}

func reloadTestProcessor(values chan<- any) *mocks.Processor {
	return &mocks.Processor{
		ProcessWALEventFn: func(_ context.Context, event *wal.Event) error {
			values <- event.Data.Columns[0].Value
			return nil
		},
	}
}

func waitForReloadTestValue(t *testing.T, values <-chan any) any {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(reloadTestTimeout):
		t.Fatal("timed out waiting for processed value")
		return nil
	}
}

func waitForReloadTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(reloadTestTimeout):
		t.Fatal("timed out waiting for signal")
	}
}

func TestTransformer_Reload_AppliesUpdatedRules(t *testing.T) {
	t.Parallel()

	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "old:" + value.TransformValue.(string), nil
		},
	}
	newTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "new:" + value.TransformValue.(string), nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		return reloadTestMap(newTransformer), nil
	}

	var loaderCalls atomic.Int32
	loader := func(context.Context) (*Config, error) {
		loaderCalls.Add(1)
		return reloadTestConfig("new"), nil
	}

	values := make(chan any, 2)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))

	changed, err := transformer.Reload(context.Background())
	require.NoError(t, err)
	require.True(t, changed)

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("two")))
	require.Equal(t, "new:two", waitForReloadTestValue(t, values))
	require.Equal(t, int32(2), parserCalls.Load())
	require.Equal(t, int32(1), loaderCalls.Load())
}

func TestTransformer_Reload_InvalidCandidateKeepsActiveRules(t *testing.T) {
	t.Parallel()

	errInvalid := errors.New("invalid candidate")
	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "old:" + value.TransformValue.(string), nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		return nil, errInvalid
	}
	loader := func(context.Context) (*Config, error) {
		return reloadTestConfig("new"), nil
	}

	values := make(chan any, 1)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.ErrorIs(t, err, errInvalid)
	require.False(t, changed)

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))
	require.Equal(t, int32(0), oldTransformer.closeCalls.Load())
}

func TestTransformer_Reload_LoaderErrorKeepsActiveRules(t *testing.T) {
	t.Parallel()

	errLoad := errors.New("loading candidate")
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
		return nil, errLoad
	}

	values := make(chan any, 1)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.ErrorIs(t, err, errLoad)
	require.False(t, changed)

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))
	require.Equal(t, int32(1), parserCalls.Load())
}

func TestTransformer_Reload_UnchangedRulesDoNotRebuild(t *testing.T) {
	t.Parallel()

	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return value.TransformValue, nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		parserCalls.Add(1)
		return reloadTestMap(oldTransformer), nil
	}
	var loaderCalls atomic.Int32
	loader := func(context.Context) (*Config, error) {
		loaderCalls.Add(1)
		// A fresh value with the same contents must be treated as unchanged.
		return reloadTestConfig("old"), nil
	}

	transformer, err := New(context.Background(), reloadTestConfig("old"), &mocks.Processor{}, nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, int32(1), loaderCalls.Load())
	require.Equal(t, int32(1), parserCalls.Load())
}

func TestTransformer_Reload_ClosesFailedCandidate(t *testing.T) {
	t.Parallel()

	errInvalid := errors.New("invalid candidate")
	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "old:" + value.TransformValue.(string), nil
		},
	}
	candidateTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return value, nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		// Return a partially built candidate alongside the validation error.
		return reloadTestMap(candidateTransformer), errInvalid
	}
	loader := func(context.Context) (*Config, error) {
		return reloadTestConfig("new"), nil
	}

	values := make(chan any, 1)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.ErrorIs(t, err, errInvalid)
	require.False(t, changed)
	require.Equal(t, int32(1), candidateTransformer.closeCalls.Load())
	require.Equal(t, int32(0), oldTransformer.closeCalls.Load())

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))
}

func TestTransformer_Reload_DoesNotCloseRetiredRulesDuringEvent(t *testing.T) {
	t.Parallel()

	eventEntered := make(chan struct{})
	releaseEvent := make(chan struct{})
	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			close(eventEntered)
			<-releaseEvent
			return "old:" + value.TransformValue.(string), nil
		},
	}
	newTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "new:" + value.TransformValue.(string), nil
		},
	}

	candidateBuilt := make(chan struct{})
	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		candidate := reloadTestMap(newTransformer)
		close(candidateBuilt)
		return candidate, nil
	}
	loader := func(context.Context) (*Config, error) {
		return reloadTestConfig("new"), nil
	}

	values := make(chan any, 2)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	eventDone := make(chan error, 1)
	go func() {
		eventDone <- transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one"))
	}()
	waitForReloadTestSignal(t, eventEntered)

	reloadDone := make(chan error, 1)
	var changed bool
	go func() {
		var err error
		changed, err = transformer.Reload(context.Background())
		reloadDone <- err
	}()
	waitForReloadTestSignal(t, candidateBuilt)

	// The old transformer is still in use by the blocked event, so it must not
	// have been closed after the candidate was built.
	require.Equal(t, int32(0), oldTransformer.closeCalls.Load())

	close(releaseEvent)
	require.NoError(t, <-eventDone)
	require.Equal(t, "old:one", waitForReloadTestValue(t, values))
	require.NoError(t, <-reloadDone)
	require.True(t, changed)
	require.Equal(t, int32(1), oldTransformer.closeCalls.Load())

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("two")))
	require.Equal(t, "new:two", waitForReloadTestValue(t, values))
}

func TestTransformer_Reload_AfterCloseReturnsError(t *testing.T) {
	t.Parallel()

	var loaderCalls atomic.Int32
	loader := func(context.Context) (*Config, error) {
		loaderCalls.Add(1)
		return reloadTestConfig("new"), nil
	}
	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		parserCalls.Add(1)
		return NewTransformerMap(), nil
	}

	transformer, err := New(context.Background(), reloadTestConfig("old"), &mocks.Processor{}, nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)
	require.NoError(t, transformer.Close())

	changed, err := transformer.Reload(context.Background())
	require.Error(t, err)
	require.False(t, changed)
	require.Equal(t, int32(0), loaderCalls.Load())
	require.Equal(t, int32(1), parserCalls.Load())
}

func TestTransformer_Reload_RetirementErrorKeepsNewRulesActive(t *testing.T) {
	t.Parallel()

	errClose := errors.New("closing retired transformer")
	oldTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "old:" + value.TransformValue.(string), nil
		},
		closeFn: func() error {
			return errClose
		},
	}
	newTransformer := &reloadTestTransformer{
		transformFn: func(_ context.Context, value transformers.Value) (any, error) {
			return "new:" + value.TransformValue.(string), nil
		},
	}

	var parserCalls atomic.Int32
	parser := func(context.Context, Rules) (*TransformerMap, error) {
		if parserCalls.Add(1) == 1 {
			return reloadTestMap(oldTransformer), nil
		}
		return reloadTestMap(newTransformer), nil
	}
	loader := func(context.Context) (*Config, error) {
		return reloadTestConfig("new"), nil
	}

	values := make(chan any, 1)
	transformer, err := New(context.Background(), reloadTestConfig("old"), reloadTestProcessor(values), nil,
		WithParser(parser), WithConfigLoader(loader))
	require.NoError(t, err)

	changed, err := transformer.Reload(context.Background())
	require.ErrorIs(t, err, errClose)
	require.True(t, changed)
	require.Equal(t, int32(1), oldTransformer.closeCalls.Load())

	require.NoError(t, transformer.ProcessWALEvent(context.Background(), reloadTestEvent("one")))
	require.Equal(t, "new:one", waitForReloadTestValue(t, values))
}
