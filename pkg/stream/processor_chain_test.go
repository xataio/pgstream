// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"go.opentelemetry.io/otel/metric/noop"
)

var errTestWrap = errors.New("wrap failed")

// select a writer, not a layer
var targetFields = map[string]struct{}{
	"Kafka":    {},
	"Search":   {},
	"Webhook":  {},
	"Postgres": {},
	"Stdout":   {},
}

// wrap a layer around the writer
var modifierFields = map[string]struct{}{
	"Transformer": {},
	"Injector":    {},
	"Filter":      {},
	"Sanitize":    {},
}

// unclassified layers get bypassed
func TestProcessorConfig_allFieldsClassified(t *testing.T) {
	t.Parallel()

	cfgType := reflect.TypeOf(ProcessorConfig{})
	for i := range cfgType.NumField() {
		name := cfgType.Field(i).Name
		_, isTarget := targetFields[name]
		_, isModifier := modifierFields[name]
		switch {
		case isTarget && isModifier:
			t.Errorf("ProcessorConfig field %q is classified as both a target and a modifier", name)
		case !isTarget && !isModifier:
			t.Errorf("unclassified ProcessorConfig field %q: add it to targetFields if it selects a target "+
				"writer, or to modifierFields and have addProcessorModifiers apply a modifier for it, after "+
				"deciding whether the layer needs to see every row", name)
		}
	}
}

func TestAddProcessorModifiers_hasRowVisibleLayers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  ProcessorConfig

		want bool
	}{
		{
			name: "no modifiers leaves the chain bypassable",
			cfg:  ProcessorConfig{},
			want: false,
		},
		{
			name: "sanitizer",
			cfg:  ProcessorConfig{Sanitize: &SanitizeConfig{StripNullCharBytes: true}},
			want: true,
		},
		{
			name: "sanitizer without null byte stripping is never applied",
			cfg:  ProcessorConfig{Sanitize: &SanitizeConfig{}},
			want: false,
		},
		{
			name: "transformer",
			cfg:  ProcessorConfig{Transformer: &transformer.Config{}},
			want: true,
		},
		{
			name: "filter",
			cfg:  ProcessorConfig{Filter: &filter.Config{IncludeTables: []string{"*"}}},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			chain, closer, err := addProcessorModifiers(t.Context(), &Config{Processor: tc.cfg},
				loglib.NewNoopLogger(), &mocks.Processor{}, nil)
			require.NoError(t, err)
			defer closer()

			require.Equal(t, tc.want, chain.hasRowVisibleLayers())
		})
	}
}

// instrumentation wraps without a config field
func TestAddProcessorModifiers_instrumentationIsRecordedButNotRowVisible(t *testing.T) {
	t.Parallel()

	instrumentation := &otel.Instrumentation{Meter: noop.NewMeterProvider().Meter("test")}
	require.True(t, instrumentation.IsEnabled())

	chain, closer, err := addProcessorModifiers(t.Context(), &Config{}, loglib.NewNoopLogger(),
		&mocks.Processor{}, instrumentation)
	require.NoError(t, err)
	defer closer()

	require.Equal(t, []modifier{modifierInstrumentation}, chain.applied)
	require.False(t, chain.hasRowVisibleLayers())
}

func TestProcessorChain_apply(t *testing.T) {
	t.Parallel()

	t.Run("records only what it wraps", func(t *testing.T) {
		t.Parallel()

		target := &mocks.Processor{}
		wrapped := &mocks.Processor{}
		chain := &processorChain{processor: target}

		require.NoError(t, chain.apply(modifierFilter, func(processor.Processor) (processor.Processor, error) {
			return wrapped, nil
		}))

		require.Same(t, wrapped, chain.processor)
		require.Equal(t, []modifier{modifierFilter}, chain.applied)
	})

	t.Run("a failed wrap leaves the chain untouched", func(t *testing.T) {
		t.Parallel()

		target := &mocks.Processor{}
		chain := &processorChain{processor: target}

		require.Error(t, chain.apply(modifierFilter, func(processor.Processor) (processor.Processor, error) {
			return nil, errTestWrap
		}))

		require.Same(t, target, chain.processor)
		require.Empty(t, chain.applied)
	})
}
