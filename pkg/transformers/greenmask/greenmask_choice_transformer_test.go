// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"context"
	"testing"

	"github.com/eminano/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewChoiceTransformer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  transformers.ParameterValues
		wantErr error
	}{
		{
			name: "ok - valid random",
			params: transformers.ParameterValues{
				"generator": random,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name: "error - invalid generator type",
			params: transformers.ParameterValues{
				"generator": "invalid",
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: transformers.ErrUnsupportedGenerator,
		},
		{
			name: "error - invalid choices",
			params: transformers.ParameterValues{
				"generator": deterministic,
			},
			wantErr: errChoicesEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewChoiceTransformer(tt.params)
			require.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, transformer)
		})
	}
}

func TestChoiceTransformer_Transform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   any
		params  transformers.ParameterValues
		wantErr error
	}{
		{
			name:  "ok - transform string randomly",
			input: "test",
			params: transformers.ParameterValues{
				"generator": random,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform []byte deterministically",
			input: []byte("test"),
			params: transformers.ParameterValues{
				"generator": deterministic,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform RawValue deterministically",
			input: toolkit.NewRawValue([]byte("test"), false),
			params: transformers.ParameterValues{
				"generator": deterministic,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: nil,
		},
		{
			name:  "ok - transform an enum label deterministically",
			input: "sad",
			params: transformers.ParameterValues{
				"generator": deterministic,
				"choices":   []string{"sad", "ok", "happy"},
			},
			wantErr: nil,
		},
		{
			name:  "error - invalid input type",
			input: 1,
			params: transformers.ParameterValues{
				"generator": random,
				"choices":   []string{"a", "b", "c", "d"},
			},
			wantErr: transformers.ErrUnsupportedValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewChoiceTransformer(tt.params)
			require.NoError(t, err)
			require.NotNil(t, transformer)
			got, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
			require.Equal(t, tt.wantErr, err)
			if err != nil {
				return
			}

			// the chosen label comes back as the same kind of value it went
			// in as, so an enum column receives a label rather than its hex
			// encoding
			require.NotNil(t, got)
			var val string
			switch out := got.(type) {
			case string:
				require.IsType(t, "", tt.input, "a string input must produce a string")
				val = out
			case []byte:
				_, inputWasString := tt.input.(string)
				require.False(t, inputWasString, "a non string input must produce bytes")
				val = string(out)
			default:
				t.Fatalf("unexpected transform output type %T", got)
			}
			require.Contains(t, tt.params["choices"], val)

			// if deterministic, check if we get the same result again
			if mustGetGeneratorType(t, tt.params) == deterministic {
				gotAgain, err := transformer.Transform(context.Background(), transformers.Value{TransformValue: tt.input})
				require.NoError(t, err)
				require.Equal(t, got, gotAgain)
			}
		})
	}
}
