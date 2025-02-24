// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

func TestNewIntegerTransformer(t *testing.T) {
	tests := []struct {
		name        string
		generator   transformers.GeneratorType
		params      transformers.Parameters
		wantErr     bool
		errContains string
	}{
		{
			name:      "ok - valid default parameters",
			generator: transformers.Random,
			params:    transformers.Parameters{},
			wantErr:   false,
		},
		{
			name:      "ok - valid custom parameters",
			generator: transformers.Random,
			params: transformers.Parameters{
				"size":      4,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
			wantErr: false,
		},
		{
			name:      "error - invalid size, too small",
			generator: transformers.Random,
			params: transformers.Parameters{
				"size": 0,
			},
			wantErr:     true,
			errContains: "size must be greater than 0",
		},
		{
			name:      "error - invalid size, too large",
			generator: transformers.Random,
			params: transformers.Parameters{
				"size": 9,
			},
			wantErr:     true,
			errContains: "size must be less than or equal to 8",
		},
		{
			name:      "error - wrong limits",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": int64(100),
				"max_value": int64(99),
			},
			wantErr:     true,
			errContains: "wrong limits",
		},
		{
			name:      "error - invalid size type",
			generator: transformers.Random,
			params: transformers.Parameters{
				"size": "invalid",
			},
			wantErr:     true,
			errContains: "size must be an integer",
		},
		{
			name:      "error - invalid min_value type",
			generator: transformers.Random,
			params: transformers.Parameters{
				"min_value": "invalid",
			},
			wantErr:     true,
			errContains: "min_value must be an integer",
		},
		{
			name:      "error - invalid max_value type",
			generator: transformers.Random,
			params: transformers.Parameters{
				"max_value": "invalid",
			},
			wantErr:     true,
			errContains: "max_value must be an integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewIntegerTransformer(tt.generator, tt.params)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewIntegerTransformer() expected error containing %v, got nil", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewIntegerTransformer() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewIntegerTransformer() unexpected error = %v", err)
				return
			}
			if transformer == nil {
				t.Error("NewIntegerTransformer() returned nil transformer")
			}
		})
	}
}

func TestIntegerTransformer_Transform(t *testing.T) {
	tests := []struct {
		name          string
		generatorType transformers.GeneratorType
		input         any
		params        transformers.Parameters
		wantErr       bool
		result        int64
	}{
		{
			name:          "ok - transform int8 randomly",
			generatorType: transformers.Random,
			input:         int8(120),
			params: map[string]any{
				"size":      8,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
			wantErr: false,
		},
		{
			name:          "ok - transform uint8 randomly",
			generatorType: transformers.Random,
			input:         uint8(120),
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
			wantErr: false,
		},
		{
			name:          "ok - transform []byte randomly",
			generatorType: transformers.Random,
			input:         []byte{0, 0, 0, 50},
			params: map[string]any{
				"size":      4,
				"min_value": int64(-100),
				"max_value": int64(500),
			},
			wantErr: false,
		},
		{
			name:          "ok - transform int16 randomly",
			generatorType: transformers.Random,
			input:         int16(500),
			params: map[string]any{
				"size":      8,
				"min_value": int64(-400),
				"max_value": int64(100),
			},
			wantErr: false,
		},
		{
			name:          "ok - transform int64 randomly with default params",
			generatorType: transformers.Random,
			input:         int64(500),
			wantErr:       false,
		},
		{
			name:          "ok - transform []byte randomly with default params",
			generatorType: transformers.Random,
			input:         []byte{0, 0, 0, 50},
			wantErr:       false,
		},
		{
			name:          "ok - transform []byte randomly with default params, oversize",
			generatorType: transformers.Random,
			input:         []byte{0, 0, 0, 0, 0, 0, 0, 50},
			wantErr:       false,
		},
		{
			name:          "ok - transform int deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         int(500),
			wantErr:       false,
			result:        2035278536,
		},
		{
			name:          "ok - transform uint32 deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         uint32(500000000),
			params:        map[string]any{},
			wantErr:       false,
			result:        -722347270,
		},
		{
			name:          "ok - transform []byte deterministically with default params",
			generatorType: transformers.Deterministic,
			input:         []byte{0, 0, 0, 50},
			params:        map[string]any{},
			wantErr:       false,
			result:        612657282,
		},
		{
			name:          "ok - transform int32 deterministically",
			generatorType: transformers.Deterministic,
			input:         int32(45000),
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
			},
			wantErr: false,
			result:  15232,
		},
		{
			name:          "ok - transform uint16 deterministically",
			generatorType: transformers.Deterministic,
			input:         uint16(0),
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
			},
			wantErr: false,
			result:  23208,
		},
		{
			name:          "ok - transform []byte deterministically, oversize",
			generatorType: transformers.Deterministic,
			input:         []byte{0, 1, 2, 3, 0, 0, 50, 0, 0, 0},
			params: map[string]any{
				"size":      2,
				"min_value": int64(-100),
			},
			wantErr: false,
			result:  18547,
		},
		{
			name:          "invalid type with deterministic with default params",
			generatorType: transformers.Deterministic,
			input:         "invalid",
			params:        map[string]any{},
			wantErr:       true,
		},
		{
			name:          "invalid type",
			generatorType: transformers.Random,
			input:         "invalid",
			params: map[string]any{
				"size":      8,
				"min_value": int64(-100),
				"max_value": int64(100),
			},
			wantErr: true,
		},
		{
			name:          "invalid type with default params",
			generatorType: transformers.Random,
			input:         "invalid",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewIntegerTransformer(tt.generatorType, tt.params)
			if err != nil {
				t.Fatalf("Failed to create transformer: %v", err)
			}
			got, err := transformer.Transform(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("Transform() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Transform() unexpected error = %v", err)
				return
			}

			result, ok := got.(int64)
			if !ok {
				t.Errorf("Transform() expected int64, got %T", got)
			}

			if tt.generatorType == transformers.Deterministic {
				require.Equal(t, tt.result, result)
				return
			}

			// check if the result is within the specified range
			minVal, found, err := transformers.FindParameter[int64](tt.params, "min_value")
			require.NoError(t, err)
			if found {
				require.True(t, result >= minVal)
			}

			maxVal, found, err := transformers.FindParameter[int64](tt.params, "max_value")
			require.NoError(t, err)
			if found {
				require.True(t, result <= maxVal)
			}
		})
	}
}
