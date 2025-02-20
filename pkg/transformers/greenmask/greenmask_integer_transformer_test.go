// SPDX-License-Identifier: Apache-2.0

package greenmask

import (
	"strings"
	"testing"

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

func TestIntegerTransformer_Transform_Random(t *testing.T) {
	transformer, err := NewIntegerTransformer(transformers.Random, transformers.Parameters{
		"size":      8,
		"min_value": int64(-100),
		"max_value": int64(100),
	})
	if err != nil {
		t.Fatalf("Failed to create transformer: %v", err)
	}

	tests := []struct {
		name      string
		input     any
		wantErr   bool
		checkFunc func(got any) bool
	}{
		{
			name:    "ok - transform int64",
			input:   int64(500),
			wantErr: false,
			checkFunc: func(got any) bool {
				v, ok := got.(int64)
				return ok && v >= -100 && v <= 100
			},
		},
		{
			name:    "ok - transform []byte",
			input:   []byte{0, 0, 0, 50},
			wantErr: false,
			checkFunc: func(got any) bool {
				v, ok := got.(int64)
				return ok && v >= -100 && v <= 100
			},
		},
		{
			name:    "ok - transform int16",
			input:   int16(500),
			wantErr: false,
			checkFunc: func(got any) bool {
				v, ok := got.(int64)
				return ok && v >= -100 && v <= 100
			},
		},
		{
			name:    "invalid type",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			if !tt.checkFunc(got) {
				t.Errorf("Transform() got = %v, which didn't pass validation", got)
			}
		})
	}
}

func TestIntegerTransformer_Transform_Random_DefaultParams(t *testing.T) {
	transformer, err := NewIntegerTransformer(transformers.Random, transformers.Parameters{})
	if err != nil {
		t.Fatalf("Failed to create transformer: %v", err)
	}

	tests := []struct {
		name      string
		input     any
		wantErr   bool
		checkFunc func(got any) bool
	}{
		{
			name:    "ok - transform int64 with default params",
			input:   int64(500),
			wantErr: false,
			checkFunc: func(got any) bool {
				v, ok := got.(int64)
				return ok && v >= minValueForSize(DefaultSize) && v <= maxValueForSize(DefaultSize)
			},
		},
		{
			name:    "ok - transform []byte with default params",
			input:   []byte{0, 0, 0, 50},
			wantErr: false,
			checkFunc: func(got any) bool {
				v, ok := got.(int64)
				return ok && v >= minValueForSize(DefaultSize) && v <= maxValueForSize(DefaultSize)
			},
		},
		{
			name:    "ok - transform []byte with default params, oversize",
			input:   []byte{0, 0, 0, 0, 0, 0, 0, 50},
			wantErr: false,
			checkFunc: func(got any) bool {
				v, ok := got.(int64)
				return ok && v >= minValueForSize(DefaultSize) && v <= maxValueForSize(DefaultSize)
			},
		},
		{
			name:    "invalid type with default params",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			if !tt.checkFunc(got) {
				t.Errorf("Transform() got = %v, which didn't pass validation", got)
			}
		})
	}
}

func TestIntegerTransformer_Transform_Deterministic(t *testing.T) {
	transformer, err := NewIntegerTransformer(transformers.Deterministic, transformers.Parameters{
		"size":      2,
		"min_value": int64(-100),
	})
	if err != nil {
		t.Fatalf("Failed to create transformer: %v", err)
	}

	tests := []struct {
		name      string
		input     any
		wantErr   bool
		checkFunc func(got1, got2 any) bool
	}{
		{
			name:    "ok - transform int64 deterministically",
			input:   int64(500),
			wantErr: false,
			checkFunc: func(got1, got2 any) bool {
				v1, ok1 := got1.(int64)
				v2, ok2 := got2.(int64)
				return ok1 && ok2 && v1 == v2 && v1 >= -100 && v1 <= maxValueForSize(2)
			},
		},
		{
			name:    "ok - transform int16 deterministically",
			input:   int16(500),
			wantErr: false,
			checkFunc: func(got1, got2 any) bool {
				v1, ok1 := got1.(int64)
				v2, ok2 := got2.(int64)
				return ok1 && ok2 && v1 == v2 && v1 >= -100 && v1 <= maxValueForSize(2)
			},
		},
		{
			name:    "ok - transform []byte deterministically, oversize",
			input:   []byte{0, 1, 2, 3, 0, 0, 50},
			wantErr: false,
			checkFunc: func(got1, got2 any) bool {
				v1, ok1 := got1.(int64)
				v2, ok2 := got2.(int64)
				return ok1 && ok2 && v1 == v2 && v1 >= -100 && v1 <= maxValueForSize(2)
			},
		},
		{
			name:    "invalid type with deterministic",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, err := transformer.Transform(tt.input)
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

			got2, err := transformer.Transform(tt.input)
			if err != nil {
				t.Errorf("Second Transform() unexpected error = %v", err)
				return
			}

			if !tt.checkFunc(got1, got2) {
				t.Errorf("Transform() not deterministic: got1 = %v, got2 = %v", got1, got2)
			}
		})
	}
}

func TestIntegerTransformer_Transform_Deterministic_DefaultParams(t *testing.T) {
	transformer, err := NewIntegerTransformer(transformers.Deterministic, transformers.Parameters{})
	if err != nil {
		t.Fatalf("Failed to create transformer: %v", err)
	}

	tests := []struct {
		name      string
		input     any
		wantErr   bool
		checkFunc func(got1, got2 any) bool
	}{
		{
			name:    "ok - transform int64 deterministically",
			input:   int64(500),
			wantErr: false,
			checkFunc: func(got1, got2 any) bool {
				v1, ok1 := got1.(int64)
				v2, ok2 := got2.(int64)
				return ok1 && ok2 && v1 == v2 && v1 >= minValueForSize(DefaultSize) && v1 <= maxValueForSize(DefaultSize)
			},
		},
		{
			name:    "ok - transform []byte deterministically",
			input:   []byte{0, 0, 0, 50},
			wantErr: false,
			checkFunc: func(got1, got2 any) bool {
				v1, ok1 := got1.(int64)
				v2, ok2 := got2.(int64)
				return ok1 && ok2 && v1 == v2 && v1 >= minValueForSize(DefaultSize) && v1 <= maxValueForSize(DefaultSize)
			},
		},
		{
			name:    "invalid type with deterministic",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, err := transformer.Transform(tt.input)
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

			got2, err := transformer.Transform(tt.input)
			if err != nil {
				t.Errorf("Second Transform() unexpected error = %v", err)
				return
			}

			if !tt.checkFunc(got1, got2) {
				t.Errorf("Transform() not deterministic: got1 = %v, got2 = %v", got1, got2)
			}
		})
	}
}
