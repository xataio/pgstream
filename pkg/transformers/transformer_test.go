// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_FindParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    Parameters
		paramName string

		wantFound bool
		wantParam int
		wantErr   error
	}{
		{
			name: "ok",
			params: map[string]any{
				"test": 1,
			},
			paramName: "test",

			wantFound: true,
			wantParam: 1,
			wantErr:   nil,
		},
		{
			name: "ok - not found",
			params: map[string]any{
				"test": 1,
			},
			paramName: "another",

			wantFound: false,
			wantParam: 0,
			wantErr:   nil,
		},
		{
			name: "error - invalid parameter type",
			params: map[string]any{
				"test": "1",
			},
			paramName: "test",

			wantFound: true,
			wantParam: 0,
			wantErr:   ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, found, err := FindParameter[int](tc.params, tc.paramName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantFound, found)
			require.Equal(t, tc.wantParam, got)
		})
	}
}

func Test_FindParameterWithDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		params       Parameters
		paramName    string
		defaultValue int

		wantValue int
		wantErr   error
	}{
		{
			name: "ok - parameter found",
			params: map[string]any{
				"test": 42,
			},
			paramName:    "test",
			defaultValue: 0,

			wantValue: 42,
			wantErr:   nil,
		},
		{
			name: "ok - parameter not found, use default",
			params: map[string]any{
				"test": 42,
			},
			paramName:    "another",
			defaultValue: 99,

			wantValue: 99,
			wantErr:   nil,
		},
		{
			name: "error - invalid parameter type",
			params: map[string]any{
				"test": "invalid",
			},
			paramName:    "test",
			defaultValue: 0,

			wantValue: 0,
			wantErr:   ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := FindParameterWithDefault(tc.params, tc.paramName, tc.defaultValue)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantValue, got)
		})
	}
}

func Test_FindParameterArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    Parameters
		paramName string

		wantFound bool
		wantParam []int
		wantErr   error
	}{
		{
			name: "ok - int array",
			params: map[string]any{
				"test": []int{1},
			},
			paramName: "test",

			wantFound: true,
			wantParam: []int{1},
			wantErr:   nil,
		},
		{
			name: "ok - interface array",
			params: map[string]any{
				"test": []any{1},
			},
			paramName: "test",

			wantFound: true,
			wantParam: []int{1},
			wantErr:   nil,
		},
		{
			name: "ok - not found",
			params: map[string]any{
				"test": 1,
			},
			paramName: "another",

			wantFound: false,
			wantParam: nil,
			wantErr:   nil,
		},
		{
			name: "error - invalid parameter type",
			params: map[string]any{
				"test": "1",
			},
			paramName: "test",

			wantFound: true,
			wantParam: nil,
			wantErr:   ErrInvalidParameters,
		},
		{
			name: "error - invalid array parameter type",
			params: map[string]any{
				"test": []any{"1"},
			},
			paramName: "test",

			wantFound: true,
			wantParam: nil,
			wantErr:   ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, found, err := FindParameterArray[int](tc.params, tc.paramName)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantFound, found)
			require.Equal(t, tc.wantParam, got)
		})
	}
}

func Test_ParseDynamicParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  Parameters
		want    map[string]*DynamicParameter
		wantErr error
	}{
		{
			name: "ok - valid dynamic parameters",
			params: map[string]any{
				"param1": map[string]any{
					"column": "col1",
				},
				"param2": map[string]any{
					"column": "col2",
				},
			},
			want: map[string]*DynamicParameter{
				"param1": {Column: "col1"},
				"param2": {Column: "col2"},
			},
			wantErr: nil,
		},
		{
			name: "error - empty parameter name",
			params: map[string]any{
				"": map[string]any{
					"column": "col1",
				},
			},
			want:    nil,
			wantErr: ErrInvalidDynamicParameters,
		},
		{
			name: "error - invalid parameter type",
			params: map[string]any{
				"param1": "invalid",
			},
			want:    nil,
			wantErr: ErrInvalidDynamicParameters,
		},
		{
			name: "error - missing column field",
			params: map[string]any{
				"param1": map[string]any{},
			},
			want:    nil,
			wantErr: ErrInvalidDynamicParameters,
		},
		{
			name: "error - column field wrong type",
			params: map[string]any{
				"param1": map[string]any{
					"column": 123,
				},
			},
			want:    nil,
			wantErr: ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseDynamicParameters(tc.params)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.want, got)
		})
	}
}

func Test_FindDynamicValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		param         *DynamicParameter
		dynamicValues map[string]any
		defaultValue  int

		wantValue int
		wantErr   error
	}{
		{
			name: "ok - value found",
			param: &DynamicParameter{
				Column: "col1",
			},
			dynamicValues: map[string]any{
				"col1": 42,
			},
			defaultValue: 0,

			wantValue: 42,
			wantErr:   nil,
		},
		{
			name: "ok - value not found, use default",
			param: &DynamicParameter{
				Column: "col1",
			},
			dynamicValues: map[string]any{},
			defaultValue:  99,

			wantValue: 99,
			wantErr:   nil,
		},
		{
			name: "error - invalid value type",
			param: &DynamicParameter{
				Column: "col1",
			},
			dynamicValues: map[string]any{
				"col1": "invalid",
			},
			defaultValue: 0,

			wantValue: 0,
			wantErr:   ErrInvalidParameters,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := FindDynamicValue(tc.param, tc.dynamicValues, tc.defaultValue)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantValue, got)
		})
	}
}
