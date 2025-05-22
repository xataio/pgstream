// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTemplateTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  Parameters
		wantErr error
	}{
		{
			name: "ok - template",
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},
			wantErr: nil,
		},
		{
			name:    "error - template not provided",
			params:  Parameters{},
			wantErr: errTemplateMustBeProvided,
		},
		{
			name: "error - template cannot be parsed",
			params: Parameters{
				"template": "{{- if eq syntaxerror",
			},
			wantErr: errors.New("template_transformer: error parsing template"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tt, err := NewTemplateTransformer(tc.params)
			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, tt)
		})
	}
}

func TestTemplateTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		value  any
		params Parameters

		wantOutput string
		wantErr    error
	}{
		{
			name:  "ok - basic template",
			value: "hello",
			params: Parameters{
				"template": "hello world",
			},
			wantOutput: "hello world",
			wantErr:    nil,
		},
		{
			name:  "ok - GetValue with if statement",
			value: "hello",
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},

			wantOutput: "first",
			wantErr:    nil,
		},
		{
			name:  "ok - GetValue with if statement - else",
			value: "world",
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},

			wantOutput: "second",
			wantErr:    nil,
		},
		{
			name:  "incompatible types for comparison",
			value: 1,
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},
			wantOutput: "",
			wantErr:    errors.New("incompatible types for comparison"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tt, err := NewTemplateTransformer(tc.params)
			require.NoError(t, err)
			got, err := tt.Transform(context.Background(), Value{TransformValue: tc.value})

			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)

			require.Equal(t, tc.wantOutput, got)
		})
	}
}

func TestTemplateTransformer_Transform_WithDynamicValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		value         any
		dynamicValues map[string]any
		params        Parameters
		wantOutput    string
		wantErr       error
	}{
		{
			name:  "ok - GetValue with dynamic values",
			value: "hello",
			dynamicValues: map[string]any{
				"value1": "first",
				"value2": "second",
			},
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} {{.GetDynamicValue \"value1\" }} {{- else -}} {{.GetDynamicValue \"value2\" }} {{- end -}}",
			},
			wantOutput: "first",
			wantErr:    nil,
		},
		{
			name:  "ok - GetValue with dynamic values - else",
			value: "world",
			dynamicValues: map[string]any{
				"value1": "first",
				"value2": "second",
			},
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} {{.GetDynamicValue \"value1\" }} {{- else -}} {{.GetDynamicValue \"value2\" }} {{- end -}}",
			},
			wantOutput: "second",
			wantErr:    nil,
		},
		{
			name:          "error - no dynamic values",
			value:         "hello",
			dynamicValues: nil,
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} {{.GetDynamicValue \"value1\" }} {{- else -}} {{.GetDynamicValue \"value2\" }} {{- end -}}",
			},
			wantOutput: "",
			wantErr:    errors.New("dynamic values are nil"),
		},
		{
			name:  "error - dynamic value not found",
			value: "world",
			dynamicValues: map[string]any{
				"value1": "first",
			},
			params: Parameters{
				"template": "{{- if eq .GetValue \"hello\" -}} {{.GetDynamicValue \"value1\" }} {{- else -}} {{.GetDynamicValue \"value2\" }} {{- end -}}",
			},
			wantOutput: "",
			wantErr:    errors.New("dynamic value 'value2' not found"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tt, err := NewTemplateTransformer(tc.params)
			require.NoError(t, err)
			got, err := tt.Transform(context.Background(), Value{
				TransformValue: tc.value,
				DynamicValues:  tc.dynamicValues,
			})

			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)

			require.Equal(t, tc.wantOutput, got)
		})
	}
}

func TestTemplateTransformer_Transform_WithGreenmaskToolkitFuncs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		value         any
		dynamicValues map[string]any
		params        Parameters
		wantOutput    string
		wantErr       error
	}{
		{
			name:  "ok - dynamic values with masking func",
			value: "email",
			dynamicValues: map[string]any{
				"value1": nil,
				"value2": "john.doe@xata.io",
			},
			params: Parameters{
				"template": "{{ $first := .GetDynamicValue \"value1\" }}{{ $second :=.GetDynamicValue \"value2\" }} {{- if eq $first nil -}} {{ masking .GetValue $second }} {{- else -}} {{ masking .GetValue $first }} {{- end -}}",
			},
			wantOutput: "joh****e@xata.io",
			wantErr:    nil,
		},
		{
			name:  "ok - random integer",
			value: 3,
			params: Parameters{
				"template": "{{ $randval := randomInt 0 .GetValue}} {{- if and (isInt $randval) (ge $randval 0) (lt $randval .GetValue) -}} {{\"yes\"}} {{- else -}} {{\"no\"}} {{- end -}}",
			},
			wantOutput: "yes",
			wantErr:    nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tt, err := NewTemplateTransformer(tc.params)
			require.NoError(t, err)
			got, err := tt.Transform(context.Background(), Value{
				TransformValue: tc.value,
				DynamicValues:  tc.dynamicValues,
			})

			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)

			require.Equal(t, tc.wantOutput, got)
		})
	}
}
