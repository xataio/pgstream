// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestNewTemplateTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  ParameterValues
		wantErr error
	}{
		{
			name: "ok - template",
			params: ParameterValues{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},
			wantErr: nil,
		},
		{
			name:    "error - template not provided",
			params:  ParameterValues{},
			wantErr: errTemplateMustBeProvided,
		},
		{
			name: "error - template cannot be parsed",
			params: ParameterValues{
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
		pgType string
		params ParameterValues

		wantOutput string
		wantErr    error
	}{
		{
			name:  "ok - basic template",
			value: "hello",
			params: ParameterValues{
				"template": "hello world",
			},
			wantOutput: "hello world",
			wantErr:    nil,
		},
		{
			name:  "ok - GetValue with if statement",
			value: "hello",
			params: ParameterValues{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},

			wantOutput: "first",
			wantErr:    nil,
		},
		{
			name:  "ok - GetValue with if statement - else",
			value: "world",
			params: ParameterValues{
				"template": "{{- if eq .GetValue \"hello\" -}} first {{- else -}} second {{- end -}}",
			},

			wantOutput: "second",
			wantErr:    nil,
		},
		{
			name:  "ok - integer value",
			value: int32(42),
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "42",
			wantErr:    nil,
		},
		{
			name:  "ok - numeric value renders as decimal text",
			value: pgtype.Numeric{Int: big.NewInt(12345678), Exp: -4, Valid: true},
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "1234.5678",
			wantErr:    nil,
		},
		{
			name:  "ok - uuid value renders as canonical text",
			value: [16]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11},
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			wantErr:    nil,
		},
		{
			name:   "ok - date value renders as postgres text",
			value:  time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			pgType: "date",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "2024-02-29",
			wantErr:    nil,
		},
		{
			name:   "ok - timestamp value renders as postgres text",
			value:  time.Date(2024, 2, 29, 12, 34, 56, 789000000, time.UTC),
			pgType: "timestamp",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "2024-02-29 12:34:56.789",
			wantErr:    nil,
		},
		{
			name:   "ok - timestamptz value renders as postgres text",
			value:  time.Date(2024, 2, 29, 11, 34, 56, 0, time.FixedZone("CET", 3600)),
			pgType: "timestamptz",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "2024-02-29 10:34:56Z",
			wantErr:    nil,
		},
		{
			name:   "ok - time value renders as postgres text",
			value:  pgtype.Time{Microseconds: 45296000000, Valid: true},
			pgType: "time",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "12:34:56.000000",
			wantErr:    nil,
		},
		{
			name:   "ok - interval value renders as postgres text",
			value:  pgtype.Interval{Days: 1, Microseconds: 7384000000, Valid: true},
			pgType: "interval",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "1 day 02:03:04",
			wantErr:    nil,
		},
		{
			name:   "ok - bytea value renders as postgres hex text",
			value:  []byte{0xde, 0xad, 0xbe, 0xef},
			pgType: "bytea",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: `\xdeadbeef`,
			wantErr:    nil,
		},
		{
			name:   "ok - array value renders as postgres text",
			value:  []any{"a", "b,c"},
			pgType: "_text",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: `{a,"b,c"}`,
			wantErr:    nil,
		},
		{
			name:   "ok - hstore value renders as postgres text",
			value:  map[string]string{"k": "v"},
			pgType: "hstore",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: `"k"=>"v"`,
			wantErr:    nil,
		},
		{
			name:   "ok - int4range value renders as postgres text",
			value:  pgtype.Range[any]{Lower: int32(1), Upper: int32(10), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
			pgType: "int4range",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "[1,10)",
			wantErr:    nil,
		},
		{
			name:   "ok - daterange value renders as postgres text",
			value:  pgtype.Range[any]{Lower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Upper: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
			pgType: "daterange",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "[2024-01-01,2024-02-01)",
			wantErr:    nil,
		},
		{
			name:   "ok - unbounded numrange value renders as postgres text",
			value:  pgtype.Range[any]{Lower: pgtype.Numeric{Int: big.NewInt(15), Exp: -1, Valid: true}, LowerType: pgtype.Exclusive, UpperType: pgtype.Unbounded, Valid: true},
			pgType: "numrange",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "(1.5,)",
			wantErr:    nil,
		},
		{
			name:   "ok - empty range value renders as postgres text",
			value:  pgtype.Range[any]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
			pgType: "int4range",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "empty",
			wantErr:    nil,
		},
		{
			name:   "ok - replicated text is passed through",
			value:  "12:34:56",
			pgType: "time",
			params: ParameterValues{
				"template": "{{ .GetValue }}",
			},
			wantOutput: "12:34:56",
			wantErr:    nil,
		},
		{
			name:  "incompatible types for comparison",
			value: 1,
			params: ParameterValues{
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
			got, err := tt.Transform(context.Background(), Value{TransformValue: tc.value, TransformType: tc.pgType})

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
		params        ParameterValues
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
			params: ParameterValues{
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
			params: ParameterValues{
				"template": "{{- if eq .GetValue \"hello\" -}} {{.GetDynamicValue \"value1\" }} {{- else -}} {{.GetDynamicValue \"value2\" }} {{- end -}}",
			},
			wantOutput: "second",
			wantErr:    nil,
		},
		{
			name:  "ok - numeric dynamic value renders as decimal text",
			value: "hello",
			dynamicValues: map[string]any{
				"amount": pgtype.Numeric{Int: big.NewInt(-5), Exp: 2, Valid: true},
			},
			params: ParameterValues{
				"template": "{{ .GetDynamicValue \"amount\" }}",
			},
			wantOutput: "-500",
			wantErr:    nil,
		},
		{
			name:  "ok - uuid dynamic value renders as canonical text",
			value: "hello",
			dynamicValues: map[string]any{
				"uid": [16]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11},
			},
			params: ParameterValues{
				"template": "{{ .GetDynamicValue \"uid\" }}",
			},
			wantOutput: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			wantErr:    nil,
		},
		{
			name:  "ok - time dynamic value keeps its go type",
			value: "hello",
			dynamicValues: map[string]any{
				"born": time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			},
			params: ParameterValues{
				"template": "{{ date \"02/01/2006\" (.GetDynamicValue \"born\") }}",
			},
			wantOutput: "29/02/2024",
			wantErr:    nil,
		},
		{
			name:          "error - no dynamic values",
			value:         "hello",
			dynamicValues: nil,
			params: ParameterValues{
				"template": "{{- if eq .GetValue \"hello\" -}} {{.GetDynamicValue \"value1\" }} {{- else -}} {{.GetDynamicValue \"value2\" }} {{- end -}}",
			},
			wantOutput: "",
			wantErr:    errDynamicValuesNil,
		},
		{
			name:  "error - dynamic value not found",
			value: "world",
			dynamicValues: map[string]any{
				"value1": "first",
			},
			params: ParameterValues{
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
		params        ParameterValues
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
			params: ParameterValues{
				"template": "{{ $first := .GetDynamicValue \"value1\" }}{{ $second :=.GetDynamicValue \"value2\" }} {{- if eq $first nil -}} {{ masking .GetValue $second }} {{- else -}} {{ masking .GetValue $first }} {{- end -}}",
			},
			wantOutput: "joh****e@xata.io",
			wantErr:    nil,
		},
		{
			name:  "ok - random integer",
			value: 3,
			params: ParameterValues{
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
