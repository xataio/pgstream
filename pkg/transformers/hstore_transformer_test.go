// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestHstoreTransformer_New(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		params  ParameterValues
		wantErr error
	}{
		{
			name: "ok",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "test",
						"value":     "value",
					},
					map[string]any{
						"operation": hstoreDeleteOpName,
						"key":       "test",
					},
				},
			},
			wantErr: nil,
		},
		{
			name:    "error - missing operations",
			params:  ParameterValues{},
			wantErr: errOperationsMustBeProvided,
		},
		{
			name: "error - operations array empty",
			params: ParameterValues{
				"operations": []any{},
			},
			wantErr: errOperationsMustBeProvided,
		},
		{
			name: "error - operations not array",
			params: ParameterValues{
				"operations": 3,
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - operations array wrong type",
			params: ParameterValues{
				"operations": []any{
					"invalid",
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid operation name type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": 3,
						"key":       "test",
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid operation name missing",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"key": "test",
					},
				},
			},
			wantErr: errOperationNameMustBeProvided,
		},
		{
			name: "error - invalid operation name",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": "invalid",
						"key":       "test",
					},
				},
			},
			wantErr: errInvalidOperationsType,
		},
		{
			name: "error - invalid operation key",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": "set",
						"key":       3.14,
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - missing operation key",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": "set",
					},
				},
			},
			wantErr: errKeyMustBeProvided,
		},
		{
			name: "error - invalid error_not_exist",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"key":             "test",
						"error_not_exist": "yes",
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid value",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": "set",
						"key":       "test",
						"value":     3.14,
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid template",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      "set",
						"key":            "test",
						"value_template": 3.14,
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid template syntax",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      "set",
						"key":            "test",
						"value_template": "{{invalidd",
					},
				},
			},
			wantErr: errors.New("error parsing template"),
		},
		{
			name: "error - both value and template missing",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": "set",
						"key":       "test",
					},
				},
			},
			wantErr: errValueOrTemplateMustBeProvided,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewHstoreTransformer(tc.params)
			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, transformer)
		})
	}
}

func TestHstoreTransformer_Transform(t *testing.T) {
	t.Parallel()

	stringPtr := func(s string) *string {
		return &s
	}

	tests := []struct {
		name               string
		params             ParameterValues
		input              any
		dynamicValues      map[string]any
		wantOutput         any
		wantOutputContains []string
		wantErr            error
	}{
		{
			name: "delete existing key",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreDeleteOpName,
						"key":       "key1",
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
				"key2": stringPtr("value2"),
			},
			wantOutput: pgtype.Hstore{
				"key2": stringPtr("value2"),
			},
			wantErr: nil,
		},
		{
			name: "delete non-existing key",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreDeleteOpName,
						"key":       "key2",
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
			},
			wantOutput: pgtype.Hstore{
				"key1": stringPtr("value1"),
			},

			wantErr: nil,
		},
		{
			name: "delete with error_not_exist",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       hstoreDeleteOpName,
						"key":             "key2",
						"error_not_exist": true,
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
			},
			wantOutput: nil,
			wantErr:    errors.New("key \"key2\" does not exist"),
		},
		{
			name: "set existing key",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key1",
						"value":     "new_value",
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
				"key2": stringPtr("value2"),
			},
			wantOutput: pgtype.Hstore{
				"key1": stringPtr("new_value"),
				"key2": stringPtr("value2"),
			},
			wantErr: nil,
		},
		{
			name: "set new key",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key2",
						"value":     "value2",
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
			},
			wantOutput: pgtype.Hstore{
				"key1": stringPtr("value1"),
				"key2": stringPtr("value2"),
			},
			wantErr: nil,
		},
		{
			name: "set key to NULL",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key1",
						"value":     nil,
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
				"key2": stringPtr("value2"),
			},
			wantOutput: pgtype.Hstore{
				"key1": nil,
				"key2": stringPtr("value2"),
			},
			wantErr: nil,
		},
		{
			name: "set key with template",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      hstoreSetOpName,
						"key":            "key1",
						"value_template": "{{ .GetDynamicValue \"other_column\" }}",
					},
				},
			},
			input: pgtype.Hstore{
				"key1": stringPtr("value1"),
			},
			dynamicValues: map[string]any{
				"other_column": "other_column_value",
			},
			wantOutput: pgtype.Hstore{
				"key1": stringPtr("other_column_value"),
			},
			wantErr: nil,
		},
		{
			name: "multiple operations",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreDeleteOpName,
						"key":       "key1",
					},
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key2",
						"value":     "modified",
					},
					map[string]any{
						"operation":      hstoreSetOpName,
						"key":            "email",
						"value_template": "{{ masking \"email\" .GetValue }}",
					},
					map[string]any{
						"operation":      hstoreSetOpName,
						"key":            "key3",
						"value_template": "{{ .GetValue }}",
					},
				},
			},
			input: pgtype.Hstore{
				"key1":  stringPtr("value1"),
				"key2":  stringPtr("value2"),
				"email": stringPtr("testing@xata.io"),
			},
			wantOutput: pgtype.Hstore{
				"key2":  stringPtr("modified"),
				"key3":  nil,
				"email": stringPtr("tes****@xata.io"),
			},
			wantErr: nil,
		},
		{
			name: "transform string input",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key3",
						"value":     "value3",
					},
				},
			},
			input:              `"key1"=>"value1", "key2"=>"value2"`,
			wantOutputContains: []string{`"key3"=>"value3"`, `"key1"=>"value1"`, `"key2"=>"value2"`},
			wantErr:            nil,
		},
		{
			name: "transform byte array input",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key3",
						"value":     "value3",
					},
				},
			},
			input:              []byte(`"key1"=>"value1", "key2"=>"value2"`),
			wantOutputContains: []string{`"key3"=>"value3"`, `"key1"=>"value1"`, `"key2"=>"value2"`},
			wantErr:            nil,
		},
		{
			name: "set and delete with skip_not_exist",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      hstoreSetOpName,
						"key":            "key1",
						"value":          "value1",
						"skip_not_exist": true,
					},
					map[string]any{
						"operation":      hstoreDeleteOpName,
						"key":            "key2",
						"skip_not_exist": true,
					},
				},
			},
			input: pgtype.Hstore{
				"key3": stringPtr("value3"),
			},
			wantOutput: pgtype.Hstore{
				"key3": stringPtr("value3"),
			},
			wantErr: nil,
		},
		{
			name: "unsupported input type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": hstoreSetOpName,
						"key":       "key3",
						"value":     "value3",
					},
				},
			},
			input:      123,
			wantOutput: nil,
			wantErr:    ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transformer, err := NewHstoreTransformer(tc.params)
			require.NoError(t, err)

			got, err := transformer.Transform(context.Background(), Value{
				TransformValue: tc.input,
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

			if tc.wantOutput != nil {
				require.Equal(t, tc.wantOutput, got)
			}

			if tc.wantOutputContains != nil {
				gotStr, ok := got.(string)
				if !ok {
					gotBytes, ok := got.([]byte)
					require.True(t, ok)
					gotStr = string(gotBytes)
				}

				for _, want := range tc.wantOutputContains {
					require.Contains(t, gotStr, want)
				}
			}
		})
	}
}
