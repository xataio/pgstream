// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/json"
)

var badValue interface{}

func TestNewJsonTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		params  ParameterValues
		wantErr error
	}{
		{
			name: "ok - valid",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "/greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
					map[string]any{
						"operation":       "delete",
						"path":            "/farewell",
						"error_not_exist": true,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "error - operations invalid type",
			params: ParameterValues{
				"operations": "not an array",
			},
			wantErr: errors.New("operations must be an array"),
		},
		{
			name:    "error - operations not provided",
			params:  ParameterValues{},
			wantErr: errOperationsMustBeProvided,
		},
		{
			name: "error - operations empty",
			params: ParameterValues{
				"operations": []any{},
			},
			wantErr: errOperationsCannotBeEmpty,
		},
		{
			name: "error - invalid operation type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       3,
						"path":            "/greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - operation name not provided",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"path":            "/greeting",
						"value":           "hello world",
						"error_not_exist": false,
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
						"operation":       "not_a_valid_operation",
						"path":            "/greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
				},
			},
			wantErr: errInvalidOperationsType,
		},
		{
			name: "error - invalid path type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            3,
						"value":           "hello world",
						"error_not_exist": false,
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - path not provided",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"value":           "hello world",
						"error_not_exist": false,
					},
				},
			},
			wantErr: errPathMustBeProvided,
		},
		{
			name: "error - invalid error_not_exist type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "delete",
						"path":            "/greeting",
						"error_not_exist": "invalid",
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - invalid value type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "/greeting",
						"value":           nil,
						"error_not_exist": false,
					},
				},
			},
			wantErr: errors.New("cannot read parameter 'value'"),
		},
		{
			name: "error - invalid value_template type",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "/greeting",
						"value_template":  3,
						"error_not_exist": false,
					},
				},
			},
			wantErr: ErrInvalidParameters,
		},
		{
			name: "error - both value and value_template absent",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "/greeting",
						"error_not_exist": false,
					},
				},
			},
			wantErr: errValueOrTemplateMustBeProvided,
		},
		{
			name: "error - template cannot be parsed",
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "/greeting",
						"error_not_exist": false,
						"value_template":  "{{ invalid template syntax",
					},
				},
			},
			wantErr: errors.New("json_transformer: error parsing template"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tt, err := NewJSONTransformer(tc.params)
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

func TestJsonTransformer_Transform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		value         any
		dynamicValues map[string]any
		params        ParameterValues

		wantOutput         any
		wantOutputContains string
		wantErr            error
	}{
		{
			name:  "ok - basic set and delete operations",
			value: map[string]any{"greeting": "hello", "farewell": "goodbye"},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
					map[string]any{
						"operation":       "delete",
						"path":            "farewell",
						"error_not_exist": true,
					},
				},
			},
			wantOutput: map[string]any{
				"greeting": "hello world",
			},
			wantErr: nil,
		},
		{
			name:  "ok - basic set and delete operations, []byte input",
			value: []byte(`{"greeting":"hello", "farewell":"goodbye"}`),
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
					map[string]any{
						"operation":       "delete",
						"path":            "farewell",
						"error_not_exist": true,
					},
				},
			},
			wantOutput: map[string]any{
				"greeting": "hello world",
			},
			wantErr: nil,
		},
		{
			name:  "ok - set and delete on array",
			value: []any{map[string]any{"greeting": "hello", "farewell": "goodbye"}},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "0.greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
					map[string]any{
						"operation":       "set",
						"path":            "1.greeting",
						"value":           "hello again",
						"error_not_exist": true,
					},
					map[string]any{
						"operation":       "delete",
						"path":            "0.farewell",
						"error_not_exist": true,
					},
				},
			},
			wantOutput: []any{map[string]any{"greeting": "hello world"}, map[string]any{"greeting": "hello again"}},
			wantErr:    nil,
		},
		{
			name: "ok - set and delete, with dynamic values, template with masking, complex path",
			value: map[string]any{
				"user": map[string]any{
					"firstname": "john",
					"lastname":  "unknown",
					"email":     "scrambleme@gmail.com",
				},
				"residency": map[string]any{
					"city":    "some city",
					"country": "some country",
				},
				"purchases": []any{
					map[string]any{"item": "book", "price": 10},
					map[string]any{"item": "pen", "price": 2},
				},
			},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "purchases.#.item",
						"value":           "-",
						"error_not_exist": true,
					},
					map[string]any{
						"operation":       "set",
						"path":            "user.lastname",
						"value_template":  "\"{{ .GetDynamicValue \"lastname\" }}\"",
						"error_not_exist": true,
					},
					map[string]any{
						"operation":       "set",
						"path":            "residency.missingcity",
						"value_template":  "\"{{ masking \"default\" .GetValue }}\"",
						"error_not_exist": false,
					},
					map[string]any{
						"operation":       "set",
						"path":            "user.email",
						"value_template":  "\"{{ atEmailScramble \"ignoreddomain.com\" \"@crypt.com\" \"asaltvalue\" .GetValue }}\"",
						"error_not_exist": true,
					},
					map[string]any{
						"operation":       "set",
						"path":            "user.missingemail",
						"value_template":  "\"{{ atEmailScramble \"ignoreddomain.com\" \"@crypt.com\" \"shouldnotcrash\" .GetValue }}\"",
						"error_not_exist": false,
					},
					map[string]any{
						"operation":       "set",
						"path":            "residency.city",
						"value_template":  "\"{{ masking \"default\" .GetValue }}\"",
						"error_not_exist": true,
					},
					map[string]any{
						"operation":       "delete",
						"path":            "residency.country",
						"error_not_exist": true,
					},
				},
			},
			dynamicValues: map[string]any{
				"lastname": "doe",
				"column":   3.1415,
			},
			wantOutput: map[string]any{
				"user": map[string]any{
					"firstname": "john",
					"lastname":  "doe",
					"email":     "S7iVt9DKRtZn7gKwVhu@crypt.com",
				},
				"residency": map[string]any{
					"city": "*********",
				},
				"purchases": []any{
					map[string]any{"item": "-", "price": float64(10)},
					map[string]any{"item": "-", "price": float64(2)},
				},
			},
			wantErr: nil,
		},
		{
			name:  "error - cannot marshal value",
			value: make(chan int), // invalid type for JSON
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "0.greeting",
						"value":           "hi",
						"error_not_exist": true,
					},
				},
			},
			wantErr: errors.New("json_transformer: error marshalling value to JSON"),
		},
		{
			name: "error - missing complex key",
			value: map[string]any{
				"residency": map[string]any{
					"city":        "some city",
					"country":     "some country",
					"missingcity": badValue,
				},
			},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "residency.missingkey",
						"value_template":  "\"{{ masking \"default\" .GetValue }}\"",
						"error_not_exist": true,
					},
				},
			},
			wantErr: errors.New("cannot apply \"set\" operation[0] with path residency.missingkey: value by path \"residency.missingkey\""),
		},
		{
			name: "error - missing complex key no error",
			value: map[string]any{
				"residency": map[string]any{
					"city":    "some city",
					"country": "some country",
				},
			},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "residency.missingkey",
						"value_template":  "\"{{ masking \"default\" .GetValue }}\"",
						"error_not_exist": false,
					},
				},
			},
			wantErr: nil,
		},
		{
			name:  "error - setting invalid value for json",
			value: []any{map[string]any{"greeting": "hello", "farewell": "goodbye"}},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "0.greeting",
						"value":           make(chan int),
						"error_not_exist": true,
					},
				},
			},
			wantErr: errors.New("json: unsupported type: chan int"),
		},
		{
			name:  "error - set path not found in object",
			value: map[string]any{"greeting": "hello"},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "farewell",
						"value_template":  "goodbye",
						"error_not_exist": true,
					},
				},
			},
			wantErr: errors.New("value by path \"farewell\" does not exist"),
		},
		{
			name:  "error - delete path not found in array",
			value: []any{map[string]any{"greeting": "hello", "farewell": "goodbye"}},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "delete",
						"path":            "1.farewell",
						"error_not_exist": true,
					},
				},
			},
			wantErr: errors.New("value by path \"1.farewell\" does not exist"),
		},
		{
			name:  "error - invalid template, quotes missing",
			value: map[string]any{"greeting": "hello", "farewell": "goodbye"},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      "set",
						"path":           "farewell",
						"value_template": "{{ masking default .GetValue }}",
					},
				},
			},
			wantErr: errors.New("error executing template"),
		},
		{
			name:  "error - dynamic values missing",
			value: map[string]any{"greeting": "hello", "farewell": "goodbye"},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      "set",
						"path":           "farewell",
						"value_template": "{{ .GetDynamicValue \"notexists\" }}",
					},
				},
			},
			wantErr: errors.New("dynamic values are nil"),
		},
		{
			name:  "error - dynamic value not found",
			value: map[string]any{"greeting": "hello", "farewell": "goodbye"},
			params: ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":      "set",
						"path":           "farewell",
						"value_template": "{{ .GetDynamicValue \"notexists\" }}",
					},
				},
			},
			dynamicValues: map[string]any{"exists": "value"},
			wantErr:       errors.New("dynamic value 'notexists' not found"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tt, err := NewJSONTransformer(tc.params)
			require.NoError(t, err)
			got, err := tt.Transform(context.Background(), Value{TransformValue: tc.value, DynamicValues: tc.dynamicValues})

			if tc.wantErr != nil {
				require.Error(t, err)
				if !errors.Is(err, tc.wantErr) {
					require.Contains(t, err.Error(), tc.wantErr.Error())
				}
				return
			}
			require.NoError(t, err)

			if tc.wantOutputContains != "" {
				marshalled, err := json.Marshal(&got)
				require.NoError(t, err)
				require.Contains(t, string(marshalled), tc.wantOutputContains)
			}
			if tc.wantOutput != nil {
				require.Equal(t, tc.wantOutput, got)
			}
		})
	}
}
