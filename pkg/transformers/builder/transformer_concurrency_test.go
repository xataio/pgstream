// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"context"
	"maps"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
)

// concurrencyTestCase exercises a transformer from multiple goroutines, the
// way pgstream snapshot workers share a single transformer per column. Run
// with the race detector enabled, it catches transformers that share rng or
// buffer state across calls (issues #789 and #800).
type concurrencyTestCase struct {
	params transformers.ParameterValues
	input  any
	// supportsGenerators runs the case once with the random and once with the
	// deterministic generator parameter
	supportsGenerators bool
	validate           func(t *testing.T, got any)
	skip               string
}

func TestTransformers_ConcurrentTransform(t *testing.T) {
	t.Parallel()

	tests := map[transformers.TransformerType]concurrencyTestCase{
		transformers.Masking: {
			params: transformers.ParameterValues{"type": "default"},
			input:  "sensitive value",
		},
		transformers.Email: {
			input: "user@example.com",
		},
		transformers.Template: {
			params: transformers.ParameterValues{"template": "{{ .GetValue }} - transformed"},
			input:  "hello",
		},
		transformers.JSON: {
			params: transformers.ParameterValues{
				"operations": []any{
					map[string]any{
						"operation":       "set",
						"path":            "/greeting",
						"value":           "hello world",
						"error_not_exist": false,
					},
				},
			},
			input: []byte(`{"greeting":"hi"}`),
		},
		transformers.Hstore: {
			params: transformers.ParameterValues{
				"operations": []any{
					map[string]any{
						"operation": "set",
						"key":       "test",
						"value":     "value",
					},
				},
			},
			input: `"a"=>"b"`,
		},
		transformers.PhoneNumber: {
			params:             transformers.ParameterValues{"prefix": "+1", "min_length": 9, "max_length": 12},
			input:              "1234567890",
			supportsGenerators: true,
		},
		transformers.LiteralString: {
			params: transformers.ParameterValues{"literal": "fixed"},
			input:  "anything",
		},
		transformers.String: {
			input: "hello world",
		},
		transformers.GreenmaskString: {
			params:             transformers.ParameterValues{"min_length": 2, "max_length": 12},
			input:              "hello world",
			supportsGenerators: true,
			validate: func(t *testing.T, got any) {
				str, ok := got.(string)
				require.True(t, ok, "expected string, got %T", got)
				// a torn read of the shared rune buffer produces NUL runes,
				// which postgres rejects on COPY (issue #800)
				require.NotContains(t, str, "\x00")
			},
		},
		transformers.GreenmaskFirstName: {
			input:              "John",
			supportsGenerators: true,
		},
		transformers.GreenmaskInteger: {
			params:             transformers.ParameterValues{"min_value": 0, "max_value": 1000},
			input:              42,
			supportsGenerators: true,
		},
		transformers.GreenmaskFloat: {
			params:             transformers.ParameterValues{"min_value": 0.0, "max_value": 1000.0},
			input:              42.5,
			supportsGenerators: true,
		},
		transformers.GreenmaskUUID: {
			input:              "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			supportsGenerators: true,
		},
		transformers.GreenmaskBoolean: {
			input:              true,
			supportsGenerators: true,
		},
		transformers.GreenmaskChoice: {
			params:             transformers.ParameterValues{"choices": []any{"alpha", "beta", "gamma"}},
			input:              "x",
			supportsGenerators: true,
		},
		transformers.GreenmaskUnixTimestamp: {
			params:             transformers.ParameterValues{"min_value": "946684800", "max_value": "1893456000"},
			input:              int64(1136073600),
			supportsGenerators: true,
		},
		transformers.GreenmaskDate: {
			params:             transformers.ParameterValues{"min_value": "2020-01-01", "max_value": "2024-12-31"},
			input:              "2022-06-15",
			supportsGenerators: true,
		},
		transformers.GreenmaskUTCTimestamp: {
			params:             transformers.ParameterValues{"min_timestamp": "2020-01-01T00:00:00Z", "max_timestamp": "2024-12-31T23:59:59Z"},
			input:              "2022-06-15T12:00:00Z",
			supportsGenerators: true,
		},
		transformers.NeosyncString: {
			input: "hello world",
		},
		transformers.NeosyncFirstName: {
			input: "John",
		},
		transformers.NeosyncLastName: {
			input: "Doe",
		},
		transformers.NeosyncFullName: {
			input: "John Doe",
		},
		transformers.NeosyncEmail: {
			input: "user@example.com",
		},
		transformers.PGAnonymizer: {
			skip: "requires a live PostgreSQL connection",
		},
	}

	// every registered transformer must have a concurrency test, so that new
	// transformers can't be added without one
	for transformerType := range TransformersMap {
		_, found := tests[transformerType]
		require.True(t, found, "transformer %q is missing a concurrency test case", transformerType)
	}

	for transformerType, tc := range tests {
		t.Run(string(transformerType), func(t *testing.T) {
			t.Parallel()
			if tc.skip != "" {
				t.Skip(tc.skip)
			}

			generatorTypes := []string{""}
			if tc.supportsGenerators {
				generatorTypes = []string{"random", "deterministic"}
			}

			for _, generatorType := range generatorTypes {
				params := maps.Clone(tc.params)
				if generatorType != "" {
					if params == nil {
						params = transformers.ParameterValues{}
					}
					params["generator"] = generatorType
				}

				name := generatorType
				if name == "" {
					name = "default"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					transformer, err := NewTransformerBuilder().New(&transformers.Config{
						Name:       transformerType,
						Parameters: params,
					})
					require.NoError(t, err)
					defer transformer.Close()

					// the deterministic generator must keep producing the
					// same output for the same input, regardless of which
					// internal instance serves the call
					wantDeterministic := generatorType == "deterministic"
					runConcurrentTransforms(t, transformer, tc.input, wantDeterministic, tc.validate)
				})
			}
		})
	}
}

func runConcurrentTransforms(t *testing.T, transformer transformers.Transformer, input any, wantDeterministic bool, validate func(t *testing.T, got any)) {
	const (
		numGoroutines = 16
		numIterations = 100
	)

	value := transformers.NewValue(input, "text", nil)
	results := make([][]any, numGoroutines)
	errs := make([]error, numGoroutines)

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range numIterations {
				got, err := transformer.Transform(context.Background(), value)
				if err != nil {
					errs[g] = err
					return
				}
				results[g] = append(results[g], got)
			}
		}()
	}
	wg.Wait()

	var first any
	for g := range numGoroutines {
		require.NoError(t, errs[g])
		for _, got := range results[g] {
			if validate != nil {
				validate(t, got)
			}
			if wantDeterministic {
				if first == nil {
					first = got
				}
				require.Equal(t, first, got)
			}
		}
	}
}
