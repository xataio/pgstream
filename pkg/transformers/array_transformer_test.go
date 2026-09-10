// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// stubTransformer stands in for an element transformer. The mocks package
// cannot be used here, since it imports this one.
type stubTransformer struct {
	transformFn func(Value) (any, error)
	uniqueness  Uniqueness
	dynamic     bool
	closeErr    error
}

func (s *stubTransformer) Transform(_ context.Context, value Value) (any, error) {
	return s.transformFn(value)
}

func (s *stubTransformer) CompatibleTypes() []SupportedDataType {
	return []SupportedDataType{StringDataType}
}
func (s *stubTransformer) Type() TransformerType  { return TransformerType("stub") }
func (s *stubTransformer) IsDynamic() bool        { return s.dynamic }
func (s *stubTransformer) Uniqueness() Uniqueness { return s.uniqueness }
func (s *stubTransformer) Close() error           { return s.closeErr }

func upperTransformer() *stubTransformer {
	return &stubTransformer{transformFn: func(value Value) (any, error) {
		str, ok := value.TransformValue.(string)
		if !ok {
			return nil, fmt.Errorf("stub: got %T: %w", value.TransformValue, ErrUnsupportedValueType)
		}
		return strings.ToUpper(str), nil
	}}
}

func newTestArrayTransformer(t *testing.T, cfg ArrayConfig) *ArrayTransformer {
	t.Helper()
	if cfg.Generator == "" {
		cfg.Generator = ArrayGeneratorMap
	}
	if cfg.ArrayOID == 0 {
		cfg.ArrayOID = pgtype.TextArrayOID
		cfg.ElementTypeName = "text"
	}
	if cfg.Column == "" {
		cfg.Column = "emails"
	}
	transformer, err := NewArrayTransformer(cfg)
	require.NoError(t, err)
	return transformer
}

func TestNewArrayTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ArrayConfig
		wantErr error
	}{
		{
			name: "ok - map",
			cfg:  ArrayConfig{ElementTransformer: upperTransformer(), Generator: ArrayGeneratorMap},
		},
		{
			name: "ok - random",
			cfg:  ArrayConfig{ElementTransformer: upperTransformer(), Generator: ArrayGeneratorRandom, MinCount: 0, MaxCount: 3},
		},
		{
			name:    "error - counts under map",
			cfg:     ArrayConfig{ElementTransformer: upperTransformer(), Generator: ArrayGeneratorMap, MaxCount: 3},
			wantErr: ErrInvalidArrayOptions,
		},
		{
			name:    "error - min greater than max",
			cfg:     ArrayConfig{ElementTransformer: upperTransformer(), Generator: ArrayGeneratorRandom, MinCount: 4, MaxCount: 3},
			wantErr: ErrInvalidArrayOptions,
		},
		{
			name:    "error - negative count",
			cfg:     ArrayConfig{ElementTransformer: upperTransformer(), Generator: ArrayGeneratorRandom, MinCount: -1, MaxCount: 3},
			wantErr: ErrInvalidArrayOptions,
		},
		{
			name:    "error - unknown generator",
			cfg:     ArrayConfig{ElementTransformer: upperTransformer(), Generator: ArrayGenerator("shuffle")},
			wantErr: ErrInvalidArrayOptions,
		},
		{
			name:    "error - no element transformer",
			cfg:     ArrayConfig{Generator: ArrayGeneratorMap},
			wantErr: ErrInvalidArrayOptions,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewArrayTransformer(tc.cfg)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestArrayTransformer_Transform_map(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name        string
		cfg         ArrayConfig
		value       any
		wantValue   any
		wantErr     error
		wantSeen    []any
		checkValues bool
	}{
		{
			name:      "literal in, literal out, quoting round trips",
			cfg:       ArrayConfig{ElementTransformer: upperTransformer()},
			value:     `{a,"b,c",NULL,"NULL","d\"e","f\\g"}`,
			wantValue: `{A,"B,C",NULL,"NULL","D\"E","F\\G"}`,
			// a NULL element is never handed to the element transformer, but
			// the string "NULL" is
			wantSeen:    []any{"a", "b,c", "NULL", `d"e`, `f\g`},
			checkValues: true,
		},
		{
			name:      "slice in, slice out",
			cfg:       ArrayConfig{ElementTransformer: upperTransformer()},
			value:     []any{"a", nil, "b"},
			wantValue: []any{"A", nil, "B"},
		},
		{
			name:      "bytes in, bytes out",
			cfg:       ArrayConfig{ElementTransformer: upperTransformer()},
			value:     []byte(`{a,b}`),
			wantValue: []byte(`{A,B}`),
		},
		{
			name:      "empty array",
			cfg:       ArrayConfig{ElementTransformer: upperTransformer()},
			value:     `{}`,
			wantValue: `{}`,
		},
		{
			name: "int4 elements are decoded to int32 on the literal path",
			cfg: ArrayConfig{
				ArrayOID:        pgtype.Int4ArrayOID,
				ElementTypeName: "int4",
				ElementTransformer: &stubTransformer{transformFn: func(value Value) (any, error) {
					i, ok := value.TransformValue.(int32)
					if !ok {
						return nil, fmt.Errorf("stub: got %T: %w", value.TransformValue, ErrUnsupportedValueType)
					}
					return i * 2, nil
				}},
			},
			value:     `{1,NULL,3}`,
			wantValue: `{2,NULL,6}`,
		},
		{
			name: "a nil result is a NULL element, and the length is preserved",
			cfg: ArrayConfig{ElementTransformer: &stubTransformer{transformFn: func(value Value) (any, error) {
				return nil, nil
			}}},
			value:     `{a,b,c}`,
			wantValue: `{NULL,NULL,NULL}`,
		},
		{
			name: "the first element error is the column error",
			cfg: ArrayConfig{ElementTransformer: &stubTransformer{transformFn: func(value Value) (any, error) {
				if value.TransformValue == "b" {
					return nil, errTest
				}
				return value.TransformValue, nil
			}}},
			value:   `{a,b,c}`,
			wantErr: errTest,
		},
		{
			name:    "multi-dimensional literal",
			cfg:     ArrayConfig{ElementTransformer: upperTransformer()},
			value:   `{{a,b},{c,d}}`,
			wantErr: ErrMultiDimensionalArray,
		},
		{
			name:    "unsupported value type",
			cfg:     ArrayConfig{ElementTransformer: upperTransformer()},
			value:   42,
			wantErr: ErrUnsupportedValueType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var seen []any
			if tc.checkValues {
				inner := tc.cfg.ElementTransformer
				tc.cfg.ElementTransformer = &stubTransformer{transformFn: func(value Value) (any, error) {
					seen = append(seen, value.TransformValue)
					return inner.Transform(context.Background(), value)
				}}
			}

			transformer := newTestArrayTransformer(t, tc.cfg)
			got, err := transformer.Transform(context.Background(), NewValue(tc.value, "text[]", nil))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Contains(t, err.Error(), "emails")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantValue, got)
			if tc.checkValues {
				require.Equal(t, tc.wantSeen, seen)
			}
		})
	}
}

func TestArrayTransformer_Transform_random(t *testing.T) {
	t.Parallel()

	t.Run("draws the count from the range and resamples the source", func(t *testing.T) {
		t.Parallel()
		transformer := newTestArrayTransformer(t, ArrayConfig{
			ElementTransformer: upperTransformer(),
			Generator:          ArrayGeneratorRandom,
			MinCount:           2,
			MaxCount:           5,
		})
		// the first draw picks the count within [0,max-min], the rest pick
		// source indexes
		draws := []int{2, 0, 0, 2, 2}
		transformer.randIntN = func(n int) int {
			draw := draws[0]
			draws = draws[1:]
			require.Less(t, draw, n)
			return draw
		}

		got, err := transformer.Transform(context.Background(), NewValue(`{a,b,c}`, "text[]", nil))
		require.NoError(t, err)
		require.Equal(t, `{A,A,C,C}`, got)
		require.Empty(t, draws)
	})

	t.Run("a NULL element is drawn as NULL without a redraw", func(t *testing.T) {
		t.Parallel()
		transformer := newTestArrayTransformer(t, ArrayConfig{
			ElementTransformer: upperTransformer(),
			Generator:          ArrayGeneratorRandom,
			MinCount:           2,
			MaxCount:           2,
		})
		transformer.randIntN = func(n int) int { return 1 }

		got, err := transformer.Transform(context.Background(), NewValue(`{a,NULL}`, "text[]", nil))
		require.NoError(t, err)
		require.Equal(t, `{NULL,NULL}`, got)
	})

	t.Run("an empty source array stays empty", func(t *testing.T) {
		t.Parallel()
		transformer := newTestArrayTransformer(t, ArrayConfig{
			ElementTransformer: &stubTransformer{transformFn: func(Value) (any, error) {
				t.Fatal("the element transformer must not be called for an empty array")
				return nil, nil
			}},
			Generator: ArrayGeneratorRandom,
			MinCount:  1,
			MaxCount:  3,
		})

		got, err := transformer.Transform(context.Background(), NewValue(`{}`, "text[]", nil))
		require.NoError(t, err)
		require.Equal(t, `{}`, got)
	})

	t.Run("a zero count emits an empty array", func(t *testing.T) {
		t.Parallel()
		transformer := newTestArrayTransformer(t, ArrayConfig{
			ElementTransformer: upperTransformer(),
			Generator:          ArrayGeneratorRandom,
			MinCount:           0,
			MaxCount:           0,
		})

		got, err := transformer.Transform(context.Background(), NewValue(`{a,b}`, "text[]", nil))
		require.NoError(t, err)
		require.Equal(t, `{}`, got)
	})
}

func TestArrayTransformer_Uniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		generator ArrayGenerator
		inner     Uniqueness
		want      Uniqueness
	}{
		{name: "map preserves a preserving element transformer", generator: ArrayGeneratorMap, inner: UniquenessPreserved, want: UniquenessPreserved},
		{name: "map keeps a lossy element transformer lossy", generator: ArrayGeneratorMap, inner: UniquenessLossy, want: UniquenessLossy},
		{name: "map resolves an unclassified element transformer", generator: ArrayGeneratorMap, inner: UniquenessUnspecified, want: UniquenessNotGuaranteed},
		{name: "random is lossy over a preserving element transformer", generator: ArrayGeneratorRandom, inner: UniquenessPreserved, want: UniquenessLossy},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := ArrayConfig{
				ElementTransformer: &stubTransformer{transformFn: func(Value) (any, error) { return nil, nil }, uniqueness: tc.inner},
				Generator:          tc.generator,
			}
			if tc.generator == ArrayGeneratorRandom {
				cfg.MaxCount = 3
			}
			transformer := newTestArrayTransformer(t, cfg)
			require.Equal(t, tc.want, UniquenessOf(transformer))
		})
	}
}

func TestArrayTransformer_delegatesToTheElementTransformer(t *testing.T) {
	t.Parallel()

	errClose := errors.New("oh noes")
	inner := &stubTransformer{
		transformFn: func(Value) (any, error) { return nil, nil },
		dynamic:     true,
		closeErr:    errClose,
	}
	transformer := newTestArrayTransformer(t, ArrayConfig{ElementTransformer: inner})

	require.Equal(t, inner.Type(), transformer.Type())
	require.Equal(t, inner.CompatibleTypes(), transformer.CompatibleTypes())
	require.True(t, transformer.IsDynamic())
	require.ErrorIs(t, transformer.Close(), errClose)
}

func TestArrayTransformer_sharesDynamicValuesWithEveryElement(t *testing.T) {
	t.Parallel()

	dynamicValues := map[string]any{"country": "ES"}
	var seen []map[string]any
	transformer := newTestArrayTransformer(t, ArrayConfig{
		ElementTransformer: &stubTransformer{transformFn: func(value Value) (any, error) {
			seen = append(seen, value.DynamicValues)
			require.Equal(t, "text", value.TransformType)
			return value.TransformValue, nil
		}},
	})

	_, err := transformer.Transform(context.Background(), NewValue(`{a,b}`, "text[]", dynamicValues))
	require.NoError(t, err)
	require.Equal(t, []map[string]any{dynamicValues, dynamicValues}, seen)
}
