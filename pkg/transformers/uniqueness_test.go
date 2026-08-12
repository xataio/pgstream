// SPDX-License-Identifier: Apache-2.0

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniqueness_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		uniqueness Uniqueness
		want       string
	}{
		{uniqueness: UniquenessUnspecified, want: "unspecified"},
		{uniqueness: UniquenessLossy, want: "lossy"},
		{uniqueness: UniquenessNotGuaranteed, want: "not_guaranteed"},
		{uniqueness: UniquenessPreserved, want: "preserved"},
		{uniqueness: Uniqueness(99), want: "unspecified"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.uniqueness.String())
		})
	}
}

// the zero value must not read as a deliberate classification, or a
// transformer whose author forgot to classify it ships as lossy
func TestUniqueness_ZeroValueIsUnspecified(t *testing.T) {
	t.Parallel()

	var zero Uniqueness
	require.Equal(t, UniquenessUnspecified, zero)
	require.Equal(t, "unspecified", zero.String())
	require.Equal(t, UniquenessUnspecified, Definition{}.Uniqueness)
}

type unclassifiedTransformer struct{}

func (unclassifiedTransformer) Transform(context.Context, Value) (any, error) { return nil, nil }
func (unclassifiedTransformer) IsDynamic() bool                               { return false }
func (unclassifiedTransformer) CompatibleTypes() []SupportedDataType          { return nil }
func (unclassifiedTransformer) Type() TransformerType                         { return "unclassified" }
func (unclassifiedTransformer) Close() error                                  { return nil }

type classifiedTransformer struct {
	unclassifiedTransformer
	uniqueness Uniqueness
}

func (c classifiedTransformer) Uniqueness() Uniqueness { return c.uniqueness }

func TestUniquenessOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		transformer Transformer
		want        Uniqueness
	}{
		{
			name:        "transformer that does not implement the interface",
			transformer: unclassifiedTransformer{},
			want:        UniquenessNotGuaranteed,
		},
		{
			name:        "transformer that reports the zero value",
			transformer: classifiedTransformer{uniqueness: UniquenessUnspecified},
			want:        UniquenessNotGuaranteed,
		},
		{
			name:        "lossy",
			transformer: classifiedTransformer{uniqueness: UniquenessLossy},
			want:        UniquenessLossy,
		},
		{
			name:        "preserved",
			transformer: classifiedTransformer{uniqueness: UniquenessPreserved},
			want:        UniquenessPreserved,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, UniquenessOf(tt.transformer))
		})
	}
}
