// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
	transformermocks "github.com/xataio/pgstream/pkg/transformers/mocks"
)

func TestTransformerParser_parse(t *testing.T) {
	t.Parallel()

	testSchema := "test_schema"
	testTable := "test_table"
	testTransformer, err := transformers.NewStringTransformer(nil)
	require.NoError(t, err)
	testKey := "\"test_schema\".\"test_table\""

	mockBuilder := &transformermocks.TransformerBuilder{
		NewFn: func(cfg *transformers.Config) (transformers.Transformer, error) {
			if cfg.Name == "string" {
				return testTransformer, nil
			}
			return nil, transformers.ErrUnsupportedTransformer
		},
	}

	tests := []struct {
		name  string
		rules []TableRules

		wantTransformerMap *TransformerMap
		wantErr            error
	}{
		{
			name: "ok",
			rules: []TableRules{
				{
					Schema: testSchema,
					Table:  testTable,
					ColumnRules: map[string]TransformerRules{
						"column_1": {
							Name: "string",
						},
						"column_2": {
							Name: "string",
						},
					},
				},
			},

			wantTransformerMap: &TransformerMap{
				activeTransformerMap: map[string]ColumnTransformers{
					testKey: {
						"column_1": testTransformer,
						"column_2": testTransformer,
					},
				},
				noopTransformerMap: map[string]ColumnTransformers{},
			},
			wantErr: nil,
		},
		{
			name:  "ok - no rules",
			rules: []TableRules{},

			wantTransformerMap: NewTransformerMap(),
			wantErr:            nil,
		},
		{
			name: "error - invalid transformer rules",
			rules: []TableRules{
				{
					Schema: testSchema,
					Table:  testTable,
					ColumnRules: map[string]TransformerRules{
						"column_1": {
							Name: "invalid",
						},
					},
				},
			},

			wantTransformerMap: nil,
			wantErr:            transformers.ErrUnsupportedTransformer,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tp := newTransformerParser(mockBuilder)

			transformerMap, err := tp.parse(context.Background(), Rules{Transformers: tc.rules})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantTransformerMap, transformerMap)
		})
	}
}
