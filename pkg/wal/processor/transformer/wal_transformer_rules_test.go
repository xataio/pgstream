// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_readRulesFromFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string

		wantRules *Rules
		wantErr   error
	}{
		{
			name:     "ok",
			filename: "test/test_transformer_rules.yaml",
			wantRules: &Rules{
				Transformers: []TableRules{
					{
						Schema: "public",
						Table:  "test1",
						ColumnRules: map[string]TransformerRules{
							"column_1": {
								Name: "string",
								Parameters: map[string]any{
									"min_length": 1,
									"max_length": 2,
								},
							},
						},
					},
					{
						Schema: "test",
						Table:  "test2",
						ColumnRules: map[string]TransformerRules{
							"column_2": {
								Name: "string",
								Parameters: map[string]any{
									"symbols": "abcdef",
								},
							},
						},
					},
				},
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rules, err := readRulesFromFile(tc.filename)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantRules, rules)
		})
	}
}
