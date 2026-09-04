// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
)

// an enum's OID is assigned by the database, so which transformers a column of
// one accepts cannot be decided without reading the catalog. The mocked unit
// tests cannot cover that lookup, nor the labels it feeds greenmask_choice.
func TestPostgresTransformerParser_EnumColumns_Integration(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres17)
	require.NoError(t, err)
	defer cleanup()

	adminConn, err := pglib.NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer adminConn.Close(ctx)

	_, err = adminConn.Exec(ctx, `
		CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
		CREATE DOMAIN mood_domain AS mood;
		CREATE TABLE public.feelings (
			id     bigint PRIMARY KEY,
			mood   mood,
			backup mood_domain,
			moods  mood[],
			note   text
		);
	`)
	require.NoError(t, err)

	tests := []struct {
		name        string
		column      string
		rules       TransformerRules
		wantErr     error
		wantErrMsg  string
		wantChoices []string
	}{
		{
			name:        "choices default to the enum labels",
			column:      "mood",
			rules:       TransformerRules{Name: "greenmask_choice"},
			wantChoices: []string{"sad", "ok", "happy"},
		},
		{
			name:        "a domain over an enum resolves to the enum",
			column:      "backup",
			rules:       TransformerRules{Name: "greenmask_choice"},
			wantChoices: []string{"sad", "ok", "happy"},
		},
		{
			name:        "an explicit subset of the labels is kept",
			column:      "mood",
			rules:       TransformerRules{Name: "greenmask_choice", Parameters: map[string]any{"choices": []any{"ok"}}},
			wantChoices: []string{"ok"},
		},
		{
			name:    "a choice the enum does not have is rejected",
			column:  "mood",
			rules:   TransformerRules{Name: "greenmask_choice", Parameters: map[string]any{"choices": []any{"elated"}}},
			wantErr: ErrInvalidEnumChoice,
		},
		{
			// an array resolves to no enum, so nothing defaults its choices
			// and the builder rejects it first
			name:       "an array of the enum gets no defaulted choices",
			column:     "moods",
			rules:      TransformerRules{Name: "greenmask_choice"},
			wantErrMsg: `choices must not be empty`,
		},
		{
			name:       "an array of the enum has no transformer",
			column:     "moods",
			rules:      TransformerRules{Name: "greenmask_choice", Parameters: map[string]any{"choices": []any{"sad"}}},
			wantErrMsg: `does not support pg data type: _mood`,
		},
		{
			name:       "a transformer that cannot produce a label is rejected",
			column:     "mood",
			rules:      TransformerRules{Name: "masking", Parameters: map[string]any{"type": "default"}},
			wantErrMsg: `does not support pg data type: mood`,
		},
		{
			name:   "a transformer supporting every type is still accepted",
			column: "mood",
			rules:  TransformerRules{Name: "literal_string", Parameters: map[string]any{"literal": "ok"}},
		},
		{
			name:        "a non enum column is unaffected",
			column:      "note",
			rules:       TransformerRules{Name: "greenmask_choice", Parameters: map[string]any{"choices": []any{"x", "y"}}},
			wantChoices: []string{"x", "y"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := NewPostgresTransformerParser(ctx, pgURL, builder.NewTransformerBuilder(), nil)
			require.NoError(t, err)
			defer parser.Close()

			transformerMap, err := parser.ParseAndValidate(ctx, Rules{
				Transformers: []TableRules{{
					Schema:      "public",
					Table:       "feelings",
					ColumnRules: map[string]TransformerRules{tc.column: tc.rules},
				}},
			})

			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
				return
			case tc.wantErrMsg != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErrMsg)
				return
			}
			require.NoError(t, err)
			defer transformerMap.Close()

			if tc.wantChoices == nil {
				return
			}

			// the configured choices are not readable from the transformer, so
			// exercise it until it has produced every one of them
			columnTransformers, found := transformerMap.GetActiveColumnTransformers("public", "feelings")
			require.True(t, found)
			tr := columnTransformers[tc.column]
			require.NotNil(t, tr)

			// 200 draws over at most three labels: the chance of missing one
			// is about 3*(2/3)^200, far below any flake worth worrying about
			seen := map[string]bool{}
			for range 200 {
				got, err := tr.Transform(ctx, transformers.NewValue("sad", "mood", nil))
				require.NoError(t, err)
				label, ok := got.(string)
				require.True(t, ok, "expected a string label, got %T", got)
				require.Contains(t, tc.wantChoices, label)
				seen[label] = true
			}
			require.Len(t, seen, len(tc.wantChoices), "expected every choice to be reachable")
		})
	}
}
