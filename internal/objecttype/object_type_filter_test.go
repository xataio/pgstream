// SPDX-License-Identifier: Apache-2.0

package objecttype

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testCategories = map[string][]string{
	"tables":    {"TABLE", "DEFAULT"},
	"sequences": {"SEQUENCE"},
	"functions": {"FUNCTION", "PROCEDURE"},
}

func TestNewFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		include []string
		exclude []string

		wantNil    bool
		wantErr    error
		wantExcCat []string
		wantExcTyp []string
	}{
		{
			name:    "no include or exclude",
			wantNil: true,
		},
		{
			name:    "both include and exclude",
			include: []string{"tables"},
			exclude: []string{"sequences"},
			wantErr: ErrIncludeExcludeConflict,
		},
		{
			name:    "unknown include category",
			include: []string{"widgets"},
			wantErr: ErrUnknownCategory,
		},
		{
			name:    "unknown exclude category",
			exclude: []string{"widgets"},
			wantErr: ErrUnknownCategory,
		},
		{
			name:       "exclude list",
			exclude:    []string{"functions"},
			wantExcCat: []string{"functions"},
			wantExcTyp: []string{"FUNCTION", "PROCEDURE"},
		},
		{
			name:       "include list excludes everything else",
			include:    []string{"tables"},
			wantExcCat: []string{"sequences", "functions"},
			wantExcTyp: []string{"SEQUENCE", "FUNCTION", "PROCEDURE"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f, err := NewFilter(testCategories, tc.include, tc.exclude)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			if tc.wantNil {
				require.Nil(t, f)
				return
			}
			require.NotNil(t, f)

			for _, cat := range tc.wantExcCat {
				require.True(t, f.IsCategoryExcluded(cat), "category %q should be excluded", cat)
			}
			for _, typ := range tc.wantExcTyp {
				require.True(t, f.IsTypeExcluded(typ), "type %q should be excluded", typ)
			}
		})
	}
}

func TestFilter_NilIsNoop(t *testing.T) {
	t.Parallel()

	var f *Filter
	require.False(t, f.IsCategoryExcluded("tables"))
	require.False(t, f.IsTypeExcluded("TABLE"))
}
