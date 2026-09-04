// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/transformers"
	transformermocks "github.com/xataio/pgstream/pkg/transformers/mocks"
)

func newUniquenessMock(i transformers.Uniqueness) *transformermocks.Transformer {
	return &transformermocks.Transformer{
		TransformFn:  func(transformers.Value) (any, error) { return nil, nil },
		UniquenessFn: func() transformers.Uniqueness { return i },
	}
}

func TestValidateUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		indexes             []uniqueIndex
		columnTransformers  ColumnTransformers
		allowUniquenessLoss map[string]bool

		wantErrors   []string
		wantWarnings []string
	}{
		{
			name:               "ok - no unique indexes",
			indexes:            nil,
			columnTransformers: ColumnTransformers{"id": newUniquenessMock(transformers.UniquenessLossy)},
		},
		{
			name:               "ok - unique index on untransformed column",
			indexes:            []uniqueIndex{{name: "users_email_key", columns: []string{"email"}}},
			columnTransformers: ColumnTransformers{"name": newUniquenessMock(transformers.UniquenessLossy)},
		},
		{
			name:               "ok - uniqueness preserving transformer on unique index",
			indexes:            []uniqueIndex{{name: "users_email_key", columns: []string{"email"}}},
			columnTransformers: ColumnTransformers{"email": newUniquenessMock(transformers.UniquenessPreserved)},
		},
		{
			name:               "ok - noop transformer on unique index",
			indexes:            []uniqueIndex{{name: "users_email_key", columns: []string{"email"}}},
			columnTransformers: ColumnTransformers{"email": nil},
		},
		{
			name:               "error - lossy transformer on unique index",
			indexes:            []uniqueIndex{{name: "users_email_key", columns: []string{"email"}}},
			columnTransformers: ColumnTransformers{"email": newUniquenessMock(transformers.UniquenessLossy)},
			wantErrors: []string{
				`"public"."users": unique index "users_email_key" (email) is covered by a transformer that maps distinct values to the same output ("email" uses "mock"), ` +
					`which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, ` +
					`or set allow_uniqueness_loss on the column to override`,
			},
		},
		{
			name:               "error - lossy transformer on one column of a composite unique index",
			indexes:            []uniqueIndex{{name: "patients_pms_idx", columns: []string{"pms_patient_id", "pms_type"}}},
			columnTransformers: ColumnTransformers{"pms_patient_id": newUniquenessMock(transformers.UniquenessLossy)},
			wantErrors: []string{
				`"public"."users": unique index "patients_pms_idx" (pms_patient_id, pms_type) is covered by a transformer that maps distinct values to the same output ` +
					`("pms_patient_id" uses "mock"), which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, ` +
					`or set allow_uniqueness_loss on the column to override`,
			},
		},
		{
			name:               "error - primary key described as such",
			indexes:            []uniqueIndex{{name: "users_pkey", primary: true, columns: []string{"id"}}},
			columnTransformers: ColumnTransformers{"id": newUniquenessMock(transformers.UniquenessLossy)},
			wantErrors: []string{
				`"public"."users": primary key "users_pkey" (id) is covered by a transformer that maps distinct values to the same output ("id" uses "mock"), ` +
					`which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, ` +
					`or set allow_uniqueness_loss on the column to override`,
			},
		},
		{
			name:               "warning - not guaranteed transformer on unique index",
			indexes:            []uniqueIndex{{name: "users_email_key", columns: []string{"email"}}},
			columnTransformers: ColumnTransformers{"email": newUniquenessMock(transformers.UniquenessNotGuaranteed)},
			wantWarnings: []string{
				`"public"."users": unique index "users_email_key" (email) is covered by a transformer that does not guarantee unique output ("email" uses "mock"), ` +
					`so duplicate key violations are possible`,
			},
		},
		{
			name:    "error - lossy takes precedence over not guaranteed on the same index",
			indexes: []uniqueIndex{{name: "users_idx", columns: []string{"email", "name"}}},
			columnTransformers: ColumnTransformers{
				"email": newUniquenessMock(transformers.UniquenessNotGuaranteed),
				"name":  newUniquenessMock(transformers.UniquenessLossy),
			},
			wantErrors: []string{
				`"public"."users": unique index "users_idx" (email, name) is covered by a transformer that maps distinct values to the same output ("name" uses "mock"), ` +
					`which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, ` +
					`or set allow_uniqueness_loss on the column to override`,
			},
		},
		{
			name:                "ok - lossy transformer with allow_uniqueness_loss",
			indexes:             []uniqueIndex{{name: "users_email_key", columns: []string{"email"}}},
			columnTransformers:  ColumnTransformers{"email": newUniquenessMock(transformers.UniquenessLossy)},
			allowUniquenessLoss: map[string]bool{"email": true},
		},
		{
			name: "findings sorted for stable output",
			indexes: []uniqueIndex{
				{name: "users_z_key", columns: []string{"z"}},
				{name: "users_a_key", columns: []string{"a"}},
			},
			columnTransformers: ColumnTransformers{
				"z": newUniquenessMock(transformers.UniquenessLossy),
				"a": newUniquenessMock(transformers.UniquenessLossy),
			},
			wantErrors: []string{
				`"public"."users": unique index "users_a_key" (a) is covered by a transformer that maps distinct values to the same output ("a" uses "mock"), ` +
					`which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, ` +
					`or set allow_uniqueness_loss on the column to override`,
				`"public"."users": unique index "users_z_key" (z) is covered by a transformer that maps distinct values to the same output ("z" uses "mock"), ` +
					`which will cause duplicate key violations. Use a transformer that preserves uniqueness, such as encrypted_aes_siv or fpe_ff1, ` +
					`or set allow_uniqueness_loss on the column to override`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			findings := validateUniqueness("public", "users", tc.indexes, tc.columnTransformers, tc.allowUniquenessLoss)
			require.Equal(t, tc.wantErrors, findings.errors)
			require.Equal(t, tc.wantWarnings, findings.warnings)
		})
	}
}

func TestValidateUniqueness_transformerTypesAreClassified(t *testing.T) {
	t.Parallel()

	maskingTransformer, err := transformers.NewMaskingTransformer(transformers.ParameterValues{"type": "id"})
	require.NoError(t, err)
	require.Equal(t, transformers.UniquenessLossy, maskingTransformer.Uniqueness())

	findings := validateUniqueness("public", "patients",
		[]uniqueIndex{{name: "patients_pms_idx", columns: []string{"pms_patient_id"}}},
		ColumnTransformers{"pms_patient_id": maskingTransformer},
		nil)

	require.Len(t, findings.errors, 1)
	require.Contains(t, findings.errors[0], `"pms_patient_id" uses "masking"`)
	require.Empty(t, findings.warnings)
}
