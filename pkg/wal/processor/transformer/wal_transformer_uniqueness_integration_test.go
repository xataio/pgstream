// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
)

// the unique index lookup is a raw catalog query, so the mocked unit tests
// cannot tell whether it actually selects the right indexes and columns
func TestPostgresTransformerParser_getUniqueIndexes_Integration(t *testing.T) {
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
		CREATE TABLE public.patients (
			id bigint PRIMARY KEY,
			pms_patient_id text,
			pms_type text,
			email text,
			display_name text,
			nickname text
		);
		CREATE UNIQUE INDEX patients_pms_idx ON public.patients (pms_patient_id, pms_type);
		CREATE UNIQUE INDEX patients_lower_email ON public.patients (lower(email));
		CREATE UNIQUE INDEX patients_id_incl ON public.patients (id) INCLUDE (display_name);
		CREATE INDEX patients_nickname_idx ON public.patients (nickname);
	`)
	require.NoError(t, err)

	// an invalid index, as left behind by a failed CREATE INDEX CONCURRENTLY:
	// pg_dump skips it, so the target never enforces it
	_, err = adminConn.Exec(ctx, `
		INSERT INTO public.patients (id, nickname) VALUES (1, 'dup'), (2, 'dup');
		CREATE UNIQUE INDEX CONCURRENTLY patients_nickname_key ON public.patients (nickname);
	`)
	require.Error(t, err, "expected the concurrent unique index build to fail on duplicates")

	parser, err := NewPostgresTransformerParser(ctx, pgURL, builder.NewTransformerBuilder(), nil)
	require.NoError(t, err)
	defer parser.Close()

	indexes, err := parser.getUniqueIndexes(ctx, "public", "patients")
	require.NoError(t, err)

	byName := make(map[string]uniqueIndex, len(indexes))
	for _, index := range indexes {
		byName[index.name] = index
	}

	// composite unique index: both key columns, in index order
	require.Equal(t, uniqueIndex{
		name:    "patients_pms_idx",
		columns: []string{"pms_patient_id", "pms_type"},
	}, byName["patients_pms_idx"])

	// primary key, flagged as such
	require.Equal(t, uniqueIndex{
		name:    "patients_pkey",
		primary: true,
		columns: []string{"id"},
	}, byName["patients_pkey"])

	// INCLUDE columns do not enforce uniqueness, so display_name must not appear
	require.Equal(t, uniqueIndex{
		name:    "patients_id_incl",
		columns: []string{"id"},
	}, byName["patients_id_incl"])

	// an expression index resolves to no column, but must still be reported so
	// the caller can warn rather than silently pass the table
	require.Equal(t, uniqueIndex{
		name:           "patients_lower_email",
		hasExpressions: true,
	}, byName["patients_lower_email"])

	// non-unique and invalid indexes are not constraints to preserve
	require.NotContains(t, byName, "patients_nickname_idx")
	require.NotContains(t, byName, "patients_nickname_key")
	require.Len(t, indexes, 4)
}
