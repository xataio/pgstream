// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
)

func TestPGSchemaObserver_queryTableSequences_catalogDependencies(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		image testcontainers.PostgresImage
	}{
		{name: "postgres 14", image: testcontainers.Postgres14},
		{name: "postgres 17", image: testcontainers.Postgres17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pgURL string
			cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, tc.image)
			require.NoError(t, err)
			defer cleanup()

			conn, err := pglib.NewConn(ctx, pgURL)
			require.NoError(t, err)
			defer conn.Close(ctx)

			setup := []string{
				`CREATE SCHEMA source_schema`,
				`CREATE SCHEMA sequence_schema`,
				`CREATE SEQUENCE sequence_schema.explicit_id_sequence`,
				`CREATE SEQUENCE sequence_schema.derived_id_sequence`,
				`CREATE TABLE source_schema.sequence_catalog_table (
					serial_id bigserial,
					identity_id bigint GENERATED ALWAYS AS IDENTITY,
					explicit_id bigint DEFAULT nextval('sequence_schema.explicit_id_sequence'::regclass),
					derived_id bigint DEFAULT (nextval('sequence_schema.derived_id_sequence'::regclass) + 100),
					payload text
				)`,
			}
			for _, stmt := range setup {
				_, err := conn.Exec(ctx, stmt)
				require.NoError(t, err, stmt)
			}

			// The fixture must exercise the issue's unowned-sequence shape. If it
			// becomes owned, the old catalog query would also find it.
			requireCount(t, conn, `SELECT count(*)
				FROM pg_depend d
				WHERE d.classid = 'pg_class'::regclass
					AND d.objid = 'sequence_schema.explicit_id_sequence'::regclass
					AND d.refclassid = 'pg_class'::regclass
					AND d.deptype IN ('a', 'i')`, 0)

			observer := &pgSchemaObserver{}
			got, err := observer.queryTableSequences(ctx, conn, "source_schema", "sequence_catalog_table")
			require.NoError(t, err)
			require.Equal(t, map[string]string{
				`"serial_id"`:   `"source_schema"."sequence_catalog_table_serial_id_seq"`,
				`"identity_id"`: `"source_schema"."sequence_catalog_table_identity_id_seq"`,
				`"explicit_id"`: `"sequence_schema"."explicit_id_sequence"`,
			}, got)
			require.NotContains(t, got, `"derived_id"`)
		})
	}
}
