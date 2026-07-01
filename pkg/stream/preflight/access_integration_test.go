// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/testcontainers"
)

func TestSourceSequenceSelectPrivilegesCheck_Run_IdentityColumnsAreChecked(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, testcontainers.Postgres14)
	require.NoError(t, err)
	defer cleanup()

	adminConn, err := pglib.NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer adminConn.Close(ctx)

	_, err = adminConn.Exec(ctx, `
		CREATE TABLE public.identity_orders (
			id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			payload text
		)
	`)
	require.NoError(t, err)

	_, err = adminConn.Exec(ctx, `CREATE ROLE identity_reader LOGIN PASSWORD 'secret'`)
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, `GRANT USAGE ON SCHEMA public TO identity_reader`)
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, `GRANT SELECT ON TABLE public.identity_orders TO identity_reader`)
	require.NoError(t, err)
	_, err = adminConn.Exec(ctx, `CREATE SEQUENCE public.free_standing_seq`)
	require.NoError(t, err)

	var sequenceName string
	err = adminConn.QueryRow(ctx, []any{&sequenceName}, `SELECT pg_get_serial_sequence('public.identity_orders', 'id')`)
	require.NoError(t, err)
	require.Equal(t, "public.identity_orders_id_seq", sequenceName)

	readerURL := mustRewritePGUser(t, pgURL, "identity_reader", "secret")
	check := &SourceSequenceSelectPrivilegesCheck{
		Source: func(ctx context.Context) (pglib.Querier, error) {
			return pglib.NewConn(ctx, readerURL)
		},
	}

	findings, err := check.Run(ctx)

	require.NoError(t, err)
	require.Len(t, findings, 2)
	allMessages := findings[0].Message + findings[1].Message
	require.Contains(t, allMessages, "public.identity_orders_id_seq")
	require.Contains(t, allMessages, "public.free_standing_seq")
	require.Contains(t, allMessages, `source role "identity_reader"`)
	require.Contains(t, allMessages, `GRANT SELECT ON SEQUENCE "public"."identity_orders_id_seq" TO "identity_reader"`)
	require.Contains(t, allMessages, `GRANT SELECT ON SEQUENCE "public"."free_standing_seq" TO "identity_reader"`)
}

func mustRewritePGUser(t *testing.T, rawURL, user, password string) string {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}
