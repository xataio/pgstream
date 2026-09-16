// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/testcontainers"
)

// EnumForOID filters on typtype='e' alone, which is only correct if postgres
// reports a domain's base type in the row description rather than the domain's
// own OID. Everything else in this package resolves typbasetype from the
// catalog explicitly, so the assumption is checked here against a real server
// instead of being asserted in a comment.
func TestMapper_EnumForOID_Integration(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	for _, image := range []testcontainers.PostgresImage{testcontainers.Postgres14, testcontainers.Postgres17} {
		t.Run(string(image), func(t *testing.T) {
			assertEnumForOID(t, ctx, image)
		})
	}
}

func assertEnumForOID(t *testing.T, ctx context.Context, image testcontainers.PostgresImage) {
	t.Helper()

	var pgURL string
	cleanup, err := testcontainers.SetupPostgresContainer(ctx, &pgURL, image)
	require.NoError(t, err)
	defer cleanup()

	conn, err := NewConn(ctx, pgURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
		CREATE DOMAIN mood_domain AS mood;
		CREATE DOMAIN text_domain AS text;
		CREATE TABLE feelings (
			id     bigint,
			mood   mood,
			backup mood_domain,
			moods  mood[],
			note   text,
			label  text_domain
		)`)
	require.NoError(t, err)

	// the OIDs the parser sees, taken the same way getFieldDescriptions and
	// the snapshot row adapter take them
	rows, err := conn.Query(ctx, "SELECT * FROM feelings LIMIT 0")
	require.NoError(t, err)
	oidByColumn := map[string]uint32{}
	for _, desc := range rows.FieldDescriptions() {
		oidByColumn[string(desc.Name)] = desc.DataTypeOID
	}
	rows.Close()
	require.Len(t, oidByColumn, 6)

	labels := []string{"sad", "ok", "happy"}

	tests := []struct {
		column   string
		wantEnum *EnumType
	}{
		{column: "mood", wantEnum: &EnumType{Name: "mood", Labels: labels}},
		{
			// the assumption under test: the row description carries mood's
			// OID, not mood_domain's, so no typbasetype lookup is needed
			column:   "backup",
			wantEnum: &EnumType{Name: "mood", Labels: labels},
		},
		{
			// an array has its own OID, so it names no enum here; a caller
			// that wants the element enum resolves the element type first
			column:   "moods",
			wantEnum: nil,
		},
		{column: "note", wantEnum: nil},
		{column: "label", wantEnum: nil},
		{column: "id", wantEnum: nil},
	}

	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			mapper := NewMapper(conn)

			enum, err := mapper.EnumForOID(ctx, oidByColumn[tc.column])
			require.NoError(t, err)
			require.Equal(t, tc.wantEnum, enum)
		})
	}
}
