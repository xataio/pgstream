// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/internal/postgres/mocks"
)

func TestPgVersion_String(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", pgVersion{}.String())
	require.Equal(t, "18.4", pgVersion{num: 180004}.String())
	require.Equal(t, "16.10", pgVersion{num: 160010}.String())
	require.Equal(t, "15.0", pgVersion{num: 150000}.String())
}

// versionConn returns an AcquireFunc whose Querier answers the
// server_version_num query with the given major version (as major.4, e.g.
// major=18 -> 180004), exercising the /10000 major extraction.
func versionConn(t *testing.T, major int) postgres.AcquireFunc {
	return func(context.Context) (postgres.Querier, error) {
		return &mocks.Querier{
			QueryRowFn: func(_ context.Context, dest []any, _ string, _ ...any) error {
				num, ok := dest[0].(*int)
				require.True(t, ok)
				*num = major*10000 + 4
				return nil
			},
		}, nil
	}
}

func TestPostgresVersionCheck_Run_SourceOnly(t *testing.T) {
	t.Parallel()

	// No target: informational only — never a finding, and Details carries just
	// the source version (no target_version key at all).
	check := &PostgresVersionCheck{Source: versionConn(t, 18)}

	findings, err := check.Run(context.Background())

	require.NoError(t, err)
	require.Empty(t, findings)
	require.Equal(t, map[string]any{"source_version": "18.4"}, check.Details())
}

func TestPostgresVersionCheck_Run_WithTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sourceMajor int
		targetMajor int
		wantHit     bool
		wantSubs    []string
	}{
		{
			name:        "target newer than source",
			sourceMajor: 15,
			targetMajor: 18,
		},
		{
			name:        "target equal to source",
			sourceMajor: 16,
			targetMajor: 16,
		},
		{
			name:        "target older than source is a finding",
			sourceMajor: 18,
			targetMajor: 15,
			wantHit:     true,
			wantSubs: []string{
				"source is PostgreSQL 18.4",
				"target is PostgreSQL 15.4",
				"PostgreSQL 18 or newer",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			check := &PostgresVersionCheck{
				Source: versionConn(t, tc.sourceMajor),
				Target: versionConn(t, tc.targetMajor),
			}

			findings, err := check.Run(context.Background())
			require.NoError(t, err)

			if !tc.wantHit {
				require.Empty(t, findings)
			} else {
				require.Len(t, findings, 1)
				for _, sub := range tc.wantSubs {
					require.Contains(t, findings[0].Message, sub)
				}
			}

			// With a target, Details records both full versions regardless of
			// outcome (versionConn reports each major as major.4).
			require.Equal(t, map[string]any{
				"source_version": fmt.Sprintf("%d.4", tc.sourceMajor),
				"target_version": fmt.Sprintf("%d.4", tc.targetMajor),
			}, check.Details())
		})
	}
}

func TestPostgresVersionCheck_Details_BeforeRun(t *testing.T) {
	t.Parallel()

	// Source-only: only source_version, empty until Run reads it.
	require.Equal(t, map[string]any{"source_version": ""}, (&PostgresVersionCheck{}).Details())

	// With a target configured, target_version is present too (empty pre-Run).
	withTarget := &PostgresVersionCheck{Target: versionConn(t, 18)}
	require.Equal(t, map[string]any{"source_version": "", "target_version": ""}, withTarget.Details())
}

func TestPostgresVersionCheck_Run_SourceAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &PostgresVersionCheck{
		Source: func(context.Context) (postgres.Querier, error) { return nil, checkErr },
		Target: versionConn(t, 18),
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to source")
}

func TestPostgresVersionCheck_Run_SourceQueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &PostgresVersionCheck{
		Source: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryRowFn: func(context.Context, []any, string, ...any) error { return queryErr },
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying source version")
}

func TestPostgresVersionCheck_Run_TargetAcquireFails(t *testing.T) {
	t.Parallel()

	checkErr := errors.New("boom")
	check := &PostgresVersionCheck{
		Source: versionConn(t, 15),
		Target: func(context.Context) (postgres.Querier, error) { return nil, checkErr },
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, checkErr)
	require.ErrorContains(t, err, "connecting to target")
}

func TestPostgresVersionCheck_Run_TargetQueryFails(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	check := &PostgresVersionCheck{
		Source: versionConn(t, 15),
		Target: func(context.Context) (postgres.Querier, error) {
			return &mocks.Querier{
				QueryRowFn: func(context.Context, []any, string, ...any) error { return queryErr },
			}, nil
		},
	}

	findings, err := check.Run(context.Background())

	require.Nil(t, findings)
	require.ErrorIs(t, err, queryErr)
	require.ErrorContains(t, err, "querying target version")
}

func TestPostgresVersionCheck_Name(t *testing.T) {
	t.Parallel()

	require.Equal(t, "postgres_version", (&PostgresVersionCheck{}).Name())
}
