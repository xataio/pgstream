// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "github.com/xataio/pgstream/internal/postgres"
	pgmocks "github.com/xataio/pgstream/internal/postgres/mocks"
)

func TestSnapshotGenerator_setTransactionSnapshot(t *testing.T) {
	t.Parallel()

	snapshotMissing := &pglib.ErrRelationDoesNotExist{Details: `snapshot "abc" does not exist`}
	otherErr := errors.New("some other failure")

	tests := []struct {
		name          string
		execErr       error
		wantErr       bool
		wantExplained bool // load-balanced hint joined onto the error
	}{
		{name: "success", execErr: nil, wantErr: false},
		{name: "snapshot missing enriched", execErr: snapshotMissing, wantErr: true, wantExplained: true},
		{name: "other error not enriched", execErr: otherErr, wantErr: true, wantExplained: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sg := &SnapshotGenerator{}
			tx := &pgmocks.Tx{
				ExecFn: func(context.Context, uint, string, ...any) (pglib.CommandTag, error) {
					return pglib.CommandTag{}, tt.execErr
				},
			}
			err := sg.setTransactionSnapshot(context.Background(), tx, "abc")
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, tt.execErr)
			require.Equal(t, tt.wantExplained, errors.Is(err, pglib.ErrLoadBalancedSource))
		})
	}
}
