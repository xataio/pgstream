// SPDX-License-Identifier: Apache-2.0

package sync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopyBudgetSize(t *testing.T) {
	t.Parallel()

	// zero would block every copy forever
	require.Equal(t, int64(1), CopyBudgetSize(1))
	require.Equal(t, int64(1), CopyBudgetSize(CopyBudgetReserve))
	require.Equal(t, int64(45), CopyBudgetSize(50))
}
