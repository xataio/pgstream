// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A caller tells a query failure from a batch failure with errors.As, so the
// wrapper has to carry the index and let the cause through.
func TestBatchQueryError(t *testing.T) {
	t.Parallel()

	cause := errors.New("duplicate key value violates unique constraint")
	err := error(&BatchQueryError{Index: 2, Err: cause})

	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), "batch query 2")
	require.Contains(t, err.Error(), cause.Error())

	var queryErr *BatchQueryError
	require.True(t, errors.As(err, &queryErr))
	require.Equal(t, 2, queryErr.Index)

	// a batch failure carries no index, so the same check must not match it
	require.False(t, errors.As(errors.New("conn closed"), &queryErr))
}
