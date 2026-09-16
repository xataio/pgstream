// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// preprocessingError stands in for pgx.ErrPreprocessingBatch, whose fields are
// not exported. What matters to the code under test is the shape: an error
// that wraps a cause and names the statement it belongs to.
type preprocessingError struct {
	sql   string
	cause error
}

func (e preprocessingError) Error() string {
	return fmt.Sprintf("error preprocessing batch: %v", e.cause)
}
func (e preprocessingError) Unwrap() error { return e.cause }
func (e preprocessingError) SQL() string   { return e.sql }

func newPreprocessingError(sql string, cause error) error {
	return preprocessingError{sql: sql, cause: cause}
}

// The real type satisfies the same shape. This assertion is the one that
// matters: batchError reads the SQL through an interface, so a pgx release
// that stops reporting it would turn that branch into dead code and leave
// every preprocessing failure to the isolation pass, with nothing failing.
var (
	_ interface{ SQL() string } = preprocessingError{}
	_ interface{ SQL() string } = pgx.ErrPreprocessingBatch{}
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

// pgx prepares and encodes the whole batch before it executes any of it, so a
// failure of either step surfaces on the first read, whichever query owns it.
// Reading that as "query 0 failed" drops a query that did nothing wrong and
// lets the real one run again, which is the loss BatchQueryError exists to
// prevent.
func TestBatchError(t *testing.T) {
	t.Parallel()

	queries := []BatchQuery{
		{SQL: "INSERT INTO a VALUES ($1)"},
		{SQL: "INSERT INTO b VALUES ($1)"},
		{SQL: "INSERT INTO c VALUES ($1)"},
	}
	serverErr := &pgconn.PgError{Message: "relation does not exist", Code: "42P01"}

	t.Run("a preprocessing failure names the query that owns the SQL", func(t *testing.T) {
		t.Parallel()
		// The read that failed is the first one, and the statement at fault is
		// the second.
		err := batchError(queries, 0, newPreprocessingError("INSERT INTO b VALUES ($1)", serverErr))

		var queryErr *BatchQueryError
		require.True(t, errors.As(err, &queryErr))
		require.Equal(t, 1, queryErr.Index)
	})

	t.Run("a preprocessing failure of a repeated SQL stays with the batch", func(t *testing.T) {
		t.Parallel()
		repeated := []BatchQuery{
			{SQL: "INSERT INTO a VALUES ($1)"},
			{SQL: "INSERT INTO a VALUES ($1)"},
		}
		err := batchError(repeated, 0, newPreprocessingError("INSERT INTO a VALUES ($1)", serverErr))

		var queryErr *BatchQueryError
		require.False(t, errors.As(err, &queryErr), "an ambiguous match must not name a query")
	})

	t.Run("a server error for the query just read names that query", func(t *testing.T) {
		t.Parallel()
		err := batchError(queries, 2, serverErr)

		var queryErr *BatchQueryError
		require.True(t, errors.As(err, &queryErr))
		require.Equal(t, 2, queryErr.Index)
	})

	t.Run("a failure that is not the server stays with the batch", func(t *testing.T) {
		t.Parallel()
		err := batchError(queries, 1, errors.New("conn closed"))

		var queryErr *BatchQueryError
		require.False(t, errors.As(err, &queryErr))
	})

	t.Run("an index outside the batch names no query", func(t *testing.T) {
		t.Parallel()
		err := batchError(queries, len(queries), serverErr)

		var queryErr *BatchQueryError
		require.False(t, errors.As(err, &queryErr))
	})
}
