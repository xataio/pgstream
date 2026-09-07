// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const testConnURL = "postgres://user:pass@pgstream.example:5432/testdb"

var errRefused = errors.New("dialling refused by test")

// refusingHook makes every dial attempt fail without touching the network: the
// lookup answers from memory and the dialler refuses. It records the
// configurations it was given.
func refusingHook(seen *[]*pgx.ConnConfig) ConnOption {
	return func(cfg *pgx.ConnConfig) {
		*seen = append(*seen, cfg)
		cfg.LookupFunc = func(context.Context, string) ([]string, error) {
			return []string{"192.0.2.1"}, nil
		}
		cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			return nil, errRefused
		}
	}
}

func TestNewConn_ConnOption(t *testing.T) {
	t.Parallel()

	var seen []*pgx.ConnConfig
	conn, err := NewConn(context.Background(), testConnURL, refusingHook(&seen))
	require.Error(t, err)
	require.Nil(t, conn)
	require.Len(t, seen, 1)
	require.Equal(t, "pgstream.example", seen[0].Host)
	require.Equal(t, "testdb", seen[0].Database)
}

// TestNewConn_ConnOptionRunsAfterKeepalive pins the ordering the option
// contract promises: the hook sees the configuration pgstream has already
// applied, and its own dialler survives.
func TestNewConn_ConnOptionRunsAfterKeepalive(t *testing.T) {
	t.Parallel()

	var (
		keepaliveDialFunc bool
		connectTimeout    time.Duration
		dialled           int
	)
	hook := func(cfg *pgx.ConnConfig) {
		keepaliveDialFunc = cfg.DialFunc != nil
		connectTimeout = cfg.ConnectTimeout
		cfg.LookupFunc = func(context.Context, string) ([]string, error) {
			return []string{"192.0.2.1"}, nil
		}
		cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			dialled++
			return nil, errRefused
		}
	}

	_, err := NewConn(context.Background(), testConnURL, hook)
	require.Error(t, err)
	require.True(t, keepaliveDialFunc, "hook must run after TCP keepalive configuration")
	require.Equal(t, 90*time.Second, connectTimeout)
	require.Positive(t, dialled, "the hook's dialler must be the one used")
}

func TestNewConn_ConnOptionOrder(t *testing.T) {
	t.Parallel()

	order := []string{}
	first := func(*pgx.ConnConfig) { order = append(order, "first") }
	second := func(cfg *pgx.ConnConfig) {
		order = append(order, "second")
		cfg.LookupFunc = func(context.Context, string) ([]string, error) {
			return []string{"192.0.2.1"}, nil
		}
		cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			return nil, errRefused
		}
	}

	_, err := NewConn(context.Background(), testConnURL, first, second)
	require.Error(t, err)
	require.Equal(t, []string{"first", "second"}, order)
}

func TestNewConn_InvalidURLDoesNotRunOptions(t *testing.T) {
	t.Parallel()

	called := 0
	_, err := NewConn(context.Background(), "not-a-postgres-url", func(*pgx.ConnConfig) { called++ })
	require.Error(t, err)
	require.Equal(t, 0, called)
}

// TestLazyConn_OptionsAppliedOnFirstDial pins that the options are stored at
// construction and applied when the connection is finally opened, once.
func TestLazyConn_OptionsAppliedOnFirstDial(t *testing.T) {
	t.Parallel()

	var seen []*pgx.ConnConfig
	lazy := NewLazyConn(testConnURL, refusingHook(&seen))
	require.Empty(t, seen, "construction must not configure a connection")

	_, err := lazy.Acquire(context.Background())
	require.Error(t, err)
	require.Len(t, seen, 1)

	// the dial error is memoised, so no second connection is configured
	_, err = lazy.Acquire(context.Background())
	require.Error(t, err)
	require.Len(t, seen, 1)

	require.NoError(t, lazy.Close(context.Background()))
}

func TestLazyConn_NoOptions(t *testing.T) {
	t.Parallel()

	lazy := NewLazyConn(testConnURL)
	require.Empty(t, lazy.opts)
}

// TestLazyConn_AcquireIsSafeForConcurrentUse covers the preflight engine
// bounding a check with a per-check timeout: the abandoned check keeps using
// the shared conn while the next check acquires it.
func TestLazyConn_AcquireIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	// an unparseable URL fails before anything is dialled, so the test needs
	// no database to exercise the memoisation the callers race on
	lazy := NewLazyConn("://not-a-postgres-url")

	const callers = 8
	errs := make([]error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := range callers {
		go func() {
			defer wg.Done()
			_, errs[i] = lazy.Acquire(context.Background())
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err, "caller %d", i)
		require.Equal(t, errs[0].Error(), err.Error(), "every caller sees the memoised dial error")
	}
	require.NoError(t, lazy.Close(context.Background()))
}
