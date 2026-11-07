// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/backoff"
)

// testLookupRetryConfig keeps the retry budget of the production configuration
// while removing the waiting from it, so the tests exercise the same number of
// attempts in no time.
func testLookupRetryConfig() *backoff.ExponentialConfig {
	return &backoff.ExponentialConfig{
		InitialInterval: time.Microsecond,
		MaxInterval:     time.Microsecond,
		MaxRetries:      lookupRetryMaxRetries,
	}
}

func dnsTimeout(host string) error {
	return &net.DNSError{
		Err:       "read udp 10.1.0.11:5704->10.0.0.2:53: i/o timeout",
		Name:      host,
		IsTimeout: true,
	}
}

// TestRetryTransientLookupFailures_RetriesATransientFailure pins the reason the
// retry exists: a resolver that times out once must not fail the connection,
// since the database it names is healthy.
func TestRetryTransientLookupFailures_RetriesATransientFailure(t *testing.T) {
	t.Parallel()

	attempts := 0
	lookup := retryTransientLookupFailures(func(_ context.Context, host string) ([]string, error) {
		attempts++
		if attempts == 1 {
			return nil, dnsTimeout(host)
		}
		return []string{"192.0.2.1"}, nil
	}, testLookupRetryConfig())

	addrs, err := lookup(context.Background(), "pgstream.example")
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.1"}, addrs)
	require.Equal(t, 2, attempts)
}

func TestRetryTransientLookupFailures_RetriesATemporaryFailure(t *testing.T) {
	t.Parallel()

	attempts := 0
	lookup := retryTransientLookupFailures(func(_ context.Context, host string) ([]string, error) {
		attempts++
		if attempts == 1 {
			return nil, &net.DNSError{Err: "server misbehaving", Name: host, IsTemporary: true}
		}
		return []string{"192.0.2.1"}, nil
	}, testLookupRetryConfig())

	addrs, err := lookup(context.Background(), "pgstream.example")
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.1"}, addrs)
	require.Equal(t, 2, attempts)
}

// TestRetryTransientLookupFailures_DoesNotRetryAPermanentFailure pins that a
// misconfigured host name still fails at once: retrying it would only delay the
// error the operator needs to see.
func TestRetryTransientLookupFailures_DoesNotRetryAPermanentFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "name does not exist",
			err:  &net.DNSError{Err: "no such host", Name: "pgstream.example", IsNotFound: true},
		},
		{
			name: "not a resolver failure",
			err:  errors.New("resolver unavailable by test"),
		},
		{
			name: "context cancelled",
			err:  context.Canceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			attempts := 0
			lookup := retryTransientLookupFailures(func(context.Context, string) ([]string, error) {
				attempts++
				return nil, tc.err
			}, testLookupRetryConfig())

			_, err := lookup(context.Background(), "pgstream.example")
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, 1, attempts)
		})
	}
}

// TestRetryTransientLookupFailures_ExhaustsTheBudget pins that the retries are
// bounded and that the failure the caller sees is the resolver's own, so the
// log still names the host that could not be resolved.
func TestRetryTransientLookupFailures_ExhaustsTheBudget(t *testing.T) {
	t.Parallel()

	attempts := 0
	lookupErr := dnsTimeout("pgstream.example")
	lookup := retryTransientLookupFailures(func(context.Context, string) ([]string, error) {
		attempts++
		return nil, lookupErr
	}, testLookupRetryConfig())

	_, err := lookup(context.Background(), "pgstream.example")
	require.ErrorIs(t, err, lookupErr)
	require.Equal(t, int(lookupRetryMaxRetries)+1, attempts)
}

// TestRetryTransientLookupFailures_StopsOnContextCancellation pins that a
// cancelled run is not retried further: the caller is shutting down, so the
// resolver failure it already has is the answer.
func TestRetryTransientLookupFailures_StopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	lookupErr := dnsTimeout("pgstream.example")
	lookup := retryTransientLookupFailures(func(context.Context, string) ([]string, error) {
		attempts++
		cancel()
		return nil, lookupErr
	}, testLookupRetryConfig())

	_, err := lookup(ctx, "pgstream.example")
	require.ErrorIs(t, err, lookupErr)
	require.Equal(t, 1, attempts)
}

func TestRetryTransientLookupFailures_NilLookup(t *testing.T) {
	t.Parallel()

	require.Nil(t, retryTransientLookupFailures(nil, testLookupRetryConfig()))
}

// TestConfigureLookupRetries_EveryConnectionCarriesIt pins that each way
// pgstream opens a connection resolves through the retrying resolver, rather
// than through the driver's own.
func TestConfigureLookupRetries_EveryConnectionCarriesIt(t *testing.T) {
	t.Parallel()

	driverCfg, err := pgx.ParseConfig(testConnURL)
	require.NoError(t, err)
	driverLookup := reflect.ValueOf(driverCfg.LookupFunc).Pointer()

	connCfg, err := ParseConfig(testConnURL)
	require.NoError(t, err)
	configureLookupRetries(connCfg)
	require.NotEqual(t, driverLookup, reflect.ValueOf(connCfg.LookupFunc).Pointer())

	poolCfg, err := newConnPoolConfig(testConnURL)
	require.NoError(t, err)
	require.NotEqual(t, driverLookup, reflect.ValueOf(poolCfg.ConnConfig.LookupFunc).Pointer())

	replicationCfg, err := newReplicationConnConfig(testConnURL)
	require.NoError(t, err)
	require.NotEqual(t, driverLookup, reflect.ValueOf(replicationCfg.LookupFunc).Pointer())
}

// TestWithLookupFunc_RetriesATransientFailure pins that replacing the resolver
// does not cost the connection its resolution retries.
func TestWithLookupFunc_RetriesATransientFailure(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig(testConnURL)
	require.NoError(t, err)

	attempts := 0
	WithLookupFunc(func(_ context.Context, host string) ([]string, error) {
		attempts++
		if attempts == 1 {
			return nil, dnsTimeout(host)
		}
		return []string{"192.0.2.1"}, nil
	})(cfg)

	addrs, err := cfg.LookupFunc(context.Background(), "pgstream.example")
	require.NoError(t, err)
	require.Equal(t, []string{"192.0.2.1"}, addrs)
	require.Equal(t, 2, attempts)
}

// TestNewConn_RetriesATransientLookupFailure pins the behaviour end to end: the
// driver reaches the resolved address even though the first resolution attempt
// timed out, so the connection fails on the dial the test refuses rather than
// on the resolution.
func TestNewConn_RetriesATransientLookupFailure(t *testing.T) {
	t.Parallel()

	lookups := 0
	lookup := func(_ context.Context, host string) ([]string, error) {
		lookups++
		if lookups == 1 {
			return nil, dnsTimeout(host)
		}
		return []string{"192.0.2.1"}, nil
	}
	dial := func(context.Context, string, string) (net.Conn, error) {
		return nil, errRefused
	}

	_, err := NewConn(context.Background(), testConnURL, WithLookupFunc(lookup), WithDialFunc(dial))
	require.ErrorIs(t, err, errRefused)
	require.Greater(t, lookups, 1, "the resolution must be retried")
}
