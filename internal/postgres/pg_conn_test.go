// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestWithDialFunc_DialsTheResolvedAddress pins that the two options together
// decide which address is reached: the lookup answers from memory and the dial
// receives that answer, so no name is resolved twice.
func TestWithDialFunc_DialsTheResolvedAddress(t *testing.T) {
	t.Parallel()

	addresses := []string{}
	lookup := func(_ context.Context, host string) ([]string, error) {
		require.Equal(t, "pgstream.example", host)
		return []string{"192.0.2.1"}, nil
	}
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		addresses = append(addresses, address)
		return nil, errRefused
	}

	_, err := NewConn(context.Background(), testConnURL, WithLookupFunc(lookup), WithDialFunc(dial))
	require.ErrorIs(t, err, errRefused)
	require.NotEmpty(t, addresses)
	for _, address := range addresses {
		require.Equal(t, "192.0.2.1:5432", address)
	}
}

func TestWithDialFunc_NilLeavesTheDiallerUnchanged(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig(testConnURL)
	require.NoError(t, err)
	configureTCPKeepalive(cfg)

	pgstreamDialler := cfg.DialFunc
	require.NotNil(t, pgstreamDialler)

	WithDialFunc(nil)(cfg)
	require.NotNil(t, cfg.DialFunc)

	lookup := cfg.LookupFunc
	WithLookupFunc(nil)(cfg)
	require.Equal(t, lookup == nil, cfg.LookupFunc == nil)
}

// TestWithDialFunc_AppliesKeepalive pins that a caller-supplied dialler does
// not cost the connection pgstream's keepalive settings: a broken connection
// is still detected on the same timescale as one pgstream dialled itself.
func TestWithDialFunc_AppliesKeepalive(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
		}
	}()

	dialled := 0
	cfg, err := ParseConfig(testConnURL)
	require.NoError(t, err)
	WithDialFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
		dialled++
		return net.Dial(network, listener.Addr().String())
	})(cfg)

	conn, err := cfg.DialFunc(context.Background(), "tcp", "192.0.2.1:5432")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.Equal(t, 1, dialled)
	require.IsType(t, &net.TCPConn{}, conn, "the caller's connection must be returned as it is")
}

func TestApplyTCPKeepalive(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
		}
	}()

	tcpConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { tcpConn.Close() })
	require.NoError(t, applyTCPKeepalive(tcpConn))

	// a connection that is not TCP carries no keepalive settings
	pipe, _ := net.Pipe()
	t.Cleanup(func() { pipe.Close() })
	require.NoError(t, applyTCPKeepalive(pipe))
}

// TestWithLookupFunc_ResolvesEveryTarget pins that the resolver applies to
// every connection target the driver derives from the URL, fallbacks included.
func TestWithLookupFunc_ResolvesEveryTarget(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig(testConnURL)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.Fallbacks, "the default sslmode must produce a fallback target")

	lookups := 0
	WithLookupFunc(func(context.Context, string) ([]string, error) {
		lookups++
		return []string{"192.0.2.1"}, nil
	})(cfg)
	WithDialFunc(func(context.Context, string, string) (net.Conn, error) {
		return nil, errRefused
	})(cfg)

	_, err = pgx.ConnectConfig(context.Background(), cfg)
	require.Error(t, err)
	require.GreaterOrEqual(t, lookups, len(cfg.Fallbacks)+1,
		"every target must resolve through the supplied resolver")
}
