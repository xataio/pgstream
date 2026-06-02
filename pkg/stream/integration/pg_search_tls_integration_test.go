// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/internal/searchstore/elasticsearch"
	"github.com/xataio/pgstream/internal/searchstore/opensearch"
	pgtls "github.com/xataio/pgstream/pkg/tls"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
)

// Test_Search_TLSInsecureSkipVerify verifies that the TLS plumbing introduced
// for #798 lets pgstream talk to a search backend over HTTPS with a
// self-signed certificate when InsecureSkipVerify is set — and that without
// it, the TLS handshake fails with the expected x509 error.
//
// Uses httptest.NewTLSServer (Go's stdlib generates a self-signed cert) as a
// stand-in for an OpenSearch 3 cluster behind self-signed HTTPS. The test
// asserts on the request that reaches the server, which is enough to prove
// the HTTPS connection was established.
func Test_Search_TLSInsecureSkipVerify(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	ctx := context.Background()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// minimal OpenSearch-shaped responses; only the status code and
		// the body shape matter for the calls we exercise below.
		switch {
		case r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/_cluster/health"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"test","cluster_name":"test"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	t.Run("opensearch insecure_skip_verify=true succeeds", func(t *testing.T) {
		t.Parallel()
		client, err := opensearch.NewClient(srv.URL, opensearch.WithTLS(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // opt-in for the test
			MinVersion:         tls.VersionTLS12,
		}))
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		require.NoError(t, err)

		resp, err := client.Perform(req)
		require.NoError(t, err, "TLS handshake should succeed with InsecureSkipVerify=true")
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("opensearch without TLS option errors on x509", func(t *testing.T) {
		t.Parallel()
		client, err := opensearch.NewClient(srv.URL) // no WithTLS
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		require.NoError(t, err)

		_, err = client.Perform(req)
		require.Error(t, err, "expected TLS verification to fail without InsecureSkipVerify")
		require.Contains(t, err.Error(), "x509",
			"expected an x509 / certificate verification error, got: %v", err)
	})

	t.Run("elasticsearch insecure_skip_verify=true succeeds", func(t *testing.T) {
		t.Parallel()
		client, err := elasticsearch.NewClient(srv.URL, elasticsearch.WithTLS(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // opt-in for the test
			MinVersion:         tls.VersionTLS12,
		}))
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
		require.NoError(t, err)

		resp, err := client.Perform(req)
		require.NoError(t, err, "TLS handshake should succeed with InsecureSkipVerify=true")
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	// End-to-end through store.NewStore — the path pgstream's config
	// surface (YAML/env) actually traverses to build a search client.
	t.Run("store.NewStore wires TLS through to the underlying client", func(t *testing.T) {
		t.Parallel()
		_, err := store.NewStore(store.Config{
			OpenSearchURL: srv.URL,
			TLS: pgtls.Config{
				Enabled:            true,
				InsecureSkipVerify: true,
			},
		})
		require.NoError(t, err, "NewStore should succeed against the self-signed HTTPS server")
	})
}
