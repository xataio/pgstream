// SPDX-License-Identifier: Apache-2.0

package elasticsearch

import (
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
)

func TestWithTLS_NilLeavesTransportUntouched(t *testing.T) {
	t.Parallel()

	cfg := &elasticsearch.Config{Transport: http.DefaultTransport}
	WithTLS(nil)(cfg)
	require.Equal(t, http.DefaultTransport, cfg.Transport)
}

func TestWithTLS_InstallsTLSConfig(t *testing.T) {
	t.Parallel()

	tlsCfg := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} //nolint:gosec
	cfg := &elasticsearch.Config{Transport: http.DefaultTransport}
	WithTLS(tlsCfg)(cfg)

	transport, ok := cfg.Transport.(*http.Transport)
	require.True(t, ok, "expected *http.Transport, got %T", cfg.Transport)
	require.NotNil(t, transport.TLSClientConfig)
	require.True(t, transport.TLSClientConfig.InsecureSkipVerify)
}

func TestNewClient_NoAddressErrors(t *testing.T) {
	t.Parallel()

	_, err := NewClient("")
	require.Error(t, err)
}
