// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/otel"
)

func TestNewServer_DefaultAddress(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, nil)
	require.Equal(t, DefaultAddress, s.address)
}

func TestNewServer_CustomAddress(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{Address: "127.0.0.1:0"}, nil)
	require.Equal(t, "127.0.0.1:0", s.address)
}

func TestHandleHealth(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, nil, WithVersion("v1.2.3"))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	s.handleHealth(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	body, _ := io.ReadAll(rec.Body)
	var got map[string]string
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "ok", got["status"])
	require.Equal(t, "v1.2.3", got["version"])
}

func TestHandleReady_NoCheck(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, nil, WithVersion("v1.2.3"))

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	s.handleReady(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	var got map[string]string
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "ok", got["status"])
}

func TestHandleReady_CheckPasses(t *testing.T) {
	t.Parallel()
	var called bool
	s := NewServer(Config{}, nil, WithReadinessCheck(func(_ context.Context) error {
		called = true
		return nil
	}))

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	s.handleReady(rec, req)

	require.True(t, called)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleReady_CheckFails(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, nil, WithReadinessCheck(func(_ context.Context) error {
		return errors.New("source unreachable")
	}))

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	s.handleReady(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	body, _ := io.ReadAll(rec.Body)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "unready", got["status"])
	failures, ok := got["failures"].([]any)
	require.True(t, ok)
	require.Len(t, failures, 1)
	failure, ok := failures[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "postgres_source", failure["component"])
	require.Equal(t, "source unreachable", failure["error"])
}

func TestHandleStatus_NoProvider(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, nil, WithVersion("v1.2.3"))

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	s.handleStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	body, _ := io.ReadAll(rec.Body)
	var got map[string]string
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "ok", got["status"])
	require.Equal(t, "v1.2.3", got["version"])
	require.Equal(t, "", got["phase"])
}

func TestHandleStatus_WithProvider(t *testing.T) {
	t.Parallel()
	phase := "snapshot"
	s := NewServer(Config{}, nil, WithVersion("v1.2.3"), WithPhaseProvider(func() string {
		return phase
	}))

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	s.handleStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body, _ := io.ReadAll(rec.Body)
	var got map[string]string
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "snapshot", got["phase"])

	phase = "replication"
	rec = httptest.NewRecorder()
	s.handleStatus(rec, req)
	body, _ = io.ReadAll(rec.Body)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "replication", got["phase"])
}

func TestListenServeShutdown(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{Address: "127.0.0.1:0"}, nil)

	require.NoError(t, s.Listen())

	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve() }()

	require.NoError(t, s.Shutdown(context.Background()))
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}
}

func TestServe_BeforeListen(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{Address: "127.0.0.1:0"}, nil)
	require.ErrorIs(t, s.Serve(), errNotListening)
}

func TestShutdown_BeforeListen(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{Address: "127.0.0.1:0"}, nil)
	require.NoError(t, s.Shutdown(context.Background()))
}

func TestListen_BindError(t *testing.T) {
	t.Parallel()
	first := NewServer(Config{Address: "127.0.0.1:0"}, nil)
	require.NoError(t, first.Listen())
	defer first.Shutdown(context.Background())

	second := NewServer(Config{Address: first.listener.Addr().String()}, nil)
	require.Error(t, second.Listen())
}

func makeMetricsConfig() *otel.MetricsConfig {
	return &otel.MetricsConfig{
		Prometheus: &otel.PrometheusConfig{
			Enabled: true,
		},
	}
}

func TestHandlePrometheus(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, makeMetricsConfig())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", DefaultPrometheusEndpoint, nil)
	s.handlePrometheus(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlePrometheusDisabled(t *testing.T) {
	t.Parallel()
	s := NewServer(Config{}, nil)
	req := httptest.NewRequest("GET", DefaultPrometheusEndpoint, nil)
	rec := httptest.NewRecorder()
	s.handlePrometheus(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}
