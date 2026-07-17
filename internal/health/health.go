// SPDX-License-Identifier: Apache-2.0

// Package health exposes a small HTTP server with /health (liveness),
// /ready (readiness), and /status (pipeline phase) endpoints so that
// operators and orchestrators can monitor a running pgstream process.
package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/xataio/pgstream/internal/json"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
)

const (
	DefaultAddress            = "localhost:9910"
	DefaultPrometheusEndpoint = "/metrics"

	readinessCheckTimeout = 2 * time.Second
	readHeaderTimeout     = 5 * time.Second
)

// Config controls how the health server is exposed.
type Config struct {
	Enabled bool
	Address string
}

// Server serves /health, /ready, and /status over HTTP.
type Server struct {
	logger         loglib.Logger
	address        string
	version        string
	readinessCheck func(ctx context.Context) error
	phaseProvider  func() string
	listener       net.Listener
	httpServer     *http.Server
	metricsConfig  *otel.MetricsConfig
}

var errNotListening = errors.New("health server not initialised: call Listen first")

type Option func(*Server)

// NewServer builds a health server from the given config. The server is not
// started until Start() is called.
func NewServer(cfg Config, metricsCfg *otel.MetricsConfig, opts ...Option) *Server {
	address := cfg.Address
	if address == "" {
		address = DefaultAddress
	}
	s := &Server{
		logger:        loglib.NewNoopLogger(),
		address:       address,
		metricsConfig: metricsCfg,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithLogger(l loglib.Logger) Option {
	return func(s *Server) {
		s.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "health_server",
		})
	}
}

func WithVersion(v string) Option {
	return func(s *Server) {
		s.version = v
	}
}

// WithReadinessCheck registers a function the /ready handler will invoke. The
// function is given a context with a short timeout; returning a non-nil error
// causes /ready to respond with 503. If no check is registered, /ready
// behaves like /health.
func WithReadinessCheck(fn func(ctx context.Context) error) Option {
	return func(s *Server) {
		s.readinessCheck = fn
	}
}

// WithPhaseProvider registers a function the /status handler will invoke to
// report the current pipeline phase (e.g. "snapshot" or "replication").
// If no provider is registered, /status still returns status/version with an
// empty phase.
func WithPhaseProvider(fn func() string) Option {
	return func(s *Server) {
		s.phaseProvider = fn
	}
}

// Listen binds the configured address synchronously and prepares the HTTP
// server. After it returns successfully, Serve must be called (typically
// from a goroutine) to start accepting requests. Splitting bind from serve
// lets callers surface bind errors synchronously and ensures Shutdown
// always observes an initialised server.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/status", s.handleStatus)

	if s.metricsConfig != nil && s.metricsConfig.Prometheus != nil && s.metricsConfig.Prometheus.Enabled {
		endpoint := DefaultPrometheusEndpoint
		if promEndpoint := s.metricsConfig.Prometheus.Endpoint; promEndpoint != "" {
			endpoint = promEndpoint
		}
		mux.Handle(endpoint, promhttp.Handler())
	}

	s.listener = ln
	s.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	return nil
}

// Serve starts serving requests on the listener bound by Listen. It blocks
// until Shutdown is called. ErrServerClosed is treated as a clean exit.
func (s *Server) Serve() error {
	if s.httpServer == nil || s.listener == nil {
		return errNotListening
	}
	s.logger.Info("starting health server", loglib.Fields{"address": s.address})
	if err := s.httpServer.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the underlying HTTP server gracefully. Safe to call when
// the server was never started.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, s.okPayload())
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, s.statusPayload())
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.readinessCheck != nil {
		ctx, cancel := context.WithTimeout(r.Context(), readinessCheckTimeout)
		defer cancel()
		if err := s.readinessCheck(ctx); err != nil {
			s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "unready",
				"failures": []map[string]string{{
					"component": "postgres_source",
					"error":     err.Error(),
				}},
			})
			return
		}
	}
	s.writeJSON(w, http.StatusOK, s.okPayload())
}

func (s *Server) okPayload() map[string]string {
	payload := map[string]string{"status": "ok"}
	if s.version != "" {
		payload["version"] = s.version
	}
	return payload
}

func (s *Server) statusPayload() map[string]string {
	payload := s.okPayload()
	phase := ""
	if s.phaseProvider != nil {
		phase = s.phaseProvider()
	}
	payload["phase"] = phase
	return payload
}

func (s *Server) writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	payload, err := json.Marshal(body)
	if err != nil {
		s.logger.Warn(err, "health server: marshalling response")
		return
	}
	if _, err := w.Write(payload); err != nil {
		s.logger.Warn(err, "health server: writing response")
	}
}
