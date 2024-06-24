// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	httplib "github.com/xataio/pgstream/internal/http"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
)

type SubscriptionServer struct {
	server  httplib.Server
	logger  loglib.Logger
	store   webhook.SubscriptionStore
	address string
}

type Option func(*SubscriptionServer)

func New(cfg *Config, store webhook.SubscriptionStore, opts ...Option) *SubscriptionServer {
	s := &SubscriptionServer{
		address: cfg.address(),
		store:   store,
		logger:  loglib.NewNoopLogger(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/webhooks/subscribe", s.subscribe)
	mux.HandleFunc("/webhooks/unsubscribe", s.unsubscribe)

	s.server = &http.Server{
		Handler:      mux,
		Addr:         cfg.address(),
		ReadTimeout:  cfg.readTimeout(),
		WriteTimeout: cfg.writeTimeout(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func WithLogger(l loglib.Logger) Option {
	return func(s *SubscriptionServer) {
		s.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ServiceField: "webhook_subscription_server",
		})
	}
}

// Serve will start the subscription server. This call is blocking.
func (s *SubscriptionServer) Serve() error {
	s.logger.Info(fmt.Sprintf("subscription server listening on: %s...", s.address))
	return s.server.ListenAndServe()
}

func (s *SubscriptionServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *SubscriptionServer) subscribe(w http.ResponseWriter, r *http.Request) {
	s.logger.Trace("request received on /subscribe endpoint")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	subscription := &webhook.Subscription{}
	if err := json.NewDecoder(r.Body).Decode(subscription); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := s.store.CreateSubscription(ctx, subscription); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *SubscriptionServer) unsubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	subscription := &webhook.Subscription{}
	if err := json.NewDecoder(r.Body).Decode(subscription); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := s.store.DeleteSubscription(ctx, subscription); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}
