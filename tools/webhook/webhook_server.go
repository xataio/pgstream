// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/xataio/pgstream/internal/log/zerolog"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
)

var logger loglib.Logger

func main() {
	address := flag.String("address", ":9910", "Webhook server address")
	logLevel := flag.String("log-level", "debug", "Webhook server log level")
	flag.Parse()

	logger = zerolog.NewStdLogger(zerolog.NewLogger(&zerolog.Config{
		LogLevel: *logLevel,
	}))

	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", processWebhook)

	server := &http.Server{
		Handler:      mux,
		Addr:         *address,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	logger.Info(fmt.Sprintf("listening on %s...", *address))
	if err := server.ListenAndServe(); err != nil {
		logger.Error(err, "listening on http server", loglib.Fields{"address": *address})
		os.Exit(1)
	}
}

func processWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	logger.Debug("got /webhook request")

	payload := webhook.Payload{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if payload.Data != nil {
		logger.Debug("webhook request payload received", loglib.Fields{
			"event_action": payload.Data.Action,
			"event_schema": payload.Data.Schema,
			"event_table":  payload.Data.Table,
		})
	}
	w.WriteHeader(http.StatusOK)
}
