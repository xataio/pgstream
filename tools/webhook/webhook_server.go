// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/xataio/pgstream/internal/log/zerolog"
	loglib "github.com/xataio/pgstream/pkg/log"
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

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var prettyJSON bytes.Buffer
	if err = json.Indent(&prettyJSON, bodyBytes, "", "    "); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logger.Info(prettyJSON.String())

	w.WriteHeader(http.StatusOK)
}
