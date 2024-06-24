// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"net/http"
)

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Server interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}
