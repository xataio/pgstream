// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/mocks"
)

func TestSubscriptionServer_subscribe(t *testing.T) {
	t.Parallel()

	testSubscription := &webhook.Subscription{
		URL:        "url-1",
		Schema:     "test_schema",
		Table:      "test_table",
		EventTypes: []string{"I"},
	}
	subscriptionBytes, err := json.Marshal(testSubscription)
	require.NoError(t, err)

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		store   webhook.SubscriptionStore
		method  string
		payload io.Reader

		wantStatusCode int
	}{
		{
			name: "ok",
			store: &mocks.SubscriptionStore{
				CreateSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					require.Equal(t, testSubscription, s)
					return nil
				},
			},
			payload:        bytes.NewBuffer(subscriptionBytes),
			method:         http.MethodPost,
			wantStatusCode: http.StatusCreated,
		},
		{
			name: "error - creating subscription",
			store: &mocks.SubscriptionStore{
				CreateSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					return errTest
				},
			},
			payload:        bytes.NewBuffer(subscriptionBytes),
			method:         http.MethodPost,
			wantStatusCode: http.StatusServiceUnavailable,
		},
		{
			name: "error - method not allowed",
			store: &mocks.SubscriptionStore{
				CreateSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					return errors.New("CreateSubscriptionFn: should not be called")
				},
			},
			payload:        bytes.NewBuffer(subscriptionBytes),
			method:         http.MethodGet,
			wantStatusCode: http.StatusMethodNotAllowed,
		},
		{
			name: "error - invalid payload",
			store: &mocks.SubscriptionStore{
				CreateSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					return errors.New("CreateSubscriptionFn: should not be called")
				},
			},
			payload:        bytes.NewBufferString("not a subscription"),
			method:         http.MethodPost,
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := &SubscriptionServer{
				logger: log.NewNoopLogger(),
				store:  tc.store,
			}

			req := httptest.NewRequest(tc.method, "/subscribe", tc.payload)
			req.Header.Add(echo.HeaderContentType, echo.MIMEApplicationJSON)
			w := httptest.NewRecorder()
			echoCtx := echo.New().NewContext(req, w)

			server.subscribe(echoCtx)
			require.Equal(t, tc.wantStatusCode, w.Result().StatusCode)
		})
	}
}

func TestSubscriptionServer_unsubscribe(t *testing.T) {
	t.Parallel()

	testSubscription := &webhook.Subscription{
		URL:        "url-1",
		Schema:     "test_schema",
		Table:      "test_table",
		EventTypes: []string{"I"},
	}
	subscriptionBytes, err := json.Marshal(testSubscription)
	require.NoError(t, err)

	errTest := errors.New("oh noes")

	tests := []struct {
		name    string
		store   webhook.SubscriptionStore
		method  string
		payload io.Reader

		wantStatusCode int
	}{
		{
			name: "ok",
			store: &mocks.SubscriptionStore{
				DeleteSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					require.Equal(t, testSubscription, s)
					return nil
				},
			},
			payload:        bytes.NewBuffer(subscriptionBytes),
			method:         http.MethodPost,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "error - creating subscription",
			store: &mocks.SubscriptionStore{
				DeleteSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					return errTest
				},
			},
			payload:        bytes.NewBuffer(subscriptionBytes),
			method:         http.MethodPost,
			wantStatusCode: http.StatusServiceUnavailable,
		},
		{
			name: "error - method not allowed",
			store: &mocks.SubscriptionStore{
				DeleteSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					return errors.New("DeleteSubscriptionFn: should not be called")
				},
			},
			payload:        bytes.NewBuffer(subscriptionBytes),
			method:         http.MethodGet,
			wantStatusCode: http.StatusMethodNotAllowed,
		},
		{
			name: "error - invalid payload",
			store: &mocks.SubscriptionStore{
				DeleteSubscriptionFn: func(ctx context.Context, s *webhook.Subscription) error {
					return errors.New("DeleteSubscriptionFn: should not be called")
				},
			},
			payload:        bytes.NewBufferString("not a subscription"),
			method:         http.MethodPost,
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := &SubscriptionServer{
				logger: log.NewNoopLogger(),
				store:  tc.store,
			}

			req := httptest.NewRequest(tc.method, "/unsubscribe", tc.payload)
			req.Header.Add(echo.HeaderContentType, echo.MIMEApplicationJSON)
			w := httptest.NewRecorder()
			echoCtx := echo.New().NewContext(req, w)

			server.unsubscribe(echoCtx)
			require.Equal(t, tc.wantStatusCode, w.Result().StatusCode)
		})
	}
}