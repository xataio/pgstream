// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

func Test_PostgresToWebhook(t *testing.T) {
	if os.Getenv("PGSTREAM_INTEGRATION_TESTS") == "" {
		t.Skip("skipping integration test...")
	}

	cfg := &stream.Config{
		Listener:  testPostgresListenerCfg(t),
		Processor: testWebhookProcessorCfg(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runStream(t, ctx, cfg)

	mockWebhookServer := newMockWebhookServer()
	defer mockWebhookServer.close()

	testTable := "pg2webhook_integration_test"
	// create a subscription to the test table with the mock server url
	createSubscription(t, mockWebhookServer.URL, "public", testTable)

	// The DDL event's contents are the source side's doing — the event trigger
	// and the injector build it before any sink is chosen — so its full shape
	// is asserted once, in Test_PostgresToKafka. What this test is for is the
	// chain that is webhook specific: the subscription server accepting a
	// subscription, the store resolving it for a live event, and the notifier
	// delivering there.
	tests := []struct {
		name  string
		query string

		wantData    *wal.Data
		wantContent bool
	}{
		{
			name:  "schema event",
			query: fmt.Sprintf("create table %s(id serial primary key, name text)", testTable),

			wantData: &wal.Data{
				Action: wal.LogicalMessageAction,
				Prefix: wal.DDLPrefix,
			},
			wantContent: true,
		},
		{
			name:  "data event",
			query: fmt.Sprintf("insert into %s(name) values('a')", testTable),

			wantData: &wal.Data{
				Action: "I",
				Schema: "public",
				Table:  testTable,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execQuery(t, ctx, tc.query)

			timer := time.NewTimer(20 * time.Second)
			defer timer.Stop()
			for {
				select {
				case <-timer.C:
					cancel()
					t.Error("timeout waiting for webhook payload")
					return
				case data := <-mockWebhookServer.dataChan:
					require.NotNil(t, data)
					require.Equal(t, tc.wantData.Action, data.Action)
					require.Equal(t, tc.wantData.Schema, data.Schema)
					require.Equal(t, tc.wantData.Table, data.Table)
					require.Equal(t, tc.wantData.Prefix, data.Prefix)
					if tc.wantContent {
						// the payload survived the round trip as a DDL event
						// for the table under test
						ddlEvent := &wal.DDLEvent{}
						require.NoError(t, json.Unmarshal([]byte(data.Content), ddlEvent))
						require.Equal(t, "CREATE TABLE", ddlEvent.CommandTag)
						require.Contains(t, ddlEvent.DDL, testTable)
					}
					return
				}
			}
		})
	}
}

func createSubscription(t *testing.T, url, schema, table string) {
	subscription := subscription.Subscription{
		URL:    url,
		Schema: schema,
		Table:  table,
	}
	subscriptionBytes, err := json.Marshal(subscription)
	require.NoError(t, err)

	req := func() *http.Request {
		req, err := http.NewRequest(http.MethodPost, "http://localhost:9900/webhooks/subscribe", bytes.NewBuffer(subscriptionBytes))
		require.NoError(t, err)
		req.Header.Add("Content-Type", "application/json")
		return req
	}

	var sendErr error
	var sendResp *http.Response
	// retry the request sending up to 5 times or error out. This gives time to
	// the subscription server to be available
	for i := 0; i < 5; i++ {
		sendResp, sendErr = http.DefaultClient.Do(req())
		if sendErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, sendErr)
	require.Equal(t, http.StatusCreated, sendResp.StatusCode)
}
