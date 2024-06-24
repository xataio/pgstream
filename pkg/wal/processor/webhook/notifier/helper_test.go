// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"errors"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
)

var testCommitPos = wal.CommitPosition("test-pos")

var errTest = errors.New("oh noes")

func newTestSubscription(url, schema, table string, eventTypes []string) *webhook.Subscription {
	return &webhook.Subscription{
		URL:        url,
		Schema:     schema,
		Table:      table,
		EventTypes: eventTypes,
	}
}

func testNotifyMsg(urls []string, payload []byte) *notifyMsg {
	return &notifyMsg{
		urls:           urls,
		payload:        payload,
		commitPosition: testCommitPos,
	}
}
