// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"errors"

	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

var errTest = errors.New("oh noes")

func newTestSubscription(url, schema, table string, eventTypes []string) *subscription.Subscription {
	return &subscription.Subscription{
		URL:        url,
		Schema:     schema,
		Table:      table,
		EventTypes: eventTypes,
	}
}
