// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"fmt"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

type notifyMsg struct {
	urls           []string
	payload        []byte
	lsn            string
	commitPosition wal.CommitPosition
}

type serialiser func(any) ([]byte, error)

func newNotifyMsg(event *wal.Event, subscriptions []*subscription.Subscription, serialiser serialiser) (*notifyMsg, error) {
	var payload []byte
	var lsn string
	urls := make([]string, 0, len(subscriptions))
	if len(subscriptions) > 0 {
		var err error
		payload, err = serialiser(&webhook.Payload{Data: event.Data})
		if err != nil {
			return nil, fmt.Errorf("serialising webhook payload: %w", err)
		}
		lsn = event.Data.LSN

		for _, s := range subscriptions {
			urls = append(urls, s.URL)
		}
	}

	return &notifyMsg{
		urls:           urls,
		payload:        payload,
		lsn:            lsn,
		commitPosition: event.CommitPosition,
	}, nil
}

func (m *notifyMsg) size() int {
	urlSize := 0
	for _, url := range m.urls {
		urlSize += len(url)
	}
	return len(m.payload) + urlSize
}
