// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

type Store struct {
	CreateSubscriptionFn func(ctx context.Context, s *subscription.Subscription) error
	DeleteSubscriptionFn func(ctx context.Context, s *subscription.Subscription) error
	GetSubscriptionsFn   func(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error)
}

func (m *Store) CreateSubscription(ctx context.Context, s *subscription.Subscription) error {
	return m.CreateSubscriptionFn(ctx, s)
}

func (m *Store) DeleteSubscription(ctx context.Context, s *subscription.Subscription) error {
	return m.DeleteSubscriptionFn(ctx, s)
}

func (m *Store) GetSubscriptions(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
	return m.GetSubscriptionsFn(ctx, action, schema, table)
}
