// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal/processor/webhook"
)

type SubscriptionStore struct {
	CreateSubscriptionFn func(ctx context.Context, s *webhook.Subscription) error
	DeleteSubscriptionFn func(ctx context.Context, s *webhook.Subscription) error
	GetSubscriptionsFn   func(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error)
}

func (m *SubscriptionStore) CreateSubscription(ctx context.Context, s *webhook.Subscription) error {
	return m.CreateSubscriptionFn(ctx, s)
}

func (m *SubscriptionStore) DeleteSubscription(ctx context.Context, s *webhook.Subscription) error {
	return m.DeleteSubscriptionFn(ctx, s)
}

func (m *SubscriptionStore) GetSubscriptions(ctx context.Context, action, schema, table string) ([]*webhook.Subscription, error) {
	return m.GetSubscriptionsFn(ctx, action, schema, table)
}
