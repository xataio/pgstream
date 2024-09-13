// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription"
)

type Store struct {
	conn   pglib.Querier
	logger loglib.Logger
}

type Option func(*Store)

const (
	subscriptionsTableName = "webhook_subscriptions"
	pgstreamSchema         = "pgstream"
)

func NewSubscriptionStore(ctx context.Context, url string, opts ...Option) (*Store, error) {
	pgpool, err := pglib.NewConnPool(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create postgres connection pool: %w", err)
	}
	ss := &Store{
		conn: pgpool,
	}

	for _, opt := range opts {
		opt(ss)
	}

	// create subscriptions table if it doesn't exist
	if err := ss.createTable(ctx); err != nil {
		return nil, fmt.Errorf("creating subscriptions table: %w", err)
	}

	return ss, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(ss *Store) {
		ss.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "webhook_subscription_store",
		})
	}
}

func (s *Store) CreateSubscription(ctx context.Context, subscription *subscription.Subscription) error {
	query := fmt.Sprintf(`
	INSERT INTO %s(url, schema_name, table_name, event_types) VALUES($1, $2, $3, $4)
	ON CONFLICT (url,schema_name,table_name) DO UPDATE SET event_types = EXCLUDED.event_types;`, subscriptionsTable())
	_, err := s.conn.Exec(ctx, query, subscription.URL, subscription.Schema, subscription.Table, subscription.EventTypes)
	return err
}

func (s *Store) DeleteSubscription(ctx context.Context, subscription *subscription.Subscription) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE url=$1 AND schema_name=$2 AND table=$3;`, subscriptionsTable())
	_, err := s.conn.Exec(ctx, query, subscription.URL, subscription.Schema, subscription.Table)
	return err
}

func (s *Store) GetSubscriptions(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error) {
	query, params := s.buildGetQuery(action, schema, table)
	s.logger.Trace("getting subscriptions", loglib.Fields{
		"query":  query,
		"params": params,
	})
	rows, err := s.conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("querying subscriptions table: %w", err)
	}
	defer rows.Close()

	subscriptions := []*subscription.Subscription{}
	for rows.Next() {
		subscription := &subscription.Subscription{}
		if err := rows.Scan(&subscription.URL, &subscription.Schema, &subscription.Table, &subscription.EventTypes); err != nil {
			return nil, fmt.Errorf("scanning subscription row: %w", err)
		}

		subscriptions = append(subscriptions, subscription)
	}

	return subscriptions, nil
}

func (s *Store) createTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
	url TEXT,
	schema_name TEXT,
	table_name TEXT,
	event_types TEXT[],
	PRIMARY KEY(url,schema_name,table_name))`, subscriptionsTable())
	_, err := s.conn.Exec(ctx, query)
	return err
}

func (s *Store) buildGetQuery(action, schema, table string) (string, []any) {
	query := fmt.Sprintf(`SELECT url, schema_name, table_name, event_types FROM %s`, subscriptionsTable())

	separator := func(params []any) string {
		if len(params) == 0 {
			return "WHERE"
		}
		return "AND"
	}
	var params []any
	if schema != "" {
		query = fmt.Sprintf("%s %s (schema_name=$%d OR schema_name='')", query, separator(params), len(params)+1)
		params = append(params, schema)
	}
	if table != "" {
		query = fmt.Sprintf("%s %s (table_name=$%d OR table_name='')", query, separator(params), len(params)+1)
		params = append(params, table)
	}
	if action != "" {
		query = fmt.Sprintf("%s %s ($%d=ANY(event_types) OR event_types IS NULL)", query, separator(params), len(params)+1)
		params = append(params, action)
	}

	return fmt.Sprintf("%s LIMIT 1000", query), params
}

func subscriptionsTable() string {
	return fmt.Sprintf("%s.%s", pgstreamSchema, subscriptionsTableName)
}
