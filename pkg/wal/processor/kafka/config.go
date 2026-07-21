// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"fmt"

	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
)

type Config struct {
	Kafka kafka.ConnConfig
	Batch batch.Config
	// PartitionKey determines the message key strategy used for DML events,
	// and therefore how they are distributed across the topic partitions. One
	// of "schema" (default), "table" or "primary_key".
	PartitionKey PartitionKey
}

// PartitionKey represents the message key strategy used for DML events.
type PartitionKey string

const (
	// PartitionKeySchema keys messages by schema name. All events for a given
	// schema are routed to the same partition, which guarantees ordering per
	// schema. This is the default.
	PartitionKeySchema PartitionKey = "schema"
	// PartitionKeyTable keys messages by schema qualified table name. Events
	// for a given table are routed to the same partition, which guarantees
	// ordering per table, but not across tables in the same schema.
	PartitionKeyTable PartitionKey = "table"
	// PartitionKeyPrimaryKey keys messages by schema qualified table name and
	// the event primary key values. Events for a given row are routed to the
	// same partition, which guarantees ordering per row, but not across rows
	// or against DDL events. Events without an identifiable primary key
	// degrade to table keying.
	PartitionKeyPrimaryKey PartitionKey = "primary_key"
)

func (c *Config) partitionKey() (PartitionKey, error) {
	switch c.PartitionKey {
	case "":
		return PartitionKeySchema, nil
	case PartitionKeySchema, PartitionKeyTable, PartitionKeyPrimaryKey:
		return c.PartitionKey, nil
	default:
		return "", fmt.Errorf("invalid kafka partition key %q, must be one of %q, %q, %q",
			c.PartitionKey, PartitionKeySchema, PartitionKeyTable, PartitionKeyPrimaryKey)
	}
}
