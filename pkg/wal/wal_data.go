// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"fmt"

	"github.com/rs/xid"
	"github.com/xataio/pgstream/internal/kafka"
	"github.com/xataio/pgstream/internal/replication"
)

// Event represents the WAL information. If the data is not set but there's a
// commit position present, it represents a keep alive event that needs to be
// checkpointed.
type Event struct {
	Data           *Data
	CommitPosition CommitPosition
}

type Data struct {
	Action    string   `json:"action"`    // "I" -- insert, "U" -- update, "D" -- delete, "T" -- truncate
	Timestamp string   `json:"timestamp"` // ISO8601, i.e. 2019-12-29 04:58:34.806671
	LSN       string   `json:"lsn"`
	Schema    string   `json:"schema"`
	Table     string   `json:"table"`
	Columns   []Column `json:"columns"`
	Identity  []Column `json:"identity"`
	Metadata  Metadata `json:"metadata"` // pgstream specific metadata
}

type Metadata struct {
	SchemaID        xid.ID `json:"schema_id"`         // the schema ID the event was stamped with
	TablePgstreamID string `json:"table_pgstream_id"` // the ID of the table to which the event belongs
	// This is the Pgstream ID of the "id" column. We track this specifically, as we extract it from the event
	// in order to use as the ID for the OS record.
	InternalColID string `json:"id_col_pgstream_id"`
	// This is the Pgstream ID of the "version" column. We track this specifically, as we extract it from the event
	// in order to use as the version when working with OS' optimistic concurrency checks.
	InternalColVersion string `json:"version_col_pgstream_id"`
}

type Column struct {
	// ID is a pgstream assigned immutable column id. Id does not change when column is renamed.
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func (d *Data) IsUpdate() bool {
	return d.Action == "U"
}

func (d *Data) IsInsert() bool {
	return d.Action == "I"
}

// IsEmpty is true if string fields are empty
func (m Metadata) IsEmpty() bool {
	if m.TablePgstreamID == "" && m.InternalColID == "" && m.InternalColVersion == "" {
		return true
	}
	return false
}

// CommitPosition represents a position in the input stream, which can be either postgres or kafka
type CommitPosition struct {
	PGPos    replication.LSN
	KafkaPos *kafka.Message
}

func (c *CommitPosition) IsEmpty() bool {
	return c.PGPos == 0 && c.KafkaPos == nil
}

func (c *CommitPosition) After(pos *CommitPosition) bool {
	switch {
	case c.KafkaPos != nil && pos.KafkaPos != nil:
		return c.KafkaPos.Time.After(pos.KafkaPos.Time)
	default:
		return c.PGPos > pos.PGPos
	}
}

func (c *CommitPosition) String() string {
	if c.KafkaPos != nil {
		return fmt.Sprintf("topic: %s, partition: %d, offset: %d", c.KafkaPos.Topic, c.KafkaPos.Partition, c.KafkaPos.Offset)
	}
	return fmt.Sprintf("%X", c.PGPos)
}
