// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/rs/xid"
)

// Event represents WAL information. Data can be nil for a keepalive or an
// internal transaction boundary; Transaction distinguishes the latter.
type Event struct {
	Data           *Data
	CommitPosition CommitPosition

	// CheckpointPosition is an internal acknowledgement token. When empty,
	// CommitPosition is used for checkpointing. Keeping the two values separate
	// allows a target to retain the real source position in its payload while a
	// transaction tracker correlates target acknowledgements without exposing
	// its tokens externally.
	CheckpointPosition CommitPosition `json:"-"`

	// Transaction identifies a wal2json transaction boundary or one of its
	// entries. It is internal pipeline metadata and is not serialised as part of
	// Data.
	Transaction *Transaction `json:"-"`

	// Generation identifies the replication connection that produced the
	// event. It lets the transaction tracker discard replayed connection state
	// and ignore acknowledgements from older target batches.
	Generation uint64 `json:"-"`
}

// PositionToCheckpoint returns the internal acknowledgement position when one
// has been assigned, or the source commit position for regular events.
func (e *Event) PositionToCheckpoint() CommitPosition {
	if e == nil {
		return ""
	}
	if e.CheckpointPosition != "" {
		return e.CheckpointPosition
	}
	return e.CommitPosition
}

type TransactionBoundary uint8

const (
	TransactionEntry TransactionBoundary = iota
	TransactionBegin
	TransactionCommit
)

// Transaction contains internal transaction metadata derived from wal2json's
// B and C records. ID is local to one listener lifetime; PostgreSQL XIDs are
// not required because wal2json emits non-interleaved transaction records.
type Transaction struct {
	ID       uint64
	Boundary TransactionBoundary
}

// Data contains the wal data properties identifying the table operation.
type Data struct {
	Action    string   `json:"action"`    // "I" -- insert, "U" -- update, "D" -- delete, "T" -- truncate, "M" -- logical message, "B"/"C" -- transaction boundaries
	Timestamp string   `json:"timestamp"` // ISO8601, i.e. 2019-12-29 04:58:34.806671
	LSN       string   `json:"lsn"`
	Schema    string   `json:"schema"`
	Table     string   `json:"table"`
	Columns   []Column `json:"columns"`
	Identity  []Column `json:"identity"`
	XID       int64    `json:"xid,omitempty"`
	// For logical messages (when Action == "M")
	Prefix  string `json:"prefix,omitempty"`
	Content string `json:"content,omitempty"`
	// pgstream specific metadata
	Metadata Metadata `json:"metadata"`

	ddlEventOnce sync.Once
	ddlEvent     *DDLEvent
	ddlEventErr  error
}

// Metadata is pgstream specific properties to help identify the id/version
// within the wal event as well as some pgstream unique immutable ids for the
// schema and the table it relates to.
type Metadata struct {
	SchemaID        xid.ID `json:"schema_id"`         // the schema ID the event was stamped with
	TablePgstreamID string `json:"table_pgstream_id"` // the ID of the table to which the event belongs
	// This is the Pgstream ID of the "id" column(s). We track this specifically, as we extract it from the event
	// in order to use as the ID for the record.
	InternalColIDs []string `json:"id_col_pgstream_id"`
}

type Column struct {
	// ID is a pgstream assigned immutable column id. Id does not change when column is renamed.
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

const ZeroLSN = "0/0"

const iso8601Format = "2006-01-02 15:04:05.999999+00"

var validTimeFormats = []string{iso8601Format, time.RFC3339}

var errUnrecognizedEventTimestampFormat = errors.New("unrecognized wal event timestamp format")

func (d *Data) GetTimestamp() (time.Time, error) {
	for _, format := range validTimeFormats {
		t, err := time.Parse(format, d.Timestamp)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, errUnrecognizedEventTimestampFormat
}

func (d *Data) IsUpdate() bool {
	return d.Action == "U"
}

func (d *Data) IsInsert() bool {
	return d.Action == "I"
}

// IsEmpty returns true if the pgstream metadata hasn't been populated, false
// otherwise.
func (m Metadata) IsEmpty() bool {
	if m.TablePgstreamID == "" && len(m.InternalColIDs) == 0 {
		return true
	}
	return false
}

// IsIDColumn returns true if the column id on input is part of the pgstream
// identified identity columns.
func (m Metadata) IsIDColumn(colID string) bool {
	return slices.Contains(m.InternalColIDs, colID)
}

// CommitPosition represents a position in the input stream
type CommitPosition string
