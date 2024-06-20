// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"slices"
	"time"

	"github.com/rs/xid"
)

// Event represents the WAL information. If the data is nil but there's a
// commit position present, it represents a keep alive event that needs to be
// checkpointed.
type Event struct {
	Data           *Data
	CommitPosition CommitPosition
}

// Data contains the wal data properties identifying the table operation.
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

// Metadata is pgstream specific properties to help identify the id/version
// within the wal event as well as some pgstream unique immutable ids for the
// schema and the table it relates to.
type Metadata struct {
	SchemaID        xid.ID `json:"schema_id"`         // the schema ID the event was stamped with
	TablePgstreamID string `json:"table_pgstream_id"` // the ID of the table to which the event belongs
	// This is the Pgstream ID of the "id" column(s). We track this specifically, as we extract it from the event
	// in order to use as the ID for the record.
	InternalColIDs []string `json:"id_col_pgstream_id"`
	// This is the Pgstream ID of the "version" column. We track this specifically, as we extract it from the event
	// in order to use as the version when working with optimistic concurrency checks.
	InternalColVersion string `json:"version_col_pgstream_id"`
}

type Column struct {
	// ID is a pgstream assigned immutable column id. Id does not change when column is renamed.
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

const iso8601Format = "2006-01-02 15:04:05.999999+00"

func (d *Data) GetTimestamp() (time.Time, error) {
	return time.Parse(iso8601Format, d.Timestamp)
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
	if m.TablePgstreamID == "" && len(m.InternalColIDs) == 0 && m.InternalColVersion == "" {
		return true
	}
	return false
}

// IsVersionColumn returns true if the column id on input matches the pgstream
// identified version column.
func (m Metadata) IsVersionColumn(colID string) bool {
	return m.InternalColVersion == colID
}

// IsIDColumn returns true if the column id on input is part of the pgstream
// identified identity columns.
func (m Metadata) IsIDColumn(colID string) bool {
	return slices.Contains(m.InternalColIDs, colID)
}

// CommitPosition represents a position in the input stream
type CommitPosition string
