// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xataio/pgstream/internal/json"
)

// DDLEvent represents a parsed DDL logical message from pgstream.ddl prefix
type DDLEvent struct {
	DDL        string      `json:"ddl"`
	SchemaName string      `json:"schema_name"`
	CommandTag string      `json:"command_tag"`
	Objects    []DDLObject `json:"objects"`
}

// DDLObject represents an object affected by a DDL command
type DDLObject struct {
	Type              string      `json:"type"`
	Identity          string      `json:"identity"`
	Schema            string      `json:"schema"`
	OID               string      `json:"oid"`
	PgstreamID        string      `json:"pgstream_id,omitempty"`
	Columns           []DDLColumn `json:"columns,omitempty"`
	PrimaryKeyColumns []string    `json:"primary_key_columns,omitempty"`
}

// DDLColumn represents a column in a DDL event
type DDLColumn struct {
	Attnum    int     `json:"attnum"`
	Name      string  `json:"name"`
	Type      string  `json:"type"`
	Nullable  bool    `json:"nullable"`
	Default   *string `json:"default,omitempty"`
	Generated bool    `json:"generated"`
	Identity  *string `json:"identity,omitempty"`
	Unique    bool    `json:"unique"`
}

var (
	ErrNotDDLEvent            = fmt.Errorf("not a DDL event")
	ErrInvalidDDLEventContent = errors.New("invalid DDL event content")
)

const DDLPrefix = "pgstream.ddl"

const LogicalMessageAction = "M"

// IsDDLEvent returns true if the data represents a DDL logical message
func (d *Data) IsDDLEvent() bool {
	return d.Action == LogicalMessageAction && d.Prefix == DDLPrefix
}

// WalDataToDDLEvent parses the wal data content field as a DDL event
func WalDataToDDLEvent(d *Data) (*DDLEvent, error) {
	if !d.IsDDLEvent() {
		return nil, fmt.Errorf("%w: action=%s, prefix=%s", ErrNotDDLEvent, d.Action, d.Prefix)
	}

	var ddlEvent DDLEvent
	if err := json.Unmarshal([]byte(d.Content), &ddlEvent); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDDLEventContent, err)
	}

	return &ddlEvent, nil
}

// GetTableObjects returns only the table objects from the DDL event
func (e *DDLEvent) GetTableObjects() []DDLObject {
	return e.GetObjectsByType("table")
}

// GetTableColumnObjects returns only the table column objects from the DDL event
func (e *DDLEvent) GetTableColumnObjects() []DDLObject {
	return e.GetObjectsByType("table column")
}

// GetMaterializedViewObjects returns only the materialized view objects from
// the DDL event
func (e *DDLEvent) GetMaterializedViewObjects() []DDLObject {
	return e.GetObjectsByType("materialized_view")
}

func (e *DDLEvent) GetObjectsByType(objectType string) []DDLObject {
	objs := make([]DDLObject, 0)
	for _, obj := range e.Objects {
		if obj.Type == objectType {
			objs = append(objs, obj)
		}
	}
	return objs
}

func (e *DDLEvent) GetTableObjectByName(schema, table string) *DDLObject {
	for _, obj := range e.GetTableObjects() {
		if obj.Schema == schema && obj.GetTable() == table {
			return &obj
		}
	}
	return nil
}

func (e *DDLEvent) IsDropEvent() bool {
	return strings.HasPrefix(e.CommandTag, "DROP")
}

// GetSchema extracts the schema name from the identity.
// For example:
//   - "public.users" returns "public"
//   - "public.test_table.username" returns "public"
func (e *DDLObject) GetSchema() string {
	parts := strings.SplitN(e.Identity, ".", 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// GetTable extracts the table name from the identity.
// For table objects: "public.users" returns "users"
// For table column objects: "public.test_table.username" returns "test_table"
func (e *DDLObject) GetTable() string {
	parts := strings.Split(e.Identity, ".")
	switch len(parts) {
	case 2:
		return parts[1] // schema.table
	case 3:
		return parts[1] // schema.table.column
	default:
		return e.GetName() // fallback to last part
	}
}

// GetName extracts the unqualified object name from the identity.
// For example:
//   - "public.users" returns "users"
//   - "public.test_table.username" returns "username"
func (e *DDLObject) GetName() string {
	// Find the last dot and return everything after it
	for i := len(e.Identity) - 1; i >= 0; i-- {
		if e.Identity[i] == '.' {
			return e.Identity[i+1:]
		}
	}
	// If no dot found, return the whole identity
	return e.Identity
}

func (e *DDLObject) GetColumnByName(name string) (*DDLColumn, bool) {
	for i, col := range e.Columns {
		if col.Name == name {
			return &e.Columns[i], true
		}
	}
	return nil, false
}

// GetColumnPgstreamID returns the pgstream ID for a column based on table pgstream ID and attnum
func (c *DDLColumn) GetColumnPgstreamID(tablePgstreamID string) string {
	return fmt.Sprintf("%s-%d", tablePgstreamID, c.Attnum)
}

func (c *DDLColumn) HasSequence() bool {
	return c.GetSequenceName() != ""
}

func (c *DDLColumn) GetSequenceName() string {
	if c.Default == nil {
		return ""
	}

	def := *c.Default
	prefix := "nextval('"
	suffix := "'::regclass)"

	if strings.HasPrefix(def, prefix) && strings.HasSuffix(def, suffix) {
		return def[len(prefix) : len(def)-len(suffix)]
	}

	return ""
}

func (c *DDLColumn) IsGenerated() bool {
	return c.Generated || c.Identity != nil
}
