// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"encoding/json"
	"fmt"
	"time"

	// TODO: remove pg dependency from schemalog pkg (move to postgres)
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/xid"
)

// LogEntry contains the information relating to a schema log
type LogEntry struct {
	ID         xid.ID                   `json:"id"`
	Version    int64                    `json:"version"`
	SchemaName string                   `json:"schema_name"`
	CreatedAt  SchemaCreatedAtTimestamp `json:"created_at"`
	Schema     Schema                   `json:"schema"`
	// Acked indicates if the schema has been processed and acknowledged by
	// pgstream after being updated in the source database
	Acked bool `json:"acked"`
}

func (m *LogEntry) UnmarshalJSON(b []byte) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}

	for k, v := range obj {
		switch k {
		case "id":
			if err := json.Unmarshal(v, &m.ID); err != nil {
				return err
			}
		case "version":
			if err := json.Unmarshal(v, &m.Version); err != nil {
				return err
			}
		case "schema_name":
			if err := json.Unmarshal(v, &m.SchemaName); err != nil {
				return err
			}
		case "created_at":
			if err := json.Unmarshal(v, &m.CreatedAt); err != nil {
				return err
			}
		case "acked":
			if err := json.Unmarshal(v, &m.Acked); err != nil {
				return err
			}
		case "schema":
			if len(v) == 0 {
				m.Schema = Schema{}
				continue
			}
			var schemaStr string
			if err := json.Unmarshal(v, &schemaStr); err != nil {
				return err
			}
			if err := json.Unmarshal([]byte(schemaStr), &m.Schema); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *LogEntry) IsEmpty() bool {
	return m == nil || m.IsEqual(&LogEntry{})
}

func (m *LogEntry) IsEqual(other *LogEntry) bool {
	switch {
	case m == nil && other == nil:
		return true
	case m == nil && other != nil, m != nil && other == nil:
		return false
	default:
		return m.ID == other.ID && m.SchemaName == other.SchemaName && m.CreatedAt == other.CreatedAt && m.Schema.IsEqual(&other.Schema)
	}
}

func (m *LogEntry) After(other *LogEntry) bool {
	return m.ID.Compare(other.ID) > 0
}

func (m *LogEntry) Diff(previous *LogEntry) *SchemaDiff {
	if previous == nil {
		return m.Schema.Diff(&Schema{})
	}
	return m.Schema.Diff(&previous.Schema)
}

func (m *LogEntry) GetTableByName(tableName string) (Table, bool) {
	return m.Schema.getTableByName(tableName)
}

// SchemaCreatedAtTimestamp is a wrapper around time.Time that allows us to parse to and from the PG timestamp format.
type SchemaCreatedAtTimestamp struct {
	time.Time
}

func NewSchemaCreatedAtTimestamp(t time.Time) SchemaCreatedAtTimestamp {
	return SchemaCreatedAtTimestamp{time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		(t.Nanosecond()/1000000)*1000, // here to truncate nanoseconds to 6 digits to match up with PG
		time.UTC,
	)}
}

func (s *SchemaCreatedAtTimestamp) Scan(src interface{}) error {
	switch src := src.(type) {
	case time.Time:
		*s = SchemaCreatedAtTimestamp{src}
		return nil
	case []byte:
		return s.UnmarshalJSON(src)
	case string:
		return s.UnmarshalJSON([]byte(src))
	default:
		return fmt.Errorf("cannot convert %T to SchemaCreatedAtTimestamp", src)
	}
}

func (s SchemaCreatedAtTimestamp) TimestampValue() (pgtype.Timestamp, error) {
	return pgtype.Timestamp{Time: s.Time.UTC(), Valid: true}, nil
}

func (s SchemaCreatedAtTimestamp) MarshalJSON() ([]byte, error) {
	// We do it like this because ES is strict. When PG sends is a timestamp, it might have milliseconds
	// set to something like 999990. ES will accept this. However, when we parse it using time.Parse, and
	// then marhsal it again here, it will set the milliseconds to 99999 (i.e. trim the 0). Which is then
	// rejected by ES. Life is better when we have same expectations on format everywhere.
	pgTimeLayout := "2006-01-02 15:04:05"
	return json.Marshal(fmt.Sprintf("%s.%06d", s.Format(pgTimeLayout), s.Nanosecond()/1000))
}

func (s *SchemaCreatedAtTimestamp) UnmarshalJSON(b []byte) error {
	var ts string
	var err error
	if err = json.Unmarshal(b, &ts); err != nil {
		return err
	}

	pgTimeLayout := "2006-01-02 15:04:05.999999"
	if s.Time, err = time.Parse(pgTimeLayout, ts); err != nil {
		return err
	}

	return nil
}
