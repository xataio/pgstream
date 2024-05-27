// SPDX-License-Identifier: Apache-2.0

package schemalog

import "encoding/json"

type Schema struct {
	Tables []Table `json:"tables"`
	// Dropped will be true if the schema has been deleted
	Dropped bool `json:"dropped,omitempty"`
}

type Table struct {
	Oid     string   `json:"oid"`
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	// PgstreamID is a unique identifier of the table generated by pgstream
	PgstreamID string `json:"pgstream_id"`
}

type Column struct {
	Name         string  `json:"name"`
	DataType     string  `json:"type"`
	DefaultValue *string `json:"default,omitempty"`
	Nullable     bool    `json:"nullable"`
	// Metadata is NOT typed here because we don't fully control the content that is sent from the publisher.
	Metadata   *string `json:"metadata"`
	PgstreamID string  `json:"pgstream_id"`
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	type schemaAlias Schema
	schemaJSON, err := json.Marshal(schemaAlias(*s))
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(schemaJSON))
}

func (s *Schema) IsEqual(other *Schema) bool {
	switch {
	case s == nil && other == nil:
		return true
	case s != nil && other == nil, s == nil && other != nil:
		return false
	default:
		if len(s.Tables) != len(other.Tables) {
			return false
		}

		for i := range s.Tables {
			if !s.Tables[i].IsEqual(&other.Tables[i]) {
				return false
			}
		}

		return true
	}
}

func (s *Schema) Diff(previous *Schema) *SchemaDiff {
	var d SchemaDiff

	// if a table ID exists in previous, but not s: remove the table
	for _, previousTable := range previous.Tables {
		if !s.hasTableID(previousTable.PgstreamID) {
			d.TablesToRemove = append(d.TablesToRemove, previousTable)
		}
	}

	for i, table := range s.Tables {
		if previousTable := previous.getTableByID(table.PgstreamID); previousTable != nil {
			d.ColumnsToAdd = append(d.ColumnsToAdd, diffColumns(&s.Tables[i], previousTable)...)
		} else {
			// if the "old" schema does not have the table, we can add all
			// columns without checking further.
			d.ColumnsToAdd = append(d.ColumnsToAdd, table.Columns...)
		}
	}

	return &d
}

func (s *Schema) getTableByName(tableName string) *Table {
	for _, t := range s.Tables {
		if t.Name == tableName {
			return &t
		}
	}
	return nil
}

func (s *Schema) getTableByID(pgstreamID string) *Table {
	for i := range s.Tables {
		if s.Tables[i].PgstreamID == pgstreamID {
			return &s.Tables[i]
		}
	}
	return nil
}

func (s *Schema) hasTableID(pgstreamID string) bool {
	return s.getTableByID(pgstreamID) != nil
}

func (t *Table) IsEqual(other *Table) bool {
	switch {
	case t == nil && other == nil:
		return true
	case t != nil && other == nil, t == nil && other != nil:
		return false
	default:
		if len(t.Columns) != len(other.Columns) {
			return false
		}

		if t.Oid != other.Oid || t.PgstreamID != other.PgstreamID || t.Name != other.Name {
			return false
		}

		return unorderedColumnsEqual(t.Columns, other.Columns)
	}
}

func (t *Table) GetColumnByName(name string) *Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return &c
		}
	}
	return nil
}

func (c *Column) IsEqual(other *Column) bool {
	return c.Name == other.Name &&
		c.DataType == other.DataType &&
		c.Nullable == other.Nullable &&
		c.PgstreamID == other.PgstreamID &&
		c.DefaultValue == other.DefaultValue &&
		c.Metadata == other.Metadata
}

type SchemaDiff struct {
	TablesToRemove []Table
	ColumnsToAdd   []Column
}

func (d *SchemaDiff) Empty() bool {
	return len(d.TablesToRemove) == 0 && len(d.ColumnsToAdd) == 0
}

func unorderedColumnsEqual(a, b []Column) bool {
	if len(a) != len(b) {
		return false
	}

	for _, colA := range a {
		var found bool

		for i := range b {
			if colA.IsEqual(&b[i]) {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func diffColumns(new, old *Table) []Column {
	var colsAdded []Column

	for _, newCol := range new.Columns {
		var found bool

		for _, oldCol := range old.Columns {
			if newCol.PgstreamID == oldCol.PgstreamID {
				found = true
				break
			}
		}

		if !found {
			colsAdded = append(colsAdded, newCol)
		}
	}

	return colsAdded
}
