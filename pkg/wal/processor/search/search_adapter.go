// SPDX-License-Identifier: Apache-2.0

package search

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type walAdapter interface {
	walDataToQueueItem(*wal.Data) (*queueItem, error)
}

type adapter struct {
	mapper      Mapper
	marshaler   func(any) ([]byte, error)
	unmarshaler func([]byte, any) error
}

var (
	errInvalidData     = errors.New("wal data event is not a schema log entry")
	errUnsupportedType = errors.New("type not supported for column")
)

func newAdapter(m Mapper) *adapter {
	return &adapter{
		mapper:      m,
		marshaler:   json.Marshal,
		unmarshaler: json.Unmarshal,
	}
}

func (a *adapter) walDataToQueueItem(d *wal.Data) (*queueItem, error) {
	if processor.IsSchemaLogEvent(d) {
		// we only care about inserts - updates can happen when the schema log
		// is acked
		if d.Action != "I" {
			return nil, nil
		}

		logEntry, size, err := a.walDataToLogEntry(d)
		if err != nil {
			return nil, err
		}

		return &queueItem{
			schemaChange: logEntry,
			bytesSize:    size,
		}, nil
	}

	if d.Metadata.IsEmpty() {
		return nil, errMetadataMissing
	}

	switch d.Action {
	case "I", "U", "D":
		doc, err := a.walDataToDocument(d)
		if err != nil {
			return nil, err
		}
		size, err := a.documentSize(doc)
		if err != nil {
			return nil, fmt.Errorf("calculating document size: %w", err)
		}

		return &queueItem{
			write:     doc,
			bytesSize: size,
		}, nil

	case "T":
		truncateItem := &truncateItem{
			schemaName: d.Schema,
			tableID:    d.Metadata.TablePgstreamID,
		}
		return &queueItem{
			truncate:  truncateItem,
			bytesSize: len(truncateItem.schemaName) + len(truncateItem.tableID),
		}, nil

	default:
		return nil, nil
	}
}

func (a *adapter) walDataToLogEntry(d *wal.Data) (*schemalog.LogEntry, int, error) {
	if !processor.IsSchemaLogEvent(d) {
		return nil, 0, errInvalidData
	}

	intermediateRec := make(map[string]any, len(d.Columns))
	for _, col := range d.Columns { // we only process inserts, so identity columns should never be set
		intermediateRec[col.Name] = col.Value
	}

	intermediateRecBytes, err := a.marshaler(intermediateRec)
	if err != nil {
		return nil, 0, fmt.Errorf("parsing wal event into schema log entry, intermediate record is not valid JSON: %w", err)
	}

	var le schemalog.LogEntry
	if err := a.unmarshaler(intermediateRecBytes, &le); err != nil {
		return nil, 0, fmt.Errorf("parsing wal event into schema, intermediate record is not valid JSON: %w", err)
	}

	return &le, len(intermediateRecBytes), nil
}

func (a *adapter) walDataToDocument(data *wal.Data) (*Document, error) {
	var doc *Document
	var err error
	switch data.Action {
	case "D":
		doc, err = a.parseColumns(data.Identity, data.Metadata)
		if err != nil {
			return nil, err
		}
		doc.Delete = true
		doc.Version += 1
	default:
		doc, err = a.parseColumns(data.Columns, data.Metadata)
		if err != nil {
			return nil, err
		}
		// Go through the identity columns (old values) to include any values
		// that were not found in the replication event but are in the identity.
		// This is needed because TOAST columns are not included in the replication
		// event unless they change in the transaction.
		for _, col := range data.Identity {
			if col.ID == data.Metadata.InternalColID || col.ID == data.Metadata.InternalColVersion {
				continue
			}
			if _, exists := doc.Data[col.ID]; !exists {
				parsedColumn, err := a.mapper.MapColumnValue(schemalog.Column{
					Name:       col.Name,
					DataType:   col.Type,
					PgstreamID: col.ID,
				}, col.Value)
				if err != nil {
					// we do not map unsupported types
					if errors.As(err, &ErrTypeInvalid{}) {
						continue
					}
					return nil, err
				}
				doc.Data[col.ID] = parsedColumn
			}
		}
	}

	doc.Schema = data.Schema
	return doc, nil
}

func (a *adapter) parseColumns(columns []wal.Column, metadata wal.Metadata) (*Document, error) {
	recIDFound := false
	versionFound := false
	doc := &Document{
		Data: map[string]any{},
	}

	var err error
	for _, col := range columns {
		switch col.ID {
		case metadata.InternalColID:
			if doc.ID, err = a.parseIDColumn(metadata.TablePgstreamID, col.Value); err != nil {
				return nil, fmt.Errorf("id found, but unable to parse: %w", err)
			}
			recIDFound = true
		case metadata.InternalColVersion:
			if doc.Version, err = a.parseVersionColumn(col.Value); err != nil {
				return nil, fmt.Errorf("version found, but unable to parse: %w", err)
			}
			versionFound = true
		default:
			parsedColumn, err := a.mapper.MapColumnValue(schemalog.Column{
				Name:       col.Name,
				DataType:   col.Type,
				PgstreamID: col.ID,
			}, col.Value)
			if err != nil {
				// we do not map unsupported types
				if errors.As(err, &ErrTypeInvalid{}) {
					continue
				}
				return nil, err
			}
			doc.Data[col.ID] = parsedColumn
		}
	}

	if !recIDFound {
		return nil, errIDNotFound
	}
	if !versionFound {
		return nil, errVersionNotFound
	}

	doc.Data["_table"] = metadata.TablePgstreamID

	return doc, nil
}

// parseIDColumn extracts the ID from an `any` type. Prepends the table name to the ID
// since we have multiple tables in the same schema.
func (a *adapter) parseIDColumn(tableName string, id any) (string, error) {
	i := ""

	switch v := id.(type) {
	case string:
		i = v
	case int64:
		i = strconv.FormatInt(v, 10)
	case float64:
		i = strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		return "", errNilIDValue
	default:
		return "", fmt.Errorf("%T: %w", v, errUnsupportedType)
	}

	return fmt.Sprintf("%s_%s", tableName, i), nil
}

func (a *adapter) parseVersionColumn(version any) (int, error) {
	switch v := version.(type) {
	case int64:
		return int(v), nil
	case float64:
		return roundFloat64(v), nil
	case nil:
		return 0, errNilVersionValue
	default:
		return 0, fmt.Errorf("%T: %w", v, errUnsupportedType)
	}
}

func (a *adapter) documentSize(doc *Document) (int, error) {
	bytes, err := a.marshaler(doc.Data)
	if err != nil {
		return 0, err
	}
	return len(bytes), nil
}

// roundFloat64 converts a float64 to int by rounding.
func roundFloat64(val float64) int {
	if val < 0 {
		return int(val - 0.5)
	}
	return int(val + 0.5)
}
