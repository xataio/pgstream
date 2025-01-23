// SPDX-License-Identifier: Apache-2.0

package search

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/xataio/pgstream/internal/json"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
	"github.com/xataio/pgstream/pkg/wal/replication"
)

// walAdapter converts wal events to search messages
type walAdapter interface {
	walEventToMsg(*wal.Event) (*msg, error)
}

type adapter struct {
	mapper      Mapper
	marshaler   func(any) ([]byte, error)
	unmarshaler func([]byte, any) error
	lsnParser   replication.LSNParser
}

var errUnsupportedType = errors.New("type not supported for column")

func newAdapter(m Mapper, parser replication.LSNParser) *adapter {
	return &adapter{
		mapper:      m,
		marshaler:   json.Marshal,
		unmarshaler: json.Unmarshal,
		lsnParser:   parser,
	}
}

func (a *adapter) walEventToMsg(e *wal.Event) (*msg, error) {
	if e.Data == nil {
		return &msg{}, nil
	}

	if processor.IsSchemaLogEvent(e.Data) {
		// we only care about inserts - updates can happen when the schema log
		// is acked
		if !e.Data.IsInsert() {
			return nil, nil
		}

		logEntry, size, err := a.walDataToLogEntry(e.Data)
		if err != nil {
			return nil, err
		}

		return &msg{
			schemaChange: logEntry,
			bytesSize:    size,
		}, nil
	}

	if e.Data.Metadata.IsEmpty() {
		return nil, errMetadataMissing
	}

	switch e.Data.Action {
	case "I", "U", "D":
		doc, err := a.walDataToDocument(e.Data)
		if err != nil {
			return nil, err
		}
		size, err := a.documentSize(doc)
		if err != nil {
			return nil, fmt.Errorf("calculating document size: %w", err)
		}

		return &msg{
			write:     doc,
			bytesSize: size,
		}, nil

	case "T":
		truncateItem := &truncateItem{
			schemaName: e.Data.Schema,
			tableID:    e.Data.Metadata.TablePgstreamID,
		}
		return &msg{
			truncate:  truncateItem,
			bytesSize: len(truncateItem.schemaName) + len(truncateItem.tableID),
		}, nil

	default:
		return nil, nil
	}
}

func (a *adapter) walDataToLogEntry(d *wal.Data) (*schemalog.LogEntry, int, error) {
	if !processor.IsSchemaLogEvent(d) {
		return nil, 0, processor.ErrIncompatibleWalData
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
		doc, err = a.parseColumns(data.Identity, data.Metadata, data.LSN)
		if err != nil {
			return nil, err
		}
		doc.Delete = true
		doc.Version += 1
	default:
		doc, err = a.parseColumns(data.Columns, data.Metadata, data.LSN)
		if err != nil {
			return nil, err
		}

		// Go through the identity columns (old values) to include any values
		// that were not found in the replication event but are in the identity.
		// This is needed because TOAST columns are not included in the replication
		// event unless they change in the transaction.
		for _, col := range data.Identity {
			if data.Metadata.IsIDColumn(col.ID) || data.Metadata.IsVersionColumn(col.ID) {
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

		// if we use the LSN as version, we need to increment it manually on
		// update operations to avoid version conflicts
		if useLSNAsVersion(data.Metadata) && data.IsUpdate() {
			doc.Version += 1
		}
	}

	doc.Schema = data.Schema
	return doc, nil
}

func (a *adapter) parseColumns(columns []wal.Column, metadata wal.Metadata, lsnStr string) (*Document, error) {
	versionFound := false
	doc := &Document{
		Data: map[string]any{},
	}

	var err error
	idColumns := []wal.Column{}
	for _, col := range columns {
		switch {
		case metadata.IsIDColumn(col.ID):
			idColumns = append(idColumns, col)
		case metadata.IsVersionColumn(col.ID):
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

	if err := a.parseIDColumns(metadata.TablePgstreamID, idColumns, doc); err != nil {
		return nil, fmt.Errorf("parsing id columns: %w", err)
	}

	// if version is not found, default to use the LSN if no version column is
	// provided. Return an error otherwise.
	if !versionFound {
		if useLSNAsVersion(metadata) {
			lsn, err := a.lsnParser.FromString(lsnStr)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errIncompatibleLSN, err)
			}
			doc.Version = int(lsn)
		} else {
			return nil, processor.ErrVersionNotFound
		}
	}

	doc.Data["_table"] = metadata.TablePgstreamID

	return doc, nil
}

// parseIDColumns extracts the IDs from `any` type. Prepends the table name to
// the ID since we have multiple tables in the same schema. It will combine them
// if there are multiple identity columns.
func (a *adapter) parseIDColumns(tableName string, idColumns []wal.Column, doc *Document) error {
	if len(idColumns) == 0 {
		return processor.ErrIDNotFound
	}

	id := ""
	addToID := func(s string) {
		if id == "" {
			id = s
			return
		}
		id = fmt.Sprintf("%s-%s", id, s)
	}

	isCompositeID := len(idColumns) > 1
	for _, col := range idColumns {
		switch v := col.Value.(type) {
		case string:
			addToID(v)
		case int32:
			addToID(strconv.FormatInt(int64(v), 10))
		case int64:
			addToID(strconv.FormatInt(v, 10))
		case float64:
			addToID(strconv.FormatFloat(v, 'f', -1, 64))
		case nil:
			return errNilIDValue
		default:
			return fmt.Errorf("%T: %w", v, errUnsupportedType)
		}

		// if the identity is a combination of multiple fields, we need to map
		// the individual fields as part of the document. Otherwise, the
		// identity will be the document id.
		if isCompositeID {
			parsedColumn, err := a.mapper.MapColumnValue(schemalog.Column{
				Name:       col.Name,
				DataType:   col.Type,
				PgstreamID: col.ID,
			}, col.Value)
			if err != nil {
				return fmt.Errorf("mapping id column value: %w", err)
			}
			doc.Data[col.ID] = parsedColumn
		}
	}

	doc.ID = fmt.Sprintf("%s_%s", tableName, id)
	return nil
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

func useLSNAsVersion(metadata wal.Metadata) bool {
	return metadata.InternalColVersion == ""
}
