// SPDX-License-Identifier: Apache-2.0

package search

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/xataio/pgstream/internal/replication"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

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
		return &msg{
			pos: e.CommitPosition,
		}, nil
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
			pos:          e.CommitPosition,
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
			pos:       e.CommitPosition,
		}, nil

	case "T":
		truncateItem := &truncateItem{
			schemaName: e.Data.Schema,
			tableID:    e.Data.Metadata.TablePgstreamID,
		}
		return &msg{
			truncate:  truncateItem,
			bytesSize: len(truncateItem.schemaName) + len(truncateItem.tableID),
			pos:       e.CommitPosition,
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
		return nil, processor.ErrIDNotFound
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

func useLSNAsVersion(metadata wal.Metadata) bool {
	return metadata.InternalColVersion == ""
}
