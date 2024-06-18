// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type Processor interface {
	ProcessWALEvent(ctx context.Context, walEvent *wal.Event) error
	Name() string
}

var (
	ErrPanic               = errors.New("panic while processing wal event")
	ErrIncompatibleWalData = errors.New("wal data event is not a schema log entry")
)

func IsSchemaLogEvent(d *wal.Data) bool {
	return d.Schema == schemalog.SchemaName && d.Table == schemalog.TableName
}

func WalDataToLogEntry(d *wal.Data) (*schemalog.LogEntry, error) {
	if !IsSchemaLogEvent(d) {
		return nil, ErrIncompatibleWalData
	}

	intermediateRec := make(map[string]any, len(d.Columns))
	for _, col := range d.Columns { // we only process inserts, so identity columns should never be set
		intermediateRec[col.Name] = col.Value
	}

	intermediateRecBytes, err := json.Marshal(intermediateRec)
	if err != nil {
		return nil, fmt.Errorf("parsing wal event into schema log entry, intermediate record is not valid JSON: %w", err)
	}

	var le schemalog.LogEntry
	if err := json.Unmarshal(intermediateRecBytes, &le); err != nil {
		return nil, fmt.Errorf("parsing wal event into schema, intermediate record is not valid JSON: %w", err)
	}

	return &le, nil
}
