// SPDX-License-Identifier: Apache-2.0

package schemalog

import (
	"context"
	"errors"
)

type Store interface {
	Insert(ctx context.Context, schemaName string) (*LogEntry, error)
	Fetch(ctx context.Context, schemaName string, ackedOnly bool) (*LogEntry, error)
	Ack(ctx context.Context, le *LogEntry) error
	Close() error
}

var ErrNoRows = errors.New("no rows")

const (
	SchemaName = "pgstream"
	TableName  = "schema_log"
)
