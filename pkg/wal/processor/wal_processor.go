// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
)

type Processor interface {
	ProcessWALEvent(ctx context.Context, walEvent *wal.Data) error
	Close() error
}

func IsSchemaLogEvent(d *wal.Data) bool {
	return d.Schema == schemalog.SchemaName && d.Table == schemalog.TableName
}

var ErrPanic = errors.New("panic while processing wal event")
