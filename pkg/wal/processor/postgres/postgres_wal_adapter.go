// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type walAdapter interface {
	walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error)
}

type adapter struct {
	dmlAdapter *dmlAdapter
	ddlAdapter *ddlAdapter
}

func newAdapter(schemaQuerier schemalogQuerier) *adapter {
	return &adapter{
		dmlAdapter: &dmlAdapter{},
		ddlAdapter: newDDLAdapter(schemaQuerier),
	}
}

func (a *adapter) walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error) {
	if e.Data == nil {
		return []*query{{}}, nil
	}

	if processor.IsSchemaLogEvent(e.Data) {
		return a.ddlAdapter.walDataToQueries(ctx, e.Data)
	}

	return []*query{a.dmlAdapter.walDataToQuery(e.Data)}, nil
}
