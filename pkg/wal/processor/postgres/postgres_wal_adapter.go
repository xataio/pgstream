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

func newAdapter(schemaQuerier schemalogQuerier, onConflictAction string) (*adapter, error) {
	dmlAdapter, err := newDMLAdapter(onConflictAction)
	if err != nil {
		return nil, err
	}

	var ddl *ddlAdapter
	if schemaQuerier != nil {
		ddl = newDDLAdapter(schemaQuerier)
	}
	return &adapter{
		dmlAdapter: dmlAdapter,
		ddlAdapter: ddl,
	}, nil
}

func (a *adapter) walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error) {
	if e.Data == nil {
		return []*query{{}}, nil
	}

	if processor.IsSchemaLogEvent(e.Data) && a.ddlAdapter != nil {
		return a.ddlAdapter.walDataToQueries(ctx, e.Data)
	}

	return []*query{a.dmlAdapter.walDataToQuery(e.Data)}, nil
}
