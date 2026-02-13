// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

type walAdapter interface {
	walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error)
	close() error
}

type schemaObserver interface {
	getGeneratedColumnNames(ctx context.Context, schema, table string) (map[string]struct{}, error)
	getSequenceColumns(ctx context.Context, schema, table string) (map[string]string, error)
	isMaterializedView(ctx context.Context, schema, table string) bool
	update(ddlEvent *wal.DDLEvent)
	close() error
}

type dmlQueryAdapter interface {
	walDataToQueries(d *wal.Data, schemaInfo schemaInfo) ([]*query, error)
}

type ddlQueryAdapter interface {
	walDataToQueries(ctx context.Context, d *wal.Data) ([]*query, error)
}

type schemaInfo struct {
	generatedColumns map[string]struct{}
	sequenceColumns  map[string]string
}

type adapter struct {
	dmlAdapter      dmlQueryAdapter
	ddlAdapter      ddlQueryAdapter
	ddlEventAdapter ddlEventAdapter

	schemaObserver schemaObserver
}

type (
	ddlEventAdapter func(*wal.Data) (*wal.DDLEvent, error)
)

func newAdapter(ctx context.Context, logger loglib.Logger, ignoreDDL bool, pgURL string, onConflictAction string, forCopy bool) (*adapter, error) {
	schemaObserver, err := newPGSchemaObserver(ctx, pgURL, logger, forCopy)
	if err != nil {
		return nil, err
	}

	dmlAdapter, err := newDMLAdapter(onConflictAction, forCopy, logger)
	if err != nil {
		return nil, err
	}

	var ddl ddlQueryAdapter
	if !ignoreDDL {
		ddl = newDDLAdapter()
	}

	return &adapter{
		dmlAdapter:      dmlAdapter,
		ddlAdapter:      ddl,
		schemaObserver:  schemaObserver,
		ddlEventAdapter: wal.WalDataToDDLEvent,
	}, nil
}

func (a *adapter) walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error) {
	switch {
	case e.Data == nil,
		a.schemaObserver.isMaterializedView(ctx, e.Data.Schema, e.Data.Table):
		// skip DML processing for materialized views (read only)
		return []*query{{}}, nil

	case e.Data.IsDDLEvent():
		ddlEvent, err := a.ddlEventAdapter(e.Data)
		if err != nil {
			return nil, err
		}
		a.schemaObserver.update(ddlEvent)

		// there's no ddl adapter, the ddl query will not be processed
		if a.ddlAdapter == nil {
			return []*query{{}}, nil
		}

		return a.ddlAdapter.walDataToQueries(ctx, e.Data)

	default:
		generatedColumns, err := a.schemaObserver.getGeneratedColumnNames(ctx, e.Data.Schema, e.Data.Table)
		if err != nil {
			return nil, err
		}

		columnSequences, err := a.schemaObserver.getSequenceColumns(ctx, e.Data.Schema, e.Data.Table)
		if err != nil {
			return nil, err
		}

		qs, err := a.dmlAdapter.walDataToQueries(e.Data, schemaInfo{
			generatedColumns: generatedColumns,
			sequenceColumns:  columnSequences,
		})
		if err != nil {
			return nil, err
		}

		return qs, nil
	}
}

func (a *adapter) close() error {
	return a.schemaObserver.close()
}
