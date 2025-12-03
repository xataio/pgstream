// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

type walAdapter interface {
	walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error)
	close() error
}

type schemaObserver interface {
	getGeneratedColumnNames(ctx context.Context, schema, table string) ([]string, error)
	updateGeneratedColumnNames(schemalog *schemalog.LogEntry)
	close() error
}

type adapter struct {
	dmlAdapter      *dmlAdapter
	ddlAdapter      *ddlAdapter
	logEntryAdapter logEntryAdapter

	schemaObserver schemaObserver
}

func newAdapter(ctx context.Context, schemaQuerier schemalogQuerier, pgURL string, onConflictAction string, forCopy bool) (*adapter, error) {
	schemaObserver, err := newpgSchemaObserver(ctx, pgURL)
	if err != nil {
		return nil, err
	}

	dmlAdapter, err := newDMLAdapter(onConflictAction, forCopy)
	if err != nil {
		return nil, err
	}

	var ddl *ddlAdapter
	if schemaQuerier != nil {
		ddl = newDDLAdapter(schemaQuerier)
	}
	return &adapter{
		dmlAdapter:      dmlAdapter,
		ddlAdapter:      ddl,
		schemaObserver:  schemaObserver,
		logEntryAdapter: processor.WalDataToLogEntry,
	}, nil
}

func (a *adapter) walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error) {
	if e.Data == nil {
		return []*query{{}}, nil
	}

	if processor.IsSchemaLogEvent(e.Data) {
		schemaLog, err := a.logEntryAdapter(e.Data)
		if err != nil {
			return nil, err
		}
		a.schemaObserver.updateGeneratedColumnNames(schemaLog)
		// there's no ddl adapter, the ddl query will not be processed
		if a.ddlAdapter == nil {
			return []*query{{}}, nil
		}

		return a.ddlAdapter.schemaLogToQueries(ctx, schemaLog)
	}

	generatedColumns, err := a.schemaObserver.getGeneratedColumnNames(ctx, e.Data.Schema, e.Data.Table)
	if err != nil {
		return nil, err
	}

	q, err := a.dmlAdapter.walDataToQuery(e.Data, generatedColumns)
	if err != nil {
		return nil, err
	}

	return []*query{q}, nil
}

func (a *adapter) close() error {
	return a.schemaObserver.close()
}
