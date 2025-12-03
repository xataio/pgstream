// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
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
	isMaterializedView(ctx context.Context, schema, table string) bool
	updateGeneratedColumnNames(schemalog *schemalog.LogEntry)
	updateMaterializedViews(schemalog *schemalog.LogEntry)
	close() error
}

type dmlQueryAdapter interface {
	walDataToQuery(d *wal.Data, generatedColumns []string) (*query, error)
}

type ddlQueryAdapter interface {
	schemaLogToQueries(ctx context.Context, l *schemalog.LogEntry) ([]*query, error)
}

type adapter struct {
	dmlAdapter      dmlQueryAdapter
	ddlAdapter      ddlQueryAdapter
	logEntryAdapter logEntryAdapter

	schemaObserver schemaObserver
}

func newAdapter(ctx context.Context, schemaQuerier schemalogQuerier, logger loglib.Logger, pgURL string, onConflictAction string, forCopy bool) (*adapter, error) {
	schemaObserver, err := newPGSchemaObserver(ctx, pgURL, logger)
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
	switch {
	case e.Data == nil,
		a.schemaObserver.isMaterializedView(ctx, e.Data.Schema, e.Data.Table):
		// skip DML processing for materialized views (read only)
		return []*query{{}}, nil

	case processor.IsSchemaLogEvent(e.Data):
		schemaLog, err := a.logEntryAdapter(e.Data)
		if err != nil {
			return nil, err
		}
		a.schemaObserver.updateGeneratedColumnNames(schemaLog)
		a.schemaObserver.updateMaterializedViews(schemaLog)

		// there's no ddl adapter, the ddl query will not be processed
		if a.ddlAdapter == nil {
			return []*query{{}}, nil
		}

		return a.ddlAdapter.schemaLogToQueries(ctx, schemaLog)

	default:
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
}

func (a *adapter) close() error {
	return a.schemaObserver.close()
}
