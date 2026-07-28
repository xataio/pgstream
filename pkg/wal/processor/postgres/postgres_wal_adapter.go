// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
)

type walAdapter interface {
	walEventToQueries(ctx context.Context, e *wal.Event) ([]*query, error)
	walEventToMessage(ctx context.Context, e *wal.Event) (*walMessage, error)
	close() error
}

type schemaObserver interface {
	getSchemaInfo(ctx context.Context, schema, table string) (schemaInfo, error)
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
	generatedColumns      map[string]struct{}
	alwaysIdentityColumns map[string]struct{}
	sequenceColumns       map[string]string
	// enumColumns maps quoted column names whose type resolves to a
	// user-defined enum to what a query needs in order to cast them. pgx has no
	// binary codec registered for such database-specific OIDs, so batches
	// touching these columns must use text-format COPY instead of binary COPY
	// (see needsTextCopyForColumns), and the bulk delete path must bind their
	// values as text[] and cast them back on the target (see bindAsText).
	enumColumns map[string]enumColumn
}

// enumColumn describes how to cast a column whose type resolves to a
// user-defined enum. Every field is resolved from the target catalog by
// enumTableColumnsQuery, never from the replication stream, so enumType is safe
// to interpolate into a statement.
type enumColumn struct {
	// enumType is the enum itself, schema-qualified and quoted — resolved
	// through the array element type and the domain base type, since the
	// column's own type name is not always usable in a comparison.
	enumType string
	// isArray reports whether the column resolves to an array of the enum,
	// which compares as a whole array rather than element-wise.
	isArray bool
	// isDomain reports whether the column is a domain over the enum. The enum
	// comparison operators are polymorphic over anyenum, which does not accept
	// a domain, so the column side needs an explicit cast to the base enum —
	// which forfeits use of any plain index on that column.
	isDomain bool
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

func newAdapter(ctx context.Context, logger loglib.Logger, cfg *Config, forCopy bool) (*adapter, error) {
	schemaObserver, err := newPGSchemaObserver(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}

	dmlAdapter, err := newDMLAdapter(cfg.OnConflictAction, forCopy, logger)
	if err != nil {
		return nil, err
	}

	var ddl ddlQueryAdapter
	if !cfg.IgnoreDDL {
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
		info, err := a.schemaObserver.getSchemaInfo(ctx, e.Data.Schema, e.Data.Table)
		if err != nil {
			return nil, err
		}

		qs, err := a.dmlAdapter.walDataToQueries(e.Data, info)
		if err != nil {
			return nil, err
		}

		return qs, nil
	}
}

func (a *adapter) walEventToMessage(ctx context.Context, e *wal.Event) (*walMessage, error) {
	switch {
	case e.Data == nil,
		a.schemaObserver.isMaterializedView(ctx, e.Data.Schema, e.Data.Table):
		return &walMessage{}, nil

	case e.Data.IsDDLEvent():
		ddlEvent, err := a.ddlEventAdapter(e.Data)
		if err != nil {
			return nil, err
		}
		a.schemaObserver.update(ddlEvent)

		if a.ddlAdapter == nil {
			return &walMessage{}, nil
		}

		return &walMessage{data: e.Data, isDDL: true}, nil

	default:
		info, err := a.schemaObserver.getSchemaInfo(ctx, e.Data.Schema, e.Data.Table)
		if err != nil {
			return nil, err
		}

		return &walMessage{
			data:       e.Data,
			schemaInfo: info,
		}, nil
	}
}

func (a *adapter) close() error {
	return a.schemaObserver.close()
}
