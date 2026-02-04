// SPDX-License-Identifier: Apache-2.0

package injector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	pginstrumentation "github.com/xataio/pgstream/internal/postgres/instrumentation"
	synclib "github.com/xataio/pgstream/internal/sync"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Injector is a decorator around a wal processor that injects the wal metadata
// with the schemalog entry for the relevant schema. This allows following
// processors to have more information for processing the event effectively.
type Injector struct {
	logger               loglib.Logger
	processor            processor.Processor
	walToDDLEventAdapter walToDDLEventAdapter
	querier              pglib.Querier
	deserializer         func([]byte, any) error

	tableCache *synclib.Map[string, *wal.DDLObject]
}

type (
	walToDDLEventAdapter func(*wal.Data) (*wal.DDLEvent, error)
)

type Config struct {
	URL string
}

type Option func(t *Injector)

// New will return an injector processor wrapper that will inject pgstream
// metadata into the wal data events before passing them over to the processor
// on input. By default, all schemas are processed and the pgstream identity
// will be the primary key/not null unique column if present.
func New(ctx context.Context, cfg *Config, p processor.Processor, opts ...Option) (*Injector, error) {
	connPool, err := pglib.NewConnPool(ctx, cfg.URL)
	if err != nil {
		return nil, err
	}

	i := &Injector{
		logger:               loglib.NewNoopLogger(),
		processor:            p,
		walToDDLEventAdapter: wal.WalDataToDDLEvent,
		tableCache:           synclib.NewMap[string, *wal.DDLObject](),
		querier:              connPool,
		deserializer:         json.Unmarshal,
	}

	for _, opt := range opts {
		opt(i)
	}

	return i, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(in *Injector) {
		in.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_injector",
		})
	}
}

func WithInstrumentation(instr *otel.Instrumentation) Option {
	return func(in *Injector) {
		var err error
		in.querier, err = pginstrumentation.NewQuerier(in.querier, instr)
		if err != nil {
			// should never happen
			panic(err)
		}
	}
}

// ProcessWALEvent populates the metadata of the wal event on input, before
// passing it over to the configured wal processor.
func (in *Injector) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if event.Data == nil {
		return in.processor.ProcessWALEvent(ctx, event)
	}

	data := event.Data

	switch {
	case data.IsDDLEvent():
		ddlEvent, err := in.walToDDLEventAdapter(data)
		if err != nil {
			return err
		}

		in.updateTableCache(ddlEvent)

	default:
		// by default, we inject the metadata and pass on the event. If we fail
		// to inject metadata, log a DATALOSS severity error and continue
		// processing the event without it
		if err := in.inject(ctx, data); err != nil {
			// for now, do not consider events missing id field to be
			// data loss, since we don't expect to replicate tables that do not
			// have these fields
			if errors.Is(err, processor.ErrIDNotFound) {
				in.logger.Debug(fmt.Sprintf("ignoring event: %v", err), loglib.Fields{
					"schema": data.Schema,
					"table":  data.Table,
				})
				// treat the event as a keep alive, so that the event data is
				// ignored, but the commit position is checkpointed
				event.Data = nil
			} else {
				in.logger.Error(err, "injecting event metadata", loglib.Fields{
					"severity": "DATALOSS",
					"schema":   data.Schema,
					"table":    data.Table,
				})
			}
		}
	}

	return in.processor.ProcessWALEvent(ctx, event)
}

func (in *Injector) Name() string {
	return in.processor.Name()
}

func (in *Injector) Close() error {
	if in.querier != nil {
		in.querier.Close(context.Background())
	}

	return nil
}

func (in *Injector) updateTableCache(ddlEvent *wal.DDLEvent) {
	switch ddlEvent.CommandTag {
	case "DROP TABLE":
		tableObjects := ddlEvent.GetTableObjects()
		// remove from cache any table that was dropped
		for _, tableObj := range tableObjects {
			qualifiedTableName := tableObj.Schema + "." + tableObj.GetTable()
			if _, found := in.tableCache.Get(qualifiedTableName); found {
				in.tableCache.Delete(qualifiedTableName)
			}
		}
	default:
		tableObjects := append(ddlEvent.GetTableObjects(), ddlEvent.GetTableColumnObjects()...)
		in.logger.Debug("updating table cache with table objects", loglib.Fields{
			"table_objects": tableObjects,
		})
		// update the table cache with any new/updated table
		for _, tableObj := range tableObjects {
			qualifiedTableName := tableObj.Schema + "." + tableObj.GetTable()
			in.tableCache.Set(qualifiedTableName, &tableObj)
		}
	}
}

func (in *Injector) inject(ctx context.Context, data *wal.Data) error {
	tableObject, err := in.getTableObject(ctx, data.Schema, data.Table)
	if err != nil {
		return fmt.Errorf("failed to get table object: %w", err)
	}
	if tableObject == nil {
		return processor.ErrTableNotFound
	}

	if err := in.fillEventMetadata(data, tableObject); err != nil {
		return fmt.Errorf("failed to fill event metadata from table object: %w", err)
	}

	if err := in.injectColumnIDs(data, tableObject); err != nil {
		return fmt.Errorf("failed to inject column ids from table object: %w", err)
	}

	return nil
}

// fillEventMetadata will update the event on input with the
// pgstream ids for the table and the internal id column. It will return an
// error if the id column is not found.
func (in *Injector) fillEventMetadata(event *wal.Data, tbl *wal.DDLObject) error {
	event.Metadata.TablePgstreamID = tbl.PgstreamID

	identityColumn := getIdentityColumn(tbl)
	if identityColumn == nil {
		// the id is required
		return fmt.Errorf("table [%s]: %w", tbl.Identity, processor.ErrIDNotFound)
	}

	event.Metadata.InternalColIDs = append(event.Metadata.InternalColIDs, identityColumn.GetColumnPgstreamID(tbl.PgstreamID))

	return nil
}

// injectColumnIDs will replace the existing column ids from the
// wal data event with the pgstream ids. It will error if the column on input
// does not exist.
func (in *Injector) injectColumnIDs(event *wal.Data, tbl *wal.DDLObject) error {
	var err error
	for i, col := range event.Columns {
		schemaCol, found := tbl.GetColumnByName(col.Name)
		if !found {
			in.logger.Debug("column not found in table object", loglib.Fields{
				"column": col.Name,
				"table":  tbl.Identity,
				"object": tbl,
			})
			err = errors.Join(err, fmt.Errorf("failed to find column %q in table %s: %w", col.Name, tbl.Identity, processor.ErrColumnNotFound))
			continue
		}
		event.Columns[i].ID = schemaCol.GetColumnPgstreamID(tbl.PgstreamID)
	}

	for i, col := range event.Identity { // should only be filled if event.Type is "D" or "U"
		schemaCol, found := tbl.GetColumnByName(col.Name)
		if !found {
			in.logger.Debug("column not found in table object", loglib.Fields{
				"column": col.Name,
				"table":  tbl.Identity,
				"object": tbl,
			})
			err = errors.Join(err, fmt.Errorf("failed to find column %q in table %s: %w", col.Name, tbl.Identity, processor.ErrColumnNotFound))
			continue
		}
		event.Identity[i].ID = schemaCol.GetColumnPgstreamID(tbl.PgstreamID)
	}
	return err
}

const tableObjectQuery = `
		SELECT jsonb_build_object(
			'type', 'table',
			'identity', n.nspname || '.' || c.relname,
			'schema', n.nspname,
			'oid', c.oid::text,
			'pgstream_id', COALESCE(t.id::text, pgstream.create_table_mapping(c.oid)::text)
		) || pgstream.get_table_metadata(c.oid)
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pgstream.table_ids t ON c.oid = t.oid
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')
	`

func (in *Injector) getTableObject(ctx context.Context, schema, table string) (*wal.DDLObject, error) {
	qualifiedName := schema + "." + table
	tableObject, found := in.tableCache.Get(qualifiedName)
	if found {
		return tableObject, nil
	}

	var tableObjJSON []byte
	dest := []any{&tableObjJSON}
	err := in.querier.QueryRow(ctx, dest, tableObjectQuery, schema, table)
	if err != nil {
		if errors.Is(err, pglib.ErrNoRows) {
			return nil, processor.ErrTableNotFound
		}
		return nil, fmt.Errorf("failed to query table metadata: %w", err)
	}

	// Parse the complete DDLObject JSON
	var tableObj wal.DDLObject
	if err := in.deserializer(tableObjJSON, &tableObj); err != nil {
		return nil, fmt.Errorf("failed to parse table object: %w", err)
	}

	// Cache it for future use
	in.tableCache.Set(qualifiedName, &tableObj)

	return &tableObj, nil
}

func getIdentityColumn(tbl *wal.DDLObject) *wal.DDLColumn {
	hasPrimaryKeys := len(tbl.PrimaryKeyColumns) > 0

	// sort columns by attnum to have a deterministic order if no primary key is
	// set when selecting the unique not null identity column.
	if !hasPrimaryKeys {
		sort.Slice(tbl.Columns, func(i, j int) bool {
			return tbl.Columns[i].Attnum < tbl.Columns[j].Attnum
		})
	}

	// Flag as identity column the primary key of the table on input. If there's
	// no primary key defined for the table, it will use the first
	// (alphabetically ordered) not null unique column in the table. If there's
	// no unique not null columns or primary keys, then no column will be
	// flagged as identity. Composite primary keys are not currently supported,
	// and will not be flagged as identity either.
	for _, col := range tbl.Columns {
		switch {
		case hasPrimaryKeys:
			if slices.Contains(tbl.PrimaryKeyColumns, col.Name) {
				return &col
			}
		case col.Unique && !col.Nullable:
			// only use the first unique not null column as identity
			return &col
		}
	}

	return nil
}
