// SPDX-License-Identifier: Apache-2.0

package translator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	schemalogpg "github.com/xataio/pgstream/pkg/schemalog/postgres"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Translator is a decorator around a processor that populates the wal data with
// the schemalog entry for the relevant schema (such as pgstream ids). This
// allows following processors to have more information for processing the event
// effectively.
type Translator struct {
	logger               loglib.Logger
	processor            processor.Processor
	walToLogEntryAdapter walToLogEntryAdapter
	skipSchema           schemaFilter
	schemaLogStore       schemalog.Store
	idFinder             columnFinder
	versionFinder        columnFinder
}

type walToLogEntryAdapter func(*wal.Data) (*schemalog.LogEntry, error)

type Config struct {
	Store schemalogpg.Config
}

// configurable filters that allow the user of this library to have flexibility
// when processing and translating the wal event data
type (
	schemaFilter func(string) bool
	columnFinder func(*schemalog.Column, *schemalog.Table) bool
)

type Option func(t *Translator)

// New will return a translator processor wrapper that will inject pgstream
// metadata into the wal data events before passing them over the processor on
// input. By default, all schemas are processed.
func New(cfg *Config, p processor.Processor, opts ...Option) (*Translator, error) {
	var schemaLogStore schemalog.Store
	var err error
	schemaLogStore, err = schemalogpg.NewStore(context.Background(), cfg.Store)
	if err != nil {
		return nil, fmt.Errorf("create schema log postgres store: %w", err)
	}
	schemaLogStore = schemalog.NewStoreCache(schemaLogStore)

	t := &Translator{
		logger:               loglib.NewNoopLogger(),
		processor:            p,
		schemaLogStore:       schemaLogStore,
		walToLogEntryAdapter: processor.WalDataToLogEntry,
		// by default all schemas are processed
		skipSchema: func(s string) bool { return false },
		// by default we look for the primary key to use as identity column
		idFinder: primaryKeyFinder,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func WithIDFinder(idFinder columnFinder) Option {
	return func(t *Translator) {
		t.idFinder = idFinder
	}
}

func WithVersionFinder(versionFinder columnFinder) Option {
	return func(t *Translator) {
		t.versionFinder = versionFinder
	}
}

func WithSkipSchema(skipSchema schemaFilter) Option {
	return func(t *Translator) {
		t.skipSchema = skipSchema
	}
}

func WithLogger(l loglib.Logger) Option {
	return func(t *Translator) {
		t.logger = loglib.NewLogger(l)
	}
}

func (t *Translator) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if event.Data == nil {
		return t.processor.ProcessWALEvent(ctx, event)
	}

	data := event.Data
	if t.skipSchema(data.Schema) {
		return nil
	}

	switch {
	case isSchemaLogSchema(data.Schema):
		// this happens when a write occurs to the `table_ids` table or if the
		// schema log table rows are acked
		if !isSchemaLogTable(data.Table) || !data.IsInsert() {
			return nil
		}
		logEntry, err := t.walToLogEntryAdapter(data)
		if err != nil {
			return err
		}

		if t.skipSchema(logEntry.SchemaName) {
			return nil
		}

		if err := t.schemaLogStore.Ack(ctx, logEntry); err != nil {
			t.logger.Error(err, "ack schema log")
		}
	default:
		// by default, we translate columns and pass on the event. If we fail to
		// translate, log a DATALOSS severity error and continue processing the
		// event without translating
		if err := t.translate(ctx, data); err != nil {
			// for now, do not consider events missing id/version fields to be
			// data loss, since we don't expect to replicate tables that do not
			// have these fields
			if errors.Is(err, processor.ErrIDNotFound) || errors.Is(err, processor.ErrVersionNotFound) {
				t.logger.Warn(err, "ignoring event", loglib.Fields{
					"schema": data.Schema,
					"table":  data.Table,
				})
				return nil
			} else {
				t.logger.Error(err, "", loglib.Fields{
					"severity": "DATALOSS",
					"schema":   data.Schema,
					"table":    data.Table,
				})
			}
		}
	}

	return t.processor.ProcessWALEvent(ctx, event)
}

func (t *Translator) Close() error {
	return t.schemaLogStore.Close()
}

func (t *Translator) translate(ctx context.Context, data *wal.Data) error {
	if data == nil {
		return nil
	}

	logEntry, err := t.schemaLogStore.Fetch(ctx, data.Schema, true)
	if err != nil {
		// if schema does NOT exist in the log, skip the event translation.
		if errors.Is(err, schemalog.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to retrieve schema for translate %w", err)
	}

	table := logEntry.GetTableByName(data.Table)
	if table == nil {
		return processor.ErrTableNotFound
	}

	if err = t.fillEventMetadata(data, logEntry, table); err != nil {
		return fmt.Errorf("failed to fill event metadata: %w", err)
	}

	if err = t.translateColumnNames(data, table); err != nil {
		return fmt.Errorf("failed to translate column names: %w", err)
	}

	return nil
}

// fillEventMetadata will update the event on input with the pgstream ids for
// the table and the internal id/version columns. It will return an error if the
// id column is not found, or if a version finder was set but no version was
// found.
func (t *Translator) fillEventMetadata(event *wal.Data, log *schemalog.LogEntry, tbl *schemalog.Table) error {
	event.Metadata.SchemaID = log.ID
	event.Metadata.TablePgstreamID = tbl.PgstreamID

	foundID, foundVersion := false, false
	for i := range tbl.Columns {
		col := &tbl.Columns[i]
		if t.idFinder(col, tbl) {
			foundID = true
			event.Metadata.InternalColIDs = append(event.Metadata.InternalColIDs, col.PgstreamID)
			continue
		}

		if t.versionFinder != nil && t.versionFinder(col, tbl) && !foundVersion {
			foundVersion = true
			event.Metadata.InternalColVersion = col.PgstreamID
			continue
		}
	}

	switch {
	case !foundID:
		// the id is required
		return fmt.Errorf("table [%s]: %w", tbl.Name, processor.ErrIDNotFound)
	case t.versionFinder != nil && !foundVersion:
		// if there's a version finder and the column wasn't found, return an error
		return fmt.Errorf("table [%s]: %w", tbl.Name, processor.ErrVersionNotFound)
	}

	return nil
}

// translateColumnNames will replace the existing column ids from the wal data
// event with the pgstream ids. It will error if the column on input does not
// exist in the relevant schemalog entry.
func (t *Translator) translateColumnNames(event *wal.Data, schemaTable *schemalog.Table) error {
	for i, col := range event.Columns {
		schemaCol := schemaTable.GetColumnByName(col.Name)
		if schemaCol == nil {
			return fmt.Errorf("failed to find column in table %s: %w", schemaTable.Name, processor.ErrColumnNotFound)
		}
		event.Columns[i].ID = schemaCol.PgstreamID
	}

	for i, col := range event.Identity { // should only be filled if event.Type is "D" or "U"
		schemaCol := schemaTable.GetColumnByName(col.Name)
		if schemaCol == nil {
			return fmt.Errorf("failed to find column in table: %s: %w", schemaTable.Name, processor.ErrColumnNotFound)
		}
		event.Identity[i].ID = schemaCol.PgstreamID
	}
	return nil
}

func isSchemaLogSchema(schema string) bool {
	return schema == schemalog.SchemaName
}

func isSchemaLogTable(table string) bool {
	return table == schemalog.TableName
}

// primaryKeyFinder will flag as identity column the primary key of the table on
// input. If there's no primary key defined for the table, it will use the first
// (alphabetically ordered) not null unique column in the table. If there's no
// unique not null columns or primary keys, then no column will be flagged as
// identity. Composite primary keys are not currently supported, and will not be
// flagged as identity either.
func primaryKeyFinder(c *schemalog.Column, tbl *schemalog.Table) bool {
	if c == nil || tbl == nil {
		return false
	}

	switch len(tbl.PrimaryKeyColumns) {
	case 0:
		// If no primary key present, choose a not nullable unique column if it
		// exists
		notNullUniqueCol := tbl.GetFirstUniqueNotNullColumn()
		if notNullUniqueCol == nil {
			return false
		}

		return c.Name == notNullUniqueCol.Name
	default:
		// single or composite primary key
		return slices.Contains(tbl.PrimaryKeyColumns, c.Name)
	}
}
