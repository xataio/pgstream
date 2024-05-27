// SPDX-License-Identifier: Apache-2.0

package translator

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
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
	processor            processor.Processor
	walToLogEntryAdapter walToLogEntryAdapter
	skipSchema           schemaFilter
	schemaLogStore       schemalog.Store
	idFinder             columnFinder
	versionFinder        columnFinder
}

type walToLogEntryAdapter func(*wal.Data) (*schemalog.LogEntry, error)

type Config struct {
	Postgres schemalogpg.Config
}

// configurable filters that allow the user of this library to have flexibility
// when processing and translating the wal event data
type (
	schemaFilter func(string) bool
	columnFinder func(*schemalog.Column) bool
)

// NewTranslator will return a translator processor wrapper that will inject
// pgstream metadata into the wal data events before passing them over the
// processor on input.
func NewTranslator(cfg Config, p processor.Processor, skipSchema schemaFilter, idFinder, versionFinder columnFinder) (*Translator, error) {
	var schemaLogStore schemalog.Store
	var err error
	schemaLogStore, err = schemalogpg.NewStore(context.Background(), cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("create schema log postgres store: %w", err)
	}
	schemaLogStore = schemalog.NewStoreCache(schemaLogStore)

	return &Translator{
		processor:            p,
		schemaLogStore:       schemaLogStore,
		walToLogEntryAdapter: processor.WalDataToLogEntry,
		skipSchema:           skipSchema,
		idFinder:             idFinder,
		versionFinder:        versionFinder,
	}, nil
}

func (t *Translator) ProcessWALEvent(ctx context.Context, data *wal.Data) error {
	if t.skipSchema(data.Schema) {
		return nil
	}

	switch {
	case isSchemaLogSchema(data.Schema):
		// this happens when a write occurs to the `table_ids` table or if the
		// schema log table rows are acked
		if !isSchemaLogTable(data.Table) || data.Action != "I" {
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
			log.Ctx(ctx).Error().Err(err).Msgf("ack schema log")
		}
	default:
		// by default, we translate columns and pass on the event. If we fail to
		// translate, log a DATALOSS severity error and continue processing the
		// event without translating
		if err := t.translate(ctx, data); err != nil {
			// for now, do not consider events missing id/version fields to be data loss,
			// since we don't expect to replicate tables that do not have these
			// fields (opensearch requires them)
			if errors.Is(err, processor.ErrIDNotFound) || errors.Is(err, processor.ErrVersionNotFound) {
				log.Ctx(ctx).Debug().
					Str("schema", data.Schema).
					Str("table", data.Table).
					Msgf("ignoring event: %v", err)
				return nil
			} else {
				log.Ctx(ctx).Error().
					Str("severity", "DATALOSS").
					Str("error", err.Error()).
					Str("schema", data.Schema).
					Str("table", data.Table)
			}
		}
	}

	return t.processor.ProcessWALEvent(ctx, data)
}

func (t *Translator) Close() error {
	return t.processor.Close()
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
// the table and the internal id/version columns. It will return an error if
// either of the columns is not found.
func (t *Translator) fillEventMetadata(event *wal.Data, log *schemalog.LogEntry, tbl *schemalog.Table) error {
	event.Metadata.SchemaID = log.ID
	event.Metadata.TablePgstreamID = tbl.PgstreamID

	foundID, foundVersion := false, false
	for i := range tbl.Columns {
		col := &tbl.Columns[i]
		if t.idFinder(col) && !foundID {
			foundID = true
			event.Metadata.InternalColID = col.PgstreamID
			continue
		}

		if t.versionFinder(col) && !foundVersion {
			foundVersion = true
			event.Metadata.InternalColVersion = col.PgstreamID
			continue
		}
	}

	switch {
	case !foundID:
		return fmt.Errorf("table [%s]: %w", tbl.Name, processor.ErrIDNotFound)
	case !foundVersion:
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
