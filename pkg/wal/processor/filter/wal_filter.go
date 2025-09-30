// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"errors"

	"github.com/xataio/pgstream/internal/json"
	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/schemalog"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Filter is a processor wrapper that filter table WAL events based on the
// configured table include/exclude lists.
type Filter struct {
	processor       processor.Processor
	includeTableMap pglib.SchemaTableMap
	excludeTableMap pglib.SchemaTableMap
	logger          loglib.Logger
}

type Config struct {
	// List of tables to allow. Tables should be schema qualified. If no schema
	// is provided, the public schema will be assumed. Wildcards "*" are
	// supported.
	IncludeTables []string
	// List of tables to skip. Tables should be schema qualified. If no schema
	// is provided, the public schema will be assumed. Wildcards "*" are
	// supported.
	ExcludeTables []string
}

type Option func(*Filter)

var (
	errIncludeExcludeList     = errors.New("cannot use both include and exclude lists for table filtering")
	errMissingFilteringConfig = errors.New("missing filtering configuration")
)

// New will return a filter processor wrapper that will skip WAL events as per
// the configuration provided. Only one of include or exclude table list can be
// provided (not both).
func New(
	processor processor.Processor, cfg *Config, opts ...Option,
) (*Filter, error) {
	if len(cfg.ExcludeTables) > 0 && len(cfg.IncludeTables) > 0 {
		return nil, errIncludeExcludeList
	}
	if len(cfg.ExcludeTables) == 0 && len(cfg.IncludeTables) == 0 {
		return nil, errMissingFilteringConfig
	}

	f := &Filter{
		processor: processor,
		logger:    loglib.NewNoopLogger(),
	}

	var err error
	if len(cfg.IncludeTables) > 0 {
		f.includeTableMap, err = pglib.NewSchemaTableMap(cfg.IncludeTables)
		if err != nil {
			return nil, err
		}
	}

	if len(cfg.ExcludeTables) > 0 {
		f.excludeTableMap, err = pglib.NewSchemaTableMap(cfg.ExcludeTables)
		if err != nil {
			return nil, err
		}
	}

	for _, opt := range opts {
		opt(f)
	}

	return f, nil
}

func WithLogger(logger loglib.Logger) Option {
	return func(f *Filter) {
		f.logger = loglib.NewLogger(logger).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_filter",
		})
	}
}

// WithDefaultIncludeTables will use the tables on input as default include
// table list or add them to the configured one if provided. If the filter has
// an exclude table list configured, the default include table list won't apply,
// since only one can be provided.
func WithDefaultIncludeTables(tables []string) Option {
	return func(f *Filter) {
		if len(f.excludeTableMap) > 0 {
			return
		}
		for _, table := range tables {
			if err := f.includeTableMap.Add(table); err != nil {
				// should never happen
				panic(err)
			}
		}
	}
}

func (f *Filter) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	switch {
	case event == nil || event.Data == nil:
		// nothing to do, pass it along to the internal processor
	case processor.IsSchemaLogEvent(event.Data):
		// remove filtered tables from schema log events
		f.updateSchemaEvent(event)
	default:
		// data events
		if f.skipEvent(event) {
			f.logger.Trace("skipping event", loglib.Fields{"schema": event.Data.Schema, "table": event.Data.Table})
			return nil
		}
	}

	return f.processor.ProcessWALEvent(ctx, event)
}

func (f *Filter) Name() string {
	return f.processor.Name()
}

func (f *Filter) Close() error {
	return f.processor.Close()
}

// skip event for table if it's not in the include table list or if it's in the exclude one
func (f *Filter) skipEvent(event *wal.Event) bool {
	if len(f.includeTableMap) > 0 {
		return !f.includeTableMap.ContainsSchemaTable(event.Data.Schema, event.Data.Table)
	}
	if len(f.excludeTableMap) > 0 {
		return f.excludeTableMap.ContainsSchemaTable(event.Data.Schema, event.Data.Table)
	}

	return false
}

func (f *Filter) updateSchemaEvent(event *wal.Event) {
	if !event.Data.IsInsert() {
		// we only care about insert actions for schema log events
		return
	}

	logEntry, err := processor.WalDataToLogEntry(event.Data)
	if err != nil {
		f.logger.Error(err, "failed to convert WAL data to log entry", loglib.Fields{"data": event.Data})
		return
	}

	f.filterTablesFromSchema(logEntry.SchemaName, &logEntry.Schema)

	f.logger.Debug("filtered schema", loglib.Fields{
		"schema_name": logEntry.SchemaName,
		"tables":      logEntry.Schema.TableNames(),
	})

	for i, c := range event.Data.Columns {
		if c.Name == "schema" {
			// Marshal the filtered schema to JSON
			schemaBytes, err := json.Marshal(&logEntry.Schema)
			if err != nil {
				f.logger.Error(err, "failed to marshal schema", loglib.Fields{"schema": logEntry.SchemaName})
				return
			}

			var schemaStr string
			err = json.Unmarshal(schemaBytes, &schemaStr)
			if err != nil {
				f.logger.Error(err, "failed to unmarshal schema bytes to string", loglib.Fields{"schema": logEntry.SchemaName})
				return
			}

			// Store the double-encoded value to ensure compatibility with downstream consumers
			// who expect the schema to be a JSON-encoded string. This process ensures that the
			// schema is serialized into a JSON string format that can be safely transmitted
			// and interpreted by downstream systems.
			event.Data.Columns[i].Value = string(schemaStr)
			return
		}
	}
}

func (f *Filter) filterTablesFromSchema(schemaName string, schema *schemalog.Schema) {
	if len(f.includeTableMap) == 0 && len(f.excludeTableMap) == 0 {
		return // no filtering applied, keep all tables
	}
	filteredTables := make([]schemalog.Table, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		switch {
		case len(f.includeTableMap) != 0:
			if f.includeTableMap.ContainsSchemaTable(schemaName, table.Name) {
				filteredTables = append(filteredTables, table)
			}
		case len(f.excludeTableMap) != 0:
			if !f.excludeTableMap.ContainsSchemaTable(schemaName, table.Name) {
				filteredTables = append(filteredTables, table)
			}
		}
	}

	schema.Tables = filteredTables
}
