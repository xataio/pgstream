// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"errors"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Filter is a processor wrapper that filter table WAL events based on the
// configured table include/exclude lists.
type Filter struct {
	processor          processor.Processor
	includeTableMap    pglib.SchemaTableMap
	excludeTableMap    pglib.SchemaTableMap
	schemaOnlyTableMap pglib.SchemaTableMap
	logger             loglib.Logger
	walEventToDDLEvent func(*wal.Data) (*wal.DDLEvent, error)
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
	// List of tables for which DDL events are allowed but data (DML) events
	// are skipped. Tables should be schema qualified. If no schema is
	// provided, the public schema will be assumed. Wildcards "*" are
	// supported. Tables listed explicitly (no wildcards) in the include list
	// take precedence over a schema-only wildcard match, and the exclude list
	// takes precedence over the schema-only list.
	SchemaOnlyTables []string
}

type Option func(*Filter)

var (
	errIncludeExcludeList     = errors.New("cannot use both include and exclude lists for table filtering")
	errMissingFilteringConfig = errors.New("missing filtering configuration: at least one of the include, exclude or schema-only table lists must be provided")
)

// New will return a filter processor wrapper that will skip WAL events as per
// the configuration provided. Only one of include or exclude table list can be
// provided (not both). The schema-only table list can be combined with either.
func New(
	processor processor.Processor, cfg *Config, opts ...Option,
) (*Filter, error) {
	if len(cfg.ExcludeTables) > 0 && len(cfg.IncludeTables) > 0 {
		return nil, errIncludeExcludeList
	}
	if len(cfg.ExcludeTables) == 0 && len(cfg.IncludeTables) == 0 && len(cfg.SchemaOnlyTables) == 0 {
		return nil, errMissingFilteringConfig
	}

	f := &Filter{
		processor:          processor,
		logger:             loglib.NewNoopLogger(),
		walEventToDDLEvent: wal.WalDataToDDLEvent,
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

	if len(cfg.SchemaOnlyTables) > 0 {
		f.schemaOnlyTableMap, err = pglib.NewSchemaTableMap(cfg.SchemaOnlyTables)
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
		// when only a schema-only list is configured there's no include list
		// to add defaults to, and creating one would switch the filter to
		// include mode, dropping data events for all other tables
		if len(f.includeTableMap) == 0 && len(f.schemaOnlyTableMap) > 0 {
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
	case event.Data.IsDDLEvent():
		// skip events for filtered tables
		if f.skipDDLEvent(event) {
			return nil
		}
	default:
		// data events
		if f.skipEvent(event) {
			if f.logger.IsTraceEnabled() {
				f.logger.Trace("skipping event", loglib.Fields{"schema": event.Data.Schema, "table": event.Data.Table})
			}
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

// skip data (DML) event for table if it's in the exclude or schema-only
// lists, or if it's not in the include table list. Tables listed explicitly
// in the include list take precedence over a schema-only wildcard match.
func (f *Filter) skipEvent(event *wal.Event) bool {
	schema, table := event.Data.Schema, event.Data.Table
	if f.excludeTableMap.ContainsSchemaTable(schema, table) {
		return true
	}
	if f.includeTableMap.ContainsExactSchemaTable(schema, table) {
		return false
	}
	if f.schemaOnlyTableMap.ContainsSchemaTable(schema, table) {
		return true
	}
	if len(f.includeTableMap) > 0 {
		return !f.includeTableMap.ContainsSchemaTable(schema, table)
	}

	return false
}

// skip DDL for table if it's in the exclude list, or if it's not in the
// include table list. Schema-only tables always keep their DDL.
func (f *Filter) skipDDLTable(schema, table string) bool {
	if f.excludeTableMap.ContainsSchemaTable(schema, table) {
		return true
	}
	if f.schemaOnlyTableMap.ContainsSchemaTable(schema, table) {
		return false
	}
	if len(f.includeTableMap) > 0 {
		return !f.includeTableMap.ContainsSchemaTable(schema, table)
	}

	return false
}

// skipDDLEvent skips the whole DDL event if ANY of the tables it references is
// filtered out for DDL. DDL is replayed as a single raw statement on the
// target, so it can't be partially applied; a statement mixing filtered and
// unfiltered tables is dropped.
func (f *Filter) skipDDLEvent(event *wal.Event) bool {
	ddlEvent, err := f.walEventToDDLEvent(event.Data)
	if err != nil {
		// if we can't determine the DDL event, don't filter it out
		f.logger.Error(err, "failed to convert WAL data to DDL event", loglib.Fields{"data": event.Data})
		return false
	}

	tableObjects := append(ddlEvent.GetTableObjects(), ddlEvent.GetTableColumnObjects()...)
	for _, obj := range tableObjects {
		table := obj.GetTable()
		if f.skipDDLTable(obj.Schema, table) {
			f.logger.Trace("skipping DDL event", loglib.Fields{"schema": obj.Schema, "table": table})
			return true
		}
	}
	return false
}
