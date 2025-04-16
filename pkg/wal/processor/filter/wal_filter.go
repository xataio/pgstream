// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"errors"
	"strings"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Filter is a processor wrapper that filter table WAL events based on the
// configured table include/exclude lists.
type Filter struct {
	processor       processor.Processor
	includeTableMap schemaTableMap
	excludeTableMap schemaTableMap
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

type schemaTableMap map[string]map[string]struct{}

type Option func(*Filter)

var (
	errIncludeExcludeList     = errors.New("cannot use both include and exclude lists for table filtering")
	errMissingFilteringConfig = errors.New("missing filtering configuration")
	errInvalidTableName       = errors.New("invalid table name format")
)

const (
	wildcard     = "*"
	publicSchema = "public"
)

// New will return a filter processor wrapper that will skip WAL events as per
// the configuration provided. Only one of include or exclude table list can be
// provided (not both).
func New(processor processor.Processor, cfg *Config, opts ...Option) (*Filter, error) {
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
		f.includeTableMap, err = parseTables(cfg.IncludeTables)
		if err != nil {
			return nil, err
		}
	}

	if len(cfg.ExcludeTables) > 0 {
		f.excludeTableMap, err = parseTables(cfg.ExcludeTables)
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
			schema, table, err := parseTableName(table)
			// should never happen
			if err != nil {
				panic(err)
			}
			f.includeTableMap.add(schema, table)
		}
	}
}

func (f *Filter) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	if f.skipEvent(event) {
		f.logger.Trace("skipping event", loglib.Fields{"schema": event.Data.Schema, "table": event.Data.Table})
		return nil
	}

	return f.processor.ProcessWALEvent(ctx, event)
}

func (f *Filter) Name() string {
	return f.processor.Name()
}

// skip event for table if it's not in the include table list or if it's in the exclude one
func (f *Filter) skipEvent(event *wal.Event) bool {
	if event == nil || event.Data == nil {
		return false
	}

	if len(f.includeTableMap) > 0 {
		return !f.includeTableMap.containsSchemaTable(event.Data.Schema, event.Data.Table)
	}
	if len(f.excludeTableMap) > 0 {
		return f.excludeTableMap.containsSchemaTable(event.Data.Schema, event.Data.Table)
	}

	return false
}

func (t schemaTableMap) containsSchemaTable(schema, table string) bool {
	if len(t) == 0 {
		return false
	}

	tables := t.getSchemaTables(schema)
	if len(tables) == 0 {
		return false
	}

	_, found := tables[table]
	_, wildcardFound := tables[wildcard]
	return found || wildcardFound
}

func (t schemaTableMap) getSchemaTables(schema string) map[string]struct{} {
	tables, found := t[schema]
	if !found {
		return t[wildcard]
	}

	// make sure it's merged with the wildcard schema tables if any
	for table := range t[wildcard] {
		tables[table] = struct{}{}
	}

	return tables
}

func (t schemaTableMap) add(schema, table string) {
	_, found := t[schema]
	if !found {
		t[schema] = map[string]struct{}{}
	}
	t[schema][table] = struct{}{}
}

func parseTables(tables []string) (schemaTableMap, error) {
	schemaTablesMap := make(schemaTableMap, len(tables))
	for _, table := range tables {
		schemaName, tableName, err := parseTableName(table)
		if err != nil {
			return nil, err
		}
		if _, found := schemaTablesMap[schemaName]; !found {
			schemaTablesMap[schemaName] = make(map[string]struct{})
		}
		schemaTablesMap[schemaName][tableName] = struct{}{}
	}
	return schemaTablesMap, nil
}

func parseTableName(qualifiedTableName string) (string, string, error) {
	parts := strings.Split(qualifiedTableName, ".")
	switch len(parts) {
	case 1:
		return publicSchema, parts[0], nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", errInvalidTableName
	}
}
