// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"

	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Transformer is a decorator around a wal processor that transforms wal event
// column values following the configured transformation rules.
type Transformer struct {
	logger         loglib.Logger
	processor      processor.Processor
	transformerMap *TransformerMap
	parser         ParseFn

	walDataToDDLEvent    func(data *wal.Data) (*wal.DDLEvent, error)
	ddlEventToSchemaDiff func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error)

	validationMode       string
	tableValidationModes map[string]string
}

type ParseFn func(ctx context.Context, rules Rules) (*TransformerMap, error)

type ColumnTransformers map[string]transformers.Transformer

type transformerBuilder interface {
	New(*transformers.Config) (transformers.Transformer, error)
}

type Option func(t *Transformer)

const (
	validationModeStrict     = "strict"
	validationModeRelaxed    = "relaxed"
	validationModeTableLevel = "table_level"
)

var (
	errValidatorRequiredForStrictMode = errors.New("strict validation mode requires a validator function")
	errDDLNotSupportedInStrictMode    = errors.New("DDL events are not supported in strict validation mode, update the transformation rules to include the new table/column before applying DDL changes")
)

// New will return a transformer processor wrapper that will transform incoming
// wal event column values as configured by the transformation rules.
func New(ctx context.Context, cfg *Config, processor processor.Processor, builder transformerBuilder, opts ...Option) (*Transformer, error) {
	validationMode := cfg.validationMode()
	t := &Transformer{
		logger:               loglib.NewNoopLogger(),
		processor:            processor,
		parser:               newTransformerParser(builder).parse,
		walDataToDDLEvent:    wal.WalDataToDDLEvent,
		ddlEventToSchemaDiff: wal.DDLEventToSchemaDiff,
		validationMode:       validationMode,
		tableValidationModes: map[string]string{},
	}

	for _, opt := range opts {
		opt(t)
	}

	var err error
	t.transformerMap, err = t.parser(ctx, Rules{
		Transformers:   cfg.TransformerRules,
		ValidationMode: validationMode,
	})
	if err != nil {
		return nil, err
	}

	if validationMode == validationModeTableLevel {
		t.tableValidationModes = make(map[string]string, len(cfg.TransformerRules))
		for _, rule := range cfg.TransformerRules {
			key := schemaTableKey(rule.Schema, rule.Table)
			t.tableValidationModes[key] = rule.ValidationMode
		}
	}

	return t, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(in *Transformer) {
		in.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_transformer",
		})
	}
}

func WithParser(parser ParseFn) Option {
	return func(in *Transformer) {
		in.parser = parser
	}
}

func (t *Transformer) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	err := t.applyTransformations(ctx, event)
	if err != nil {
		return err
	}

	return t.processor.ProcessWALEvent(ctx, event)
}

func (t *Transformer) Name() string {
	return t.processor.Name()
}

func (t *Transformer) Close() error {
	if err := t.transformerMap.Close(); err != nil {
		t.logger.Error(err, "closing transformer map")
	}
	return t.processor.Close()
}

func (t *Transformer) applyTransformations(ctx context.Context, event *wal.Event) error {
	if event.Data == nil {
		return nil
	}

	// even if there are no transformations configured, we still want to
	// validate DDL events in strict mode, so we check for DDL events first
	if event.Data.IsDDLEvent() {
		return t.processDDLEvent(event)
	}

	columnTransformers, found := t.transformerMap.GetActiveColumnTransformers(event.Data.Schema, event.Data.Table)
	if !found || len(columnTransformers) == 0 {
		return nil
	}

	columns := event.Data.Columns
	for i, col := range columns {
		// do not transform nil column values for now
		if col.Value == nil {
			continue
		}

		columnTransformer, found := columnTransformers[col.Name]
		if !found || columnTransformer == nil {
			continue
		}

		var dynamicValues map[string]any
		if columnTransformer.IsDynamic() {
			dynamicValues = t.getDynamicColumnValues(col.Name, event.Data.Columns)
		}

		newValue, err := columnTransformer.Transform(ctx, transformers.NewValue(col.Value, col.Type, dynamicValues))
		if err != nil {
			t.logger.Error(err, "transforming column", loglib.Fields{
				"severity":    "DATALOSS",
				"column_name": col.Name,
				"schema":      event.Data.Schema,
				"table":       event.Data.Table,
			})
			newValue = nil
		}
		// avoid logging large values on the hot path unless trace is enabled
		if t.logger.IsTraceEnabled() {
			t.logger.Trace("applying column transformation", loglib.Fields{"column_name": col.Name, "column_type": col.Type, "new_column_value": newValue})
		}
		columns[i].Value = newValue
	}

	return nil
}

func (t *Transformer) processDDLEvent(event *wal.Event) error {
	if !event.Data.IsDDLEvent() || t.validationMode == validationModeRelaxed {
		return nil
	}

	ddlEvent, err := t.walDataToDDLEvent(event.Data)
	if err != nil {
		return err
	}

	schemaDiff, err := t.ddlEventToSchemaDiff(ddlEvent)
	if err != nil {
		return err
	}

	if schemaDiff.IsEmpty() {
		return nil
	}

	// We want to block DDL changes that are not covered by the existing
	// transformation rules in strict mode

	// Block DDL for new tables even if the validation mode is table level, as
	// we can't determine the validation mode for the new table unless it's
	// explicitly defined. It's safer to block and require it to be defined
	// rather than allowing it by default and potentially missing
	// transformations on it.
	for _, table := range schemaDiff.TablesAdded {
		if err := t.validateTableDDL(schemaDiff.SchemaName, table.GetTable(), ddlEvent.DDL, table.Columns); err != nil {
			return err
		}
	}

	for _, tableDiff := range schemaDiff.TablesChanged {
		if len(tableDiff.ColumnsAdded) == 0 && tableDiff.TableNameChange == nil && len(tableDiff.ColumnsChanged) == 0 {
			continue
		}

		// make sure table renames are captured in the transformation rules
		if tableDiff.TableNameChange != nil {
			if err := t.validateTableDDL(schemaDiff.SchemaName, tableDiff.TableNameChange.New, ddlEvent.DDL, []wal.DDLColumn{}); err != nil {
				return err
			}
		}

		// make sure added columns to existing tables are part of the transformation rules
		if len(tableDiff.ColumnsAdded) > 0 {
			if err := t.validateTableDDL(schemaDiff.SchemaName, tableDiff.TableName, ddlEvent.DDL, tableDiff.ColumnsAdded); err != nil {
				return err
			}
		}

		// make sure column renames are captured in the transformation rules
		for _, colDiff := range tableDiff.ColumnsChanged {
			if colDiff.NameChange == nil {
				continue
			}
			if err := t.validateTableDDL(schemaDiff.SchemaName, tableDiff.TableName, ddlEvent.DDL, []wal.DDLColumn{
				{Name: colDiff.NameChange.New},
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateTableDDL checks if the DDL change is allowed based on the validation
// mode and transformation rules. In strict mode, it blocks any DDL changes to
// tables that are not present in the transformation rules, or that include
// columns that are not present in the transformation rules. In relaxed mode, it
// allows all DDL changes.
func (t *Transformer) validateTableDDL(schema, table, ddl string, columns []wal.DDLColumn) error {
	tableValidationMode := t.getTableValidationMode(schema, table)
	if tableValidationMode == validationModeRelaxed {
		return nil
	}

	// check the table exists in the transformation rules
	columnTransformers, found := t.transformerMap.GetAllColumnTransformers(schema, table)
	if !found {
		t.logger.Error(errDDLNotSupportedInStrictMode, "DDL event includes changes to a table that is not present in the transformation rules, which is not supported in strict validation mode", loglib.Fields{
			"schema": schema,
			"table":  table,
			"query":  ddl,
		})
		return errDDLNotSupportedInStrictMode
	}

	// check all the columns in the table exist in the transformation rules
	for _, col := range columns {
		if _, found := columnTransformers[col.Name]; !found {
			t.logger.Error(errDDLNotSupportedInStrictMode, "DDL event includes columns that are not present in the transformation rules, which is not supported in strict validation mode", loglib.Fields{
				"schema": schema,
				"table":  table,
				"column": col.Name,
				"query":  ddl,
			})
			return errDDLNotSupportedInStrictMode
		}
	}

	return nil
}

func (t *Transformer) getDynamicColumnValues(excludeColName string, columns []wal.Column) map[string]any {
	values := make(map[string]any, len(columns))
	for _, col := range columns {
		if col.Name == excludeColName {
			continue
		}
		values[col.Name] = col.Value
	}
	return values
}

// getTableValidationMode returns the validation mode for the given table. If
// the global validation mode is not table level, it returns the global
// validation mode. If the global validation mode is table level, it returns the
// validation mode for the specific table, or defaults to strict if not found.
func (t *Transformer) getTableValidationMode(schema, table string) string {
	if t.validationMode != validationModeTableLevel {
		return t.validationMode
	}

	key := schemaTableKey(schema, table)
	mode, found := t.tableValidationModes[key]
	if !found {
		// default to strict if not found, as it's safer to fail on unknown tables/columns
		return validationModeStrict
	}
	return mode
}
