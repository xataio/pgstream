// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

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
	loader         LoadConfigFn
	lastConfig     *Config

	walDataToDDLEvent    func(data *wal.Data) (*wal.DDLEvent, error)
	ddlEventToSchemaDiff func(ddlEvent *wal.DDLEvent) (*wal.SchemaDiff, error)

	validationMode       string
	tableValidationModes map[string]string
	onError              string

	// reloadMu serializes reload attempts and Close. stateMu protects the
	// installed configuration and transformer map while events use them.
	reloadMu sync.Mutex
	stateMu  sync.RWMutex
	closed   bool
}

type ParseFn func(ctx context.Context, rules Rules) (*TransformerMap, error)

// LoadConfigFn reads and parses a candidate configuration. It does not mutate
// the active configuration or build transformers.
type LoadConfigFn func(ctx context.Context) (*Config, error)

type ColumnTransformers map[string]transformers.Transformer

type transformerBuilder interface {
	New(*transformers.Config) (transformers.Transformer, error)
}

type Option func(t *Transformer)

const (
	validationModeStrict     = "strict"
	validationModeRelaxed    = "relaxed"
	validationModeTableLevel = "table_level"

	OnErrorFail        = "fail"
	OnErrorPassThrough = "pass-through"
	OnErrorNull        = "null"
)

var (
	errValidatorRequiredForStrictMode = errors.New("strict validation mode requires a validator function")
	errDDLNotSupportedInStrictMode    = errors.New("DDL events are not supported in strict validation mode, update the transformation rules to include the new table/column before applying DDL changes")

	errReloadNotConfigured      = errors.New("transformer reload requires a configuration loader")
	errTransformerClosed        = errors.New("transformer is closed")
	errReloadEmptyConfig        = errors.New("transformation configuration loader returned no configuration")
	errReloadEmptyTransformer   = errors.New("transformation parser returned no transformer map")
	errReloadFixedSettingChange = errors.New("transformation settings outside the reload scope cannot change")
	errReloadNoRules            = errors.New("transformation reload candidate has no rules")

	ErrUniquenessNotPreserved = errors.New("transformation rules break a unique index")
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
		onError:              cfg.onError(),
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

	t.lastConfig = normalizeConfig(cfg)
	t.tableValidationModes = tableValidationModes(cfg)

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

// WithConfigLoader sets the function used by Reload to read a candidate
// configuration. It does not execute the loader during construction.
func WithConfigLoader(load LoadConfigFn) Option {
	return func(in *Transformer) {
		in.loader = load
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

// Reload loads a candidate configuration, validates and builds it with the
// configured parser, and installs it atomically. Unchanged configurations are
// a no-op. A rejected candidate leaves the active rules and policies in place.
// The boolean reports whether a replacement was installed; if it is true and
// err is non-nil, the new rules are active and cleanup of the retired
// transformers failed.
func (t *Transformer) Reload(ctx context.Context) (changed bool, err error) {
	t.reloadMu.Lock()
	defer t.reloadMu.Unlock()

	t.stateMu.RLock()
	closed := t.closed
	loader := t.loader
	lastConfig := t.lastConfig
	t.stateMu.RUnlock()

	if closed {
		return false, errTransformerClosed
	}
	if loader == nil {
		return false, errReloadNotConfigured
	}

	cfg, err := loader(ctx)
	if err != nil {
		return false, fmt.Errorf("loading transformation configuration: %w", err)
	}
	if cfg == nil {
		return false, errReloadEmptyConfig
	}
	candidateConfig := normalizeConfig(cfg)

	if err := validateReloadableSettings(lastConfig, candidateConfig); err != nil {
		return false, err
	}

	if reflect.DeepEqual(lastConfig, candidateConfig) {
		return false, nil
	}

	if len(candidateConfig.TransformerRules) == 0 {
		return false, errReloadNoRules
	}

	candidateMap, err := t.parser(ctx, Rules{
		Transformers:   cfg.TransformerRules,
		ValidationMode: cfg.validationMode(),
	})
	if err != nil {
		if candidateMap != nil {
			if closeErr := candidateMap.Close(); closeErr != nil {
				return false, errors.Join(err, fmt.Errorf("closing failed transformation candidate: %w", closeErr))
			}
		}
		return false, err
	}
	if candidateMap == nil {
		return false, errReloadEmptyTransformer
	}

	t.stateMu.Lock()
	retiredMap := t.transformerMap
	t.transformerMap = candidateMap
	t.validationMode = candidateConfig.ValidationMode
	t.tableValidationModes = tableValidationModes(candidateConfig)
	t.lastConfig = candidateConfig
	t.stateMu.Unlock()

	if retiredMap != nil {
		if err := retiredMap.Close(); err != nil {
			return true, fmt.Errorf("closing retired transformers: %w", err)
		}
	}

	return true, nil
}

func (t *Transformer) Close() error {
	t.reloadMu.Lock()
	defer t.reloadMu.Unlock()

	t.stateMu.Lock()
	if t.closed {
		t.stateMu.Unlock()
		return nil
	}
	t.closed = true
	transformerMap := t.transformerMap
	t.transformerMap = nil
	t.stateMu.Unlock()

	if transformerMap != nil {
		if err := transformerMap.Close(); err != nil {
			t.logger.Error(err, "closing transformer map")
		}
	}
	return t.processor.Close()
}

func (t *Transformer) applyTransformations(ctx context.Context, event *wal.Event) error {
	if event.Data == nil {
		return nil
	}

	// Hold the read lock for the whole transformation so a concurrent reload
	// cannot retire a transformer while an event is using it.
	t.stateMu.RLock()
	defer t.stateMu.RUnlock()

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
			switch t.onError {
			case OnErrorFail:
				return fmt.Errorf("transforming column %q in %s.%s: %w", col.Name, event.Data.Schema, event.Data.Table, err)
			case OnErrorPassThrough:
				t.logger.Warn(err, "transforming column, passing through original value", loglib.Fields{
					"on_error":    OnErrorPassThrough,
					"column_name": col.Name,
					"schema":      event.Data.Schema,
					"table":       event.Data.Table,
				})
				newValue = col.Value
			default:
				t.logger.Error(err, "transforming column", loglib.Fields{
					"severity":    "DATALOSS",
					"on_error":    OnErrorNull,
					"column_name": col.Name,
					"schema":      event.Data.Schema,
					"table":       event.Data.Table,
				})
				newValue = nil
			}
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
			// if the renamed table is included in the DDL event objects, we can
			// validate the columns in the rename.
			columns := []wal.DDLColumn{}
			tableObj := ddlEvent.GetTableObjectByName(schemaDiff.SchemaName, tableDiff.TableName)
			if tableObj != nil {
				columns = tableObj.Columns
			}

			if err := t.validateTableDDL(schemaDiff.SchemaName, tableDiff.TableNameChange.New, ddlEvent.DDL, columns); err != nil {
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
	if !found || mode == "" {
		// default to strict if not found, as it's safer to fail on unknown tables/columns
		return validationModeStrict
	}
	return mode
}

// validateReloadableSettings rejects candidates that try to change settings
// which are fixed for the lifetime of the transformer. Validation modes and
// transformation rules are reloadable; error handling and rule inference are
// not.
func validateReloadableSettings(active, candidate *Config) error {
	if active == nil || candidate == nil {
		return nil
	}
	if active.OnError != candidate.OnError ||
		active.InferFromSecurityLabels != candidate.InferFromSecurityLabels ||
		active.DumpInferredRules != candidate.DumpInferredRules {
		return fmt.Errorf("%w: on_error, infer_from_security_labels and dump_inferred_rules must not change", errReloadFixedSettingChange)
	}
	return nil
}

// normalizeConfig applies the same defaults used when building rules, so a
// candidate that omits an explicit default compares equal to the initial
// configuration.
func normalizeConfig(cfg *Config) *Config {
	if cfg == nil {
		return nil
	}
	normalized := *cfg
	normalized.ValidationMode = cfg.validationMode()
	normalized.OnError = cfg.onError()
	return &normalized
}

func tableValidationModes(cfg *Config) map[string]string {
	modes := make(map[string]string)
	if cfg == nil || cfg.validationMode() != validationModeTableLevel {
		return modes
	}
	for _, rule := range cfg.TransformerRules {
		key := schemaTableKey(rule.Schema, rule.Table)
		modes[key] = rule.ValidationMode
	}
	return modes
}
