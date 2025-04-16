// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"

	pglib "github.com/xataio/pgstream/internal/postgres"
	loglib "github.com/xataio/pgstream/pkg/log"
	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
	"github.com/xataio/pgstream/pkg/wal"
	"github.com/xataio/pgstream/pkg/wal/processor"
)

// Transformer is a decorator around a wal processor that transforms wal event
// column values following the configured transformation rules.
type Transformer struct {
	logger         loglib.Logger
	processor      processor.Processor
	transformerMap map[string]ColumnTransformers
	validator      ValidatorFn
}

type ValidatorFn func(ctx context.Context, transformerMap map[string]ColumnTransformers) error

type ColumnTransformers map[string]transformers.Transformer

type Config struct {
	TransformerRules []TableRules
}

type Option func(t *Transformer)

// New will return a transformer processor wrapper that will transform incoming
// wal event column values as configured by the transformation rules.
func New(ctx context.Context, cfg *Config, processor processor.Processor, opts ...Option) (*Transformer, error) {
	transformerMap, err := transformerMapFromRules(cfg.TransformerRules)
	if err != nil {
		return nil, err
	}

	t := &Transformer{
		logger:         loglib.NewNoopLogger(),
		transformerMap: transformerMap,
		processor:      processor,
	}

	for _, opt := range opts {
		opt(t)
	}

	if t.validator != nil {
		if err := t.validator(ctx, t.transformerMap); err != nil {
			return nil, err
		}
	}

	// Noop transformers are only for validation phase. After validation, we can safely delete them.
	t.DeleteNoopTransformers()

	return t, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(in *Transformer) {
		in.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_transformer",
		})
	}
}

func WithValidator(validator ValidatorFn) Option {
	return func(in *Transformer) {
		in.validator = validator
	}
}

func (t *Transformer) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	err := t.applyTransformations(event)
	if err != nil {
		return err
	}

	return t.processor.ProcessWALEvent(ctx, event)
}

func (t *Transformer) Name() string {
	return t.processor.Name()
}

func (t *Transformer) Close() error {
	return nil
}

func (t *Transformer) applyTransformations(event *wal.Event) error {
	if event.Data == nil || len(t.transformerMap) == 0 {
		return nil
	}

	columnTransformers, found := t.transformerMap[schemaTableKey(event.Data.Schema, event.Data.Table)]
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
		if !found {
			continue
		}

		newValue, err := columnTransformer.Transform(t.getTransformValue(&col, event.Data.Columns))
		if err != nil {
			t.logger.Error(err, "transforming column", loglib.Fields{
				"severity":    "DATALOSS",
				"column_name": col.Name,
				"schema":      event.Data.Schema,
				"table":       event.Data.Table,
			})
			newValue = nil
		}
		t.logger.Trace("applying column transformation", loglib.Fields{"column_name": col.Name, "column_value": col.Value, "column_type": col.Type, "new_column_value": newValue})
		columns[i].Value = newValue
	}

	return nil
}

func (t *Transformer) getTransformValue(column *wal.Column, columns []wal.Column) transformers.Value {
	values := make(map[string]any, len(columns)-1)
	for _, col := range columns {
		if col.Name == column.Name {
			continue
		}
		values[col.Name] = col.Value
	}
	return transformers.NewValue(column.Value, values)
}

func (t *Transformer) DeleteNoopTransformers() {
	for schemaTable, columnTransformers := range t.transformerMap {
		for colName, transformer := range columnTransformers {
			if transformers.IsNoopTransformer(transformer) {
				delete(columnTransformers, colName)
			}
		}
		if len(columnTransformers) == 0 {
			delete(t.transformerMap, schemaTable)
		}
	}
}

func schemaTableKey(schema, table string) string {
	return pglib.QuoteQualifiedIdentifier(schema, table)
}

func transformerMapFromRules(rules []TableRules) (map[string]ColumnTransformers, error) {
	var err error
	transformerMap := map[string]ColumnTransformers{}
	for _, table := range rules {
		schemaTableTransformers := make(map[string]transformers.Transformer)
		transformerMap[schemaTableKey(table.Schema, table.Table)] = schemaTableTransformers
		for colName, transformerRules := range table.ColumnRules {
			schemaTableTransformers[colName], err = builder.New(transformerRulesToConfig(transformerRules))
			if err != nil {
				return nil, err
			}
		}
	}
	return transformerMap, nil
}

func transformerRulesToConfig(rules TransformerRules) *transformers.Config {
	return &transformers.Config{
		Name:              transformers.TransformerType(rules.Name),
		Parameters:        rules.Parameters,
		DynamicParameters: rules.DynamicParameters,
	}
}
