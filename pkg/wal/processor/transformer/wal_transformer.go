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

type ValidatorFn func(ctx context.Context, schemaTable string, transformers ColumnTransformers, columns []string) error

type ColumnTransformers map[string]transformers.Transformer

type Config struct {
	TransformerRules []TableRules
}

type Option func(t *Transformer)

// New will return a transformer processor wrapper that will transform incoming
// wal event column values as configured by the transformation rules.
func New(ctx context.Context, cfg *Config, processor processor.Processor, opts ...Option) (*Transformer, error) {
	t := &Transformer{
		logger:    loglib.NewNoopLogger(),
		processor: processor,
	}

	for _, opt := range opts {
		opt(t)
	}

	transformerMap, err := transformerMapFromRules(cfg.TransformerRules, t.validator)
	if err != nil {
		return nil, err
	}
	t.transformerMap = transformerMap

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

func schemaTableKey(schema, table string) string {
	return pglib.QuoteQualifiedIdentifier(schema, table)
}

func transformerMapFromRules(rules []TableRules, validator ValidatorFn) (map[string]ColumnTransformers, error) {
	var err error
	transformerMap := map[string]ColumnTransformers{}
	for _, table := range rules {
		schemaTableTransformers := make(map[string]transformers.Transformer)
		transformerMap[schemaTableKey(table.Schema, table.Table)] = schemaTableTransformers
		columnNames := make([]string, 0, len(table.ColumnRules))
		for colName, transformerRules := range table.ColumnRules {
			cfg := transformerRulesToConfig(transformerRules)
			if cfg.Name == "" || cfg.Name == "noop" {
				// noop transformer, skip
				continue
			}
			schemaTableTransformers[colName], err = builder.New(cfg)
			if err != nil {
				return nil, err
			}
			columnNames = append(columnNames, colName)
		}

		if validator != nil {
			if err = validator(context.Background(), schemaTableKey(table.Schema, table.Table), schemaTableTransformers, columnNames); err != nil {
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
