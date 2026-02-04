// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"errors"

	pglib "github.com/xataio/pgstream/internal/postgres"
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
	transformerMap map[string]ColumnTransformers
	parser         ParseFn
}

type ParseFn func(ctx context.Context, rules Rules) (map[string]ColumnTransformers, error)

type ColumnTransformers map[string]transformers.Transformer

type transformerBuilder interface {
	New(*transformers.Config) (transformers.Transformer, error)
}

type Config struct {
	InferFromSecurityLabels bool
	DumpInferredRules       bool
	TransformerRules        []TableRules
	ValidationMode          string
}

type Option func(t *Transformer)

const validationModeStrict = "strict"

var errValidatorRequiredForStrictMode = errors.New("strict validation mode requires a validator function")

// New will return a transformer processor wrapper that will transform incoming
// wal event column values as configured by the transformation rules.
func New(ctx context.Context, cfg *Config, processor processor.Processor, builder transformerBuilder, opts ...Option) (*Transformer, error) {
	t := &Transformer{
		logger:    loglib.NewNoopLogger(),
		processor: processor,
		parser:    newTransformerParser(builder).parse,
	}

	for _, opt := range opts {
		opt(t)
	}

	var err error
	t.transformerMap, err = t.parser(ctx, Rules{
		Transformers:   cfg.TransformerRules,
		ValidationMode: cfg.ValidationMode,
	})
	if err != nil {
		return nil, err
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
	for _, transformer := range t.transformerMap {
		for _, colTransformer := range transformer {
			if err := colTransformer.Close(); err != nil {
				t.logger.Error(err, "closing transformer")
			}
		}
	}
	return t.processor.Close()
}

func (t *Transformer) applyTransformations(ctx context.Context, event *wal.Event) error {
	if event.Data == nil || len(t.transformerMap) == 0 || event.Data.IsDDLEvent() {
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

func schemaTableKey(schema, table string) string {
	return pglib.QuoteQualifiedIdentifier(schema, table)
}

func (c *Config) HasNoRules() bool {
	return c == nil || len(c.TransformerRules) == 0
}
