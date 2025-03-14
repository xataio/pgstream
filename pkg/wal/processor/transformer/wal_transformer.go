// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"

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
	transformerMap map[string]columnTransformers
}

type columnTransformers map[string]transformers.Transformer

type Config struct {
	TransformerRulesFile string
}

type Option func(t *Transformer)

// New will return a transformer processor wrapper that will transform incoming
// wal event column values as configured by the transformation rules.
func New(cfg *Config, processor processor.Processor, opts ...Option) (*Transformer, error) {
	rules, err := readRulesFromFile(cfg.TransformerRulesFile)
	if err != nil {
		return nil, err
	}

	transformerMap, err := transformerMapFromRules(rules)
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

	return t, nil
}

func WithLogger(l loglib.Logger) Option {
	return func(in *Transformer) {
		in.logger = loglib.NewLogger(l).WithFields(loglib.Fields{
			loglib.ModuleField: "wal_transformer",
		})
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

		newValue, err := columnTransformer.Transform(col.Value)
		if err != nil {
			t.logger.Error(err, "transforming column", loglib.Fields{
				"severity":    "DATALOSS",
				"column_name": col.Name,
				"schema":      event.Data.Schema,
				"table":       event.Data.Table,
			})
			newValue = nil
		}
		t.logger.Trace("applying column transformation", loglib.Fields{"column_name": col.Name, "column_value": col.Value, "new_column_value": newValue})
		columns[i].Value = newValue
	}

	return nil
}

func schemaTableKey(schema, table string) string {
	return schema + "/" + table
}

func transformerMapFromRules(rules *Rules) (map[string]columnTransformers, error) {
	var err error
	transformerMap := map[string]columnTransformers{}
	for _, table := range rules.Transformers {
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
		Name:       transformers.TransformerType(rules.Name),
		Generator:  transformers.GeneratorType(rules.Generator),
		Parameters: rules.Parameters,
	}
}
