// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"

	"github.com/xataio/pgstream/pkg/transformers"
)

type transformerParser struct {
	builder transformerBuilder
}

func newTransformerParser(b transformerBuilder) *transformerParser {
	return &transformerParser{
		builder: b,
	}
}

func (p *transformerParser) parse(_ context.Context, rules Rules) (map[string]ColumnTransformers, error) {
	var err error
	transformerMap := map[string]ColumnTransformers{}
	for _, table := range rules.Transformers {
		if table.ValidationMode == validationModeStrict {
			return nil, errValidatorRequiredForStrictMode
		}
		schemaTableTransformers := make(map[string]transformers.Transformer)
		transformerMap[schemaTableKey(table.Schema, table.Table)] = schemaTableTransformers
		for colName, transformerRules := range table.ColumnRules {
			cfg := transformerRulesToConfig(transformerRules)
			if cfg.Name == "" || cfg.Name == "noop" {
				// noop transformer, skip
				continue
			}
			if schemaTableTransformers[colName], err = p.builder.New(cfg); err != nil {
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
