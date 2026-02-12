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

func (p *transformerParser) parse(_ context.Context, rules Rules) (*TransformerMap, error) {
	transformerMap := NewTransformerMap()
	for _, table := range rules.Transformers {
		if table.ValidationMode == validationModeStrict {
			return nil, errValidatorRequiredForStrictMode
		}

		for colName, transformerRules := range table.ColumnRules {
			cfg := transformerRulesToConfig(transformerRules)
			if cfg.Name == "" || cfg.Name == "noop" {
				transformerMap.AddNoopTransformer(table.Schema, table.Table, colName)
				continue
			}

			transformer, err := p.builder.New(cfg)
			if err != nil {
				return nil, err
			}
			transformerMap.AddActiveTransformer(table.Schema, table.Table, colName, transformer)
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
