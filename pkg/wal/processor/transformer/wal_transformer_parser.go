// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"context"
	"fmt"

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
				return nil, columnRuleError(table.Schema, table.Table, colName, err)
			}
			transformerMap.AddActiveTransformer(table.Schema, table.Table, colName, transformer)
		}
	}
	return transformerMap, nil
}

// columnRuleError attributes err to the column rule that produced it, using the
// phrasing every check in the rule parsers shares.
func columnRuleError(schema, table, column string, err error) error {
	return fmt.Errorf("column '%s' in table %q.%q: %w", column, schema, table, err)
}

func transformerRulesToConfig(rules TransformerRules) *transformers.Config {
	return &transformers.Config{
		Name:              transformers.TransformerType(rules.Name),
		Parameters:        rules.Parameters,
		DynamicParameters: rules.DynamicParameters,
	}
}
