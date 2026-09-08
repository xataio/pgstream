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
	for tableIdx, table := range rules.Transformers {
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
				return nil, columnRuleError(tableIdx, table.Schema, table.Table, colName, err)
			}
			transformerMap.AddActiveTransformer(table.Schema, table.Table, colName, transformer)
		}
	}
	return transformerMap, nil
}

// columnRuleError attributes err to the column rule that produced it, using the
// phrasing every check in the rule parsers shares.
func columnRuleError(tableIdx int, schema, table, column string, err error) error {
	return fmt.Errorf("%s: column '%s' in table %q.%q: %w", tableRulePosition(tableIdx), column, schema, table, err)
}

// tableRulePosition names the entry of the table_transformers list that a rule
// belongs to. The list is a slice all the way from the config file, so the
// index matches the position the user wrote, and reads back as a yq path:
// yq '.transformations.table_transformers[2]' config.yaml
func tableRulePosition(tableIdx int) string {
	return fmt.Sprintf("table_transformers[%d]", tableIdx)
}

func transformerRulesToConfig(rules TransformerRules) *transformers.Config {
	return &transformers.Config{
		Name:              transformers.TransformerType(rules.Name),
		Parameters:        rules.Parameters,
		DynamicParameters: rules.DynamicParameters,
	}
}
