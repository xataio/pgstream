// SPDX-License-Identifier: Apache-2.0

package transformer

type Rules struct {
	Transformers   []TableRules `yaml:"transformations"`
	ValidationMode string       `yaml:"validation_mode"`
}

type TableRules struct {
	Schema         string                      `yaml:"schema"`
	Table          string                      `yaml:"table"`
	ColumnRules    map[string]TransformerRules `yaml:"column_transformers"`
	ValidationMode string                      `yaml:"validation_mode"`
}

type TransformerRules struct {
	Name              string         `yaml:"name"`
	Parameters        map[string]any `yaml:"parameters"`
	DynamicParameters map[string]any `yaml:"dynamic_parameters"`
}
