// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type RulesConfig struct{}

type Rules struct {
	Transformers []TableRules `yaml:"transformations"`
}

type TableRules struct {
	Schema      string                      `yaml:"schema"`
	Table       string                      `yaml:"table"`
	ColumnRules map[string]TransformerRules `yaml:"column_transformers"`
}

type TransformerRules struct {
	Name       string         `yaml:"name"`
	Generator  string         `yaml:"generator"`
	Parameters map[string]any `yaml:"parameters"`
}

func readRulesFromFile(filePath string) (*Rules, error) {
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading rules from file: %w", err)
	}

	rules := &Rules{}
	err = yaml.Unmarshal(yamlFile, &rules)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling yaml file into transformer rules: %w", err)
	}
	return rules, nil
}
