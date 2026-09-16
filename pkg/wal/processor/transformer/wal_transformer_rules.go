// SPDX-License-Identifier: Apache-2.0

package transformer

import (
	"errors"

	"github.com/xataio/pgstream/pkg/transformers"
)

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
	Name                string         `yaml:"name"`
	Parameters          map[string]any `yaml:"parameters"`
	DynamicParameters   map[string]any `yaml:"dynamic_parameters"`
	AllowUniquenessLoss bool           `yaml:"allow_uniqueness_loss"`
	ArrayOptions        *ArrayOptions  `yaml:"array_options"` // optional
}

type ArrayOptions struct {
	Generator string `yaml:"generator"`
	MinCount  *int   `yaml:"min_count"`
	MaxCount  *int   `yaml:"max_count"`
}

var (
	errArrayOptionsOnScalarColumn  = errors.New("array_options is only valid on an array column")
	errArrayCountsRequired         = errors.New(`min_count and max_count are required when array_options.generator is "random"`)
	errArrayCountsNotAllowed       = errors.New(`min_count and max_count are only valid when array_options.generator is "random"`)
	errArrayOptionsRequirePostgres = errors.New("array_options requires a source postgres connection, which resolves the column type")
)

func (o *ArrayOptions) toArrayConfig() (transformers.ArrayConfig, error) {
	cfg := transformers.ArrayConfig{Generator: transformers.ArrayGeneratorMap}
	if o == nil {
		return cfg, nil
	}
	if o.Generator != "" {
		cfg.Generator = transformers.ArrayGenerator(o.Generator)
	}

	switch cfg.Generator {
	case transformers.ArrayGeneratorMap:
		if o.MinCount != nil || o.MaxCount != nil {
			return cfg, errArrayCountsNotAllowed
		}
	case transformers.ArrayGeneratorRandom:
		if o.MinCount == nil || o.MaxCount == nil {
			return cfg, errArrayCountsRequired
		}
		cfg.MinCount, cfg.MaxCount = *o.MinCount, *o.MaxCount
	}
	// an unknown generator is rejected by the array transformer itself
	return cfg, nil
}
