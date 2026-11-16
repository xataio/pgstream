// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"gopkg.in/yaml.v3"
)

var errNoTransformationsToReload = errors.New("no transformation rules configured to reload")

// NewTransformerConfigLoader returns a loader for the transformation rules
// configured by the given files. Inline rules in a YAML config file take
// precedence; otherwise the separate transformation rules file is used. A nil
// loader means there is no file-based transformation source to reload.
func NewTransformerConfigLoader(configFile, transformerRulesFile string) transformer.LoadConfigFn {
	switch {
	case isYAMLConfigFile(configFile):
		return func(ctx context.Context) (*transformer.Config, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return loadYAMLTransformerConfig(configFile)
		}
	case transformerRulesFile != "":
		return func(ctx context.Context) (*transformer.Config, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			cfg, err := ParseTransformerConfig(transformerRulesFile)
			if err != nil {
				return nil, err
			}
			return validateReloadTransformerConfig(cfg)
		}
	default:
		return nil
	}
}

func loadYAMLTransformerConfig(filename string) (*transformer.Config, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	buf = []byte(expandBracedEnvVars(string(buf)))

	yamlConfig := YAMLConfig{}
	if err := yaml.Unmarshal(buf, &yamlConfig); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if yamlConfig.Modifiers.Transformations == nil {
		return nil, errNoTransformationsToReload
	}

	cfg, err := yamlConfig.Modifiers.Transformations.parseTransformationConfig()
	if err != nil {
		return nil, err
	}
	return validateReloadTransformerConfig(cfg)
}

func validateReloadTransformerConfig(cfg *transformer.Config) (*transformer.Config, error) {
	if cfg == nil || len(cfg.TransformerRules) == 0 {
		return nil, errNoTransformationsToReload
	}
	if cfg.InferFromSecurityLabels {
		return nil, errors.New("reloading transformations inferred from security labels is not supported")
	}
	return cfg, nil
}
