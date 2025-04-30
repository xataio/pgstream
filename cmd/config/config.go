// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/pkg/stream"
	"gopkg.in/yaml.v3"
)

func Load() error {
	return LoadFile(viper.GetString("config"))
}

func LoadFile(file string) error {
	if file == "" {
		return nil
	}
	fmt.Printf("using config file: %s\n", file) //nolint:forbidigo //logger hasn't been configured yet
	viper.SetConfigFile(file)
	viper.SetConfigType(filepath.Ext(file)[1:])
	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("reading config: %w", err)
	}

	transformerRulesFile := viper.GetString("PGSTREAM_TRANSFORMER_RULES_FILE")
	if transformerRulesFile != "" {
		viper.SetConfigFile(transformerRulesFile)
		viper.SetConfigType(filepath.Ext(transformerRulesFile)[1:])
		if err := viper.MergeInConfig(); err != nil {
			return fmt.Errorf("reading transformer rules config: %w", err)
		}
		// reset after merge
		viper.SetConfigFile(file)
		viper.SetConfigType(filepath.Ext(file)[1:])
	}

	return nil
}

func ParseStreamConfig() (*stream.Config, error) {
	cfgFile := viper.GetViper().ConfigFileUsed()
	switch ext := filepath.Ext(cfgFile); ext {
	case ".yml", ".yaml":
		buf, err := os.ReadFile(cfgFile)
		if err != nil {
			return nil, err
		}
		yamlCfg := YAMLConfig{}
		if err := viper.Unmarshal(&yamlCfg); err != nil {
			return nil, fmt.Errorf("invalid format in config file %q: %w", cfgFile, err)
		}
		// yaml.Unmarshal is used to override the viper.Umarshal to be able to
		// parse the transformers configuration with support for case sensitive
		// keys.
		// https://github.com/spf13/viper/issues/260
		err = yaml.Unmarshal(buf, &yamlCfg.Modifiers)
		if err != nil {
			return nil, fmt.Errorf("invalid format for modifiers config in file %q: %w", cfgFile, err)
		}
		return yamlCfg.toStreamConfig()
	default:
		return envConfigToStreamConfig()
	}
}
