// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"gopkg.in/yaml.v3"
)

const (
	defaultPostgresBulkBatchSize    = 20000
	defaultPostgresBulkBatchTimeout = time.Duration(30 * time.Second)
	defaultPostgresBulkBatchBytes   = int64(80 * 1024 * 1024) // 80MiB
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

func ParseInstrumentationConfig() (*otel.Config, error) {
	cfgFile := viper.GetViper().ConfigFileUsed()
	switch ext := filepath.Ext(cfgFile); ext {
	case ".yml", ".yaml":
		yamlCfg := struct {
			Instrumentation InstrumentationConfig `mapstructure:"instrumentation" yaml:"instrumentation"`
		}{}
		if err := viper.Unmarshal(&yamlCfg); err != nil {
			return nil, fmt.Errorf("invalid format for instrumentation in config file %q: %w", cfgFile, err)
		}

		return yamlCfg.Instrumentation.toOtelConfig()
	default:
		return envToOtelConfig()
	}
}

func ParseStreamConfig() (*stream.Config, error) {
	cfgFile := viper.GetViper().ConfigFileUsed()
	switch ext := filepath.Ext(cfgFile); ext {
	case ".yml", ".yaml":
		yamlCfg := YAMLConfig{}
		if err := viper.Unmarshal(&yamlCfg); err != nil {
			return nil, fmt.Errorf("invalid format in config file %q: %w", cfgFile, err)
		}

		buf, err := os.ReadFile(cfgFile)
		if err != nil {
			return nil, err
		}
		// yaml.Unmarshal is used to override the viper.Umarshal to be able to
		// parse the transformers configuration with support for case sensitive
		// keys.
		// https://github.com/spf13/viper/issues/260
		yamlLibCfg := YAMLConfig{}
		err = yaml.Unmarshal(buf, &yamlLibCfg)
		if err != nil {
			return nil, fmt.Errorf("invalid format for config in file %q: %w", cfgFile, err)
		}
		yamlCfg.Modifiers.Transformations = yamlLibCfg.Modifiers.Transformations

		return yamlCfg.toStreamConfig()
	default:
		return envConfigToStreamConfig()
	}
}

func applyPostgresBulkBatchDefaults(batchCfg *batch.Config) {
	if batchCfg.MaxBatchSize == 0 {
		batchCfg.MaxBatchSize = defaultPostgresBulkBatchSize
	}
	if batchCfg.MaxBatchBytes == 0 {
		batchCfg.MaxBatchBytes = defaultPostgresBulkBatchBytes
	}
	if batchCfg.BatchTimeout == 0 {
		batchCfg.BatchTimeout = defaultPostgresBulkBatchTimeout
	}
}

func getRolesSnapshotMode(mode string) (string, error) {
	switch mode {
	case enabledRolesSnapshotMode, disabledRolesSnapshotMode, noPasswordsRolesSnapshotMode:
		return mode, nil
	case "":
		return enabledRolesSnapshotMode, nil
	default:
		return "", errUnsupportedRolesSnapshotMode
	}
}
