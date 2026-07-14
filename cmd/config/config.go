// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
	"github.com/xataio/pgstream/internal/health"
	"github.com/xataio/pgstream/pkg/otel"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"gopkg.in/yaml.v3"
)

const (
	defaultPostgresBulkBatchSize    = 20000
	defaultPostgresBulkBatchTimeout = time.Duration(30 * time.Second)
	defaultPostgresBulkBatchBytes   = int64(80 * 1024 * 1024) // 80MiB
	defaultPostgresBulkCopyWorkers  = 8
)

func Load() error {
	return LoadFile(viper.GetString("config"))
}

func LoadFile(file string) error {
	return loadFile(file, true)
}

func LoadQuiet() error {
	return loadFile(viper.GetString("config"), false)
}

func loadFile(file string, announce bool) error {
	if file == "" {
		return nil
	}
	if announce {
		fmt.Printf("using config file: %s\n", file) //nolint:forbidigo //logger hasn't been configured yet
	}
	buf, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}
	buf = []byte(expandBracedEnvVars(string(buf)))

	viper.SetConfigFile(file)
	viper.SetConfigType(filepath.Ext(file)[1:])
	if err := viper.ReadConfig(bytes.NewReader(buf)); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}

	transformerRulesFile := viper.GetString("PGSTREAM_TRANSFORMER_RULES_FILE")
	if transformerRulesFile != "" {
		trBuf, err := os.ReadFile(transformerRulesFile)
		if err != nil {
			return fmt.Errorf("reading transformer rules config file: %w", err)
		}
		trBuf = []byte(expandBracedEnvVars(string(trBuf)))

		viper.SetConfigFile(transformerRulesFile)
		viper.SetConfigType(filepath.Ext(transformerRulesFile)[1:])
		if err := viper.MergeConfig(bytes.NewReader(trBuf)); err != nil {
			return fmt.Errorf("parsing transformer rules config: %w", err)
		}
		// reset after merge
		viper.SetConfigFile(file)
		viper.SetConfigType(filepath.Ext(file)[1:])
	}

	return nil
}

func isYAMLConfigFile(cfgFile string) bool {
	switch filepath.Ext(cfgFile) {
	case ".yml", ".yaml":
		return true
	}
	return false
}

func ParseHealthConfig() (*health.Config, error) {
	cfgFile := viper.GetViper().ConfigFileUsed()
	switch {
	case isYAMLConfigFile(cfgFile):
		yamlCfg := struct {
			Instrumentation InstrumentationConfig `mapstructure:"instrumentation" yaml:"instrumentation"`
		}{}
		if err := viper.Unmarshal(&yamlCfg); err != nil {
			return nil, fmt.Errorf("invalid format for instrumentation in config file %q: %w", cfgFile, err)
		}
		return yamlCfg.Instrumentation.toHealthConfig(), nil
	default:
		return envToHealthConfig(), nil
	}
}

func ParseInstrumentationConfig() (*otel.Config, error) {
	cfgFile := viper.GetViper().ConfigFileUsed()
	switch {
	case isYAMLConfigFile(cfgFile):
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
	switch {
	case isYAMLConfigFile(cfgFile):
		yamlCfg := YAMLConfig{}
		if err := viper.Unmarshal(&yamlCfg, viper.DecodeHook(byteSizeDecodeHook())); err != nil {
			return nil, fmt.Errorf("invalid format in config file %q: %w", cfgFile, err)
		}

		buf, err := os.ReadFile(cfgFile)
		if err != nil {
			return nil, err
		}
		buf = []byte(expandBracedEnvVars(string(buf)))
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

func ParseTransformerConfig(filename string) (*transformer.Config, error) {
	if filename == "" {
		return nil, nil
	}

	buf, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	buf = []byte(expandBracedEnvVars(string(buf)))

	yamlConfig := struct {
		Transformations TransformationsConfig `mapstructure:"transformations" yaml:"transformations"`
	}{}
	err = yaml.Unmarshal(buf, &yamlConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid format for transformations config in file %q: %w", filename, err)
	}

	return yamlConfig.Transformations.parseTransformationConfig()
}

func applyPostgresBulkBatchDefaults(batchCfg *batch.Config) {
	if batchCfg.MaxBatchSize == 0 {
		batchCfg.MaxBatchSize = defaultPostgresBulkBatchSize
	}
	if batchCfg.SendConcurrency == 0 {
		batchCfg.SendConcurrency = defaultPostgresBulkCopyWorkers
	}
	if batchCfg.MaxBatchBytes == 0 {
		batchCfg.MaxBatchBytes = defaultPostgresBulkBatchBytes
		// Split the batch bytes budget across the send workers: the max queue
		// bytes semaphore holds both in-flight and accumulating batches, so a
		// single large default batch (80MiB vs the 100MiB default queue) would
		// only ever allow one batch in flight and the send workers could never
		// run concurrently on tables with large rows.
		if batchCfg.SendConcurrency > 1 {
			batchCfg.MaxBatchBytes = defaultPostgresBulkBatchBytes / int64(batchCfg.SendConcurrency)
		}
	}
	if batchCfg.MaxQueueBytes == 0 && batchCfg.SendConcurrency > 1 {
		// Size the in-flight queue to hold every COPY worker's batch plus one
		// accumulating batch, so the send-drainer pool is never starved. Since
		// the batch bytes above are split by SendConcurrency, the total
		// footprint stays bounded to roughly the pre-parallel default (~80MiB)
		// regardless of copy_workers, rather than growing with it.
		batchCfg.MaxQueueBytes = batchCfg.MaxBatchBytes * int64(batchCfg.SendConcurrency+1)
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
		return noPasswordsRolesSnapshotMode, nil
	default:
		return "", errUnsupportedRolesSnapshotMode
	}
}
