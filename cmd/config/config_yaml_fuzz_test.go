// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

var config, _ = os.ReadFile("test/test_config.yaml")

func FuzzToStreamConfig(f *testing.F) {
	f.Add(config)
	// Seed with edge cases
	f.Add([]byte(`{}`))
	f.Add([]byte(`source: {}`))
	f.Add([]byte(`target: {}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var yamlConfig YAMLConfig
		if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
			return
		}

		_, err := yamlConfig.toStreamConfig()
		if err != nil {
			t.Logf("Expected error: %v", err)
		}
	})
}

func FuzzYAMLConfigStructure(f *testing.F) {
	// Test various YAML structures that could break parsing
	malformedInputs := []string{
		`source: !!str invalid_source_type`,
		`source: !!int 12345`,
		`source: !!float 123.45`,
		`source: !!bool true`,
		`source: [1, 2, 3]`,
		`target: !!seq ["invalid", "target"]`,
		`modifiers: !!null`,
		`source: {postgres: {url: !!binary "SGVsbG8="}}`,
		`source: {postgres: {mode: !!null}}`,
		`target: {postgres: {batch: {size: !!str "not_a_number"}}}`,
	}

	for _, input := range malformedInputs {
		f.Add([]byte(input))
	}

	f.Fuzz(func(t *testing.T, yamlInput []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("YAML structure fuzzing panicked: %v", r)
			}
		}()

		var yamlConfig YAMLConfig
		err := yaml.Unmarshal(yamlInput, &yamlConfig)
		if err != nil {
			// YAML parsing errors are fine
			return
		}

		// Try to convert to stream config
		_, err = yamlConfig.toStreamConfig()
		// Conversion errors are acceptable, panics are not
		_ = err
	})
}

func FuzzConfigProperties(f *testing.F) {
	f.Fuzz(func(t *testing.T,
		sourceMode uint8,
		snapshotMode uint8,
		targetMode uint8,
		searchEngine uint8,
		modifiersMode uint8,
		batchSize uint16,
		timeout uint32,
	) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Property-based fuzzing panicked: %v", r)
			}
		}()

		cfg := generateYAMLConfigFromProperties(
			sourceMode, snapshotMode, targetMode, searchEngine, modifiersMode, batchSize, timeout)

		// Test marshaling/unmarshaling
		data, err := yaml.Marshal(cfg)
		if err != nil {
			return
		}

		var parsed YAMLConfig
		if err := yaml.Unmarshal(data, &parsed); err != nil {
			return
		}

		// Test conversion to stream config
		_, err = parsed.toStreamConfig()
		// Errors are acceptable for invalid combinations
		_ = err
	})
}

// Helper functions

func generateYAMLConfigFromProperties(
	sourceMode, snapshotMode, targetMode, searchEngine, modifiersMode uint8,
	batchSize uint16,
	timeout uint32,
) *YAMLConfig {
	cfg := &YAMLConfig{}

	// Generate source config
	switch sourceMode % 2 {
	case 0:
		mode := []string{"replication", "snapshot", "snapshot_and_replication"}[sourceMode%3]
		cfg.Source.Postgres = &PostgresConfig{
			URL:  "postgresql://test:test@localhost:5432/test",
			Mode: mode,
		}
		if mode == "snapshot" || mode == "snapshot_and_replication" {
			snapshotModeStr := []string{"full", "data", "schema"}[snapshotMode%3]
			cfg.Source.Postgres.Snapshot = &SnapshotConfig{
				Mode: snapshotModeStr,
			}
		}
	case 1:
		cfg.Source.Kafka = &KafkaConfig{
			Servers: []string{"localhost:9092"},
			Topic:   TopicConfig{Name: "test"},
		}
	}

	// Generate target config
	switch targetMode % 4 {
	case 0:
		cfg.Target.Postgres = &PostgresTargetConfig{
			URL: "postgresql://test:test@localhost:5432/test",
			Batch: &BatchConfig{
				Size:    int(batchSize),
				Timeout: int(timeout),
			},
		}
	case 1:
		cfg.Target.Kafka = &KafkaTargetConfig{
			Servers: []string{"localhost:9092"},
			Topic:   KafkaTopicConfig{Name: "test"},
			Batch: &BatchConfig{
				Size:    int(batchSize),
				Timeout: int(timeout),
			},
		}
	case 2:
		engine := []string{"elasticsearch", "opensearch"}[searchEngine%2]
		cfg.Target.Search = &SearchConfig{
			Engine: engine,
			URL:    "http://localhost:9200",
			Batch: &BatchConfig{
				Size:    int(batchSize),
				Timeout: int(timeout),
			},
		}
	case 3:
		cfg.Target.Webhooks = &WebhooksConfig{
			Subscriptions: WebhookSubscriptionsConfig{
				Store: WebhookStoreConfig{URL: "postgresql://test:test@localhost:5432/test"},
			},
			Notifier: WebhookNotifierConfig{
				ClientTimeout: int(timeout),
			},
		}
	}

	// Generate modifiers config
	switch modifiersMode % 4 {
	case 0:
		// no modifiers
	case 1:
		cfg.Modifiers.Injector = &InjectorConfig{Enabled: true}
	case 2:
		cfg.Modifiers.Filter = &FilterConfig{
			IncludeTables: []string{"test.*"},
		}
	case 3:
		validationModes := []string{"strict", "relaxed", "table_level"}
		cfg.Modifiers.Transformations = &TransformationsConfig{
			ValidationMode: validationModes[modifiersMode%3],
			TransformerRules: []TableTransformersConfig{
				{
					ValidationMode: validationModes[modifiersMode%3],
					Schema:         "public",
					Table:          "test",
					ColumnRules: map[string]ColumnTransformersConfig{
						"name": {Name: "test_transformer"},
					},
				},
			},
		}
	}

	return cfg
}

// Benchmark to catch performance regressions
func BenchmarkToStreamConfig(b *testing.B) {
	var yamlConfig YAMLConfig
	yaml.Unmarshal(config, &yamlConfig)

	for b.Loop() {
		_, _ = yamlConfig.toStreamConfig()
	}
}
