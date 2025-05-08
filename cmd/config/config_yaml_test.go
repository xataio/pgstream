// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgstream/pkg/otel"
)

func TestYAMLConfig_toStreamConfig(t *testing.T) {
	require.NoError(t, LoadFile("test/test_config.yaml"))

	var config YAMLConfig
	err := viper.Unmarshal(&config)
	require.NoError(t, err)

	streamConfig, err := config.toStreamConfig()
	require.NoError(t, err)

	validateTestStreamConfig(t, streamConfig)
}

func TestYAMLConfig_toStreamConfig_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  YAMLConfig
		wantErr error
	}{
		{
			name: "err - invalid postgres source mode",
			config: YAMLConfig{
				Source: SourceConfig{
					Postgres: &PostgresConfig{
						Mode: "invalid",
					},
				},
			},

			wantErr: errUnsupportedPostgresSourceMode,
		},
		{
			name: "err - invalid postgres snapshot mode",
			config: YAMLConfig{
				Source: SourceConfig{
					Postgres: &PostgresConfig{
						Mode: snapshotMode,
						Snapshot: &SnapshotConfig{
							Mode: "invalid",
						},
					},
				},
			},

			wantErr: errUnsupportedSnapshotMode,
		},
		{
			name: "err - invalid snapshot recorder config",
			config: YAMLConfig{
				Source: SourceConfig{
					Postgres: &PostgresConfig{
						Mode: snapshotMode,
						Snapshot: &SnapshotConfig{
							Mode:     fullSnapshotMode,
							Recorder: &SnapshotRecorderConfig{},
						},
					},
				},
			},

			wantErr: errInvalidSnapshotRecorderConfig,
		},
		{
			name: "err - invalid postgres snapshot schema mode",
			config: YAMLConfig{
				Source: SourceConfig{
					Postgres: &PostgresConfig{
						Mode: snapshotMode,
						Snapshot: &SnapshotConfig{
							Mode: schemaSnapshotMode,
							Schema: &SnapshotSchemaConfig{
								Mode: "invalid",
							},
						},
					},
				},
			},

			wantErr: errUnsupportedSchemaSnapshotMode,
		},
		{
			name: "err - invalid postgres snapshot schema mode",
			config: YAMLConfig{
				Source: SourceConfig{
					Postgres: &PostgresConfig{
						Mode: snapshotMode,
						Snapshot: &SnapshotConfig{
							Mode: schemaSnapshotMode,
							Schema: &SnapshotSchemaConfig{
								Mode: "invalid",
							},
						},
					},
				},
			},

			wantErr: errUnsupportedSchemaSnapshotMode,
		},
		{
			name: "err - invalid pgdump pgrestore config",
			config: YAMLConfig{
				Source: SourceConfig{
					Postgres: &PostgresConfig{
						Mode: snapshotMode,
						Snapshot: &SnapshotConfig{
							Mode: schemaSnapshotMode,
							Schema: &SnapshotSchemaConfig{
								Mode: pgdumprestoreSchemaMode,
								PgDumpPgRestore: &PgDumpPgRestoreConfig{
									CleanTargetDB: false,
								},
							},
						},
					},
				},
			},

			wantErr: errInvalidPgdumpPgrestoreConfig,
		},
		{
			name: "err - invalid injector config",
			config: YAMLConfig{
				Modifiers: ModifiersConfig{
					Injector: &InjectorConfig{
						Enabled: true,
					},
				},
			},

			wantErr: errInvalidInjectorConfig,
		},
		{
			name: "err - invalid transformers config",
			config: YAMLConfig{
				Modifiers: ModifiersConfig{
					Transformations: &TransformationsConfig{
						ValidationMode:   "strict",
						TransformerRules: nil,
					},
				},
			},

			wantErr: errTableTransformersNotProvided,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.config.toStreamConfig()
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestInstrumentationConfig_toOtelConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config InstrumentationConfig

		wantConfig *otel.Config
		wantErr    error
	}{
		{
			name: "valid config",
			config: InstrumentationConfig{
				Metrics: &MetricsConfig{
					Endpoint:           "http://localhost:8080/metrics",
					CollectionInterval: 10,
				},
				Traces: &TracesConfig{
					Endpoint:    "http://localhost:8080/traces",
					SampleRatio: 0.5,
				},
			},
			wantConfig: &otel.Config{
				Metrics: &otel.MetricsConfig{
					Endpoint:           "http://localhost:8080/metrics",
					CollectionInterval: time.Second * 10,
				},
				Traces: &otel.TracesConfig{
					Endpoint:    "http://localhost:8080/traces",
					SampleRatio: 0.5,
				},
			},
			wantErr: nil,
		},
		{
			name: "err - invalid trace sample ratio",
			config: InstrumentationConfig{
				Traces: &TracesConfig{
					SampleRatio: 1.5,
				},
			},
			wantConfig: nil,
			wantErr:    errInvalidSampleRatio,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := tt.config.toOtelConfig()
			require.Equal(t, tt.wantConfig, cfg)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
