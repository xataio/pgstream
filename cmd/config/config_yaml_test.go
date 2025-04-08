// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.config.toStreamConfig()
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
