// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
	loglib "github.com/xataio/pgstream/pkg/log"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/mocks"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
)

// every row visible layer must block it
func TestConfig_snapshotCopyPassthroughEligible_blockedByEveryModifier(t *testing.T) {
	t.Parallel()

	// injector is left out: injector.New dials the source
	modifiers := map[string]func(*ProcessorConfig){
		"transformer": func(c *ProcessorConfig) { c.Transformer = &transformer.Config{} },
		"filter":      func(c *ProcessorConfig) { c.Filter = &filter.Config{IncludeTables: []string{"*"}} },
		"sanitizer":   func(c *ProcessorConfig) { c.Sanitize = &SanitizeConfig{StripNullCharBytes: true} },
	}

	for name, enable := range modifiers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			config := newPassthroughEligibleConfig()
			enable(&config.Processor)

			require.False(t, config.snapshotCopyPassthroughEligible(newTestChain(t, config)),
				"%s must block the copy passthrough", name)
		})
	}
}

func TestConfig_snapshotCopyPassthroughEligible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func(*Config)

		want bool
	}{
		{
			name:  "postgres target with bulk ingest and no modifiers",
			build: func(*Config) {},
			want:  true,
		},
		{
			name:  "non postgres target",
			build: func(c *Config) { c.Processor.Postgres = nil },
		},
		{
			name:  "bulk ingest disabled",
			build: func(c *Config) { c.Processor.Postgres.BatchWriter.BulkIngestEnabled = false },
		},
		{
			name:  "no data snapshot configured",
			build: func(c *Config) { c.Listener.Postgres.Snapshot.Data = nil },
		},
		{
			name:  "a sanitizer that strips nothing is never wrapped",
			build: func(c *Config) { c.Processor.Sanitize = &SanitizeConfig{} },
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := newPassthroughEligibleConfig()
			tc.build(config)

			require.Equal(t, tc.want, config.snapshotCopyPassthroughEligible(newTestChain(t, config)))
		})
	}
}

func TestConfig_applySnapshotCopyPassthrough(t *testing.T) {
	t.Parallel()

	t.Run("carries the target settings the writer would have applied", func(t *testing.T) {
		t.Parallel()

		config := newPassthroughEligibleConfig()
		config.Processor.Postgres.BatchWriter.DisableTriggers = true
		config.Processor.Postgres.BatchWriter.MaxConnections = 25

		require.True(t, config.applySnapshotCopyPassthrough(newTestChain(t, config)))

		require.Equal(t, &pgsnapshotgenerator.CopyPassthroughConfig{
			TargetURL:       "postgresql://target",
			DisableTriggers: true,
			MaxConnections:  25,
			RetryPolicy:     config.Processor.Postgres.BatchWriter.EffectiveRetryPolicy(),
		}, config.Listener.Postgres.Snapshot.Data.CopyPassthrough)
	})

	t.Run("leaves the data snapshot alone when not eligible", func(t *testing.T) {
		t.Parallel()

		config := newPassthroughEligibleConfig()
		config.Processor.Transformer = &transformer.Config{}

		require.False(t, config.applySnapshotCopyPassthrough(newTestChain(t, config)))
		require.Nil(t, config.Listener.Postgres.Snapshot.Data.CopyPassthrough)
	})
}

// assembled the way the pipeline assembles it
func newTestChain(t *testing.T, config *Config) *processorChain {
	t.Helper()

	chain, closer, err := addProcessorModifiers(t.Context(), config, loglib.NewNoopLogger(),
		&mocks.Processor{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closer()) })
	return chain
}

func newPassthroughEligibleConfig() *Config {
	return &Config{
		Listener: ListenerConfig{
			Postgres: &PostgresListenerConfig{
				// empty: a source url makes the transformer layer dial it
				Snapshot: &builder.SnapshotListenerConfig{
					Data: &pgsnapshotgenerator.Config{URL: "postgresql://source"},
				},
			},
		},
		Processor: ProcessorConfig{
			Postgres: &PostgresProcessorConfig{
				BatchWriter: pgwriter.Config{
					URL:               "postgresql://target",
					BulkIngestEnabled: true,
				},
			},
		},
	}
}
