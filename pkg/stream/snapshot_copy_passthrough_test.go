// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	pgwriter "github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
)

// select a writer, not a layer
var targetFields = map[string]struct{}{
	"Kafka":    {},
	"Search":   {},
	"Webhook":  {},
	"Postgres": {},
	"Stdout":   {},
}

// each enables its layer alone
var modifierFields = map[string]func(*ProcessorConfig){
	"Transformer": func(c *ProcessorConfig) { c.Transformer = &transformer.Config{} },
	"Injector":    func(c *ProcessorConfig) { c.Injector = &injector.Config{} },
	"Filter":      func(c *ProcessorConfig) { c.Filter = &filter.Config{} },
	"Sanitize":    func(c *ProcessorConfig) { c.Sanitize = &SanitizeConfig{StripNullCharBytes: true} },
}

// unclassified layers get bypassed
func TestProcessorConfig_allFieldsClassified(t *testing.T) {
	t.Parallel()

	cfgType := reflect.TypeOf(ProcessorConfig{})
	for i := range cfgType.NumField() {
		name := cfgType.Field(i).Name
		_, isTarget := targetFields[name]
		_, isModifier := modifierFields[name]
		switch {
		case isTarget && isModifier:
			t.Errorf("ProcessorConfig field %q is classified as both a target and a modifier", name)
		case !isTarget && !isModifier:
			t.Errorf("unclassified ProcessorConfig field %q: add it to targetFields if it selects a target "+
				"writer, or to modifierFields and have rowVisibleModifiers report it, after deciding whether "+
				"the layer needs to see every row", name)
		}
	}
}

// with allFieldsClassified, fails closed
func TestConfig_snapshotCopyPassthroughEligible_blockedByEveryModifier(t *testing.T) {
	t.Parallel()

	for name, enable := range modifierFields {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			config := newPassthroughEligibleConfig()
			enable(&config.Processor)

			eligible, reason := config.snapshotCopyPassthroughEligible()
			require.False(t, eligible, "%s must block the copy passthrough", name)
			require.Contains(t, reason, "need to see every row")
		})
	}
}

func TestConfig_snapshotCopyPassthroughEligible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		build   func(*Config)
		want    bool
		wantErr string
	}{
		{
			name:  "postgres target with bulk ingest and no modifiers",
			build: func(*Config) {},
			want:  true,
		},
		{
			name:    "non postgres target",
			build:   func(c *Config) { c.Processor.Postgres = nil },
			wantErr: "target is not postgres",
		},
		{
			name:    "bulk ingest disabled",
			build:   func(c *Config) { c.Processor.Postgres.BatchWriter.BulkIngestEnabled = false },
			wantErr: "bulk ingest is disabled",
		},
		{
			name:    "no data snapshot configured",
			build:   func(c *Config) { c.Listener.Postgres.Snapshot.Data = nil },
			wantErr: "no postgres data snapshot configured",
		},
		{
			name: "sanitizer that strips nothing does not block",
			build: func(c *Config) {
				c.Processor.Sanitize = &SanitizeConfig{StripNullCharBytes: false}
			},
			want: true,
		},
		{
			name: "every modifier is named in the reason",
			build: func(c *Config) {
				c.Processor.Transformer = &transformer.Config{}
				c.Processor.Filter = &filter.Config{}
			},
			wantErr: "these layers need to see every row: transformer, filter",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := newPassthroughEligibleConfig()
			tc.build(config)

			eligible, reason := config.snapshotCopyPassthroughEligible()
			require.Equal(t, tc.want, eligible)
			if tc.wantErr != "" {
				require.Equal(t, tc.wantErr, reason)
			}
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

		applied, reason := config.applySnapshotCopyPassthrough()
		require.True(t, applied)
		require.Empty(t, reason)

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

		applied, reason := config.applySnapshotCopyPassthrough()
		require.False(t, applied)
		require.Contains(t, reason, "transformer")
		require.Nil(t, config.Listener.Postgres.Snapshot.Data.CopyPassthrough)
	})
}

func newPassthroughEligibleConfig() *Config {
	return &Config{
		Listener: ListenerConfig{
			Postgres: &PostgresListenerConfig{
				URL: "postgresql://source",
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
