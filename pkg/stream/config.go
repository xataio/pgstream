// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	pglib "github.com/xataio/pgstream/internal/postgres"
	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/filter"
	"github.com/xataio/pgstream/pkg/wal/processor/injector"
	kafkaprocessor "github.com/xataio/pgstream/pkg/wal/processor/kafka"
	"github.com/xataio/pgstream/pkg/wal/processor/postgres"
	"github.com/xataio/pgstream/pkg/wal/processor/search"
	"github.com/xataio/pgstream/pkg/wal/processor/search/store"
	"github.com/xataio/pgstream/pkg/wal/processor/transformer"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/notifier"
	"github.com/xataio/pgstream/pkg/wal/processor/webhook/subscription/server"
	pgreplication "github.com/xataio/pgstream/pkg/wal/replication/postgres"
)

type Config struct {
	Listener  ListenerConfig
	Processor ProcessorConfig
}

type ListenerConfig struct {
	Postgres *PostgresListenerConfig
	Kafka    *KafkaListenerConfig
}

type PostgresListenerConfig struct {
	URL         string
	Replication pgreplication.Config
	RetryPolicy backoff.Config
	Snapshot    *snapshotbuilder.SnapshotListenerConfig
}

type KafkaListenerConfig struct {
	Reader       kafka.ReaderConfig
	Checkpointer kafkacheckpoint.Config
}

type SanitizeConfig struct {
	StripNullCharBytes bool
}

type ProcessorConfig struct {
	Kafka       *KafkaProcessorConfig
	Search      *SearchProcessorConfig
	Webhook     *WebhookProcessorConfig
	Postgres    *PostgresProcessorConfig
	Stdout      *StdoutProcessorConfig
	Injector    *injector.Config
	Transformer *transformer.Config
	Filter      *filter.Config
	Sanitize    *SanitizeConfig
}

type StdoutProcessorConfig struct{}

type KafkaProcessorConfig struct {
	Writer *kafkaprocessor.Config
}

type SearchProcessorConfig struct {
	Indexer search.IndexerConfig
	Store   store.Config
	Retrier search.StoreRetryConfig
}

type WebhookProcessorConfig struct {
	Notifier           notifier.Config
	SubscriptionServer server.Config
	SubscriptionStore  WebhookSubscriptionStoreConfig
}

type PostgresProcessorConfig struct {
	BatchWriter postgres.Config
}

type WebhookSubscriptionStoreConfig struct {
	URL                  string
	CacheEnabled         bool
	CacheRefreshInterval time.Duration
}

func (c *Config) IsValid() error {
	if err := c.Listener.IsValid(); err != nil {
		return err
	}
	if err := c.Processor.IsValid(); err != nil {
		return err
	}
	return c.validateTableSelections()
}

func (c *ListenerConfig) IsValid() error {
	listenerCount := 0
	if c.Kafka != nil {
		listenerCount++
	}
	if c.Postgres != nil {
		listenerCount++
	}

	switch listenerCount {
	case 0:
		return errors.New("need at least one listener configured")
	case 1:
		// Only one listener is configured, do nothing
		return nil
	default:
		// More than one listener is configured, return an error
		return fmt.Errorf("only one listener can be configured at a time, found %d", listenerCount)
	}
}

func (c *ProcessorConfig) IsValid() error {
	processorCount := 0
	if c.Kafka != nil {
		processorCount++
	}
	if c.Postgres != nil {
		processorCount++
	}
	if c.Search != nil {
		processorCount++
	}
	if c.Webhook != nil {
		processorCount++
	}
	if c.Stdout != nil {
		processorCount++
	}

	switch processorCount {
	case 0:
		return errors.New("need at least one processor configured")
	case 1:
		// Only one processor is configured, do nothing
		return nil
	default:
		// More than one processor is configured, return an error
		return fmt.Errorf("only one processor can be configured at a time, found %d", processorCount)
	}
}

func (c *Config) validateTableSelections() error {
	if c.Listener.Postgres != nil && c.Listener.Postgres.Snapshot != nil {
		adapter := c.Listener.Postgres.Snapshot.Adapter
		if _, err := NewTableSelection(adapter.Tables, adapter.ExcludedTables); err != nil {
			return fmt.Errorf("snapshot table selection: %w", err)
		}
		schemaOnlyMap, err := pglib.NewSchemaTableMap(adapter.SchemaOnlyTables)
		if err != nil {
			return fmt.Errorf("snapshot schema-only table selection: %w", err)
		}
		// the snapshot generators can't resolve a wildcard schema with a
		// specific table; reject it up front instead of mid-snapshot
		if err := schemaOnlyMap.ValidateWildcardSchema(); err != nil {
			return fmt.Errorf("snapshot schema-only table selection: %w", err)
		}
		// schema-only tables are consumed by the schema snapshot layer; without
		// it they would be silently dropped from the snapshot altogether
		if len(adapter.SchemaOnlyTables) > 0 && c.Listener.Postgres.Snapshot.Schema == nil {
			return errors.New("snapshot schema-only tables require a schema snapshot to be configured")
		}
	}
	if c.Processor.Filter != nil {
		filter := c.Processor.Filter
		if _, err := NewTableSelection(filter.IncludeTables, filter.ExcludeTables); err != nil {
			return fmt.Errorf("replication table selection: %w", err)
		}
		if _, err := pglib.NewSchemaTableMap(filter.SchemaOnlyTables); err != nil {
			return fmt.Errorf("replication schema-only table selection: %w", err)
		}
	}
	return nil
}

func (c *Config) SourcePostgresURL() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.URL
	}
	return ""
}

func (c *Config) PostgresReplicationSlot() string {
	if c.Listener.Postgres != nil {
		return c.Listener.Postgres.Replication.ReplicationSlotName
	}
	return ""
}

func (c *Config) isInjectorEnabled() bool {
	return c.Processor.Injector != nil && c.Processor.Injector.URL != ""
}

// restoreConflictTargetsBeforeData reports whether the schema snapshot must
// restore primary keys, unique constraints and unique indexes before the data
// snapshot runs. This is required when the postgres batch writer emits
// INSERT ... ON CONFLICT DO UPDATE, since the target table needs a matching
// conflict target at insert time.
func (c *Config) restoreConflictTargetsBeforeData() bool {
	if c.Processor.Postgres == nil {
		return false
	}
	bw := c.Processor.Postgres.BatchWriter
	return !bw.BulkIngestEnabled && strings.EqualFold(bw.OnConflictAction, "update")
}

// applySnapshotRawJSONValues enables raw (text) decoding of json/jsonb values
// on the snapshot data generator when the target is postgres. The default pgx
// decoding unmarshals json/jsonb into Go values, turning the JSON null value
// ('null'::jsonb) into Go nil — indistinguishable from SQL NULL — which the
// postgres writer would then write as SQL NULL. Other targets keep the
// unmarshalled representation they expect.
func (c *Config) applySnapshotRawJSONValues() {
	if c.Processor.Postgres == nil {
		return
	}
	if c.Listener.Postgres == nil || c.Listener.Postgres.Snapshot == nil || c.Listener.Postgres.Snapshot.Data == nil {
		return
	}
	c.Listener.Postgres.Snapshot.Data.RawJSONValues = true
}

func (c *Config) GetInitConfig(opts ...InitOption) *InitConfig {
	initConfig := &InitConfig{
		PostgresURL:               c.SourcePostgresURL(),
		ReplicationSlotName:       c.PostgresReplicationSlot(),
		InjectorMigrationsEnabled: c.isInjectorEnabled(),
	}

	for _, opt := range opts {
		opt(initConfig)
	}

	return initConfig
}

// SnapshotConnectionDemand reports the peak number of concurrent source
// connections the data snapshot will open (snapshot_workers × table_workers,
// with defaults applied), and whether a data snapshot is configured at all.
// Callers use ok to gate the resources preflight check: there's no demand to
// size when no data snapshot runs.
func (c *Config) SnapshotConnectionDemand() (demand uint, ok bool) {
	if c.Listener.Postgres == nil || c.Listener.Postgres.Snapshot == nil {
		return 0, false
	}
	data := c.Listener.Postgres.Snapshot.Data
	if data == nil {
		return 0, false
	}
	return data.EffectiveSnapshotWorkers() * data.EffectiveTableWorkers(), true
}

func (c *Config) RequiredTables() []string {
	requiredTables := []string{}
	if c.Listener.Postgres != nil {
		if c.Listener.Postgres.Snapshot != nil {
			requiredTables = append(requiredTables, c.Listener.Postgres.Snapshot.Adapter.Tables...)
		}
	}
	return requiredTables
}

// TableSelection captures the include/exclude filter pgstream applies to the
// WAL stream. Empty include means "every user table is in scope"; exclude
// names tables to skip. Include and exclude are mutually exclusive at the
// source-config level (validated by the filter package).
type TableSelection struct {
	include       []string
	exclude       []string
	includeMap    pglib.SchemaTableMap
	excludeMap    pglib.SchemaTableMap
	schemaOnlyMap pglib.SchemaTableMap
}

func NewTableSelection(include, exclude []string) (TableSelection, error) {
	s := TableSelection{include: include, exclude: exclude}
	var err error
	if len(include) > 0 {
		s.includeMap, err = pglib.NewSchemaTableMap(include)
		if err != nil {
			return TableSelection{}, fmt.Errorf("include: %w", err)
		}
	}
	if len(exclude) > 0 {
		s.excludeMap, err = pglib.NewSchemaTableMap(exclude)
		if err != nil {
			return TableSelection{}, fmt.Errorf("exclude: %w", err)
		}
	}
	return s, nil
}

func (s TableSelection) IsUnfiltered() bool {
	return len(s.include) == 0 && len(s.exclude) == 0
}

// IsTableInScope mirrors the precedence the wal filter applies to data (DML)
// events: exclude beats everything, an exact include entry beats a schema-only
// match, and a schema-only match beats a wildcard include.
func (s TableSelection) IsTableInScope(schema, table string) bool {
	if s.excludeMap.ContainsSchemaTable(schema, table) {
		return false
	}
	if s.includeMap.ContainsExactSchemaTable(schema, table) {
		return true
	}
	if s.schemaOnlyMap.ContainsSchemaTable(schema, table) {
		return false
	}
	if s.includeMap == nil {
		return true
	}
	return s.includeMap.ContainsSchemaTable(schema, table)
}

func (s TableSelection) Include() []string { return s.include }

func (s TableSelection) Exclude() []string { return s.exclude }

func (c *Config) SnapshotTableSelection() TableSelection {
	if c.Listener.Postgres == nil || c.Listener.Postgres.Snapshot == nil {
		return TableSelection{}
	}
	// IsValid is the gate that catches malformed entries; if a caller skipped
	// it the constructor error is swallowed and the lazy fallback in
	// IsTableInScope produces a defined (over-permissive) answer.
	// Schema-only tables are part of the snapshot scope (pg_dump reads them),
	// so they are added to the include list.
	adapter := c.Listener.Postgres.Snapshot.Adapter
	sel, _ := NewTableSelection(appendTables(adapter.Tables, adapter.SchemaOnlyTables), adapter.ExcludedTables)
	return sel
}

func (c *Config) ReplicationTableSelection() TableSelection {
	if c.Processor.Filter == nil {
		return TableSelection{}
	}
	// IsValid is the gate that catches malformed entries; if a caller skipped
	// it the constructor error is swallowed and the lazy fallback in
	// IsTableInScope produces a defined (over-permissive) answer.
	// Schema-only tables have their data events filtered out, so they don't
	// need a replica identity; IsTableInScope applies the same per-table
	// precedence as the wal filter.
	filter := c.Processor.Filter
	sel, _ := NewTableSelection(filter.IncludeTables, filter.ExcludeTables)
	sel.schemaOnlyMap, _ = pglib.NewSchemaTableMap(filter.SchemaOnlyTables)
	return sel
}

func (c *Config) AccessTableSelection() TableSelection {
	snap := c.SnapshotTableSelection()
	rep := c.ReplicationTableSelection()

	if snap.IsUnfiltered() || rep.IsUnfiltered() {
		return TableSelection{}
	}

	switch {
	case len(snap.include) > 0 && len(rep.include) > 0:
		sel, _ := NewTableSelection(appendTables(snap.include, rep.include), nil)
		return sel
	case len(snap.exclude) > 0 && len(rep.exclude) > 0:
		sel, _ := NewTableSelection(nil, intersection(snap.exclude, rep.exclude))
		return sel
	default:
		return TableSelection{}
	}
}

// appendTables returns the deduplicated union of both table lists, preserving
// order and without mutating either of them.
func appendTables(tables, moreTables []string) []string {
	merged := slices.Clone(tables)
	for _, table := range moreTables {
		if !slices.Contains(merged, table) {
			merged = append(merged, table)
		}
	}
	return merged
}

func intersection(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	aSet := make(map[string]struct{}, len(a))
	for _, s := range a {
		aSet[s] = struct{}{}
	}
	var out []string
	for _, s := range b {
		if _, ok := aSet[s]; ok {
			out = append(out, s)
		}
	}
	return out
}
