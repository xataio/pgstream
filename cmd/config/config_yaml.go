// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/xataio/pgstream/pkg/backoff"
	"github.com/xataio/pgstream/pkg/kafka"
	"github.com/xataio/pgstream/pkg/otel"
	pgschemalog "github.com/xataio/pgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "github.com/xataio/pgstream/pkg/snapshot/generator/postgres/data"
	"github.com/xataio/pgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"github.com/xataio/pgstream/pkg/stream"
	"github.com/xataio/pgstream/pkg/tls"
	kafkacheckpoint "github.com/xataio/pgstream/pkg/wal/checkpointer/kafka"
	"github.com/xataio/pgstream/pkg/wal/listener/snapshot/adapter"
	snapshotbuilder "github.com/xataio/pgstream/pkg/wal/listener/snapshot/builder"
	"github.com/xataio/pgstream/pkg/wal/processor/batch"
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

type InstrumentationConfig struct {
	Metrics *MetricsConfig `mapstructure:"metrics" yaml:"metrics"`
	Traces  *TracesConfig  `mapstructure:"traces" yaml:"traces"`
}

type MetricsConfig struct {
	Endpoint           string `mapstructure:"endpoint" yaml:"endpoint"`
	CollectionInterval int    `mapstructure:"collection_interval" yaml:"collection_interval"`
}

type TracesConfig struct {
	Endpoint    string  `mapstructure:"endpoint" yaml:"endpoint"`
	SampleRatio float64 `mapstructure:"sample_ratio" yaml:"sample_ratio"`
}

type YAMLConfig struct {
	Source    SourceConfig    `mapstructure:"source" yaml:"source"`
	Target    TargetConfig    `mapstructure:"target" yaml:"target"`
	Modifiers ModifiersConfig `mapstructure:"modifiers" yaml:"modifiers"`
}

type SourceConfig struct {
	Postgres *PostgresConfig `mapstructure:"postgres" yaml:"postgres"`
	Kafka    *KafkaConfig    `mapstructure:"kafka" yaml:"kafka"`
}

type TargetConfig struct {
	Postgres *PostgresTargetConfig `mapstructure:"postgres" yaml:"postgres"`
	Kafka    *KafkaTargetConfig    `mapstructure:"kafka" yaml:"kafka"`
	Search   *SearchConfig         `mapstructure:"search" yaml:"search"`
	Webhooks *WebhooksConfig       `mapstructure:"webhooks" yaml:"webhooks"`
}

type PostgresConfig struct {
	URL         string             `mapstructure:"url" yaml:"url"`
	Mode        string             `mapstructure:"mode" yaml:"mode"`
	Snapshot    *SnapshotConfig    `mapstructure:"snapshot" yaml:"snapshot"`
	Replication *ReplicationConfig `mapstructure:"replication" yaml:"replication"`
}

type SnapshotConfig struct {
	Mode            string                  `mapstructure:"mode" yaml:"mode"`
	Tables          []string                `mapstructure:"tables" yaml:"tables"`
	Recorder        *SnapshotRecorderConfig `mapstructure:"recorder" yaml:"recorder"`
	SnapshotWorkers int                     `mapstructure:"snapshot_workers" yaml:"snapshot_workers"`
	Data            *SnapshotDataConfig     `mapstructure:"data" yaml:"data"`
	Schema          *SnapshotSchemaConfig   `mapstructure:"schema" yaml:"schema"`
}

type SnapshotRecorderConfig struct {
	RepeatableSnapshots bool   `mapstructure:"repeatable_snapshots" yaml:"repeatable_snapshots"`
	PostgresURL         string `mapstructure:"postgres_url" yaml:"postgres_url"`
}

type SnapshotDataConfig struct {
	SchemaWorkers int `mapstructure:"schema_workers" yaml:"schema_workers"`
	TableWorkers  int `mapstructure:"table_workers" yaml:"table_workers"`
	BatchPageSize int `mapstructure:"batch_page_size" yaml:"batch_page_size"`
}

type SnapshotSchemaConfig struct {
	Mode            string                 `mapstructure:"mode" yaml:"mode"`
	PgDumpPgRestore *PgDumpPgRestoreConfig `mapstructure:"pgdump_pgrestore" yaml:"pgdump_pgrestore"`
}

type PgDumpPgRestoreConfig struct {
	CleanTargetDB          bool   `mapstructure:"clean_target_db" yaml:"clean_target_db"`
	CreateTargetDB         bool   `mapstructure:"create_target_db" yaml:"create_target_db"`
	IncludeGlobalDBObjects bool   `mapstructure:"include_global_db_objects" yaml:"include_global_db_objects"`
	Role                   string `mapstructure:"role" yaml:"role"`
}

type ReplicationConfig struct {
	ReplicationSlot string `mapstructure:"replication_slot" yaml:"replication_slot"`
}

type KafkaConfig struct {
	Servers       []string            `mapstructure:"servers" yaml:"servers"`
	Topic         TopicConfig         `mapstructure:"topic" yaml:"topic"`
	ConsumerGroup ConsumerGroupConfig `mapstructure:"consumer_group" yaml:"consumer_group"`
	TLS           *TLSConfig          `mapstructure:"tls" yaml:"tls"`
	Backoff       *BackoffConfig      `mapstructure:"backoff" yaml:"backoff"`
}

type TopicConfig struct {
	Name              string `mapstructure:"name" yaml:"name"`
	Partitions        int    `mapstructure:"partitions" yaml:"partitions"`
	ReplicationFactor int    `mapstructure:"replication_factor" yaml:"replication_factor"`
	AutoCreate        bool   `mapstructure:"auto_create" yaml:"auto_create"`
}

type ConsumerGroupConfig struct {
	ID          string `mapstructure:"id" yaml:"id"`
	StartOffset string `mapstructure:"start_offset" yaml:"start_offset"`
}

type TLSConfig struct {
	CACert     string `mapstructure:"ca_cert" yaml:"ca_cert"`
	ClientCert string `mapstructure:"client_cert" yaml:"client_cert"`
	ClientKey  string `mapstructure:"client_key" yaml:"client_key"`
}

type BackoffConfig struct {
	Exponential *ExponentialBackoffConfig `mapstructure:"exponential" yaml:"exponential"`
	Constant    *ConstantBackoffConfig    `mapstructure:"constant" yaml:"constant"`
}

type ExponentialBackoffConfig struct {
	MaxRetries      int `mapstructure:"max_retries" yaml:"max_retries"`
	InitialInterval int `mapstructure:"initial_interval" yaml:"initial_interval"`
	MaxInterval     int `mapstructure:"max_interval" yaml:"max_interval"`
}

type ConstantBackoffConfig struct {
	MaxRetries int `mapstructure:"max_retries" yaml:"max_retries"`
	Interval   int `mapstructure:"interval" yaml:"interval"`
}

type PostgresTargetConfig struct {
	URL               string            `mapstructure:"url" yaml:"url"`
	Batch             *BatchConfig      `mapstructure:"batch" yaml:"batch"`
	BulkIngest        *BulkIngestConfig `mapstructure:"bulk_ingest" yaml:"bulk_ingest"`
	SchemaLogStoreURL string            `mapstructure:"schema_log_store_url" yaml:"schema_log_store_url"`
	DisableTriggers   bool              `mapstructure:"disable_triggers" yaml:"disable_triggers"`
	OnConflictAction  string            `mapstructure:"on_conflict_action" yaml:"on_conflict_action"`
}

type KafkaTargetConfig struct {
	Servers []string         `mapstructure:"servers" yaml:"servers"`
	Topic   KafkaTopicConfig `mapstructure:"topic" yaml:"topic"`
	TLS     *TLSConfig       `mapstructure:"tls" yaml:"tls"`
	Batch   *BatchConfig     `mapstructure:"batch" yaml:"batch"`
}

type KafkaTopicConfig struct {
	Name              string `mapstructure:"name" yaml:"name"`
	Partitions        int    `mapstructure:"partitions" yaml:"partitions"`
	ReplicationFactor int    `mapstructure:"replication_factor" yaml:"replication_factor"`
	AutoCreate        bool   `mapstructure:"auto_create" yaml:"auto_create"`
}

type SearchConfig struct {
	Engine  string         `mapstructure:"engine" yaml:"engine"`
	URL     string         `mapstructure:"url" yaml:"url"`
	Batch   *BatchConfig   `mapstructure:"batch" yaml:"batch"`
	Backoff *BackoffConfig `mapstructure:"backoff" yaml:"backoff"`
}

type BatchConfig struct {
	Timeout       int `mapstructure:"timeout" yaml:"timeout"`
	Size          int `mapstructure:"size" yaml:"size"`
	MaxBytes      int `mapstructure:"max_bytes" yaml:"max_bytes"`
	MaxQueueBytes int `mapstructure:"max_queue_bytes" yaml:"max_queue_bytes"`
}

type BulkIngestConfig struct {
	Enabled bool `mapstructure:"enabled" yaml:"enabled"`
}

type WebhooksConfig struct {
	Subscriptions WebhookSubscriptionsConfig `mapstructure:"subscriptions" yaml:"subscriptions"`
	Notifier      WebhookNotifierConfig      `mapstructure:"notifier" yaml:"notifier"`
}

type WebhookSubscriptionsConfig struct {
	Store  WebhookStoreConfig  `mapstructure:"store" yaml:"store"`
	Server WebhookServerConfig `mapstructure:"server" yaml:"server"`
}

type WebhookStoreConfig struct {
	URL   string              `mapstructure:"url" yaml:"url"`
	Cache *WebhookCacheConfig `mapstructure:"cache" yaml:"cache"`
}

type WebhookCacheConfig struct {
	Enabled         bool `mapstructure:"enabled" yaml:"enabled"`
	RefreshInterval int  `mapstructure:"refresh_interval" yaml:"refresh_interval"`
}

type WebhookServerConfig struct {
	Address      string `mapstructure:"address" yaml:"address"`
	ReadTimeout  int    `mapstructure:"read_timeout" yaml:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout" yaml:"write_timeout"`
}

type WebhookNotifierConfig struct {
	WorkerCount   int `mapstructure:"worker_count" yaml:"worker_count"`
	ClientTimeout int `mapstructure:"client_timeout" yaml:"client_timeout"`
}

type ModifiersConfig struct {
	Injector        *InjectorConfig        `mapstructure:"injector" yaml:"injector"`
	Transformations *TransformationsConfig `mapstructure:"transformations" yaml:"transformations"`
	Filter          *FilterConfig          `mapstructure:"filter" yaml:"filter"`
}

type InjectorConfig struct {
	Enabled      bool   `mapstructure:"enabled" yaml:"enabled"`
	SchemalogURL string `mapstructure:"schemalog_url" yaml:"schemalog_url"`
}

type FilterConfig struct {
	IncludeTables []string `mapstructure:"include_tables" yaml:"include_tables"`
	ExcludeTables []string `mapstructure:"exclude_tables" yaml:"exclude_tables"`
}

type TransformationsConfig struct {
	TransformerRules []TableTransformersConfig `mapstructure:"table_transformers" yaml:"table_transformers"`
	ValidationMode   string                    `mapstructure:"validation_mode" yaml:"validation_mode"`
}
type TableTransformersConfig struct {
	Schema         string                              `mapstructure:"schema" yaml:"schema"`
	Table          string                              `mapstructure:"table" yaml:"table"`
	ColumnRules    map[string]ColumnTransformersConfig `mapstructure:"column_transformers" yaml:"column_transformers"`
	ValidationMode string                              `mapstructure:"validation_mode" yaml:"validation_mode"`
}

type ColumnTransformersConfig struct {
	Name              string         `mapstructure:"name" yaml:"name"`
	Parameters        map[string]any `mapstructure:"parameters" yaml:"parameters"`
	DynamicParameters map[string]any `mapstructure:"dynamic_parameters" yaml:"dynamic_parameters"`
}

// postgres source modes
const (
	replicationMode            = "replication"
	snapshotMode               = "snapshot"
	snapshotAndReplicationMode = "snapshot_and_replication"
)

// snapshot modes
const (
	fullSnapshotMode   = "full"
	dataSnapshotMode   = "data"
	schemaSnapshotMode = "schema"
)

// schema snapshot modes
const (
	pgdumprestoreSchemaMode = "pgdump_pgrestore"
	schemalogSchemaMode     = "schemalog"
)

// search engines
const (
	elasticsearchEngine = "elasticsearch"
	opensearchEngine    = "opensearch"
)

// transformer validation modes
const (
	strictValidationMode     = "strict"
	tableLevelValidationMode = "table_level"
	relaxedValidationMode    = "relaxed"
)

var (
	errUnsupportedSchemaSnapshotMode           = errors.New("unsupported schema snapshot mode, must be one of 'pgdump_pgrestore' or 'schemalog'")
	errUnsupportedSnapshotMode                 = errors.New("unsupported snapshot mode, must be one of 'full', 'schema' or 'data'")
	errUnsupportedPostgresSourceMode           = errors.New("unsupported postgres source mode, must be one of 'replication', 'snapshot' or 'snapshot_and_replication'")
	errUnsupportedTransformationValidationMode = errors.New("unsupported transformation validation mode, must be one of 'strict', 'table_level' or 'relaxed'")
	errUnsupportedTableValidationMode          = errors.New("unsupported table level validation mode, must be either 'strict' or 'relaxed'")
	errTableTransformersNotProvided            = errors.New("table_transformers must be provided when transformation config is set")
	errInvalidTableValidationConfig            = errors.New("table level validation mode should be used when transformation validation mode is set to 'table_level'")
	errUnsupportedSearchEngine                 = errors.New("unsupported search engine, must be one of 'opensearch' or 'elasticsearch'")
	errInvalidPgdumpPgrestoreConfig            = errors.New("pgdump_pgrestore snapshot mode requires target postgres config")
	errInvalidInjectorConfig                   = errors.New("injector config can't infer schemalog url from source postgres url, schemalog_url must be provided")
	errInvalidSnapshotRecorderConfig           = errors.New("snapshot recorder config requires a postgres url")
	errInvalidSampleRatio                      = errors.New("trace sample ratio must be a value between 0.0 and 1.0")
)

func (c *InstrumentationConfig) toOtelConfig() (*otel.Config, error) {
	cfg := &otel.Config{}
	if c.Metrics != nil {
		cfg.Metrics = &otel.MetricsConfig{
			Endpoint:           c.Metrics.Endpoint,
			CollectionInterval: time.Duration(c.Metrics.CollectionInterval) * time.Second,
		}
	}

	if c.Traces != nil {
		if c.Traces.SampleRatio < 0.0 || c.Traces.SampleRatio > 1.0 {
			return nil, errInvalidSampleRatio
		}
		cfg.Traces = &otel.TracesConfig{
			Endpoint:    c.Traces.Endpoint,
			SampleRatio: c.Traces.SampleRatio,
		}
	}

	return cfg, nil
}

func (c *YAMLConfig) toStreamConfig() (*stream.Config, error) {
	listener, err := c.parseListenerConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing source config: %w", err)
	}

	processor, err := c.parseProcessorConfig()
	if err != nil {
		return nil, fmt.Errorf("parsing target and modifier config: %w", err)
	}
	return &stream.Config{
		Listener:  listener,
		Processor: processor,
	}, nil
}

func (c *YAMLConfig) parseListenerConfig() (stream.ListenerConfig, error) {
	streamCfg := stream.ListenerConfig{
		Kafka: c.Source.Kafka.parseKafkaListenerConfig(),
	}

	if c.Source.Postgres == nil {
		return streamCfg, nil
	}

	var err error
	switch c.Source.Postgres.Mode {
	case replicationMode, snapshotAndReplicationMode:
		streamCfg.Postgres, err = c.parsePostgresListenerConfig()
		if err != nil {
			return stream.ListenerConfig{}, fmt.Errorf("parsing postgres listener config: %w", err)
		}
	case snapshotMode:
		streamCfg.Snapshot, err = c.parseSnapshotConfig()
		if err != nil {
			return stream.ListenerConfig{}, fmt.Errorf("parsing postgres snapshot listener config: %w", err)
		}
	default:
		return stream.ListenerConfig{}, errUnsupportedPostgresSourceMode
	}

	return streamCfg, nil
}

func (c *YAMLConfig) parseProcessorConfig() (stream.ProcessorConfig, error) {
	streamCfg := stream.ProcessorConfig{
		Kafka:    c.parseKafkaProcessorConfig(),
		Postgres: c.parsePostgresProcessorConfig(),
		Webhook:  c.parseWebhookProcessorConfig(),
		Filter:   c.parseFilterConfig(),
	}

	var err error
	streamCfg.Injector, err = c.parseInjectorConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	streamCfg.Search, err = c.parseSearchProcessorConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	streamCfg.Transformer, err = c.parseTransformationConfig()
	if err != nil {
		return stream.ProcessorConfig{}, err
	}

	return streamCfg, nil
}

func (c *YAMLConfig) parsePostgresListenerConfig() (*stream.PostgresListenerConfig, error) {
	if c.Source.Postgres == nil {
		return nil, nil
	}

	streamCfg := &stream.PostgresListenerConfig{
		Replication: pgreplication.Config{
			PostgresURL: c.Source.Postgres.URL,
		},
	}

	if c.Source.Postgres.Mode == replicationMode || c.Source.Postgres.Mode == snapshotAndReplicationMode {
		replicationSlotName := ""
		if c.Source.Postgres.Replication != nil {
			replicationSlotName = c.Source.Postgres.Replication.ReplicationSlot
		}
		streamCfg.Replication.ReplicationSlotName = replicationSlotName
	}

	if c.Source.Postgres.Mode == snapshotAndReplicationMode {
		var err error
		streamCfg.Snapshot, err = c.parseSnapshotConfig()
		if err != nil {
			return nil, err
		}
	}

	return streamCfg, nil
}

func (c *YAMLConfig) parseSnapshotConfig() (*snapshotbuilder.SnapshotListenerConfig, error) {
	snapshotConfig := c.Source.Postgres.Snapshot
	if snapshotConfig == nil {
		return nil, nil
	}

	streamCfg := &snapshotbuilder.SnapshotListenerConfig{
		Adapter: adapter.SnapshotConfig{
			Tables:          snapshotConfig.Tables,
			SnapshotWorkers: uint(snapshotConfig.SnapshotWorkers),
		},
	}

	if snapshotConfig.Recorder != nil {
		if snapshotConfig.Recorder.PostgresURL == "" {
			return nil, errInvalidSnapshotRecorderConfig
		}
		streamCfg.Recorder = &snapshotbuilder.SnapshotRecorderConfig{
			RepeatableSnapshots: snapshotConfig.Recorder.RepeatableSnapshots,
			SnapshotStoreURL:    snapshotConfig.Recorder.PostgresURL,
		}
	}

	switch snapshotConfig.Mode {
	case fullSnapshotMode, dataSnapshotMode, schemaSnapshotMode:
	default:
		return nil, errUnsupportedSnapshotMode
	}

	if snapshotConfig.Mode == fullSnapshotMode || snapshotConfig.Mode == dataSnapshotMode {
		streamCfg.Generator = pgsnapshotgenerator.Config{
			URL:           c.Source.Postgres.URL,
			BatchPageSize: uint(snapshotConfig.Data.BatchPageSize),
			SchemaWorkers: uint(snapshotConfig.Data.SchemaWorkers),
			TableWorkers:  uint(snapshotConfig.Data.TableWorkers),
		}
	}

	if snapshotConfig.Mode == fullSnapshotMode || snapshotConfig.Mode == schemaSnapshotMode {
		var err error
		streamCfg.Schema, err = c.parseSchemaSnapshotConfig()
		if err != nil {
			return nil, err
		}
	}

	return streamCfg, nil
}

func (c *YAMLConfig) parseSchemaSnapshotConfig() (snapshotbuilder.SchemaSnapshotConfig, error) {
	schemaSnapshotCfg := c.Source.Postgres.Snapshot.Schema
	if schemaSnapshotCfg == nil {
		return snapshotbuilder.SchemaSnapshotConfig{}, nil
	}

	switch schemaSnapshotCfg.Mode {
	case schemalogSchemaMode:
		return snapshotbuilder.SchemaSnapshotConfig{
			SchemaLogStore: &pgschemalog.Config{
				URL: c.Source.Postgres.URL,
			},
		}, nil
	case pgdumprestoreSchemaMode:
		if c.Target.Postgres == nil {
			return snapshotbuilder.SchemaSnapshotConfig{}, errInvalidPgdumpPgrestoreConfig
		}
		streamSchemaCfg := snapshotbuilder.SchemaSnapshotConfig{
			DumpRestore: &pgdumprestore.Config{
				SourcePGURL: c.Source.Postgres.URL,
				TargetPGURL: c.Target.Postgres.URL,
			},
		}

		if schemaSnapshotCfg.PgDumpPgRestore != nil {
			streamSchemaCfg.DumpRestore.CleanTargetDB = schemaSnapshotCfg.PgDumpPgRestore.CleanTargetDB
			streamSchemaCfg.DumpRestore.IncludeGlobalDBObjects = schemaSnapshotCfg.PgDumpPgRestore.IncludeGlobalDBObjects
			streamSchemaCfg.DumpRestore.CreateTargetDB = schemaSnapshotCfg.PgDumpPgRestore.CreateTargetDB
			streamSchemaCfg.DumpRestore.Role = schemaSnapshotCfg.PgDumpPgRestore.Role
		}

		return streamSchemaCfg, nil
	default:
		return snapshotbuilder.SchemaSnapshotConfig{}, errUnsupportedSchemaSnapshotMode
	}
}

func (c *YAMLConfig) parseInjectorConfig() (*injector.Config, error) {
	if c.Modifiers.Injector == nil || !c.Modifiers.Injector.Enabled {
		return nil, nil
	}

	url := c.Modifiers.Injector.SchemalogURL
	if url == "" {
		if c.Source.Postgres == nil || c.Source.Postgres.URL == "" {
			return nil, errInvalidInjectorConfig
		}
		url = c.Source.Postgres.URL
	}

	return &injector.Config{
		Store: pgschemalog.Config{
			URL: url,
		},
	}, nil
}

func (c *YAMLConfig) parseKafkaProcessorConfig() *stream.KafkaProcessorConfig {
	if c.Target.Kafka == nil {
		return nil
	}

	return &stream.KafkaProcessorConfig{
		Writer: &kafkaprocessor.Config{
			Kafka: kafka.ConnConfig{
				Servers: c.Target.Kafka.Servers,
				Topic: kafka.TopicConfig{
					Name:              c.Target.Kafka.Topic.Name,
					NumPartitions:     c.Target.Kafka.Topic.Partitions,
					ReplicationFactor: c.Target.Kafka.Topic.ReplicationFactor,
					AutoCreate:        c.Target.Kafka.Topic.AutoCreate,
				},
				TLS: c.Target.Kafka.TLS.parseTLSConfig(),
			},
			Batch: c.Target.Kafka.Batch.parseBatchConfig(),
		},
	}
}

func (c *YAMLConfig) parsePostgresProcessorConfig() *stream.PostgresProcessorConfig {
	if c.Target.Postgres == nil {
		return nil
	}

	cfg := &stream.PostgresProcessorConfig{
		BatchWriter: postgres.Config{
			URL:         c.Target.Postgres.URL,
			BatchConfig: c.Target.Postgres.Batch.parseBatchConfig(),
			SchemaLogStore: pgschemalog.Config{
				URL: c.Target.Postgres.SchemaLogStoreURL,
			},
			DisableTriggers:  c.Target.Postgres.DisableTriggers,
			OnConflictAction: c.Target.Postgres.OnConflictAction,
		},
	}

	if c.Target.Postgres.BulkIngest != nil {
		cfg.BatchWriter.BulkIngestEnabled = c.Target.Postgres.BulkIngest.Enabled
		if cfg.BatchWriter.BulkIngestEnabled {
			applyPostgresBulkBatchDefaults(&cfg.BatchWriter.BatchConfig)
		}
	}

	return cfg
}

func (c *YAMLConfig) parseSearchProcessorConfig() (*stream.SearchProcessorConfig, error) {
	if c.Target.Search == nil {
		return nil, nil
	}

	storeCfg := store.Config{}

	switch c.Target.Search.Engine {
	case elasticsearchEngine:
		storeCfg.ElasticsearchURL = c.Target.Search.URL
	case opensearchEngine:
		storeCfg.OpenSearchURL = c.Target.Search.URL
	default:
		return nil, errUnsupportedSearchEngine
	}

	return &stream.SearchProcessorConfig{
		Store: storeCfg,
		Indexer: search.IndexerConfig{
			Batch: c.Target.Search.Batch.parseBatchConfig(),
		},
		Retrier: search.StoreRetryConfig{
			Backoff: c.Target.Search.Backoff.parseBackoffConfig(),
		},
	}, nil
}

func (c *YAMLConfig) parseWebhookProcessorConfig() *stream.WebhookProcessorConfig {
	if c.Target.Webhooks == nil {
		return nil
	}
	streamCfg := &stream.WebhookProcessorConfig{
		SubscriptionStore: stream.WebhookSubscriptionStoreConfig{
			URL: c.Target.Webhooks.Subscriptions.Store.URL,
		},
		Notifier: notifier.Config{
			URLWorkerCount: uint(c.Target.Webhooks.Notifier.WorkerCount),
			ClientTimeout:  time.Duration(c.Target.Webhooks.Notifier.ClientTimeout) * time.Millisecond,
		},
		SubscriptionServer: server.Config{
			Address:      c.Target.Webhooks.Subscriptions.Server.Address,
			ReadTimeout:  time.Duration(c.Target.Webhooks.Subscriptions.Server.ReadTimeout) * time.Second,
			WriteTimeout: time.Duration(c.Target.Webhooks.Subscriptions.Server.WriteTimeout) * time.Second,
		},
	}

	if c.Target.Webhooks.Subscriptions.Store.Cache != nil {
		streamCfg.SubscriptionStore.CacheEnabled = c.Target.Webhooks.Subscriptions.Store.Cache.Enabled
		streamCfg.SubscriptionStore.CacheRefreshInterval = time.Duration(c.Target.Webhooks.Subscriptions.Store.Cache.RefreshInterval) * time.Second
	}

	return streamCfg
}

func (c *YAMLConfig) parseTransformationConfig() (*transformer.Config, error) {
	if c.Modifiers.Transformations == nil {
		// no transformers configured
		return nil, nil
	}

	return c.Modifiers.Transformations.parseTransformationConfig()
}

func (c YAMLConfig) parseFilterConfig() *filter.Config {
	if c.Modifiers.Filter == nil {
		return nil
	}
	return &filter.Config{
		ExcludeTables: c.Modifiers.Filter.ExcludeTables,
		IncludeTables: c.Modifiers.Filter.IncludeTables,
	}
}

func (c TransformationsConfig) parseTransformationConfig() (*transformer.Config, error) {
	if c.TransformerRules == nil {
		// transformation configuration provided, but no rules defined
		return nil, errTableTransformersNotProvided
	}
	if len(c.TransformerRules) == 0 {
		// no transformers configured
		return nil, nil
	}

	var globalValidationMode string
	switch c.ValidationMode {
	case strictValidationMode, relaxedValidationMode, tableLevelValidationMode:
		globalValidationMode = c.ValidationMode
	case "":
		globalValidationMode = relaxedValidationMode
	default:
		return nil, errUnsupportedTransformationValidationMode
	}

	rules := make([]transformer.TableRules, 0, len(c.TransformerRules))
	for _, t := range c.TransformerRules {
		var tableValidationMode string
		switch t.ValidationMode {
		case strictValidationMode, relaxedValidationMode:
			if globalValidationMode != tableLevelValidationMode {
				return nil, errInvalidTableValidationConfig
			}
			tableValidationMode = t.ValidationMode
		case "":
			if globalValidationMode == tableLevelValidationMode {
				return nil, errInvalidTableValidationConfig
			}
			tableValidationMode = globalValidationMode
		default:
			return nil, errUnsupportedTableValidationMode
		}

		columnRules := make(map[string]transformer.TransformerRules, len(t.ColumnRules))
		for column, cr := range t.ColumnRules {
			columnRules[column] = transformer.TransformerRules{
				Name:              cr.Name,
				Parameters:        cr.Parameters,
				DynamicParameters: cr.DynamicParameters,
			}
		}
		rules = append(rules, transformer.TableRules{
			Schema:         t.Schema,
			Table:          t.Table,
			ColumnRules:    columnRules,
			ValidationMode: tableValidationMode,
		})
	}

	return &transformer.Config{
		TransformerRules: rules,
		ValidationMode:   globalValidationMode,
	}, nil
}

func (c *KafkaConfig) parseKafkaListenerConfig() *stream.KafkaListenerConfig {
	if c == nil {
		return nil
	}

	return &stream.KafkaListenerConfig{
		Reader: c.parseKafkaReaderConfig(),
		Checkpointer: kafkacheckpoint.Config{
			CommitBackoff: c.Backoff.parseBackoffConfig(),
		},
	}
}

func (c *KafkaConfig) parseKafkaReaderConfig() kafka.ReaderConfig {
	if c == nil {
		return kafka.ReaderConfig{}
	}
	return kafka.ReaderConfig{
		Conn: kafka.ConnConfig{
			Servers: c.Servers,
			Topic: kafka.TopicConfig{
				Name:              c.Topic.Name,
				NumPartitions:     c.Topic.Partitions,
				ReplicationFactor: c.Topic.ReplicationFactor,
				AutoCreate:        c.Topic.AutoCreate,
			},
			TLS: c.TLS.parseTLSConfig(),
		},
		ConsumerGroupID:          c.ConsumerGroup.ID,
		ConsumerGroupStartOffset: c.ConsumerGroup.StartOffset,
	}
}

func (t *TLSConfig) parseTLSConfig() tls.Config {
	if t == nil {
		return tls.Config{Enabled: false}
	}
	return tls.Config{
		Enabled:        true,
		CaCertFile:     t.CACert,
		ClientCertFile: t.ClientCert,
		ClientKeyFile:  t.ClientKey,
	}
}

func (bo *BackoffConfig) parseBackoffConfig() backoff.Config {
	return backoff.Config{
		Exponential: bo.parseExponentialBackoffConfig(),
		Constant:    bo.parseConstantBackoffConfig(),
	}
}

func (bo *BackoffConfig) parseExponentialBackoffConfig() *backoff.ExponentialConfig {
	if bo == nil || bo.Exponential == nil {
		return nil
	}
	return &backoff.ExponentialConfig{
		InitialInterval: time.Duration(bo.Exponential.InitialInterval) * time.Millisecond,
		MaxInterval:     time.Duration(bo.Exponential.MaxInterval) * time.Millisecond,
		MaxRetries:      uint(bo.Exponential.MaxRetries),
	}
}

func (bo *BackoffConfig) parseConstantBackoffConfig() *backoff.ConstantConfig {
	if bo == nil || bo.Constant == nil {
		return nil
	}
	return &backoff.ConstantConfig{
		Interval:   time.Duration(bo.Constant.Interval) * time.Millisecond,
		MaxRetries: uint(bo.Constant.MaxRetries),
	}
}

func (bc *BatchConfig) parseBatchConfig() batch.Config {
	if bc == nil {
		return batch.Config{}
	}
	return batch.Config{
		BatchTimeout:  time.Duration(bc.Timeout) * time.Millisecond,
		MaxBatchBytes: int64(bc.MaxBytes),
		MaxQueueBytes: int64(bc.MaxQueueBytes),
		MaxBatchSize:  int64(bc.Size),
	}
}
